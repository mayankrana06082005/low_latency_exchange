// benchmark: Section 8 of the plan. Runs the same harness against the binary
// protocol and the HTTP/1.1 + JSON baseline.
//
//   benchmark sizes        1. wire bytes per message type
//   benchmark codec        2. encode/decode CPU cost, in-process, no sockets
//   benchmark rtt          3+4. dispatch latency and round-trip latency, M clients
//   benchmark throughput   5. peak messages/s through one dispatcher
//   benchmark idle         7. idle policy comparison (never vs fixed 100 ms / 1 s / 5 s)
//   benchmark all          everything above
//
// rtt and throughput start an in-process server on loopback, or target a
// running server with --host/--port/--text-port (6: two machines on a LAN).
// Build with the release preset for meaningful numbers.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "bench/LoadClient.hpp"
#include "bench/Report.hpp"
#include "client/ClientSession.hpp"
#include "codec/BinaryCodec.hpp"
#include "codec/TextCodec.hpp"
#include "core/LatencyRecorder.hpp"
#include "core/MarketConfig.hpp"
#include "net/Socket.hpp"
#include "server/Exchange.hpp"

using namespace wire;
using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;
using net::Protocol;

namespace {

struct Options {
    std::string command = "all";
    std::string host;  // empty = in-process server
    uint16_t port = 9000;
    uint16_t text_port = 9001;
    int clients = 4;
    int requests = 20000;  // per client, rtt
    int warmup = 2000;     // per client, rtt
    int window = 32;       // in-flight requests per client, throughput
    double seconds = 3;    // throughput duration per codec
    int iters = 200000;    // codec microbenchmark
    int gap_ms = 1500;     // idle: quiet time before each measured action
    int samples = 5;       // idle: measurements per policy
    int trickle_period_ms = 2000;
    double trickle_seconds = 8;
    std::string csv_dir;
};

const Protocol kProtocols[] = {Protocol::Binary, Protocol::Text};

// Display name: our custom binary protocol vs the HTTP/1.1 + JSON baseline.
const char* label(Protocol p) { return p == Protocol::Binary ? "custom" : "http-json"; }

int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

uint64_t nsSince(Clock::time_point t0) {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count());
}

void title(const char* text) { std::printf("\n=== %s ===\n", text); }

std::shared_ptr<codec::SymbolTable> marketSymbols() {
    auto t = std::make_shared<codec::SymbolTable>();
    t->addAll(core::MarketConfig::defaults().toListFeed());
    return t;
}

// ----------------------------------------------------------- shared fixtures

struct Sample {
    const char* name;
    Message msg;
};

// Representative messages with realistic magnitudes (16-digit client ids,
// worker-tagged order ids), so text sizes are not flattered by small numbers.
std::vector<Sample> sampleMessages() {
    const uint64_t cid = 8841120934410029ULL;
    const uint64_t order = (1ULL << 56) | 1042;
    const Trade trade{(1ULL << 56) | 77, 180, 25, (1ULL << 56) | 1001};
    auto with = [](Message m, uint16_t callback) {
        m.header.callback_port = callback;
        return m;
    };
    return {
        {"BID", with(makeMessage(MsgType::Bid, 17, cid, OrderRequest{1, 180, 50}), 9200)},
        {"ASK", with(makeMessage(MsgType::Ask, 18, cid, OrderRequest{1, 181, 50}), 9200)},
        {"CANCEL", with(makeMessage(MsgType::Cancel, 19, cid, CancelRequest{order}), 9200)},
        {"LIST_FEED", makeMessage(MsgType::ListFeed, 1, 0, Empty{})},
        {"SUBSCRIBE", with(makeMessage(MsgType::Subscribe, 20, cid, SubscribeRequest{9100, 0b011}), 9200)},
        {"ORDER_ACK resting", makeMessage(MsgType::OrderAck, 17, cid, OrderAck{order, OrderStatus::Resting, 50, {}})},
        {"ORDER_ACK 1 trade", makeMessage(MsgType::OrderAck, 17, cid, OrderAck{order, OrderStatus::PartialFill, 25, {trade}})},
        {"ORDER_UPDATE 1 trade", makeMessage(MsgType::OrderUpdate, 0, cid, OrderAck{order, OrderStatus::Filled, 0, {trade}})},
        {"CANCEL_ACK", makeMessage(MsgType::CancelAck, 19, cid, CancelAck{order, OrderStatus::Cancelled})},
        {"SUBSCRIBE_ACK", makeMessage(MsgType::SubscribeAck, 20, cid, GroupMask{0b011})},
        {"ERROR", makeMessage(MsgType::Error, 19, cid, Error{ErrorCode::NotOwner})},
        {"DISCONNECT", makeMessage(MsgType::Disconnect, 0, cid, Disconnect{DisconnectReason::Idle})},
        {"LIST_FEED_RSP", makeMessage(MsgType::ListFeedRsp, 1, cid, core::MarketConfig::defaults().toListFeed())},
    };
}

size_t encodedSize(Protocol p, const Message& m, const codec::SymbolTable& symbols) {
    std::vector<uint8_t> out;
    if (p == Protocol::Binary) {
        codec::BinaryCodec::encode(m, out);
    } else {
        codec::TextCodec::encode(m, symbols, out);
    }
    return out.size();
}

// ------------------------------------------------------------ 1. sizes

void benchSizes() {
    title("1. Wire bytes per message (exact): custom binary protocol vs HTTP/1.1 + JSON");
    auto symbols = marketSymbols();
    std::printf("  %-22s %8s %10s %10s\n", "message", "custom", "http-json", "http/custom");
    for (const Sample& s : sampleMessages()) {
        const size_t b = encodedSize(Protocol::Binary, s.msg, *symbols);
        const size_t t = encodedSize(Protocol::Text, s.msg, *symbols);
        std::printf("  %-22s %8zu %10zu %9.1fx\n", s.name, b, t, static_cast<double>(t) / static_cast<double>(b));
    }
    std::printf("  %-22s %8zu %8s\n", "UDP snapshot", MARKET_DATA_SIZE, "-");
}

// ------------------------------------------------------------ 2. codec CPU

double medianOf(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

// Runs fn `iters` times per round, 5 rounds; returns the median ns per call.
template <typename Fn>
double timePerCall(int iters, Fn fn) {
    std::vector<double> rounds;
    for (int r = 0; r < 5; ++r) {
        const auto t0 = Clock::now();
        for (int i = 0; i < iters; ++i) fn();
        rounds.push_back(static_cast<double>(nsSince(t0)) / iters);
    }
    return medianOf(rounds);
}

struct CodecCost {
    double encode_ns;
    double decode_ns;
};

CodecCost measureCodec(Protocol p, const Message& m, const codec::SymbolTable& symbols, int iters,
                       uint64_t& sink) {
    std::vector<uint8_t> buf;
    buf.reserve(1024);
    const bool binary = p == Protocol::Binary;
    auto encode = [&] {
        buf.clear();
        if (binary) {
            codec::BinaryCodec::encode(m, buf);
        } else {
            codec::TextCodec::encode(m, symbols, buf);
        }
        sink += buf.size();
    };
    encode();
    const std::vector<uint8_t> bytes = buf;
    Message out;
    auto decode = [&] {
        size_t consumed = 0;
        if (binary) {
            codec::BinaryCodec::decode(bytes.data(), bytes.size(), out);
        } else {
            codec::TextCodec::decode(bytes.data(), bytes.size(), symbols, DEFAULT_MAX_FRAME_LEN, out, consumed);
        }
        sink += out.header.req_id;
    };
    return CodecCost{timePerCall(iters, encode), timePerCall(iters, decode)};
}

void benchCodec(const Options& o) {
    title("2. Codec CPU cost per message (ns, median of 5 rounds, no sockets)");
    auto symbols = marketSymbols();
    uint64_t sink = 0;
    std::printf("  %-22s %10s %10s %10s %10s %10s %10s\n", "message", "custom enc", "custom dec", "custom tot",
                "http enc", "http dec", "http tot");
    for (const Sample& s : sampleMessages()) {
        const std::string name = s.name;
        if (name != "BID" && name != "CANCEL" && name != "ORDER_ACK resting" && name != "ORDER_ACK 1 trade") continue;
        const CodecCost b = measureCodec(Protocol::Binary, s.msg, *symbols, o.iters, sink);
        const CodecCost t = measureCodec(Protocol::Text, s.msg, *symbols, o.iters, sink);
        std::printf("  %-22s %10.1f %10.1f %10.1f %10.1f %10.1f %10.1f   (http %.0fx)\n", s.name, b.encode_ns,
                    b.decode_ns, b.encode_ns + b.decode_ns, t.encode_ns, t.decode_ns, t.encode_ns + t.decode_ns,
                    (t.encode_ns + t.decode_ns) / (b.encode_ns + b.decode_ns));
    }
    std::printf("  (text decode includes the ticker -> symbol_id hash lookup; checksum %llu)\n",
                static_cast<unsigned long long>(sink % 10));
}

// ------------------------------------------------------------ server fixture

struct Target {
    std::unique_ptr<server::Exchange> local;
    std::string host;
    uint16_t port = 0;
    uint16_t text_port = 0;

    uint16_t portFor(Protocol p) const { return p == Protocol::Binary ? port : text_port; }
};

Target startTarget(const Options& o, bool record_dispatch) {
    Target t;
    if (!o.host.empty()) {
        t.host = o.host;
        t.port = o.port;
        t.text_port = o.text_port;
        return t;
    }
    server::ExchangeConfig cfg;
    cfg.dispatcher.port = 0;
    cfg.dispatcher.text_port = 0;
    cfg.dispatcher.idle_policy = net::IdlePolicy::Never;  // measure the codec, not reconnects
    cfg.dispatcher.record_dispatch_latency = record_dispatch;
    cfg.queue_capacity = 65536;
    t.local = std::make_unique<server::Exchange>(cfg);
    if (!t.local->start()) {
        std::fprintf(stderr, "cannot start in-process exchange\n");
        std::exit(1);
    }
    t.host = "127.0.0.1";
    t.port = t.local->port();
    t.text_port = t.local->textPort();
    return t;
}

uint16_t symbolFor(int client) { return static_cast<uint16_t>(client % 7 + 1); }

// ------------------------------------------------------------ 3 + 4. latency

struct RttRun {
    core::LatencyRecorder rtt;
    core::LatencyRecorder dispatch;
    uint64_t failures = 0;
    double seconds = 0;
};

// Closed loop: each client sends one order, waits for its ack, repeats.
// Orders alternate BID/ASK at one price, so every second one fills.
RttRun runRtt(const Options& o, Protocol p) {
    Target target = startTarget(o, true);
    std::vector<core::LatencyRecorder> per_client(static_cast<size_t>(o.clients));
    std::atomic<uint64_t> failures{0};
    std::vector<std::thread> threads;
    const auto t0 = Clock::now();
    for (int c = 0; c < o.clients; ++c) {
        threads.emplace_back([&, c] {
            client::ClientSession s({target.host, target.portFor(p), p});
            if (!s.listFeed()) {
                failures += static_cast<uint64_t>(o.requests);
                return;
            }
            core::LatencyRecorder& rec = per_client[static_cast<size_t>(c)];
            rec.setCapacity(static_cast<size_t>(o.requests));
            const uint16_t symbol = symbolFor(c);
            for (int i = 0; i < o.warmup + o.requests; ++i) {
                const auto start = Clock::now();
                auto rsp = (i % 2 == 0) ? s.bid(symbol, 100, 1) : s.ask(symbol, 100, 1);
                const uint64_t ns = nsSince(start);
                if (!rsp || rsp->header.type != MsgType::OrderAck) {
                    ++failures;
                } else if (i >= o.warmup) {
                    rec.add(ns);
                }
            }
        });
    }
    for (std::thread& t : threads) t.join();
    RttRun run;
    run.seconds = static_cast<double>(nsSince(t0)) / 1e9;
    for (const auto& rec : per_client) run.rtt.merge(rec);
    run.failures = failures.load();
    if (target.local) {
        target.local->stop();
        run.dispatch = target.local->dispatchLatency();
    }
    return run;
}

void benchLatency(const Options& o) {
    title("3 + 4. Dispatch latency and round-trip latency (microseconds)");
    std::printf("  %d clients x %d orders each (after %d warm-up), closed loop, %s\n", o.clients, o.requests,
                o.warmup, o.host.empty() ? "loopback, in-process server" : ("server " + o.host).c_str());
    RttRun runs[2];
    for (int i = 0; i < 2; ++i) runs[i] = runRtt(o, kProtocols[i]);

    std::printf("\n  Round trip: client sends BID/ASK -> receives ORDER_ACK\n");
    bench::printLatencyHeader("protocol");
    for (int i = 0; i < 2; ++i) bench::printLatencyRow(label(kProtocols[i]), runs[i].rtt.summarize());

    if (o.host.empty()) {
        std::printf("\n  Dispatch: end of socket read -> pushed on worker queue (SRS target: <= 50 us)\n");
        bench::printLatencyHeader("protocol");
        for (int i = 0; i < 2; ++i) {
            core::LatencySummary s = runs[i].dispatch.summarize();
            bench::printLatencyRow(label(kProtocols[i]), s);
        }
        for (int i = 0; i < 2; ++i) {
            const double p999 = runs[i].dispatch.summarize().p999_us;
            std::printf("  %s: p99.9 dispatch %.1f us -> %s the 50 us target\n", label(kProtocols[i]), p999,
                        p999 <= 50.0 ? "meets" : "MISSES");
        }
    }
    for (int i = 0; i < 2; ++i) {
        std::printf("  %s: %.0f orders/s closed-loop, %llu failed requests\n", label(kProtocols[i]),
                    static_cast<double>(runs[i].rtt.samples().size()) / runs[i].seconds,
                    static_cast<unsigned long long>(runs[i].failures));
    }
    for (int i = 0; i < 2; ++i) {
        bench::printHistogram(std::string("Round-trip histogram, ") + label(kProtocols[i]), runs[i].rtt);
    }
    if (!o.csv_dir.empty()) {
        for (int i = 0; i < 2; ++i) {
            const std::string p = label(kProtocols[i]);
            bench::writeCsv(o.csv_dir, "rtt_" + p, runs[i].rtt);
            bench::writeCsv(o.csv_dir, "dispatch_" + p, runs[i].dispatch);
        }
        std::printf("\n  samples written to %s/\n", o.csv_dir.c_str());
    }
}

// ------------------------------------------------------------ 5. throughput

void benchThroughput(const Options& o) {
    title("5. Peak throughput through one dispatcher");
    std::printf("  %d clients, %d requests in flight each, %.1f s per codec\n", o.clients, o.window, o.seconds);
    std::printf("  %-10s %14s %12s %12s %12s\n", "protocol", "replies/s", "replies", "errors", "updates");
    for (Protocol p : kProtocols) {
        Target target = startTarget(o, false);
        std::vector<std::unique_ptr<bench::LoadClient>> loaders;
        for (int c = 0; c < o.clients; ++c) {
            loaders.push_back(std::make_unique<bench::LoadClient>(target.host, target.portFor(p), p));
            if (!loaders.back()->connect()) {
                std::fprintf(stderr, "load client %d could not connect\n", c);
                return;
            }
        }
        std::vector<bench::LoadResult> results(static_cast<size_t>(o.clients));
        const auto start = Clock::now();
        const auto until = start + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(o.seconds));
        std::vector<std::thread> threads;
        for (int c = 0; c < o.clients; ++c) {
            threads.emplace_back([&, c] {
                results[static_cast<size_t>(c)] = loaders[static_cast<size_t>(c)]->run(symbolFor(c), o.window, until);
            });
        }
        for (std::thread& t : threads) t.join();
        const double elapsed = static_cast<double>(nsSince(start)) / 1e9;
        bench::LoadResult total;
        for (const auto& r : results) {
            total.responses += r.responses;
            total.errors += r.errors;
            total.updates += r.updates;
        }
        std::printf("  %-10s %14.0f %12llu %12llu %12llu\n", label(p),
                    static_cast<double>(total.responses) / elapsed,
                    static_cast<unsigned long long>(total.responses), static_cast<unsigned long long>(total.errors),
                    static_cast<unsigned long long>(total.updates));
    }
    std::printf("  (errors are QUEUE_FULL back-pressure; updates are fills pushed to the resting side)\n");
}

// ------------------------------------------------------------ 7. idle policy

struct IdleCase {
    const char* name;
    net::IdlePolicy policy;
    int idle_ms;
};

struct IdleResult {
    core::LatencyRecorder placement;
    core::LatencyRecorder delivery;
    double conns_per_minute = 0;
    uint64_t pushes = 0;
    uint64_t disconnects = 0;
};

std::unique_ptr<server::Exchange> idleExchange(const IdleCase& c) {
    server::ExchangeConfig cfg;
    cfg.dispatcher.port = 0;
    cfg.dispatcher.idle_policy = c.policy;
    cfg.dispatcher.idle_timeout = milliseconds(c.idle_ms);
    auto ex = std::make_unique<server::Exchange>(cfg);
    if (!ex->start()) std::exit(1);
    return ex;
}

// a) A client places an order after being quiet for gap_ms.
void measurePlacement(const Options& o, uint16_t port, IdleResult& r) {
    client::ClientSession s({"127.0.0.1", port});
    s.listFeed();
    for (int i = 0; i < o.samples; ++i) {
        std::this_thread::sleep_for(milliseconds(o.gap_ms));
        const auto t0 = Clock::now();
        s.bid(2, static_cast<Price>(10 + i), 1);  // MSFT, far below any ask: rests
        r.placement.add(nsSince(t0));
    }
}

// b) A resting order is filled after its owner has been quiet for gap_ms;
//    measures fill (seller's send) -> ORDER_UPDATE received by the owner.
void measureDelivery(const Options& o, uint16_t port, IdleResult& r) {
    client::ClientSession seller({"127.0.0.1", port});
    seller.listFeed();
    for (int i = 0; i < o.samples; ++i) {
        std::atomic<bool> resting{false};
        std::atomic<int64_t> arrived_ns{0};
        std::thread owner([&] {
            client::ClientSession s({"127.0.0.1", port});
            s.enablePush();
            s.onUpdate([&](const OrderAck&) {
                arrived_ns = nowNs();
            });
            s.bid(3, 500, 1);  // GOOG
            resting = true;
            const auto stop = Clock::now() + milliseconds(o.gap_ms + 3000);
            while (arrived_ns.load() == 0 && Clock::now() < stop) s.pump(1);
        });
        while (!resting.load()) std::this_thread::yield();
        std::this_thread::sleep_for(milliseconds(o.gap_ms));
        seller.listFeed();  // make sure the seller's own reconnect is not measured
        const int64_t t0 = nowNs();
        seller.ask(3, 500, 1);
        owner.join();
        if (arrived_ns.load() != 0) r.delivery.add(static_cast<uint64_t>(arrived_ns.load() - t0));
    }
}

// c) Three clients each act once per trickle period; count TCP connections opened.
void measureTrickle(const Options& o, server::Exchange& ex, uint16_t port, IdleResult& r) {
    std::vector<std::unique_ptr<client::ClientSession>> clients;
    for (int i = 0; i < 3; ++i) {
        clients.push_back(std::make_unique<client::ClientSession>(client::ClientConfig{"127.0.0.1", port}));
        clients.back()->listFeed();  // first connect is not counted
    }
    const uint64_t before = ex.stats().accepted.load() + ex.stats().push_connected.load();
    const auto start = Clock::now();
    const auto end = start + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(o.trickle_seconds));
    while (Clock::now() < end) {
        for (auto& c : clients) {
            c->listFeed();
            std::this_thread::sleep_for(milliseconds(o.trickle_period_ms / 3));
        }
    }
    const double minutes = static_cast<double>(nsSince(start)) / 60e9;
    const uint64_t opened = ex.stats().accepted.load() + ex.stats().push_connected.load() - before;
    r.conns_per_minute = static_cast<double>(opened) / minutes;
}

void benchIdle(const Options& o) {
    title("7. Idle policy comparison");
    std::printf("  gap before each action %d ms, %d samples per measurement, trickle: 3 clients x 1 request per %d ms\n",
                o.gap_ms, o.samples, o.trickle_period_ms);
    const IdleCase cases[] = {
        {"never", net::IdlePolicy::Never, 1000},
        {"fixed 100 ms", net::IdlePolicy::Fixed, 100},
        {"fixed 1 s", net::IdlePolicy::Fixed, 1000},
        {"fixed 5 s", net::IdlePolicy::Fixed, 5000},
    };
    std::vector<IdleResult> results;
    for (const IdleCase& c : cases) {
        std::fprintf(stderr, "  running idle policy: %s ...\n", c.name);
        auto ex = idleExchange(c);
        IdleResult r;
        measurePlacement(o, ex->port(), r);
        measureDelivery(o, ex->port(), r);
        measureTrickle(o, *ex, ex->port(), r);
        r.pushes = ex->stats().push_connected.load();
        r.disconnects = ex->stats().disconnects_sent.load();
        ex->stop();
        results.push_back(std::move(r));
    }

    std::printf("\n  %-14s %22s %22s %12s %8s %12s\n", "policy", "place after gap (us)", "fill -> update (us)",
                "conns/min", "pushes", "DISCONNECTs");
    std::printf("  %-14s %10s %11s %10s %11s\n", "", "p50", "max", "p50", "max");
    for (size_t i = 0; i < results.size(); ++i) {
        const auto pl = results[i].placement.summarize();
        const auto dl = results[i].delivery.summarize();
        std::printf("  %-14s %10.1f %11.1f %10.1f %11.1f %12.1f %8llu %12llu\n", cases[i].name, pl.p50_us, pl.max_us,
                    dl.p50_us, dl.max_us, results[i].conns_per_minute,
                    static_cast<unsigned long long>(results[i].pushes),
                    static_cast<unsigned long long>(results[i].disconnects));
    }
    std::printf("  (place after gap includes a TCP reconnect whenever the gap exceeded the idle timeout;\n"
                "   fill -> update goes through a server push connect in the same case)\n");
}

// ------------------------------------------------------------ main

bool parseArgs(int argc, char** argv, Options& o) {
    int i = 1;
    if (i < argc && argv[i][0] != '-') o.command = argv[i++];
    for (; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--quick") {
            o.requests = 5000;
            o.warmup = 500;
            o.seconds = 1.5;
            o.iters = 50000;
            o.gap_ms = 1200;
            o.samples = 3;
            o.trickle_seconds = 4;
            continue;
        }
        if (i + 1 >= argc) return false;
        const char* v = argv[++i];
        if (a == "--host") o.host = v;
        else if (a == "--port") o.port = static_cast<uint16_t>(std::atoi(v));
        else if (a == "--text-port") o.text_port = static_cast<uint16_t>(std::atoi(v));
        else if (a == "--clients") o.clients = std::atoi(v);
        else if (a == "--requests") o.requests = std::atoi(v);
        else if (a == "--warmup") o.warmup = std::atoi(v);
        else if (a == "--window") o.window = std::atoi(v);
        else if (a == "--seconds") o.seconds = std::atof(v);
        else if (a == "--iters") o.iters = std::atoi(v);
        else if (a == "--gap-ms") o.gap_ms = std::atoi(v);
        else if (a == "--samples") o.samples = std::atoi(v);
        else if (a == "--csv") o.csv_dir = v;
        else return false;
    }
    return o.clients > 0 && o.requests > 0 && o.window > 0 && o.iters > 0 && o.samples > 0;
}

void usage() {
    std::puts(
        "usage: benchmark [sizes|codec|rtt|throughput|idle|all] [options]\n"
        "  --quick                 shorter runs\n"
        "  --clients N --requests N --warmup N     rtt (default 4 x 20000, warm-up 2000)\n"
        "  --window N --seconds S                  throughput (default 32 in flight, 3 s)\n"
        "  --iters N                               codec (default 200000)\n"
        "  --gap-ms N --samples N                  idle (default 1500 ms, 5)\n"
        "  --host H --port P --text-port Q         rtt/throughput against a running server\n"
        "  --csv DIR                               write latency samples as CSV");
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    if (!parseArgs(argc, argv, o)) {
        usage();
        return 2;
    }
    net::ignoreSigpipe();
#ifndef NDEBUG
    std::puts("WARNING: debug build. Use: cmake --preset release && cmake --build --preset release");
#endif
    std::printf("benchmark '%s' on %u hardware threads\n", o.command.c_str(), std::thread::hardware_concurrency());

    const std::string& c = o.command;
    const bool all = c == "all";
    bool ran = false;
    if (all || c == "sizes") benchSizes(), ran = true;
    if (all || c == "codec") benchCodec(o), ran = true;
    if (all || c == "rtt") benchLatency(o), ran = true;
    if (all || c == "throughput") benchThroughput(o), ran = true;
    if (all || c == "idle") benchIdle(o), ran = true;
    if (!ran) {
        usage();
        return 2;
    }
    return 0;
}
