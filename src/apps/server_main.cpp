// exchange_server: runs the dispatcher and workers until Ctrl-C.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "server/Exchange.hpp"

namespace {

std::atomic<bool> g_stop{false};

void onSignal(int) { g_stop.store(true); }

void usage() {
    std::puts(
        "usage: exchange_server [--port N] [--text-port N|off] [--idle fixed|never] [--idle-ms N]\n"
        "                       [--grace-ms N] [--push-timeout-ms N] [--snapshot-ms N]\n"
        "                       [--snapshot-policy fixed|ewma] [--record-latency on] [--trace on]");
}

bool g_trace = false;

bool parseArgs(int argc, char** argv, server::ExchangeConfig& cfg) {
    using std::chrono::milliseconds;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (i + 1 >= argc) return false;
        const char* value = argv[++i];
        if (arg == "--port") {
            cfg.dispatcher.port = static_cast<uint16_t>(std::atoi(value));
        } else if (arg == "--text-port") {
            cfg.dispatcher.text_port = std::strcmp(value, "off") == 0 ? -1 : std::atoi(value);
        } else if (arg == "--snapshot-policy") {
            if (std::strcmp(value, "ewma") == 0) {
                cfg.snapshot_policy = core::SnapshotPolicy::Ewma;
            } else if (std::strcmp(value, "fixed") == 0) {
                cfg.snapshot_policy = core::SnapshotPolicy::Fixed;
            } else {
                return false;
            }
        } else if (arg == "--trace") {
            g_trace = std::strcmp(value, "on") == 0;
        } else if (arg == "--record-latency") {
            cfg.dispatcher.record_dispatch_latency = std::strcmp(value, "on") == 0;
        } else if (arg == "--idle") {
            if (std::strcmp(value, "never") == 0) {
                cfg.dispatcher.idle_policy = net::IdlePolicy::Never;
            } else if (std::strcmp(value, "fixed") == 0) {
                cfg.dispatcher.idle_policy = net::IdlePolicy::Fixed;
            } else {
                return false;
            }
        } else if (arg == "--idle-ms") {
            cfg.dispatcher.idle_timeout = milliseconds(std::atoi(value));
        } else if (arg == "--grace-ms") {
            cfg.dispatcher.drain_grace = milliseconds(std::atoi(value));
        } else if (arg == "--push-timeout-ms") {
            cfg.dispatcher.push_timeout = milliseconds(std::atoi(value));
        } else if (arg == "--snapshot-ms") {
            cfg.snapshot_interval = milliseconds(std::atoi(value));
        } else {
            return false;
        }
    }
    return true;
}

void printStats(const server::Exchange& ex) {
    const net::DispatcherStats& s = ex.stats();
    std::printf(
        "connections accepted %llu | frames in %llu out %llu | DISCONNECT sent %llu | "
        "push attempts %llu ok %llu failed %llu | protocol closes %llu | UDP sent %llu "
        "dropped %llu\n",
        (unsigned long long)s.accepted.load(), (unsigned long long)s.frames_in.load(),
        (unsigned long long)s.frames_out.load(), (unsigned long long)s.disconnects_sent.load(),
        (unsigned long long)s.push_attempts.load(), (unsigned long long)s.push_connected.load(),
        (unsigned long long)s.push_failed.load(), (unsigned long long)s.protocol_closes.load(),
        (unsigned long long)ex.datagramsSent(), (unsigned long long)ex.datagramsDropped());
}

}  // namespace

int main(int argc, char** argv) {
    server::ExchangeConfig cfg;
    cfg.dispatcher.text_port = 9001;
    if (!parseArgs(argc, argv, cfg)) {
        usage();
        return 2;
    }
    core::ServerTrace trace;
    if (g_trace) {
        setvbuf(stdout, nullptr, _IOLBF, 0);  // show each event line immediately
        cfg.trace = &trace;
    }
    server::Exchange exchange(cfg);
    if (!exchange.start()) {
        std::fprintf(stderr, "failed to listen on port %u\n", cfg.dispatcher.port);
        return 1;
    }
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    std::printf("exchange: binary protocol on TCP port %u", exchange.port());
    if (cfg.dispatcher.text_port >= 0) std::printf(", HTTP/1.1+JSON on TCP port %u", exchange.textPort());
    std::printf("\n  %zu workers, idle %s %lld ms, snapshots %s %lld ms\n", cfg.market.groups.size(),
                cfg.dispatcher.idle_policy == net::IdlePolicy::Fixed ? "fixed" : "never",
                (long long)cfg.dispatcher.idle_timeout.count(),
                cfg.snapshot_policy == core::SnapshotPolicy::Ewma ? "EWMA, dt_safety" : "every",
                (long long)cfg.snapshot_interval.count());
    for (const auto& g : cfg.market.groups) {
        std::printf("  group %u %-8s:", g.group_id, g.name.c_str());
        for (const auto& s : g.symbols) std::printf(" %s(%u)", s.ticker.c_str(), s.symbol_id);
        std::printf("\n");
    }
    std::fflush(stdout);

    auto next_report = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (std::chrono::steady_clock::now() >= next_report) {
            printStats(exchange);
            std::fflush(stdout);
            next_report += std::chrono::seconds(10);
        }
    }
    exchange.stop();
    printStats(exchange);
    if (cfg.dispatcher.record_dispatch_latency) {
        const core::LatencySummary s = exchange.dispatchLatency().summarize();
        std::printf("dispatch latency (us): count %zu p50 %.1f p99 %.1f p99.9 %.1f max %.1f\n", s.count,
                    s.p50_us, s.p99_us, s.p999_us, s.max_us);
    }
    return 0;
}
