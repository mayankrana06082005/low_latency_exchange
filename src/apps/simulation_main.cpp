// simulation: starts the exchange in-process on loopback and runs scripted
// scenarios that exercise every behaviour in the plan. Every frame between the
// clients and the dispatcher is traced, order states are tabulated after each
// trading step, and each expectation prints PASS/FAIL.
//
// usage: simulation [--quiet | --client-only]
//   --quiet        only checks and the summary
//   --client-only  client-side trace only, without the server-side lines

#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "client/ClientSession.hpp"
#include "client/RawConnection.hpp"
#include "client/SessionTracer.hpp"
#include "client/UdpListener.hpp"
#include "codec/BinaryCodec.hpp"
#include "codec/TextCodec.hpp"
#include "core/MarketConfig.hpp"
#include "core/ServerTrace.hpp"
#include "server/Exchange.hpp"

using namespace wire;
using client::ClientSession;
using client::SessionTracer;
using Clock = std::chrono::steady_clock;

namespace {

constexpr uint16_t AAPL = 1;
constexpr uint16_t MSFT = 2;
constexpr uint16_t GOOG = 3;
constexpr int kIdleMs = 300;

int g_pass = 0;
int g_fail = 0;

void section(const char* title) { std::printf("\n=== %s ===\n", title); }

void check(bool ok, const std::string& what) {
    std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what.c_str());
    (ok ? g_pass : g_fail)++;
}

std::string ull(uint64_t v) { return std::to_string(static_cast<unsigned long long>(v)); }

bool isType(const std::optional<Message>& m, MsgType t) { return m && m->header.type == t; }

bool isError(const std::optional<Message>& m, ErrorCode code) {
    return isType(m, MsgType::Error) && std::get<Error>(m->payload).code == code;
}

const OrderAck* ackOf(const std::optional<Message>& m) {
    return m ? std::get_if<OrderAck>(&m->payload) : nullptr;
}

uint32_t maskOf(const std::optional<Message>& m) {
    const auto* g = m ? std::get_if<GroupMask>(&m->payload) : nullptr;
    return g ? g->group_mask : 0xFFFFFFFF;
}

// Records updates delivered to one client.
struct UpdateLog {
    std::vector<OrderAck> updates;
    void attach(ClientSession& s) {
        s.onUpdate([this](const OrderAck& u) { updates.push_back(u); });
    }
};

// Keeps sessions and UDP listeners serviced for `ms`, single-threaded.
void spin(int ms, std::vector<ClientSession*> sessions, std::vector<client::UdpListener*> feeds = {}) {
    const auto end = Clock::now() + std::chrono::milliseconds(ms);
    while (Clock::now() < end) {
        for (ClientSession* s : sessions) s->pump(2);
        for (client::UdpListener* f : feeds) f->poll(2);
    }
}

std::string levels(const std::array<Level, BOOK_LEVELS>& side, uint8_t n) {
    if (n == 0) return "-";
    std::string s;
    for (uint8_t i = 0; i < n; ++i) s += (i ? " " : "") + ull(side[i].vol) + "@" + ull(side[i].price);
    return s;
}

// A raw connection whose frames are traced like a session's.
struct TracedRaw {
    SessionTracer& tracer;
    std::string name;
    client::RawConnection conn;

    void send(const Message& m, const std::string& remark = "") {
        tracer.frame(name, true, m, remark);
        conn.send(m);
    }
    void sendMutated(const Message& m, size_t offset, uint8_t value, const std::string& remark) {
        std::vector<uint8_t> bytes = client::encodeFrame(m);
        bytes[offset] = value;
        tracer.frame(name, true, m, remark);
        conn.sendBytes(bytes);
    }
    std::optional<Message> read(int timeout_ms) {
        auto m = conn.read(timeout_ms);
        if (m) tracer.frame(name, false, *m);
        if (conn.closed()) tracer.note(name, "-x- dispatcher closed the connection");
        return m;
    }
};

}  // namespace

int main(int argc, char** argv) {
    const bool quiet = argc > 1 && std::strcmp(argv[1], "--quiet") == 0;
    const bool client_only = argc > 1 && std::strcmp(argv[1], "--client-only") == 0;

    server::ExchangeConfig cfg;
    cfg.dispatcher.port = 0;
    cfg.dispatcher.text_port = 0;
    cfg.dispatcher.idle_timeout = std::chrono::milliseconds(kIdleMs);
    cfg.dispatcher.drain_grace = std::chrono::milliseconds(50);
    cfg.dispatcher.push_timeout = std::chrono::milliseconds(500);
    cfg.snapshot_interval = std::chrono::milliseconds(20);

    std::map<uint16_t, std::string> names;
    for (const auto& g : cfg.market.groups) {
        for (const auto& s : g.symbols) names[s.symbol_id] = s.ticker;
    }
    SessionTracer trace(
        [&names](uint16_t id) {
            auto it = names.find(id);
            return it != names.end() ? it->second : "sym" + std::to_string(id);
        },
        !quiet);

    core::ServerTrace server_trace(trace.start());
    if (!quiet && !client_only) cfg.trace = &server_trace;
    trace.onClientId([&server_trace](const std::string& name, uint64_t id) { server_trace.nameClient(id, name); });

    server::Exchange exchange(cfg);
    if (!exchange.start()) {
        std::puts("failed to start exchange");
        return 1;
    }
    const client::ClientConfig at{"127.0.0.1", exchange.port()};
    std::printf("exchange on 127.0.0.1: binary port %u, text port %u  (idle %d ms, grace 50 ms, "
                "snapshots every 20 ms)\n", exchange.port(), exchange.textPort(), kIdleMs);
    if (!quiet) {
        std::puts("trace legend:  -->  client sends to dispatcher    <--  dispatcher sends to client");
        std::puts("               <==  dispatcher opened a push connection    -x-  connection closed");
        if (cfg.trace != nullptr) {
            std::puts("               | dispatcher | ... and | worker N | ...  server-side events (inside the exchange);");
            std::puts("               the server shows \"cid ..xxxxxx\" for a client until that client has learned its id");
        }
    }

    ClientSession alice(at), bob(at), carol(at);
    trace.attach(alice, "alice");
    trace.attach(bob, "bob");
    trace.attach(carol, "carol");
    UpdateLog alice_log, carol_log;
    alice_log.attach(alice);
    carol_log.attach(carol);
    alice.enablePush();  // carol deliberately has no callback port
    trace.note("alice", "listening for push connections on callback port " +
                            std::to_string(alice.callbackPort()));

    // ------------------------------------------------------------------
    section("1. Identity: first frame with client_id 0 gets an id");
    auto feed = alice.listFeed();
    check(isType(feed, MsgType::ListFeedRsp), "LIST_FEED answered with LIST_FEED_RSP");
    check(alice.clientId() != 0, "alice was assigned client_id " + ull(alice.clientId()));
    if (feed) {
        for (const GroupInfo& g : std::get<ListFeedRsp>(feed->payload).groups) {
            std::string line = "group " + std::to_string(g.group_id) + " " + g.name + ":";
            for (const TickerInfo& t : g.tickers) {
                line += " " + core::tickerName(t.ticker) + "(" + std::to_string(t.symbol_id) + ")";
            }
            trace.note("alice", line);
        }
    }

    // ------------------------------------------------------------------
    section("2. UDP market data: subscribe, receive, idempotent unsubscribe");
    client::UdpListener md;
    md.open();
    std::set<uint32_t> groups_seen;
    md.onSnapshot([&groups_seen](const MarketData& m) { groups_seen.insert(m.group_id); });
    auto sub = alice.subscribe(md.port(), 0b011);
    check(maskOf(sub) == 0b011, "SUBSCRIBE groups {0,1} -> SUBSCRIBE_ACK groups {0,1}");
    spin(200, {&alice}, {&md});
    trace.note("alice", "UDP: " + ull(md.received()) + " snapshots received in 200 ms on port " +
                            std::to_string(md.port()));
    check(md.received() > 0, "snapshots arrive over UDP");
    check(groups_seen == std::set<uint32_t>{0, 1}, "only groups 0 and 1 arrive (not group 2)");
    check(md.gaps() == 0, "no sequence gaps on loopback");
    check(maskOf(alice.unsubscribe(0b010)) == 0b010, "UNSUBSCRIBE group 1 -> removed {1}");
    check(maskOf(alice.unsubscribe(0b010)) == 0, "repeat UNSUBSCRIBE group 1 -> removed {} (idempotent)");

    // ------------------------------------------------------------------
    section("3. Orders: resting bid, crossing ask, ORDER_UPDATE to the resting side");
    auto a_bid = alice.bid(AAPL, 100, 10);
    const OrderAck* a_ack = ackOf(a_bid);
    const uint64_t alice_order = a_ack ? a_ack->order_id : 0;
    check(a_ack && a_ack->status == OrderStatus::Resting && a_ack->remaining_vol == 10,
          "alice BID AAPL 10 @ 100 -> RESTING as order " + ull(alice_order) + " (worker " +
              ull(alice_order >> 56) + ")");
    auto b_ask = bob.ask(AAPL, 100, 4);
    const OrderAck* b_ack = ackOf(b_ask);
    check(b_ack && b_ack->status == OrderStatus::Filled && b_ack->trades.size() == 1,
          "bob ASK AAPL 4 @ 100 -> FILLED immediately with 1 trade");
    spin(100, {&alice});
    check(!alice_log.updates.empty() && alice_log.updates.back().order_id == alice_order &&
              alice_log.updates.back().status == OrderStatus::PartialFill &&
              alice_log.updates.back().remaining_vol == 6,
          "alice got ORDER_UPDATE: PARTIAL_FILL, remaining 6");
    MarketData last_aapl;
    md.onSnapshot([&last_aapl](const MarketData& m) {
        if (core::tickerName(m.ticker) == "AAPL") last_aapl = m;
    });
    spin(100, {&alice}, {&md});
    trace.note("alice", "UDP snapshot AAPL seq " + ull(last_aapl.seq) + "  bids " +
                            levels(last_aapl.bids, last_aapl.n_bids) + "  asks " +
                            levels(last_aapl.asks, last_aapl.n_asks));
    check(last_aapl.n_bids == 1 && last_aapl.bids[0].price == 100 && last_aapl.bids[0].vol == 6,
          "UDP snapshot for AAPL shows best bid 6 @ 100");
    trace.printLedger("after scenario 3");

    // ------------------------------------------------------------------
    section("4. Ownership and unknown orders");
    check(isError(bob.cancel(alice_order), ErrorCode::NotOwner), "bob CANCEL alice's order -> NOT_OWNER");
    check(isError(bob.cancel(12345), ErrorCode::OrderNotFound), "CANCEL unknown order -> ORDER_NOT_FOUND");
    check(isError(bob.bid(999, 1, 1), ErrorCode::UnknownSymbol), "BID unknown symbol -> UNKNOWN_SYMBOL");
    check(isError(bob.bid(AAPL, 0, 5), ErrorCode::InvalidField), "BID price 0 -> INVALID_FIELD");

    // ------------------------------------------------------------------
    section("5. Idle timer: DISCONNECT, then fill delivered by server push");
    const uint64_t alice_connects = alice.stats().connects;
    trace.note("alice", "goes quiet for " + std::to_string(kIdleMs + 200) + " ms");
    spin(kIdleMs + 200, {&alice}, {&md});
    check(alice.stats().disconnects_received >= 1,
          "alice idle > " + std::to_string(kIdleMs) + " ms -> received DISCONNECT");
    const size_t before = alice_log.updates.size();
    auto b_ask2 = bob.ask(AAPL, 100, 2);
    check(ackOf(b_ask2) && ackOf(b_ask2)->status == OrderStatus::Filled, "bob ASK AAPL 2 @ 100 -> FILLED");
    spin(300, {&alice});
    check(alice.stats().pushes_accepted >= 1, "dispatcher connected to alice's callback port");
    check(alice_log.updates.size() > before && alice_log.updates.back().remaining_vol == 4,
          "alice got the fill over the push connection: remaining 4");
    check(alice.stats().connects == alice_connects, "alice did not reconnect by herself to get it");
    trace.printLedger("after scenario 5");

    // ------------------------------------------------------------------
    section("6. Mailbox coalescing: client with no callback port");
    auto c_bid = carol.bid(MSFT, 50, 9);
    const uint64_t carol_order = ackOf(c_bid) ? ackOf(c_bid)->order_id : 0;
    check(ackOf(c_bid) && ackOf(c_bid)->status == OrderStatus::Resting, "carol BID MSFT 9 @ 50 -> RESTING");
    trace.note("carol", "goes quiet for " + std::to_string(kIdleMs + 200) + " ms (no callback port)");
    spin(kIdleMs + 200, {&carol});
    check(carol.stats().disconnects_received >= 1, "carol idled out (DISCONNECT)");
    for (int i = 0; i < 3; ++i) bob.ask(MSFT, 50, 3);
    trace.note("carol", "is offline: her 3 fills are merged in her mailbox at the dispatcher");
    spin(20, {&bob});
    carol_log.updates.clear();
    auto c_feed = carol.listFeed();  // any request reconnects with the saved client_id
    const bool one = carol_log.updates.size() == 1;
    check(isType(c_feed, MsgType::ListFeedRsp), "carol reconnected lazily with her saved id");
    check(one, "mailbox delivered exactly one ORDER_UPDATE for the order");
    if (one) {
        const OrderAck& u = carol_log.updates[0];
        check(u.order_id == carol_order && u.status == OrderStatus::Filled && u.remaining_vol == 0 &&
                  u.trades.size() == 3,
              "coalesced update: FILLED, remaining 0, all 3 trades kept");
    }
    trace.printLedger("after scenario 6");

    // ------------------------------------------------------------------
    section("7. Lazy reconnect keeps identity; cancel the rest");
    const uint64_t id_before = alice.clientId();
    trace.note("alice", "goes quiet for " + std::to_string(kIdleMs + 200) + " ms");
    spin(kIdleMs + 200, {&alice});
    check(isType(alice.cancel(alice_order), MsgType::CancelAck), "alice CANCEL her resting order -> CANCEL_ACK");
    check(alice.clientId() == id_before, "same client_id after reconnecting");
    check(isError(alice.cancel(alice_order), ErrorCode::OrderNotFound),
          "second CANCEL of the same order -> ORDER_NOT_FOUND");
    trace.printLedger("after scenario 7");

    // ------------------------------------------------------------------
    section("8. Robustness: bad input only affects its own connection");
    {
        TracedRaw raw{trace, "raw1", {}};
        raw.conn.connect("127.0.0.1", exchange.port());
        raw.send(makeMessage(MsgType::ListFeed, 1, 0, Empty{}), "pipelined, id 0");
        raw.send(makeMessage(MsgType::ListFeed, 2, 0, Empty{}), "pipelined, id 0");
        auto r1 = raw.read(1000);
        auto r2 = raw.read(1000);
        check(r1 && r2 && r1->header.client_id != 0 && r1->header.client_id == r2->header.client_id,
              "two pipelined id-0 frames on one connection share one client_id");

        raw.sendMutated(makeMessage(MsgType::ListFeed, 3, 0, Empty{}), 18, 0x7E, "type byte forced to 0x7E");
        check(isError(raw.read(1000), ErrorCode::UnknownType), "unknown type -> ERROR UNKNOWN_TYPE");
        raw.send(makeMessage(MsgType::ListFeed, 4, 0, Empty{}));
        check(isType(raw.read(1000), MsgType::ListFeedRsp), "connection still usable afterwards");

        raw.send(makeMessage(MsgType::ListFeed, 5, 0xDEADBEEF, Empty{}), "someone else's id");
        check(isError(raw.read(1000), ErrorCode::ClientMismatch),
              "different id on a bound connection -> CLIENT_MISMATCH");
        raw.read(300);
    }
    {
        TracedRaw raw{trace, "raw2", {}};
        raw.conn.connect("127.0.0.1", exchange.port());
        raw.send(makeMessage(MsgType::ListFeed, 1, 0xDEADBEEF, Empty{}), "id never assigned");
        check(isError(raw.read(1000), ErrorCode::UnknownClient), "unknown nonzero client_id -> UNKNOWN_CLIENT");
    }
    {
        TracedRaw raw{trace, "raw3", {}};
        raw.conn.connect("127.0.0.1", exchange.port());
        raw.sendMutated(makeMessage(MsgType::ListFeed, 1, 0, Empty{}), 19, 9, "version byte forced to 9");
        auto r = raw.read(1000);
        raw.read(500);
        check(isError(r, ErrorCode::UnsupportedVersion) && raw.conn.closed(),
              "bad version -> ERROR UNSUPPORTED_VERSION, then the server closes");
    }
    {
        TracedRaw raw{trace, "raw4", {}};
        raw.conn.connect("127.0.0.1", exchange.port());
        raw.sendMutated(makeMessage(MsgType::ListFeed, 1, 0, Empty{}), 0, 0x7F, "length forced to ~2 GB");
        raw.read(500);
        check(raw.conn.closed(), "oversize length field -> connection closed, no allocation");
    }
    check(isType(bob.listFeed(), MsgType::ListFeedRsp), "other clients unaffected");

    // ------------------------------------------------------------------
    section("9. UDP loss detection");
    {
        ClientSession dave(at);
        trace.attach(dave, "dave");
        client::UdpListener lossy;
        lossy.open();
        lossy.dropEvery(4);
        trace.note("dave", "UDP listener discards every 4th datagram to simulate loss");
        dave.subscribe(lossy.port(), 0b001);
        spin(200, {&dave}, {&lossy});
        trace.note("dave", "UDP: received " + ull(lossy.received()) + ", gaps " + ull(lossy.gaps()) +
                               ", missing " + ull(lossy.missing()));
        check(lossy.gaps() > 0, "sequence numbers reveal the lost datagrams");
    }

    // ------------------------------------------------------------------
    section("10. Endpoint ownership: newest claimant wins");
    {
        ClientSession first(at), second(at);
        trace.attach(first, "erin");
        trace.attach(second, "frank");
        client::UdpListener shared;
        shared.open();
        first.subscribe(shared.port(), 0b100);
        second.subscribe(shared.port(), 0b100);
        check(maskOf(first.unsubscribe(0b100)) == 0,
              "erin's subscription was dropped when frank claimed the same endpoint");
        check(maskOf(second.unsubscribe(0b100)) == 0b100, "frank holds the subscription");
    }

    // ------------------------------------------------------------------
    section("11. Text baseline: the same protocol as HTTP/1.1 + JSON");
    {
        const Message sample = makeMessage(MsgType::Bid, 1, 8841120934410029ULL, OrderRequest{GOOG, 250, 7});
        std::vector<uint8_t> bin;
        codec::BinaryCodec::encode(sample, bin);
        codec::SymbolTable symbols;
        symbols.add(GOOG, "GOOG");
        std::vector<uint8_t> txt;
        codec::TextCodec::encode(sample, symbols, txt);
        if (!quiet) {
            std::string hex;
            char byte[4];
            for (uint8_t b : bin) {
                std::snprintf(byte, sizeof byte, "%02x ", b);
                hex += byte;
            }
            std::printf("  the same BID on the wire:\n  binary (%zu bytes): %s\n  text (%zu bytes):\n", bin.size(),
                        hex.c_str(), txt.size());
            std::string t(txt.begin(), txt.end());
            size_t pos = 0;
            while ((pos = t.find("\r\n", pos)) != std::string::npos) t.replace(pos, 2, "\\r\\n\n    ");
            std::printf("    %s\n\n", t.c_str());
        }

        const client::ClientConfig text_at{"127.0.0.1", exchange.textPort(), net::Protocol::Text};
        ClientSession tina(text_at), tom(text_at);
        trace.attach(tina, "tina");
        trace.attach(tom, "tom");
        tina.enablePush();
        UpdateLog tina_log;
        tina_log.attach(tina);
        check(isType(tina.listFeed(), MsgType::ListFeedRsp) && isType(tom.listFeed(), MsgType::ListFeedRsp),
              "text clients get LIST_FEED_RSP and learn the ticker table");
        auto t_bid = tina.bid(GOOG, 250, 7);
        const uint64_t tina_order = ackOf(t_bid) ? ackOf(t_bid)->order_id : 0;
        check(ackOf(t_bid) && ackOf(t_bid)->status == OrderStatus::Resting, "tina BID GOOG 7 @ 250 -> RESTING");
        check(isError(tom.cancel(tina_order), ErrorCode::NotOwner), "tom CANCEL tina's order -> 403 NOT_OWNER");
        check(ackOf(tom.ask(GOOG, 250, 3)) != nullptr, "tom ASK GOOG 3 @ 250 -> ORDER_ACK");
        spin(100, {&tina});
        check(!tina_log.updates.empty() && tina_log.updates.back().remaining_vol == 4,
              "tina got ORDER_UPDATE (POST /order/update): remaining 4");
        trace.note("tina", "goes quiet for " + std::to_string(kIdleMs + 200) + " ms");
        spin(kIdleMs + 200, {&tina});
        tom.ask(GOOG, 250, 4);
        spin(300, {&tina});
        check(tina.stats().pushes_accepted >= 1 && tina_log.updates.back().status == OrderStatus::Filled,
              "after idling out, the final fill was pushed to tina over HTTP");
        bob.bid(GOOG, 260, 1);
        auto cross = tom.ask(GOOG, 260, 1);
        check(ackOf(cross) && ackOf(cross)->status == OrderStatus::Filled,
              "binary client bob and text client tom trade against the same book");
    }

    exchange.stop();
    trace.printLedger("final");

    const net::DispatcherStats& s = exchange.stats();
    std::printf("=== server stats ===\n");
    std::printf("  connections accepted %llu, frames in %llu, frames out %llu\n",
                (unsigned long long)s.accepted.load(), (unsigned long long)s.frames_in.load(),
                (unsigned long long)s.frames_out.load());
    std::printf("  DISCONNECT sent %llu, push attempts %llu (ok %llu, failed %llu), protocol closes %llu\n",
                (unsigned long long)s.disconnects_sent.load(), (unsigned long long)s.push_attempts.load(),
                (unsigned long long)s.push_connected.load(), (unsigned long long)s.push_failed.load(),
                (unsigned long long)s.protocol_closes.load());
    std::printf("  UDP datagrams sent %llu, dropped %llu\n", (unsigned long long)exchange.datagramsSent(),
                (unsigned long long)exchange.datagramsDropped());
    std::printf("\nRESULT: %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
