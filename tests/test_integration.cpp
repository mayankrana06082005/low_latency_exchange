// End-to-end tests: real Exchange on loopback, real sockets (Phase 6-8 exit criteria).

#include <doctest/doctest.h>

#include <chrono>
#include <vector>

#include "client/ClientSession.hpp"
#include "client/RawConnection.hpp"
#include "client/UdpListener.hpp"
#include "server/Exchange.hpp"

using namespace wire;
using client::ClientSession;
using Clock = std::chrono::steady_clock;

namespace {

constexpr uint16_t AAPL = 1;
constexpr int kIdleMs = 150;

server::ExchangeConfig testConfig(net::IdlePolicy policy = net::IdlePolicy::Fixed) {
    server::ExchangeConfig cfg;
    cfg.dispatcher.port = 0;
    cfg.dispatcher.idle_policy = policy;
    cfg.dispatcher.idle_timeout = std::chrono::milliseconds(kIdleMs);
    cfg.dispatcher.drain_grace = std::chrono::milliseconds(30);
    cfg.dispatcher.push_timeout = std::chrono::milliseconds(300);
    cfg.snapshot_interval = std::chrono::milliseconds(10);
    return cfg;
}

void spin(int ms, std::vector<ClientSession*> sessions, std::vector<client::UdpListener*> feeds = {}) {
    const auto end = Clock::now() + std::chrono::milliseconds(ms);
    while (Clock::now() < end) {
        for (ClientSession* s : sessions) s->pump(2);
        for (client::UdpListener* f : feeds) f->poll(2);
    }
}

const OrderAck* ackOf(const std::optional<Message>& m) {
    return m ? std::get_if<OrderAck>(&m->payload) : nullptr;
}

bool isError(const std::optional<Message>& m, ErrorCode code) {
    return m && m->header.type == MsgType::Error && std::get<Error>(m->payload).code == code;
}

}  // namespace

TEST_CASE("BID with client_id 0 gets an ORDER_ACK carrying a new id") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    ClientSession alice({"127.0.0.1", ex.port()});
    auto rsp = alice.bid(AAPL, 100, 5);
    REQUIRE(ackOf(rsp) != nullptr);
    CHECK(rsp->header.client_id != 0);
    CHECK(alice.clientId() == rsp->header.client_id);
    CHECK(ackOf(rsp)->status == OrderStatus::Resting);
}

TEST_CASE("pipelined id-0 frames on one connection share one id") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    client::RawConnection raw;
    REQUIRE(raw.connect("127.0.0.1", ex.port()));
    std::vector<uint8_t> bytes = client::encodeFrame(makeMessage(MsgType::ListFeed, 1, 0, Empty{}));
    std::vector<uint8_t> second = client::encodeFrame(makeMessage(MsgType::ListFeed, 2, 0, Empty{}));
    bytes.insert(bytes.end(), second.begin(), second.end());
    raw.sendBytes(bytes);
    auto r1 = raw.read(1000);
    auto r2 = raw.read(1000);
    REQUIRE(r1);
    REQUIRE(r2);
    CHECK(r1->header.req_id == 1);
    CHECK(r2->header.req_id == 2);
    CHECK(r1->header.client_id == r2->header.client_id);
}

TEST_CASE("fill of a resting order reaches its owner; ownership is enforced") {
    server::Exchange ex(testConfig(net::IdlePolicy::Never));
    REQUIRE(ex.start());
    ClientSession alice({"127.0.0.1", ex.port()}), bob({"127.0.0.1", ex.port()});
    std::vector<OrderAck> updates;
    alice.onUpdate([&updates](const OrderAck& u) { updates.push_back(u); });

    const uint64_t order = ackOf(alice.bid(AAPL, 100, 10))->order_id;
    CHECK(isError(bob.cancel(order), ErrorCode::NotOwner));
    CHECK(ackOf(bob.ask(AAPL, 99, 4))->status == OrderStatus::Filled);
    spin(100, {&alice});
    REQUIRE(updates.size() == 1);
    CHECK(updates[0].order_id == order);
    CHECK(updates[0].remaining_vol == 6);
    CHECK(updates[0].trades.at(0).price == 100);  // trades at the resting price

    auto cancel = alice.cancel(order);
    REQUIRE(cancel);
    CHECK(cancel->header.type == MsgType::CancelAck);
    CHECK(isError(alice.cancel(order), ErrorCode::OrderNotFound));
}

TEST_CASE("idle connection gets DISCONNECT; next request reconnects with the same id") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    ClientSession alice({"127.0.0.1", ex.port()});
    alice.listFeed();
    const uint64_t id = alice.clientId();
    spin(kIdleMs + 100, {&alice});
    CHECK(alice.stats().disconnects_received == 1);
    CHECK(alice.listFeed());
    CHECK(alice.clientId() == id);
    CHECK(alice.stats().connects == 2);
}

TEST_CASE("idle policy 'never' keeps the connection open") {
    server::Exchange ex(testConfig(net::IdlePolicy::Never));
    REQUIRE(ex.start());
    ClientSession alice({"127.0.0.1", ex.port()});
    alice.listFeed();
    spin(kIdleMs + 100, {&alice});
    CHECK(alice.stats().disconnects_received == 0);
    CHECK(alice.listFeed());
    CHECK(alice.stats().connects == 1);
}

TEST_CASE("resting order idles out; later fill arrives via server-initiated connection") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    ClientSession alice({"127.0.0.1", ex.port()}), bob({"127.0.0.1", ex.port()});
    REQUIRE(alice.enablePush());
    std::vector<OrderAck> updates;
    alice.onUpdate([&updates](const OrderAck& u) { updates.push_back(u); });

    const uint64_t order = ackOf(alice.bid(AAPL, 100, 10))->order_id;
    spin(kIdleMs + 100, {&alice});
    REQUIRE(alice.stats().disconnects_received == 1);
    bob.ask(AAPL, 100, 10);
    spin(200, {&alice});
    CHECK(alice.stats().pushes_accepted == 1);
    REQUIRE(updates.size() == 1);
    CHECK(updates[0].order_id == order);
    CHECK(updates[0].status == OrderStatus::Filled);
    CHECK(ex.stats().push_connected.load() == 1);
}

TEST_CASE("unreachable client keeps a coalesced mailbox until it returns") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    ClientSession carol({"127.0.0.1", ex.port()}), bob({"127.0.0.1", ex.port()});
    std::vector<OrderAck> updates;
    carol.onUpdate([&updates](const OrderAck& u) { updates.push_back(u); });

    const uint64_t order = ackOf(carol.bid(AAPL, 100, 9))->order_id;
    spin(kIdleMs + 100, {&carol});
    for (int i = 0; i < 3; ++i) bob.ask(AAPL, 100, 3);
    spin(20, {&bob});
    CHECK(updates.empty());

    REQUIRE(carol.listFeed());
    REQUIRE(updates.size() == 1);
    CHECK(updates[0].order_id == order);
    CHECK(updates[0].status == OrderStatus::Filled);
    CHECK(updates[0].trades.size() == 3);
}

TEST_CASE("unknown or mismatched client ids are rejected") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    client::RawConnection raw;
    REQUIRE(raw.connect("127.0.0.1", ex.port()));
    raw.send(makeMessage(MsgType::ListFeed, 1, 0xABCDEF, Empty{}));
    CHECK(isError(raw.read(1000), ErrorCode::UnknownClient));
    raw.send(makeMessage(MsgType::ListFeed, 2, 0, Empty{}));
    auto bound = raw.read(1000);
    REQUIRE(bound);
    raw.send(makeMessage(MsgType::ListFeed, 3, bound->header.client_id + 1, Empty{}));
    CHECK(isError(raw.read(1000), ErrorCode::ClientMismatch));
    raw.read(300);
    CHECK(raw.closed());
}

TEST_CASE("malformed input closes only that connection") {
    server::Exchange ex(testConfig(net::IdlePolicy::Never));
    REQUIRE(ex.start());
    ClientSession good({"127.0.0.1", ex.port()});
    REQUIRE(good.listFeed());

    client::RawConnection bad;
    REQUIRE(bad.connect("127.0.0.1", ex.port()));
    std::vector<uint8_t> frame = client::encodeFrame(makeMessage(MsgType::Bid, 1, 0, OrderRequest{1, 1, 1}));
    frame[3] = 10;  // claims a 10-byte payload: BID needs 18
    frame.resize(wire::HEADER_SIZE + 10);
    bad.sendBytes(frame);
    bad.read(500);
    CHECK(bad.closed());
    CHECK(good.listFeed());
    CHECK(good.stats().connects == 1);
}

TEST_CASE("UDP: subscription delivers snapshots, survives TCP close, detects gaps") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    client::UdpListener feed;
    REQUIRE(feed.open());
    feed.dropEvery(5);
    {
        ClientSession alice({"127.0.0.1", ex.port()});
        auto ack = alice.subscribe(feed.port(), 0b001);
        REQUIRE(ack);
        CHECK(std::get<GroupMask>(ack->payload).group_mask == 0b001);
    }  // session destroyed: TCP closed without UNSUBSCRIBE
    spin(200, {}, {&feed});
    CHECK(feed.received() > 0);
    CHECK(feed.gaps() > 0);
    CHECK(feed.malformed() == 0);
}

TEST_CASE("UDP: unknown group rejected, unsubscribe is idempotent") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    ClientSession alice({"127.0.0.1", ex.port()});
    client::UdpListener feed;
    REQUIRE(feed.open());
    CHECK(isError(alice.subscribe(feed.port(), 1u << 20), ErrorCode::UnknownGroup));
    alice.subscribe(feed.port(), 0b111);
    CHECK(std::get<GroupMask>(alice.unsubscribe(0b011)->payload).group_mask == 0b011);
    CHECK(std::get<GroupMask>(alice.unsubscribe(0b011)->payload).group_mask == 0);
    CHECK(std::get<GroupMask>(alice.unsubscribe(0b100)->payload).group_mask == 0b100);
}

TEST_CASE("UDP: an endpoint claimed by a newer client leaves the old owner") {
    server::Exchange ex(testConfig());
    REQUIRE(ex.start());
    ClientSession first({"127.0.0.1", ex.port()}), second({"127.0.0.1", ex.port()});
    client::UdpListener feed;
    REQUIRE(feed.open());
    first.subscribe(feed.port(), 0b001);
    second.subscribe(feed.port(), 0b001);
    CHECK(std::get<GroupMask>(first.unsubscribe(0b001)->payload).group_mask == 0);
    CHECK(std::get<GroupMask>(second.unsubscribe(0b001)->payload).group_mask == 0b001);
}

// ---------------------------------------------------------------- text baseline

namespace {

server::ExchangeConfig textConfig(net::IdlePolicy policy) {
    server::ExchangeConfig cfg = testConfig(policy);
    cfg.dispatcher.text_port = 0;
    return cfg;
}

client::ClientConfig textClient(const server::Exchange& ex) {
    return client::ClientConfig{"127.0.0.1", ex.textPort(), net::Protocol::Text};
}

}  // namespace

TEST_CASE("text protocol: order flow, update and ownership over HTTP/1.1 + JSON") {
    server::Exchange ex(textConfig(net::IdlePolicy::Never));
    REQUIRE(ex.start());
    ClientSession alice(textClient(ex)), bob(textClient(ex));
    std::vector<OrderAck> updates;
    alice.onUpdate([&updates](const OrderAck& u) { updates.push_back(u); });
    REQUIRE(alice.listFeed());  // learns the ticker table
    REQUIRE(bob.listFeed());
    CHECK(alice.clientId() != 0);

    auto rest = alice.bid(AAPL, 100, 10);
    REQUIRE(ackOf(rest));
    CHECK(ackOf(rest)->status == OrderStatus::Resting);
    const uint64_t order = ackOf(rest)->order_id;
    CHECK(isError(bob.cancel(order), ErrorCode::NotOwner));
    CHECK(ackOf(bob.ask(AAPL, 100, 4))->status == OrderStatus::Filled);
    spin(100, {&alice});
    REQUIRE(updates.size() == 1);
    CHECK(updates[0].remaining_vol == 6);
    CHECK(isError(bob.bid(999, 1, 1), ErrorCode::UnknownSymbol));

    auto cancel = alice.cancel(order);
    REQUIRE(cancel);
    CHECK(cancel->header.type == MsgType::CancelAck);
}

TEST_CASE("text protocol: push after idle arrives as POST /order/update") {
    server::Exchange ex(textConfig(net::IdlePolicy::Fixed));
    REQUIRE(ex.start());
    ClientSession alice(textClient(ex)), bob(textClient(ex));
    REQUIRE(alice.enablePush());
    std::vector<OrderAck> updates;
    alice.onUpdate([&updates](const OrderAck& u) { updates.push_back(u); });
    alice.listFeed();
    bob.listFeed();

    alice.bid(AAPL, 100, 5);
    spin(kIdleMs + 100, {&alice});
    REQUIRE(alice.stats().disconnects_received == 1);
    bob.ask(AAPL, 100, 5);
    spin(200, {&alice});
    CHECK(alice.stats().pushes_accepted == 1);
    REQUIRE(updates.size() == 1);
    CHECK(updates[0].status == OrderStatus::Filled);
}

TEST_CASE("binary and text clients trade against the same book") {
    server::Exchange ex(textConfig(net::IdlePolicy::Never));
    REQUIRE(ex.start());
    ClientSession bin({"127.0.0.1", ex.port()}), txt(textClient(ex));
    std::vector<OrderAck> updates;
    bin.onUpdate([&updates](const OrderAck& u) { updates.push_back(u); });
    txt.listFeed();
    bin.bid(AAPL, 100, 3);
    auto fill = txt.ask(AAPL, 100, 3);
    REQUIRE(ackOf(fill));
    CHECK(ackOf(fill)->status == OrderStatus::Filled);
    spin(100, {&bin});
    REQUIRE(updates.size() == 1);
    CHECK(updates[0].status == OrderStatus::Filled);
}

TEST_CASE("text protocol: garbage closes only that connection; dispatch latency is recorded") {
    server::ExchangeConfig cfg = textConfig(net::IdlePolicy::Never);
    cfg.dispatcher.record_dispatch_latency = true;
    server::Exchange ex(cfg);
    REQUIRE(ex.start());
    ClientSession good(textClient(ex));
    good.listFeed();
    good.bid(AAPL, 100, 1);

    client::RawConnection bad;
    REQUIRE(bad.connect("127.0.0.1", ex.textPort()));
    bad.sendBytes(std::vector<uint8_t>{'G', 'E', 'T', ' ', '/', ' ', 'H', 'T', 'T', 'P', '/', '0', '.', '9', '\r', '\n', '\r', '\n'});
    CHECK(bad.waitForClose(1000));
    CHECK(good.listFeed());
    ex.stop();
    CHECK(ex.dispatchLatency().samples().size() == 1);  // the one BID
}
