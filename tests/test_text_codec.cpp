#include <doctest/doctest.h>

#include <memory>
#include <string>
#include <vector>

#include "codec/BinaryCodec.hpp"
#include "codec/TextCodec.hpp"
#include "core/Ewma.hpp"
#include "core/MarketConfig.hpp"
#include "core/Worker.hpp"
#include "net/StreamCodec.hpp"

using codec::DecodeStatus;
using codec::SymbolTable;
using codec::TextCodec;
using namespace wire;

namespace {

SymbolTable market() {
    SymbolTable t;
    t.addAll(core::MarketConfig::defaults().toListFeed());
    return t;
}

std::string text(const Message& m) {
    std::vector<uint8_t> out;
    TextCodec::encode(m, market(), out);
    return std::string(out.begin(), out.end());
}

DecodeStatus decodeText(const std::string& s, Message& out, size_t& consumed) {
    return TextCodec::decode(reinterpret_cast<const uint8_t*>(s.data()), s.size(), market(),
                             DEFAULT_MAX_FRAME_LEN, out, consumed);
}

std::vector<uint8_t> binary(const Message& m) {
    std::vector<uint8_t> out;
    codec::BinaryCodec::encode(m, out);
    return out;
}

Message msg(MsgType type, uint32_t req_id, uint64_t client_id, Payload payload, uint16_t callback = 0) {
    Message m = makeMessage(type, req_id, client_id, std::move(payload));
    m.header.callback_port = callback;
    return m;
}

// Text round trip must give the same logical message, so both re-encode to the
// same binary bytes (the binary encoder recomputes length).
void checkEquivalent(const Message& m) {
    const std::string s = text(m);
    Message back;
    size_t consumed = 0;
    REQUIRE(decodeText(s, back, consumed) == DecodeStatus::Ok);
    CHECK(consumed == s.size());
    CHECK(binary(back) == binary(m));
}

}  // namespace

TEST_CASE("golden text vector: BID") {
    Message m = msg(MsgType::Bid, 7, 8841120934410029ULL, OrderRequest{1, 180, 50}, 9200);
    const std::string expected =
        "POST /order/bid HTTP/1.1\r\n"
        "Content-Length: 60\r\n"
        "Content-Type: application/json\r\n"
        "Client-ID: 8841120934410029\r\n"
        "Callback-Port: 9200\r\n"
        "Request-ID: 7\r\n"
        "\r\n"
        "{\"max_price\":180,\"ticker\":\"AAPL\",\"type\":\"LIMIT\",\"volume\":50}";
    CHECK(text(m) == expected);
}

TEST_CASE("golden text vector: CANCEL and an ERROR response") {
    CHECK(text(msg(MsgType::Cancel, 2, 5, CancelRequest{1042})) ==
          "DELETE /order/cancel HTTP/1.1\r\nClient-ID: 5\r\nRequest-ID: 2\r\nOrder-ID: 1042\r\n\r\n");
    CHECK(text(msg(MsgType::Error, 2, 5, Error{ErrorCode::NotOwner})) ==
          "HTTP/1.1 403 Forbidden\r\nContent-Length: 30\r\nContent-Type: application/json\r\n"
          "Client-ID: 5\r\nRequest-ID: 2\r\n\r\n{\"code\":6,\"error\":\"NOT_OWNER\"}");
}

TEST_CASE("same logical message through both codecs (Phase 3 exit criterion)") {
    OrderAck ack{(1ULL << 56) | 9, OrderStatus::PartialFill, 6, {Trade{(1ULL << 56) | 3, 100, 4, 77}}};
    ListFeedRsp feed = core::MarketConfig::defaults().toListFeed();

    SUBCASE("requests") {
        checkEquivalent(msg(MsgType::Bid, 1, 42, OrderRequest{3, 250, 9}, 9200));
        checkEquivalent(msg(MsgType::Ask, 2, 42, OrderRequest{6, 1, UINT64_MAX}));
        checkEquivalent(msg(MsgType::Cancel, 3, 42, CancelRequest{(2ULL << 56) | 5}));
        checkEquivalent(msg(MsgType::ListFeed, 4, 0, Empty{}));
        checkEquivalent(msg(MsgType::Subscribe, 5, 42, SubscribeRequest{9100, 0b101}));
        checkEquivalent(msg(MsgType::Unsubscribe, 6, 42, GroupMask{0b100}));
    }
    SUBCASE("responses and pushes") {
        checkEquivalent(msg(MsgType::OrderAck, 1, 42, ack));
        checkEquivalent(msg(MsgType::OrderAck, 1, 42, OrderAck{5, OrderStatus::Resting, 10, {}}));
        checkEquivalent(msg(MsgType::OrderUpdate, 0, 42, ack));
        checkEquivalent(msg(MsgType::CancelAck, 3, 42, CancelAck{5, OrderStatus::Cancelled}));
        checkEquivalent(msg(MsgType::ListFeedRsp, 4, 42, feed));
        checkEquivalent(msg(MsgType::SubscribeAck, 5, 42, GroupMask{0b011}));
        checkEquivalent(msg(MsgType::UnsubscribeAck, 6, 42, GroupMask{0}));
        checkEquivalent(msg(MsgType::Error, 7, 42, Error{ErrorCode::QueueFull}));
        checkEquivalent(msg(MsgType::Disconnect, 0, 42, Disconnect{DisconnectReason::Idle}));
    }
}

TEST_CASE("text decode: unknown ticker maps to symbol 0, which the dispatcher rejects") {
    Message out;
    size_t consumed = 0;
    std::string s = text(msg(MsgType::Bid, 1, 0, OrderRequest{999, 1, 1}));  // encodes "#999"
    REQUIRE(decodeText(s, out, consumed) == DecodeStatus::Ok);
    CHECK(std::get<OrderRequest>(out.payload).symbol_id == 0);
}

TEST_CASE("text decode: a curl-style request with extra headers and no Request-ID") {
    const std::string s =
        "GET /feed HTTP/1.1\r\nHost: localhost:9001\r\nUser-Agent: curl/8.5.0\r\nAccept: */*\r\n\r\n";
    Message out;
    size_t consumed = 0;
    REQUIRE(decodeText(s, out, consumed) == DecodeStatus::Ok);
    CHECK(out.header.type == MsgType::ListFeed);
    CHECK(out.header.req_id == 0);
}

TEST_CASE("text decode: bad input") {
    Message out;
    size_t consumed = 0;
    const std::string bid = text(msg(MsgType::Bid, 1, 0, OrderRequest{1, 100, 5}));

    SUBCASE("every proper prefix is incomplete") {
        for (size_t n = 0; n < bid.size(); ++n) {
            REQUIRE(decodeText(bid.substr(0, n), out, consumed) == DecodeStatus::Incomplete);
        }
    }
    SUBCASE("HTTP/1.0 is an unsupported version") {
        std::string s = bid;
        s.replace(s.find("HTTP/1.1"), 8, "HTTP/1.0");
        CHECK(decodeText(s, out, consumed) == DecodeStatus::UnsupportedVersion);
    }
    SUBCASE("unknown path is UnknownType and still consumed") {
        const std::string s = "GET /nope HTTP/1.1\r\n\r\n";
        CHECK(decodeText(s + s, out, consumed) == DecodeStatus::UnknownType);
        CHECK(consumed == s.size());
    }
    SUBCASE("broken JSON is malformed") {
        std::string s = bid;
        s[s.size() - 1] = 'x';
        CHECK(decodeText(s, out, consumed) == DecodeStatus::Malformed);
    }
    SUBCASE("negative price is malformed") {
        std::string s = bid;
        s.replace(s.find("100"), 3, "-10");
        CHECK(decodeText(s, out, consumed) == DecodeStatus::Malformed);
    }
    SUBCASE("CANCEL without Order-ID is malformed") {
        CHECK(decodeText("DELETE /order/cancel HTTP/1.1\r\n\r\n", out, consumed) == DecodeStatus::Malformed);
    }
    SUBCASE("oversize body and endless headers are malformed") {
        CHECK(decodeText("POST /order/bid HTTP/1.1\r\nContent-Length: 999999999\r\n\r\n", out, consumed) ==
              DecodeStatus::Malformed);
        CHECK(decodeText("GET /feed HTTP/1.1\r\n" + std::string(TextCodec::MAX_HEADER_BYTES, 'x'), out,
                         consumed) == DecodeStatus::Malformed);
    }
}

TEST_CASE("text stream: pipelined messages split at every byte") {
    auto symbols = std::make_shared<SymbolTable>(market());
    std::string all;
    all += text(msg(MsgType::Bid, 1, 9, OrderRequest{1, 100, 5}));
    all += text(msg(MsgType::ListFeed, 2, 9, Empty{}));
    all += text(msg(MsgType::Cancel, 3, 9, CancelRequest{77}));

    auto stream = net::makeStreamCodec(net::Protocol::Text, symbols);
    std::vector<uint32_t> ids;
    Message m;
    for (char c : all) {
        auto space = stream->prepare(1);
        space.first[0] = static_cast<uint8_t>(c);
        stream->commit(1);
        while (stream->next(m) == net::FrameStatus::Frame) ids.push_back(m.header.req_id);
    }
    CHECK(ids == std::vector<uint32_t>{1, 2, 3});
}

TEST_CASE("text stream: requests without Request-ID get a per-connection counter") {
    auto stream = net::makeStreamCodec(net::Protocol::Text, std::make_shared<SymbolTable>(market()));
    const std::string s = "GET /feed HTTP/1.1\r\n\r\nGET /feed HTTP/1.1\r\n\r\n";
    auto space = stream->prepare(s.size());
    std::copy(s.begin(), s.end(), space.first);
    stream->commit(s.size());
    Message m;
    REQUIRE(stream->next(m) == net::FrameStatus::Frame);
    CHECK(m.header.req_id == 1);
    REQUIRE(stream->next(m) == net::FrameStatus::Frame);
    CHECK(m.header.req_id == 2);
}

TEST_CASE("Ewma: primes on the first sample, then moves 1/8 of the way") {
    core::Ewma e;
    CHECK_FALSE(e.primed());
    CHECK(e.update(800) == doctest::Approx(800));
    CHECK(e.update(0) == doctest::Approx(700));
    CHECK(e.update(700) == doctest::Approx(700));
}

TEST_CASE("Worker: EWMA snapshot delay is T_UDP + dt_safety") {
    core::SpscQueue<core::WorkItem> in(4);
    core::SpscQueue<core::WorkerOutput> out(4);
    core::WorkerConfig cfg;
    cfg.snapshot_policy = core::SnapshotPolicy::Ewma;
    cfg.snapshot_interval = std::chrono::milliseconds(10);
    core::Worker ewma(cfg, in, out);
    using us = std::chrono::microseconds;
    CHECK(ewma.nextSnapshotDelay(us(800)) == us(10800));
    CHECK(ewma.nextSnapshotDelay(us(0)) == us(10700));

    cfg.snapshot_policy = core::SnapshotPolicy::Fixed;
    core::Worker fixed(cfg, in, out);
    CHECK(fixed.nextSnapshotDelay(us(800)) == us(10000));
}
