#include <doctest/doctest.h>

#include <cstring>
#include <vector>

#include "codec/BinaryCodec.hpp"

using codec::BinaryCodec;
using codec::DecodeStatus;
using namespace wire;

namespace {

using Bytes = std::vector<uint8_t>;

Message makeMessage(MsgType type, Payload payload) {
    Message m;
    m.header.req_id = 7;
    m.header.client_id = 0x0102030405060708ULL;
    m.header.callback_port = 9200;
    m.header.type = type;
    m.payload = std::move(payload);
    return m;
}

Bytes encode(const Message& m) {
    Bytes out;
    BinaryCodec::encode(m, out);
    return out;
}

// Encodes, decodes, re-encodes; the two byte strings must match.
Bytes roundTrip(const Message& m, Message& decoded) {
    Bytes first = encode(m);
    REQUIRE(BinaryCodec::decode(first.data(), first.size(), decoded) == DecodeStatus::Ok);
    CHECK(decoded.header.type == m.header.type);
    CHECK(decoded.header.req_id == m.header.req_id);
    CHECK(decoded.header.client_id == m.header.client_id);
    CHECK(decoded.header.callback_port == m.header.callback_port);
    CHECK(decoded.header.length == first.size() - HEADER_SIZE);
    CHECK(encode(decoded) == first);
    return first;
}

Ticker ticker(const char* s) {
    Ticker t{};
    std::strncpy(t.data(), s, TICKER_LEN);
    return t;
}

OrderAck sampleAck(size_t n_trades) {
    OrderAck a;
    a.order_id = (3ULL << 56) | 42;
    a.status = n_trades ? OrderStatus::PartialFill : OrderStatus::Resting;
    a.remaining_vol = 30;
    for (size_t i = 0; i < n_trades; ++i) {
        a.trades.push_back(Trade{100 + i, 180, 10, 0x55});
    }
    return a;
}

}  // namespace

TEST_CASE("golden vector: BID") {
    Message m = makeMessage(MsgType::Bid, OrderRequest{5, 180, 50});
    const Bytes expected = {
        0x00, 0x00, 0x00, 0x12,                          // length 18
        0x00, 0x00, 0x00, 0x07,                          // req_id 7
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,  // client_id
        0x23, 0xF0,                                      // callback_port 9200
        0x01,                                            // type BID
        0x01,                                            // version 1
        0x00, 0x05,                                      // symbol_id 5
        0, 0, 0, 0, 0, 0, 0, 0xB4,                       // price 180
        0, 0, 0, 0, 0, 0, 0, 0x32,                       // volume 50
    };
    CHECK(encode(m) == expected);
}

TEST_CASE("golden vector: ERROR from server") {
    Message m = makeMessage(MsgType::Error, Error{ErrorCode::QueueFull});
    m.header.callback_port = 0;
    const Bytes expected = {
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x07, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
        0x07, 0x08, 0x00, 0x00, 0x8F, 0x01, 0x00, 0x07,
    };
    CHECK(encode(m) == expected);
}

TEST_CASE("frame sizes match the size budget") {
    CHECK(encode(makeMessage(MsgType::Bid, OrderRequest{1, 1, 1})).size() == 38);
    CHECK(encode(makeMessage(MsgType::Cancel, CancelRequest{1})).size() == 28);
    CHECK(encode(makeMessage(MsgType::OrderAck, sampleAck(0))).size() ==
          HEADER_SIZE + ORDER_ACK_FIXED_SIZE);
    CHECK(encode(makeMessage(MsgType::OrderAck, sampleAck(1))).size() ==
          HEADER_SIZE + ORDER_ACK_FIXED_SIZE + TRADE_SIZE);
}

TEST_CASE("round trip: requests") {
    Message d;

    SUBCASE("BID") {
        roundTrip(makeMessage(MsgType::Bid, OrderRequest{5, 180, 50}), d);
        const auto& p = std::get<OrderRequest>(d.payload);
        CHECK(p.symbol_id == 5);
        CHECK(p.price == 180);
        CHECK(p.volume == 50);
    }
    SUBCASE("ASK") {
        roundTrip(makeMessage(MsgType::Ask, OrderRequest{65535, UINT64_MAX, 1}), d);
        CHECK(std::get<OrderRequest>(d.payload).price == UINT64_MAX);
    }
    SUBCASE("CANCEL") {
        roundTrip(makeMessage(MsgType::Cancel, CancelRequest{(2ULL << 56) | 9}), d);
        CHECK(std::get<CancelRequest>(d.payload).order_id == ((2ULL << 56) | 9));
    }
    SUBCASE("LIST_FEED") {
        Bytes b = roundTrip(makeMessage(MsgType::ListFeed, Empty{}), d);
        CHECK(b.size() == HEADER_SIZE);
    }
    SUBCASE("SUBSCRIBE") {
        roundTrip(makeMessage(MsgType::Subscribe, SubscribeRequest{9100, 0x80000001}), d);
        CHECK(std::get<SubscribeRequest>(d.payload).udp_port == 9100);
        CHECK(std::get<SubscribeRequest>(d.payload).group_mask == 0x80000001);
    }
    SUBCASE("UNSUBSCRIBE") {
        roundTrip(makeMessage(MsgType::Unsubscribe, GroupMask{0x6}), d);
        CHECK(std::get<GroupMask>(d.payload).group_mask == 0x6);
    }
}

TEST_CASE("round trip: responses") {
    Message d;

    SUBCASE("ORDER_ACK with no trades") {
        roundTrip(makeMessage(MsgType::OrderAck, sampleAck(0)), d);
        const auto& a = std::get<OrderAck>(d.payload);
        CHECK(a.status == OrderStatus::Resting);
        CHECK(a.trades.empty());
    }
    SUBCASE("ORDER_UPDATE with trades") {
        roundTrip(makeMessage(MsgType::OrderUpdate, sampleAck(3)), d);
        const auto& a = std::get<OrderAck>(d.payload);
        REQUIRE(a.trades.size() == 3);
        CHECK(a.trades[2].trade_id == 102);
        CHECK(a.trades[2].counterpart_id == 0x55);
    }
    SUBCASE("CANCEL_ACK") {
        roundTrip(makeMessage(MsgType::CancelAck, CancelAck{77, OrderStatus::Cancelled}), d);
        CHECK(std::get<CancelAck>(d.payload).order_id == 77);
    }
    SUBCASE("LIST_FEED_RSP") {
        ListFeedRsp rsp;
        rsp.groups.push_back(GroupInfo{0, "tech", {{1, ticker("AAPL")}, {2, ticker("MSFT")}}});
        rsp.groups.push_back(GroupInfo{1, "", {}});
        roundTrip(makeMessage(MsgType::ListFeedRsp, rsp), d);
        const auto& r = std::get<ListFeedRsp>(d.payload);
        REQUIRE(r.groups.size() == 2);
        CHECK(r.groups[0].name == "tech");
        REQUIRE(r.groups[0].tickers.size() == 2);
        CHECK(r.groups[0].tickers[1].symbol_id == 2);
        CHECK(std::string(r.groups[0].tickers[1].ticker.data(), 4) == "MSFT");
        CHECK(r.groups[1].tickers.empty());
    }
    SUBCASE("SUBSCRIBE_ACK and UNSUBSCRIBE_ACK") {
        roundTrip(makeMessage(MsgType::SubscribeAck, GroupMask{0x3}), d);
        roundTrip(makeMessage(MsgType::UnsubscribeAck, GroupMask{0x1}), d);
        CHECK(std::get<GroupMask>(d.payload).group_mask == 0x1);
    }
    SUBCASE("ERROR") {
        roundTrip(makeMessage(MsgType::Error, Error{ErrorCode::ClientMismatch}), d);
        CHECK(std::get<Error>(d.payload).code == ErrorCode::ClientMismatch);
    }
    SUBCASE("DISCONNECT") {
        roundTrip(makeMessage(MsgType::Disconnect, Disconnect{DisconnectReason::Idle}), d);
        CHECK(std::get<Disconnect>(d.payload).reason == DisconnectReason::Idle);
    }
}

TEST_CASE("decode rejects bad input") {
    Message d;
    Bytes bid = encode(makeMessage(MsgType::Bid, OrderRequest{5, 180, 50}));

    SUBCASE("short header is incomplete") {
        CHECK(BinaryCodec::decode(bid.data(), HEADER_SIZE - 1, d) == DecodeStatus::Incomplete);
    }
    SUBCASE("short payload is incomplete") {
        CHECK(BinaryCodec::decode(bid.data(), bid.size() - 1, d) == DecodeStatus::Incomplete);
    }
    SUBCASE("trailing bytes beyond the frame are malformed") {
        bid.push_back(0);
        CHECK(BinaryCodec::decode(bid.data(), bid.size(), d) == DecodeStatus::Malformed);
    }
    SUBCASE("payload shorter than the type needs is malformed") {
        bid[3] = 17;  // claim 17-byte payload
        bid.pop_back();
        CHECK(BinaryCodec::decode(bid.data(), bid.size(), d) == DecodeStatus::Malformed);
    }
    SUBCASE("wrong version") {
        bid[19] = 2;
        CHECK(BinaryCodec::decode(bid.data(), bid.size(), d) ==
              DecodeStatus::UnsupportedVersion);
    }
    SUBCASE("unknown type with valid framing") {
        bid[18] = 0x7E;
        CHECK(BinaryCodec::decode(bid.data(), bid.size(), d) == DecodeStatus::UnknownType);
    }
    SUBCASE("order status out of range") {
        Bytes ack = encode(makeMessage(MsgType::CancelAck, CancelAck{1, OrderStatus::Cancelled}));
        ack[HEADER_SIZE + 8] = 9;
        CHECK(BinaryCodec::decode(ack.data(), ack.size(), d) == DecodeStatus::Malformed);
    }
    SUBCASE("ORDER_ACK claiming more trades than it carries") {
        Bytes ack = encode(makeMessage(MsgType::OrderAck, sampleAck(1)));
        ack[HEADER_SIZE + 17] = 0xFF;  // n_trades high byte: 65281 trades
        ack[HEADER_SIZE + 18] = 0x01;
        CHECK(BinaryCodec::decode(ack.data(), ack.size(), d) == DecodeStatus::Malformed);
    }
    SUBCASE("ORDER_ACK with fewer trades than its bytes") {
        Bytes ack = encode(makeMessage(MsgType::OrderAck, sampleAck(2)));
        ack[HEADER_SIZE + 18] = 1;  // n_trades = 1, but 2 trades present
        CHECK(BinaryCodec::decode(ack.data(), ack.size(), d) == DecodeStatus::Malformed);
    }
}

TEST_CASE("UDP market data datagram") {
    MarketData md;
    md.seq = 12345;
    md.timestamp_us = 1700000000000000ULL;
    md.group_id = 4;
    md.ticker = ticker("AAPL");
    md.n_bids = 2;
    md.n_asks = 1;
    md.bids = {Level{180, 10}, Level{179, 20}, Level{999, 999}};  // third level unused
    md.asks = {Level{181, 5}, Level{}, Level{}};

    Bytes out;
    BinaryCodec::encodeMarketData(md, out);
    REQUIRE(out.size() == MARKET_DATA_SIZE);
    CHECK(out[28] == 2);
    CHECK(out[29] == 1);

    MarketData d;
    REQUIRE(BinaryCodec::decodeMarketData(out.data(), out.size(), d));
    CHECK(d.seq == 12345);
    CHECK(d.timestamp_us == md.timestamp_us);
    CHECK(d.group_id == 4);
    CHECK(d.bids[1].price == 179);
    CHECK(d.bids[2].price == 0);  // unused levels are zeroed on the wire
    CHECK(d.asks[0].vol == 5);

    SUBCASE("wrong size is rejected") {
        CHECK_FALSE(BinaryCodec::decodeMarketData(out.data(), out.size() - 1, d));
    }
    SUBCASE("level count above 3 is rejected") {
        out[28] = 4;
        CHECK_FALSE(BinaryCodec::decodeMarketData(out.data(), out.size(), d));
    }
}
