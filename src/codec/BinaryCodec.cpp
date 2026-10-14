#include "codec/BinaryCodec.hpp"

#include <cassert>
#include <utility>

#include "wire/ByteReader.hpp"
#include "wire/ByteWriter.hpp"

namespace codec {

using namespace wire;

namespace {

// ---------------------------------------------------------------- encoding

void encodePayload(ByteWriter&, const Empty&) {}

void encodePayload(ByteWriter& w, const OrderRequest& p) {
    w.u16(p.symbol_id);
    w.u64(p.price);
    w.u64(p.volume);
}

void encodePayload(ByteWriter& w, const CancelRequest& p) { w.u64(p.order_id); }

void encodePayload(ByteWriter& w, const SubscribeRequest& p) {
    w.u16(p.udp_port);
    w.u32(p.group_mask);
}

void encodePayload(ByteWriter& w, const GroupMask& p) { w.u32(p.group_mask); }

void encodeTrade(ByteWriter& w, const Trade& t) {
    w.u64(t.trade_id);
    w.u64(t.price);
    w.u64(t.vol);
    w.u64(t.counterpart_id);
}

void encodePayload(ByteWriter& w, const OrderAck& p) {
    assert(p.trades.size() <= UINT16_MAX);
    w.u64(p.order_id);
    w.u8(static_cast<uint8_t>(p.status));
    w.u64(p.remaining_vol);
    w.u16(static_cast<uint16_t>(p.trades.size()));
    for (const Trade& t : p.trades) encodeTrade(w, t);
}

void encodePayload(ByteWriter& w, const CancelAck& p) {
    w.u64(p.order_id);
    w.u8(static_cast<uint8_t>(p.status));
}

void encodePayload(ByteWriter& w, const ListFeedRsp& p) {
    assert(p.groups.size() <= UINT8_MAX);
    w.u8(static_cast<uint8_t>(p.groups.size()));
    for (const GroupInfo& g : p.groups) {
        assert(g.name.size() <= UINT8_MAX && g.tickers.size() <= UINT8_MAX);
        w.u8(g.group_id);
        w.u8(static_cast<uint8_t>(g.name.size()));
        w.bytes(g.name.data(), g.name.size());
        w.u8(static_cast<uint8_t>(g.tickers.size()));
        for (const TickerInfo& t : g.tickers) {
            w.u16(t.symbol_id);
            w.bytes(t.ticker.data(), TICKER_LEN);
        }
    }
}

void encodePayload(ByteWriter& w, const Disconnect& p) { w.u8(static_cast<uint8_t>(p.reason)); }

void encodePayload(ByteWriter& w, const Error& p) { w.u16(static_cast<uint16_t>(p.code)); }

[[maybe_unused]] bool payloadMatches(MsgType type, const Payload& p) {
    switch (type) {
        case MsgType::Bid:
        case MsgType::Ask: return std::holds_alternative<OrderRequest>(p);
        case MsgType::Cancel: return std::holds_alternative<CancelRequest>(p);
        case MsgType::ListFeed: return std::holds_alternative<Empty>(p);
        case MsgType::Subscribe: return std::holds_alternative<SubscribeRequest>(p);
        case MsgType::Unsubscribe:
        case MsgType::SubscribeAck:
        case MsgType::UnsubscribeAck: return std::holds_alternative<GroupMask>(p);
        case MsgType::OrderAck:
        case MsgType::OrderUpdate: return std::holds_alternative<OrderAck>(p);
        case MsgType::CancelAck: return std::holds_alternative<CancelAck>(p);
        case MsgType::ListFeedRsp: return std::holds_alternative<ListFeedRsp>(p);
        case MsgType::Error: return std::holds_alternative<Error>(p);
        case MsgType::Disconnect: return std::holds_alternative<Disconnect>(p);
    }
    return false;
}

// ---------------------------------------------------------------- decoding

bool readStatus(ByteReader& r, OrderStatus& s) {
    uint8_t v = 0;
    if (!r.u8(v) || v > static_cast<uint8_t>(OrderStatus::Cancelled)) return false;
    s = static_cast<OrderStatus>(v);
    return true;
}

bool decodePayload(ByteReader&, Empty&) { return true; }

bool decodePayload(ByteReader& r, OrderRequest& p) {
    return r.u16(p.symbol_id) && r.u64(p.price) && r.u64(p.volume);
}

bool decodePayload(ByteReader& r, CancelRequest& p) { return r.u64(p.order_id); }

bool decodePayload(ByteReader& r, SubscribeRequest& p) {
    return r.u16(p.udp_port) && r.u32(p.group_mask);
}

bool decodePayload(ByteReader& r, GroupMask& p) { return r.u32(p.group_mask); }

bool decodeTrade(ByteReader& r, Trade& t) {
    return r.u64(t.trade_id) && r.u64(t.price) && r.u64(t.vol) && r.u64(t.counterpart_id);
}

bool decodePayload(ByteReader& r, OrderAck& p) {
    uint16_t n_trades = 0;
    if (!(r.u64(p.order_id) && readStatus(r, p.status) && r.u64(p.remaining_vol) &&
          r.u16(n_trades))) {
        return false;
    }
    // Check the count against the bytes actually present before allocating, so a
    // forged n_trades cannot force a 2 MB allocation from a 40-byte frame.
    if (r.remaining() < static_cast<size_t>(n_trades) * TRADE_SIZE) return false;
    p.trades.resize(n_trades);
    for (Trade& t : p.trades) {
        if (!decodeTrade(r, t)) return false;
    }
    return true;
}

bool decodePayload(ByteReader& r, CancelAck& p) {
    return r.u64(p.order_id) && readStatus(r, p.status);
}

bool decodePayload(ByteReader& r, ListFeedRsp& p) {
    uint8_t n_groups = 0;
    if (!r.u8(n_groups)) return false;
    p.groups.resize(n_groups);
    for (GroupInfo& g : p.groups) {
        uint8_t name_len = 0;
        uint8_t n_tickers = 0;
        if (!(r.u8(g.group_id) && r.u8(name_len))) return false;
        g.name.resize(name_len);
        if (!(r.bytes(g.name.data(), name_len) && r.u8(n_tickers))) return false;
        g.tickers.resize(n_tickers);
        for (TickerInfo& t : g.tickers) {
            if (!(r.u16(t.symbol_id) && r.bytes(t.ticker.data(), TICKER_LEN))) return false;
        }
    }
    return true;
}

bool decodePayload(ByteReader& r, Disconnect& p) {
    uint8_t v = 0;
    if (!r.u8(v)) return false;
    p.reason = static_cast<DisconnectReason>(v);
    return true;
}

bool decodePayload(ByteReader& r, Error& p) {
    uint16_t v = 0;
    if (!r.u16(v)) return false;
    p.code = static_cast<ErrorCode>(v);
    return true;
}

// Decodes a payload of type T; the payload must be consumed exactly.
template <typename T>
DecodeStatus decodeAs(ByteReader& r, Payload& out) {
    T p;
    if (!decodePayload(r, p) || !r.atEnd()) return DecodeStatus::Malformed;
    out = std::move(p);
    return DecodeStatus::Ok;
}

DecodeStatus decodeBody(MsgType type, ByteReader& r, Payload& out) {
    switch (type) {
        case MsgType::Bid:
        case MsgType::Ask: return decodeAs<OrderRequest>(r, out);
        case MsgType::Cancel: return decodeAs<CancelRequest>(r, out);
        case MsgType::ListFeed: return decodeAs<Empty>(r, out);
        case MsgType::Subscribe: return decodeAs<SubscribeRequest>(r, out);
        case MsgType::Unsubscribe:
        case MsgType::SubscribeAck:
        case MsgType::UnsubscribeAck: return decodeAs<GroupMask>(r, out);
        case MsgType::OrderAck:
        case MsgType::OrderUpdate: return decodeAs<OrderAck>(r, out);
        case MsgType::CancelAck: return decodeAs<CancelAck>(r, out);
        case MsgType::ListFeedRsp: return decodeAs<ListFeedRsp>(r, out);
        case MsgType::Error: return decodeAs<Error>(r, out);
        case MsgType::Disconnect: return decodeAs<Disconnect>(r, out);
    }
    return DecodeStatus::UnknownType;
}

void encodeLevels(ByteWriter& w, const std::array<Level, BOOK_LEVELS>& levels, uint8_t n) {
    for (size_t i = 0; i < BOOK_LEVELS; ++i) {
        bool used = i < n;
        w.u64(used ? levels[i].price : 0);
        w.u64(used ? levels[i].vol : 0);
    }
}

bool decodeLevels(ByteReader& r, std::array<Level, BOOK_LEVELS>& levels) {
    for (Level& l : levels) {
        if (!(r.u64(l.price) && r.u64(l.vol))) return false;
    }
    return true;
}

}  // namespace

// ---------------------------------------------------------------- public API

void BinaryCodec::encode(const Message& msg, std::vector<uint8_t>& out) {
    const FrameHeader& h = msg.header;
    assert(payloadMatches(h.type, msg.payload));

    ByteWriter w(out);
    const size_t start = w.size();
    w.u32(0);  // length, patched once the payload size is known
    w.u32(h.req_id);
    w.u64(h.client_id);
    w.u16(h.callback_port);
    w.u8(static_cast<uint8_t>(h.type));
    w.u8(h.version);
    std::visit([&w](const auto& p) { encodePayload(w, p); }, msg.payload);
    w.patchU32(start, static_cast<uint32_t>(w.size() - start - HEADER_SIZE));
}

DecodeStatus BinaryCodec::decodeHeader(const uint8_t* data, size_t len, FrameHeader& out) {
    if (len < HEADER_SIZE) return DecodeStatus::Incomplete;
    ByteReader r(data, HEADER_SIZE);
    uint8_t type = 0;
    r.u32(out.length);
    r.u32(out.req_id);
    r.u64(out.client_id);
    r.u16(out.callback_port);
    r.u8(type);
    r.u8(out.version);
    out.type = static_cast<MsgType>(type);
    if (out.version != PROTOCOL_VERSION) return DecodeStatus::UnsupportedVersion;
    return DecodeStatus::Ok;
}

DecodeStatus BinaryCodec::decode(const uint8_t* data, size_t len, Message& out) {
    DecodeStatus status = decodeHeader(data, len, out.header);
    if (status != DecodeStatus::Ok) return status;

    const size_t frame_len = HEADER_SIZE + static_cast<size_t>(out.header.length);
    if (len < frame_len) return DecodeStatus::Incomplete;
    if (len > frame_len) return DecodeStatus::Malformed;
    if (!isKnownType(static_cast<uint8_t>(out.header.type))) return DecodeStatus::UnknownType;

    ByteReader r(data + HEADER_SIZE, out.header.length);
    return decodeBody(out.header.type, r, out.payload);
}

void BinaryCodec::encodeMarketData(const MarketData& md, std::vector<uint8_t>& out) {
    assert(md.n_bids <= BOOK_LEVELS && md.n_asks <= BOOK_LEVELS);
    ByteWriter w(out);
    [[maybe_unused]] const size_t start = w.size();
    w.u64(md.seq);
    w.u64(md.timestamp_us);
    w.u32(md.group_id);
    w.bytes(md.ticker.data(), TICKER_LEN);
    w.u8(md.n_bids);
    w.u8(md.n_asks);
    w.zeros(2);  // reserved
    encodeLevels(w, md.bids, md.n_bids);
    encodeLevels(w, md.asks, md.n_asks);
    assert(w.size() - start == MARKET_DATA_SIZE);
}

bool BinaryCodec::decodeMarketData(const uint8_t* data, size_t len, MarketData& out) {
    if (len != MARKET_DATA_SIZE) return false;
    ByteReader r(data, len);
    bool ok = r.u64(out.seq) && r.u64(out.timestamp_us) && r.u32(out.group_id) &&
              r.bytes(out.ticker.data(), TICKER_LEN) && r.u8(out.n_bids) && r.u8(out.n_asks) &&
              r.skip(2) && decodeLevels(r, out.bids) && decodeLevels(r, out.asks);
    return ok && out.n_bids <= BOOK_LEVELS && out.n_asks <= BOOK_LEVELS;
}

}  // namespace codec
