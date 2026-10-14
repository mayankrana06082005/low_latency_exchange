#pragma once

// Logical host-side messages. The wire layout (protocol doc Section 4) is the
// contract; these structs are never copied onto the wire directly.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace wire {

using Price = uint64_t;
using Volume = uint64_t;

constexpr uint8_t PROTOCOL_VERSION = 1;
constexpr uint32_t DEFAULT_MAX_FRAME_LEN = 64 * 1024;
constexpr size_t TICKER_LEN = 8;
constexpr size_t BOOK_LEVELS = 3;

// Wire sizes in bytes (Section 4.2, 4.4, 4.5, 4.6).
constexpr size_t HEADER_SIZE = 4 + 4 + 8 + 2 + 1 + 1;
constexpr size_t ORDER_REQUEST_SIZE = 2 + 8 + 8;
constexpr size_t CANCEL_REQUEST_SIZE = 8;
constexpr size_t SUBSCRIBE_REQUEST_SIZE = 2 + 4;
constexpr size_t GROUP_MASK_SIZE = 4;
constexpr size_t TRADE_SIZE = 8 + 8 + 8 + 8;
constexpr size_t ORDER_ACK_FIXED_SIZE = 8 + 1 + 8 + 2;
constexpr size_t CANCEL_ACK_SIZE = 8 + 1;
constexpr size_t MARKET_DATA_SIZE = 128;

static_assert(HEADER_SIZE == 20, "frame header is 20 bytes");
static_assert(TRADE_SIZE == 32, "trade record is 32 bytes");
static_assert(8 + 8 + 4 + TICKER_LEN + 1 + 1 + 2 + 2 * BOOK_LEVELS * 16 == MARKET_DATA_SIZE,
              "UDP datagram is 128 bytes");

enum class MsgType : uint8_t {
    Bid = 0x01,
    Ask = 0x02,
    Cancel = 0x03,
    ListFeed = 0x04,
    Subscribe = 0x05,
    Unsubscribe = 0x06,
    OrderAck = 0x81,
    CancelAck = 0x83,
    ListFeedRsp = 0x84,
    SubscribeAck = 0x85,
    UnsubscribeAck = 0x86,
    Error = 0x8F,
    OrderUpdate = 0xC1,
    Disconnect = 0xC2,
};

inline bool isKnownType(uint8_t raw) {
    switch (static_cast<MsgType>(raw)) {
        case MsgType::Bid:
        case MsgType::Ask:
        case MsgType::Cancel:
        case MsgType::ListFeed:
        case MsgType::Subscribe:
        case MsgType::Unsubscribe:
        case MsgType::OrderAck:
        case MsgType::CancelAck:
        case MsgType::ListFeedRsp:
        case MsgType::SubscribeAck:
        case MsgType::UnsubscribeAck:
        case MsgType::Error:
        case MsgType::OrderUpdate:
        case MsgType::Disconnect:
            return true;
    }
    return false;
}

enum class OrderStatus : uint8_t { Resting = 0, PartialFill = 1, Filled = 2, Cancelled = 3 };

enum class ErrorCode : uint16_t {
    Malformed = 1,
    UnknownType = 2,
    InvalidField = 3,
    UnknownSymbol = 4,
    OrderNotFound = 5,
    NotOwner = 6,
    QueueFull = 7,
    UnknownGroup = 8,
    UnsupportedVersion = 9,
    UnknownClient = 10,
    ClientMismatch = 11,
};

enum class DisconnectReason : uint8_t { Idle = 0 };

using Ticker = std::array<char, TICKER_LEN>;  // NUL padded

struct FrameHeader {
    uint32_t length = 0;  // payload bytes; filled in by the encoder
    uint32_t req_id = 0;
    uint64_t client_id = 0;
    uint16_t callback_port = 0;
    MsgType type = MsgType::Bid;
    uint8_t version = PROTOCOL_VERSION;
};

// ---- Payloads. One struct may serve several types; header.type disambiguates.

struct Empty {};  // LIST_FEED

struct OrderRequest {  // BID, ASK
    uint16_t symbol_id = 0;
    Price price = 0;
    Volume volume = 0;
};

struct CancelRequest {  // CANCEL
    uint64_t order_id = 0;
};

struct SubscribeRequest {  // SUBSCRIBE
    uint16_t udp_port = 0;
    uint32_t group_mask = 0;
};

struct GroupMask {  // UNSUBSCRIBE, SUBSCRIBE_ACK, UNSUBSCRIBE_ACK
    uint32_t group_mask = 0;
};

struct Trade {
    uint64_t trade_id = 0;
    Price price = 0;
    Volume vol = 0;
    uint64_t counterpart_id = 0;
};

struct OrderAck {  // ORDER_ACK, ORDER_UPDATE
    uint64_t order_id = 0;
    OrderStatus status = OrderStatus::Resting;
    Volume remaining_vol = 0;
    std::vector<Trade> trades;  // at most 65535 (u16 count)
};

struct CancelAck {  // CANCEL_ACK
    uint64_t order_id = 0;
    OrderStatus status = OrderStatus::Cancelled;
};

struct TickerInfo {
    uint16_t symbol_id = 0;
    Ticker ticker{};
};

struct GroupInfo {
    uint8_t group_id = 0;
    std::string name;  // at most 255 bytes
    std::vector<TickerInfo> tickers;  // at most 255
};

struct ListFeedRsp {  // LIST_FEED_RSP
    std::vector<GroupInfo> groups;  // at most 255
};

struct Disconnect {  // DISCONNECT
    DisconnectReason reason = DisconnectReason::Idle;
};

struct Error {  // ERROR
    ErrorCode code = ErrorCode::Malformed;
};

using Payload = std::variant<Empty, OrderRequest, CancelRequest, SubscribeRequest, GroupMask,
                             OrderAck, CancelAck, ListFeedRsp, Disconnect, Error>;

struct Message {
    FrameHeader header;
    Payload payload;
};

inline Message makeMessage(MsgType type, uint32_t req_id, uint64_t client_id, Payload payload) {
    Message m;
    m.header.type = type;
    m.header.req_id = req_id;
    m.header.client_id = client_id;
    m.payload = std::move(payload);
    return m;
}

// Human-readable names for logs and the demo programs.
const char* nameOf(MsgType type);
const char* nameOf(OrderStatus status);
const char* nameOf(ErrorCode code);

// ---- UDP market data datagram (Section 4.6)

struct Level {
    Price price = 0;
    Volume vol = 0;
};

struct MarketData {
    uint64_t seq = 0;
    uint64_t timestamp_us = 0;
    uint32_t group_id = 0;
    Ticker ticker{};
    uint8_t n_bids = 0;
    uint8_t n_asks = 0;
    std::array<Level, BOOK_LEVELS> bids{};
    std::array<Level, BOOK_LEVELS> asks{};
};

}  // namespace wire
