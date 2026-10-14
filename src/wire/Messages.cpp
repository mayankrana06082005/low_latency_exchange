#include "wire/Messages.hpp"

namespace wire {

const char* nameOf(MsgType type) {
    switch (type) {
        case MsgType::Bid: return "BID";
        case MsgType::Ask: return "ASK";
        case MsgType::Cancel: return "CANCEL";
        case MsgType::ListFeed: return "LIST_FEED";
        case MsgType::Subscribe: return "SUBSCRIBE";
        case MsgType::Unsubscribe: return "UNSUBSCRIBE";
        case MsgType::OrderAck: return "ORDER_ACK";
        case MsgType::CancelAck: return "CANCEL_ACK";
        case MsgType::ListFeedRsp: return "LIST_FEED_RSP";
        case MsgType::SubscribeAck: return "SUBSCRIBE_ACK";
        case MsgType::UnsubscribeAck: return "UNSUBSCRIBE_ACK";
        case MsgType::Error: return "ERROR";
        case MsgType::OrderUpdate: return "ORDER_UPDATE";
        case MsgType::Disconnect: return "DISCONNECT";
    }
    return "UNKNOWN";
}

const char* nameOf(OrderStatus status) {
    switch (status) {
        case OrderStatus::Resting: return "RESTING";
        case OrderStatus::PartialFill: return "PARTIAL_FILL";
        case OrderStatus::Filled: return "FILLED";
        case OrderStatus::Cancelled: return "CANCELLED";
    }
    return "UNKNOWN";
}

const char* nameOf(ErrorCode code) {
    switch (code) {
        case ErrorCode::Malformed: return "MALFORMED";
        case ErrorCode::UnknownType: return "UNKNOWN_TYPE";
        case ErrorCode::InvalidField: return "INVALID_FIELD";
        case ErrorCode::UnknownSymbol: return "UNKNOWN_SYMBOL";
        case ErrorCode::OrderNotFound: return "ORDER_NOT_FOUND";
        case ErrorCode::NotOwner: return "NOT_OWNER";
        case ErrorCode::QueueFull: return "QUEUE_FULL";
        case ErrorCode::UnknownGroup: return "UNKNOWN_GROUP";
        case ErrorCode::UnsupportedVersion: return "UNSUPPORTED_VERSION";
        case ErrorCode::UnknownClient: return "UNKNOWN_CLIENT";
        case ErrorCode::ClientMismatch: return "CLIENT_MISMATCH";
    }
    return "UNKNOWN";
}

}  // namespace wire
