#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <utility>

#include "wire/Messages.hpp"

namespace client {

// Client-side view of one order, rebuilt only from frames seen on the wire.
struct OrderRecord {
    std::string owner;
    wire::MsgType side = wire::MsgType::Bid;
    uint16_t symbol_id = 0;
    wire::Price price = 0;
    wire::Volume qty = 0;
    wire::Volume filled = 0;
    wire::Volume remaining = 0;
    wire::Volume cancelled = 0;
    wire::OrderStatus status = wire::OrderStatus::Resting;
    size_t trades = 0;
    size_t updates = 0;  // ORDER_UPDATE frames applied
};

// Tracks every order placed by the sessions it observes. A BID/ASK is matched
// to its ORDER_ACK by (owner, req_id); later ORDER_UPDATE and CANCEL_ACK frames
// are matched by order_id.
class OrderLedger {
public:
    using SymbolName = std::function<std::string(uint16_t)>;

    void onSent(const std::string& owner, const wire::Message& msg);
    void onReceived(const std::string& owner, const wire::Message& msg);

    const std::map<uint64_t, OrderRecord>& orders() const { return orders_; }
    std::string table(const SymbolName& symbol_name) const;

private:
    struct Pending {
        wire::MsgType side;
        uint16_t symbol_id;
        wire::Price price;
        wire::Volume qty;
    };

    void openOrder(const Pending& p, const std::string& owner, const wire::OrderAck& ack);
    void applyUpdate(const wire::OrderAck& update);

    std::map<std::pair<std::string, uint32_t>, Pending> pending_;
    std::map<uint64_t, OrderRecord> orders_;
};

}  // namespace client
