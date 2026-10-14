#include "client/OrderLedger.hpp"

#include <cstdio>

namespace client {

using namespace wire;

namespace {

Volume tradedVolume(const OrderAck& ack) {
    Volume total = 0;
    for (const Trade& t : ack.trades) total += t.vol;
    return total;
}

}  // namespace

void OrderLedger::onSent(const std::string& owner, const Message& msg) {
    const auto* o = std::get_if<OrderRequest>(&msg.payload);
    if (o == nullptr) return;
    pending_[{owner, msg.header.req_id}] = Pending{msg.header.type, o->symbol_id, o->price, o->volume};
}

void OrderLedger::onReceived(const std::string& owner, const Message& msg) {
    const auto pending = pending_.find({owner, msg.header.req_id});
    const bool answers_order = msg.header.req_id != 0 && pending != pending_.end();

    if (msg.header.type == MsgType::OrderAck && answers_order) {
        openOrder(pending->second, owner, std::get<OrderAck>(msg.payload));
        pending_.erase(pending);
    } else if (const auto* ack = std::get_if<OrderAck>(&msg.payload)) {
        applyUpdate(*ack);  // ORDER_UPDATE, or a late ORDER_ACK
    } else if (const auto* c = std::get_if<CancelAck>(&msg.payload)) {
        auto it = orders_.find(c->order_id);
        if (it != orders_.end()) {
            it->second.cancelled = it->second.remaining;
            it->second.remaining = 0;
            it->second.status = c->status;
        }
    } else if (msg.header.type == MsgType::Error && answers_order) {
        pending_.erase(pending);  // order rejected; it never existed
    }
}

void OrderLedger::openOrder(const Pending& p, const std::string& owner, const OrderAck& ack) {
    OrderRecord r;
    r.owner = owner;
    r.side = p.side;
    r.symbol_id = p.symbol_id;
    r.price = p.price;
    r.qty = p.qty;
    r.filled = tradedVolume(ack);
    r.remaining = ack.remaining_vol;
    r.status = ack.status;
    r.trades = ack.trades.size();
    orders_[ack.order_id] = r;
}

void OrderLedger::applyUpdate(const OrderAck& update) {
    auto it = orders_.find(update.order_id);
    if (it == orders_.end()) return;
    OrderRecord& r = it->second;
    r.filled += tradedVolume(update);  // a coalesced update carries several trades
    r.remaining = update.remaining_vol;
    r.status = update.status;
    r.trades += update.trades.size();
    ++r.updates;
}

std::string OrderLedger::table(const SymbolName& symbol_name) const {
    std::string out;
    char line[200];
    std::snprintf(line, sizeof line, "  %-6s %-7s %-4s %-6s %6s %5s %7s %10s %10s  %-13s %7s %8s\n",
                  "order", "owner", "side", "symbol", "price", "qty", "filled", "remaining",
                  "cancelled", "status", "trades", "updates");
    out += line;
    for (const auto& entry : orders_) {
        const OrderRecord& r = entry.second;
        std::snprintf(line, sizeof line,
                      "  %-6llu %-7s %-4s %-6s %6llu %5llu %7llu %10llu %10llu  %-13s %7zu %8zu\n",
                      (unsigned long long)entry.first, r.owner.c_str(),
                      r.side == MsgType::Bid ? "BUY" : "SELL", symbol_name(r.symbol_id).c_str(),
                      (unsigned long long)r.price, (unsigned long long)r.qty,
                      (unsigned long long)r.filled, (unsigned long long)r.remaining,
                      (unsigned long long)r.cancelled, nameOf(r.status), r.trades, r.updates);
        out += line;
    }
    if (orders_.empty()) out += "  (no orders)\n";
    return out;
}

}  // namespace client
