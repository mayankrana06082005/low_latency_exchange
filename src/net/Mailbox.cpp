#include "net/Mailbox.hpp"

namespace net {

void Mailbox::merge(const wire::OrderAck& update) {
    auto it = index_.find(update.order_id);
    if (it == index_.end()) {
        index_[update.order_id] = entries_.size();
        entries_.push_back(update);
        return;
    }
    wire::OrderAck& entry = entries_[it->second];
    entry.status = update.status;
    entry.remaining_vol = update.remaining_vol;
    entry.trades.insert(entry.trades.end(), update.trades.begin(), update.trades.end());
}

std::vector<wire::OrderAck> Mailbox::takeAll() {
    std::vector<wire::OrderAck> out;
    out.swap(entries_);
    index_.clear();
    return out;
}

}  // namespace net
