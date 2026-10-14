#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "wire/Messages.hpp"

namespace net {

// Pending order updates for a client with no live connection (D24, v0.5).
// One entry per order_id, in first-insertion order. A newer update for the same
// order overwrites status and remaining_vol (both absolute) and appends its
// trades, so nothing is ever dropped and the size is bounded by orders, not fills.
class Mailbox {
public:
    void merge(const wire::OrderAck& update);
    std::vector<wire::OrderAck> takeAll();  // in order; leaves the mailbox empty
    bool empty() const { return entries_.empty(); }
    size_t size() const { return entries_.size(); }

private:
    std::vector<wire::OrderAck> entries_;
    std::unordered_map<uint64_t, size_t> index_;  // order_id -> position in entries_
};

}  // namespace net
