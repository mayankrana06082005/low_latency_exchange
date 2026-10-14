#pragma once

#include <array>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <unordered_map>
#include <vector>

#include "wire/Messages.hpp"

namespace core {

enum class Side : uint8_t { Buy, Sell };

struct Fill {
    uint64_t resting_order_id;
    wire::Price price;
    wire::Volume vol;
    wire::Volume resting_remaining;  // after this fill
};

struct MatchResult {
    wire::Volume remaining = 0;  // left on the incoming order (rests if > 0)
    std::vector<Fill> fills;
};

// Stub limit order book for one ticker: price-time priority, trades at the
// resting price. Enough to drive fills through the network path; replaced by
// the pro-rata book in Phase 10.
class OrderBook {
public:
    MatchResult submit(uint64_t order_id, Side side, wire::Price price, wire::Volume volume);
    bool cancel(uint64_t order_id, wire::Volume& cancelled);
    uint8_t topLevels(Side side, std::array<wire::Level, wire::BOOK_LEVELS>& out) const;
    size_t restingCount() const { return index_.size(); }

private:
    struct Resting {
        uint64_t order_id;
        wire::Volume remaining;
    };
    using Queue = std::deque<Resting>;
    struct Locator {
        Side side;
        wire::Price price;
    };

    template <typename Book, typename Crosses>
    wire::Volume match(Book& book, wire::Price limit, wire::Volume volume, Crosses crosses,
                       std::vector<Fill>& fills);
    template <typename Book>
    static bool removeFrom(Book& book, wire::Price price, uint64_t order_id,
                           wire::Volume& cancelled);
    template <typename Book>
    static uint8_t collect(const Book& book, std::array<wire::Level, wire::BOOK_LEVELS>& out);

    std::map<wire::Price, Queue, std::greater<wire::Price>> bids_;  // best (highest) first
    std::map<wire::Price, Queue> asks_;                             // best (lowest) first
    std::unordered_map<uint64_t, Locator> index_;
};

}  // namespace core
