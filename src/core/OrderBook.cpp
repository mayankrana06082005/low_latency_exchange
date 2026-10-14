#include "core/OrderBook.hpp"

#include <algorithm>

namespace core {

using wire::Price;
using wire::Volume;

template <typename Book, typename Crosses>
Volume OrderBook::match(Book& book, Price limit, Volume volume, Crosses crosses,
                        std::vector<Fill>& fills) {
    while (volume > 0 && !book.empty()) {
        auto level = book.begin();
        if (!crosses(level->first, limit)) break;
        Queue& queue = level->second;
        while (volume > 0 && !queue.empty()) {
            Resting& r = queue.front();
            const Volume traded = std::min(volume, r.remaining);
            volume -= traded;
            r.remaining -= traded;
            fills.push_back(Fill{r.order_id, level->first, traded, r.remaining});
            if (r.remaining == 0) {
                index_.erase(r.order_id);
                queue.pop_front();
            }
        }
        if (queue.empty()) book.erase(level);
    }
    return volume;
}

MatchResult OrderBook::submit(uint64_t order_id, Side side, Price price, Volume volume) {
    MatchResult result;
    if (side == Side::Buy) {
        result.remaining = match(asks_, price, volume,
                                 [](Price ask, Price limit) { return ask <= limit; }, result.fills);
        if (result.remaining > 0) bids_[price].push_back(Resting{order_id, result.remaining});
    } else {
        result.remaining = match(bids_, price, volume,
                                 [](Price bid, Price limit) { return bid >= limit; }, result.fills);
        if (result.remaining > 0) asks_[price].push_back(Resting{order_id, result.remaining});
    }
    if (result.remaining > 0) index_[order_id] = Locator{side, price};
    return result;
}

template <typename Book>
bool OrderBook::removeFrom(Book& book, Price price, uint64_t order_id, Volume& cancelled) {
    auto level = book.find(price);
    if (level == book.end()) return false;
    Queue& queue = level->second;
    auto it = std::find_if(queue.begin(), queue.end(),
                           [order_id](const Resting& r) { return r.order_id == order_id; });
    if (it == queue.end()) return false;
    cancelled = it->remaining;
    queue.erase(it);
    if (queue.empty()) book.erase(level);
    return true;
}

bool OrderBook::cancel(uint64_t order_id, Volume& cancelled) {
    auto it = index_.find(order_id);
    if (it == index_.end()) return false;
    const Locator loc = it->second;
    index_.erase(it);
    return loc.side == Side::Buy ? removeFrom(bids_, loc.price, order_id, cancelled)
                                 : removeFrom(asks_, loc.price, order_id, cancelled);
}

template <typename Book>
uint8_t OrderBook::collect(const Book& book, std::array<wire::Level, wire::BOOK_LEVELS>& out) {
    uint8_t n = 0;
    for (auto level = book.begin(); level != book.end() && n < wire::BOOK_LEVELS; ++level, ++n) {
        Volume total = 0;
        for (const Resting& r : level->second) total += r.remaining;
        out[n] = wire::Level{level->first, total};
    }
    for (size_t i = n; i < wire::BOOK_LEVELS; ++i) out[i] = wire::Level{};
    return n;
}

uint8_t OrderBook::topLevels(Side side, std::array<wire::Level, wire::BOOK_LEVELS>& out) const {
    return side == Side::Buy ? collect(bids_, out) : collect(asks_, out);
}

}  // namespace core
