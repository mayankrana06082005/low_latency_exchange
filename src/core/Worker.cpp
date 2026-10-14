#include "core/Worker.hpp"

#include <thread>

#include "codec/BinaryCodec.hpp"
#include "net/Socket.hpp"

namespace core {

using namespace wire;
using Clock = std::chrono::steady_clock;

namespace {

constexpr int kBatch = 64;

uint64_t unixMicros() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
}

}  // namespace

Worker::Worker(WorkerConfig cfg, SpscQueue<WorkItem>& in, SpscQueue<WorkerOutput>& out)
    : cfg_(std::move(cfg)), in_(in), out_(out) {
    for (const GroupConfig& g : cfg_.groups) {
        for (const SymbolConfig& s : g.symbols) {
            tickers_[s.symbol_id] = TickerState{OrderBook{}, g.group_id, toTicker(s.ticker)};
        }
    }
}

void Worker::run(const std::atomic<bool>& running) {
    running_ = &running;
    next_snapshot_ = Clock::now() + cfg_.snapshot_interval;
    WorkItem item;
    while (running.load(std::memory_order_relaxed)) {
        int processed = 0;
        while (processed < kBatch && in_.tryPop(item)) {
            process(item);
            ++processed;
        }
        maybePublish(Clock::now());
        if (processed == 0) std::this_thread::yield();
    }
}

void Worker::process(const WorkItem& item) {
    switch (item.type) {
        case MsgType::Bid:
        case MsgType::Ask: handleOrder(item); break;
        case MsgType::Cancel: handleCancel(item); break;
        case MsgType::Subscribe:
        case MsgType::Unsubscribe: handleSubscription(item); break;
        default: break;
    }
}

void Worker::handleOrder(const WorkItem& item) {
    auto it = tickers_.find(item.symbol_id);
    if (it == tickers_.end()) {
        emit(makeMessage(MsgType::Error, item.req_id, item.client_id, Error{ErrorCode::UnknownSymbol}));
        return;
    }
    const uint64_t id = nextId(order_counter_);
    const Side side = item.type == MsgType::Bid ? Side::Buy : Side::Sell;
    MatchResult result = it->second.book.submit(id, side, item.price, item.volume);

    OrderAck ack;
    ack.order_id = id;
    ack.remaining_vol = result.remaining;
    ack.status = result.remaining == 0   ? OrderStatus::Filled
                 : result.fills.empty() ? OrderStatus::Resting
                                        : OrderStatus::PartialFill;
    if (result.remaining > 0) order_symbol_[id] = item.symbol_id;

    // counterpart_id carries the other side's order_id (review item 4 still open).
    std::vector<OrderAck> updates;
    for (const Fill& f : result.fills) {
        const uint64_t trade_id = nextId(trade_counter_);
        ack.trades.push_back(Trade{trade_id, f.price, f.vol, f.resting_order_id});

        OrderAck update;
        update.order_id = f.resting_order_id;
        update.remaining_vol = f.resting_remaining;
        update.status = f.resting_remaining == 0 ? OrderStatus::Filled : OrderStatus::PartialFill;
        update.trades.push_back(Trade{trade_id, f.price, f.vol, id});
        if (f.resting_remaining == 0) order_symbol_.erase(f.resting_order_id);
        updates.push_back(std::move(update));
    }

    emit(makeMessage(MsgType::OrderAck, item.req_id, item.client_id, std::move(ack)));
    for (OrderAck& u : updates) emit(makeMessage(MsgType::OrderUpdate, 0, 0, std::move(u)));
}

void Worker::handleCancel(const WorkItem& item) {
    auto it = order_symbol_.find(item.order_id);
    Volume cancelled = 0;
    if (it == order_symbol_.end() || !tickers_[it->second].book.cancel(item.order_id, cancelled)) {
        emit(makeMessage(MsgType::Error, item.req_id, item.client_id, Error{ErrorCode::OrderNotFound}));
        return;
    }
    order_symbol_.erase(it);
    emit(makeMessage(MsgType::CancelAck, item.req_id, item.client_id,
                     CancelAck{item.order_id, OrderStatus::Cancelled}));
}

void Worker::handleSubscription(const WorkItem& item) {
    const uint64_t key = net::endpointKey(item.endpoint);
    if (item.type == MsgType::Subscribe) {
        Subscriber& s = subscribers_[key];
        s.addr = item.endpoint;
        s.group_mask |= item.group_mask;
        return;
    }
    auto it = subscribers_.find(key);
    if (it == subscribers_.end()) return;
    it->second.group_mask &= ~item.group_mask;
    if (it->second.group_mask == 0) subscribers_.erase(it);
}

void Worker::maybePublish(Clock::time_point now) {
    if (now < next_snapshot_) return;
    publishSnapshots();
    const Clock::time_point done = Clock::now();
    const auto round = std::chrono::duration_cast<std::chrono::microseconds>(done - now);
    next_snapshot_ = done + nextSnapshotDelay(round);
}

std::chrono::microseconds Worker::nextSnapshotDelay(std::chrono::microseconds last_round) {
    const std::chrono::microseconds interval = cfg_.snapshot_interval;
    if (cfg_.snapshot_policy == SnapshotPolicy::Fixed) return interval;
    const double t_udp = udp_round_.update(static_cast<double>(last_round.count()));
    udp_round_us_.store(static_cast<uint64_t>(t_udp), std::memory_order_relaxed);
    return std::chrono::microseconds(static_cast<int64_t>(t_udp)) + interval;  // T_UDP + dt_safety
}

void Worker::publishSnapshots() {
    if (subscribers_.empty()) return;
    std::vector<uint8_t> buf;
    buf.reserve(MARKET_DATA_SIZE);
    for (auto& entry : tickers_) publishTicker(entry.second, buf);
}

void Worker::publishTicker(TickerState& t, std::vector<uint8_t>& buf) {
    const uint32_t bit = 1u << t.group_id;
    bool wanted = false;
    for (const auto& s : subscribers_) wanted = wanted || (s.second.group_mask & bit);
    if (!wanted) return;

    MarketData md;
    md.seq = ++group_seq_[t.group_id];  // one seq per datagram generated for the group
    md.timestamp_us = unixMicros();
    md.group_id = t.group_id;
    md.ticker = t.name;
    md.n_bids = t.book.topLevels(Side::Buy, md.bids);
    md.n_asks = t.book.topLevels(Side::Sell, md.asks);

    buf.clear();
    codec::BinaryCodec::encodeMarketData(md, buf);
    for (const auto& s : subscribers_) {
        if (s.second.group_mask & bit) publisher_.sendTo(buf.data(), buf.size(), s.second.addr);
    }
}

void Worker::emit(Message&& msg) {
    // The dispatcher drains out-queues every loop, so a full queue is transient.
    while (!out_.tryPush(std::move(msg))) {
        if (running_ != nullptr && !running_->load(std::memory_order_relaxed)) return;
        std::this_thread::yield();
    }
}

uint64_t Worker::nextId(uint64_t& counter) const {
    return (static_cast<uint64_t>(cfg_.worker_id) << 56) | ++counter;
}

}  // namespace core
