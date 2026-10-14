#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <unordered_map>
#include <vector>

#include "core/MarketConfig.hpp"
#include "core/OrderBook.hpp"
#include "core/SpscQueue.hpp"
#include "core/WorkItem.hpp"
#include "net/UdpPublisher.hpp"

namespace core {

struct WorkerConfig {
    uint8_t worker_id = 0;            // top byte of every order_id and trade_id (D9)
    std::vector<GroupConfig> groups;  // groups this worker owns
    std::chrono::milliseconds snapshot_interval{100};
};

// Single-threaded owner of its order books, order_id counter, subscriber
// endpoints and UDP socket. Knows nothing about clients or connections (D7, D29).
class Worker {
public:
    Worker(WorkerConfig cfg, SpscQueue<WorkItem>& in, SpscQueue<WorkerOutput>& out);

    void run(const std::atomic<bool>& running);  // thread body
    void process(const WorkItem& item);
    void publishSnapshots();

    uint64_t datagramsSent() const { return publisher_.sent(); }
    uint64_t datagramsDropped() const { return publisher_.dropped(); }

private:
    struct TickerState {
        OrderBook book;
        uint8_t group_id;
        wire::Ticker name;
    };
    struct Subscriber {
        sockaddr_in addr;
        uint32_t group_mask;
    };

    void handleOrder(const WorkItem& item);
    void handleCancel(const WorkItem& item);
    void handleSubscription(const WorkItem& item);
    void maybePublish(std::chrono::steady_clock::time_point now);
    void publishTicker(TickerState& t, std::vector<uint8_t>& buf);
    void emit(wire::Message&& msg);
    uint64_t nextId(uint64_t& counter) const;

    WorkerConfig cfg_;
    SpscQueue<WorkItem>& in_;
    SpscQueue<WorkerOutput>& out_;
    std::map<uint16_t, TickerState> tickers_;
    std::unordered_map<uint64_t, uint16_t> order_symbol_;  // resting order -> symbol
    std::unordered_map<uint64_t, Subscriber> subscribers_;  // endpointKey -> subscriber
    std::array<uint64_t, 32> group_seq_{};
    uint64_t order_counter_ = 0;
    uint64_t trade_counter_ = 0;
    net::UdpPublisher publisher_;
    const std::atomic<bool>* running_ = nullptr;
    std::chrono::steady_clock::time_point next_snapshot_;
};

}  // namespace core
