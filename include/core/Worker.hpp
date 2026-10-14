#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <unordered_map>
#include <vector>

#include "core/Ewma.hpp"
#include "core/MarketConfig.hpp"
#include "core/OrderBook.hpp"
#include "core/ServerTrace.hpp"
#include "core/SpscQueue.hpp"
#include "core/WorkItem.hpp"
#include "net/UdpPublisher.hpp"

namespace core {

// Fixed: one snapshot round every `snapshot_interval`.
// Ewma:  next round after Ttimeout = T_UDP + dt_safety (SRS), where T_UDP is the
//        EWMA of how long a snapshot round (encode + sendto fan-out) takes and
//        dt_safety = `snapshot_interval`. The worker therefore always gets at
//        least dt_safety of matching time between rounds, however many
//        subscribers there are.
enum class SnapshotPolicy { Fixed, Ewma };

struct WorkerConfig {
    uint8_t worker_id = 0;            // top byte of every order_id and trade_id (D9)
    std::vector<GroupConfig> groups;  // groups this worker owns
    SnapshotPolicy snapshot_policy = SnapshotPolicy::Fixed;
    std::chrono::milliseconds snapshot_interval{100};  // interval, or dt_safety for Ewma
    ServerTrace* trace = nullptr;  // server-side event log; nullptr = off
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
    // Current T_UDP estimate in microseconds (0 until the first round).
    uint64_t udpRoundMicros() const { return udp_round_us_.load(std::memory_order_relaxed); }
    // Delay before the next round, given how long the last round took.
    std::chrono::microseconds nextSnapshotDelay(std::chrono::microseconds last_round);

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
    void log(const std::string& text) const { cfg_.trace->log(actor_.c_str(), text); }
    std::string bookText(const TickerState& t) const;

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
    Ewma udp_round_;
    std::string actor_;  // "worker 0", for the trace
    std::atomic<uint64_t> udp_round_us_{0};
};

}  // namespace core
