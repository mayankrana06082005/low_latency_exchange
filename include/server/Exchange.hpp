#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "core/MarketConfig.hpp"
#include "core/SpscQueue.hpp"
#include "core/Worker.hpp"
#include "net/Dispatcher.hpp"

namespace server {

struct ExchangeConfig {
    net::DispatcherConfig dispatcher;
    std::chrono::milliseconds snapshot_interval{100};
    size_t queue_capacity = 4096;
    core::MarketConfig market = core::MarketConfig::defaults();
};

// Wires the whole server: one dispatcher thread plus one worker thread per
// ticker group (D16), connected by SPSC queues.
class Exchange {
public:
    explicit Exchange(ExchangeConfig cfg);
    ~Exchange();
    Exchange(const Exchange&) = delete;
    Exchange& operator=(const Exchange&) = delete;

    bool start();
    void stop();
    uint16_t port() const;
    const net::DispatcherStats& stats() const { return dispatcher_->stats(); }
    uint64_t datagramsSent() const;
    uint64_t datagramsDropped() const;

private:
    ExchangeConfig cfg_;
    std::vector<std::unique_ptr<core::SpscQueue<core::WorkItem>>> in_queues_;
    std::vector<std::unique_ptr<core::SpscQueue<core::WorkerOutput>>> out_queues_;
    std::vector<std::unique_ptr<core::Worker>> workers_;
    std::unique_ptr<net::Dispatcher> dispatcher_;
    std::vector<std::thread> threads_;
    std::atomic<bool> running_{false};
};

}  // namespace server
