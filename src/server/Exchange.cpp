#include "server/Exchange.hpp"

#include "net/Socket.hpp"

namespace server {

Exchange::Exchange(ExchangeConfig cfg) : cfg_(std::move(cfg)) {}

Exchange::~Exchange() { stop(); }

bool Exchange::start() {
    net::ignoreSigpipe();
    std::vector<net::WorkerLink> links;
    for (size_t i = 0; i < cfg_.market.groups.size(); ++i) {
        const core::GroupConfig& group = cfg_.market.groups[i];
        in_queues_.push_back(std::make_unique<core::SpscQueue<core::WorkItem>>(cfg_.queue_capacity));
        out_queues_.push_back(
            std::make_unique<core::SpscQueue<core::WorkerOutput>>(cfg_.queue_capacity));

        core::WorkerConfig wc;
        wc.worker_id = static_cast<uint8_t>(i);
        wc.groups = {group};
        wc.snapshot_interval = cfg_.snapshot_interval;
        workers_.push_back(
            std::make_unique<core::Worker>(wc, *in_queues_.back(), *out_queues_.back()));
        links.push_back(net::WorkerLink{in_queues_.back().get(), out_queues_.back().get(),
                                        1u << group.group_id});
    }

    dispatcher_ = std::make_unique<net::Dispatcher>(cfg_.dispatcher, cfg_.market, links);
    if (!dispatcher_->open()) return false;

    running_.store(true);
    for (auto& worker : workers_) {
        core::Worker* w = worker.get();
        threads_.emplace_back([this, w] { w->run(running_); });
    }
    threads_.emplace_back([this] { dispatcher_->run(running_); });
    return true;
}

void Exchange::stop() {
    running_.store(false);
    for (std::thread& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
}

uint16_t Exchange::port() const { return dispatcher_ ? dispatcher_->port() : 0; }

uint64_t Exchange::datagramsSent() const {
    uint64_t total = 0;
    for (const auto& w : workers_) total += w->datagramsSent();
    return total;
}

uint64_t Exchange::datagramsDropped() const {
    uint64_t total = 0;
    for (const auto& w : workers_) total += w->datagramsDropped();
    return total;
}

}  // namespace server
