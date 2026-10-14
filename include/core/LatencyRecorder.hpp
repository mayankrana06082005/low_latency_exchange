#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace core {

struct LatencySummary {
    size_t count = 0;
    double mean_us = 0;
    double p50_us = 0;
    double p90_us = 0;
    double p99_us = 0;
    double p999_us = 0;
    double max_us = 0;
};

// Collects latency samples in nanoseconds. With a capacity set, add() stops
// recording once full instead of reallocating on a hot path.
class LatencyRecorder {
public:
    void setCapacity(size_t n) {
        samples_.reserve(n);
        bounded_ = true;
    }

    void add(uint64_t ns) {
        if (!bounded_ || samples_.size() < samples_.capacity()) samples_.push_back(ns);
    }

    void merge(const LatencyRecorder& other) {
        samples_.insert(samples_.end(), other.samples_.begin(), other.samples_.end());
    }

    const std::vector<uint64_t>& samples() const { return samples_; }
    bool empty() const { return samples_.empty(); }

    LatencySummary summarize() const {
        LatencySummary s;
        if (samples_.empty()) return s;
        std::vector<uint64_t> v = samples_;
        std::sort(v.begin(), v.end());
        double total = 0;
        for (uint64_t x : v) total += static_cast<double>(x);
        auto at = [&v](double q) {
            size_t i = static_cast<size_t>(q * static_cast<double>(v.size()));
            return static_cast<double>(v[std::min(i, v.size() - 1)]) / 1000.0;
        };
        s.count = v.size();
        s.mean_us = total / static_cast<double>(v.size()) / 1000.0;
        s.p50_us = at(0.50);
        s.p90_us = at(0.90);
        s.p99_us = at(0.99);
        s.p999_us = at(0.999);
        s.max_us = static_cast<double>(v.back()) / 1000.0;
        return s;
    }

private:
    std::vector<uint64_t> samples_;
    bool bounded_ = false;
};

}  // namespace core
