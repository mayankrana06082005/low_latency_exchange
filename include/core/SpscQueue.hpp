#pragma once

#include <atomic>
#include <cstddef>
#include <utility>
#include <vector>

namespace core {

// Lock-free single-producer single-consumer ring. Exactly one thread may call
// tryPush and exactly one other thread may call tryPop.
template <typename T>
class SpscQueue {
public:
    explicit SpscQueue(size_t capacity) : slots_(roundUpPow2(capacity)), mask_(slots_.size() - 1) {}

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    // On failure (queue full) `value` is left untouched.
    bool tryPush(T&& value) {
        const size_t head = head_.load(std::memory_order_relaxed);
        if (head - tail_cache_ == slots_.size()) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (head - tail_cache_ == slots_.size()) return false;
        }
        slots_[head & mask_] = std::move(value);
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    bool tryPop(T& out) {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_cache_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (tail == head_cache_) return false;
        }
        out = std::move(slots_[tail & mask_]);
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    size_t capacity() const { return slots_.size(); }

private:
    static size_t roundUpPow2(size_t n) {
        size_t c = 1;
        while (c < n) c <<= 1;
        return c;
    }

    std::vector<T> slots_;
    const size_t mask_;
    // Producer-owned cache line.
    alignas(64) std::atomic<size_t> head_{0};
    size_t tail_cache_ = 0;
    // Consumer-owned cache line.
    alignas(64) std::atomic<size_t> tail_{0};
    size_t head_cache_ = 0;
};

}  // namespace core
