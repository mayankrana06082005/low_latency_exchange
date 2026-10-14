#pragma once

#include <netinet/in.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace net {

// Owns one non-blocking UDP socket. sendTo never blocks: a datagram that the
// kernel will not take right now (EAGAIN) is dropped and counted (Section 3.3).
class UdpPublisher {
public:
    UdpPublisher();
    ~UdpPublisher();
    UdpPublisher(const UdpPublisher&) = delete;
    UdpPublisher& operator=(const UdpPublisher&) = delete;

    void sendTo(const uint8_t* data, size_t len, const sockaddr_in& to);
    uint64_t sent() const { return sent_.load(std::memory_order_relaxed); }
    uint64_t dropped() const { return dropped_.load(std::memory_order_relaxed); }

private:
    int fd_ = -1;
    std::atomic<uint64_t> sent_{0};
    std::atomic<uint64_t> dropped_{0};
};

}  // namespace net
