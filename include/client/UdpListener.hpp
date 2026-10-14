#pragma once

#include <cstdint>
#include <functional>
#include <unordered_map>

#include "wire/Messages.hpp"

namespace client {

// Receives market data datagrams and detects gaps per group from `seq`.
class UdpListener {
public:
    using SnapshotHandler = std::function<void(const wire::MarketData&)>;

    UdpListener() = default;
    ~UdpListener();
    UdpListener(const UdpListener&) = delete;
    UdpListener& operator=(const UdpListener&) = delete;

    bool open(uint16_t port = 0);  // 0 picks a free port
    uint16_t port() const { return port_; }
    void onSnapshot(SnapshotHandler handler) { on_snapshot_ = std::move(handler); }

    // Test aid: discard every n-th datagram before processing, to induce loss.
    void dropEvery(uint32_t n) { drop_every_ = n; }

    // Waits up to timeout_ms, then drains every queued datagram. Returns how many.
    int poll(int timeout_ms);

    uint64_t received() const { return received_; }
    uint64_t gaps() const { return gaps_; }        // gap events
    uint64_t missing() const { return missing_; }  // datagrams known lost
    uint64_t malformed() const { return malformed_; }

private:
    void process(const uint8_t* data, size_t len);

    int fd_ = -1;
    uint16_t port_ = 0;
    SnapshotHandler on_snapshot_;
    std::unordered_map<uint32_t, uint64_t> expected_seq_;  // group_id -> next seq
    uint32_t drop_every_ = 0;
    uint64_t arrivals_ = 0;
    uint64_t received_ = 0;
    uint64_t gaps_ = 0;
    uint64_t missing_ = 0;
    uint64_t malformed_ = 0;
};

}  // namespace client
