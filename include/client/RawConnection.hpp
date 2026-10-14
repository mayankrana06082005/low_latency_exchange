#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "net/FrameReader.hpp"
#include "wire/Messages.hpp"

namespace client {

// Bare TCP connection that sends exactly the bytes it is given. Used by tests
// and the simulation to send pipelined, malformed or hand-crafted frames.
class RawConnection {
public:
    RawConnection() = default;
    ~RawConnection();
    RawConnection(const RawConnection&) = delete;
    RawConnection& operator=(const RawConnection&) = delete;

    bool connect(const std::string& host, uint16_t port);
    bool sendBytes(const std::vector<uint8_t>& bytes);
    bool send(const wire::Message& msg);
    // Next complete frame, or nullopt on timeout or close (see closed()).
    std::optional<wire::Message> read(int timeout_ms);
    bool closed() const { return closed_; }

private:
    int fd_ = -1;
    net::FrameReader reader_;
    bool closed_ = false;
};

std::vector<uint8_t> encodeFrame(const wire::Message& msg);

}  // namespace client
