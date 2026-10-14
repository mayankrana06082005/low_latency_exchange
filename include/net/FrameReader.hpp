#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "codec/BinaryCodec.hpp"
#include "wire/Messages.hpp"

namespace net {

enum class FrameStatus {
    NeedMore,  // no complete frame buffered yet
    Frame,     // `out` holds a decoded frame
    BadFrame,  // framing valid but type unknown: reply ERROR UNKNOWN_TYPE, keep connection
    Fatal,     // bad version, oversize length or malformed payload: close connection
};

// Reassembles frames from a TCP byte stream: partial reads, several frames per
// read, and the MAX_FRAME_LEN cap (Section 4.1).
class FrameReader {
public:
    explicit FrameReader(uint32_t max_frame_len = wire::DEFAULT_MAX_FRAME_LEN)
        : max_frame_len_(max_frame_len) {}

    // Space to recv() into directly; call commit() with the bytes received.
    std::pair<uint8_t*, size_t> prepare(size_t n);
    void commit(size_t n) { end_ += n; }
    void feed(const uint8_t* data, size_t n);

    // On BadFrame and Fatal, out.header holds whatever header was read.
    FrameStatus next(wire::Message& out);
    codec::DecodeStatus lastError() const { return last_error_; }
    size_t buffered() const { return end_ - start_; }

private:
    std::vector<uint8_t> buf_;
    size_t start_ = 0;
    size_t end_ = 0;
    uint32_t max_frame_len_;
    codec::DecodeStatus last_error_ = codec::DecodeStatus::Ok;
};

}  // namespace net
