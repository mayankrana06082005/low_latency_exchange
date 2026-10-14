#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

namespace net {

// Growable receive buffer: recv() into prepare()/commit(), parse from data(),
// then consume() what was parsed. Unconsumed bytes are moved to the front only
// when space runs out, so steady-state reads do not copy.
class ByteBuffer {
public:
    std::pair<uint8_t*, size_t> prepare(size_t n) {
        if (start_ == end_) start_ = end_ = 0;
        if (buf_.size() - end_ < n && start_ > 0) {
            std::memmove(buf_.data(), buf_.data() + start_, end_ - start_);
            end_ -= start_;
            start_ = 0;
        }
        if (buf_.size() - end_ < n) buf_.resize(end_ + n);
        return {buf_.data() + end_, n};
    }

    void commit(size_t n) { end_ += n; }

    void append(const uint8_t* data, size_t n) {
        auto space = prepare(n);
        std::memcpy(space.first, data, n);
        commit(n);
    }

    const uint8_t* data() const { return buf_.data() + start_; }
    size_t size() const { return end_ - start_; }
    void consume(size_t n) { start_ += n; }

private:
    std::vector<uint8_t> buf_;
    size_t start_ = 0;
    size_t end_ = 0;
};

}  // namespace net
