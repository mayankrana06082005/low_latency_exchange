#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace wire {

// Bounds-checked big-endian reader over a byte range it does not own.
// Every read returns false (and consumes nothing) if too few bytes remain,
// so decoders can chain reads with && and never read past the end.
class ByteReader {
public:
    ByteReader(const uint8_t* data, size_t len) : data_(data), len_(len) {}

    bool u8(uint8_t& v) { return get(v, 1); }
    bool u16(uint16_t& v) { return get(v, 2); }
    bool u32(uint32_t& v) { return get(v, 4); }
    bool u64(uint64_t& v) { return get(v, 8); }

    bool bytes(void* dst, size_t n) {
        if (remaining() < n) return false;
        std::memcpy(dst, data_ + pos_, n);
        pos_ += n;
        return true;
    }

    bool skip(size_t n) {
        if (remaining() < n) return false;
        pos_ += n;
        return true;
    }

    size_t remaining() const { return len_ - pos_; }
    bool atEnd() const { return pos_ == len_; }

private:
    template <typename T>
    bool get(T& v, size_t n) {
        if (remaining() < n) return false;
        uint64_t acc = 0;
        for (size_t i = 0; i < n; ++i) {
            acc = (acc << 8) | data_[pos_ + i];
        }
        v = static_cast<T>(acc);
        pos_ += n;
        return true;
    }

    const uint8_t* data_;
    size_t len_;
    size_t pos_ = 0;
};

}  // namespace wire
