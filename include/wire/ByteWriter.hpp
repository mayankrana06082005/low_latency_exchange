#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace wire {

// Appends big-endian integers to a byte buffer. The buffer may already hold
// data (e.g. a connection write buffer); writes always go to the end.
class ByteWriter {
public:
    explicit ByteWriter(std::vector<uint8_t>& out) : out_(out) {}

    void u8(uint8_t v) { out_.push_back(v); }
    void u16(uint16_t v) { putBE(v, 2); }
    void u32(uint32_t v) { putBE(v, 4); }
    void u64(uint64_t v) { putBE(v, 8); }

    void bytes(const void* src, size_t n) {
        const auto* p = static_cast<const uint8_t*>(src);
        out_.insert(out_.end(), p, p + n);
    }

    void zeros(size_t n) { out_.insert(out_.end(), n, 0); }

    // Overwrites a u32 at an absolute buffer offset (used to patch the frame length).
    void patchU32(size_t offset, uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out_[offset + i] = static_cast<uint8_t>(v >> (8 * (3 - i)));
        }
    }

    size_t size() const { return out_.size(); }

private:
    void putBE(uint64_t v, int n) {
        const size_t at = out_.size();
        out_.resize(at + static_cast<size_t>(n));  // one size update per field, not per byte
        uint8_t* p = out_.data() + at;
        for (int i = 0; i < n; ++i) p[i] = static_cast<uint8_t>(v >> (8 * (n - 1 - i)));
    }

    std::vector<uint8_t>& out_;
};

}  // namespace wire
