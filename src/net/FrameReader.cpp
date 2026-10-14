#include "net/FrameReader.hpp"

#include <cstring>

namespace net {

using codec::BinaryCodec;
using codec::DecodeStatus;

std::pair<uint8_t*, size_t> FrameReader::prepare(size_t n) {
    if (start_ == end_) start_ = end_ = 0;
    if (buf_.size() - end_ < n && start_ > 0) {
        std::memmove(buf_.data(), buf_.data() + start_, end_ - start_);
        end_ -= start_;
        start_ = 0;
    }
    if (buf_.size() - end_ < n) buf_.resize(end_ + n);
    return {buf_.data() + end_, n};
}

void FrameReader::feed(const uint8_t* data, size_t n) {
    auto space = prepare(n);
    std::memcpy(space.first, data, n);
    commit(n);
}

FrameStatus FrameReader::next(wire::Message& out) {
    const uint8_t* data = buf_.data() + start_;
    const size_t avail = end_ - start_;

    DecodeStatus status = BinaryCodec::decodeHeader(data, avail, out.header);
    if (status == DecodeStatus::Incomplete) return FrameStatus::NeedMore;
    if (status != DecodeStatus::Ok || out.header.length > max_frame_len_) {
        last_error_ = status == DecodeStatus::Ok ? DecodeStatus::Malformed : status;
        return FrameStatus::Fatal;
    }

    const size_t frame_len = wire::HEADER_SIZE + out.header.length;
    if (avail < frame_len) return FrameStatus::NeedMore;

    status = BinaryCodec::decode(data, frame_len, out);
    start_ += frame_len;
    if (status == DecodeStatus::Ok) return FrameStatus::Frame;
    last_error_ = status;
    return status == DecodeStatus::UnknownType ? FrameStatus::BadFrame : FrameStatus::Fatal;
}

}  // namespace net
