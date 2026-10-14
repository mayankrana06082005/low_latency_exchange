#include "net/FrameReader.hpp"

namespace net {

using codec::BinaryCodec;
using codec::DecodeStatus;

FrameStatus FrameReader::next(wire::Message& out) {
    const uint8_t* data = buf_.data();
    const size_t avail = buf_.size();

    DecodeStatus status = BinaryCodec::decodeHeader(data, avail, out.header);
    if (status == DecodeStatus::Incomplete) return FrameStatus::NeedMore;
    if (status != DecodeStatus::Ok || out.header.length > max_frame_len_) {
        last_error_ = status == DecodeStatus::Ok ? DecodeStatus::Malformed : status;
        return FrameStatus::Fatal;
    }

    const size_t frame_len = wire::HEADER_SIZE + out.header.length;
    if (avail < frame_len) return FrameStatus::NeedMore;

    status = BinaryCodec::decode(data, frame_len, out);
    buf_.consume(frame_len);
    if (status == DecodeStatus::Ok) return FrameStatus::Frame;
    last_error_ = status;
    return status == DecodeStatus::UnknownType ? FrameStatus::BadFrame : FrameStatus::Fatal;
}

}  // namespace net
