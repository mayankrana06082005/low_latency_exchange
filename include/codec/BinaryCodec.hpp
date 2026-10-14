#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "wire/Messages.hpp"

namespace codec {

enum class DecodeStatus {
    Ok,
    Incomplete,          // fewer bytes than the header or the declared frame
    Malformed,           // truncated field, trailing bytes, or out-of-range enum
    UnknownType,         // framing valid, type byte unknown: reply ERROR, keep connection
    UnsupportedVersion,  // treat as garbage stream: close connection
};

// Stateless encoder/decoder for the binary TCP frames and the UDP datagram.
// Stream reassembly (partial reads, frame-length cap) belongs to FrameReader.
class BinaryCodec {
public:
    // Appends one frame to `out`. header.length is computed, not read.
    // header.type must match the payload alternative (asserted).
    static void encode(const wire::Message& msg, std::vector<uint8_t>& out);

    // Reads only the 20-byte header. Ok means version is valid; length and type
    // are not yet checked.
    static DecodeStatus decodeHeader(const uint8_t* data, size_t len, wire::FrameHeader& out);

    // Decodes exactly one complete frame: `len` must equal 20 + header.length.
    static DecodeStatus decode(const uint8_t* data, size_t len, wire::Message& out);

    static void encodeMarketData(const wire::MarketData& md, std::vector<uint8_t>& out);
    static bool decodeMarketData(const uint8_t* data, size_t len, wire::MarketData& out);
};

}  // namespace codec
