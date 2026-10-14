#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "codec/BinaryCodec.hpp"
#include "codec/SymbolTable.hpp"
#include "net/FrameReader.hpp"
#include "wire/Messages.hpp"

namespace net {

enum class Protocol { Binary, Text };

const char* nameOf(Protocol p);

// Per-connection codec (the doc's ICodec): bytes from recv() go in through
// prepare()/commit(), decoded messages come out of next(), and encode() appends
// one message to a write buffer. The dispatcher and clients only see
// wire::Message, so the binary and text protocols share everything else.
class StreamCodec {
public:
    virtual ~StreamCodec() = default;
    virtual Protocol protocol() const = 0;
    virtual std::pair<uint8_t*, size_t> prepare(size_t n) = 0;
    virtual void commit(size_t n) = 0;
    virtual FrameStatus next(wire::Message& out) = 0;
    virtual codec::DecodeStatus lastError() const = 0;
    virtual void encode(const wire::Message& msg, std::vector<uint8_t>& out) = 0;
};

// `symbols` is used by the text codec only (ticker <-> symbol_id); it may be
// updated later by its owner (a client learns it from LIST_FEED_RSP).
std::unique_ptr<StreamCodec> makeStreamCodec(Protocol protocol,
                                             std::shared_ptr<const codec::SymbolTable> symbols,
                                             uint32_t max_frame_len = wire::DEFAULT_MAX_FRAME_LEN);

}  // namespace net
