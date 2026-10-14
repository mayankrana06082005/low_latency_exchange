#include "net/StreamCodec.hpp"

#include "codec/TextCodec.hpp"
#include "net/ByteBuffer.hpp"

namespace net {

using codec::DecodeStatus;

const char* nameOf(Protocol p) { return p == Protocol::Binary ? "binary" : "text"; }

namespace {

class BinaryStream final : public StreamCodec {
public:
    explicit BinaryStream(uint32_t max_frame_len) : reader_(max_frame_len) {}

    Protocol protocol() const override { return Protocol::Binary; }
    std::pair<uint8_t*, size_t> prepare(size_t n) override { return reader_.prepare(n); }
    void commit(size_t n) override { reader_.commit(n); }
    FrameStatus next(wire::Message& out) override { return reader_.next(out); }
    DecodeStatus lastError() const override { return reader_.lastError(); }
    void encode(const wire::Message& msg, std::vector<uint8_t>& out) override {
        codec::BinaryCodec::encode(msg, out);
    }

private:
    FrameReader reader_;
};

class TextStream final : public StreamCodec {
public:
    TextStream(std::shared_ptr<const codec::SymbolTable> symbols, uint32_t max_body)
        : symbols_(std::move(symbols)), max_body_(max_body) {}

    Protocol protocol() const override { return Protocol::Text; }
    std::pair<uint8_t*, size_t> prepare(size_t n) override { return buf_.prepare(n); }
    void commit(size_t n) override { buf_.commit(n); }
    DecodeStatus lastError() const override { return last_error_; }

    FrameStatus next(wire::Message& out) override {
        size_t consumed = 0;
        DecodeStatus st = codec::TextCodec::decode(buf_.data(), buf_.size(), *symbols_, max_body_,
                                                   out, consumed);
        if (st == DecodeStatus::Incomplete) return FrameStatus::NeedMore;
        if (st != DecodeStatus::Ok && st != DecodeStatus::UnknownType) {
            last_error_ = st;
            return FrameStatus::Fatal;
        }
        buf_.consume(consumed);
        assignRequestId(out);
        if (st == DecodeStatus::Ok) return FrameStatus::Frame;
        last_error_ = st;
        return FrameStatus::BadFrame;
    }

    void encode(const wire::Message& msg, std::vector<uint8_t>& out) override {
        codec::TextCodec::encode(msg, *symbols_, out);
    }

private:
    // A request without a Request-ID header (e.g. from curl) gets a
    // per-connection counter, the plan's original text-mode rule.
    void assignRequestId(wire::Message& m) {
        const bool client_to_server = (static_cast<uint8_t>(m.header.type) & 0x80) == 0;
        if (m.header.req_id == 0 && client_to_server) m.header.req_id = ++fallback_req_id_;
    }

    std::shared_ptr<const codec::SymbolTable> symbols_;
    uint32_t max_body_;
    ByteBuffer buf_;
    uint32_t fallback_req_id_ = 0;
    DecodeStatus last_error_ = DecodeStatus::Ok;
};

}  // namespace

std::unique_ptr<StreamCodec> makeStreamCodec(Protocol protocol,
                                             std::shared_ptr<const codec::SymbolTable> symbols,
                                             uint32_t max_frame_len) {
    if (protocol == Protocol::Text) {
        if (!symbols) symbols = std::make_shared<codec::SymbolTable>();
        return std::make_unique<TextStream>(std::move(symbols), max_frame_len);
    }
    return std::make_unique<BinaryStream>(max_frame_len);
}

}  // namespace net
