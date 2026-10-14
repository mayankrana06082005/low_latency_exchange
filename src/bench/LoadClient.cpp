#include "bench/LoadClient.hpp"

#include <poll.h>
#include <sys/socket.h>

#include <cerrno>

#include "net/Socket.hpp"

namespace bench {

using namespace wire;
using Clock = std::chrono::steady_clock;

LoadClient::LoadClient(std::string host, uint16_t port, net::Protocol protocol)
    : host_(std::move(host)), port_(port), protocol_(protocol) {}

LoadClient::~LoadClient() { net::closeFd(fd_); }

bool LoadClient::connect() {
    net::ignoreSigpipe();
    fd_ = net::connectTcp(host_, port_);
    if (fd_ < 0) return false;
    codec_ = net::makeStreamCodec(protocol_, symbols_);
    codec_->encode(makeMessage(MsgType::ListFeed, next_req_id_++, 0, Empty{}), out_);
    if (!flush()) return false;

    const auto deadline = Clock::now() + std::chrono::seconds(2);
    Message m;
    while (Clock::now() < deadline) {
        pollfd p{fd_, POLLIN, 0};
        if (::poll(&p, 1, 100) <= 0) continue;
        auto space = codec_->prepare(64 * 1024);
        ssize_t n = ::recv(fd_, space.first, space.second, 0);
        if (n <= 0) return false;
        codec_->commit(static_cast<size_t>(n));
        if (codec_->next(m) != net::FrameStatus::Frame) continue;
        if (m.header.type != MsgType::ListFeedRsp) return false;
        client_id_ = m.header.client_id;
        symbols_->addAll(std::get<ListFeedRsp>(m.payload));
        return true;
    }
    return false;
}

void LoadClient::queueOrder(bool buy, uint16_t symbol_id) {
    Message m = makeMessage(buy ? MsgType::Bid : MsgType::Ask, next_req_id_++, client_id_,
                            OrderRequest{symbol_id, 100, 1});
    codec_->encode(m, out_);
}

bool LoadClient::flush() {
    size_t off = 0;
    while (off < out_.size()) {
        ssize_t n = net::sendSome(fd_, out_.data() + off, out_.size() - off);
        if (n > 0) {
            off += static_cast<size_t>(n);
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            pollfd p{fd_, POLLOUT, 0};
            ::poll(&p, 1, 10);
        } else {
            return false;
        }
    }
    out_.clear();
    return true;
}

bool LoadClient::readAvailable(int timeout_ms, LoadResult& result, int& replies) {
    pollfd p{fd_, POLLIN, 0};
    if (::poll(&p, 1, timeout_ms) <= 0) return true;
    auto space = codec_->prepare(256 * 1024);
    ssize_t n = ::recv(fd_, space.first, space.second, 0);
    if (n == 0) return false;
    if (n < 0) return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
    codec_->commit(static_cast<size_t>(n));
    Message m;
    for (;;) {
        net::FrameStatus st = codec_->next(m);
        if (st == net::FrameStatus::NeedMore) return true;
        if (st == net::FrameStatus::Fatal) return false;
        if (m.header.type == MsgType::OrderUpdate) {
            ++result.updates;
        } else if (m.header.req_id != 0) {
            ++result.responses;
            ++replies;
            if (m.header.type == MsgType::Error) ++result.errors;
        }
    }
}

LoadResult LoadClient::run(uint16_t symbol_id, int window, Clock::time_point until) {
    LoadResult result;
    bool buy = true;
    for (int i = 0; i < window; ++i, buy = !buy) queueOrder(buy, symbol_id);
    result.sent += static_cast<uint64_t>(window);
    if (!flush()) {
        result.connection_lost = true;
        return result;
    }
    while (Clock::now() < until) {
        int replies = 0;
        if (!readAvailable(10, result, replies)) {
            result.connection_lost = true;
            break;
        }
        for (int i = 0; i < replies; ++i, buy = !buy) queueOrder(buy, symbol_id);
        result.sent += static_cast<uint64_t>(replies);
        if (!flush()) {
            result.connection_lost = true;
            break;
        }
    }
    return result;
}

}  // namespace bench
