#include "client/RawConnection.hpp"

#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <chrono>

#include "codec/BinaryCodec.hpp"
#include "net/Socket.hpp"

namespace client {

using Clock = std::chrono::steady_clock;

std::vector<uint8_t> encodeFrame(const wire::Message& msg) {
    std::vector<uint8_t> out;
    codec::BinaryCodec::encode(msg, out);
    return out;
}

RawConnection::~RawConnection() { net::closeFd(fd_); }

bool RawConnection::connect(const std::string& host, uint16_t port) {
    net::ignoreSigpipe();
    fd_ = net::connectTcp(host, port);
    return fd_ >= 0;
}

bool RawConnection::sendBytes(const std::vector<uint8_t>& bytes) {
    size_t off = 0;
    while (off < bytes.size()) {
        ssize_t n = net::sendSome(fd_, bytes.data() + off, bytes.size() - off);
        if (n > 0) {
            off += static_cast<size_t>(n);
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            pollfd p{fd_, POLLOUT, 0};
            ::poll(&p, 1, 100);
        } else {
            return false;
        }
    }
    return true;
}

bool RawConnection::send(const wire::Message& msg) { return sendBytes(encodeFrame(msg)); }

bool RawConnection::waitForClose(int timeout_ms) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    uint8_t buf[4096];
    while (!closed_) {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
        if (left.count() <= 0) return false;
        pollfd p{fd_, POLLIN, 0};
        if (::poll(&p, 1, static_cast<int>(left.count())) <= 0) continue;
        ssize_t n = ::recv(fd_, buf, sizeof buf, 0);
        if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) closed_ = true;
    }
    return true;
}

std::optional<wire::Message> RawConnection::read(int timeout_ms) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    wire::Message msg;
    while (!closed_) {
        net::FrameStatus st = reader_.next(msg);
        if (st == net::FrameStatus::Frame || st == net::FrameStatus::BadFrame) return msg;
        if (st == net::FrameStatus::Fatal) return std::nullopt;

        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
        if (left.count() <= 0) return std::nullopt;
        pollfd p{fd_, POLLIN, 0};
        if (::poll(&p, 1, static_cast<int>(left.count())) <= 0) continue;
        auto space = reader_.prepare(64 * 1024);
        ssize_t n = ::recv(fd_, space.first, space.second, 0);
        if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
            closed_ = true;
        } else if (n > 0) {
            reader_.commit(static_cast<size_t>(n));
        }
    }
    return std::nullopt;
}

}  // namespace client
