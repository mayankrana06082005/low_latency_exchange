#include "client/ClientSession.hpp"

#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <chrono>
#include <thread>
#include <vector>

#include "codec/BinaryCodec.hpp"
#include "net/Socket.hpp"

namespace client {

using namespace wire;
using Clock = std::chrono::steady_clock;

namespace {

int msUntil(Clock::time_point deadline) {
    auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    return left.count() > 0 ? static_cast<int>(left.count()) : 0;
}

bool wouldBlock() { return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR; }

}  // namespace

ClientSession::ClientSession(ClientConfig cfg) : cfg_(std::move(cfg)) { net::ignoreSigpipe(); }

ClientSession::~ClientSession() {
    net::closeFd(fd_);
    net::closeFd(listen_fd_);
}

bool ClientSession::enablePush(uint16_t port) {
    listen_fd_ = net::listenTcp(port);
    if (listen_fd_ < 0) return false;
    callback_port_ = net::localPort(listen_fd_);
    return true;
}

std::optional<Message> ClientSession::request(MsgType type, Payload payload, int timeout_ms) {
    if (!ensureConnected()) return std::nullopt;
    const uint32_t req_id = next_req_id_++;
    Message msg = makeMessage(type, req_id, client_id_, std::move(payload));
    msg.header.callback_port = callback_port_;
    trace(TraceEvent::Sent, &msg);  // before sending, so it prints before the server reacts
    if (!sendFrame(msg)) {
        dropConnection();
        return std::nullopt;
    }
    return awaitResponse(req_id, timeout_ms);
}

std::optional<Message> ClientSession::bid(uint16_t symbol_id, Price price, Volume volume) {
    return request(MsgType::Bid, OrderRequest{symbol_id, price, volume});
}

std::optional<Message> ClientSession::ask(uint16_t symbol_id, Price price, Volume volume) {
    return request(MsgType::Ask, OrderRequest{symbol_id, price, volume});
}

std::optional<Message> ClientSession::cancel(uint64_t order_id) {
    return request(MsgType::Cancel, CancelRequest{order_id});
}

std::optional<Message> ClientSession::listFeed() { return request(MsgType::ListFeed, Empty{}); }

std::optional<Message> ClientSession::subscribe(uint16_t udp_port, uint32_t group_mask) {
    return request(MsgType::Subscribe, SubscribeRequest{udp_port, group_mask});
}

std::optional<Message> ClientSession::unsubscribe(uint32_t group_mask) {
    return request(MsgType::Unsubscribe, GroupMask{group_mask});
}

void ClientSession::pump(int timeout_ms) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    std::optional<Message> unused;
    do {
        pumpOnce(msUntil(deadline), 0, unused);
    } while (msUntil(deadline) > 0);
}

bool ClientSession::ensureConnected() {
    if (fd_ >= 0) {
        // Read anything already waiting (a DISCONNECT, or the close that follows it)
        // so we do not send a request into a connection the server has dropped.
        std::optional<Message> unused;
        pumpOnce(0, 0, unused);
    }
    if (fd_ >= 0 && !server_closing_) return true;
    dropConnection();  // after DISCONNECT the client closes first and reconnects
    int fd = net::connectTcp(cfg_.host, cfg_.port);
    if (fd < 0) return false;
    adopt(fd);
    ++stats_.connects;
    trace(TraceEvent::Connected);
    return true;
}

void ClientSession::dropConnection() {
    if (fd_ >= 0) trace(TraceEvent::Closed);
    net::closeFd(fd_);
    stream_.reset();
    server_closing_ = false;
}

void ClientSession::adopt(int fd) {
    fd_ = fd;
    stream_ = net::makeStreamCodec(cfg_.protocol, symbols_);
}

bool ClientSession::sendFrame(const Message& msg) {
    std::vector<uint8_t> buf;
    stream_->encode(msg, buf);
    size_t off = 0;
    while (off < buf.size()) {
        ssize_t n = net::sendSome(fd_, buf.data() + off, buf.size() - off);
        if (n > 0) {
            off += static_cast<size_t>(n);
        } else if (n < 0 && wouldBlock()) {
            pollfd p{fd_, POLLOUT, 0};
            ::poll(&p, 1, 100);
        } else {
            return false;
        }
    }
    return true;
}

std::optional<Message> ClientSession::awaitResponse(uint32_t req_id, int timeout_ms) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    std::optional<Message> got;
    while (!got && msUntil(deadline) > 0) {
        if (!pumpOnce(msUntil(deadline), req_id, got)) break;  // lost: outcome unknown
    }
    return got;
}

bool ClientSession::pumpOnce(int timeout_ms, uint32_t want_req, std::optional<Message>& got) {
    pollfd fds[2];
    nfds_t n = 0;
    if (fd_ >= 0) fds[n++] = pollfd{fd_, POLLIN, 0};
    if (listen_fd_ >= 0) fds[n++] = pollfd{listen_fd_, POLLIN, 0};
    if (n == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
        return false;
    }
    if (::poll(fds, n, timeout_ms) <= 0) return true;

    bool session_ok = true;
    for (nfds_t i = 0; i < n; ++i) {
        if (fds[i].revents == 0) continue;
        if (fds[i].fd == listen_fd_) {
            acceptPush();
        } else if (fds[i].fd == fd_ && !readSocket(want_req, got)) {
            dropConnection();
            session_ok = false;
        }
    }
    return session_ok;
}

bool ClientSession::readSocket(uint32_t want_req, std::optional<Message>& got) {
    auto space = stream_->prepare(64 * 1024);
    ssize_t n = ::recv(fd_, space.first, space.second, 0);
    if (n == 0) return false;
    if (n < 0) return wouldBlock();
    stream_->commit(static_cast<size_t>(n));

    Message msg;
    for (;;) {
        net::FrameStatus st = stream_->next(msg);
        if (st == net::FrameStatus::NeedMore) return true;
        if (st == net::FrameStatus::Fatal) return false;
        if (st == net::FrameStatus::Frame) handleFrame(msg, want_req, got);
    }
}

void ClientSession::handleFrame(Message& msg, uint32_t want_req, std::optional<Message>& got) {
    trace(TraceEvent::Received, &msg);
    const FrameHeader& h = msg.header;
    if (client_id_ == 0 && h.client_id != 0) client_id_ = h.client_id;  // assigned by server

    if (h.type == MsgType::OrderUpdate) {
        ++stats_.updates;
        if (on_update_) on_update_(std::get<OrderAck>(msg.payload));
        return;
    }
    if (h.type == MsgType::Disconnect) {
        ++stats_.disconnects_received;
        server_closing_ = true;  // keep reading until the server closes; reconnect lazily
        return;
    }
    if (h.type == MsgType::ListFeedRsp) symbols_->addAll(std::get<ListFeedRsp>(msg.payload));
    if (h.type == MsgType::Error && std::get<Error>(msg.payload).code == ErrorCode::UnknownClient) {
        client_id_ = 0;  // D26: the server forgot us (e.g. restart); start over
    }
    if (want_req != 0 && h.req_id == want_req) {
        got = std::move(msg);
        return;
    }
    // A late response to an earlier request still tells us the order's state.
    if (const auto* ack = std::get_if<OrderAck>(&msg.payload)) {
        ++stats_.updates;
        if (on_update_) on_update_(*ack);
    }
}

void ClientSession::acceptPush() {
    int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) return;
    net::setNonBlocking(fd);
    net::setNoDelay(fd);
    // The server-initiated connection becomes our live session.
    dropConnection();
    adopt(fd);
    ++stats_.pushes_accepted;
    trace(TraceEvent::PushAccepted);
}

void ClientSession::trace(TraceEvent event, const Message* msg) {
    if (on_trace_) on_trace_(event, msg);
}

}  // namespace client
