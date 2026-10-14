#include "net/Dispatcher.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#include "codec/BinaryCodec.hpp"
#include "net/Socket.hpp"

namespace net {

using namespace wire;
using core::WorkItem;

namespace {

constexpr size_t kReadChunk = 64 * 1024;

bool isTerminal(OrderStatus s) { return s == OrderStatus::Filled || s == OrderStatus::Cancelled; }

bool wouldBlock() { return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR; }

}  // namespace

Dispatcher::Dispatcher(DispatcherConfig cfg, core::MarketConfig market,
                       std::vector<WorkerLink> workers)
    : cfg_(cfg), market_(std::move(market)), workers_(std::move(workers)),
      pending_control_(workers_.size()), symbol_worker_(65536, -1),
      list_feed_(market_.toListFeed()) {
    for (const core::GroupConfig& g : market_.groups) {
        const uint32_t bit = 1u << g.group_id;
        market_mask_ |= bit;
        for (size_t w = 0; w < workers_.size(); ++w) {
            if (!(workers_[w].group_mask & bit)) continue;
            for (const core::SymbolConfig& s : g.symbols) {
                symbol_worker_[s.symbol_id] = static_cast<int16_t>(w);
            }
        }
    }
}

Dispatcher::~Dispatcher() {
    for (auto& entry : conns_) ::close(entry.second->fd);
    closeFd(listen_fd_);
}

bool Dispatcher::open() {
    listen_fd_ = listenTcp(cfg_.port);
    if (listen_fd_ < 0) return false;
    port_ = localPort(listen_fd_);
    poller_.add(listen_fd_, POLLIN);
    return true;
}

void Dispatcher::run(const std::atomic<bool>& running) {
    while (running.load(std::memory_order_relaxed)) runOnce();
}

void Dispatcher::runOnce() {
    now_ = Clock::now();
    pollSockets();
    drainWorkers();
    flushWrites();
    checkIdle();
    checkDeadlines();
    sweepClosed();
}

// ===================================================================== step 1

void Dispatcher::pollSockets() {
    poller_.wait(0, events_);
    for (const PollEvent& ev : events_) {
        if (ev.fd == listen_fd_) {
            acceptAll();
            continue;
        }
        auto it = fd_conn_.find(ev.fd);
        Connection* c = it == fd_conn_.end() ? nullptr : findConn(it->second);
        if (c == nullptr || c->state == ConnState::Closed) continue;
        if (c->state == ConnState::Connecting) {
            finishConnect(*c);
        } else {
            readFrom(*c);
        }
    }
}

void Dispatcher::acceptAll() {
    for (;;) {
        sockaddr_in peer{};
        socklen_t len = sizeof peer;
        int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &len);
        if (fd < 0) return;
        setNonBlocking(fd);
        setNoDelay(fd);
        touch(addConnection(fd, peer, ConnState::Open, false));
        stats_.accepted.fetch_add(1, std::memory_order_relaxed);
    }
}

void Dispatcher::readFrom(Connection& c) {
    auto space = c.reader.prepare(kReadChunk);
    ssize_t n = ::recv(c.fd, space.first, space.second, 0);
    if (n == 0 || (n < 0 && !wouldBlock())) {
        closeConnection(c);
        return;
    }
    if (n < 0) return;
    c.reader.commit(static_cast<size_t>(n));

    Message msg;
    while (c.state != ConnState::Closed) {
        FrameStatus st = c.reader.next(msg);
        if (st == FrameStatus::NeedMore) break;
        stats_.frames_in.fetch_add(1, std::memory_order_relaxed);
        if (st == FrameStatus::Frame) {
            handleFrame(c, msg);
        } else if (st == FrameStatus::BadFrame) {
            touch(c);
            replyError(c, msg.header.req_id, ErrorCode::UnknownType);
        } else {
            // Bad version, oversize length or malformed payload: close (NFR-4).
            if (c.reader.lastError() == codec::DecodeStatus::UnsupportedVersion) {
                replyError(c, 0, ErrorCode::UnsupportedVersion);
            }
            flushConnection(c);
            stats_.protocol_closes.fetch_add(1, std::memory_order_relaxed);
            closeConnection(c);
        }
    }
}

void Dispatcher::handleFrame(Connection& c, const Message& msg) {
    const FrameHeader& h = msg.header;
    ClientEntry* e = resolveClient(c, h);
    if (e == nullptr) return;
    touch(c);
    e->callback_port = h.callback_port;

    switch (h.type) {
        case MsgType::Bid:
        case MsgType::Ask: handleOrder(c, *e, msg); break;
        case MsgType::Cancel: handleCancel(c, *e, msg); break;
        case MsgType::ListFeed: reply(c, h.req_id, MsgType::ListFeedRsp, list_feed_); break;
        case MsgType::Subscribe: handleSubscribe(c, *e, msg); break;
        case MsgType::Unsubscribe: handleUnsubscribe(c, *e, msg); break;
        default: replyError(c, h.req_id, ErrorCode::UnknownType); break;  // server-side type
    }
}

ClientEntry* Dispatcher::resolveClient(Connection& c, const FrameHeader& h) {
    if (c.client != nullptr) {
        // Bound connection: one integer compare, no lookup. 0 means "the bound id".
        if (h.client_id == 0 || h.client_id == c.client->client_id) return c.client;
        replyError(c, h.req_id, ErrorCode::ClientMismatch);
        flushConnection(c);
        closeConnection(c);
        return nullptr;
    }
    ClientEntry* e = h.client_id == 0 ? &registry_.create() : registry_.find(h.client_id);
    if (e == nullptr) {
        replyError(c, h.req_id, ErrorCode::UnknownClient);
        return nullptr;
    }
    bindConnection(*e, c);
    return e;
}

void Dispatcher::bindConnection(ClientEntry& e, Connection& c) {
    Connection* old = findConn(e.conn_id);
    if (old != nullptr && old != &c) closeConnection(*old);  // newest wins
    Connection* pending_push = findConn(e.push_conn_id);
    if (pending_push != nullptr && pending_push != &c) closeConnection(*pending_push);
    e.push_conn_id = 0;
    e.conn_id = c.conn_id;
    c.client = &e;
    if (!c.push) e.peer_ip = ntohl(c.peer.sin_addr.s_addr);
    flushMailbox(e, c);
}

// =================================================================== requests

void Dispatcher::handleOrder(Connection& c, ClientEntry& e, const Message& msg) {
    const auto& o = std::get<OrderRequest>(msg.payload);
    const uint32_t req = msg.header.req_id;
    if (o.price == 0 || o.volume == 0) {
        replyError(c, req, ErrorCode::InvalidField);
        return;
    }
    const int16_t w = symbol_worker_[o.symbol_id];
    if (w < 0) {
        replyError(c, req, ErrorCode::UnknownSymbol);
        return;
    }
    WorkItem item;
    item.type = msg.header.type;
    item.client_id = e.client_id;
    item.req_id = req;
    item.symbol_id = o.symbol_id;
    item.price = o.price;
    item.volume = o.volume;
    if (!sendToWorker(static_cast<size_t>(w), std::move(item))) {
        replyError(c, req, ErrorCode::QueueFull);
    }
}

void Dispatcher::handleCancel(Connection& c, ClientEntry& e, const Message& msg) {
    const uint64_t order_id = std::get<CancelRequest>(msg.payload).order_id;
    const uint32_t req = msg.header.req_id;
    auto owner = order_owner_.find(order_id);
    const size_t w = static_cast<size_t>(order_id >> 56);  // D9: worker id in the top byte
    if (owner == order_owner_.end() || w >= workers_.size()) {
        replyError(c, req, ErrorCode::OrderNotFound);  // unknown or already terminal (D19)
        return;
    }
    if (owner->second != e.client_id) {
        replyError(c, req, ErrorCode::NotOwner);
        return;
    }
    WorkItem item;
    item.type = MsgType::Cancel;
    item.client_id = e.client_id;
    item.req_id = req;
    item.order_id = order_id;
    if (!sendToWorker(w, std::move(item))) replyError(c, req, ErrorCode::QueueFull);
}

void Dispatcher::handleSubscribe(Connection& c, ClientEntry& e, const Message& msg) {
    const auto& s = std::get<SubscribeRequest>(msg.payload);
    const uint32_t req = msg.header.req_id;
    if (s.group_mask & ~market_mask_) {
        replyError(c, req, ErrorCode::UnknownGroup);
        return;
    }
    if (s.udp_port == 0) {
        replyError(c, req, ErrorCode::InvalidField);
        return;
    }
    const sockaddr_in endpoint = makeAddr(ntohl(c.peer.sin_addr.s_addr), s.udp_port);
    const uint64_t key = endpointKey(endpoint);

    // An endpoint belongs to one client: a newer claimant evicts the old owner.
    auto owner = endpoint_owner_.find(key);
    if (owner != endpoint_owner_.end() && owner->second != e.client_id) {
        if (ClientEntry* prev = registry_.find(owner->second)) dropSubscriptions(*prev);
    }

    const uint32_t newly = s.group_mask & ~e.group_mask;
    const uint32_t total = e.group_mask | s.group_mask;
    if (e.has_udp && endpointKey(e.udp_endpoint) != key) {
        dropSubscriptions(e);  // one endpoint per client: move all groups to the new one
        sendControl(MsgType::Subscribe, endpoint, total);
    } else {
        sendControl(MsgType::Subscribe, endpoint, newly);
    }
    e.has_udp = true;
    e.udp_endpoint = endpoint;
    e.group_mask = total;
    endpoint_owner_[key] = e.client_id;
    reply(c, req, MsgType::SubscribeAck, GroupMask{newly});
}

void Dispatcher::handleUnsubscribe(Connection& c, ClientEntry& e, const Message& msg) {
    const uint32_t removed = std::get<GroupMask>(msg.payload).group_mask & e.group_mask;
    if (removed != 0) sendControl(MsgType::Unsubscribe, e.udp_endpoint, removed);
    e.group_mask &= ~removed;
    reply(c, msg.header.req_id, MsgType::UnsubscribeAck, GroupMask{removed});
}

void Dispatcher::reply(Connection& c, uint32_t req_id, MsgType type, Payload payload) {
    const uint64_t client_id = c.client != nullptr ? c.client->client_id : 0;
    queueFrame(c, makeMessage(type, req_id, client_id, std::move(payload)));
}

void Dispatcher::replyError(Connection& c, uint32_t req_id, ErrorCode code) {
    reply(c, req_id, MsgType::Error, Error{code});
}

bool Dispatcher::sendToWorker(size_t w, WorkItem&& item) {
    // Orders never overtake control items still waiting for queue space.
    if (!pending_control_[w].empty()) return false;
    return workers_[w].in->tryPush(std::move(item));
}

void Dispatcher::sendControl(MsgType type, const sockaddr_in& endpoint, uint32_t mask) {
    for (size_t w = 0; w < workers_.size(); ++w) {
        const uint32_t worker_mask = mask & workers_[w].group_mask;
        if (worker_mask == 0) continue;
        WorkItem item;
        item.type = type;
        item.endpoint = endpoint;
        item.group_mask = worker_mask;
        // Control items must not be lost; park them if the queue is full.
        if (!pending_control_[w].empty() || !workers_[w].in->tryPush(std::move(item))) {
            pending_control_[w].push_back(item);
        }
    }
}

void Dispatcher::dropSubscriptions(ClientEntry& e) {
    if (!e.has_udp) return;
    if (e.group_mask != 0) sendControl(MsgType::Unsubscribe, e.udp_endpoint, e.group_mask);
    auto owner = endpoint_owner_.find(endpointKey(e.udp_endpoint));
    if (owner != endpoint_owner_.end() && owner->second == e.client_id) endpoint_owner_.erase(owner);
    e.has_udp = false;
    e.group_mask = 0;
}

// ===================================================================== step 2

void Dispatcher::drainWorkers() {
    Message msg;
    for (size_t w = 0; w < workers_.size(); ++w) {
        retryPendingControl(w);
        for (int i = 0; i < cfg_.items_per_queue && workers_[w].out->tryPop(msg); ++i) {
            routeWorkerOutput(msg);
        }
    }
}

void Dispatcher::retryPendingControl(size_t w) {
    auto& pending = pending_control_[w];
    while (!pending.empty() && workers_[w].in->tryPush(std::move(pending.front()))) {
        pending.pop_front();
    }
}

void Dispatcher::routeWorkerOutput(Message& msg) {
    uint64_t client_id = msg.header.client_id;
    int resting_delta = 0;

    if (msg.header.type == MsgType::OrderAck) {
        const auto& ack = std::get<OrderAck>(msg.payload);
        if (!isTerminal(ack.status)) {
            order_owner_[ack.order_id] = client_id;
            resting_delta = 1;
        }
    } else if (msg.header.type == MsgType::OrderUpdate) {
        // Workers do not know clients (D7): resolve the owner from order_id.
        const auto& upd = std::get<OrderAck>(msg.payload);
        auto owner = order_owner_.find(upd.order_id);
        if (owner == order_owner_.end()) return;
        client_id = owner->second;
        if (isTerminal(upd.status)) {
            order_owner_.erase(owner);
            resting_delta = -1;
        }
    } else if (msg.header.type == MsgType::CancelAck) {
        if (order_owner_.erase(std::get<CancelAck>(msg.payload).order_id) != 0) resting_delta = -1;
    }

    ClientEntry* e = registry_.find(client_id);
    if (e == nullptr) return;
    e->resting_count += resting_delta;
    deliver(*e, msg);
}

void Dispatcher::deliver(ClientEntry& e, Message& msg) {
    msg.header.client_id = e.client_id;
    msg.header.callback_port = 0;
    Connection* c = liveConnection(e);
    const bool unsolicited = msg.header.req_id == 0;
    // A draining connection still answers requests, but new unsolicited updates
    // go to the mailbox so they cannot race the close.
    if (c != nullptr && (c->state == ConnState::Open || !unsolicited)) {
        queueFrame(*c, msg);
        return;
    }
    if (const auto* ack = std::get_if<OrderAck>(&msg.payload)) {
        e.mailbox.merge(*ack);
    } else if (const auto* cancel = std::get_if<CancelAck>(&msg.payload)) {
        e.mailbox.merge(OrderAck{cancel->order_id, cancel->status, 0, {}});
    } else {
        return;  // an error for a request whose connection is gone has no one to tell
    }
    startPush(e);
}

void Dispatcher::flushMailbox(ClientEntry& e, Connection& c) {
    if (e.mailbox.empty()) return;
    for (OrderAck& update : e.mailbox.takeAll()) {
        queueFrame(c, makeMessage(MsgType::OrderUpdate, 0, e.client_id, std::move(update)));
    }
}

// ===================================================================== step 3

void Dispatcher::queueFrame(Connection& c, const Message& msg) {
    if (c.state == ConnState::Closed || c.state == ConnState::Connecting) return;
    codec::BinaryCodec::encode(msg, c.wbuf);
    stats_.frames_out.fetch_add(1, std::memory_order_relaxed);
    if (c.wbuf.size() - c.wpos > cfg_.write_buffer_cap) {
        closeConnection(c);  // slow reader (Section 3.2 backpressure)
        return;
    }
    if (!c.in_write_list) {
        c.in_write_list = true;
        write_list_.push_back(c.conn_id);
    }
    touch(c);  // a frame sent to the client is activity too
}

void Dispatcher::flushWrites() {
    size_t keep = 0;
    for (uint64_t id : write_list_) {
        Connection* c = findConn(id);
        if (c == nullptr) continue;
        if (c->state == ConnState::Closed || flushConnection(*c)) {
            c->in_write_list = false;
            continue;
        }
        write_list_[keep++] = id;
    }
    write_list_.resize(keep);
}

bool Dispatcher::flushConnection(Connection& c) {
    while (c.wpos < c.wbuf.size()) {
        ssize_t n = sendSome(c.fd, c.wbuf.data() + c.wpos, c.wbuf.size() - c.wpos);
        if (n > 0) {
            c.wpos += static_cast<size_t>(n);
        } else if (n < 0 && wouldBlock()) {
            return false;  // kernel buffer full; retry next iteration
        } else {
            closeConnection(c);
            return true;
        }
    }
    c.wbuf.clear();
    c.wpos = 0;
    return true;
}

// ===================================================================== step 4

void Dispatcher::touch(Connection& c) {
    if (cfg_.idle_policy != IdlePolicy::Fixed || c.state != ConnState::Open) return;
    c.idle_deadline = now_ + cfg_.idle_timeout;
    if (c.in_idle_list) {
        idle_list_.splice(idle_list_.end(), idle_list_, c.idle_it);  // O(1) move to back
    } else {
        c.idle_it = idle_list_.insert(idle_list_.end(), c.conn_id);
        c.in_idle_list = true;
    }
}

void Dispatcher::untrackIdle(Connection& c) {
    if (!c.in_idle_list) return;
    idle_list_.erase(c.idle_it);
    c.in_idle_list = false;
}

void Dispatcher::checkIdle() {
    while (!idle_list_.empty()) {
        Connection* c = findConn(idle_list_.front());
        if (c == nullptr) {
            idle_list_.pop_front();
            continue;
        }
        if (c->idle_deadline > now_) break;  // front is the earliest deadline
        beginDrain(*c);
    }
}

void Dispatcher::beginDrain(Connection& c) {
    untrackIdle(c);
    c.state = ConnState::Draining;
    c.deadline = now_ + cfg_.drain_grace;
    timed_list_.push_back(c.conn_id);
    const uint64_t client_id = c.client != nullptr ? c.client->client_id : 0;
    queueFrame(c, makeMessage(MsgType::Disconnect, 0, client_id, Disconnect{DisconnectReason::Idle}));
    stats_.disconnects_sent.fetch_add(1, std::memory_order_relaxed);
}

// ===================================================================== step 5

void Dispatcher::startPush(ClientEntry& e) {
    if (e.callback_port == 0 || e.peer_ip == 0 || e.push_conn_id != 0) return;
    if (liveConnection(e) != nullptr) return;
    stats_.push_attempts.fetch_add(1, std::memory_order_relaxed);
    const sockaddr_in addr = makeAddr(e.peer_ip, e.callback_port);
    bool in_progress = false;
    int fd = connectTcpNonBlocking(addr, in_progress);
    if (fd < 0) {
        stats_.push_failed.fetch_add(1, std::memory_order_relaxed);
        return;  // no retry: next update for this client tries again
    }
    Connection& c = addConnection(fd, addr, in_progress ? ConnState::Connecting : ConnState::Open, true);
    c.client = &e;
    e.push_conn_id = c.conn_id;
    if (!in_progress) {
        onPushConnected(c);
        return;
    }
    c.deadline = now_ + cfg_.push_timeout;
    timed_list_.push_back(c.conn_id);
}

void Dispatcher::finishConnect(Connection& c) {
    int err = 0;
    socklen_t len = sizeof err;
    if (getsockopt(c.fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0) {
        stats_.push_failed.fetch_add(1, std::memory_order_relaxed);
        closeConnection(c);  // mailbox is kept
        return;
    }
    onPushConnected(c);
}

void Dispatcher::onPushConnected(Connection& c) {
    c.state = ConnState::Open;
    poller_.modify(c.fd, POLLIN);
    ClientEntry& e = *c.client;
    e.push_conn_id = 0;
    stats_.push_connected.fetch_add(1, std::memory_order_relaxed);
    if (liveConnection(e) != nullptr) {
        closeConnection(c);  // the client came back on its own meanwhile
        return;
    }
    // The push connection becomes the client's live session (D13).
    bindConnection(e, c);
    touch(c);
}

void Dispatcher::checkDeadlines() {
    size_t keep = 0;
    for (uint64_t id : timed_list_) {
        Connection* c = findConn(id);
        if (c == nullptr) continue;
        const bool timed = c->state == ConnState::Connecting || c->state == ConnState::Draining;
        if (!timed) continue;
        if (c->deadline > now_) {
            timed_list_[keep++] = id;
            continue;
        }
        if (c->state == ConnState::Connecting) {
            stats_.push_failed.fetch_add(1, std::memory_order_relaxed);
        } else {
            flushConnection(*c);
        }
        closeConnection(*c);
    }
    timed_list_.resize(keep);
}

// ========================================================== connection lifetime

Connection& Dispatcher::addConnection(int fd, const sockaddr_in& peer, ConnState state, bool push) {
    const uint64_t id = next_conn_id_++;
    auto conn = std::make_unique<Connection>(id, fd, peer, state, push, cfg_.max_frame_len);
    Connection& ref = *conn;
    conns_.emplace(id, std::move(conn));
    fd_conn_[fd] = id;
    poller_.add(fd, state == ConnState::Connecting ? POLLOUT : POLLIN);
    return ref;
}

Connection* Dispatcher::findConn(uint64_t conn_id) {
    if (conn_id == 0) return nullptr;
    auto it = conns_.find(conn_id);
    return it == conns_.end() ? nullptr : it->second.get();
}

Connection* Dispatcher::liveConnection(ClientEntry& e) {
    Connection* c = findConn(e.conn_id);
    if (c == nullptr) return nullptr;
    return c->state == ConnState::Open || c->state == ConnState::Draining ? c : nullptr;
}

void Dispatcher::closeConnection(Connection& c) {
    if (c.state == ConnState::Closed) return;
    c.state = ConnState::Closed;
    untrackIdle(c);
    close_list_.push_back(c.conn_id);
}

void Dispatcher::sweepClosed() {
    for (uint64_t id : close_list_) {
        auto it = conns_.find(id);
        if (it == conns_.end()) continue;
        Connection& c = *it->second;
        poller_.remove(c.fd);
        fd_conn_.erase(c.fd);
        ::close(c.fd);
        // Closing never touches subscriptions or resting orders (Section 7).
        ClientEntry* e = c.client;
        bool lost_session = false;
        if (e != nullptr) {
            lost_session = e->conn_id == id;
            if (lost_session) e->conn_id = 0;
            if (e->push_conn_id == id) e->push_conn_id = 0;
        }
        conns_.erase(it);
        // Updates parked while the session was draining still need a push.
        if (lost_session && !e->mailbox.empty()) startPush(*e);
    }
    close_list_.clear();
}

}  // namespace net
