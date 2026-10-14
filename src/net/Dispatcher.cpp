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
    auto symbols = std::make_shared<codec::SymbolTable>();
    symbols->addAll(list_feed_);
    symbols_ = symbols;
    if (cfg_.record_dispatch_latency) dispatch_latency_.setCapacity(cfg_.dispatch_latency_capacity);
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
    closeFd(text_listen_fd_);
}

bool Dispatcher::open() {
    listen_fd_ = listenTcp(cfg_.port);
    if (listen_fd_ < 0) return false;
    port_ = localPort(listen_fd_);
    poller_.add(listen_fd_, POLLIN);
    if (cfg_.text_port >= 0) {
        text_listen_fd_ = listenTcp(static_cast<uint16_t>(cfg_.text_port));
        if (text_listen_fd_ < 0) return false;
        text_port_ = localPort(text_listen_fd_);
        poller_.add(text_listen_fd_, POLLIN);
    }
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
        if (ev.fd == listen_fd_ || ev.fd == text_listen_fd_) {
            acceptAll(ev.fd, ev.fd == listen_fd_ ? Protocol::Binary : Protocol::Text);
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

void Dispatcher::acceptAll(int listen_fd, Protocol protocol) {
    for (;;) {
        sockaddr_in peer{};
        socklen_t len = sizeof peer;
        int fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&peer), &len);
        if (fd < 0) return;
        setNonBlocking(fd);
        setNoDelay(fd);
        Connection& c = addConnection(fd, peer, ConnState::Open, false, protocol);
        touch(c);
        stats_.accepted.fetch_add(1, std::memory_order_relaxed);
        if (tracing()) log("conn#" + std::to_string(c.conn_id) + " accepted from " + toString(peer) + " (" + nameOf(protocol) + " port)");
    }
}

void Dispatcher::readFrom(Connection& c) {
    auto space = c.codec->prepare(kReadChunk);
    ssize_t n = ::recv(c.fd, space.first, space.second, 0);
    if (n == 0 || (n < 0 && !wouldBlock())) {
        closeConnection(c, n == 0 ? "peer closed it" : "recv error");
        return;
    }
    if (n < 0) return;
    c.codec->commit(static_cast<size_t>(n));
    if (cfg_.record_dispatch_latency) read_time_ = Clock::now();

    Message msg;
    while (c.state != ConnState::Closed) {
        FrameStatus st = c.codec->next(msg);
        if (st == FrameStatus::NeedMore) break;
        stats_.frames_in.fetch_add(1, std::memory_order_relaxed);
        if (st == FrameStatus::Frame) {
            if (tracing()) trace_req_ = who(c) + " " + summary(msg);
            handleFrame(c, msg);
        } else if (st == FrameStatus::BadFrame) {
            if (tracing()) trace_req_ = who(c) + " frame of unknown type";
            touch(c);
            replyError(c, msg.header.req_id, ErrorCode::UnknownType);
        } else {
            // Bad version, oversize length or malformed payload: close (NFR-4).
            const bool bad_version = c.codec->lastError() == codec::DecodeStatus::UnsupportedVersion;
            if (tracing()) trace_req_ = who(c) + (bad_version ? " bad protocol version" : " malformed frame or oversize length");
            if (bad_version) replyError(c, 0, ErrorCode::UnsupportedVersion);
            flushConnection(c);
            stats_.protocol_closes.fetch_add(1, std::memory_order_relaxed);
            closeConnection(c, "protocol error");
        }
    }
}

void Dispatcher::handleFrame(Connection& c, const Message& msg) {
    const FrameHeader& h = msg.header;
    ClientEntry* e = resolveClient(c, h);
    if (e == nullptr) return;
    if (tracing()) trace_req_ = who(c) + " " + summary(msg);  // now with the client's name
    touch(c);
    e->callback_port = h.callback_port;

    switch (h.type) {
        case MsgType::Bid:
        case MsgType::Ask: handleOrder(c, *e, msg); break;
        case MsgType::Cancel: handleCancel(c, *e, msg); break;
        case MsgType::ListFeed:
            if (tracing()) logRequest("LIST_FEED_RSP");
            reply(c, h.req_id, MsgType::ListFeedRsp, list_feed_);
            break;
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
        closeConnection(c, "client_id mismatch");
        return nullptr;
    }
    const bool is_new = h.client_id == 0;
    ClientEntry* e = is_new ? &registry_.create() : registry_.find(h.client_id);
    if (e == nullptr) {
        replyError(c, h.req_id, ErrorCode::UnknownClient);
        return nullptr;
    }
    if (tracing()) {
        log(who(c) + (is_new ? " first frame has client_id 0 -> registered new client " + clientName(e->client_id)
                             : " client_id in header -> bound to existing client " + clientName(e->client_id)));
    }
    bindConnection(*e, c);
    return e;
}

void Dispatcher::bindConnection(ClientEntry& e, Connection& c) {
    Connection* old = findConn(e.conn_id);
    if (old != nullptr && old != &c) closeConnection(*old, "replaced by the client's newer connection");  // newest wins
    Connection* pending_push = findConn(e.push_conn_id);
    if (pending_push != nullptr && pending_push != &c) {
        closeConnection(*pending_push, "push no longer needed, client reconnected");
    }
    e.push_conn_id = 0;
    e.conn_id = c.conn_id;
    c.client = &e;
    if (!c.push) {
        e.peer_ip = ntohl(c.peer.sin_addr.s_addr);
        e.protocol = c.codec->protocol();  // pushes use the client's own protocol
    }
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
    } else if (tracing()) {
        logRequest("validated, pushed to worker " + std::to_string(w) + " queue");
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
    if (!sendToWorker(w, std::move(item))) {
        replyError(c, req, ErrorCode::QueueFull);
    } else if (tracing()) {
        logRequest("owner check ok (order table), worker id from order_id top byte -> worker " +
                   std::to_string(w) + " queue");
    }
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
        if (ClientEntry* prev = registry_.find(owner->second)) {
            if (tracing()) {
                log("UDP endpoint " + toString(endpoint) + " was owned by " + clientName(prev->client_id) +
                    " -> its subscriptions are dropped (newest claimant wins)");
            }
            dropSubscriptions(*prev);
        }
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
    if (tracing()) {
        logRequest("UDP endpoint " + toString(endpoint) + ", subscribe control item to " + workersFor(newly) +
                   ", SUBSCRIBE_ACK " + core::groupSet(newly));
    }
    reply(c, req, MsgType::SubscribeAck, GroupMask{newly});
}

void Dispatcher::handleUnsubscribe(Connection& c, ClientEntry& e, const Message& msg) {
    const uint32_t removed = std::get<GroupMask>(msg.payload).group_mask & e.group_mask;
    if (removed != 0) sendControl(MsgType::Unsubscribe, e.udp_endpoint, removed);
    e.group_mask &= ~removed;
    if (tracing()) {
        logRequest("removed " + core::groupSet(removed) +
                   (removed != 0 ? ", unsubscribe control item to " + workersFor(removed) : " (nothing to remove)"));
    }
    reply(c, msg.header.req_id, MsgType::UnsubscribeAck, GroupMask{removed});
}

void Dispatcher::reply(Connection& c, uint32_t req_id, MsgType type, Payload payload) {
    const uint64_t client_id = c.client != nullptr ? c.client->client_id : 0;
    queueFrame(c, makeMessage(type, req_id, client_id, std::move(payload)));
}

void Dispatcher::replyError(Connection& c, uint32_t req_id, ErrorCode code) {
    if (tracing()) logRequest(std::string("ERROR ") + nameOf(code) + " (never reaches a worker)");
    reply(c, req_id, MsgType::Error, Error{code});
}

bool Dispatcher::sendToWorker(size_t w, WorkItem&& item) {
    // Orders never overtake control items still waiting for queue space.
    if (!pending_control_[w].empty()) return false;
    if (!workers_[w].in->tryPush(std::move(item))) return false;
    if (cfg_.record_dispatch_latency) {
        dispatch_latency_.add(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - read_time_).count()));
    }
    return true;
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
            routeWorkerOutput(msg, w);
        }
    }
}

void Dispatcher::retryPendingControl(size_t w) {
    auto& pending = pending_control_[w];
    while (!pending.empty() && workers_[w].in->tryPush(std::move(pending.front()))) {
        pending.pop_front();
    }
}

void Dispatcher::routeWorkerOutput(Message& msg, size_t worker) {
    uint64_t client_id = msg.header.client_id;
    if (tracing()) trace_out_ = "from worker " + std::to_string(worker) + ": " + summary(msg);
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
        if (owner == order_owner_.end()) {
            if (tracing()) log(trace_out_ + "  -> owner unknown, dropped");
            return;
        }
        client_id = owner->second;
        if (tracing()) trace_out_ += ", owner from order table = " + clientName(client_id);
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
        if (tracing()) log(trace_out_ + "  -> " + who(*c));
        queueFrame(*c, msg);
        return;
    }
    if (const auto* ack = std::get_if<OrderAck>(&msg.payload)) {
        e.mailbox.merge(*ack);
    } else if (const auto* cancel = std::get_if<CancelAck>(&msg.payload)) {
        e.mailbox.merge(OrderAck{cancel->order_id, cancel->status, 0, {}});
    } else {
        if (tracing()) log(trace_out_ + "  -> requester's connection is gone, dropped");
        return;  // an error for a request whose connection is gone has no one to tell
    }
    if (tracing()) {
        log(trace_out_ + "  -> " + (c != nullptr ? "connection is draining" : "no live connection") +
            ", parked in mailbox (" + std::to_string(e.mailbox.size()) + " order(s) pending)");
    }
    startPush(e);
}

void Dispatcher::flushMailbox(ClientEntry& e, Connection& c) {
    if (e.mailbox.empty()) return;
    if (tracing()) {
        log(clientName(e.client_id) + " mailbox: " + std::to_string(e.mailbox.size()) +
            " merged ORDER_UPDATE(s) flushed -> " + who(c));
    }
    for (OrderAck& update : e.mailbox.takeAll()) {
        queueFrame(c, makeMessage(MsgType::OrderUpdate, 0, e.client_id, std::move(update)));
    }
}

// ===================================================================== step 3

void Dispatcher::queueFrame(Connection& c, const Message& msg) {
    if (c.state == ConnState::Closed || c.state == ConnState::Connecting) return;
    c.codec->encode(msg, c.wbuf);
    stats_.frames_out.fetch_add(1, std::memory_order_relaxed);
    if (c.wbuf.size() - c.wpos > cfg_.write_buffer_cap) {
        closeConnection(c, "write buffer over cap (slow reader)");  // Section 3.2 backpressure
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
            closeConnection(c, "send error");
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
    if (tracing()) {
        log(who(c) + " idle for " + std::to_string(cfg_.idle_timeout.count()) + " ms -> DISCONNECT, draining for " +
            std::to_string(cfg_.drain_grace.count()) + " ms");
    }
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
        if (tracing()) log("push connect to " + toString(addr) + " for " + clientName(e.client_id) + " failed");
        return;  // no retry: next update for this client tries again
    }
    Connection& c = addConnection(fd, addr, in_progress ? ConnState::Connecting : ConnState::Open,
                                  true, e.protocol);
    c.client = &e;
    e.push_conn_id = c.conn_id;
    if (tracing()) {
        log(clientName(e.client_id) + " has pending updates -> server opens conn#" + std::to_string(c.conn_id) +
            " to its callback port " + toString(addr));
    }
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
        closeConnection(c, "push connect refused, mailbox kept");
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
        closeConnection(c, "push connected but the client is already back");
        return;
    }
    if (tracing()) log(who(c) + " push connection established -> becomes the client's live session");
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
        const bool connecting = c->state == ConnState::Connecting;
        if (connecting) {
            stats_.push_failed.fetch_add(1, std::memory_order_relaxed);
        } else {
            flushConnection(*c);
        }
        closeConnection(*c, connecting ? "push connect timed out, mailbox kept" : "drain grace over");
    }
    timed_list_.resize(keep);
}

// ========================================================== connection lifetime

Connection& Dispatcher::addConnection(int fd, const sockaddr_in& peer, ConnState state, bool push,
                                      Protocol protocol) {
    const uint64_t id = next_conn_id_++;
    auto conn = std::make_unique<Connection>(id, fd, peer, state, push,
                                             makeStreamCodec(protocol, symbols_, cfg_.max_frame_len));
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

void Dispatcher::closeConnection(Connection& c, const char* reason) {
    if (c.state == ConnState::Closed) return;
    if (tracing() && reason != nullptr) log(who(c) + " closed: " + reason);
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

// ================================================================ trace helpers

std::string Dispatcher::who(const Connection& c) const {
    return "conn#" + std::to_string(c.conn_id) + " " + (c.client != nullptr ? clientName(c.client->client_id) : "(new)");
}

std::string Dispatcher::workersFor(uint32_t mask) const {
    std::string s;
    for (size_t w = 0; w < workers_.size(); ++w) {
        if (!(mask & workers_[w].group_mask)) continue;
        s += (s.empty() ? "worker " : ",") + std::to_string(w);
    }
    return s.empty() ? "no worker" : s;
}

std::string Dispatcher::summary(const Message& m) const {
    std::string s = std::string(nameOf(m.header.type));
    if (m.header.req_id != 0) s += " req=" + std::to_string(m.header.req_id);
    if (const auto* o = std::get_if<OrderRequest>(&m.payload)) {
        s += " " + symbols_->nameOf(o->symbol_id) + " " + std::to_string(o->volume) + " @ " + std::to_string(o->price);
    } else if (const auto* c = std::get_if<CancelRequest>(&m.payload)) {
        s += " order " + std::to_string(c->order_id);
    } else if (const auto* sub = std::get_if<SubscribeRequest>(&m.payload)) {
        s += " groups " + core::groupSet(sub->group_mask) + " udp port " + std::to_string(sub->udp_port);
    } else if (const auto* g = std::get_if<GroupMask>(&m.payload)) {
        s += " groups " + core::groupSet(g->group_mask);
    } else if (const auto* a = std::get_if<OrderAck>(&m.payload)) {
        s += " order " + std::to_string(a->order_id) + " " + nameOf(a->status) + " remaining " +
             std::to_string(a->remaining_vol);
        if (!a->trades.empty()) s += " (" + std::to_string(a->trades.size()) + " trade(s))";
    } else if (const auto* ca = std::get_if<CancelAck>(&m.payload)) {
        s += " order " + std::to_string(ca->order_id) + " " + nameOf(ca->status);
    } else if (const auto* e = std::get_if<Error>(&m.payload)) {
        s += std::string(" ") + nameOf(e->code);
    }
    return s;
}

}  // namespace net
