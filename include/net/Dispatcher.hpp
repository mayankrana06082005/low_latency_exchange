#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <unordered_map>
#include <vector>

#include "codec/SymbolTable.hpp"
#include "core/LatencyRecorder.hpp"
#include "core/MarketConfig.hpp"
#include "core/ServerTrace.hpp"
#include "core/SpscQueue.hpp"
#include "core/WorkItem.hpp"
#include "net/ClientRegistry.hpp"
#include "net/Connection.hpp"
#include "net/Poller.hpp"

namespace net {

enum class IdlePolicy { Never, Fixed };  // D23

struct DispatcherConfig {
    uint16_t port = 9000;  // binary protocol; 0 = pick a free port
    int text_port = -1;    // HTTP/1.1 + JSON baseline; -1 = disabled, 0 = pick a free port
    IdlePolicy idle_policy = IdlePolicy::Fixed;
    std::chrono::milliseconds idle_timeout{1000};  // x (D14)
    std::chrono::milliseconds drain_grace{100};    // g
    std::chrono::milliseconds push_timeout{1000};
    uint32_t max_frame_len = wire::DEFAULT_MAX_FRAME_LEN;
    size_t write_buffer_cap = 4 * 1024 * 1024;
    int items_per_queue = 64;  // out-queue budget per loop iteration
    // Benchmark 3: time from end of socket read to worker-queue push, per order/cancel.
    bool record_dispatch_latency = false;
    size_t dispatch_latency_capacity = 4 * 1024 * 1024;
    core::ServerTrace* trace = nullptr;  // server-side event log; nullptr = off
};

struct DispatcherStats {
    std::atomic<uint64_t> accepted{0};
    std::atomic<uint64_t> frames_in{0};
    std::atomic<uint64_t> frames_out{0};
    std::atomic<uint64_t> disconnects_sent{0};
    std::atomic<uint64_t> push_attempts{0};
    std::atomic<uint64_t> push_connected{0};
    std::atomic<uint64_t> push_failed{0};
    std::atomic<uint64_t> protocol_closes{0};
};

struct WorkerLink {
    core::SpscQueue<core::WorkItem>* in;
    core::SpscQueue<core::WorkerOutput>* out;
    uint32_t group_mask;  // groups owned by this worker
};

// The single network thread: accepts and reads TCP, validates and routes to
// workers, drains worker replies, writes, and runs idle timers and push
// connects (Section 3.2).
class Dispatcher {
public:
    Dispatcher(DispatcherConfig cfg, core::MarketConfig market, std::vector<WorkerLink> workers);
    ~Dispatcher();
    Dispatcher(const Dispatcher&) = delete;
    Dispatcher& operator=(const Dispatcher&) = delete;

    bool open();  // binds the listening socket
    uint16_t port() const { return port_; }
    uint16_t textPort() const { return text_port_; }
    // Only read after the dispatcher thread has stopped.
    const core::LatencyRecorder& dispatchLatency() const { return dispatch_latency_; }
    void run(const std::atomic<bool>& running);
    void runOnce();
    const DispatcherStats& stats() const { return stats_; }

private:
    using Clock = std::chrono::steady_clock;

    // Step 1: sockets in.
    void pollSockets();
    void acceptAll(int listen_fd, Protocol protocol);
    void readFrom(Connection& c);
    void handleFrame(Connection& c, const wire::Message& msg);
    ClientEntry* resolveClient(Connection& c, const wire::FrameHeader& h);
    void bindConnection(ClientEntry& e, Connection& c);

    // Requests.
    void handleOrder(Connection& c, ClientEntry& e, const wire::Message& msg);
    void handleCancel(Connection& c, ClientEntry& e, const wire::Message& msg);
    void handleSubscribe(Connection& c, ClientEntry& e, const wire::Message& msg);
    void handleUnsubscribe(Connection& c, ClientEntry& e, const wire::Message& msg);
    void reply(Connection& c, uint32_t req_id, wire::MsgType type, wire::Payload payload);
    void replyError(Connection& c, uint32_t req_id, wire::ErrorCode code);
    bool sendToWorker(size_t w, core::WorkItem&& item);
    void sendControl(wire::MsgType type, const sockaddr_in& endpoint, uint32_t mask);
    void dropSubscriptions(ClientEntry& e);

    // Step 2: worker replies out.
    void drainWorkers();
    void retryPendingControl(size_t w);
    void routeWorkerOutput(wire::Message& msg, size_t worker);
    void deliver(ClientEntry& e, wire::Message& msg);
    void flushMailbox(ClientEntry& e, Connection& c);

    // Step 3: writes.
    void queueFrame(Connection& c, const wire::Message& msg);
    void flushWrites();
    bool flushConnection(Connection& c);  // true when nothing is left to send

    // Step 4: idle timer.
    void touch(Connection& c);
    void untrackIdle(Connection& c);
    void checkIdle();
    void beginDrain(Connection& c);

    // Step 5: push connects and deadlines.
    void startPush(ClientEntry& e);
    void finishConnect(Connection& c);
    void onPushConnected(Connection& c);
    void checkDeadlines();

    // Connection lifetime.
    Connection& addConnection(int fd, const sockaddr_in& peer, ConnState state, bool push,
                              Protocol protocol);
    Connection* findConn(uint64_t conn_id);
    Connection* liveConnection(ClientEntry& e);
    void closeConnection(Connection& c, const char* reason = nullptr);
    void sweepClosed();

    // Server-side trace (only called when cfg_.trace is set).
    bool tracing() const { return cfg_.trace != nullptr; }
    void log(const std::string& text) const { cfg_.trace->log("dispatcher", text); }
    void logRequest(const std::string& outcome) const { log(trace_req_ + "  -> " + outcome); }
    std::string who(const Connection& c) const;
    std::string clientName(uint64_t client_id) const { return cfg_.trace->client(client_id); }
    std::string summary(const wire::Message& msg) const;
    std::string workersFor(uint32_t mask) const;

    DispatcherConfig cfg_;
    core::MarketConfig market_;
    std::vector<WorkerLink> workers_;
    std::vector<std::deque<core::WorkItem>> pending_control_;  // control items awaiting queue space
    std::vector<int16_t> symbol_worker_;                       // symbol_id -> worker, -1 unknown
    uint32_t market_mask_ = 0;
    wire::ListFeedRsp list_feed_;
    std::shared_ptr<const codec::SymbolTable> symbols_;

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    int text_listen_fd_ = -1;
    uint16_t text_port_ = 0;
    Poller poller_;
    std::vector<PollEvent> events_;
    std::unordered_map<uint64_t, std::unique_ptr<Connection>> conns_;
    std::unordered_map<int, uint64_t> fd_conn_;
    uint64_t next_conn_id_ = 1;

    std::list<uint64_t> idle_list_;  // FIFO by deadline: constant x keeps it sorted
    std::vector<uint64_t> write_list_;
    std::vector<uint64_t> timed_list_;  // Connecting or Draining
    std::vector<uint64_t> close_list_;

    ClientRegistry registry_;
    std::unordered_map<uint64_t, uint64_t> order_owner_;     // order_id -> client_id
    std::unordered_map<uint64_t, uint64_t> endpoint_owner_;  // endpointKey -> client_id

    Clock::time_point now_;
    Clock::time_point read_time_;  // when the current read() returned
    core::LatencyRecorder dispatch_latency_;
    std::string trace_req_;  // "conn#3 alice BID req=5 AAPL 10 @ 100" for the frame being handled
    std::string trace_out_;  // description of the worker output being routed
    DispatcherStats stats_;
};

}  // namespace net
