#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "net/FrameReader.hpp"
#include "wire/Messages.hpp"

namespace client {

struct ClientConfig {
    std::string host = "127.0.0.1";
    uint16_t port = 9000;
};

// Connection-level events reported to a trace handler, in the order they happen.
enum class TraceEvent {
    Sent,          // msg = frame written to the socket
    Received,      // msg = frame decoded from the socket
    Connected,     // client-initiated connect succeeded (msg = nullptr)
    PushAccepted,  // server-initiated connection adopted (msg = nullptr)
    Closed,        // session connection closed (msg = nullptr)
};

struct SessionStats {
    uint64_t connects = 0;            // client-initiated TCP connects
    uint64_t disconnects_received = 0;
    uint64_t pushes_accepted = 0;     // server-initiated connections adopted
    uint64_t updates = 0;             // ORDER_UPDATEs (and late acks) delivered to the handler
};

// Programmatic client API (Section 3.5 client behaviour):
//  - saves the server-assigned client_id and sends it on every frame;
//  - reconnects lazily on the next request after DISCONNECT or a lost connection;
//  - never auto-resends a request whose outcome is unknown (returns nullopt);
//  - optionally listens on a callback port and adopts server push connections.
// Single-threaded: call every method from the same thread.
class ClientSession {
public:
    using UpdateHandler = std::function<void(const wire::OrderAck&)>;
    using TraceHandler = std::function<void(TraceEvent, const wire::Message*)>;

    explicit ClientSession(ClientConfig cfg);
    ~ClientSession();
    ClientSession(const ClientSession&) = delete;
    ClientSession& operator=(const ClientSession&) = delete;

    // Starts the callback listener (port 0 picks one). Without it callback_port is 0.
    bool enablePush(uint16_t port = 0);
    void onUpdate(UpdateHandler handler) { on_update_ = std::move(handler); }
    void onTrace(TraceHandler handler) { on_trace_ = std::move(handler); }

    // Sends one request and waits for its response. nullopt = outcome unknown.
    std::optional<wire::Message> request(wire::MsgType type, wire::Payload payload,
                                         int timeout_ms = 2000);
    std::optional<wire::Message> bid(uint16_t symbol_id, wire::Price price, wire::Volume volume);
    std::optional<wire::Message> ask(uint16_t symbol_id, wire::Price price, wire::Volume volume);
    std::optional<wire::Message> cancel(uint64_t order_id);
    std::optional<wire::Message> listFeed();
    std::optional<wire::Message> subscribe(uint16_t udp_port, uint32_t group_mask);
    std::optional<wire::Message> unsubscribe(uint32_t group_mask);

    // Processes unsolicited traffic (updates, DISCONNECT, push connects) for up to timeout_ms.
    void pump(int timeout_ms);

    uint64_t clientId() const { return client_id_; }
    uint16_t callbackPort() const { return callback_port_; }
    bool connected() const { return fd_ >= 0 && !server_closing_; }
    const SessionStats& stats() const { return stats_; }

private:
    bool ensureConnected();
    void dropConnection();
    bool sendFrame(const wire::Message& msg);
    std::optional<wire::Message> awaitResponse(uint32_t req_id, int timeout_ms);
    // Waits once for socket activity. Returns false if the session connection was lost.
    bool pumpOnce(int timeout_ms, uint32_t want_req, std::optional<wire::Message>& got);
    bool readSocket(uint32_t want_req, std::optional<wire::Message>& got);
    void handleFrame(wire::Message& msg, uint32_t want_req, std::optional<wire::Message>& got);
    void acceptPush();
    void trace(TraceEvent event, const wire::Message* msg = nullptr);

    ClientConfig cfg_;
    int fd_ = -1;
    int listen_fd_ = -1;
    uint16_t callback_port_ = 0;
    net::FrameReader reader_;
    uint64_t client_id_ = 0;
    uint32_t next_req_id_ = 1;
    bool server_closing_ = false;  // DISCONNECT received; reconnect before the next request
    UpdateHandler on_update_;
    TraceHandler on_trace_;
    SessionStats stats_;
};

}  // namespace client
