#pragma once

#include <netinet/in.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <vector>

#include "net/FrameReader.hpp"

namespace net {

struct ClientEntry;

enum class ConnState {
    Connecting,  // server-initiated push connect not finished yet
    Open,
    Draining,    // DISCONNECT sent; still answering in-flight requests until grace ends
    Closed,      // waiting to be swept at the end of the loop iteration
};

// One TCP connection, owned by the dispatcher. conn_id never leaves the dispatcher.
struct Connection {
    using Clock = std::chrono::steady_clock;

    Connection(uint64_t id, int socket_fd, const sockaddr_in& peer_addr, ConnState initial,
               bool is_push, uint32_t max_frame_len)
        : conn_id(id), fd(socket_fd), peer(peer_addr), state(initial), push(is_push),
          reader(max_frame_len) {}

    uint64_t conn_id;
    int fd;
    sockaddr_in peer;
    ConnState state;
    bool push;                      // server-initiated (callback) connection
    ClientEntry* client = nullptr;  // bound client, cached so frames need no hash lookup

    FrameReader reader;
    std::vector<uint8_t> wbuf;
    size_t wpos = 0;
    bool in_write_list = false;

    bool in_idle_list = false;
    std::list<uint64_t>::iterator idle_it;
    Clock::time_point idle_deadline;
    Clock::time_point deadline;  // connect timeout (Connecting) or end of grace (Draining)
};

}  // namespace net
