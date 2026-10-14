#pragma once

#include <netinet/in.h>

#include <cstddef>
#include <cstdint>
#include <random>
#include <unordered_map>

#include "net/Mailbox.hpp"

namespace net {

// Per-client state owned by the dispatcher (Section 3.5).
struct ClientEntry {
    uint64_t client_id = 0;
    uint64_t conn_id = 0;       // live session, 0 = none
    uint64_t push_conn_id = 0;  // server-initiated connect in progress, 0 = none
    uint32_t peer_ip = 0;       // host order, from the last client-initiated connection
    uint16_t callback_port = 0;
    bool has_udp = false;
    sockaddr_in udp_endpoint{};
    uint32_t group_mask = 0;  // groups subscribed on udp_endpoint
    uint32_t resting_count = 0;
    Mailbox mailbox;
};

// client_id -> entry. Entries are never removed in the prototype, so pointers
// to them stay valid (unordered_map nodes do not move on rehash).
class ClientRegistry {
public:
    ClientRegistry();
    ClientEntry& create();  // random, nonzero, unused id (D11)
    ClientEntry* find(uint64_t client_id);
    size_t size() const { return clients_.size(); }

private:
    std::unordered_map<uint64_t, ClientEntry> clients_;
    std::mt19937_64 rng_;
};

}  // namespace net
