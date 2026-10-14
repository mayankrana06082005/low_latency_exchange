#pragma once

#include <netinet/in.h>

#include <cstdint>

#include "wire/Messages.hpp"

namespace core {

// Dispatcher -> worker. Orders, cancels and subscription control items share
// one queue so their relative order is preserved (Section 3.4).
struct WorkItem {
    wire::MsgType type = wire::MsgType::Bid;  // Bid, Ask, Cancel, Subscribe, Unsubscribe
    uint64_t client_id = 0;                   // opaque reply token, echoed back
    uint32_t req_id = 0;
    uint16_t symbol_id = 0;
    wire::Price price = 0;
    wire::Volume volume = 0;
    uint64_t order_id = 0;      // Cancel
    sockaddr_in endpoint{};     // Subscribe / Unsubscribe
    uint32_t group_mask = 0;    // Subscribe / Unsubscribe
};

// Worker -> dispatcher: a ready-to-encode frame. header.client_id is the reply
// token, or 0 for ORDER_UPDATE (the dispatcher resolves it from order_id).
using WorkerOutput = wire::Message;

}  // namespace core
