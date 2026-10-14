#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "codec/SymbolTable.hpp"
#include "net/StreamCodec.hpp"

namespace bench {

struct LoadResult {
    uint64_t sent = 0;
    uint64_t responses = 0;  // replies to our requests (acks and errors)
    uint64_t errors = 0;     // of which ERROR (e.g. QUEUE_FULL)
    uint64_t updates = 0;    // unsolicited ORDER_UPDATEs received
    bool connection_lost = false;
};

// Pipelined load generator for the throughput benchmark: keeps `window`
// requests in flight on one connection and sends a new one per reply.
// Alternates BID/ASK at one price so orders keep matching and the book stays small.
class LoadClient {
public:
    LoadClient(std::string host, uint16_t port, net::Protocol protocol);
    ~LoadClient();
    LoadClient(const LoadClient&) = delete;
    LoadClient& operator=(const LoadClient&) = delete;

    bool connect();  // TCP connect + LIST_FEED (learns client_id and tickers)
    LoadResult run(uint16_t symbol_id, int window, std::chrono::steady_clock::time_point until);

private:
    void queueOrder(bool buy, uint16_t symbol_id);
    bool flush();
    bool readAvailable(int timeout_ms, LoadResult& result, int& replies);

    std::string host_;
    uint16_t port_;
    net::Protocol protocol_;
    int fd_ = -1;
    std::shared_ptr<codec::SymbolTable> symbols_ = std::make_shared<codec::SymbolTable>();
    std::unique_ptr<net::StreamCodec> codec_;
    uint64_t client_id_ = 0;
    uint32_t next_req_id_ = 1;
    std::vector<uint8_t> out_;
};

}  // namespace bench
