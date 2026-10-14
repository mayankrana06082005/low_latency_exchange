#pragma once

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <unordered_map>

namespace core {

// Server-side event log: one line per event, written by the dispatcher thread
// and every worker thread, showing the path a message takes inside the server
// (socket -> dispatcher -> worker queue -> worker -> dispatcher -> client).
// Off unless a ServerTrace is passed in the config; the hot path then pays one
// null-pointer check. Thread-safe.
class ServerTrace {
public:
    explicit ServerTrace(std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now(),
                         std::FILE* out = stdout);

    // Optional display names (e.g. from a demo script); unknown ids print as "cid ..a1b2c3".
    void nameClient(uint64_t client_id, const std::string& name);
    std::string client(uint64_t client_id) const;

    void log(const char* actor, const std::string& text) const;

private:
    std::chrono::steady_clock::time_point start_;
    std::FILE* out_;
    mutable std::mutex names_mu_;
    std::unordered_map<uint64_t, std::string> names_;
};

std::string groupSet(uint32_t mask);  // 0b101 -> "{0,2}"

}  // namespace core
