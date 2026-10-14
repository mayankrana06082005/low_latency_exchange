#include "core/ServerTrace.hpp"

namespace core {

ServerTrace::ServerTrace(std::chrono::steady_clock::time_point start, std::FILE* out)
    : start_(start), out_(out) {}

void ServerTrace::nameClient(uint64_t client_id, const std::string& name) {
    std::lock_guard<std::mutex> lock(names_mu_);
    names_[client_id] = name;
}

std::string ServerTrace::client(uint64_t client_id) const {
    {
        std::lock_guard<std::mutex> lock(names_mu_);
        auto it = names_.find(client_id);
        if (it != names_.end()) return it->second;
    }
    char buf[32];
    std::snprintf(buf, sizeof buf, "cid ..%06llx", (unsigned long long)(client_id & 0xFFFFFF));
    return buf;
}

void ServerTrace::log(const char* actor, const std::string& text) const {
    const double ms =
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start_).count();
    // One fprintf per line: stdio locks the stream per call, so lines from
    // different threads never interleave mid-line.
    std::fprintf(out_, "  %8.1f ms      | %-10s | %s\n", ms, actor, text.c_str());
}

std::string groupSet(uint32_t mask) {
    std::string s = "{";
    for (int g = 0; g < 32; ++g) {
        if (!(mask & (1u << g))) continue;
        if (s.size() > 1) s += ",";
        s += std::to_string(g);
    }
    return s + "}";
}

}  // namespace core
