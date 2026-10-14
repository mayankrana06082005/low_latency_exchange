#pragma once

#include <poll.h>

#include <unordered_map>
#include <vector>

namespace net {

struct PollEvent {
    int fd;
    short revents;
};

// Readiness polling over a set of fds. poll() backend; the interface is kept
// small so an epoll (Linux) or kqueue (macOS) backend can replace it.
class Poller {
public:
    void add(int fd, short events);
    void modify(int fd, short events);
    void remove(int fd);
    // Fills `out` with ready fds; timeout 0 never blocks (busy-poll).
    void wait(int timeout_ms, std::vector<PollEvent>& out);
    size_t size() const { return fds_.size(); }

private:
    std::vector<pollfd> fds_;
    std::unordered_map<int, size_t> index_;
};

}  // namespace net
