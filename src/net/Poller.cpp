#include "net/Poller.hpp"

namespace net {

void Poller::add(int fd, short events) {
    index_[fd] = fds_.size();
    fds_.push_back(pollfd{fd, events, 0});
}

void Poller::modify(int fd, short events) {
    auto it = index_.find(fd);
    if (it != index_.end()) fds_[it->second].events = events;
}

void Poller::remove(int fd) {
    auto it = index_.find(fd);
    if (it == index_.end()) return;
    const size_t i = it->second;
    index_.erase(it);
    if (i != fds_.size() - 1) {
        fds_[i] = fds_.back();
        index_[fds_[i].fd] = i;
    }
    fds_.pop_back();
}

void Poller::wait(int timeout_ms, std::vector<PollEvent>& out) {
    out.clear();
    if (fds_.empty()) return;
    int n = ::poll(fds_.data(), static_cast<nfds_t>(fds_.size()), timeout_ms);
    if (n <= 0) return;
    for (const pollfd& p : fds_) {
        if (p.revents != 0) out.push_back(PollEvent{p.fd, p.revents});
    }
}

}  // namespace net
