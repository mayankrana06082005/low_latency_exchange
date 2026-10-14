#include "net/UdpPublisher.hpp"

#include <sys/socket.h>

#include "net/Socket.hpp"

namespace net {

UdpPublisher::UdpPublisher() : fd_(bindUdp(0)) {}

UdpPublisher::~UdpPublisher() { closeFd(fd_); }

void UdpPublisher::sendTo(const uint8_t* data, size_t len, const sockaddr_in& to) {
    ssize_t n = fd_ < 0 ? -1
                        : ::sendto(fd_, data, len, 0, reinterpret_cast<const sockaddr*>(&to),
                                   sizeof to);
    if (n == static_cast<ssize_t>(len)) {
        sent_.fetch_add(1, std::memory_order_relaxed);
    } else {
        dropped_.fetch_add(1, std::memory_order_relaxed);
    }
}

}  // namespace net
