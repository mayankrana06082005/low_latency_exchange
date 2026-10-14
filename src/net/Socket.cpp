#include "net/Socket.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

namespace net {

bool setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

void setNoDelay(int fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

void ignoreSigpipe() { signal(SIGPIPE, SIG_IGN); }

void closeFd(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

sockaddr_in makeAddr(uint32_t ip_host_order, uint16_t port) {
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(ip_host_order);
    a.sin_port = htons(port);
    return a;
}

uint64_t endpointKey(const sockaddr_in& addr) {
    return (static_cast<uint64_t>(ntohl(addr.sin_addr.s_addr)) << 16) | ntohs(addr.sin_port);
}

uint16_t localPort(int fd) {
    sockaddr_in a{};
    socklen_t len = sizeof a;
    if (getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len) != 0) return 0;
    return ntohs(a.sin_port);
}

std::string toString(const sockaddr_in& addr) {
    char ip[INET_ADDRSTRLEN] = {};
    inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof ip);
    return std::string(ip) + ":" + std::to_string(ntohs(addr.sin_port));
}

bool resolveIPv4(const std::string& host, in_addr& out) {
    if (inet_pton(AF_INET, host.c_str(), &out) == 1) return true;
    addrinfo hints{};
    hints.ai_family = AF_INET;
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) return false;
    out = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
    freeaddrinfo(res);
    return true;
}

int listenTcp(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in a = makeAddr(INADDR_ANY, port);
    if (bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0 || listen(fd, 128) != 0 ||
        !setNonBlocking(fd)) {
        closeFd(fd);
    }
    return fd;
}

int connectTcpNonBlocking(const sockaddr_in& addr, bool& in_progress) {
    in_progress = false;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    setNonBlocking(fd);
    setNoDelay(fd);
    if (connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof addr) == 0) return fd;
    if (errno == EINPROGRESS) {
        in_progress = true;
        return fd;
    }
    closeFd(fd);
    return -1;
}

int connectTcp(const std::string& host, uint16_t port) {
    in_addr ip{};
    if (!resolveIPv4(host, ip)) return -1;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr = ip;
    a.sin_port = htons(port);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    if (connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) {
        closeFd(fd);
        return -1;
    }
    setNoDelay(fd);
    setNonBlocking(fd);
    return fd;
}

int bindUdp(uint16_t port) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;
    sockaddr_in a = makeAddr(INADDR_ANY, port);
    if (bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0 || !setNonBlocking(fd)) {
        closeFd(fd);
    }
    return fd;
}

ssize_t sendSome(int fd, const uint8_t* data, size_t len) {
#ifdef MSG_NOSIGNAL
    return ::send(fd, data, len, MSG_NOSIGNAL);
#else
    return ::send(fd, data, len, 0);
#endif
}

}  // namespace net
