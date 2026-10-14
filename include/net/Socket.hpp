#pragma once

// Thin POSIX socket helpers shared by server and client (Linux and macOS).

#include <netinet/in.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace net {

bool setNonBlocking(int fd);
void setNoDelay(int fd);
void ignoreSigpipe();
void closeFd(int& fd);

sockaddr_in makeAddr(uint32_t ip_host_order, uint16_t port);
uint64_t endpointKey(const sockaddr_in& addr);  // (ip << 16) | port, host order
uint16_t localPort(int fd);
std::string toString(const sockaddr_in& addr);
bool resolveIPv4(const std::string& host, in_addr& out);

// Non-blocking listening TCP socket on all interfaces; port 0 picks one.
int listenTcp(uint16_t port);
// Non-blocking connect; in_progress is set when the connect has not finished yet.
int connectTcpNonBlocking(const sockaddr_in& addr, bool& in_progress);
// Blocking connect, then switched to non-blocking. Returns -1 on failure.
int connectTcp(const std::string& host, uint16_t port);
// Non-blocking UDP socket bound to all interfaces; port 0 picks one.
int bindUdp(uint16_t port);

// send() that never raises SIGPIPE where the platform allows it.
ssize_t sendSome(int fd, const uint8_t* data, size_t len);

}  // namespace net
