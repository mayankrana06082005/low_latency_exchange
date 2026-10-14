#include "client/UdpListener.hpp"

#include <poll.h>
#include <sys/socket.h>

#include "codec/BinaryCodec.hpp"
#include "net/Socket.hpp"

namespace client {

UdpListener::~UdpListener() { net::closeFd(fd_); }

bool UdpListener::open(uint16_t port) {
    fd_ = net::bindUdp(port);
    if (fd_ < 0) return false;
    port_ = net::localPort(fd_);
    return true;
}

int UdpListener::poll(int timeout_ms) {
    if (fd_ < 0) return 0;
    pollfd p{fd_, POLLIN, 0};
    if (::poll(&p, 1, timeout_ms) <= 0) return 0;
    int count = 0;
    uint8_t buf[2048];
    for (;;) {
        ssize_t n = ::recvfrom(fd_, buf, sizeof buf, 0, nullptr, nullptr);
        if (n < 0) break;  // EAGAIN: drained
        ++arrivals_;
        if (drop_every_ != 0 && arrivals_ % drop_every_ == 0) continue;  // induced loss
        process(buf, static_cast<size_t>(n));
        ++count;
    }
    return count;
}

void UdpListener::process(const uint8_t* data, size_t len) {
    wire::MarketData md;
    if (!codec::BinaryCodec::decodeMarketData(data, len, md)) {
        ++malformed_;
        return;
    }
    ++received_;
    auto it = expected_seq_.find(md.group_id);
    if (it != expected_seq_.end() && md.seq > it->second) {
        ++gaps_;
        missing_ += md.seq - it->second;
    }
    if (it == expected_seq_.end() || md.seq >= it->second) expected_seq_[md.group_id] = md.seq + 1;
    if (on_snapshot_) on_snapshot_(md);
}

}  // namespace client
