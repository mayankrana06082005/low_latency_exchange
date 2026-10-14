#include "net/ClientRegistry.hpp"

namespace net {

ClientRegistry::ClientRegistry() : rng_(std::random_device{}()) {}

ClientEntry& ClientRegistry::create() {
    uint64_t id = 0;
    while (id == 0 || clients_.count(id) != 0) id = rng_();
    ClientEntry& e = clients_[id];
    e.client_id = id;
    return e;
}

ClientEntry* ClientRegistry::find(uint64_t client_id) {
    auto it = clients_.find(client_id);
    return it == clients_.end() ? nullptr : &it->second;
}

}  // namespace net
