// exchange_client: interactive client for manual testing on loopback or a LAN.

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#include "client/ClientSession.hpp"
#include "client/SessionTracer.hpp"
#include "client/UdpListener.hpp"
#include "core/MarketConfig.hpp"

using namespace wire;

namespace {

void printResponse(const std::optional<Message>& rsp) {
    if (!rsp) {
        std::puts("  no response (connection lost or timeout); outcome unknown, not resent");
        return;
    }
    const Message& m = *rsp;
    std::printf("  %s", nameOf(m.header.type));
    if (const auto* a = std::get_if<OrderAck>(&m.payload)) {
        std::printf(" order_id=%llu status=%s remaining=%llu trades=%zu",
                    (unsigned long long)a->order_id, nameOf(a->status),
                    (unsigned long long)a->remaining_vol, a->trades.size());
        for (const Trade& t : a->trades) {
            std::printf("\n    trade %llu: %llu @ %llu (counterpart order %llu)",
                        (unsigned long long)t.trade_id, (unsigned long long)t.vol,
                        (unsigned long long)t.price, (unsigned long long)t.counterpart_id);
        }
    } else if (const auto* c = std::get_if<CancelAck>(&m.payload)) {
        std::printf(" order_id=%llu status=%s", (unsigned long long)c->order_id, nameOf(c->status));
    } else if (const auto* g = std::get_if<GroupMask>(&m.payload)) {
        std::printf(" group_mask=0x%x", g->group_mask);
    } else if (const auto* e = std::get_if<Error>(&m.payload)) {
        std::printf(" %s", nameOf(e->code));
    } else if (const auto* l = std::get_if<ListFeedRsp>(&m.payload)) {
        for (const GroupInfo& gi : l->groups) {
            std::printf("\n    group %u %s:", gi.group_id, gi.name.c_str());
            for (const TickerInfo& t : gi.tickers) {
                std::printf(" %s(%u)", core::tickerName(t.ticker).c_str(), t.symbol_id);
            }
        }
    }
    std::printf("\n");
}

void help() {
    std::puts(
        "commands:\n"
        "  list                         LIST_FEED\n"
        "  bid <TICKER|id> <price> <vol>\n"
        "  ask <TICKER|id> <price> <vol>\n"
        "  cancel <order_id>\n"
        "  sub <group_mask>             subscribe this client's UDP port (mask e.g. 3 = groups 0,1)\n"
        "  unsub <group_mask>\n"
        "  book                         latest UDP snapshot per ticker\n"
        "  orders                       state of every order this client placed\n"
        "  wait <ms>                    process pushed updates for ms\n"
        "  id | help | quit");
}

// Latest snapshot per ticker, written by the UDP thread, read by the prompt.
struct Book {
    std::mutex mu;
    std::map<std::string, MarketData> latest;
    uint64_t received = 0, gaps = 0, missing = 0;
};

void printBook(Book& book) {
    std::lock_guard<std::mutex> lock(book.mu);
    std::printf("  UDP received %llu, gaps %llu, missing %llu\n",
                (unsigned long long)book.received, (unsigned long long)book.gaps,
                (unsigned long long)book.missing);
    for (const auto& entry : book.latest) {
        const MarketData& md = entry.second;
        std::printf("  %-6s seq %-6llu bids:", entry.first.c_str(), (unsigned long long)md.seq);
        for (int i = 0; i < md.n_bids; ++i) {
            std::printf(" %llux%llu", (unsigned long long)md.bids[i].price,
                        (unsigned long long)md.bids[i].vol);
        }
        std::printf("  asks:");
        for (int i = 0; i < md.n_asks; ++i) {
            std::printf(" %llux%llu", (unsigned long long)md.asks[i].price,
                        (unsigned long long)md.asks[i].vol);
        }
        std::printf("\n");
    }
}

}  // namespace

int main(int argc, char** argv) {
    client::ClientConfig cfg;
    uint16_t callback_port = 0;
    uint16_t udp_port = 0;
    bool push = true;
    bool trace_frames = false;
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string arg = argv[i];
        if (arg == "--host") cfg.host = argv[i + 1];
        else if (arg == "--port") cfg.port = static_cast<uint16_t>(std::atoi(argv[i + 1]));
        else if (arg == "--callback-port") callback_port = static_cast<uint16_t>(std::atoi(argv[i + 1]));
        else if (arg == "--udp-port") udp_port = static_cast<uint16_t>(std::atoi(argv[i + 1]));
        else if (arg == "--push") push = std::string(argv[i + 1]) != "off";
        else if (arg == "--trace") trace_frames = std::string(argv[i + 1]) == "on";
    }

    client::ClientSession session(cfg);
    if (push && !session.enablePush(callback_port)) std::puts("warning: callback listener failed");
    session.onUpdate([](const OrderAck& u) {
        std::printf("  << ORDER_UPDATE order_id=%llu status=%s remaining=%llu new trades=%zu\n",
                    (unsigned long long)u.order_id, nameOf(u.status),
                    (unsigned long long)u.remaining_vol, u.trades.size());
    });

    client::UdpListener udp;
    if (!udp.open(udp_port)) std::puts("warning: UDP listener failed");
    Book book;
    udp.onSnapshot([&book, &udp](const MarketData& md) {
        std::lock_guard<std::mutex> lock(book.mu);
        book.latest[core::tickerName(md.ticker)] = md;
        book.received = udp.received();
        book.gaps = udp.gaps();
        book.missing = udp.missing();
    });
    std::atomic<bool> running{true};
    std::thread udp_thread([&] {
        while (running.load()) udp.poll(50);
    });

    std::map<std::string, uint16_t> symbols;
    std::map<uint16_t, std::string> symbol_names;
    client::SessionTracer tracer(
        [&symbol_names](uint16_t id) {
            auto it = symbol_names.find(id);
            return it != symbol_names.end() ? it->second : std::to_string(id);
        },
        trace_frames);
    tracer.attach(session, "me");
    auto feed = session.listFeed();
    if (!feed) {
        std::printf("cannot reach %s:%u\n", cfg.host.c_str(), cfg.port);
        running = false;
        udp_thread.join();
        return 1;
    }
    for (const GroupInfo& g : std::get<ListFeedRsp>(feed->payload).groups) {
        for (const TickerInfo& t : g.tickers) {
            symbols[core::tickerName(t.ticker)] = t.symbol_id;
            symbol_names[t.symbol_id] = core::tickerName(t.ticker);
        }
    }
    std::printf("connected, client_id=%llu, callback port %u, UDP port %u\n",
                (unsigned long long)session.clientId(), session.callbackPort(), udp.port());
    help();

    auto symbolOf = [&symbols](const std::string& s) -> uint16_t {
        auto it = symbols.find(s);
        return it != symbols.end() ? it->second : static_cast<uint16_t>(std::atoi(s.c_str()));
    };

    std::string line;
    while (true) {
        session.pump(0);
        std::printf("> ");
        std::fflush(stdout);
        if (!std::getline(std::cin, line)) break;
        std::istringstream in(line);
        std::string cmd, a;
        in >> cmd;
        if (cmd.empty()) continue;
        if (cmd == "quit" || cmd == "exit") break;
        if (cmd == "help") help();
        else if (cmd == "id") std::printf("  client_id=%llu\n", (unsigned long long)session.clientId());
        else if (cmd == "list") printResponse(session.listFeed());
        else if (cmd == "bid" || cmd == "ask") {
            unsigned long long price = 0, vol = 0;
            in >> a >> price >> vol;
            printResponse(cmd == "bid" ? session.bid(symbolOf(a), price, vol)
                                       : session.ask(symbolOf(a), price, vol));
        } else if (cmd == "cancel") {
            unsigned long long id = 0;
            in >> id;
            printResponse(session.cancel(id));
        } else if (cmd == "sub" || cmd == "unsub") {
            unsigned mask = 0;
            in >> mask;
            printResponse(cmd == "sub" ? session.subscribe(udp.port(), mask) : session.unsubscribe(mask));
        } else if (cmd == "book") {
            printBook(book);
        } else if (cmd == "orders") {
            session.pump(0);
            std::fputs(tracer.ledger().table([&symbol_names](uint16_t id) { return symbol_names[id]; }).c_str(), stdout);
        }
        else if (cmd == "wait") {
            int ms = 1000;
            in >> ms;
            session.pump(ms);
        } else std::puts("  unknown command (help)");
    }
    running = false;
    udp_thread.join();
    return 0;
}
