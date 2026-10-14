#include "client/SessionTracer.hpp"

#include <cstdio>

namespace client {

using namespace wire;

namespace {

std::string num(uint64_t v) { return std::to_string(static_cast<unsigned long long>(v)); }

std::string shortId(uint64_t client_id) {
    if (client_id == 0) return "0";
    char buf[24];
    std::snprintf(buf, sizeof buf, "..%06llx", (unsigned long long)(client_id & 0xFFFFFF));
    return buf;
}

}  // namespace

std::string groupList(uint32_t mask) {
    std::string s = "{";
    for (int g = 0; g < 32; ++g) {
        if (!(mask & (1u << g))) continue;
        if (s.size() > 1) s += ",";
        s += std::to_string(g);
    }
    return s + "}";
}

SessionTracer::SessionTracer(OrderLedger::SymbolName symbol_name, bool print)
    : symbol_name_(std::move(symbol_name)), print_(print), start_(std::chrono::steady_clock::now()) {}

void SessionTracer::attach(ClientSession& session, const std::string& name) {
    session.onTrace([this, name](TraceEvent ev, const Message* msg) { event(name, ev, msg); });
}

void SessionTracer::event(const std::string& who, TraceEvent ev, const Message* msg) {
    switch (ev) {
        case TraceEvent::Sent: frame(who, true, *msg); break;
        case TraceEvent::Received: frame(who, false, *msg); break;
        case TraceEvent::Connected: line(who, "---", "TCP connect to dispatcher"); break;
        case TraceEvent::PushAccepted:
            line(who, "<==", "PUSH: dispatcher opened a connection to our callback port");
            break;
        case TraceEvent::Closed: line(who, "-x-", "TCP connection closed"); break;
    }
}

void SessionTracer::frame(const std::string& who, bool outbound, const Message& msg,
                          const std::string& remark) {
    if (outbound) {
        ledger_.onSent(who, msg);
    } else {
        ledger_.onReceived(who, msg);
    }
    std::string text = describe(msg);
    if (!remark.empty()) text += "   <- " + remark;
    line(who, outbound ? "-->" : "<--", text);
}

void SessionTracer::note(const std::string& who, const std::string& text) { line(who, "   ", text); }

void SessionTracer::line(const std::string& who, const char* arrow, const std::string& text) {
    if (!print_) return;
    const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() -
                                                                start_).count();
    std::printf("  %8.1f ms  %-7s %s dispatcher  %s\n", ms, who.c_str(), arrow, text.c_str());
}

std::string SessionTracer::orderText(const OrderAck& ack) const {
    std::string s = "order " + num(ack.order_id) + " " + nameOf(ack.status) + ", remaining " +
                    num(ack.remaining_vol);
    if (!ack.trades.empty()) {
        s += ", trades [";
        for (size_t i = 0; i < ack.trades.size(); ++i) {
            const Trade& t = ack.trades[i];
            if (i > 0) s += ", ";
            s += num(t.vol) + "@" + num(t.price) + " vs order " + num(t.counterpart_id);
        }
        s += "]";
    }
    return s;
}

std::string SessionTracer::describe(const Message& msg) const {
    char head[64];
    std::snprintf(head, sizeof head, "%-15s req=%-3u cid=%-9s ", nameOf(msg.header.type),
                  msg.header.req_id, shortId(msg.header.client_id).c_str());
    std::string s = head;

    if (const auto* o = std::get_if<OrderRequest>(&msg.payload)) {
        s += symbol_name_(o->symbol_id) + " " + num(o->volume) + " @ " + num(o->price);
    } else if (const auto* c = std::get_if<CancelRequest>(&msg.payload)) {
        s += "order " + num(c->order_id);
    } else if (const auto* sub = std::get_if<SubscribeRequest>(&msg.payload)) {
        s += "groups " + groupList(sub->group_mask) + " to UDP port " + num(sub->udp_port);
    } else if (const auto* g = std::get_if<GroupMask>(&msg.payload)) {
        s += "groups " + groupList(g->group_mask);
    } else if (const auto* ack = std::get_if<OrderAck>(&msg.payload)) {
        s += orderText(*ack);
    } else if (const auto* ca = std::get_if<CancelAck>(&msg.payload)) {
        s += "order " + num(ca->order_id) + " " + nameOf(ca->status);
    } else if (const auto* l = std::get_if<ListFeedRsp>(&msg.payload)) {
        s += num(l->groups.size()) + " groups";
    } else if (const auto* e = std::get_if<Error>(&msg.payload)) {
        s += nameOf(e->code);
    } else if (std::get_if<Disconnect>(&msg.payload) != nullptr) {
        s += "reason=idle (server will close)";
    }
    return s;
}

void SessionTracer::printLedger(const std::string& title) const {
    if (!print_) return;
    std::printf("\n  -- order states: %s --\n%s\n", title.c_str(), ledger_.table(symbol_name_).c_str());
}

}  // namespace client
