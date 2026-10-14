#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "client/ClientSession.hpp"
#include "client/OrderLedger.hpp"
#include "wire/Messages.hpp"

namespace client {

// Prints every frame exchanged between named clients and the dispatcher, plus
// connection events, with a timestamp relative to the tracer's creation. Also
// feeds an OrderLedger so order states can be printed at any point.
// Single-threaded: use from the thread that drives the sessions.
class SessionTracer {
public:
    explicit SessionTracer(OrderLedger::SymbolName symbol_name, bool print = true);

    void attach(ClientSession& session, const std::string& name);
    void frame(const std::string& who, bool outbound, const wire::Message& msg,
               const std::string& remark = "");
    void note(const std::string& who, const std::string& text);
    void setPrinting(bool print) { print_ = print; }

    std::string describe(const wire::Message& msg) const;
    OrderLedger& ledger() { return ledger_; }
    void printLedger(const std::string& title) const;

private:
    void event(const std::string& who, TraceEvent ev, const wire::Message* msg);
    void line(const std::string& who, const char* arrow, const std::string& text);
    std::string orderText(const wire::OrderAck& ack) const;

    OrderLedger::SymbolName symbol_name_;
    OrderLedger ledger_;
    bool print_;
    std::chrono::steady_clock::time_point start_;
};

std::string groupList(uint32_t mask);  // 0b101 -> "{0,2}"

}  // namespace client
