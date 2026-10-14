#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include "wire/Messages.hpp"

namespace codec {

// ticker <-> symbol_id. The text protocol carries tickers as strings, so the
// server resolves them at decode time (a hash lookup counted in the text
// baseline, Section 5) and a text client learns the table from LIST_FEED_RSP.
class SymbolTable {
public:
    void add(uint16_t symbol_id, const std::string& ticker);
    void addAll(const wire::ListFeedRsp& feed);

    uint16_t idOf(const std::string& ticker) const;  // 0 if unknown (never a valid symbol)
    std::string nameOf(uint16_t symbol_id) const;    // "#<id>" if unknown
    bool empty() const { return ids_.empty(); }

private:
    std::unordered_map<std::string, uint16_t> ids_;
    std::unordered_map<uint16_t, std::string> names_;
};

std::string tickerString(const wire::Ticker& ticker);  // NUL padding stripped

}  // namespace codec
