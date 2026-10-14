#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "wire/Messages.hpp"

namespace core {

struct SymbolConfig {
    uint16_t symbol_id;
    std::string ticker;  // at most 8 characters
};

struct GroupConfig {
    uint8_t group_id;  // 0..31 (D4)
    std::string name;
    std::vector<SymbolConfig> symbols;
};

// Static market definition: ticker groups and their symbols.
struct MarketConfig {
    std::vector<GroupConfig> groups;

    static MarketConfig defaults();
    wire::ListFeedRsp toListFeed() const;
};

wire::Ticker toTicker(const std::string& name);
std::string tickerName(const wire::Ticker& ticker);

}  // namespace core
