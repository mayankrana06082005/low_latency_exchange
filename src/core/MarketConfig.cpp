#include "core/MarketConfig.hpp"

#include <cstring>

namespace core {

MarketConfig MarketConfig::defaults() {
    MarketConfig m;
    m.groups = {
        {0, "tech", {{1, "AAPL"}, {2, "MSFT"}, {3, "GOOG"}}},
        {1, "energy", {{4, "XOM"}, {5, "CVX"}}},
        {2, "finance", {{6, "JPM"}, {7, "GS"}}},
    };
    return m;
}

wire::ListFeedRsp MarketConfig::toListFeed() const {
    wire::ListFeedRsp rsp;
    for (const GroupConfig& g : groups) {
        wire::GroupInfo info;
        info.group_id = g.group_id;
        info.name = g.name;
        for (const SymbolConfig& s : g.symbols) {
            info.tickers.push_back(wire::TickerInfo{s.symbol_id, toTicker(s.ticker)});
        }
        rsp.groups.push_back(std::move(info));
    }
    return rsp;
}

wire::Ticker toTicker(const std::string& name) {
    wire::Ticker t{};
    std::memcpy(t.data(), name.data(), std::min(name.size(), wire::TICKER_LEN));
    return t;
}

std::string tickerName(const wire::Ticker& ticker) {
    size_t len = 0;
    while (len < wire::TICKER_LEN && ticker[len] != '\0') ++len;
    return std::string(ticker.data(), len);
}

}  // namespace core
