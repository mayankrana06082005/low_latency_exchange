#include "codec/SymbolTable.hpp"

namespace codec {

void SymbolTable::add(uint16_t symbol_id, const std::string& ticker) {
    ids_[ticker] = symbol_id;
    names_[symbol_id] = ticker;
}

void SymbolTable::addAll(const wire::ListFeedRsp& feed) {
    for (const wire::GroupInfo& g : feed.groups) {
        for (const wire::TickerInfo& t : g.tickers) add(t.symbol_id, tickerString(t.ticker));
    }
}

uint16_t SymbolTable::idOf(const std::string& ticker) const {
    auto it = ids_.find(ticker);
    return it == ids_.end() ? 0 : it->second;
}

std::string SymbolTable::nameOf(uint16_t symbol_id) const {
    auto it = names_.find(symbol_id);
    return it == names_.end() ? "#" + std::to_string(symbol_id) : it->second;
}

std::string tickerString(const wire::Ticker& ticker) {
    size_t len = 0;
    while (len < wire::TICKER_LEN && ticker[len] != '\0') ++len;
    return std::string(ticker.data(), len);
}

}  // namespace codec
