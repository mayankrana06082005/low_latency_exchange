#include "codec/TextCodec.hpp"

#include <nlohmann/json.hpp>

#include <cctype>
#include <cstring>
#include <string>
#include <string_view>

namespace codec {

using namespace wire;
using json = nlohmann::json;
using std::string_view;

namespace {

// ================================================================== encoding

int httpStatus(ErrorCode code) {
    switch (code) {
        case ErrorCode::InvalidField: return 422;
        case ErrorCode::UnknownSymbol:
        case ErrorCode::OrderNotFound:
        case ErrorCode::UnknownGroup:
        case ErrorCode::UnknownClient: return 404;
        case ErrorCode::NotOwner:
        case ErrorCode::ClientMismatch: return 403;
        case ErrorCode::QueueFull: return 429;
        default: return 400;
    }
}

const char* reasonPhrase(int status) {
    switch (status) {
        case 200: return "OK";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 422: return "Unprocessable Entity";
        case 429: return "Too Many Requests";
        default: return "Bad Request";
    }
}

json groupArray(uint32_t mask) {
    json a = json::array();
    for (uint64_t g = 0; g < 32; ++g) {
        if (mask & (1u << g)) a.push_back(g);
    }
    return a;
}

json orderAckJson(const OrderAck& a) {
    json trades = json::array();
    for (const Trade& t : a.trades) {
        trades.push_back({{"trade_id", t.trade_id},
                          {"price", t.price},
                          {"vol", t.vol},
                          {"counterpart_id", t.counterpart_id}});
    }
    return {{"order_id", a.order_id},
            {"status", nameOf(a.status)},
            {"remaining_vol", a.remaining_vol},
            {"trades", trades}};
}

json listFeedJson(const ListFeedRsp& rsp) {
    json groups = json::array();
    for (const GroupInfo& g : rsp.groups) {
        json tickers = json::array();
        for (const TickerInfo& t : g.tickers) {
            tickers.push_back({{"symbol_id", static_cast<uint64_t>(t.symbol_id)},
                               {"ticker", tickerString(t.ticker)}});
        }
        groups.push_back({{"group_id", static_cast<uint64_t>(g.group_id)},
                          {"name", g.name},
                          {"tickers", tickers}});
    }
    return {{"groups", groups}};
}

struct Outgoing {
    std::string start_line;
    std::string extra_headers;
    std::string body;
};

Outgoing request(const char* line, std::string body = "", std::string extra = "") {
    return Outgoing{line, std::move(extra), std::move(body)};
}

Outgoing ok(const json& body) { return Outgoing{"HTTP/1.1 200 OK", "", body.dump()}; }

Outgoing toHttp(const Message& m, const SymbolTable& symbols) {
    switch (m.header.type) {
        case MsgType::Bid:
        case MsgType::Ask: {
            const auto& o = std::get<OrderRequest>(m.payload);
            const bool bid = m.header.type == MsgType::Bid;
            json j = {{"ticker", symbols.nameOf(o.symbol_id)},
                      {"type", "LIMIT"},
                      {bid ? "max_price" : "min_price", o.price},
                      {"volume", o.volume}};
            return request(bid ? "POST /order/bid HTTP/1.1" : "POST /order/ask HTTP/1.1", j.dump());
        }
        case MsgType::Cancel:
            return request("DELETE /order/cancel HTTP/1.1", "",
                           "Order-ID: " + std::to_string(std::get<CancelRequest>(m.payload).order_id) + "\r\n");
        case MsgType::ListFeed: return request("GET /feed HTTP/1.1");
        case MsgType::Subscribe: {
            const auto& s = std::get<SubscribeRequest>(m.payload);
            json j = {{"groups", groupArray(s.group_mask)}, {"udp_port", static_cast<uint64_t>(s.udp_port)}};
            return request("POST /feed/subscribe HTTP/1.1", j.dump());
        }
        case MsgType::Unsubscribe: {
            json j = {{"groups", groupArray(std::get<GroupMask>(m.payload).group_mask)}};
            return request("POST /feed/unsubscribe HTTP/1.1", j.dump());
        }
        case MsgType::OrderAck: return ok(orderAckJson(std::get<OrderAck>(m.payload)));
        case MsgType::OrderUpdate:
            return request("POST /order/update HTTP/1.1", orderAckJson(std::get<OrderAck>(m.payload)).dump());
        case MsgType::CancelAck: {
            const auto& c = std::get<CancelAck>(m.payload);
            return ok({{"order_id", c.order_id}, {"status", nameOf(c.status)}});
        }
        case MsgType::ListFeedRsp: return ok(listFeedJson(std::get<ListFeedRsp>(m.payload)));
        case MsgType::SubscribeAck:
            return ok({{"subscribed", groupArray(std::get<GroupMask>(m.payload).group_mask)}});
        case MsgType::UnsubscribeAck:
            return ok({{"unsubscribed", groupArray(std::get<GroupMask>(m.payload).group_mask)}});
        case MsgType::Error: {
            const ErrorCode code = std::get<Error>(m.payload).code;
            const int status = httpStatus(code);
            json j = {{"error", nameOf(code)}, {"code", static_cast<uint64_t>(code)}};
            return Outgoing{"HTTP/1.1 " + std::to_string(status) + " " + reasonPhrase(status), "", j.dump()};
        }
        case MsgType::Disconnect:
            return request("POST /session/disconnect HTTP/1.1", json{{"reason", "idle"}}.dump());
    }
    return request("GET / HTTP/1.1");
}

// ================================================================== decoding

struct Head {
    bool response = false;
    string_view method;
    string_view path;
    int status = 0;
    uint64_t content_length = 0;
    uint64_t client_id = 0;
    uint64_t callback_port = 0;
    uint64_t req_id = 0;
    uint64_t order_id = 0;
    bool has_order_id = false;
};

bool parseU64(string_view s, uint64_t& out) {
    if (s.empty() || s.size() > 20) return false;
    uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        const uint64_t d = static_cast<uint64_t>(c - '0');
        if (v > (UINT64_MAX - d) / 10) return false;
        v = v * 10 + d;
    }
    out = v;
    return true;
}

string_view trim(string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
    return s;
}

bool nameIs(string_view name, const char* lower) {
    const size_t n = std::strlen(lower);
    if (name.size() != n) return false;
    for (size_t i = 0; i < n; ++i) {
        if (std::tolower(static_cast<unsigned char>(name[i])) != lower[i]) return false;
    }
    return true;
}

DecodeStatus parseStartLine(string_view line, Head& h) {
    const size_t sp1 = line.find(' ');
    if (sp1 == string_view::npos) return DecodeStatus::Malformed;
    if (line.substr(0, 5) == "HTTP/") {
        if (line.substr(0, sp1) != "HTTP/1.1") return DecodeStatus::UnsupportedVersion;
        uint64_t code = 0;
        if (!parseU64(line.substr(sp1 + 1, 3), code)) return DecodeStatus::Malformed;
        h.response = true;
        h.status = static_cast<int>(code);
        return DecodeStatus::Ok;
    }
    const size_t sp2 = line.find(' ', sp1 + 1);
    if (sp2 == string_view::npos) return DecodeStatus::Malformed;
    if (line.substr(sp2 + 1) != "HTTP/1.1") return DecodeStatus::UnsupportedVersion;
    h.method = line.substr(0, sp1);
    h.path = line.substr(sp1 + 1, sp2 - sp1 - 1);
    return DecodeStatus::Ok;
}

bool parseHeaderLine(string_view line, Head& h) {
    const size_t colon = line.find(':');
    if (colon == string_view::npos) return false;
    const string_view name = trim(line.substr(0, colon));
    const string_view value = trim(line.substr(colon + 1));
    if (nameIs(name, "content-length")) return parseU64(value, h.content_length);
    if (nameIs(name, "client-id")) return parseU64(value, h.client_id);
    if (nameIs(name, "callback-port")) return parseU64(value, h.callback_port) && h.callback_port <= 0xFFFF;
    if (nameIs(name, "request-id")) return parseU64(value, h.req_id) && h.req_id <= 0xFFFFFFFF;
    if (nameIs(name, "order-id")) return h.has_order_id = parseU64(value, h.order_id);
    return true;  // other headers (Host, User-Agent, ...) are ignored
}

bool parseJson(string_view body, json& j) {
    j = json::parse(body.begin(), body.end(), nullptr, false);
    return !j.is_discarded() && j.is_object();
}

bool getU64(const json& j, const char* key, uint64_t& out) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_number_unsigned()) return false;
    out = it->get<uint64_t>();
    return true;
}

bool getString(const json& j, const char* key, std::string& out) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return false;
    out = it->get<std::string>();
    return true;
}

bool getMask(const json& j, const char* key, uint32_t& mask) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_array()) return false;
    mask = 0;
    for (const json& g : *it) {
        if (!g.is_number_unsigned() || g.get<uint64_t>() > 31) return false;
        mask |= 1u << g.get<uint64_t>();
    }
    return true;
}

bool parseStatus(const std::string& s, OrderStatus& out) {
    for (OrderStatus st : {OrderStatus::Resting, OrderStatus::PartialFill, OrderStatus::Filled,
                           OrderStatus::Cancelled}) {
        if (s == nameOf(st)) {
            out = st;
            return true;
        }
    }
    return false;
}

bool getOrderAck(const json& j, OrderAck& a) {
    std::string status;
    if (!getU64(j, "order_id", a.order_id) || !getString(j, "status", status) ||
        !parseStatus(status, a.status) || !getU64(j, "remaining_vol", a.remaining_vol)) {
        return false;
    }
    auto it = j.find("trades");
    if (it == j.end()) return true;
    if (!it->is_array() || it->size() > UINT16_MAX) return false;
    for (const json& t : *it) {
        Trade tr;
        if (!t.is_object() || !getU64(t, "trade_id", tr.trade_id) || !getU64(t, "price", tr.price) ||
            !getU64(t, "vol", tr.vol) || !getU64(t, "counterpart_id", tr.counterpart_id)) {
            return false;
        }
        a.trades.push_back(tr);
    }
    return true;
}

bool getListFeed(const json& j, ListFeedRsp& rsp) {
    auto groups = j.find("groups");
    if (groups == j.end() || !groups->is_array() || groups->size() > UINT8_MAX) return false;
    for (const json& g : *groups) {
        GroupInfo info;
        uint64_t id = 0;
        auto tickers = g.find("tickers");
        if (!g.is_object() || !getU64(g, "group_id", id) || id > UINT8_MAX ||
            !getString(g, "name", info.name) || info.name.size() > UINT8_MAX ||
            tickers == g.end() || !tickers->is_array() || tickers->size() > UINT8_MAX) {
            return false;
        }
        info.group_id = static_cast<uint8_t>(id);
        for (const json& t : *tickers) {
            uint64_t symbol = 0;
            std::string name;
            if (!t.is_object() || !getU64(t, "symbol_id", symbol) || symbol > UINT16_MAX ||
                !getString(t, "ticker", name) || name.size() > TICKER_LEN) {
                return false;
            }
            TickerInfo ti;
            ti.symbol_id = static_cast<uint16_t>(symbol);
            std::memcpy(ti.ticker.data(), name.data(), name.size());
            info.tickers.push_back(ti);
        }
        rsp.groups.push_back(std::move(info));
    }
    return true;
}

DecodeStatus set(Message& out, MsgType type, Payload payload) {
    out.header.type = type;
    out.payload = std::move(payload);
    return DecodeStatus::Ok;
}

DecodeStatus decodeRequest(const Head& h, string_view body, const SymbolTable& symbols, Message& out) {
    const string_view m = h.method;
    const string_view p = h.path;
    json j;
    if (m == "POST" && (p == "/order/bid" || p == "/order/ask")) {
        const bool bid = p == "/order/bid";
        std::string ticker;
        uint64_t price = 0;
        uint64_t volume = 0;
        if (!parseJson(body, j) || !getString(j, "ticker", ticker) ||
            !getU64(j, bid ? "max_price" : "min_price", price) || !getU64(j, "volume", volume)) {
            return DecodeStatus::Malformed;
        }
        // Ticker -> symbol_id hash lookup: a real cost of the text design.
        return set(out, bid ? MsgType::Bid : MsgType::Ask, OrderRequest{symbols.idOf(ticker), price, volume});
    }
    if (m == "DELETE" && p == "/order/cancel") {
        if (!h.has_order_id) return DecodeStatus::Malformed;
        return set(out, MsgType::Cancel, CancelRequest{h.order_id});
    }
    if (m == "GET" && p == "/feed") return set(out, MsgType::ListFeed, Empty{});
    if (m == "POST" && p == "/feed/subscribe") {
        uint32_t mask = 0;
        uint64_t port = 0;
        if (!parseJson(body, j) || !getMask(j, "groups", mask) || !getU64(j, "udp_port", port) ||
            port > 0xFFFF) {
            return DecodeStatus::Malformed;
        }
        return set(out, MsgType::Subscribe, SubscribeRequest{static_cast<uint16_t>(port), mask});
    }
    if (m == "POST" && p == "/feed/unsubscribe") {
        uint32_t mask = 0;
        if (!parseJson(body, j) || !getMask(j, "groups", mask)) return DecodeStatus::Malformed;
        return set(out, MsgType::Unsubscribe, GroupMask{mask});
    }
    if (m == "POST" && p == "/order/update") {
        OrderAck a;
        if (!parseJson(body, j) || !getOrderAck(j, a)) return DecodeStatus::Malformed;
        return set(out, MsgType::OrderUpdate, std::move(a));
    }
    if (m == "POST" && p == "/session/disconnect") {
        return set(out, MsgType::Disconnect, Disconnect{DisconnectReason::Idle});
    }
    return DecodeStatus::UnknownType;
}

// HTTP responses carry no method, so the message type is recovered from the
// status code and the body's shape.
DecodeStatus decodeResponse(const Head& h, string_view body, Message& out) {
    json j;
    if (!parseJson(body, j)) return DecodeStatus::Malformed;
    if (h.status >= 300 || j.contains("error")) {
        uint64_t code = 0;
        if (!getU64(j, "code", code) || code > 0xFFFF) return DecodeStatus::Malformed;
        return set(out, MsgType::Error, Error{static_cast<ErrorCode>(code)});
    }
    if (j.contains("remaining_vol")) {
        OrderAck a;
        if (!getOrderAck(j, a)) return DecodeStatus::Malformed;
        return set(out, MsgType::OrderAck, std::move(a));
    }
    uint32_t mask = 0;
    if (j.contains("subscribed")) {
        if (!getMask(j, "subscribed", mask)) return DecodeStatus::Malformed;
        return set(out, MsgType::SubscribeAck, GroupMask{mask});
    }
    if (j.contains("unsubscribed")) {
        if (!getMask(j, "unsubscribed", mask)) return DecodeStatus::Malformed;
        return set(out, MsgType::UnsubscribeAck, GroupMask{mask});
    }
    if (j.contains("groups")) {
        ListFeedRsp rsp;
        if (!getListFeed(j, rsp)) return DecodeStatus::Malformed;
        return set(out, MsgType::ListFeedRsp, std::move(rsp));
    }
    CancelAck c;
    std::string status;
    if (!getU64(j, "order_id", c.order_id) || !getString(j, "status", status) ||
        !parseStatus(status, c.status)) {
        return DecodeStatus::Malformed;
    }
    return set(out, MsgType::CancelAck, c);
}

}  // namespace

void TextCodec::encode(const Message& msg, const SymbolTable& symbols, std::vector<uint8_t>& out) {
    const Outgoing o = toHttp(msg, symbols);
    const FrameHeader& h = msg.header;
    std::string s;
    s.reserve(128 + o.body.size());
    s += o.start_line;
    s += "\r\n";
    if (!o.body.empty()) {
        s += "Content-Length: " + std::to_string(o.body.size()) + "\r\n";
        s += "Content-Type: application/json\r\n";
    }
    if (h.client_id != 0) s += "Client-ID: " + std::to_string(h.client_id) + "\r\n";
    if (h.callback_port != 0) s += "Callback-Port: " + std::to_string(h.callback_port) + "\r\n";
    if (h.req_id != 0) s += "Request-ID: " + std::to_string(h.req_id) + "\r\n";
    s += o.extra_headers;
    s += "\r\n";
    s += o.body;
    out.insert(out.end(), s.begin(), s.end());
}

DecodeStatus TextCodec::decode(const uint8_t* data, size_t len, const SymbolTable& symbols,
                               uint32_t max_body, Message& out, size_t& consumed) {
    const string_view buf(reinterpret_cast<const char*>(data), len);
    const size_t head_end = buf.substr(0, MAX_HEADER_BYTES).find("\r\n\r\n");
    if (head_end == string_view::npos) {
        return len >= MAX_HEADER_BYTES ? DecodeStatus::Malformed : DecodeStatus::Incomplete;
    }

    Head h;
    const string_view head = buf.substr(0, head_end);
    const size_t eol = head.find("\r\n");
    DecodeStatus status = parseStartLine(head.substr(0, eol), h);
    if (status != DecodeStatus::Ok) return status;
    string_view rest = eol == string_view::npos ? string_view() : head.substr(eol + 2);
    while (!rest.empty()) {
        const size_t e = rest.find("\r\n");
        if (!parseHeaderLine(rest.substr(0, e), h)) return DecodeStatus::Malformed;
        rest = e == string_view::npos ? string_view() : rest.substr(e + 2);
    }

    if (h.content_length > max_body) return DecodeStatus::Malformed;
    const size_t total = head_end + 4 + h.content_length;
    if (len < total) return DecodeStatus::Incomplete;
    consumed = total;

    out.header = FrameHeader{};
    out.header.length = static_cast<uint32_t>(h.content_length);
    out.header.req_id = static_cast<uint32_t>(h.req_id);
    out.header.client_id = h.client_id;
    out.header.callback_port = static_cast<uint16_t>(h.callback_port);
    const string_view body = buf.substr(head_end + 4, h.content_length);
    return h.response ? decodeResponse(h, body, out) : decodeRequest(h, body, symbols, out);
}

}  // namespace codec
