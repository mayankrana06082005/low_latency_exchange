-- Wireshark dissector for the exchange's custom binary protocol
-- (protocol_design_and_plan.md, Section 4).
--
--   TCP: 20-byte frame header + typed payload, big-endian, many frames per segment
--        or one frame split across segments.
--   UDP: fixed 128-byte market data snapshot.
--
-- Ports are not fixed (the simulation uses random ports, pushes go to client
-- callback ports), so both dissectors are heuristic: they recognise the protocol
-- from the bytes. Port 9000 (TCP) is also registered for "Decode As".
--
-- Install:  ln -s "$PWD/tools/exchange.lua" ~/.local/lib/wireshark/plugins/
-- Or once:  wireshark -X lua_script:tools/exchange.lua capture.pcapng

local HEADER_LEN = 20
local VERSION = 1
local MAX_FRAME = 64 * 1024

local msg_types = {
    [0x01] = "BID", [0x02] = "ASK", [0x03] = "CANCEL", [0x04] = "LIST_FEED",
    [0x05] = "SUBSCRIBE", [0x06] = "UNSUBSCRIBE",
    [0x81] = "ORDER_ACK", [0x83] = "CANCEL_ACK", [0x84] = "LIST_FEED_RSP",
    [0x85] = "SUBSCRIBE_ACK", [0x86] = "UNSUBSCRIBE_ACK", [0x8F] = "ERROR",
    [0xC1] = "ORDER_UPDATE", [0xC2] = "DISCONNECT",
}
local statuses = { [0] = "RESTING", [1] = "PARTIAL_FILL", [2] = "FILLED", [3] = "CANCELLED" }
local errors = {
    [1] = "MALFORMED", [2] = "UNKNOWN_TYPE", [3] = "INVALID_FIELD", [4] = "UNKNOWN_SYMBOL",
    [5] = "ORDER_NOT_FOUND", [6] = "NOT_OWNER", [7] = "QUEUE_FULL", [8] = "UNKNOWN_GROUP",
    [9] = "UNSUPPORTED_VERSION", [10] = "UNKNOWN_CLIENT", [11] = "CLIENT_MISMATCH",
}

-- ======================================================================= TCP

local p = Proto("exchange", "Exchange Binary Protocol")
local f = p.fields
f.length    = ProtoField.uint32("exchange.length", "Payload length", base.DEC)
f.req_id    = ProtoField.uint32("exchange.req_id", "Request ID", base.DEC)
f.client_id = ProtoField.uint64("exchange.client_id", "Client ID", base.DEC)
f.callback  = ProtoField.uint16("exchange.callback_port", "Callback port", base.DEC)
f.type      = ProtoField.uint8("exchange.type", "Type", base.HEX, msg_types)
f.version   = ProtoField.uint8("exchange.version", "Version", base.DEC)
f.symbol    = ProtoField.uint16("exchange.symbol_id", "Symbol ID", base.DEC)
f.price     = ProtoField.uint64("exchange.price", "Price", base.DEC)
f.volume    = ProtoField.uint64("exchange.volume", "Volume", base.DEC)
f.order_id  = ProtoField.uint64("exchange.order_id", "Order ID", base.DEC)
f.status    = ProtoField.uint8("exchange.status", "Status", base.DEC, statuses)
f.remaining = ProtoField.uint64("exchange.remaining", "Remaining volume", base.DEC)
f.n_trades  = ProtoField.uint16("exchange.n_trades", "Trades", base.DEC)
f.trade_id  = ProtoField.uint64("exchange.trade_id", "Trade ID", base.DEC)
f.counter   = ProtoField.uint64("exchange.counterpart_id", "Counterpart order", base.DEC)
f.udp_port  = ProtoField.uint16("exchange.udp_port", "UDP port", base.DEC)
f.groups    = ProtoField.uint32("exchange.group_mask", "Group mask", base.HEX)
f.error     = ProtoField.uint16("exchange.error", "Error", base.DEC, errors)
f.reason    = ProtoField.uint8("exchange.reason", "Disconnect reason", base.DEC, { [0] = "IDLE" })
f.n_groups  = ProtoField.uint8("exchange.n_groups", "Groups", base.DEC)

local function u64(tvb, off) return tvb(off, 8):uint64() end

-- Adds the payload fields for one frame; returns a short summary for the Info column.
local function payload(t, tvb, off, len, tree)
    local name = msg_types[t] or string.format("0x%02x", t)
    if (t == 0x01 or t == 0x02) and len >= 18 then
        tree:add(f.symbol, tvb(off, 2)); tree:add(f.price, tvb(off + 2, 8)); tree:add(f.volume, tvb(off + 10, 8))
        return string.format("%s sym=%d %s@%s", name, tvb(off, 2):uint(), u64(tvb, off + 10), u64(tvb, off + 2))
    elseif t == 0x03 and len >= 8 then
        tree:add(f.order_id, tvb(off, 8))
        return string.format("%s order=%s", name, u64(tvb, off))
    elseif t == 0x05 and len >= 6 then
        tree:add(f.udp_port, tvb(off, 2)); tree:add(f.groups, tvb(off + 2, 4))
        return string.format("%s groups=0x%x udp=%d", name, tvb(off + 2, 4):uint(), tvb(off, 2):uint())
    elseif (t == 0x06 or t == 0x85 or t == 0x86) and len >= 4 then
        tree:add(f.groups, tvb(off, 4))
        return string.format("%s groups=0x%x", name, tvb(off, 4):uint())
    elseif (t == 0x81 or t == 0xC1) and len >= 19 then
        tree:add(f.order_id, tvb(off, 8)); tree:add(f.status, tvb(off + 8, 1))
        tree:add(f.remaining, tvb(off + 9, 8)); tree:add(f.n_trades, tvb(off + 17, 2))
        local n = tvb(off + 17, 2):uint()
        for i = 0, n - 1 do
            local to = off + 19 + i * 32
            if to + 32 > off + len then break end
            local tt = tree:add(tvb(to, 32), string.format("Trade %s@%s", u64(tvb, to + 16), u64(tvb, to + 8)))
            tt:add(f.trade_id, tvb(to, 8)); tt:add(f.price, tvb(to + 8, 8))
            tt:add(f.volume, tvb(to + 16, 8)); tt:add(f.counter, tvb(to + 24, 8))
        end
        return string.format("%s order=%s %s rem=%s trades=%d", name, u64(tvb, off),
                             statuses[tvb(off + 8, 1):uint()] or "?", u64(tvb, off + 9), n)
    elseif t == 0x83 and len >= 9 then
        tree:add(f.order_id, tvb(off, 8)); tree:add(f.status, tvb(off + 8, 1))
        return string.format("%s order=%s %s", name, u64(tvb, off), statuses[tvb(off + 8, 1):uint()] or "?")
    elseif t == 0x84 and len >= 1 then
        tree:add(f.n_groups, tvb(off, 1))
        return string.format("%s groups=%d", name, tvb(off, 1):uint())
    elseif t == 0x8F and len >= 2 then
        tree:add(f.error, tvb(off, 2))
        return string.format("%s %s", name, errors[tvb(off, 2):uint()] or "?")
    elseif t == 0xC2 and len >= 1 then
        tree:add(f.reason, tvb(off, 1))
        return name .. " idle"
    end
    return name
end

local function looks_like_header(tvb, off)
    if tvb:len() - off < HEADER_LEN then return false end
    return tvb(off + 19, 1):uint() == VERSION and msg_types[tvb(off + 18, 1):uint()] ~= nil
        and tvb(off, 4):uint() <= MAX_FRAME
end

-- Walks every frame in the segment, asking TCP for more bytes when a frame is split.
local function dissect_tcp(tvb, pinfo, tree)
    local off, total, infos = 0, tvb:len(), {}
    while off < total do
        if total - off < HEADER_LEN then
            pinfo.desegment_offset = off
            pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
            break
        end
        local plen = tvb(off, 4):uint()
        if total - off < HEADER_LEN + plen then
            pinfo.desegment_offset = off
            pinfo.desegment_len = HEADER_LEN + plen - (total - off)
            break
        end
        local t = tvb(off + 18, 1):uint()
        local sub = tree:add(p, tvb(off, HEADER_LEN + plen))
        sub:add(f.length, tvb(off, 4)); sub:add(f.req_id, tvb(off + 4, 4))
        sub:add(f.client_id, tvb(off + 8, 8)); sub:add(f.callback, tvb(off + 16, 2))
        sub:add(f.type, tvb(off + 18, 1)); sub:add(f.version, tvb(off + 19, 1))
        local info = payload(t, tvb, off + HEADER_LEN, plen, sub)
        local req = tvb(off + 4, 4):uint()
        if req ~= 0 then info = info .. " req=" .. req end
        sub:append_text(": " .. info)
        infos[#infos + 1] = info
        off = off + HEADER_LEN + plen
    end
    if #infos > 0 then
        pinfo.cols.protocol = "EXCHANGE"
        pinfo.cols.info = table.concat(infos, " | ")
    end
    return off
end

function p.dissector(tvb, pinfo, tree) return dissect_tcp(tvb, pinfo, tree) end

local function heuristic_tcp(tvb, pinfo, tree)
    if not looks_like_header(tvb, 0) then return false end
    pinfo.conversation = p  -- keep decoding the rest of this connection as exchange
    dissect_tcp(tvb, pinfo, tree)
    return true
end

DissectorTable.get("tcp.port"):add(9000, p)
p:register_heuristic("tcp", heuristic_tcp)

-- ======================================================================= UDP

local u = Proto("exchange_md", "Exchange Market Data")
local g = u.fields
g.seq     = ProtoField.uint64("exchange_md.seq", "Sequence", base.DEC)
g.ts      = ProtoField.uint64("exchange_md.timestamp_us", "Timestamp (us since epoch)", base.DEC)
g.group   = ProtoField.uint32("exchange_md.group_id", "Group", base.DEC)
g.ticker  = ProtoField.string("exchange_md.ticker", "Ticker")
g.n_bids  = ProtoField.uint8("exchange_md.n_bids", "Bid levels", base.DEC)
g.n_asks  = ProtoField.uint8("exchange_md.n_asks", "Ask levels", base.DEC)
g.price   = ProtoField.uint64("exchange_md.price", "Price", base.DEC)
g.volume  = ProtoField.uint64("exchange_md.volume", "Volume", base.DEC)

local function levels(tvb, tree, off, n, label)
    local parts = {}
    for i = 0, n - 1 do
        local lo = off + i * 16
        local lt = tree:add(tvb(lo, 16), string.format("%s %d: %s@%s", label, i + 1, u64(tvb, lo + 8), u64(tvb, lo)))
        lt:add(g.price, tvb(lo, 8)); lt:add(g.volume, tvb(lo + 8, 8))
        parts[#parts + 1] = string.format("%s@%s", u64(tvb, lo + 8), u64(tvb, lo))
    end
    return #parts > 0 and table.concat(parts, " ") or "-"
end

local function heuristic_udp(tvb, pinfo, tree)
    if tvb:len() ~= 128 then return false end
    local nb, na = tvb(28, 1):uint(), tvb(29, 1):uint()
    if nb > 3 or na > 3 or tvb(30, 2):uint() ~= 0 then return false end
    local sub = tree:add(u, tvb(0, 128))
    sub:add(g.seq, tvb(0, 8)); sub:add(g.ts, tvb(8, 8)); sub:add(g.group, tvb(16, 4))
    sub:add(g.ticker, tvb(20, 8)); sub:add(g.n_bids, tvb(28, 1)); sub:add(g.n_asks, tvb(29, 1))
    local ticker = tvb(20, 8):stringz()
    local bids = levels(tvb, sub, 32, nb, "Bid")
    local asks = levels(tvb, sub, 80, na, "Ask")
    pinfo.cols.protocol = "EXCH-MD"
    pinfo.cols.info = string.format("seq=%s group=%d %s bids %s | asks %s", u64(tvb, 0), tvb(16, 4):uint(),
                                    ticker, bids, asks)
    return true
end

u:register_heuristic("udp", heuristic_udp)
