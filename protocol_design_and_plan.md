# Low Latency Trading Exchange: Protocol Design and Prototype Plan

Status: v0.5. Phases 0-9 implemented (Section 12); review items 4-14 still open. Section 11 holds build setup.
Scope: networking prototype only (TCP channel, UDP channel, dispatcher, workers). Order book logic is stubbed until Phase 9.

## 1. Goals

1. Finalize the application-layer wire format for the TCP and UDP channels.
2. Minimize bytes on the wire and CPU spent on parsing and routing.
3. Build a text-mode (HTTP/1.1 + JSON) codec as a measured baseline, so the binary design is justified by numbers.
4. Keep thread management minimal: one dispatcher thread, N single-threaded workers. Each worker owns its order books and its UDP sending.

Non-goals: TLS, persistence, order types other than limit, dispatcher thread pool, real matching engine (stub only).

## 2. Decision Log

| # | Decision | Source |
|---|---|---|
| D1 | Binary framing, HTTP-inspired (method and path become a 1-byte type, content-length becomes a length field, request/response pairing kept via `req_id`) | Design |
| D2 | All multi-byte integers in network byte order (big-endian) | Confirmed |
| D3 | Price and volume are `uint64_t`, behind `using Price = uint64_t; using Volume = uint64_t;` | Confirmed |
| D4 | Maximum 32 ticker groups, so a group set is a `uint32_t` bitmask | Confirmed |
| D5 | Frame length field is `uint32_t` | Confirmed |
| D6 | UDP packet carries the ticker as `char[8]` | Confirmed |
| D7 | Worker state is keyed by `order_id`, never by `conn_id` | Confirmed |
| D8 | Text-mode HTTP/1.1 + JSON codec built as comparison baseline | Confirmed |
| D9 | `order_id = (worker_id << 56) \| local_counter`, so CANCEL routes with a shift and no lookup | Design, pending confirmation |
| D10 | Orders carry a `u16 symbol_id` learned from LIST_FEED | Confirmed |
| D11 | Server assigns a random `u64 client_id` on first contact. It travels in the header of every frame in both directions (0 = unassigned). It is the identity and ownership key. No security in the prototype. | Confirmed |
| D12 | Dispatcher routing and ownership checks keyed by `client_id` (`order_id -> client_id`). Workers stay keyed by `order_id` only. | Follows D7 and D11 |
| D13 | Every TCP connection has an idle timer, reset by any state change or inbound frame. On expiry the server sends DISCONNECT and closes. Later updates for a client arrive over a server-initiated connection to its callback port, which becomes a full live session. | Confirmed, details in Section 3.5 |
| D14 | Idle timer is static, 1 s by default, launch option. The adaptive 2x connect-time idea is dropped for the prototype. | Confirmed |
| D15 | No resync request in the prototype | Confirmed |
| D16 | One worker per ticker group | Confirmed |
| D17 | UDP unicast fan-out | Confirmed |
| D18 | Linux and macOS, C++17, nlohmann/json for the text baseline, busy-polling dispatcher, `poll()` behind a `Poller` interface (epoll and kqueue are Linux-only and mac-only) | Confirmed, `Poller` is design |
| D19 | Cancel of a filled order and cancel of an unknown order both return ORDER_NOT_FOUND | Confirmed |
| D20 | No overflow handling and no `n_trades` edge-case handling; all values stay within 32/64 bits | Confirmed |
| D21 | No HELLO/WELCOME handshake. `client_id` rides in the header, so reconnect costs only the TCP connect. Header grows to 20 bytes. | Confirmed |
| D22 | UDP subscriptions have no timeout. They live until explicit UNSUBSCRIBE. | Confirmed |
| D23 | Idle policy is a runtime option: `never` or `fixed` | Confirmed |
| D24 | Push failure policy kept simple; mailbox coalesces per `order_id` and never drops an update | Confirmed (v0.5), details in Section 3.5 |
| D25 | Overriding goal is latency. Header size may grow when it buys latency. | Confirmed |
| D26 | A nonzero `client_id` not in the registry is rejected with ERROR UNKNOWN_CLIENT. The client resets to 0. | Confirmed |
| D27 | `callback_port` stays in every frame header | Confirmed |
| D28 | UDP feeds close only via UNSUBSCRIBE. No server-side closing, no CLOSE message. | Confirmed |
| D29 | Workers know only `endpoint -> group mask`, never clients. One UDP endpoint per client, newest claimant owns an endpoint. | Confirmed |

## 3. Prototype Architecture

```
Clients --TCP--> Dispatcher thread --SPSC in-queue--> Worker k (book + UDP send)
Clients <-TCP--- Dispatcher thread <-SPSC out-queue-- Worker k
Clients <------------------UDP unicast------------- Worker k
```

Threads: 1 dispatcher + N workers. No other threads. Each (dispatcher, worker) pair has exactly one producer and one consumer per queue, so lock-free SPSC rings are sufficient and there are no locks anywhere on the fast path.

### 3.1 Ownership rules

- The worker owns: order books, `order_id` counter, resting orders (keyed by `order_id`), subscriber UDP endpoint list for its groups, UDP socket.
- The dispatcher owns: listening socket, `Connection` objects (internal `conn_id`), `ClientRegistry` keyed by `client_id`, the `order_id -> client_id` table, `symbol_id -> worker` table.
- `client_id` travels inside a work item as an opaque reply token and is echoed back with the response. The worker never indexes or stores anything by it. The dispatcher resolves it to a live connection, or to the client mailbox if none exists. `conn_id` never leaves the dispatcher.
- When an ORDER_ACK passes through the dispatcher it records `order_id -> client_id` and, for RESTING or PARTIAL_FILL, increments that client's resting count. Terminal states remove the entry. A later fill arrives from the worker as an ORDER_UPDATE carrying only `order_id` and is routed through this table, so the worker needs no knowledge of clients or connections.

### 3.2 Dispatcher loop (round robin)

Each iteration, in fixed order, with a per-step budget so no step starves the others:

1. Accept connections and read from ready sockets (up to K frames per connection). Decode, validate, route to worker in-queue.
2. Drain each worker out-queue in turn (up to K items each). Encode and append to the owning client's connection write buffer, or to its mailbox if no connection is live.
3. Flush write buffers with non-blocking `send`; on `EAGAIN` keep the fd in the write-interest set.
4. Check idle deadlines (front of the deadline list only). For each expired connection send DISCONNECT and start draining (Section 3.5).
5. Advance push connections: start a non-blocking `connect` for clients that have mailbox entries, no live connection and a nonzero callback port; finish pending connects.

The loop busy-polls: `poll()` with timeout 0, queues spun on. The `Poller` interface keeps the backend swappable for `epoll` or `kqueue`.

Backpressure: if a worker in-queue is full, reply `ERROR QUEUE_FULL`. If a connection write buffer exceeds a cap, close the connection.

### 3.3 Worker loop

1. Pop and process work items (orders, cancels, subscription control items).
2. After each item, check the snapshot timer. When due, build a Top-3 snapshot per ticker and `sendto` each subscriber (non-blocking socket).
3. Phase 5 uses a fixed snapshot interval. EWMA adaptive timeout from the SRS (`Ttimeout = T_UDP + dt_safety`) is added in Phase 7.

Deliberate deviation from SRS NFR-2: matching and UDP send share one thread in this prototype. A blocked `sendto` would stall matching, so the socket is non-blocking and drops on `EAGAIN`. This must be stated in the demo.

### 3.4 Subscriptions

- SUBSCRIBE carries `u16 udp_port` and a group mask. The UDP endpoint is the peer IP of the client connection plus `udp_port`. One UDP endpoint per client; a later SUBSCRIBE replaces it.
- An endpoint belongs to at most one client. If a new client claims an endpoint held by another `client_id` (typical after a client restart gets a fresh id), the dispatcher first sends unsubscribe-all for the old owner. This prevents duplicate datagrams to one endpoint.
- The dispatcher sends control items (endpoint, group mask) through the in-queue of each worker owning an affected group. Workers store `endpoint -> group mask` and know nothing about clients. Control items share the order queue, so ordering between subscription changes and orders is preserved.
- Lifetime: a subscription lives until explicit UNSUBSCRIBE. TCP close, idle timeout and push failure never touch it. Consequence: a client that dies without UNSUBSCRIBE keeps receiving datagrams until its endpoint is claimed by a newer client or the server restarts. An unconnected UDP socket gives no error feedback, so the server cannot detect the death. Accepted for the demo.

### 3.5 Client Identity, Idle Timer and Push Delivery

Registry entry per client: `client_id`, current connection (nullable), peer IP, `callback_port`, UDP endpoint, resting count, mailbox (one entry per `order_id`).

Identity (no handshake):
- Every frame carries `client_id` in the header, both directions. A client sends 0 until it has been assigned one.
- First frame with id 0 on a connection: the dispatcher generates a random nonzero 64-bit id (checked for collision), creates the registry entry, binds the connection, and the response header carries the new id. The client stores it and sends it on every later frame and connection.
- Further id-0 frames on an already-bound connection mean "the bound id". Without this rule, requests pipelined before the first response would each create a new client.
- First frame with a nonzero id on a fresh connection: one registry lookup. Unknown: ERROR UNKNOWN_CLIENT. Known: bind, close the client previous connection if any (newest wins), flush the mailbox.
- Later frames on a bound connection: one integer compare of the header id against the bound id. No hash lookup on the hot path. Nonzero mismatch: ERROR CLIENT_MISMATCH and close.
- `callback_port` in the header updates the registry entry when it changes.

Idle timer:
- Every connection, from accept, has `deadline = last activity + x`. `x` is static, default 1 s, launch option. Activity is any inbound frame and any frame sent to the client (order ack, fill update, cancel ack).
- Constant `x` keeps deadlines monotonic, so deadlines live in a FIFO list: reset moves the connection to the back, the expiry check looks at the front only.
- On expiry the server sends DISCONNECT (reason 0, idle) and enters draining: it keeps processing and answering frames already in flight, then closes after a short grace period `g` (default 100 ms, launch option) or when the client closes first. This avoids dropping a request that crossed the DISCONNECT on the wire. Dropping it would leave the client unsure whether an order was placed.
- The client library treats DISCONNECT as normal: it stops using the connection, and reconnects lazily on its next request with its saved `client_id`. It never auto-resends a request whose outcome is unknown.
- A client with no resting orders has nothing to push, so its connection simply closes at expiry. DISCONNECT is sent anyway so the client can tell an idle close from an error.
- Idle policy is a runtime option: `never` (no deadline) or `fixed` (default). `never` is the keep-open baseline for the benchmark.

Push delivery:
- An update arrives for a client with no live connection: merge it into the mailbox (rules below). If `callback_port` is nonzero and no connect is in progress, start a non-blocking `connect` to (peer IP, `callback_port`) with a 1 s timeout.
- On success: the connection becomes the client live session, bound to its `client_id`, deadline `now + x`, mailbox flushed in order. Frames from the server carry `client_id` in the header, so the client attributes the connection with no handshake. The client may send requests on it.
- On failure or timeout: no retry. The mailbox is kept. The next attempt happens on the next update for that client, or the mailbox is flushed when the client next connects by itself.
- Mailbox coalescing: the mailbox holds at most one entry per `order_id`, kept in first-insertion order. An ORDER_UPDATE for an order already in the mailbox overwrites `status` and `remaining_vol` (both absolute) and appends its trades; an update for a new order adds an entry. Nothing is ever dropped, so a lost fill is impossible without any resync. Size is bounded by the client's open or recently terminated orders, not by fill count. Flush sends one ORDER_UPDATE per entry in order, then clears the mailbox.
- Defaults above (timeout 1 s, no retry, `g` = 100 ms) are my choices; change them if wrong.

Design note: x = 1 s means any client slower than one action per second pays one TCP connect (1 RTT) before its next request. The `never` policy shows exactly what that costs. Holding an idle TCP connection costs one fd and socket buffers (macOS default `ulimit -n` is 256), so the timer only pays off where idle connections are scarce.

Deployment warning: push needs inbound TCP to the client. Laptop firewalls (macOS, ufw) and access-point client isolation on institute wifi can block it. UDP unicast fan-out depends on the same reachability. Test on the real network early.

## 4. Binary Protocol

### 4.1 Conventions

- Big-endian on the wire. Use `htobe64`/`be64toh` family or a small `ByteWriter`/`ByteReader` pair. Do not `memcpy` a host struct onto the wire: with big-endian the in-memory struct is not the wire layout.
- Structs in code are logical host-side messages. The wire layout below is the contract and is validated by round-trip tests.
- Parsing never trusts lengths. Every field read is bounds-checked against the frame length.
- `MAX_FRAME_LEN` (default 64 KiB, configurable) is enforced even though the field is `u32`. A larger value is treated as malformed framing and the connection is closed (NFR-4). Without this cap a hostile length field forces a multi-GB allocation.

### 4.2 TCP frame header (20 bytes)

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 4 | `length` | Payload bytes following the header. Excludes header. |
| 4 | 4 | `req_id` | Client-chosen, echoed in the response. Enables pipelining. 0 in server-initiated frames. |
| 8 | 8 | `client_id` | Random, server-assigned. 0 means unassigned. In every frame, both directions. |
| 16 | 2 | `callback_port` | Port where the server may open a connection to push updates. 0 means no push. Confirmed (D27). |
| 18 | 1 | `type` | Message type. High bit set means response or server-initiated. |
| 19 | 1 | `version` | Protocol version, currently 1. Sanity check against garbage streams. |

Every field is naturally aligned (`client_id` at offset 8). The header grew by 8 bytes against v0.2. A BID frame is still 38 bytes, far inside one TCP segment, so the extra bytes cost almost nothing on the wire, while removing the handshake saves one round trip on every reconnect, which the idle timer makes frequent. The reserved `flags` field is gone; the `version` byte is the evolution path.

### 4.3 Message types

| Value | Name | Direction |
|---|---|---|
| 0x01 | BID | C to S |
| 0x02 | ASK | C to S |
| 0x03 | CANCEL | C to S |
| 0x04 | LIST_FEED | C to S |
| 0x05 | SUBSCRIBE | C to S |
| 0x06 | UNSUBSCRIBE | C to S |
| 0x81 | ORDER_ACK | S to C |
| 0x83 | CANCEL_ACK | S to C |
| 0x84 | LIST_FEED_RSP | S to C |
| 0x85 | SUBSCRIBE_ACK | S to C |
| 0x86 | UNSUBSCRIBE_ACK | S to C |
| 0x8F | ERROR | S to C |
| 0xC1 | ORDER_UPDATE | S to C, unsolicited, `req_id` = 0 |
| 0xC2 | DISCONNECT | S to C, unsolicited, server will close |

### 4.4 Request payloads

| Type | Payload | Size |
|---|---|---|
| BID, ASK | `u16 symbol_id, u64 price, u64 volume` | 18 |
| CANCEL | `u64 order_id` | 8 |
| LIST_FEED | empty | 0 |
| SUBSCRIBE | `u16 udp_port, u32 group_mask` | 6 |
| UNSUBSCRIBE | `u32 group_mask` | 4 |

BID and ASK are separate types, so there is one `price` field instead of the SRS `max_price` / `min_price`.

### 4.5 Response payloads

| Type | Payload |
|---|---|
| ORDER_ACK | `u64 order_id, u8 status, u64 remaining_vol, u16 n_trades, Trade[n_trades]` |
| Trade | `u64 trade_id, u64 price, u64 vol, u64 counterpart_id` (32 bytes) |
| CANCEL_ACK | `u64 order_id, u8 status` |
| LIST_FEED_RSP | `u8 n_groups`, then per group: `u8 group_id, u8 name_len, name bytes, u8 n_tickers`, then per ticker: `u16 symbol_id, char[8] ticker` (NUL padded) |
| SUBSCRIBE_ACK | `u32 group_mask` of groups newly subscribed |
| UNSUBSCRIBE_ACK | `u32 group_mask` of groups actually removed (idempotent: `requested & subscribed`) |
| ORDER_UPDATE | Same payload as ORDER_ACK. For a later fill of a resting order, `trades` holds the new fills for that order and `remaining_vol` is the new remainder. |
| DISCONNECT | `u8 reason` (0 = idle) |
| ERROR | `u16 code` |

Status enum: `0 RESTING, 1 PARTIAL_FILL, 2 FILLED, 3 CANCELLED`. One naming only; the SRS uses both PARTIALLY FILLED / PARTIAL FILL and COMPLETELY FILLED / FILLED.

Error codes: `1 MALFORMED, 2 UNKNOWN_TYPE, 3 INVALID_FIELD (zero price or volume), 4 UNKNOWN_SYMBOL, 5 ORDER_NOT_FOUND, 6 NOT_OWNER, 7 QUEUE_FULL, 8 UNKNOWN_GROUP, 9 UNSUPPORTED_VERSION, 10 UNKNOWN_CLIENT (nonzero `client_id` not in registry), 11 CLIENT_MISMATCH`. HTTP equivalents for the text codec: 400 / 404 / 422 / 429-style as appropriate.

### 4.6 UDP market data datagram (fixed 128 bytes)

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `seq` (u64, per group, increments per datagram sent for that group) |
| 8 | 8 | `timestamp_us` (u64, Unix epoch microseconds) |
| 16 | 4 | `group_id` (u32) |
| 20 | 8 | `ticker` (char[8], NUL padded) |
| 28 | 1 | `n_bids` (0 to 3) |
| 29 | 1 | `n_asks` (0 to 3) |
| 30 | 2 | reserved (0) |
| 32 | 48 | `bids[3]`, each `u64 price, u64 vol` |
| 80 | 48 | `asks[3]`, each `u64 price, u64 vol` |

Total 128 bytes, far below any MTU, so no fragmentation. Unused levels are zeroed and excluded by `n_bids`/`n_asks`. Gap detection: a client tracking one expected `seq` per group sees a gap as `seq > expected`.

### 4.7 Size budget

| Message | Binary (bytes) | Text, HTTP/1.1 + JSON (approx.) |
|---|---|---|
| BID | 20 + 18 = 38 | about 190 |
| CANCEL | 20 + 8 = 28 | about 140 |
| ORDER_ACK, resting, 0 trades | 20 + 19 = 39 | about 165 |
| ORDER_ACK, 1 trade | 20 + 19 + 32 = 71 | about 235 |

Text numbers are estimates; the benchmark (Section 8) reports exact measured sizes.

## 5. Text Baseline Protocol

Purpose: same logical messages as the SRS tables, encoded as HTTP/1.1 + JSON, to quantify what the binary format buys.

Example request:

```
POST /order/bid HTTP/1.1\r\n
Content-Length: 60\r\n
Content-Type: application/json\r\n
Client-ID: 8841120934410029\r\n
Callback-Port: 9200\r\n
\r\n
{"ticker":"AAPL","type":"LIMIT","max_price":180,"volume":50}
```

Example CANCEL:

```
DELETE /order/cancel HTTP/1.1\r\n
Order-ID: 1042\r\n
Client-ID: 8841120934410029\r\n
\r\n
```

Rules for a fair comparison:
- Same worker, same queues, same dispatcher loop, same stub book. Only the codec differs.
- Text mode listens on a separate port from binary mode.
- The text codec resolves ticker string to `symbol_id` at decode time (hash lookup). This is a real cost of the text design and is counted.
- `req_id` for text mode is a server-side per-connection counter, since responses are strictly ordered per connection in HTTP/1.1.
- Parser is hand-written for the request line and headers. JSON bodies use nlohmann/json.
- `client_id` and `callback_port` travel as headers `Client-ID` and `Callback-Port` on every request. Responses carry `Client-ID`. SUBSCRIBE body: `{"groups":[1,2],"udp_port":9100}`. Header parsing cost is part of the text baseline.
- Pushes: on a server-initiated connection the server writes `POST /order/update` and `POST /session/disconnect` requests, each with a `Client-ID` header, (server acting as HTTP client) and the client parses them. Both codecs therefore need request and response parsing on both sides.

## 6. Code Structure

Internal message passed through queues (POD, no wire bytes):

```cpp
struct WorkItem {
    uint64_t client_id;    // opaque reply token, echoed back
    uint32_t req_id;
    MsgType  type;
    uint16_t symbol_id;
    Price    price;
    Volume   volume;
    uint64_t order_id;
};
```

Control items (subscribe, unsubscribe; each carries an endpoint and a group mask) use a separate tagged type carrying a `sockaddr_in` and group mask, sent through the same queue.

Modules:

| Module | Responsibility |
|---|---|
| `wire/ByteWriter, ByteReader` | Bounds-checked big-endian primitives |
| `wire/Messages` | Logical message structs, enums |
| `codec/ICodec` | `feed(bytes)` yields decoded requests; `encode(response, out)` |
| `codec/BinaryCodec`, `codec/TextCodec` | The two implementations |
| `net/FrameReader` | Stream reassembly per connection (partial reads, multiple frames per read) |
| `net/Connection` | Socket, read buffer, write buffer, codec instance, subscription set |
| `net/Poller` | `poll()` backend now; epoll or kqueue later |
| `net/Dispatcher` | Loop, routing tables, round-robin servicing, idle deadlines |
| `net/ClientRegistry` | `client_id` entries, mailbox, resting counts |
| `net/PushConnector` | Server-initiated non-blocking connects with timeout |
| `core/SpscQueue<T>` | Lock-free single-producer single-consumer ring |
| `core/Worker` | Loop, stub book, `order_id` generation |
| `net/UdpPublisher` | Snapshot serialization and `sendto` fan-out |
| `client/ClientSession`, `client/UdpListener`, `client/CallbackListener` | Programmatic API, lazy reconnect, seq gap detection, push accept loop, callbacks |

Each function does one job. Classes own one responsibility each; no function should grow past a screen.

## 7. Robustness Rules

- EOF, connection reset, unknown version, oversize length, or truncated field: close that connection and free its buffers. Closing a connection never touches UDP subscriptions or resting orders. Other connections and all books are unaffected.
- Unknown `type` with valid framing: reply `ERROR UNKNOWN_TYPE`, keep the connection.
- Zero price or zero volume: `ERROR INVALID_FIELD`, never reaches a worker.
- CANCEL for an `order_id` whose owner is a different client: `ERROR NOT_OWNER` (checked by the dispatcher from its `order_id -> client_id` table). Ownership is only as strong as the `client_id`; there is no authentication in the prototype.

## 8. Benchmark Plan

Same harness, run against both codecs:

1. Wire bytes per message type (exact).
2. Codec CPU cost per message: encode plus decode, in-process microbenchmark, no sockets, ns per message.
3. Dispatch latency: time from end of frame read to queue push (steady clock or TSC), p50 / p99 / p99.9. Target from SRS: at most 50 microseconds.
4. Round-trip latency with stub worker, loopback, M clients: p50 / p99 / p99.9.
5. Peak throughput in messages per second, single dispatcher.
6. Repeat 4 and 5 on the real LAN with two machines.
7. Idle policy comparison (`never` versus `fixed` with `x` swept over 100 ms, 1 s, 5 s): order placement latency after an idle gap, ORDER_UPDATE delivery latency for a resting order, connections opened per minute.

Report as tables plus one latency histogram per codec.

## 9. Implementation Phases

| Phase | Deliverable | Exit criteria |
|---|---|---|
| 0 | CMake skeleton, ASan/UBSan/TSan build configs, test framework | Empty tests pass in all three sanitizer builds |
| 1 | `ByteWriter/Reader`, message structs, `BinaryCodec` | Round-trip tests for every message; static_asserts on sizes; fixed golden byte vectors |
| 2 | Binary `FrameReader` | Handles 1-byte-at-a-time feeds, multi-frame reads, oversize length, bad version; randomized split-point and byte-mutation tests run for 10 minutes without sanitizer errors (libFuzzer is unavailable on Apple clang) |
| 3 | `TextCodec` and HTTP parser | Same logical request through both codecs yields identical `WorkItem`; golden text vectors |
| 4 | `SpscQueue<T>` | Two-thread stress test clean under TSan |
| 5 | Worker stub plus `UdpPublisher` (fixed interval) | Worker acks orders as RESTING with correct `order_id` encoding; UDP packet decodes to expected bytes |
| 6 | `Poller`, Dispatcher, `Connection`, `ClientRegistry`, `client_id` in header | End-to-end: BID with `client_id` 0 gets ORDER_ACK carrying a new id; pipelined id-0 frames on one connection share one id; CANCEL routes to correct worker; reconnect with same id resumes; unknown or mismatched id rejected; malformed input closes only that connection |
| 7 | UDP listener, subscribe/unsubscribe (endpoint-keyed in workers), EWMA snapshot timeout | Seq gap detected under induced loss; idempotent unsubscribe returns correct mask; TCP close of any kind leaves subscriptions intact; an endpoint claimed by a newer client replaces the old owner |
| 8 | Idle timer, DISCONNECT, `PushConnector`, mailbox, client `CallbackListener` | Resting order idles out, later fill arrives via server-initiated connection; unreachable client keeps mailbox, repeated fills of one order coalesce into one entry with no fill lost; a request in flight during idle expiry is still answered; no lost or reordered updates across disconnect |
| 9 | Benchmark harness and results | Tables from Section 8 on loopback and LAN, including idle policy comparison |
| 10 | Replace stub with real pro-rata book | Out of scope for this document |

## 10. Corrections and Gaps in the SRS

- SUBSCRIBE does not carry a UDP port, but FR-7 requires recording the client UDP endpoint. Port added to the SUBSCRIBE payload.
- `max_price` / `min_price` replaced by `price`.
- The SRS has no way to report a later fill of a resting order and no client identity. Added: `client_id` and `callback_port` in the frame header, ORDER_UPDATE, idle timer, push delivery (Section 3.5).
- Status names inconsistent across sections; unified in Section 4.5.
- Cancel of a fully filled order and cancel of a nonexistent order both map to 404 and are deliberately treated the same (D19).
- `seq_num` is "per group" while datagrams are per ticker. Consistent only if the group counter increments per datagram. Stated explicitly in Section 4.6.
- FR-3 pro-rata formula is garbled (sum placed in the numerator, `Order Vol_i` in the denominator). Standard form: `share_i = floor(Vol_aggressive * OrderVol_i / TotalRestingVol)`. The product can overflow `u64`; deliberately not handled in the prototype (D20).
- FR-3 says remainder is resolved by time priority; the intended rule may be size priority with time tie-break. Order book concern, deferred.
- NFR-2 (matching never blocked by UDP) is violated by design in this prototype; see Section 3.3.
- NFR-3 asks for only 250 operations per second per worker, which is orders of magnitude below what this design should reach. Consider restating the target.

## 11. Build Setup and Repository Layout

- CMake >= 3.20, C++17, no other build tools. Presets: `debug`, `release` (use for benchmarks), `asan`, `ubsan`, `tsan`.
- Tests use doctest. The text baseline uses nlohmann/json (header only). Both are fetched and pinned by CMake on the first configure.
- Client library in C++ only.
- Layout: `build/` (generated), `include/` (headers), `src/` (sources), `tests/`, plus `CMakeLists.txt` and `CMakePresets.json`.
- On Linux kernels with high ASLR entropy, TSan binaries must run under `setarch $(uname -m) -R`.

## 12. Implementation Status (v0.5)

All phases 0-9 are implemented. Phase 10 (pro-rata book) remains out of scope; the worker uses a price-time stub book that exists to generate fills.

Programs: `exchange_server`, `exchange_client` (interactive), `simulation` (scripted, 47 checks with full frame trace), `benchmark` (Section 8), `unit_tests` (49 test cases). All pass under ASan, UBSan and TSan.

### 12.1 Deviations from the plan

- **Text `req_id`.** Clients send a `Request-ID` header and the server echoes it. The plan's per-connection server counter assumed HTTP's in-order responses. Replies from different workers can complete out of order, so position-based pairing would mismatch. The counter is kept as a fallback for clients that send no `Request-ID`, such as curl.
- **Text responses.** HTTP responses carry no method, so the response type is recovered from the body: `remaining_vol` means ORDER_ACK, `subscribed`/`unsubscribed` mean the subscribe acks, `groups` means LIST_FEED_RSP, `order_id`+`status` means CANCEL_ACK, and 4xx or `error` means ERROR. Pushes are `POST /order/update` and `POST /session/disconnect` on any live connection, with no HTTP response expected.
- **EWMA snapshot timer.** Implemented as `Ttimeout = T_UDP + dt_safety`, where T_UDP is the EWMA (alpha 1/8) of the measured duration of one snapshot round (encode plus sendto fan-out) and dt_safety is the configured interval (`--snapshot-policy ewma`). This interpretation guarantees dt_safety of matching time between rounds; confirm it against the SRS.
- **Provisional answers to open review items.** `counterpart_id` carries the counterpart's `order_id` (item 4). SUBSCRIBE takes the union of groups and moves existing groups to a new endpoint (item 13). `callback_port` updates on every frame (item 9).
- **Delivery during draining.** Unsolicited updates for a client whose connection is draining go to the mailbox, not the draining connection. A push starts when that connection closes.
- **Client library.** It drains already-received bytes before each request, so a pending DISCONNECT or close is seen before writing into a dead connection.
- **Module structure.** `PushConnector` is folded into the Dispatcher and `CallbackListener` into `ClientSession`. The doc's `ICodec` is `net::StreamCodec`, chosen per connection by listening port. The text protocol listens on a second port (default 9001).

### 12.2 Measured results (loopback, 16-thread laptop, release build, 2026-10-14)

| Benchmark | Custom binary protocol | HTTP/1.1 + JSON | Ratio |
|---|---|---|---|
| BID bytes | 38 | 206 | 5.4x |
| ORDER_ACK with 1 trade, bytes | 71 | 288 | 4.1x |
| Codec CPU, BID encode + decode | 49 ns | 2264 ns | 46x |
| Dispatch latency p50 / p99.9 | 0.1 / 0.3 us | 1.3 / 3.8 us | ~13x |
| Round trip p50 / p99 / p99.9 (4 clients) | 24.5 / 37.7 / 58.5 us | 31.5 / 55.1 / 66.5 us | 1.3x at p50 |
| Peak throughput, one dispatcher | 2.81 M replies/s | 0.22 M replies/s | 12.5x |

Dispatch latency meets the SRS 50 us target with a wide margin for both codecs. On loopback, round-trip latency is dominated by kernel socket costs, so the codec difference shows mostly in throughput and CPU cost.

Idle policy (gap 1.5 s before each action):

| Policy | Order placed after gap, p50 | Fill to ORDER_UPDATE, p50 | Connections/min (3 clients, 1 req / 2 s) |
|---|---|---|---|
| never | 148 us | 35 us | 0 |
| fixed 100 ms | 247 us | 74 us (push) | 84 |
| fixed 1 s | 260 us | 87 us (push) | 78 |
| fixed 5 s | 154 us | 37 us | 0 |

When the gap exceeds x, the idle timer adds roughly 100 us per order on loopback (one TCP connect). It roughly doubles fill delivery time, because the fill goes through a push connect. On a LAN each connect costs at least one network round trip, so these gaps grow. Benchmark 6 (two machines) runs with `benchmark rtt|throughput --host <ip>` and has not been run yet.
