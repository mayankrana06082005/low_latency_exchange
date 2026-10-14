# Low Latency Exchange Prototype

## Build

```bash
cmake --preset debug   && cmake --build --preset debug     # tests, simulation
cmake --preset release && cmake --build --preset release   # benchmarks
```

## Tests

Run everything:

```bash
ctest --preset debug
```

Run one test file:

```bash
./build/debug/unit_tests -sf="*test_byte_io*"        # big-endian read/write
./build/debug/unit_tests -sf="*test_binary_codec*"   # custom protocol encode/decode
./build/debug/unit_tests -sf="*test_frame_reader*"   # TCP stream reassembly
./build/debug/unit_tests -sf="*test_text_codec*"     # HTTP/1.1 + JSON encode/decode
./build/debug/unit_tests -sf="*test_core*"           # queue, order book, mailbox, worker
./build/debug/unit_tests -sf="*test_integration*"    # full server over real sockets
```

Run one test case by name (`*` is a wildcard):

```bash
./build/debug/unit_tests -ltc                  # list all test case names
./build/debug/unit_tests -tc="Mailbox*"
```

Scripted end-to-end simulation:

```bash
./build/debug/simulation                # client trace + server trace + order tables
./build/debug/simulation --client-only  # client trace only
./build/debug/simulation --quiet        # PASS/FAIL only
```

Sanitizer builds (`asan`, `ubsan`, `tsan`):

```bash
cmake --preset asan && cmake --build --preset asan && ctest --preset asan
cmake --preset tsan && cmake --build --preset tsan
setarch $(uname -m) -R ./build/tsan/unit_tests
```

## Live demo

Server (custom protocol on port 9000, HTTP/1.1 + JSON on port 9001):

```bash
./build/debug/exchange_server                 # quiet
./build/debug/exchange_server --trace on      # print dispatcher and worker events
```

Clients, one per terminal:

```bash
./build/debug/exchange_client --trace on                              # custom protocol
./build/debug/exchange_client --port 9001 --protocol text --trace on  # HTTP/1.1 + JSON
```

Client commands: `list`, `bid <TICKER> <price> <vol>`, `ask <TICKER> <price> <vol>`, `cancel <order_id>`, `orders`, `sub <mask>`, `unsub <mask>`, `book`, `wait <ms>`, `help`, `quit`.

The HTTP port also works with curl:

```bash
curl http://127.0.0.1:9001/feed
curl -X POST -d '{"ticker":"AAPL","type":"LIMIT","max_price":100,"volume":5}' http://127.0.0.1:9001/order/bid
```

Server options: `--port`, `--text-port`, `--idle fixed|never`, `--idle-ms`, `--grace-ms`, `--push-timeout-ms`, `--snapshot-ms`, `--snapshot-policy fixed|ewma`, `--record-latency on`, `--trace on`.
Client options: `--host`, `--port`, `--protocol binary|text`, `--callback-port`, `--udp-port`, `--push off`, `--trace on`.

## Wireshark

### One-time setup

```bash
sudo apt install wireshark tshark                 # answer "Yes" to non-superuser capture
sudo usermod -aG wireshark $USER                  # then log out and back in
ln -s "$PWD/tools/exchange.lua" ~/.local/lib/wireshark/plugins/exchange.lua   # protocol decoder
```

Check:

```bash
id -nG | grep -o wireshark      # must print: wireshark
```

If it prints nothing, you have not logged in again yet. Prefix the commands below with `sg wireshark -c`, e.g. `sg wireshark -c wireshark`.

### Live capture with the demo

```bash
wireshark                                          # terminal 1: double-click "Loopback: lo"
./build/debug/exchange_server --trace on           # terminal 2
./build/debug/exchange_client --callback-port 9200 --udp-port 9100   # terminal 3 (buyer)
./build/debug/exchange_client --callback-port 9201 --udp-port 9101   # terminal 4 (seller)
```

Leave the capture filter empty. Type display filters in the top bar after packets appear. Stop with the red square. Save with File → Save As.

### Capture a test or benchmark from the terminal

```bash
tools/capture.sh build/sim.pcapng ./build/debug/simulation --quiet
tools/capture.sh build/rtt.pcapng ./build/release/benchmark rtt --clients 2 --requests 200
```

It prints packets and wire bytes per protocol, SYN/FIN counts and message counts by type. Keep benchmark captures small.

### Open a saved capture

```bash
wireshark build/sim.pcapng
tshark -r build/sim.pcapng -Y exchange             # same, as text in the terminal
```

### Display filters

| Filter | Shows |
|---|---|
| `tcp.port == 9000 \|\| tcp.port == 9001 \|\| exchange \|\| exchange_md` | only exchange traffic (hides DNS etc.) |
| `exchange` | custom protocol frames |
| `http` | HTTP/1.1 + JSON frames |
| `exchange_md` | UDP market data snapshots |
| `exchange.type == 0x01` | BID (`0x02` ASK, `0x03` CANCEL, `0x81` ORDER_ACK, `0xc1` ORDER_UPDATE, `0xc2` DISCONNECT, `0x8f` ERROR) |
| `tcp.flags.syn == 1 && tcp.flags.ack == 0` | new TCP connections (client connects and server pushes) |
| `tcp.flags.syn == 1 \|\| tcp.flags.fin == 1 \|\| tcp.flags.reset == 1` | connection start, end and reset |

### Useful views

| Where | Shows |
|---|---|
| Click a packet → middle pane → expand "Exchange Binary Protocol" | decoded header and payload fields |
| Right-click a packet → Follow → TCP Stream | whole conversation (readable for HTTP) |
| Statistics → Protocol Hierarchy | bytes per protocol |
| Statistics → Conversations → TCP | one row per connection |
| View → Time Display Format → Seconds Since Previous Displayed Packet | gap between packets |

Row colors: gray = SYN/FIN, purple = TCP, light blue = UDP, red = RST, black = TCP problems.

## Benchmarks

Close any running `exchange_server` first.

```bash
./build/release/benchmark all              # everything, ~2 min
./build/release/benchmark all --quick      # shorter runs, ~1 min
./build/release/benchmark sizes
./build/release/benchmark codec
./build/release/benchmark rtt
./build/release/benchmark throughput
./build/release/benchmark idle
./build/release/benchmark rtt --csv build/bench_results   # also save latency samples
```

Each benchmark compares the custom binary protocol (`custom`) with HTTP/1.1 + JSON (`http-json`):

| Command | Measures |
|---|---|
| `sizes` | Bytes on the wire per message type |
| `codec` | Encode and decode time per message, no network (ns) |
| `rtt` | Dispatch latency (socket read to worker queue) and round-trip latency (order to ACK), p50/p90/p99/p99.9 (µs), plus histograms |
| `throughput` | Maximum orders per second through one dispatcher |
| `idle` | Idle policies `never`, `fixed 100 ms`, `fixed 1 s`, `fixed 5 s`: order latency after an idle gap, fill delivery latency, connections opened per minute |

Options:

```
--clients N --requests N --warmup N   rtt
--window N --seconds S                throughput
--iters N                             codec
--gap-ms N --samples N                idle
--host H --port P --text-port Q       rtt/throughput against a running server
```
