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
