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
./build/debug/simulation            # full trace and order tables
./build/debug/simulation --quiet    # PASS/FAIL only
```

Sanitizer builds (`asan`, `ubsan`, `tsan`):

```bash
cmake --preset asan && cmake --build --preset asan && ctest --preset asan
cmake --preset tsan && cmake --build --preset tsan
setarch $(uname -m) -R ./build/tsan/unit_tests
```

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
