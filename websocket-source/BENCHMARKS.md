# Source stage measurements

The production journey is:

```text
market.Frame: coordinate history and locking
    → generateUpdate: advance and encode quotes when an update is new
    → protocol.BuildMessage: frame the subscribed packets
    → Gorilla WriteMessage: send the completed message
```

[`internal/market/generation.go`](internal/market/generation.go) and [`internal/protocol/message.go`](internal/protocol/message.go) own the first two stages. [`internal/websocket/session.go`](internal/websocket/session.go) owns pacing and delivery; subscriptions have their own neighboring file. No interfaces or send wrappers were added solely for benchmarks.

## Local results

Go 1.27.1, darwin/arm64, Apple M1; three 300 ms samples per case, after the configurable-instrument change. Setup is excluded; measurements ran after correctness checks.

| Stage | Work per operation | Time | Timed allocations |
| --- | --- | --- | --- |
| Generation | Advance and encode 10 quotes into a 128-slot history | 434–493 ns | 0 B/op, 0 allocs/op |
| Generation | Advance and encode 400 quotes into a 128-slot history | 16.8–19.1 µs | 0 B/op, 0 allocs/op |
| Construction | 2 configured/subscribed packets, 410 bytes | 13.9–14.7 ns | 0 B/op, 0 allocs/op |
| Construction | 10 configured/subscribed packets, 1,962 bytes | 57.0–57.2 ns | 0 B/op, 0 allocs/op |
| Construction | 100 configured/subscribed packets, 19,422 bytes | 551–555 ns | 0 B/op, 0 allocs/op |
| Construction | 400 configured/subscribed packets, 77,622 bytes | 2.78–2.82 µs | 0 B/op, 0 allocs/op |

Generation uses an explicit timestamp and excludes clock reads, locks and construction. Construction reuses retained packet storage and excludes generation and locking. Each construction fixture's configured universe equals its subscription count. A larger universe with fewer subscriptions has additional filtering work.

Historical loopback delivery results (before configurable instrument storage): 2 packets took 2.04–2.71 µs, 8 B/op, 1 alloc/op; 100 packets took 3.13–5.35 µs and 400 took 7.13–7.31 µs, both 80 B/op, 3 allocs/op. Those results used a prebuilt payload, one loopback connection, no TLS/compression, and 100 warmup messages. They are not new measurements of the complete source.

At 10 instruments and 4,096 history slots, packet storage is 7,536,640 bytes (7.19 MiB), plus 98,304 bytes of slice headers on arm64 and current quote state. Storage is allocated during setup and reused. This does not imply allocation-free WebSocket delivery.

## Reproduce

From `websocket-source/`:

```bash
make test
make vet
make bench
make bench-delivery
```

For the 300 ms samples above, add `-benchtime=300ms` to the corresponding `go test` commands in the Makefile. `bench-delivery` opts into opening local test sockets; it does not connect to the running source or Kafka.

The historical generation row uses 400 instruments. Generation and storage now scale with `INSTRUMENT_COUNT` (default 10); the generation benchmark covers both 10 and 400, and construction includes 10 packets. All configured instruments still advance together to preserve shared market state. The source also defaults to 1,000 messages/sec per connection. These measurements do not establish its sustained delivered rate, A/B fan-out behavior under load, or remote-network latency, and stage times should not simply be added into a capacity claim.
