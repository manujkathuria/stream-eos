# Ingestor benchmarks

**Per message/batch**, with 2 or 400 events. Measured 12 October 2026 on Apple M1, Go 1.27.1, darwin/arm64. Values are ranges from three runs; startup and warmup are excluded. `allocs` means heap allocations per batch.

## Processing in memory

| Stage | 2 events | 400 events |
| --- | --- | --- |
| Reusable byte-buffer read | 28.2–28.3 ns | 1.41–1.43 µs |
| Decode into reused storage | 8.20–8.31 ns | 1.024–1.027 µs |
| Ingestor loop + decode | 16.9–17.0 ns | 1.51–1.55 µs |
| Build Kafka records | 42.7–43.9 ns | 10.1–10.3 µs |

**All four stages: 0 B/op, 0 allocs/op.** These exclude network and Kafka client work. The ingestor-loop fixture uses an in-memory source and a counting publisher, without broker acknowledgements. Samples run for 200 ms.

## Through the network

| Stage | 2 events: time / allocations | 400 events: time / allocations |
| --- | --- | --- |
| WebSocket read over loopback | 2.09–3.37 µs / 8 B, 1 alloc | 6.16–6.40 µs / 80 B, 3 allocs |
| Kafka publish + acknowledgements | 184–238 µs / 53 allocs | 0.511–0.547 ms / 69–70 allocs |
| WebSocket → ingestor → Kafka | 121–158 µs / 3.96–3.97 KB, 54 allocs | 0.335–0.527 ms / 26.3–39.6 KB, 73–74 allocs |

The connected loop uses the latest reusable decoder and connection-lifetime cancellation. It acknowledged 12.7–16.5K events/s for two-event batches and 0.76–1.19M events/s for 400-event batches. All three runs per size passed exact stored-record verification; throughput is from short local runs, not sustained capacity.

WebSocket samples include the loopback sender and message validation, using prebuilt binary payloads without TLS/compression. Kafka samples use 100 timed batches after 20 warmups, one local broker, three partitions, replication factor 1, idempotence, all-ISR acknowledgements, and default snappy compression. Stored IDs, bytes, keys, and provenance are verified after timing. Engine deduplication and transactions are excluded. These short runs do not establish sustained capacity or replicated fault tolerance.

## Why keep `ProduceSync`?

Profiling identifies **4 direct allocations per call**, including about **9.5 KiB of result storage for 400 events**. Other allocations occur in franz-go batching, encoding, I/O, and response handling.

| Choice | Benefit | Responsibility |
| --- | --- | --- |
| `ProduceSync` — current | Simple; waits for every completion | Client allocates results and completion bookkeeping |
| `Produce` + reusable tracking — candidate | Could avoid that result collection | We must synchronize callbacks, collect errors, and wait before reusing buffers |

Both retain the same acknowledgement guarantees when used correctly. Switching APIs alone does not add pipelining. We keep `ProduceSync` until a measured experiment justifies the extra code. [Profile details and commands](docs/allocation-profiling.md).

## Run it

From `ingestor/`:

```bash
make bench          # In-memory stages
make bench-reader   # Loopback WebSocket reader
make bench-kafka    # Kafka acknowledgements and connected loop
```

Component/reader commands use default benchmark duration; use `-benchtime=200ms` for the samples above. Kafka uses `-benchtime=100x -count=3`. Benchmarks create and clean up isolated test topics; Kafka stays running.
