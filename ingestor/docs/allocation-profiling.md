# Kafka allocation profiling

Diagnostic runs: 12 October 2026, Go 1.27.1, Apple M1, franz-go v1.21.0; 1,000 acknowledged batches per size, full allocation sampling (`-memprofilerate=1`).

| Batch | Allocations | Allocated bytes |
| --- | --- | --- |
| 2 events | 53 | 3,907 B |
| 400 events | 69 | 20,845 B |

`ProduceSync` directly allocates four objects per call: wait-group storage, result-slice backing storage, its captured slice variable, and a callback. Result storage for 400 events is approximately 9.5 KiB. Other recurring stacks include partition-batch bookkeeping, request encoding, broker I/O, and produce-response parsing.

The profile covers setup, warmup, background work, and post-timing record verification. Fetch/decompression during verification dominates the unfiltered large-batch profile, but is outside timed publishing. Profile percentages do not explain the timed allocation count exactly. Instrumented timings are diagnostic; use ordinary benchmarks for latency.

From `ingestor/`, change `packets=2` to `packets=400` for the larger batch:

```bash
STREAM_EOS_KAFKA_BENCH=1 GOWORK=off go test ./internal/kafka \
  -run '^$' -bench '^BenchmarkPublishAcknowledged/packets=2$' \
  -benchtime=1000x -count=1 -memprofilerate=1 \
  -memprofile=/private/tmp/stream-eos-publish.mem \
  -o /private/tmp/stream-eos-publish-profile.test

go tool pprof -sample_index=alloc_objects -top \
  /private/tmp/stream-eos-publish-profile.test /private/tmp/stream-eos-publish.mem
go tool pprof -sample_index=alloc_space -list='Client.*ProduceSync' \
  /private/tmp/stream-eos-publish-profile.test /private/tmp/stream-eos-publish.mem
```
