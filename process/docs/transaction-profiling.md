# Transaction publishing profile

12 October 2026, Go 1.27.1, Apple M1, franz-go v1.21.0. The real transaction benchmark used 1,000 unique events/transaction, five warmups, and 100 timed transactions. Output and fresh-owner FIFO/checkpoint verification passed.

## Findings

- **Completion wait:** the blocking profile attributes roughly 0.21 seconds to `ProduceSync`'s wait group in the benchmark's measured-run stack. That stack also includes five warmups. This is waiting for asynchronous publication to finish, not proof that the wait group itself is inefficient.
- **Allocated bytes:** producer request stacks show approximately 242 MiB in `AppendVarintBytes` while serializing keys/values, and 51.8 MiB growing compression buffers. Direct `ProduceSync` allocations account for approximately 4.19 MiB in the whole-process sampled profile. These are sampled totals across setup/warmup/measurement, not per-transaction measurements.
- **CPU:** only 320 ms of samples were collected over about six seconds of process lifetime. Runtime scheduling and memory-management stacks dominate; this sample is insufficient to name a stable producer CPU hotspot.

The whole-process memory profile also includes decompression and record construction from post-timing output/recovery verification. Those stacks are excluded when inspecting producer request allocations. Blocking totals across background goroutines overlap and must not be summed into elapsed transaction latency.

The pinned client defaults to 10 ms producer linger, but `ProduceSync` explicitly requests draining known partition buffers. Linger alone is therefore not an established explanation for the small-batch variability.

## Implication

Replacing `ProduceSync` with reusable callback tracking could remove its result collection, but would retain serialization, compression, and acknowledgement waits. The stronger next investigation is request/batch behavior and broker round-trip timing, using client hooks or a trace. No production tuning was applied.

## Reproduce

From `process/` with Kafka running:

```bash
STREAM_EOS_KAFKA_BENCH=1 GOWORK=off go test ./internal/kafka \
  -run '^$' -bench '^BenchmarkTransactionCommit/unique/batch1000$' \
  -benchmem -benchtime=100x -count=1 \
  -cpuprofile=/private/tmp/stream-eos-txn.cpu \
  -memprofile=/private/tmp/stream-eos-txn.mem \
  -blockprofile=/private/tmp/stream-eos-txn.block \
  -o /private/tmp/stream-eos-txn-profile.test

go tool pprof -sample_index=alloc_space -top \
  -focus='sink.*produce|produceRequest' \
  /private/tmp/stream-eos-txn-profile.test /private/tmp/stream-eos-txn.mem

go tool pprof -top -focus=ProduceSync -ignore='testing.*run1' \
  /private/tmp/stream-eos-txn-profile.test /private/tmp/stream-eos-txn.block
```

`testing.run1` is the benchmark calibration run. Removing that stack from the blocking view still leaves the measured run's warmup. Profiling timing is diagnostic, not the ordinary benchmark result.
