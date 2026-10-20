# Process benchmarks

## Kafka consumer

Measured 12 October 2026: Go 1.27.1, Apple M1, darwin/arm64, franz-go v1.21.0. Three runs per batch size, with the actual Kafka-backed `ReadBatch` and event-ID extraction.

Each run reads **50,000 records** after 1,000 warmup records from an isolated one-partition topic. Every record has a 4-byte key and 212-byte value. Setup and fixture production are excluded; reads check sequential offsets, source identity, record count, and an event-sequence checksum.

| Maximum records per poll | Time for 50K records | Records/s | Allocated bytes/record | Allocations/record |
| --- | --- | --- | --- | --- |
| 100 | 6.71–7.98 ms | 6.26–7.45M | 336.2–336.5 B | 1.114 |
| 1,000 | 6.62–7.42 ms | 6.74–7.55M | 335.0–335.2 B | 1.087 |

This is short local backlog replay with background prefetching. Some fetched records can already be buffered when timing begins, so timed allocation counts do not account for every allocation required to fetch those records. It is not live latency, sustained capacity, or consume/dedup/transaction throughput. The broker uses replication factor 1; input is synthetic and snappy-compressed, and the consumer uses `read_committed`.

### Allocation findings

A separate full-sampling profile identifies allocations in franz-go's `PollRecords` result structures, record/header decoding, record storage, and decompression. No direct allocation samples appear in our successful validation/copy path. The profile includes setup, fixture production, warmup, and asynchronous fetching; its totals are not an exact breakdown of the timed allocations above. Profiled timing is excluded from the table.

Larger polls only modestly change allocation cost in this workload. No production tuning or consumer rewrite has been applied. CPU profiling and the connected processing benchmark are needed before deciding which consumer work limits throughput.

### Run it

From `process/` with the local broker running:

```bash
make bench-consumer
```

This runs three replay passes per size, checks delivery, and cleans up only its isolated topics. Kafka remains running.

## Deduplication

Same machine/toolchain, three runs per workload. These isolate the production event-ID extraction, exact-map checks, pending state, and local FIFO updates. Kafka consumption is measured above; Kafka transaction commits, JSON, startup allocation, and recovery are excluded here. A local FIFO commit is not a durability acknowledgement.

### Batch classification

Time per batch. Stage checks IDs and records new pending IDs; stage + commit also promotes them into the local FIFO. Samples use 100 batches per run with a reset initial state outside timing. Duplicate/mixed fixtures start with 10,000 committed IDs; unique fixtures start empty.

| Workload | Stage, 100 records | Stage + commit, 100 | Stage, 1,000 | Stage + commit, 1,000 |
| --- | --- | --- | --- | --- |
| All unique | 4.84–6.05 µs | 6.41–7.61 µs | 29.7–30.0 µs | 49.4–49.6 µs |
| All committed duplicates | 1.84–2.15 µs | 1.84–1.86 µs | 18.6–18.9 µs | 18.6–18.7 µs |
| Mixed | 2.58–3.05 µs | 3.26–3.39 µs | 25.5–25.8 µs | 32.8–34.1 µs |

Mixed means 25% unique IDs, 25% repeats within the pending batch, and 50% committed duplicates. All runs report 0 allocations/batch; one stage sample reports 2 B/batch with the allocation count rounded to zero. Setup is not allocation-free.

### Full one-million-ID FIFO

200 ms samples; the window remains full while new IDs replace the oldest entries. These include staging, local commit, and eviction.

| Workload | 100-record batch | 1,000-record batch |
| --- | --- | --- |
| All unique | 24.0–25.2 µs (240–252 ns/record) | 249–268 µs (249–268 ns/record) |
| 50% unique / 50% retained duplicates | 18.4–19.3 µs (184–193 ns/record) | 195–199 µs (195–199 ns/record) |

All full-window runs report **0 B/op, 0 allocs/op**. A retained-ID lookup costs 69.3–72.9 ns at one million IDs. Fixtures check classification and resulting counts; race tests cover the dedup package. These measured paths do not establish that Go map churn never allocates under every workload.

From `process/`:

```bash
make bench-dedup
```

## JSON encoding

Production `AppendJSON`: **356.8–357.6 ns per event, 0 B/op, 0 allocs/op**, across three 500 ms runs on the same machine/toolchain.

One operation reads a fixed binary quote, converts its 24-byte event ID to hexadecimal, and writes all quote fields plus ten depth levels into a reused 2,048-byte buffer. The fixture uses six-digit quote values and smaller depth quantities/order counts. Setup, deduplication, and Kafka publishing are excluded; different numeric widths can change encoding cost.

Race-enabled correctness checks passed, including equality with the standard JSON reference across 1,002 zero, maximum-byte, and deterministic random records, buffer reuse, and insufficient-capacity rejection. No encoder change was needed for this measurement.

From `process/`:

```bash
make bench-json
```

## Transactional Kafka publishing

Three runs of 100 transactions after five warmups, same machine/toolchain. Each transaction contains N unique JSON outputs, N FIFO slot changes, and one input-progress checkpoint. Output payloads are 993 bytes/event with fixed synthetic numeric fields and distinct event IDs.

| Unique events/transaction | Time/transaction | Committed events/s | Allocated bytes/transaction | Allocations/transaction |
| --- | --- | --- | --- | --- |
| 100 | 8.27–16.91 ms | 5.91–12.1K | 21.1–54.1 KB | 139–141 |
| 1,000 | 2.19–2.59 ms | 386–457K | 1.62–3.84 MB | 239–256 |

Timing includes event-ID stamping, FIFO metadata planning, production `CommitBatch`, record acknowledgements, and confirmed Kafka `EndTransaction`. Quote encoding, input consumption, dedup checks, startup, and verification are excluded. The configured FIFO capacity is one million; this workload starts empty and does not fill it. One local broker, replication factor 1, one partition per isolated topic, all-ISR acknowledgements, and the client's default compression are used. Checkpoint/state topics use compact-only retention.

Every run passed exact `read_committed` output count/key/payload checks and fresh-owner recovery of the checkpoint, FIFO IDs, and cursor. Race-enabled smoke runs and vet passed. Only fixture topics are removed; Kafka stays running.

The small batches were slower per transaction in these samples, and large-batch allocation sizes varied substantially. The benchmark does not explain those effects; client/broker profiling is needed before attributing them to batching, compression, or commit latency. These are short local measurements, not sustained pipeline capacity. No production tuning was applied.

From `process/`:

```bash
make bench-transaction
```

### Where the transaction time goes

Diagnostic timing around the actual `ProduceSync` and `EndTransaction` calls, three runs of 100 transactions per size after five warmups. Values are ranges of per-run means, not latency percentiles.

| Events/transaction | `ProduceSync` | `EndTransaction` | Total transaction path |
| --- | --- | --- | --- |
| 100 | 1.11–5.78 ms | 0.159–0.241 ms | 1.29–6.04 ms |
| 1,000 | 2.05–2.30 ms | 0.177–0.201 ms | 2.29–2.57 ms |

`ProduceSync` accounts for 86–96% of total time in these runs. That boundary includes enqueueing, client batching/compression, network work, and record acknowledgements; it does not isolate result-slice allocation or prove asynchronous `Produce` would remove the wait. The remainder includes ID stamping, state preparation, transaction setup, and cleanup. Small-batch timing varied substantially across local runs.

All six diagnostic runs passed committed-output and fresh-owner recovery checks. Temporary `time.Now`/`time.Since` instrumentation was removed after measurement; ordinary `make bench-transaction` reports total timing. The diagnostic patch is retained locally at `/private/tmp/stream-eos-transaction-stage-timing.patch`. No production behavior was changed.

Further [CPU, memory, and blocking profiles](docs/transaction-profiling.md) locate substantial allocation volume in request serialization/compression and waiting in `ProduceSync` completion. They do not establish that changing producer APIs would remove the dominant latency. Output and recovery verification passed; production settings remain unchanged.

## Bounded live pipeline demo

One verified local run on 12 October 2026, Apple M1, Go 1.27.1, one native Kafka broker (replication factor 1). `make demo-performance` used a 30-second offer at 1,000 source messages/sec per connection, two subscribed events/message, A/B ingestors, one processing partition, batch limit 100, and a one-million-ID FIFO.

| Measurement | Result |
| --- | --- |
| Actual Kafka input | 118,546 records (A: 59,272; B: 59,274) |
| Committed unique output | 59,274 events |
| Duplicate copies suppressed | 59,272 |
| Intake interval, including startup/shutdown | 30.002 s |
| Observed drain to final checkpoint | 0.019 s |
| Total measured interval | 30.021 s |
| Input throughput | 3,949 records/s |
| Committed unique-output throughput | 1,974 events/s |

Exact output IDs, payloads, routing keys, and final input progress passed verification. Throughput includes startup and drain, excludes post-run verification, and reflects this configured offer rather than maximum capacity. Drain includes metadata/checkpoint observation overhead. Verification begins at Kafka-stored input and does not establish that every generated source event was delivered. See [demo instructions](docs/demo.md#live-performance-run).
