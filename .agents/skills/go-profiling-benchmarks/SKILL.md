---
name: go-profiling-benchmarks
description: Measure and diagnose Go throughput, latency, allocations, CPU, and memory with benchmarks and profiles. Use for performance claims, hot-path optimization, load testing, or suspected bottlenecks; exclude routine correctness tests and documentation-only edits.
---

# Go profiling and benchmarks

Establish reproducible evidence before optimizing. Preserve delivery, ordering, ownership, and transaction contracts.

## Measurement boundaries

- Benchmark generation, codecs, deduplication, and batch construction separately when diagnosing them. State setup exclusions and report allocations with `-benchmem`.
- For pipelines, distinguish attempted records, Kafka-acknowledged input, and transactionally committed unique output. A/B copies increase input volume.
- Record toolchain, GOOS/GOARCH, hardware, broker topology, event sizes, instruments, rate, batch limits, compression, acknowledgements, and transaction frequency. Use isolated run identities and Kafka start/end positions to exclude previous data.
- Confirm the source supplies the workload. Reader-driven generation, rate limits, retained fan-out, and backpressure can limit results independently of the engine.

## Benchmarks and load tests

Use representative fixtures, observable results, and focused selectors:

```sh
go test ./internal/feed -run '^$' -bench . -benchmem -count 5
```

Compare baseline and candidate using the same workload/environment and repeated samples; use `benchstat` when available. Disclose variance rather than inventing universal thresholds. Do not use race-instrumented timings as normal throughput results.

Load tests need warmup, a bounded measurement interval, queue growth, errors, and drain/commit completion. Define latency endpoints before reporting percentiles and account for cross-host clocks. Distinguish short bursts from sustained capacity and explain backpressure in closed-loop workloads.

## Profiling

Choose evidence from the symptom:

- CPU profiles locate execution cost.
- Allocation profiles (`alloc_space` / `alloc_objects`) measure churn; in-use heap samples measure retained memory.
- Block/mutex profiles diagnose synchronization; traces help investigate scheduling and blocking. Bound higher-cost instrumentation to diagnostic runs.

```sh
go test ./internal/feed -run '^$' -bench . -benchmem -cpuprofile /private/tmp/stream-eos-cpu.pprof -memprofile /private/tmp/stream-eos-mem.pprof
go tool pprof -top /private/tmp/stream-eos-cpu.pprof
go tool pprof -sample_index=alloc_space -top /private/tmp/stream-eos-mem.pprof
```

Profile the actual service for network/Kafka bottlenecks; codec profiles cannot establish pipeline behavior. Any diagnostic endpoint should be opt-in and localhost-only. Keep generated profiles and test binaries outside tracked source or explicitly ignore them.

## Decisions and reporting

Use profiles to justify changes, rerun comparable measurements, and verify correctness/races where relevant. Read the ownership, stack/heap, alignment, and I/O-batching skills when their triggers apply. Escape diagnostics support analysis but do not replace allocation measurements.

Report baseline/candidate results, workload, scope, variance, and correctness checks. Separate local single-broker results from replicated-cloud claims. State inconclusive results honestly. Never disable GC or weaken acknowledgements, deduplication, or transaction guarantees to improve headline throughput.
