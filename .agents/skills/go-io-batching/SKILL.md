---
name: go-io-batching
description: Design and review bounded Go network, file, and database I/O batches to reduce small operations and round trips. Use for streaming I/O loops, bulk writers, batch-size decisions, or measured syscall/scheduling bottlenecks; exclude pure computation and SQL schema-only work.
---

# I/O batching and kernel overhead

## Principle

Amortize fixed I/O costs with bounded batching where the latency contract permits it. Distinguish system calls, thread context switches, network round trips, protocol messages, and server statement execution: their counts and costs are not interchangeable. Neither a Go method call nor a gRPC message necessarily maps to one syscall. Do not claim a kernel bottleneck from code inspection alone.

## Design and review

- Identify operation size, frequency, synchronization points, and completion semantics. Inspect client-library buffering and pipelining before adding custom buffers.
- Avoid per-record round trips in bulk work. Consider pipelined batches, multi-row statements, or database COPY according to atomicity, error handling, identity mapping, and measured cost.
- A batch of individual INSERT statements can amortize round trips while still requiring one statement execution and result per record. Do not call it a multi-row INSERT or a single syscall.
- Bound batches by record count and bytes; for live streams also define a maximum flush delay. Flush the final partial batch. Batch capacity must not grow with total input size.
- Reuse caller-owned buffers only after the library has finished consuming them. Integrate cancellation, backpressure, and transaction cleanup; batching must not weaken correctness or durability.
- Start with the connections needed by actual concurrency. A sequential writer does not automatically benefit from a large pool or several prewarmed connections.
- Avoid busy polling, per-record logging, and unnecessary flushes. Do not add socket flags, custom transports, unsafe memory, or parallel writers without evidence and an ordering/ownership argument.

## Evidence

Measure generation separately from encoding, database/network I/O, and commit time. Compare representative batch sizes using the same workload, isolated test identities, and recorded environment. Include throughput, latency where relevant, memory, and allocations; distinguish committed records from attempted records.

Profile before attributing overhead to syscalls or scheduling. Use platform-appropriate tracing when syscall/context-switch claims matter, with appropriate access. Keep transport, CPU, and server-side evidence separate. Do not infer syscall counts from API-call counts or treat one wall-clock run as proof of improvement.

Report a proposed optimization and its tradeoffs if comparative measurements are unavailable. Retain changes only when correctness checks pass and evidence supports the benefit.

Read [go-data-ownership](../go-data-ownership/SKILL.md) for asynchronous buffer lifetime and [go-stack-vs-heap](../go-stack-vs-heap/SKILL.md) when changing allocation decisions.
