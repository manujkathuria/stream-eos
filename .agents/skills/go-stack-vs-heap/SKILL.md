---
name: go-stack-vs-heap
description: Apply stack-by-design, heap-by-exception reasoning to Go lifetimes, value/pointer choices, buffer reuse, and allocation-sensitive loops. Use when changing these decisions or diagnosing allocations; exclude SQL-only and prose-only edits.
---

# Stack by design, heap by exception

## Meaning

Avoid unnecessary heap allocations, especially in code that runs for every request or tick. Stack storage is generally cheap; heap allocation takes work and can increase GC work. The heap remains normal for long-lived maps, caches, shared state, and bounded reusable buffers.

The compiler decides placement. Returning a pointer, passing an interface, or constructing a slice does not alone prove a heap allocation after optimization. Fixed arrays and value semantics do not automatically imply stack placement.

## Habits

- Use simple values when they fit the job; use pointers when sharing, mutation, or copying costs justify them. No universal byte-size threshold applies.
- Prefer bounded reusable storage for established hot paths. Allocate at setup where practical; a buffer allocated once on the heap may be the best design.
- Track ownership and lifetimes before reuse. A reference retained by another goroutine, driver, or asynchronous operation must remain valid until consumption finishes.
- Avoid retaining huge backing arrays through small slices or retaining pointer-rich stale batch entries. Release or clear references when appropriate, without resetting data still in use.
- Pool only after measured benefit; bound retained capacity through design. `sync.Pool` does not guarantee retention or a fixed memory limit.
- Do not disable GC or introduce unsafe lifetime tricks to satisfy an allocation goal. Include required dependency allocations honestly.
- Write clear code first. Measure before adding complexity.

## Evidence

For a changed allocation-sensitive operation, benchmark representative inputs with allocation reporting and distinguish setup from steady-state work. Record the operation, input size, toolchain, and whether the database/network boundary is included.

When diagnosing unexpected allocations, use targeted compiler diagnostics such as `go build -gcflags='-m=2' ./internal/relevantpackage` with the actual package path. Correlate escape messages with benchmarks; compiler output alone is not an application allocation or performance measurement.

Use heap/allocation profiles when needed to identify allocation rate or retained-memory bottlenecks. Report measured allocations, memory, and throughput; describe exceptions and tradeoffs rather than claiming the entire application is GC-free.

For buffer ownership and typed bounded representations, read [go-data-ownership](../go-data-ownership/SKILL.md). Read [go-struct-alignment](../go-struct-alignment/SKILL.md) when changing concrete struct layout.

Reference: [Go allocation FAQ](https://go.dev/doc/faq#stack_or_heap).
