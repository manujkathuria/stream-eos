---
name: go-data-ownership
description: Design and review typed, fixed-capacity Go data structures with explicit buffer ownership and reuse. Use when creating or modifying event structs, arrays, slices, batches, binary codecs, channel payloads, or allocation-sensitive loops; exclude SQL-only and prose-only changes.
---

# Typed data, ownership, and reuse

## Project standard

When source data has a known bounded shape, represent it with concrete types and fixed-capacity storage. Avoid unnecessary per-event allocations and keep memory bounded. Correct delivery and safe ownership are mandatory; allocation and GC costs must be measured.

Borrow explicit ownership reasoning from Rust, without assuming Go enforces Rust ownership rules. Type safety, capacity, ownership, allocation placement, and wire encoding are distinct concerns.

## Before implementation

Identify the data shape and bounds, field meanings and units, storage lifetime, and every consumer that can retain references. Define who owns and mutates each buffer, when ownership transfers, and when reuse is safe. Choose a capacity from the contract or configured limit, and define overflow behavior.

## Representation and lifetime rules

- Prefer concrete event and depth types over per-event maps, `any`, or generic property bags for known fields.
- Use fixed arrays for protocol-fixed counts, such as five bid and five ask levels. Represent variable valid lengths explicitly; do not silently truncate input to fit.
- Use bounded preallocated slices or batches for configurable counts. Allocate reusable storage during setup where practical; avoid growth, formatting, and temporary allocations in per-event loops.
- Keep numeric wire values in suitable integer types with explicit units and scaling. Validate conversions and ranges; do not use floating point merely for display convenience.
- A slice copy shares its backing array. Sending a slice or a struct containing slices through a channel does not by itself transfer exclusive ownership.
- Reuse storage only after every consumer has finished. For asynchronous delivery, use explicit completion/ownership transfer, independent buffers, or a deliberate copy. Cancellation alone does not prove a consumer has stopped accessing memory.
- Reset reused records so omitted fields cannot retain previous-event values. On error, do not publish partially populated records as valid.
- Choose value versus pointer semantics from mutation, lifetime, and copying costs. No universal struct-size cutoff applies. Avoid expensive large copies where measurements justify another design.

## Binary boundaries

Go struct memory is not a wire-format contract: padding, alignment, field widths, and byte order must be handled explicitly. Encode/decode defined fields into caller-owned storage, validate lengths before access, and preserve complete message boundaries where required. Do not use unsafe struct overlays to bypass validation or promise zero-copy.

Example of protocol-fixed depth storage (illustrative; verify wire signedness and ranges before adoption):

```go
type DepthLevel struct {
    Price      int32
    Quantity   int32
    OrderCount int16
}

type DepthSnapshot struct {
    Bids [5]DepthLevel
    Asks [5]DepthLevel
}
```

These arrays do not require separate backing-storage allocations. The enclosing object may still live on the heap. Source packet sizes and Go struct sizes need not match.

## Allocation and verification

- Target allocation-free generation/encoding/decoding loops after setup when their contracts permit it. Report the measured scope; do not extend a codec result to the entire network/database pipeline.
- Reusable heap storage allocated once is acceptable. Stack/register placement is compiler-dependent; verify escapes with the actual toolchain when relevant.
- Network, database, and gRPC libraries may allocate. Measure these boundaries and document required allocations; do not disable GC globally or introduce unsafe ownership to meet a metric.
- Start with explicit reusable storage. Introduce pooling only when profiles justify it; a pool is not guaranteed capacity or lifetime management.
- For changed hot loops, benchmark representative inputs with allocation reporting. State setup exclusions and toolchain/workload; compare throughput and allocation results without invented universal timing thresholds.
- Test malformed lengths, capacity boundaries, and consecutive reuse where relevant. Exercise ownership and shutdown paths with race detection for concurrent changes. Report what was actually checked.

References for technical assumptions: [Go allocation FAQ](https://go.dev/doc/faq#stack_or_heap), [Go specification](https://go.dev/ref/spec), [sync.Pool](https://pkg.go.dev/sync#Pool).
