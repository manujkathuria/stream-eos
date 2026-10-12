---
name: go-struct-alignment
description: Minimize avoidable padding in Go structs and verify layout on the target architecture. Use when defining or modifying event, depth, batch, or other repeatedly stored structs, or investigating memory footprint; exclude SQL schemas and wire-format layout changes alone.
---

# Primitives, alignment, and struct layout

## Standard

Consider field alignment when designing concrete Go structs. For high-volume records, reduce avoidable padding while preserving semantics, ownership, and useful access patterns. Do not claim cache speedups from size reduction alone.

On typical 64-bit Go targets:

```go
type Padded struct {
    A bool
    B int64
    C bool
} // Commonly 24 bytes: field offsets 0, 8, 16.

type Compact struct {
    B int64
    A bool
    C bool
} // Commonly 16 bytes: field offsets 0, 8, 9.
```

Both contain ten bytes of field data. Internal padding aligns fields; trailing padding aligns successive elements in an array. A million elements require approximately 24 MB versus 16 MB of backing storage, excluding surrounding allocations. Fifty percent more elements fit in the same byte capacity; actual cache performance depends on access patterns and hardware.

## Decisions

- Group fields by alignment requirements, often placing higher-alignment fields before smaller scalar fields. Descending total field size is a heuristic, not a universal optimum.
- On typical 64-bit targets, bool/byte are size/alignment 1, int16 2, int32 4, and int64/pointers 8. Strings commonly occupy 16 bytes and slices 24 bytes, both with alignment 8. These headers exclude referenced storage. Verify the actual target rather than assuming all architectures match.
- Reordering fields does not change stack/heap placement or eliminate referenced allocations. Inspect nested structs and arrays when estimating complete batch footprint.
- Preserve deliberate field relationships, atomic alignment requirements, and false-sharing considerations. Smallest size is not always the best concurrent layout.
- Check positional literals, reflection/serialization behavior, generated code, and compatibility expectations before reordering an existing type. Do not reorder generated protocol types manually.
- Never treat Go memory layout as network encoding or use unsafe overlays to bypass explicit codecs.

## Verification

Use `unsafe.Sizeof`, `unsafe.Alignof`, and `unsafe.Offsetof` in a diagnostic or meaningful layout check for the target toolchain/GOARCH. These operations can inspect layout without unsafe pointer manipulation. Record before/after size and representative record count for footprint claims.

Use architecture-qualified size assertions only when layout is an intentional supported contract. Do not add brittle size tests for every incidental struct. Benchmark a representative access workload before claiming latency, throughput, or cache improvement.

For ownership and referenced storage, read [go-data-ownership](../go-data-ownership/SKILL.md). For placement decisions, read [go-stack-vs-heap](../go-stack-vs-heap/SKILL.md) when relevant.

Reference: [Go size and alignment guarantees](https://go.dev/ref/spec#Size_and_alignment_guarantees).
