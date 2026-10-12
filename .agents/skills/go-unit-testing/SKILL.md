---
name: go-unit-testing
description: Add or review meaningful Go unit tests for changed behavior, binary protocols, ownership, and streaming correctness. Use for Go implementation, bug fixes, or explicit test requests; exclude documentation-only edits and performance measurement alone.
---

# Go unit testing

Test observable contracts and failure behavior rather than mirroring implementation. Scale verification to the change; avoid tests for trivial edits that add no meaningful confidence.

## Design

- Define acceptance criteria before tests. For bug fixes, reproduce the defect and confirm failure before fixing it when practical.
- Prefer standard-library tests and table-driven cases with independent expected values. Do not compute expectations using the algorithm under test.
- Test pure decoding and processing without Kafka. Substitute narrow external boundaries where needed; avoid broad mock frameworks or redesigning production code solely for mocks.
- Distinguish unit tests, local WebSocket integration tests, and real Kafka end-to-end checks. A fake producer cannot prove broker transactions or crash durability.
- Use controlled identities and clocks where behavior depends on them. Synchronize instead of arbitrary sleeps; bound blocking operations with deadlines and clean up connections, goroutines, and resources.

## Streaming cases

Select cases relevant to the change:

- Codec validity, truncation, versions, counts, lengths, trailing bytes, capacity boundaries, and rejection without publishing partially decoded events.
- Identity preservation across A/B copies and redelivery, distinct events, source restarts, and out-of-order duplicates. A highest-sequence shortcut must not discard unseen older events.
- Consecutive buffer reuse, retained references, asynchronous completion, cancellation, and shutdown. Output must remain correct after input storage is reused.
- Duplicate processing, commit failures, recovery of state with offsets, and final partial batches. Broker-dependent behavior belongs in integration tests.

Fuzz parsers when useful, seeding valid and malformed input and checking bounded resource use, no panic, and contract invariants. Finite tests do not prove correctness under every possible failure.

## Verification

Run affected tests first and required project checks. Use race detection for concurrent or ownership changes. Broaden or repeat checks when new changes or failures justify it.

Report verified behavior and remaining integration gaps. Coverage identifies untested paths; it is not a correctness target. Never weaken assertions just to pass tests.

Read [go-data-ownership](../go-data-ownership/SKILL.md) for shared or reusable storage.
