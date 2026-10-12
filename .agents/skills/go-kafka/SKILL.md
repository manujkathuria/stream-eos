---
name: go-kafka
description: Design, implement, and review Go Kafka producers, consumers, and transactional processing with franz-go. Use for Kafka delivery, partitioning, offset management, rebalances, transaction recovery, or broker integration tests; exclude general Go refactoring and source-only WebSocket changes.
---

# Kafka producers, consumers, and transactions

Preserve the documented delivery contract before tuning Kafka. Inspect `go.mod`, existing adapters, topic configuration, and architecture assumptions. Consult API documentation or module source for the pinned franz-go version; defaults and examples from newer releases are not automatically applicable.

## Decide the boundary

Identify event identity, partition key, ordering scope, acknowledgement boundary, offset ownership, and recovery behavior. Distinguish source delivery, Kafka producer retries, duplicate application submissions, and committed processing effects.

In this project A/B producers preserve the same source event ID and key by instrument token. Their copies are expected duplicates. Kafka offsets identify topic-partition positions; they are not cross-feed event identities. Kafka is the engine's only input. Source loss remains governed by the project's explicit ingestion assumption.

## Producers

- Use idempotent production and all-ISR acknowledgements for the established reliable path. Verify the pinned client's settings and constraints. Idempotence does not deduplicate two application Produce calls, independent producers, or arbitrary resends after restart.
- Review replication factor, minimum ISR, retention, and broker configuration alongside client settings. `acks=all` on one broker is not replicated fault tolerance.
- Use the same stable key encoding and partitioner across ingestors. Ordering is partition-local; interleaved A/B copies can arrive out of source sequence. Increasing partition count can change key placement and needs a migration plan when ordering or partition-local state matters.
- Bound records and bytes in application and client buffers. Define batch count/byte limits and flush delay when accumulating across messages. Explain synchronous acknowledgement versus asynchronous completion, backpressure, and final drain behavior.
- Preserve record/key/value/header storage until the client finishes using it. Cancellation alone is not proof of completion. Copy borrowed source bytes when necessary; follow the ownership skill for reuse.
- Document retries, delivery deadlines, terminal failures, and ambiguous acknowledgement outcomes. A batch can partially succeed. Do not blindly replay an entire failed batch and claim idempotence makes arbitrary application retries unique.
- Report acknowledged records separately from attempted submissions. Shutdown must deliberately drain or abort outstanding work with bounded cleanup.

## Consumers

- Specify consumer group versus direct partition assignment, startup/reset behavior, and topic isolation. Do not silently reset missing or expired offsets to bypass data loss.
- Choose offset management from the processing contract; do not rely on auto-commit defaults for exactly-once processing. Commit the next offset only after the required effects are durable. Never advance past an unfinished earlier record in the same partition.
- Bound polls, queued records, bytes, and processing duration. Handle fetch and per-partition errors explicitly. Avoid unbounded goroutines and preserve partition-local order where required.
- Define rebalance ownership: stop or abandon revoked work, prevent stale workers from committing, and distinguish partition loss from orderly revocation. If blocking rebalances while processing, use the pinned client's protocol correctly and stay within its timeouts.
- Use `read_committed` when consuming transactional output or state. It filters aborted transactions; it does not deduplicate source records.
- Make polling cancelable and define bounded shutdown. Keep consumer offset decisions in the orchestrator/session contract rather than hidden adapter side effects.

## Transactions and recovery

For consume-process-produce, prefer the pinned franz-go transaction/session facilities over an ad hoc coordination protocol when they meet the design. Review their rebalance and error handling before adoption.

Commit output records, durable deduplication/state updates, and consumed offsets in the same Kafka transaction. Offsets committed separately break the intended atomic boundary. Kafka transactions do not atomically commit arbitrary local memory, files, database writes, or HTTP effects.

- Assign transactional IDs from stable logical ownership. Independent live owners need different IDs; replacement of an owner must fence the old one. Do not reuse a single ID across A/B producers or choose a random ID per restart without considering recovery.
- Check every produce and transaction outcome. Distinguish retryable errors, abort-required conditions, fencing/fatal errors, and unknown commit outcomes according to the client contract. Do not guess whether a timed-out commit succeeded.
- On abort, restore or discard tentative state and reposition/replay input as required; aborting broker output does not reset application memory. On restart, rebuild state consistently with committed offsets before accepting work.
- Define state partitioning, changelog restoration, retention, and duplicate/replay horizon. A high-water mark alone can discard unseen out-of-order events from redundant ingestion.
- Keep transaction duration and batches bounded. State exactly what committed-effects guarantee was implemented and tested; processing may execute more than once.

## Verification

Use unit tests for codecs, orchestration, and ownership. Use real Kafka integration tests for acknowledgements, commit/abort visibility, group offsets, fencing, rebalances, and crash recovery where relevant. Fake producers cannot prove these broker semantics.

Use isolated topics or run identities and explicit offset ranges. Assert unique committed outputs against known input IDs, including duplicates and failures before/after commit. Poll observable conditions with deadlines instead of arbitrary sleeps. Preserve existing broker data and keep Kafka running when requested.

Separate single-broker correctness checks from multi-broker durability/failure tests. Measure acknowledged input and committed unique output with the profiling skill; do not weaken correctness settings for throughput claims.

## References

Read only the reference needed for the current boundary, and check it against the pinned dependency:

- [franz-go producing and consuming](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md)
- [franz-go transactions](https://github.com/twmb/franz-go/blob/master/docs/transactions.md)
- [kgo API](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo)
- [Ownership](../go-data-ownership/SKILL.md), [I/O batching](../go-io-batching/SKILL.md), [Unit testing](../go-unit-testing/SKILL.md), and [Profiling](../go-profiling-benchmarks/SKILL.md) when their triggers apply.
