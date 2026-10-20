# Agent guide — version 15

## Project

Build **High-Throughput Exactly-Once Streaming Engine in Go** as a showcase for a senior backend engineering role. Read [showcase decisions](docs/design-decisions.md) and [README.md](README.md) for scope and project navigation. Architecture decisions are developed with the user; discussion alone does not authorize application implementation.

## Established scope and architecture

- The independent `websocket-source/` Go module owns the synthetic source; `cmd/source` handles startup; `feed` exposes the handler/lifecycle boundary; `internal/market`, `internal/protocol`, and `internal/websocket` own generation/history, framing, and delivery respectively. The root Makefile checks all workspace modules. `pkg/logger` provides reusable structured `slog` construction; services own their logger and output lifecycle.
- The source generates and retains only its configured instrument universe (`INSTRUMENT_COUNT`, default 10, maximum 400). Subscriptions filter that universe; event sequences retain their existing 400-position stride. The performance demo uses 10 instruments, the correctness demo uses two.
- A synthetic WebSocket source generates binary market quotes using Kite-style 184-byte quote packets. It does not connect to Zerodha. Shared envelope decoding lives in `pkg/protocol`.
- The source assigns stable, unique `(source-run ID, sequence)` event identities before delivery. Both ingestors receive the same identity and payload when observing the same event; redeliveries preserve that identity.
- The independent `ingestor/` module owns config, sequential ingestion, WebSocket reading, and binary Kafka production. Shared source event identities and decoding live in `pkg/protocol`; engine FIFO metadata remains private.
- The independent `process/` module owns explicit partition consumption, FIFO deduplication, reusable JSON encoding, transactional output/state/progress, and recovery. `cmd/process` owns typed `.env`/environment/flag config, logging, signals, and startup. Private packages are `internal/engine`, `internal/dedup`, `internal/kafka`, and `internal/protocol`. The root is a workspace coordinator with no application Go module; aggregate checks include source, ingestor, process, and pkg.
- Two independent ingestors, A and B, each have a Kafka producer. Both publish to `market.ticks.raw`, keyed by instrument token, preserving source identity. Duplicate input records are expected.
- Kafka is the engine's only input. Kafka offsets are partition positions, not source event identities.
- The current engine atomically commits JSON output and next input offset as an application checkpoint in a compacted Kafka progress topic. Explicit consumers do not join a group or commit Kafka-managed group offsets. Downstream readers use `read_committed`. Changed FIFO slots and recovery metadata commit in that same transaction.
- Consumers are explicitly assigned: consumer 0 → partition 0, consumer 1 → partition 1, consumer 2 → partition 2. Unowned partitions wait for a replacement; no automatic reassignment is planned. Stable transactional IDs derived from pipeline topics and partition fence replaced producers. Deployment leases/orchestration are not implemented.
- Duplicate checks run in Go. Startup initializes/fences the transactional producer before loading committed Kafka progress. Startup restores the committed FIFO from a compacted Kafka changelog before consuming. Redis and database sinks remain deferred.
- The connected source → A/B ingestion → raw topic → explicit consumer → FIFO dedup → reusable JSON → transactional output/state/progress pipeline is implemented. Local IDs are promoted only after confirmed Kafka commit. Transaction errors stop the process; unknown outcomes are resolved by restoring progress on restart.


## Guarantee and boundaries

Assume stable source event identities and at least one successful ingestion path per event: an ingestor must receive the event and commit it to Kafka. Merely having one instance running is insufficient.

Output, FIFO slot changes, and input progress commit atomically per batch. Restart restores committed membership and FIFO eviction order. The event-ID guarantee is bounded by the configured FIFO window; it is not unlimited deduplication. Missing or incompatible recovery state fails closed.

Source availability, simultaneous ingestion-path failures, and recovery of events never committed to Kafka are outside this guarantee. Source replay is not required. External database writes and HTTP side effects require a separate consistency design. Deduplication retains the last N committed unique IDs per partition with FIFO eviction (default 1,000,000, configurable). Duplicate reads do not refresh order; IDs evicted after successful output may be accepted again. There is no one-hour TTL. FIFO capacity must match the saved checkpoint; changing it requires migration. Both state topics require compact-only retention. Topic deletion/recreation and partition migration require a deliberate recovery/reset design.

## Development environment

Develop locally on an Apple Silicon Mac with native Kafka in single-broker KRaft mode and a modest JVM heap. Avoid Docker for this project's local setup. Use Google Cloud later for heavier benchmarks and multi-broker failure tests. Do not claim broker fault tolerance from the single-broker setup or invent throughput targets.

## Engineering guidance

Work as Go pair programmers under the [pair-programming agreement](.agents/standards/pair-programming.md). The assistant maintains [.agents](.agents/README.md): update agreed guidance and confirmed facts as work evolves, discuss material new rules, and keep the guide concise. Read the baseline once per new task and reuse it while unchanged.

When working on `process/`, start with its [code journey](process/docs/journey.md). Module-local commands use `GOWORK=off`; the executable is `process/bin/process`. Stable transaction identities and existing Kafka recovery formats are preserved across the module split.

When working on `ingestor/`, start with its [code journey](ingestor/docs/journey.md).

When working on `websocket-source/`, start with its [code journey](websocket-source/docs/journey.md) for responsibility boundaries, ownership, and benchmark locations.

Read the [standards index](.agents/standards/engineering-standards.md) when implementing, reviewing, or refactoring. Apply general coding rules and load focused standards by their task triggers: service foundations for config/logging/context, benchmarkable data journeys for streaming stages and measurement, and project structure for module/package boundaries. Reuse unchanged guidance already read. Project-specific decisions and guarantees remain in this guide and the README.

## Incremental skills

Add focused guidance as recurring engineering decisions arise during the work, rather than creating a complete rulebook in advance.

When a skill is introduced, record its path and concrete task triggers here. Infer relevant triggers from the work and read matching skills before making the corresponding decisions; the user should not need to name them. Load only relevant guidance and reuse it while it remains available and unchanged.

| Task triggers | Required skill |
| --- | --- |
| Implementing or reviewing Kafka producers, consumers, partitioning, offsets, rebalances, transactions, or broker integration tests | [go-kafka](.agents/skills/go-kafka/SKILL.md) |
| Creating or changing Go event structs, arrays, slices, buffers, batches, binary codecs, channel payloads, or allocation-sensitive loops | [go-data-ownership](.agents/skills/go-data-ownership/SKILL.md) |
| Defining or modifying repeatedly stored Go structs, including event/depth records and batch elements; investigating struct footprint | [go-struct-alignment](.agents/skills/go-struct-alignment/SKILL.md) |
| Choosing value/pointer semantics, storage lifetimes, buffer reuse, or changing/diagnosing allocation-sensitive code | [go-stack-vs-heap](.agents/skills/go-stack-vs-heap/SKILL.md) |
| Implementing or changing Go network/file I/O loops, database bulk writers, batch sizes, or investigating syscall/scheduling overhead | [go-io-batching](.agents/skills/go-io-batching/SKILL.md) |
| Implementing or fixing Go behavior; adding or reviewing unit tests for protocols, ownership, or streaming correctness | [go-unit-testing](.agents/skills/go-unit-testing/SKILL.md) |
| Making performance claims; optimizing hot paths; investigating throughput, latency, allocations, CPU, memory, or contention | [go-profiling-benchmarks](.agents/skills/go-profiling-benchmarks/SKILL.md) |

For known bounded data, use concrete types, fixed-capacity storage, and explicit ownership/reuse. Keep memory bounded and avoid unnecessary per-event allocations; verify allocation claims rather than assuming stack placement. Triggers are cumulative; load each relevant skill before its decisions.

Reuse established decisions rather than restarting discovery for already-scoped work.

## Repository organization

README.md owns the showcase overview and selected evidence; `docs/design-decisions.md` records cross-project tradeoffs. The complete deduplication walkthrough lives at `process/docs/deduplication.md`. Implementation docs belong to the owning project; consumer walkthroughs were removed in favor of `process/docs/journey.md`. Each service builds to its own `bin/`. Keep root workspace/build/agent files and the existing local `kafka/` infrastructure directory; do not move or stop its running broker as part of cleanup.

Do not restore the removed legacy technical documents or consumer walkthroughs. Keep project documentation focused on the existing READMEs, journeys, and benchmark evidence, alongside root showcase docs.

`make demo` builds real source/ingestor/process executables and runs the bounded showcase in `process/cmd/demo`. It requires an existing transaction-capable broker, uses isolated topics and child-service configuration, verifies committed output and process restart recovery, and cleans up its own resources without stopping Kafka. See `process/docs/demo.md`.

`make demo-performance` runs the live source → A/B → Kafka → process → Kafka pipeline for a bounded configured duration/rate, then drains to a captured checkpoint and verifies output. It accepts `INPUT_TPS` (unique quote events/sec before A/B copies) and `DURATION`, with 10 fixed demo instruments. Post-run verification uses bounded-memory external sorting of event IDs and SHA-256 payload/key fingerprints; temporary disk scales with the run. It reports achieved throughput including startup/drain, not maximum capacity or proof of pre-Kafka source delivery.

## Development workflow

Follow the agreed [development workflow](.agents/standards/development-workflow.md) for issue structure, branches, optional draft PRs, verification, and merging. This standard is the confirmed baseline; merge strategy and release policy remain project decisions. Existing Git skill suggestions do not finalize those policies.
