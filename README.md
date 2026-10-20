# High-Throughput Exactly-Once Streaming Engine in Go

A Go backend showcase that turns a live market-data feed into a replayable stream, removes duplicate deliveries, and recovers processing state after restart.

**[Start with the data journey →](docs/journey.md)**  
A plain-language introduction for recruiters, managers, and engineers. No setup required.

```text
WebSocket source → Ingestors A/B → Kafka input
    → Go engine: deduplicate + convert to JSON
    → Kafka transaction: output + duplicate state + progress
```

## What it demonstrates

- **Streaming correctness:** redundant ingestion, partition ownership, transactional output, and persistent FIFO recovery.
- **Go performance engineering:** bounded batches, reusable buffers, explicit memory ownership, and allocation benchmarks.
- **Verification:** unit tests, race checks, and real Kafka commit, abort, fencing, and recovery tests.

Output, duplicate state, and input progress commit together. Duplicate suppression survives restart while IDs remain in the configurable FIFO window (default: one million unique IDs per partition). Evicted IDs can be accepted again. Events must first be received and stored in Kafka.

The feed is synthetic and uses a Kite-style binary schema. The current implementation ends at Kafka output; local verification uses one broker. A bounded live-performance run has passed; maximum sustained capacity and replicated broker failure tolerance remain unverified.

## Showcase

1. [The data journey](docs/journey.md) — what happens to an event, from live feed to Kafka output.
2. [Why we built it this way](docs/design-decisions.md) — the problems, decisions, and tradeoffs.
3. [How deduplication works](docs/deduplication.md) — pending IDs, committed history, and the FIFO ring.

## Projects

| Project | Responsibility |
| --- | --- |
| [websocket-source](websocket-source/README.md) | Synthetic market feed over WebSocket |
| [ingestor](ingestor/README.md) | Redundant ingestion into Kafka |
| [process](process/README.md) | Deduplication, JSON output, and transactional recovery |
| [pkg](pkg/README.md) | Shared protocol and logging |

Each project owns its documentation and benchmarks. `kafka/` contains the local broker setup; root files coordinate the Go workspace.

## Try it locally

Install Go matching `go.work` and Make, and start a transaction-capable Kafka broker at `localhost:9092`. From the repository root, run the correctness demo:

```bash
make demo
```

This runs the real source, A/B ingestors, and process, verifies unique committed output, then restarts the process to check deduplication recovery and fresh-event delivery.

To exercise the live pipeline with a requested 100,000 unique events/sec for 60 seconds:

```bash
make demo-performance INPUT_TPS=100000 DURATION=60s
```

The performance demo stops intake, drains processing, and verifies event IDs and payload/key fingerprints with a disk-backed sorter. A local run on 12 October 2026 verified **9,800,970 input records** and **4,911,750 unique outputs**, achieving **57,599 committed unique outputs/sec** including startup, shutdown, and drain. The requested rate is offered load; verification begins at Kafka storage and does not prove delivery of every generated source event.

Both demos print PASS/FAIL, exit non-zero on failure, and clean up their services and isolated topics while leaving Kafka running.

See [demo prerequisites, expected output, and cleanup](process/docs/demo.md). Use each project’s README for individual run commands.

## Automated verification (CI)

Every push to `main` and all pull requests run GitHub Actions workflow checks using the Go version declared in [go.work](go.work):

- **Build (`make build`):** compiles binaries for `websocket-source`, `ingestor`, and `process`.
- **Static analysis (`make vet`):** runs `go vet` across all workspace modules (`websocket-source`, `pkg`, `ingestor`, `process`).
- **Unit and race tests (`make test`):** runs `go test -race ./...` across all workspace modules.

**Excluded from CI:** Broker-backed integration tests (`STREAM_EOS_KAFKA_TEST=1`), performance benchmarks, and end-to-end pipeline demos (`make demo`, `make demo-performance`) require an active, transaction-capable Kafka cluster and run locally or in dedicated benchmark environments.

