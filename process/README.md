# Process

Independent Go service: Kafka input → partition-local FIFO deduplication → JSON → transactional Kafka output, dedup state, and input progress.

Run `make demo` from the repository root for the [verified showcase demo](docs/demo.md).

Start with the [code journey](docs/journey.md). The [deduplication walkthrough](docs/deduplication.md) connects a small ring-buffer example to the Go structures, transaction ordering, and restart recovery.

```text
cmd/process/       Config, startup, recovery wiring, lifecycle
internal/engine/   Batch orchestration and reusable JSON encoding
internal/dedup/    Committed FIFO and pending event IDs
internal/kafka/    Explicit partition reads, transactions, state recovery
internal/protocol/ Private FIFO recovery metadata; shared EventID alias
```

From this directory:

```bash
cp .env.example .env
make build
make run                         # Uses PARTITION from .env
make run ARGS='-partition 1'      # Override for another owner
make test
make vet
make bench
make bench-transaction           # Real Kafka commit, output and recovery checks
make bench-json                  # Production binary-to-JSON encoding
make bench-dedup                 # Classification and full-window eviction
make bench-consumer              # Real Kafka reads and event-ID extraction
make test-kafka                  # Running local broker; isolated test topics
```

Configuration precedence: defaults → optional `.env` → environment → flags. An explicit non-negative partition is required. Existing consumer flags are retained, including offset, batch limits, FIFO capacity, and legacy-bootstrap opt-in. `LOG_LEVEL=debug` enables batch logs; normal operation logs lifecycle events.

The service restores committed FIFO state before reading. It uses Kafka application checkpoints rather than consumer-group offset commits. IDs outside the configured FIFO window may be emitted again. Transaction failures stop processing; restart resolves state from Kafka. Output readers must use `read_committed`.

See [component benchmark results](BENCHMARKS.md). Kafka stays running when this service exits.
