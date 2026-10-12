# WebSocket ingestor

An independent Go service that subscribes to the synthetic feed, preserves event identities, and publishes acknowledged records to Kafka. A and B run this same project with different identities; expected copies are deduplicated by the downstream engine.

Start with the [code journey](docs/journey.md).

```text
cmd/ingestor/        Config, logging, signals, dependency wiring, cleanup
internal/ingest/     Sequential read → decode → publish orchestration
internal/websocket/ Connection, subscriptions, reusable message buffer
internal/kafka/     Binary record construction and Kafka acknowledgements
../pkg/protocol/    Shared event identity and source-envelope decoder
internal/benchtest/ Shared fixtures, imported only by tests/benchmarks
```

From this directory:

```bash
cp .env.example .env
make build
make test
make vet
make run-a
# In another terminal:
make run-b
```

The source and Kafka must already be running, and `market.ticks.raw` must exist. Build output is `bin/ingestor`. Root `make ingest-a` and `make ingest-b` enter this project automatically.

Configuration precedence: CLI overrides → process environment → optional `.env` → defaults. Existing flags such as `-id`, `-url`, `-tokens`, and `-max-messages` remain supported. `.env.example` lists settings. `.env` is local and ignored; no shell expansion or execution is performed.

The reusable read buffer starts at 512 bytes, grows occasionally, and skips/logs messages above the protocol limit. Decoding reserves a 400-event slice once at startup and reuses it with `DecodeInto` (zero allocations in the decoder benchmark). Kafka record construction reserves one 400-event batch at startup and reuses owned storage after all publish callbacks finish. Its warmed construction benchmark reports zero allocations; Kafka client work still allocates. Publish and shutdown calls must be sequential; overlap is rejected.

`make bench` measures buffer reading, orchestration/decoding, record construction, and shared decoding. `make bench-reader` measures the actual WebSocket reader; `make bench-kafka` measures acknowledgements and the connected ingestion loop. See [results and measurement boundaries](BENCHMARKS.md). Run the isolated connected broker test with:

```bash
STREAM_EOS_KAFKA_TEST=1 GOWORK=off go test -race ./internal/ingest \
  -run '^TestWebSocketToKafkaIntegration$' -count=1 -v
```

Ingestion batches are not transactional. Errors can follow partial success; the service stops rather than inventing replay. No automatic reconnect or deduplication is implemented here.
