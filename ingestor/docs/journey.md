# Ingestor journey: follow the code

```text
Load config → initialize logging and signal context → connect Kafka
    → connect and subscribe to WebSocket
    → read message → decode events → build Kafka records → wait for acknowledgements
    → repeat → close source and clean up producer
```

## 1. Start and connect

[cmd/ingestor](../cmd/ingestor/main.go) owns startup and cleanup. Its [typed config](../cmd/ingestor/config.go) loads optional `.env`, environment values, and CLI overrides, then validates before connecting. A and B differ by `INGESTOR_ID`/`-id`; they share the event protocol and Kafka topic.

Startup configures the shared logger and creates a signal-aware context. Kafka is checked before subscribing, so the source connection starts only after the producer is available.

## 2. Read a market message

[websocket.Dial](../internal/websocket/source.go) connects and sends subscribe/full-mode commands. `Source.Read` uses `NextReader` and a reusable byte slice, skipping heartbeats and handling text notices. Oversized messages are logged, drained, and skipped on the same connection. Transport failures still stop reading.

Returned bytes are borrowed until the next `Read`. The context passed to `Dial` owns the connection lifetime. Its cancellation handler is registered once, closes even an idle connection, and unblocks reading/draining. `Close` releases the registration. Reads using that same cancellation signal need no per-message registration; a separate read context remains supported with a temporary handler.

## 3. Decode events

[Ingestor.Run](../internal/ingest/ingestor.go) passes the complete message to the [shared protocol decoder](../../pkg/protocol/protocol.go).

The ingestor reserves storage for 400 events once at startup and passes it to `DecodeInto`. The decoder validates the complete envelope before filling that storage, extracts each 24-byte event ID and instrument token, and returns events whose quote slices borrow the message bytes. It never grows the destination; insufficient capacity returns an error. Decoding performs no per-message allocations. After publishing finishes, the ingestor clears event references before reading again. It does not decode all price/depth fields, change identity, or deduplicate.

This parser is shared with the engine's event-ID type through `pkg/protocol`; the ingestor has no dependency on engine internals.

## 4. Construct and publish records

[Producer.Publish](../internal/kafka/producer.go) validates the batch and calls `buildRecords`. Each event becomes:

```text
Key:   instrument token (4 bytes)
Value: SEV1 marker (4) + event ID (24) + original quote (184)
Header: ingestor identity A or B
```

Record construction copies borrowed quote bytes into startup-reserved storage for up to 400 records. Keys, values, headers, and record wrappers are reused; client-modified metadata is reset each time. Returned record pointers are borrowed until the next construction call. Publishing uses the pinned franz-go client's idempotent producer, identical key partitioning for A/B, and all-ISR acknowledgements. These settings do not remove independent A/B copies or make a batch atomic.

`ProduceSync` waits for every record completion before returning. Only after that boundary are wrappers cleared and their storage available for reuse, including on failure. An atomic lifecycle guard rejects overlapping publish/shutdown calls; the normal ingestion loop remains sequential. On success, the runner increments acknowledged-message/event counts and reads again. On failure, it stops; some records may already have succeeded. There is no application retry of the whole batch or source replay assumption.

## 5. Shut down

The command owns the source and producer. It stops the sequential runner, closes the source, and uses a fresh cleanup context bounded by `SHUTDOWN_TIMEOUT` to flush Kafka. If flushing fails, it attempts to abandon buffered records, closes the client, and reports cleanup failure. Cancellation is not a guarantee that an in-flight Kafka record was never delivered.

## Where to test and measure

Tests sit beside config, orchestration, and transport. The WebSocket adapter test uses the actual mock source's public handler. The opt-in Kafka integration test verifies stored wire bytes and A/B provenance in an isolated topic.

`BenchmarkMessageBuffer` measures reusable reads without network I/O, `BenchmarkDecodeInto` measures parsing into reused storage; `BenchmarkDecode` retains the allocating convenience API as a reference, and `BenchmarkBuildRecords` measures binary Kafka record construction. `BenchmarkSourceRead` adds live loopback reading; `BenchmarkPublishAcknowledged` adds broker acknowledgements; `BenchmarkIngestorAcknowledged` exercises the connected loop. See [results and measurement boundaries](../BENCHMARKS.md) and the [project README](../README.md) for commands.
