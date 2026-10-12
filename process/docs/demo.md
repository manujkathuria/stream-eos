# Run the showcase demo

From the repository root:

```bash
make demo
```

Prerequisites: Go matching `go.work` (currently 1.27.1), Make, and a running Kafka broker reachable at `localhost:9092`. The demo is verified on macOS with native Kafka 4.x. It needs topic creation/deletion permission, transactions, and compacted topics; it uses plaintext without authentication. Dependencies may download on the first build. It does not install or start Kafka.

For a different reachable broker:

```bash
make demo DEMO_BROKERS=localhost:19092
```

A single development broker must support transactions with `transaction.state.log.replication.factor=1` and `transaction.state.log.min.isr=1`. Broker setup belongs to the reviewer’s environment; the demo does not change its configuration.

## What happens

1. Build the real service executables; verify broker connectivity.
2. Create four uniquely named `stream-eos-demo-<random>-*` topics, each with one partition. Progress and dedup topics use compact-only retention.
3. Start the real source on a temporary loopback port, plus ingestors A/B. Each ingestor receives 20 messages with two events each.
4. Verify 80 input records, A/B provenance, shared event IDs, and consistent payloads.
5. Run the real process executable until checkpoint 80. Read output with `read_committed` and check exactly one complete JSON payload and correct instrument key per unique input ID.
6. Stop process. Append one retained duplicate to Kafka and ingest two fresh events from the source. Start a new process, then verify checkpoint 83 and exactly two additional outputs. The retained duplicate must remain suppressed after recovery.

The replay is deliberately appended to Kafka by the demo; the WebSocket source does not provide a replay API. FIFO capacity is 1,024, large enough to retain all demo IDs. This demonstrates successful restart recovery, not a crash at every possible transaction boundary or a throughput benchmark.

## Expected result

```text
PASS ingestion: A=40, B=40, unique IDs=40, duplicate copies=40
PASS processing: 40 unique committed JSON outputs; checkpoint=80
PASS restart: retained duplicate suppressed, two fresh events delivered; checkpoint=83, outputs=42
PASS: duplicate suppression, restart recovery, and fresh-event delivery verified. Kafka remains running.
```

The number of shared IDs can vary with connection timing. Checks derive the expected output from actual input and require A/B overlap; they do not assume exactly 40 unique IDs.

## Cleanup and automation

The runner has a two-minute timeout after build. It stops only its own child services and deletes only topics whose creation it confirmed. Normal success removes temporary logs/configuration. On failure it exits non-zero and prints the retained log directory. Child shutdown and topic cleanup are bounded; a cleanup failure also fails the demo. A forced kill of the demo runner or broker outage may require removing its printed topic prefix manually.

User `.env` files and service configuration environment variables are not used by child services. Kafka and unrelated topics remain running and unchanged. Builds leave executables in each project’s `bin/`.

An AI reviewer can run `make demo` non-interactively once prerequisites are available. Exit status zero means every verification and cleanup step passed; read FAIL output and retained logs on a non-zero result. The runner lives in [cmd/demo](../cmd/demo/main.go).

## Live-performance run

Ten instruments are fixed for this demo. `INPUT_TPS` is the requested unique quote events/sec before A/B duplication; each WebSocket message carries 10 events.

```bash
make demo-performance                                  # 10,000 events/sec for 10 seconds
make demo-performance INPUT_TPS=50000 DURATION=20s       # offer 1 million events over time
make demo-performance INPUT_TPS=100000 DURATION=60s
```

The second command offers one million events; it does not guarantee exactly one million are delivered. Rates are offered load, not achieved throughput. `INPUT_TPS=100000` translates to 10,000 WebSocket messages/sec per connection, potentially producing about 200,000 Kafka input records/sec with A/B copies. No instrument-count option is exposed by the demo.

The runner starts the real process, source and A/B ingestors, stops ingestion after the requested duration, and drains to the final committed checkpoint. One processing partition uses batches of up to 100 records and a one-million-ID FIFO. No input is preloaded. Duration supports 1 second to 1 hour. Input TPS must be a multiple of 10, from 10 to 10 million (the source configuration limit); these limits are not capacity claims. The old `DEMO_RATE`/`DEMO_DURATION` interface has been replaced by `INPUT_TPS`/`DURATION`.

### Report and verification

The report prints requested unique TPS, actual Kafka input/A/B counts, committed unique outputs, duplicates, intake time, observed drain time, and achieved rates. Throughput uses ingestor launch through final checkpoint observation, including startup and shutdown. Builds, topic setup, and post-run verification are excluded. Drain timing includes checkpoint observation overhead.

### Verified local run — 12 October 2026

Run from the repository root against the existing local Kafka broker:

```bash
make demo-performance INPUT_TPS=100000 DURATION=60s
```

This run used Go 1.27.1 on macOS/arm64 with the single local broker, 10 instruments, two ingestors, and one processing partition described above. It exited successfully with the following measured result:

```text
Measured: intake=60.002s (includes startup/shutdown), observed drain=25.273s, total=85.274s
Verified counts: input=9800970 (A=4889230 B=4911740), unique output=4911750, duplicate copies=4889220, final checkpoint=9800970
Requested: 100000 unique events/sec. Achieved live + drain: 114934 Kafka input records/sec, 57599 committed unique outputs/sec
PASS: live pipeline output verified. Kafka remains running.
```

The disk-backed verifier confirmed one committed output per unique Kafka input ID and matching SHA-256 payload/key fingerprints. Verification and cleanup completed successfully. These are observations from one local run, not expected counts or throughput thresholds for subsequent runs. The achieved rate includes drain time; the requested 100,000 unique events/sec is offered load. The result does not prove delivery of every generated source event, maximum sustained capacity, or multi-broker fault tolerance.

### Disk-backed verifier and limits

After draining, verification streams input and committed output through bounded Kafka polls. It sorts event IDs and SHA-256 fingerprints of routing keys and full JSON payloads into temporary files, using 16,384-record chunks and two-way merge passes. It rejects missing/extra/duplicate output IDs and conflicting or mismatched payload fingerprints. SHA-256 matching is probabilistic payload verification; the small correctness demo retains its exact-byte checks.

Verifier memory does not retain the entire run. Each sort chunk holds 896 KiB of fingerprints; Kafka fetch buffers, decoding and runtime memory are additional. Temporary disk scales with records: 56 bytes per fingerprint, with extra files during merge passes. Verification files are removed on success or failure, while failed-run service logs are retained. Kafka disk also grows with the workload; deleting demo topics may reclaim space asynchronously.

The deduplication guarantee remains bounded to the one-million-ID FIFO. This verifier requires one output per unique ID across the run and will fail if a late duplicate arrives after eviction and is output again. This does not turn the engine's bounded guarantee into unlimited deduplication.

The performance run allows up to two minutes to drain and an overall duration plus ten minutes for startup, drain and verification. Timeouts fail visibly. Kafka stays running; only demo-owned resources are cleaned up.

The source retains 4,096 updates for these 10 instruments (about 7.2 MiB of quote history). A reader that falls beyond this history still disconnects; slower machines or overload can fail the run. Requested rate is not a delivery guarantee. Verification begins at Kafka storage and does not prove that every generated source event reached Kafka. The report does not establish maximum capacity, latency percentiles, or allocation-free network I/O.
