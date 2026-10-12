# Synthetic WebSocket source

An independent Go module that generates binary market quotes and delivers them over WebSocket. It does not depend on Kafka, ingestion, or the streaming engine. Startup uses the shared [logger package](../pkg/logger/README.md), which has no third-party dependencies.

## Where to start

Read the [source journey](docs/journey.md) to follow startup, generation, message construction, delivery, and shutdown through the code.

```text
cmd/source/           Config, logger, signals, HTTP startup and shutdown.
feed/                 Small public handler and session lifecycle boundary.
internal/market/      Quote generation and shared bounded update history.
internal/protocol/    Packet snapshot types and binary message construction.
internal/websocket/   Subscriptions, pacing, heartbeats and socket delivery.
```

Read [feed/handler.go](feed/handler.go) first, then follow the data journey:

```text
websocket session → market.Frame → generateUpdate (new position only)
                                → protocol.BuildMessage → WebSocket write
```

The market holds its lock until framing finishes, so another reader cannot
overwrite the history slot while its bytes are copied. Each session owns its
message buffer and reuses it only after its synchronous write returns.

`feed.NewHandler(Config)` and `Shutdown(ctx)` remain the public integration-test
boundary. Private packages are deliberately unavailable to other modules.
Tests and benchmarks live beside each responsibility: generation in `market`,
construction in `protocol`, delivery in `websocket`. See [BENCHMARKS.md](BENCHMARKS.md)
for scopes, results and commands. No artificial interfaces separate these stages.

Run from this directory:

```bash
cp .env.example .env
make build
make test
make vet
make run
```

`make build` writes the source executable to the project's `bin/source`. Each command disables workspace resolution to verify that this module works independently. The server listens at `ws://127.0.0.1:8081/ws`.

For direct commands:

```bash
GOWORK=off go run ./cmd/source
```

Quotes have a fixed 184-byte layout; market envelopes include stable source-run IDs and sequences. A/B readers observing the same event receive the same identity. Connections join live; no replay API is implemented.

## Configuration and shutdown

Startup loads `.env` from the current working directory using only the Go
standard library. Run from `websocket-source/`, including when using `./bin/source`.
CLI configuration arguments are no longer accepted. The local `.env` is ignored
by Git; `.env.example` documents the defaults.

```dotenv
LISTEN_ADDR=127.0.0.1:8081
RATE=1000
INSTRUMENT_COUNT=10
HISTORY_UPDATES=128
DISCONNECT_AFTER=0
LOG_LEVEL=info
LOG_FORMAT=json
SHUTDOWN_TIMEOUT=5s
```

The file supports `KEY=VALUE`, blank lines, full-line `#` comments, and matching
single or double quotes around a value. Unknown keys, duplicate keys, malformed
lines, and invalid settings stop startup. Omitted keys use defaults; the file
itself is required. There is no shell execution, variable expansion, inline
comment parsing, or process-environment override.

The source generates only `INSTRUMENT_COUNT` instruments (1–400, default 10),
with tokens `9910001 + index × 256`. Subscriptions select from that configured
universe; unavailable tokens are rejected. History and session buffers are
allocated once at the configured size.

Logging uses the shared package's `New`, `Info`, and `Error` functions.
`LOG_LEVEL=warn` hides INFO and DEBUG. `LOG_FORMAT=text` selects readable console
output. `DISCONNECT_AFTER=0` disables the simulated disconnect.

Ctrl-C and SIGTERM cancel the process context. Shutdown stops accepting HTTP
requests, rejects new feed sessions, sends active WebSockets a going-away close
message, closes their connections, and waits for session handlers to finish.
Cleanup uses a fresh context bounded by `SHUTDOWN_TIMEOUT`; the canceled process
context is not reused for cleanup. HTTP shutdown alone does not close upgraded
WebSockets, so the feed explicitly owns their shutdown. A timeout returns an
error and forces remaining ordinary HTTP connections closed.

## Stage benchmarks

Run `make bench` for generation/construction and `make bench-delivery` for isolated loopback delivery. See [measurement boundaries and results](BENCHMARKS.md).
