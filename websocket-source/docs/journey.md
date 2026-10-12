# Source journey: follow the code

This service generates synthetic market updates and delivers them over WebSocket. It does not publish to Kafka. Follow this guide to understand which component owns each step.

```text
Startup → connect and subscribe → wait for rate timer
    → market.Frame → generateUpdate if needed → protocol.BuildMessage
    → WebSocket write → advance connection position → repeat
```

## 1. Start the service

[cmd/source/main.go](../cmd/source/main.go) loads configuration, initializes logging, and creates the signal-aware process context. `run` creates the feed handler and HTTP listener; `serve` owns running and shutting down the service.

[feed/handler.go](../feed/handler.go) exposes `NewHandler`, `ServeHTTP`, and `Shutdown`. It connects the private components and tracks active session handlers.

## 2. Create one shared market

[market.New](../internal/market/market.go) initializes the configured synthetic instruments (`INSTRUMENT_COUNT=10` by default, range 1–400), a random source-run ID, and storage for the configured history (128 updates by default; `HISTORY_UPDATES` supports 1–4,096). Each history slot holds only the configured instruments: 184 bytes per instrument. One backing allocation stores all slots, with each slot borrowing its own fixed slice. At 10 instruments and 4,096 slots, quote storage is about 7.2 MiB, plus slot headers and current quote state.

This happens once per handler, not once per connection. All clients observe the same market. The retained history supports fan-out to existing readers; it is not a client replay API.

## 3. Connect and subscribe

[websocket.Handler](../internal/websocket/session.go) upgrades a connection, sends a welcome notice, and creates its session state:

- Subscribed instruments and full-mode selection.
- A position in the shared market.
- A reusable message buffer sized for the configured instrument count.

A reader goroutine handles client commands through [subscriptions.apply](../internal/websocket/subscriptions.go). One writer loop sends data, notices, and heartbeats. Without active subscriptions, the session sends periodic heartbeats.

## 4. Find or generate an update

Once instruments are active, the session joins the live position and waits for its rate timer before requesting updates. The default is at most 1,000 market messages per second per connection.

[Market.Frame](../internal/market/market.go) locks the shared market and checks the requested position:

- An existing retained position reuses its update.
- A new position calls [generateUpdate](../internal/market/generation.go), advancing all configured quotes and encoding their packets into the chosen history slot.
- A position outside the retained range returns an error.

The fastest reader drives generation. Another client reading the same position receives the same event bytes and identities. Generation receives an explicit timestamp; the coordinator supplies the clock reading.

## 5. Construct the client's message

[protocol.BuildMessage](../internal/protocol/message.go) copies only the selected instruments into the session-owned buffer:

```text
Header: version + source-run ID + packet count
Per packet: sequence + packet length + 184-byte quote
```

The event identity is the source-run ID plus sequence. Filtering instruments does not renumber events. The sequence keeps its existing 400-position stride for wire compatibility; unused positions are simply absent. Two selected quotes produce a 410-byte envelope.

The market lock stays held until copying finishes. No reference to mutable history is returned; the connection owns the resulting message bytes.

## 6. Send and repeat

The writer calls Gorilla's `WriteMessage` with the valid portion of its buffer. After success, it advances the connection's position and reuses the buffer for the next message. It does not create an unbounded send queue.

Writes have a deadline. A write failure, peer disconnect, or lag beyond the retained history ends the session. A configured `DISCONNECT_AFTER` can also end it deliberately. New connections join live; they cannot request missed events.

## 7. Shut down

The command stops HTTP acceptance and calls `feed.Handler.Shutdown` with a fresh bounded cleanup context. The handler rejects new sessions, cancels existing ones, and waits for completion. Sessions close their sockets and finish their reader goroutines.

HTTP shutdown alone does not close upgraded WebSockets; the feed owns that cleanup explicitly.

## Where to measure

| Stage | Package | Benchmark |
| --- | --- | --- |
| Generate and encode updates | `internal/market` | `BenchmarkGenerateUpdate` |
| Construct a message | `internal/protocol` | `BenchmarkBuildMessage` |
| Coordinate a retained read, including locking | `internal/market` | `BenchmarkSequencedFrameReuse` |
| Deliver prebuilt bytes over loopback WebSocket | `internal/websocket` | `BenchmarkWebSocketDelivery` |

Tests and benchmarks live beside their implementation. See [measurement boundaries and commands](../BENCHMARKS.md) before interpreting the results; stage benchmarks do not establish whole-source throughput.
