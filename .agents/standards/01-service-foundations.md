# Service foundations: config, logging, and context

Apply when creating a long-running service or changing startup, configuration, logging, or shutdown.

## Configuration

- Each service owns typed configuration and validates it before accepting work. Keep environment reads out of request and processing loops.
- Where supported, precedence is explicit command-line settings, environment, optional local .env, then defaults. Document supported sources. Missing optional .env is acceptable; malformed or explicitly required configuration fails clearly.
- Commit safe .env.example values and ignore local .env files. Never log secrets. Pass validated configuration explicitly rather than using global service state.

## Logging

- Construct a service-owned structured slog logger and pass it to components that log. A reusable constructor can be shared when justified; no particular logger package is required.
- Add stable service attributes during setup and request or operation context where useful. Log lifecycle events and actionable errors without exposing secrets or sensitive payloads.
- Avoid unnecessary hot-path logging and expensive disabled-log calculations. The service owns log outputs and cleanup; measure logging costs when relevant.

## Context and lifecycle

- Command code owns signals, dependency wiring, and the root context. Pass context explicitly through cancelable request, network, and database operations.
- Give servers, connections, workers, and goroutines clear lifecycle owners. Cancellation must reach blocking operations; document client limitations.
- Set appropriate request, I/O, and shutdown deadlines. Stop intake and finish or abandon in-flight work according to the operation's contract. Cleanup needs a bounded context that remains usable after run cancellation.
- Track and close upgraded WebSocket sessions explicitly; HTTP shutdown alone does not close them. For long-lived connections use appropriate heartbeat and idle deadlines rather than ordinary request-duration assumptions.
- Return errors to the command, release resources before exit, and distinguish expected shutdown from failures.

Verify configuration precedence, invalid inputs, and shutdown behavior when changing those contracts. This standard does not authorize unrelated service migrations.
