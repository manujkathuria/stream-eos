# Service foundations: config, logging, and context

Apply when creating a long-running service or changing its startup, configuration, logging, or shutdown. These are service responsibilities; reusable libraries receive the dependencies they need.

## Configuration

- Each service owns a typed configuration and validates it once before accepting work. Keep environment access and parsing out of processing loops.
- Load environment variables, with `.env` as a local-development convenience. Existing environment values take precedence over `.env`, followed by defaults. Explicit command-line overrides, when supported, have highest priority.
- Commit `.env.example` with supported settings and safe example values; ignore `.env`. Do not log secret values. A missing optional `.env` is acceptable; malformed or explicitly required configuration must fail clearly.
- Pass validated settings to components. Share small loading utilities when useful, not one global configuration for unrelated services.

## Logging

- Create a service-owned structured `slog` logger using the shared logger package. Pass it to components that log; avoid replacing global defaults or reading a logger from a global variable.
- Attach stable service attributes during setup. Prefer typed `LogAttrs` in frequent calls and guard expensive disabled-level calculations with `Enabled`.
- Log lifecycle events and actionable errors; avoid per-event logging in streaming loops. The service owns output resources. State whether logging is synchronous, buffered, or asynchronous rather than assuming it is free.

## Context and graceful shutdown

- The command owns a signal-aware root context. Pass context explicitly into cancelable operations; do not introduce a shared global context.
- Give every server, connection, worker, and goroutine a lifecycle owner. Cancellation must unblock I/O and processing, not just set a flag.
- Define shutdown behavior: stop intake, finish or abandon in-flight work according to the delivery contract, release resources, and wait for owned goroutines. Use a bounded shutdown deadline that can still perform cleanup after the run context is canceled.
- Handle upgraded WebSocket sessions explicitly: HTTP server shutdown alone does not close them. Transactional components must preserve their established commit/abort and unknown-outcome rules during shutdown.
- Return errors to the command. Perform cleanup before the command chooses an exit code; distinguish expected shutdown from failures in logs.

## Startup journey

```text
Load and validate config → create logger → create signal context
    → construct dependencies → run service → bounded shutdown → exit
```

Verify invalid configuration, precedence, and cancellation/shutdown behavior when changing those contracts. Applying this standard does not authorize unrelated migrations of existing services.
