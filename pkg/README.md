# Shared packages

`protocol/` owns source event IDs and binary-envelope decoding. `logger/` provides the shared slog-backed logger.

Services own configuration, cancellation, buffers, and connection lifecycles.

```bash
make test
make vet
make bench-protocol
make bench
```
