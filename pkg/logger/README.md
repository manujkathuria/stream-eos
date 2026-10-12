# Logger

Configure once, then call the package directly:

```go
if err := logger.New(logger.Config{
    Service: "ingestor",
    Level:   slog.LevelInfo,
}); err != nil {
    return err
}

logger.Info(ctx, "received %d records", 4)
logger.Warn(ctx, "retry attempt %d", 2)
logger.Error(ctx, "publish failed: %v", err)
logger.Debug(ctx, "offset=%d", offset)
```

Defaults: JSON output, INFO level, stdout. WARN hides INFO and DEBUG.
Disabled levels skip message formatting, but Go still evaluates argument expressions.
`Error` does not exit. `New` sets slog's process-wide default and redirects standard
`log` output. Configure at startup; the caller owns any custom output writer.

```bash
GOWORK=off go test -race ./logger
GOWORK=off go test ./logger -run '^$' -bench '^BenchmarkPrintf$' -benchmem
```

Run these from `pkg/`. Benchmarks exclude real output I/O and startup; prior
structured-attribute benchmark results do not describe this printf API.
