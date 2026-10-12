# Go project and module boundaries

Apply when creating services, moving packages, splitting modules, or changing workspace build/test commands.

- Keep the repository as a workspace. Put independently built services in clearly named project directories, each with its own `go.mod` and `go.sum`.
- Keep executable startup under `cmd/<name>/`: configuration, dependency wiring, process lifecycle, and cleanup. Domain behavior and network adapters belong in packages outside `cmd`.
- Prefer `internal/` for private implementation. Expose a small public package only when another project has a concrete need to reuse its boundary, such as starting a protocol server in integration tests. Do not import another project's private implementation.
- Service modules must not depend on downstream applications. Shared event formats can become a separate contract module when multiple services need them; avoid copying protocol definitions or extracting speculative shared libraries.
- `go.work` is a development convenience, not a requirement for building a service. Verify each module with `GOWORK=off`; use explicit local replacements for repository dependencies where necessary.
- Keep tests beside their implementation. Root build/test targets must explicitly include every project; `go test ./...` in one module does not cover nested modules.
- Separate responsibilities before adding packages: startup, domain behavior, orchestration, and transport should have clear owners. Add further package boundaries when they enforce a useful dependency direction, not just to create more folders.

## Responsibility-based service layout

Organize private packages around meaningful stages of the service journey. A developer or agent should be able to locate a responsibility, its production entry point, its tests, and its benchmarks without reconstructing a flat directory of unrelated files.

```text
<service>/
├── go.mod
├── cmd/<service>/        Configuration, wiring, startup and shutdown
├── <public-boundary>/   Optional small API for concrete external consumers
├── internal/
│   ├── <domain>/        Domain state and generation/transformation
│   ├── protocol/        Encoding, decoding and wire contracts, when needed
│   └── <transport>/     Connections, subscriptions, pacing and delivery
├── docs/journey.md      Code journey and ownership map
├── README.md            Short navigation guide and local commands
└── Makefile             Explicit build, test and benchmark targets
```

- Name packages after responsibilities. Choose stages appropriate to the service; do not create empty folders or copy this template mechanically.
- Keep the public boundary small. It composes private components and exposes only the lifecycle/API that external consumers actually need. If no public consumer exists, keep this boundary internal too.
- Make dependencies flow toward domain logic and protocol contracts. Protocol code must not depend on connections or service lifecycle. Avoid package cycles and do not move domain helpers into protocol merely to share them.
- Coordinators own history, synchronization and stage sequencing. Transport owns connection state and pacing. Keep mutable-buffer lifetimes safe when passing across packages; do not expose borrowed state after releasing its protecting lock.
- Keep `*_test.go` and `*_benchmark_test.go` beside their production stage. Give test files responsibility-based names, such as `subscriptions_test.go`, rather than ambiguous names such as `main_test.go` for domain behavior.
- Function boundaries enable focused measurement; package boundaries enforce responsibility and dependency direction. Benchmarks call production stages directly, following [benchmarkable data journeys](02-benchmarkable-code.md).
- Each service README points to `docs/journey.md`. The journey follows runtime execution, links to code entry points, states ownership and failure behavior, and identifies benchmark locations. Keep navigation concise; link deeper details rather than expanding the README indefinitely.
- Keep module-local build/test/benchmark commands usable independently, and include them in repository aggregate targets. Treat benchmark results as scoped evidence, not a consequence of directory structure.

## Example: WebSocket source

```text
cmd/source → feed handler → internal/websocket sessions
                                → internal/market coordinator
                                    → generateUpdate
                                    → internal/protocol.BuildMessage
                                → WebSocket write
```

Here `feed` is the public handler/lifecycle boundary, `market` owns quotes and retained history, `protocol` owns packet storage and framing, and `websocket` owns subscriptions and delivery. The market holds its history lock while copying bytes into connection-owned storage. Reuse this pattern of clear ownership, not these exact package names.
