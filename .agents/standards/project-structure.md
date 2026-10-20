# Go project and package boundaries

Apply when creating services, moving packages, selecting module boundaries, or changing build commands.

- Start with one Go module unless independent build, dependency, or release boundaries justify more. Preserve an established project's agreed module structure.
- Keep configuration, wiring, signals, and cleanup under cmd/<service>. Put application behavior and adapters outside command packages, normally under internal/.
- Name packages for actual responsibilities. Separate transport, application behavior, and persistence where those boundaries clarify ownership and dependencies; do not create empty layers or a package for every type.
- Prefer concrete dependencies. Introduce small consumer-owned interfaces when they express useful boundaries or enable meaningful behavioral tests.
- Expose public packages only for concrete external consumers. Share contracts deliberately and avoid importing another module's private implementation.
- A go.work file is optional development tooling. Projects must build independently of an unrelated parent workspace; verify modules with GOWORK=off where workspace isolation matters.
- Keep tests and benchmarks beside the implementation they exercise. Aggregate checks in multi-module repositories must include every module explicitly.
- Keep README instructions, API contracts, and code-navigation documentation with the owning project. Add documentation when it helps explain real behavior rather than copying a layout template mechanically.
- Keep dependency directions clear and avoid cycles. Extract shared libraries only when reuse is established, not anticipated.
