# Benchmarkable data journeys

Apply when designing or refactoring streaming stages, changing hot paths, or adding benchmarks.

- Structure streaming code around meaningful stages of the data journey. Give each stage one clear responsibility and explicit inputs, outputs, and buffer-ownership contracts. Name functions after the work they perform, such as `generateUpdate` and `buildMessage`.
- Keep coordination concerns—history, locking, pacing, cancellation, and lifecycle—with the coordinator. Make dependencies such as timestamps explicit when that improves correctness and repeatable testing.
- Use caller-owned reusable storage where appropriate. Document when a stage may mutate, retain, or reuse that storage; preserve ownership and completion guarantees across stage boundaries.
- Benchmark stages independently, then measure the connected flow. State whether setup, synchronization, allocation, and network I/O are included; component results do not establish end-to-end throughput.
- Benchmarks must call the same implementation used in production rather than duplicate its logic. Extract meaningful responsibilities when needed to make that possible.
- Add boundaries where responsibilities differ. Do not create extra interfaces, wrappers, or tiny functions solely to make benchmarks possible.
