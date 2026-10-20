# Performance and measurement

Apply when designing measurable stages, investigating performance, tuning concurrency or resource limits, or making performance claims.

## Design for measurement

- Structure code around meaningful operations with explicit inputs, outputs, ownership, and failure behavior. Keep lifecycle and coordination with their owners.
- Benchmarks call the production implementation. Add boundaries when responsibilities differ, not just to manufacture benchmark wrappers.
- Measure representative operations independently, then measure the connected request or event flow. Component timings do not establish service capacity.
- Keep resource use bounded according to workload and dependencies. Choose concurrency controls where they protect capacity; do not prescribe worker pools for every handler. Account for connection-pool limits across all replicas.
- Optimize from evidence. Inspect allocations and lifetimes where relevant; bounded reusable storage is useful when its contract supports it. Do not force zero allocations or fixed arrays everywhere.

## Workloads and evidence

- Record toolchain, architecture, hardware, topology, dataset size, payloads, traffic mix, offered rate, concurrency, connection limits, and relevant settings.
- Distinguish total users, active users over a stated interval, connections, in-flight requests, and requests per second. User counts alone are not a workload.
- For load tests report offered and achieved throughput, latency percentiles, errors/timeouts/rejections, queues or dependency waits, and CPU/memory. Define latency boundaries and whether startup, warmup, and drain are included.
- For HTTP workloads exercise representative reads and writes, realistic data, contention, and bursts. For pipelines distinguish attempted, acknowledged, and committed work. Verify output correctness as well as speed.
- Check the load generator can supply the workload. Report whether tests use an open or closed load model and how that affects overload and latency observations.
- Use allocation benchmarks, CPU/heap profiles, and block/mutex profiles according to the symptom. Race-detector timings are not production performance results.
- Compare baseline and candidate under equivalent conditions, with repeated samples when needed. Report variance and limitations rather than inventing capacity guarantees.
- Preserve reliability and consistency during optimization. Do not weaken validation, durability, or delivery contracts to improve a headline number.
