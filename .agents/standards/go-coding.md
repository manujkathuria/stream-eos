# General engineering and Go coding

Apply when implementing, reviewing, or refactoring Go code.

## Working principles

- Correct delivery is required. Optimize performance within that requirement and support performance claims with measurements.
- Inspect relevant code and conventions before changing them. Keep work scoped, simple, and idiomatic.
- Resolve routine choices independently. Raise ambiguity that changes intended behavior, correctness guarantees, or public contracts, with a recommendation.
- Define acceptance criteria for implementation tasks and verify meaningful behavior. Report what was checked and any remaining limitations.
- Preserve user changes. Distinguish proposals from confirmed decisions and planned work from completed work.

## Go coding standards

- Keep package responsibilities clear. Command packages handle configuration, dependency wiring, process signals, and cleanup; internal packages own protocol logic, orchestration, and external adapters.
- Encapsulate mutable state and resource handling in concrete structs with methods. Keep implementation fields private unless callers need them. Use ordinary functions for stateless transformations such as binary codecs.
- Assemble behavior through composition and explicit constructor dependencies. Avoid inheritance-style hierarchies, global service dependencies, and layers added only to resemble an enterprise architecture.
- Prefer concrete types by default. Define small interfaces in the consuming package when they express a useful dependency boundary or enable meaningful behavioral tests. Do not create an interface for every struct or mirror an entire third-party client API.
- Keep external library types inside adapters where practical. Orchestration should depend on application contracts rather than WebSocket or Kafka client details.
- Document dependency contracts: buffer ownership and lifetime, acknowledgement/completion semantics, cancellation, ordering, partial success, and concurrency safety where relevant. An interface alone does not establish these guarantees.
- Give each connection or resource a clear lifecycle owner. Make cancellation unblock I/O, keep shutdown bounded, and ensure buffers are not reused while consumers still reference them.
- Validate configuration and protocol input at boundaries. Return errors with useful context while preserving error identity; let the command layer decide process exit behavior. Keep logging contextual and avoid per-event hot-path logging.
- Keep abstractions proportional to current requirements. Introduce additional patterns or concurrency only when behavior, tests, or measurements justify them. Apply the focused skills below when their triggers match the work.
