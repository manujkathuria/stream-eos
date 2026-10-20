# Meaningful verification

Apply when implementing behavior, fixing bugs, or reviewing tests.

- Define observable acceptance criteria. For bugs reproduce the failing behavior before fixing it where practical.
- Test contracts and failure modes rather than mirroring implementation. Use independent expected values and focused fixtures; coverage percentage is not the objective.
- Use unit tests for isolated logic and real integration tests for database constraints, transactions, protocol behavior, and external-system semantics. Mocks cannot prove durability or transaction guarantees.
- Select relevant cases: validation boundaries, duplicates and retries, conflicting concurrent operations, rollback, cancellation, and partial failure. Exercise multiple instances when correctness depends on shared state.
- Synchronize tests through observable conditions rather than arbitrary sleeps. Bound waits and clean up owned goroutines, connections, and temporary resources.
- Use race detection for concurrent and ownership-sensitive behavior, plus required project checks. Use fuzzing where parsers or input boundaries benefit.
- Run focused tests first. Broaden or repeat checks when the change, failures, or unresolved risks warrant it. Documentation-only changes need documentation checks, not unrelated performance or integration runs.
- Report commands, outcomes, and unverified boundaries honestly. Never weaken assertions to pass a test or claim guarantees beyond the evidence.
