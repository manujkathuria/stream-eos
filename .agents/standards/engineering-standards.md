# Engineering standards — baseline v1.1

Reusable Go engineering principles shared by our projects. Read the pair-programming agreement once per task and apply general coding rules when implementing or reviewing Go. Load focused standards by the triggers below; reuse unchanged guidance.

| Standard | Task trigger |
| --- | --- |
| [Development workflow](development-workflow.md) | Issue scoping, branches, draft PRs, review, and delivery |
| [Pair programming](pair-programming.md) | Starting a task; collaboration and review |
| [Go coding](go-coding.md) | Go implementation, review, or refactoring |
| [Service foundations](01-service-foundations.md) | Configuration, logging, context, startup, or shutdown |
| [Project structure](project-structure.md) | Package responsibilities, module boundaries, or build commands |
| [Testing](testing.md) | Behavior changes, bug fixes, or test review |
| [Performance and measurement](02-benchmarkable-code.md) | Hot paths, concurrency limits, profiling, benchmarks, or load tests |

Project scope, architecture, guarantees, library choices, and acceptance criteria belong in the project's AGENTS.md and owning documentation. Skills provide focused workflows and are loaded by task trigger; examples do not prescribe a project's architecture.

## Maintaining the baseline

Keep this baseline stable across projects. Do not rewrite it for a new repository. Discuss material changes or additions before adopting them; corrections remain possible. Apply agreed baseline updates consistently to each project's copy and record the version. Project-specific exceptions belong in AGENTS.md, not silent edits to shared standards. User instructions take precedence.
