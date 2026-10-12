# Engineering standards — index

Read this index before implementation, review, or refactoring. Apply the [pair-programming agreement](pair-programming.md) and general coding rules, then load the focused documents whose triggers match the work. Reuse guidance already read when unchanged; do not load every standard for every task.

| Standard | When to read |
| --- | --- |
| [1. Service foundations](01-service-foundations.md) | Creating a service or changing config, `.env`, logging, context, startup, or graceful shutdown. |
| [2. Benchmarkable data journeys](02-benchmarkable-code.md) | Designing streaming stages, refactoring hot paths, or writing benchmarks. |
| [General engineering and Go coding](go-coding.md) | Any Go implementation, review, or refactoring. |
| [Project and module structure](project-structure.md) | Creating or moving projects/packages, splitting modules, or changing workspace builds/tests. |

Standards define the intended code structure and contracts. The skills listed in `AGENTS.md` provide task-specific workflows and verification guidance. Project decisions and guarantee boundaries remain in `AGENTS.md` and the architecture reference. User instructions take precedence.
