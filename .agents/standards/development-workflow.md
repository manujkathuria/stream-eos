# Development workflow

Apply when planning a meaningful feature, bug fix, or engineering task and delivering its changes. Use Git and GitHub CLI (gh), or another suitable tool; this workflow does not require a custom CLI. Git tracks versions; issues and PRs are hosting-platform collaboration features. CI runs automated checks when configured.

## Define the issue

Create an issue before starting meaningful work. One issue can cover several commits. Corrections within an existing task stay under its issue. Urgent work can have the issue recorded alongside implementation; do not create duplicate issues for already tracked work.

An issue contains:

- **Problem:** current behavior or unmet need, who it affects, and the desired outcome.
- **Scope:** what the task includes and excludes. Mark suggested implementation approaches as proposals until agreed; do not prescribe every function or package.
- **Acceptance criteria:** observable outcomes that establish completion, including relevant failure behavior and verification boundaries. Avoid vague criteria such as “works correctly.”

Optional implementation notes clarify constraints without replacing the acceptance criteria. Store the work record in repository Issues; a project board is optional. Discussion and local drafts can prepare the issue without becoming final application documentation.

## Deliver the change

1. Inspect local work and the agreed base. Create a descriptive feature or fix branch from updated main without discarding existing changes. Continue an existing task branch when one already exists.
2. Implement focused increments and run relevant local checks before committing. Review and stage only task changes; preserve unrelated edits. A local commit does not trigger remote CI until pushed.
3. Push the branch when authorized. For substantial, risky, or uncertain work that benefits from early feedback, open a draft PR after a useful commit is published. Draft PRs are optional; small completed changes can open a normal PR directly. Link the issue.
4. Mark a draft ready when implementation and local checks are complete. Review the actual proposed revision. CI runs checks on published updates when configured; report local verification separately when CI is absent.
5. Address feedback and rerun relevant checks after changes. Perform service/integration verification when needed. Testing in a running environment is not automatically UAT; UAT explicitly verifies user/business acceptance.
6. Merge into main after required checks and review pass, within the authorized scope. Use a closing reference such as “Closes #3” when the PR fully resolves that issue. Closing a PR without merging does not deliver its changes.
7. Switch the local checkout to main and update it without overwriting remaining work. Verify the PR and issue states and the resulting revision.

Merge brings accepted changes into main; it does not run or deploy the application. Release tags, artifact publication, and deployment are separate actions. Merge strategy, release versions, branch protection, and CI/CD triggers remain repository decisions rather than universal requirements.

## Authorization and maintenance

Issue creation, pushing, PR creation/update, merging, and releasing have external effects. Follow the user's actual authorization and existing session scope; this standard alone does not authorize them. Avoid repeated confirmation for already authorized work. Do not bypass configured checks or protections.

Keep issues and PR descriptions aligned with the final scope and evidence. Report incomplete acceptance criteria and unresolved decisions explicitly. Add workflow automation only when recurring needs justify it.
