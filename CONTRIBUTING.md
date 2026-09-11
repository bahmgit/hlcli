# Contributing

This is a maintainer-led project with no promised roadmap, support, response time, or acceptance
of outside changes. Unsolicited pull requests are generally not accepted. Exceptional, narrowly
scoped contributions may be considered after prior discussion with the maintainer.

Read `AGENTS.md` and the documentation before changing behavior. Trace changes through parsing,
planning, execution, receipt handling, state repair, and recovery. Preserve the daemon's sole
ownership of credentials, signing, and exchange actions.

Each test must prove a distinct public contract or reproduced regression. Use deterministic,
hermetic tests; fake exchange/time boundaries when needed and inspect the complete outcome.
Remove redundant tests that only mirror implementation. Run the full gate in
`docs/validation.md`, update affected docs, and inspect the complete diff for private data.

Never include real account addresses, credentials, profiles, logs, journals, positions, orders,
local host details, or private validation evidence in a contribution. Report vulnerabilities
through the private channel in `SECURITY.md`.
