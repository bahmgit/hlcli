# Engineering guide

Read `docs/architecture.md`, `docs/operations.md`, `docs/commands.md`, `docs/validation.md`,
and the modules owning the affected behavior before editing.

- Preserve the daemon-first architecture: only `hld` owns exchange actions and signing.
- Keep normal submit-to-ack free of hidden REST, sleeps, refresh, parsing, and UI/cache work.
- Journal pending reconciliation context before sending an action; ambiguous delivery halts execution.
- Keep one signing daemon per API-wallet key and one global execution queue across clients.
- Validate external data and trading invariants. Fail clearly rather than inventing state or fallbacks.
- Preserve contracts unless the task explicitly changes them. Update documentation with behavior.
- Keep secrets, operator configuration, private evidence, host paths, and trading records out of Git.
- Make focused changes; avoid speculative defenses, duplicate mechanisms, and unrelated refactoring.
- Tests must own distinct observable contracts or reproduced regressions. Prefer the lowest useful
  boundary; do not optimize for test count or coverage percentage.

Run the deterministic gate in `docs/validation.md` for source changes. Live trading requires
explicit authorization and a fixed exposure budget; code work does not imply trading permission.
