# Architecture

`hld` owns exchange connectivity, signing, account state, readiness/risk, and durable execution.
`hl` owns terminal interaction and sends typed local IPC requests. All clients share one global
execution order; market-scoped sessions retain independent market selection and variables.

## Modules

| Modules | Responsibility |
| --- | --- |
| `command`, `catalog` | Typed grammar, aliases, help |
| `runtime`, `operator`, `scope` | Queue, confirmations, shell state, market sessions |
| `planner` | State validation, sizing, precision, intent-to-action planning |
| `managed` | Chase, trailing, dependent protection, persisted work |
| `core`, `execution` | Serialized execution, journal, acknowledgements, reconciliation |
| `protocol`, `exchange` | Wire types, signatures, action WebSocket transport |
| `info`, `feed`, `state` | Metadata, subscriptions, snapshots, freshness, readiness |
| `security`, `config` | Credential encryption, profiles, configuration |
| `ipc`, `server`, `metrics` | Local control, read-only HTTP observation, counters |

## Execution

Parse a typed command, validate against the relevant state, and enter the daemon's global execution
queue. Persist a pending journal row containing reconciliation context, sign with the signer-owned
nonce, and post over the action WebSocket. Classify the response as accepted, rejected, or ambiguous;
append its terminal outcome and repair the typed read model from the receipt.

Normal submit-to-ack performs no hidden REST, sleep, refresh, parsing, or UI/cache work. Info refresh
and reconciliation are explicit or background work. An outcome that may have reached the exchange
but cannot be proved halts execution rather than being retried blindly. Restart uses authoritative
exchange queries and context-bearing journal records; legacy context-free records remain audit-only.

## State and managed work

One `TradingState` combines subscriptions and authoritative snapshots with timestamps and explicit
known/fresh flags. Trading validates relevant market, account, order, book, capacity, and position
state. Spot inventory is represented by balances, never perp positions. Full-DEX reconciliation
updates every relevant configured/watched market atomically. Application ping/pong bounds feed
liveness even when books are quiet; a missed pong retires the connection.

Managed chase, trailing, and attached protection persist their intent and exact order identities.
Only fills attributable to owned entry orders can arm or resize protection. Operator cancellation
and managed transitions share a lock. Restart reconstructs managed work from persistence and exchange
state. Closing a client does not stop retained commands or persisted managed work.

## Trust boundaries

- Client/UI code never signs or executes an exchange action directly.
- One API-wallet key belongs to one signing daemon; more clients do not create more signers.
- Main-user-signed actions are rejected where the API-wallet signer cannot perform them.
- IPC can submit trades and requires OS access control. HTTP is observational but exposes account data.
- Credential encryption does not protect a compromised running process or exposed password.
- Journals, profiles, configurations, and logs are sensitive operational records.
- Exchange behavior is an external dependency and requires recertification when it changes.
