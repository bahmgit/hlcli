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
| `managed` | Chase and unfinished dependent entry protection, persisted work |
| `core`, `execution` | Serialized execution, journal, acknowledgements, reconciliation |
| `protocol`, `exchange` | Wire types, signatures, action WebSocket transport |
| `info`, `feed`, `state` | Metadata, subscriptions, snapshots, freshness, readiness |
| `security`, `config` | Credential encryption, profiles, configuration |
| `ipc`, `server`, `metrics` | Local control, read-only HTTP observation, counters |

## Execution

Parse a typed command, validate against the relevant state, and enter the daemon's global execution
queue. Persist a pending journal row containing reconciliation context, sign with the signer-owned
nonce, and post over the action WebSocket. Classify the response as accepted, rejected, or ambiguous;
append its terminal outcome (including acknowledged identities) and repair the typed read model from the receipt.

Normal submit-to-ack performs no hidden REST, sleep, refresh, parsing, or UI/cache work. Info refresh
and reconciliation are explicit or background work. An outcome that may have reached the exchange
but cannot be proved halts execution rather than being retried blindly. Restart uses authoritative
exchange queries and context-bearing journal records; legacy context-free records remain audit-only.

## State and managed work

One `TradingState` combines subscriptions and authoritative snapshots with timestamps and explicit
known/fresh flags. Trading validates relevant market, account, order, book, capacity, and position
state. Position reconciliation uses the clearinghouse snapshot time when supplied, so an older
snapshot cannot erase a newer acknowledged fill. Spot inventory is represented by balances, never perp positions. Full-DEX reconciliation
updates every relevant configured/watched market atomically. Application ping/pong bounds feed
liveness even when books are quiet; a missed pong retires the connection.

Managed chase and unfinished attached protection persist their intent and exact order identities.
Only fills attributable to owned entry orders can arm or resize protection. Operator cancellation
and managed transitions share a lock. Fixed TP/SL protection grows with owned fills; trailing
protection adds an independent native order per new fill increment, retaining earlier watermarks.
If a native tranche ends before attachment work finishes, the daemon cancels the unfilled entry
and retains surviving exits for the remaining position. It does not infer a fill from disappearance.
If an increment cannot be protected, the daemon reports degraded protection and cancels the remaining
entry. Unresolved delivery halts execution instead of attempting cancellation or retry.

Native trails are reduce-only exchange orders driven by mark prices. Hyperliquid owns activation,
watermarks, and execution; the daemon has no local ratchet or active-trail database. Startup queries
open orders across configured DEXs, adopting native trails placed through any interface. Unfinished
attachments retain tranche identities locally. Their intent is durable before submission, and the
execution journal saves the acknowledged OID before the managed snapshot updates. Restart relinks
that exact OID; a historical acknowledgement never creates an active order. A missing acknowledgement
cannot be correlated by shape or retried safely, so execution halts. Completed attachments leave no
managed trailing record. Closing a client does not stop exchange orders or persisted managed work.

TWAP trigger and stop conditions are top-level exchange action details. Exchange state retains a
trigger while waiting and removes it when activated; the daemon displays that authoritative state.
There is no local TWAP trigger or stop monitor. Spot mark prices come from public asset-context
subscriptions; perp marks come from active-asset data. These prices have their own freshness checks,
separate from order-book midpoint prices.

Fast cancel is selected only for a whole batch of fresh known non-trigger orders without children.
Mixed, protective, stale, or unknown targets use ordinary cancel. The flag is omitted entirely when
false, including from the signed MessagePack encoding. Selection requires no synchronous REST.

## Trust boundaries

- Client/UI code never signs or executes an exchange action directly.
- One API-wallet key belongs to one signing daemon; more clients do not create more signers.
- Main-user-signed actions are rejected where the API-wallet signer cannot perform them.
- IPC can submit trades and requires OS access control. HTTP is observational but exposes account data.
- Credential encryption does not protect a compromised running process or exposed password.
- Journals, profiles, configurations, and logs are sensitive operational records.
- Exchange behavior is an external dependency and requires recertification when it changes.
