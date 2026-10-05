# Validation

## Deterministic gate

Run for every source change:

```bash
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo build --locked --release --bins
git diff --check
```

The GitHub Actions `CI` workflow runs this gate on Ubuntu for pushes to main, pull requests, and
manual dispatch. It uses the pinned Rust toolchain and read-only checkout without persisted
credentials. Tests require no exchange account, running trading backend, or secrets. CI checks
compilation, behavior at mocked exchange boundaries, local IPC/PTY, and release builds; it cannot
prove current exchange acceptance or profitability.

## Test policy

Each test must own a distinct observable contract or reproduced regression. Prefer table-driven
cases and the lowest useful boundary. Use real serialization, journal, IPC, and binary interfaces
where those interfaces are the contract. Fake exchange/time boundaries when needed, inspect exact
submitted actions, and prove rejection side effects where relevant. Remove redundant tests that
mirror implementation or incidental output. Test count and coverage percentages are not targets.

The suite covers typed grammar and aliases, planner action classes and invariants, journal-before-post,
ambiguous acknowledgements, dependent protection, cancellation races, restart identity, account-source
parity, HTTP authorization, encrypted credentials, real client/IPC/PTY behavior, and feed liveness.

## Dependencies

Before release, run `cargo machete`, `cargo audit`, and `cargo tree -e features --target all`.
Review every finding against the compiled feature graph. Lockfile-only optional dependencies can
appear in audits without entering the binary; demonstrate absence rather than assuming it.
Do not suppress a vulnerability without documenting its applicability and dependency path.

The current lockfile reports `RUSTSEC-2026-0235` for optional `rkyv 0.7.46` through `rust_decimal`.
`cargo tree --locked -e features --target all -i rkyv` prints no dependency path: that package is
not compiled with this manifest. The existing compiled `rustls 0.23.40` also reports `RUSTSEC-2026-0285`: TLS 1.3 handshake
messages can be accepted across encryption-level boundaries. It enters both HTTPS and WebSocket
paths through `reqwest` and `tokio-tungstenite`; upstream identifies `0.23.45` as patched.
This feature change leaves dependencies unchanged; the advisory remains an outstanding release
limitation. The lockfile also reports unmaintained `derivative`, `paste`, and
`proc-macro-error2`, plus yanked `bitcoin-io`/`bitcoin_hashes`. Of these, `paste` and
`proc-macro-error2` participate in the current Alloy build; the others are optional and absent
from the compiled graph. Retain the tested dependency set for this release and reassess upstream
maintenance before updates. The unfiltered audit remains visible and exits nonzero; CI does not
claim a clean vulnerability audit.

## Bounded live acceptance

Live trading requires explicit authorization, an isolated low-value account/API wallet, and a
fixed exposure budget. Keep operational evidence outside the public repository.

1. Pass the deterministic gate on the exact release tree and build its binaries.
2. Start read-only; exercise discovery, read commands, HTTP authorization, aliases, and action rejection.
3. Record independent exchange state, then start one signing daemon on testnet or the authorized account.
4. Exercise the smallest valid order through placement, acknowledgement, modify, cancel, fill, close,
   and state convergence. Cover native perp, builder perp, and spot only when permitted/supported.
5. Cover batch, scale, protection, chase, leverage, isolated margin, and applicable account-mode
   guards. For native trails, verify activation and mark-price watermark movement, offline continuity,
   external-interface adoption, separate attached partial-fill tranches, retained surviving exits when
   one tranche ends, cancel-family isolation,
   and amendment rejection. For TWAPs, verify waiting-to-running activation, max/min termination,
   and cancellation in both states; cover spot only when permitted/supported. Confirm fast cancellation
   is accepted for eligible plain batches and omitted for mixed/protective batches.
6. Exercise simultaneous scoped clients and `hl-grid`; prove independent scopes and one journal order.
7. Restart with controlled passive/managed work; prove reconciliation or an explicit halt.
8. Soak health, feeds, state, orders, TWAPs, and resource use while clients remain active.
9. End with two independent reconciliations agreeing on intended positions, orders, TWAPs, balances,
   leverage, and account mode. Resolve every pending journal row.

Do not broaden permission or exposure to make a scenario pass. Record skipped or unproven boundaries.
Never publish real wallet addresses, credentials, balances, fills, journals, host paths, or private logs.

## Exchange contract references

Contracts were checked against primary sources on 2026-10-05:

- [Exchange endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint):
  action encoding, TWAPs, cancellation, and omission of `f` when false.
- [Trailing stops](https://hyperliquid.gitbook.io/hyperliquid-docs/support/faq/trailing-stops) and
  [order types](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/order-types): mark-price trailing
  behavior, activation, and conditional TWAP semantics.
- [WebSocket subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions):
  order/TWAP state and spot asset context feeds.
- The official [Hyperliquid frontend](https://app.hyperliquid.xyz/) corroborates signed field order
  and supplies conditional TWAP encoding and native order-state parsing details not covered by the
  public API reference. Its inspected config bundle SHA-256 was
  `190151d92ace708da81dd6262443d479040d0b9f8c81f47a8c2d0aad65ab103a`.

The exchange endpoint reference says fast cancel currently has no additional effect; the
[latency guide](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/optimizing-latency)
recommends it. The implementation requests it for eligible batches but makes no measured latency
claim. Deterministic fixtures prove serialization and local lifecycle contracts; exchange acceptance
and live execution semantics still require the authorized bounded tests above.
