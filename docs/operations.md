# Operations

## Processes and data

Run exactly one signing `hld` per API-wallet key and any number of `hl` clients against it.
The default data directory is `~/.hyperliquid-cli`; override with `--data-dir` or `HL_V2_DATA_DIR`.
Keep it private, mode `0700`, outside the source tree. Use separate directories for testnet/mainnet.

| Path within data directory | Contents |
| --- | --- |
| `hl-v2.json` | Operator configuration; may include token and vault address |
| `wallet_profiles.json` | Profile index and account metadata |
| `wallets/<profile>/credentials.bin`, `salt.bin` | Encrypted credentials and Argon2 salt |
| `keybinds.json` | Persisted daemon-wide bindings |
| `runtime/backend.sock` | Default local control socket |
| `runtime/execution_journal_v2.jsonl` | Durable action and recovery evidence |
| `runtime/managed_state_v2.json` | Persisted chase/protection state |

Back up sensitive data only through an encrypted channel. Existing profile directories are reused;
starting a release binary against one is not an isolated test.

## Configuration

`hld` and `hl` read `<data-dir>/hl-v2.json`. Malformed JSON and invalid recognized values fail
loading. Unknown keys are currently ignored: use the exact field names below and inspect `hl config`.

| JSON field | Meaning | Default |
| --- | --- | --- |
| `network` | `Mainnet` or `Testnet` | `Mainnet` |
| `defaultSymbol` | Initial canonical market | `BTC` |
| `allowedSymbols` | Canonical market allowlist; empty permits all discovered markets | empty |
| `symbolAliases` | Alias-to-canonical map; requires nonempty allowlist and allowed targets | empty |
| `bind` | HTTP observation address | `127.0.0.1:8088` |
| `allowRemote` | Allow non-loopback HTTP bind | `false` |
| `backendToken` | Token for protected HTTP routes | unset |
| `vaultAddress` | Authorized vault/subaccount to trade for | unset |
| `requiredAccountMode` | `standard`, `unifiedAccount`, `portfolioMargin`, or `any` | `any` |
| `marketCrossBps` | IOC crossing allowance, basis points, `0`–`10000` | `50` |

The shipped example allows only BTC on testnet; it is not loaded unless copied to the data directory.
Without a file or overrides, startup uses mainnet with an unrestricted market universe.
With a nonempty allowlist, discovery queries builder metadata only for DEX prefixes named in it.
An empty allowlist discovers every DEX.
Aliases are canonicalized to uppercase and never become exchange symbols. Use `hl markets`
to discover exact canonical native, builder (`BUILDER:ASSET`), and spot (`SPOT:BASE/QUOTE`) symbols.

Environment overrides: `HL_V2_NETWORK`, `HL_V2_DEFAULT_SYMBOL`, `HL_V2_ALLOWED_SYMBOLS`
(comma-separated), `HL_V2_BIND`, `HL_V2_ALLOW_REMOTE`, `HL_V2_BACKEND_TOKEN`, `HL_V2_VAULT_ADDRESS`,
`HL_V2_REQUIRED_ACCOUNT_MODE`, and `HL_V2_MARKET_CROSS_BPS`. Command-line process options apply
after file/environment loading; file configuration is validated before those options are applied.
Keep these settings consistent. `hld --help` and `hl --help` list the exact process options.

## Credentials and startup

Use an approved API wallet, never the main account private key. The main account is queried for state;
the API-wallet key signs. First signed startup creates the selected profile interactively:

```bash
target/release/hld --testnet --wallet-profile default
```

Subsequent starts prompt for its encryption password on `/dev/tty`. Services can provide an
owner-readable password file through `--password-file` or `HL_V2_PASSWORD_FILE`. Do not place
passwords in arguments or ordinary environment variables. Profile network must match daemon network.

For read-only operation, use the actual main/subaccount/vault address in place of the all-zero example:

```bash
target/release/hld --testnet --read-only --user 0x0000000000000000000000000000000000000000
```

Read-only mode requires no private key and rejects actions before signing. Check `hl doctor` for
diagnostics and `hl health` for a machine-readable check (nonzero exit when unhealthy). Wait for
readiness before trading. `Ctrl-C` stops a foreground daemon; exchange orders may remain active.
Chases and unfinished attached entries require the daemon to run and reconcile on restart. Native
trailing orders and conditional TWAPs continue on Hyperliquid while the daemon is offline. Startup
reconciles native trails across configured DEXs without a local active-trail database.

Hyperliquid nonces are signer-scoped. Do not share one API key across signing processes or reuse
deregistered API-wallet keys; see the official
[nonce/API-wallet guidance](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets).

## Multiple market terminals

```bash
target/release/hl --market BTC
target/release/hl --market ETH
./hl-grid
```

Use the same data directory or `--ipc-path`/`HL_V2_IPC_PATH` for clients and daemon. Each scoped
client has its own market and variables. Account-wide reads, confirmations, and keybindings remain
shared. `hl-grid` discovers the daemon's allowed markets, preflights a scoped session, and creates
one tiled tmux pane per market, native perps then builder perps then spot. An existing `hl-grid`
session is preserved; attach with `tmux attach -t hl-grid`.

## HTTP observation

GET routes: `/v1/health`, `/v1/capabilities`, `/v1/status`, `/v1/portfolio`, `/v1/balances`,
`/v1/metrics`, `/v1/exposure`, `/v1/snapshot`, `/v1/orders`, `/v1/position`, `/v1/markets`,
`/v1/pending`, `/v1/commands`, and `/v1/keybinds`.

`/v1/health` is unauthenticated. With `backendToken` configured, other routes require
`x-hl-v2-token: <token>`. Remote bind requires both `allowRemote` and a token. Prefer loopback or
a private authenticated tunnel; HTTP itself has no TLS. Filter markets with `?kind=all|perps|hip3|spot`
and paginate commands with `?after=<id>&limit=<n>`. HTTP does not submit commands.

## Upgrading from local trailing

Managed-state schema v3 removes local trailing monitors. Startup refuses v1/v2 state containing an
active local trail or an unfinished trailing attachment. Before upgrading, use the previous binary
to cancel/drain those entry and protection orders and verify exchange state. Do not delete the
managed file or journal to bypass this check. Safe nontrailing v2 work (chases and fixed attached
protection) is retained; v1 unfinished attachments remain unsupported because they lack safe identity.
The daemon does not automatically convert local trails: conversion would reset watermarks or open
a protection gap. Place a new native trail explicitly after draining old work.

## Recovery and troubleshooting

- Unhealthy/stale state: inspect `health`, `doctor`, connections, account mode, and readiness reasons.
- Unknown market: check `markets`, the configured allowlist, and alias targets.
- IPC errors: check that client and daemon use the same data directory/socket.
- Credential network mismatch: select the matching network/profile; do not edit encrypted files.
- Ambiguous delivery or execution halt: stop submitting, preserve the journal, and independently
  reconcile positions, orders, and TWAPs against Hyperliquid. Do not delete recovery state or start
  another signer to bypass the halt.
- Suspected key exposure: stop the daemon, revoke the API wallet through a trusted interface,
  and provision a replacement. See `SECURITY.md`.
