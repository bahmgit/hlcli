# hlcli

An unofficial command-line trading client for Hyperliquid. One resident `hld` daemon owns
credentials, exchange connections, state, risk checks, signing, and journaled execution.
Thin `hl` clients share it, including independent terminals for different markets.

This software can submit real orders and is provided without warranty. Start on testnet with
a dedicated API wallet and limited funded exposure. Read the commands before using real funds.

## Requirements

- A Unix-like system with Rust installed through rustup. `rust-toolchain.toml` pins Rust 1.91.1.
- A Hyperliquid account and approved API wallet for signed operation.
- Bash and tmux for the optional `hl-grid` launcher.

## Build

```bash
cargo build --locked --release --bins
```

## Testnet quickstart

Use a separate data directory for testnet credentials and state:

```bash
export HL_V2_DATA_DIR="$HOME/.hlcli-testnet"
install -d -m 700 "$HL_V2_DATA_DIR"
cp examples/hl-v2.testnet.json "$HL_V2_DATA_DIR/hl-v2.json"
target/release/hld --testnet
```

On first signed start, the daemon asks for the main account address, API-wallet private key,
and a new encryption password. Subsequent starts decrypt the stored profile.
In a second terminal:

```bash
export HL_V2_DATA_DIR="$HOME/.hlcli-testnet"
target/release/hl doctor
target/release/hl
```

Use `help`, `help <category|command>`, or `help all` in the shell. One-shot commands use the
same grammar:

```bash
target/release/hl status
target/release/hl --market BTC orders
```

Run one signing daemon per API-wallet key. To open a tiled tmux pane for every allowed market
after the daemon is ready, run `./hl-grid` with the same `HL_V2_DATA_DIR` environment.
Pane order is native perps, builder perps, then spot.

## Documentation

- [Operations](docs/operations.md): configuration, credentials, services, recovery, troubleshooting.
- [Commands](docs/commands.md): command families, examples, sizing, market scope, automation.
- [Architecture](docs/architecture.md): module ownership, execution, persistence, trust boundaries.
- [Validation](docs/validation.md): local/CI checks and bounded live acceptance.
- [Security](SECURITY.md) and [contribution policy](CONTRIBUTING.md).

The exchange protocol can change independently. Consult the
[official API documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api)
and recertify against the target network before meaningful use.

Licensed under [Apache-2.0](LICENSE); attribution is in [NOTICE](NOTICE).
