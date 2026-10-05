# Commands

The daemon owns one typed command grammar for shells, one-shot calls, market sessions, bindings,
and chains. `help all` is the exhaustive version-matched catalogue; use `help <command>` for details.
Examples are syntax illustrations, not order recommendations.

## Client forms

```bash
hl                           # interactive shell
hl status                    # one-shot command
hl exec buy '20$' at 40000    # explicit one-shot form
hl --market BTC              # independent market-scoped shell
hl --market BTC orders
hl health                    # JSON health, nonzero exit if unhealthy
hl metrics
hl command get 42            # retained command result
hl press k                   # execute a persisted single-character binding
```

In an external shell, quote tokens containing `$` and entire chains to prevent shell expansion.
Inside the interactive `hl` shell, use them literally. Without `--market`, `orders`/`position`
are account-wide and `instrument` changes the default market. With it, reads and trading use that
session's market. Portfolio and balances remain account-wide. Spot inventory appears in balances.

## Families

| Family | Commands/aliases | Purpose |
| --- | --- | --- |
| Read | `status`/`st`/`refresh`/`r` | Refresh active state and show readiness, orders, TWAPs, position |
| Read | `portfolio`/`pf`, `balance(s)`/`bal`, `assets`, `borrow`, `lend` | Account, inventory, borrow/lend state |
| Read | `orders`/`ord`/`o`, `position`/`pos`/`p` | Open orders and perp positions |
| Read | `risk`, `config`, `doctor`/`preflight`/`setup check` | Inspect policy and diagnose readiness |
| Markets | `markets`/`market`, `instrument`/`inst`/`instr`/`coin` | Discover/select markets |
| Trade | `buy`/`b`, `sell`/`s`, `scale`, `batch`, `close`, `twap` | Submit entries/exits and exchange TWAPs |
| Manage | `cancel`/`c`, `move`, `resize`, `tp`, `sl`, `trail`/`tsl`, `chase` | Orders and protection |
| Account | `leverage`/`lev`, `margin`, `account mode` | Perp margin/leverage and account mode |
| Shell | `help`/`h`, `set`, `print`, `unset`, `keybinds`/`binds`, `bind`, `unbind` | Help and automation |
| Shell | `clear`/`cls`, `quit`/`exit`/`q` | Local terminal controls |

Use `markets all`, `markets perps`, `markets hip3`, or `markets spot`. `config get <key>` inspects
configuration; `risk` and `config` do not mutate it. `borrow`/`lend` are read views.

## Size and price

Sizes: `0.01` base units, `100$` quote notional, `5%` percentage according to command/account context,
`r100$` or `r0.5%` risk sizing with an attached stop loss, and `@name` variables.
Prices: `40000` absolute, `-100`/`+100` offsets, `-0.5%`/`+0.5%` offsets, or `$entry` when known.
Signed relative levels resolve from a fresh midpoint; bare percentage prices are rejected.
Wire precision and exchange minimums apply after rounding. Risk sizing does not guarantee realized
losses: slippage, gaps, fees, and execution failure can exceed the planned amount.

Spot sells and `close` use available base inventory; fee/lot rounding may leave unsellable dust.
Perp positions support reduce-only, risk sizing, leverage, margin, and position protection; these
features are rejected on spot. `close` is IOC: reduce-only on perps, a normal sell on spot.

## Trading and management

```text
buy 0.01
sell 100$ at 50500 reduce
buy 0.01 at 50000 tif alo post
buy r100$ sl -0.5% tp +1%
scale buy 0.05 into 5 from -0.1% to -1%
scale sell 0.1 reduce into 10 from 42000 to 44000
batch buy 0.01@40000 0.02@39500 tif gtc
batch move 101,102 to 40500
batch resize 101,102 to 0.005
close
close 50% at +0.2%
twap buy 0.1 over 30
twap buy 0.1 over 30 trigger above 50000 max 51000
twap sell 0.1 over 30 trigger below 50000 min 49000
twap cancel all

cancel
cancel 101,102
cancel cloid 0x00000000000000000000000000000001
move 101 to -0.2%
resize 101 to 0.005
tp +1%
sl -0.5%
tp cancel
sl cancel
trail 150
trail set 0.4%
trail 1% size 50% activate 52000
trail cancel
chase buy 0.01 quote
chase sell 100$ 0.2% tif alo post
chase cancel
```

Plain `cancel` skips TP/SL/trailing protection; cancel those explicitly. Eligible ordinary limit
cancellation batches automatically request fast cancel. Mixed batches remain one ordinary action.
Cancel an existing chase before replacing it. Chase cancellation disarms only protection attached
to its own entry; independent protected entries retain their protection intent. Attached protection starts from a fresh flat position
and follows owned entry fills; it requires the daemon to remain running until the entry finishes.
Local validation rejects an invalid batch before submission, but exchange responses can contain
per-item failures: always inspect them.

`trail`/`tsl` places a native reduce-only position trailing stop. Native trailing requires at least
10 USDC of notional after rounding, including full-position orders. An absolute retracement is a price
distance; a percentage must be greater than zero and below 100, with at most four decimal places.
Optional `activate <price>` defers tracking until Hyperliquid activates the order. Long trails track
the highest mark; short trails track the lowest mark. `orders` displays activation, retracement,
and the exchange watermark/current derived trigger, or a waiting state. Quote-denominated sizing
uses the fresh mark price; percentage sizing uses the current position. There is no local fallback.
Generic move/resize is rejected: cancel and place a new trail explicitly, which resets its watermark.
`sl cancel` targets fixed stops; `trail cancel` targets native trails and disarms future attached
trailing increments. Existing native trails continue while the daemon is offline.

Attached trailing creates a separate native order for each newly protected owned fill increment;
each has its own watermark. Earlier trails are never resized or recreated. If one tranche ends while an entry is still pending,
the daemon cancels the remaining entry, retains surviving exits, and requests position reconciliation.
It does not infer a fill from an order disappearing. Unavailable, stale, or conflicting position state
also stops an unfinished trailing entry and retains its existing exits. If a later increment is
below the exchange minimum or protection fails, existing protection remains, the remaining entry
is canceled, and the daemon reports degraded protection. An unfilled owned entry missing from the order snapshot also triggers cancellation and explicit
review rather than silently discarding its trailing attachment. Fills can still arrive while cancellation
is in flight; review the resulting filled position explicitly.

TWAP duration is 5..10080 minutes and minimum notional is 100 USDC after precision rounding.
`trigger above|below <price>` is optional and its direction is independent of buy/sell. A buy TWAP
accepts optional `max <price>`; a sell TWAP accepts `min <price>`. The stop must be above/below the
trigger respectively, or above/below the fresh current mark when no trigger is specified. Prices
must be positive and satisfy market precision. Conditions also work for supported spot markets;
spot reduce-only remains invalid. Hyperliquid owns activation and stopping. Active IDs, conditions,
and waiting/running state appear in `status`; cancel with `twap cancel <id|all>`. Unconditional TWAPs
retain their original wire format. Closing a shell does not cancel exchange orders.

## Account and shell

```text
leverage cross 5
leverage iso 3
margin add 100
margin remove 25
account mode
account mode require standard
account mode require any
account mode set unified

set @size 0.01
print @size
buy @size at -0.2%
unset @size
bind k cancel
keybinds
unbind k
buy 0.01 at -0.2%; sleep 2s; orders
```

Margin changes require a fresh active isolated perp position. Account-mode changes are account-wide
and require an expiring typed confirmation; an API wallet can move standard mode to unified or
portfolio mode, but transitions requiring the main-user signer reject locally.
Mode requirements and bindings are daemon-wide; scoped variables are session-local.
Chains stop on failure or pending confirmation; limits are 32 segments, 300 seconds per sleep,
900 seconds total sleep. Quoted semicolons are not separators.
