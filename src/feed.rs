use std::{collections::BTreeSet, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    sync::{RwLock, watch},
    time::Instant,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    exchange::Network,
    info::{
        apply_account, apply_open_orders_for_symbols, apply_positions_for_watched,
        apply_spot_balances, apply_spot_mids, canonical_symbol_for_coin,
        canonical_symbol_for_coin_in_dex, parse_active_asset_data,
    },
    metrics::Metrics,
    protocol::MarketKind,
    state::{Book, Fill, Position, TradingState, Twap},
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

pub fn spawn_state_feed(
    network: Network,
    user: Option<String>,
    state: Arc<RwLock<TradingState>>,
    metrics: Metrics,
    mut markets: watch::Receiver<BTreeSet<String>>,
) {
    metrics.start_state_feed();
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(250);
        loop {
            match run_feed_once(
                network,
                user.as_deref(),
                state.clone(),
                &metrics,
                &mut markets,
            )
            .await
            {
                Ok(()) => {
                    metrics.state_feed_down();
                    backoff = Duration::from_millis(250);
                }
                Err(err) => {
                    metrics.state_ws_disconnected();
                    eprintln!("state feed disconnected: {err:#}");
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
    });
}

async fn run_feed_once(
    network: Network,
    user: Option<&str>,
    state: Arc<RwLock<TradingState>>,
    metrics: &Metrics,
    markets: &mut watch::Receiver<BTreeSet<String>>,
) -> anyhow::Result<()> {
    let (dexes, portfolio_margin) = {
        let state = state.read().await;
        (
            state
                .markets
                .values()
                .filter_map(|market| market.dex.clone())
                .collect::<BTreeSet<_>>(),
            state
                .account_mode
                .is_some_and(|mode| mode.value == crate::state::AccountMode::PortfolioMargin),
        )
    };
    let (mut ws, _) = connect_async(network.ws_url()).await?;
    let mut watched = markets.borrow().clone();
    let initial_subscriptions = {
        let snapshot = state.read().await;
        market_subscriptions(&snapshot, &watched, user)?
    };
    for subscription in initial_subscriptions {
        subscribe(&mut ws, subscription).await?;
    }
    subscribe(&mut ws, json!({ "type": "allMids" })).await?;
    if let Some(user) = user {
        let spot_subscription = if portfolio_margin {
            json!({ "type": "spotState", "user": user, "isPortfolioMargin": true })
        } else {
            json!({ "type": "spotState", "user": user })
        };
        subscribe(&mut ws, spot_subscription).await?;
        subscribe(
            &mut ws,
            json!({ "type": "allDexsClearinghouseState", "user": user }),
        )
        .await?;
        subscribe(&mut ws, user_subscription("clearinghouseState", user, None)).await?;
        subscribe(&mut ws, user_subscription("openOrders", user, None)).await?;
        subscribe(&mut ws, user_subscription("twapStates", user, None)).await?;
        for dex in dexes {
            subscribe(&mut ws, user_subscription("openOrders", user, Some(&dex))).await?;
            subscribe(&mut ws, user_subscription("twapStates", user, Some(&dex))).await?;
        }
        subscribe(&mut ws, json!({ "type": "userFills", "user": user })).await?;
    }
    metrics.state_ws_connected();
    let mut mode_check = tokio::time::interval(Duration::from_millis(250));
    let mut heartbeat =
        tokio::time::interval_at(Instant::now() + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_pong = Instant::now();
    loop {
        tokio::select! {
            message = ws.next() => {
                let Some(message) = message else {
                    anyhow::bail!("websocket stream ended");
                };
                match message? {
                    Message::Text(text) => {
                        if application_pong(&text)? {
                            last_pong = Instant::now();
                            confirm_watched_books(&state, &watched, now_ms()).await;
                        } else {
                            apply_ws_message_for(&state, &text, user, &watched).await?;
                        }
                    }
                    Message::Ping(bytes) => ws.send(Message::Pong(bytes)).await?,
                    Message::Close(frame) => anyhow::bail!("websocket close: {frame:?}"),
                    _ => {}
                }
            }
            changed = markets.changed() => {
                changed.map_err(|_| anyhow::anyhow!("market interest registry closed"))?;
                let next = markets.borrow().clone();
                let removed = watched.difference(&next).cloned().collect::<BTreeSet<_>>();
                let added = next.difference(&watched).cloned().collect::<BTreeSet<_>>();
                let (removed_subscriptions, added_subscriptions) = {
                    let snapshot = state.read().await;
                    (
                        market_subscriptions(&snapshot, &removed, user)?,
                        market_subscriptions(&snapshot, &added, user)?,
                    )
                };
                for subscription in removed_subscriptions {
                    unsubscribe(&mut ws, subscription).await?;
                }
                for subscription in added_subscriptions {
                    subscribe(&mut ws, subscription).await?;
                }
                watched = next;
            }
            _ = mode_check.tick() => {
                let state = state.read().await;
                let current_portfolio_margin = state.account_mode.is_some_and(|mode| {
                    mode.value == crate::state::AccountMode::PortfolioMargin
                });
                if current_portfolio_margin != portfolio_margin {
                    break;
                }
            }
            _ = heartbeat.tick() => {
                anyhow::ensure!(
                    last_pong.elapsed() <= HEARTBEAT_TIMEOUT,
                    "state feed heartbeat timed out"
                );
                ws.send(Message::Text(r#"{"method":"ping"}"#.into())).await?;
            }
        }
    }
    Ok(())
}

fn market_subscriptions(
    state: &TradingState,
    symbols: &BTreeSet<String>,
    user: Option<&str>,
) -> anyhow::Result<Vec<Value>> {
    let mut subscriptions = Vec::with_capacity(symbols.len() * 2);
    for symbol in symbols {
        let market = state
            .markets
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("unknown watched market {symbol}"))?;
        subscriptions.push(json!({ "type": "l2Book", "coin": market.wire_symbol }));
        if market.kind != MarketKind::Spot
            && let Some(user) = user
        {
            subscriptions.push(
                json!({ "type": "activeAssetData", "user": user, "coin": market.wire_symbol }),
            );
        }
    }
    Ok(subscriptions)
}

fn user_subscription(kind: &str, user: &str, dex: Option<&str>) -> Value {
    let mut value = json!({ "type": kind, "user": user });
    if let Some(dex) = dex.filter(|dex| !dex.is_empty()) {
        value["dex"] = Value::String(dex.to_string());
    }
    value
}

async fn subscribe(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    subscription: Value,
) -> anyhow::Result<()> {
    send_subscription(ws, "subscribe", subscription).await
}

async fn unsubscribe(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    subscription: Value,
) -> anyhow::Result<()> {
    send_subscription(ws, "unsubscribe", subscription).await
}

async fn send_subscription(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    method: &str,
    subscription: Value,
) -> anyhow::Result<()> {
    ws.send(Message::Text(
        serde_json::to_string(&json!({
            "method": method,
            "subscription": subscription
        }))?
        .into(),
    ))
    .await?;
    Ok(())
}

async fn apply_ws_message_for(
    state: &Arc<RwLock<TradingState>>,
    text: &str,
    expected_user: Option<&str>,
    watched: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let value = serde_json::from_str::<Value>(text)?;
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !matches!(
        channel,
        "bbo"
            | "l2Book"
            | "clearinghouseState"
            | "allDexsClearinghouseState"
            | "spotState"
            | "allMids"
            | "openOrders"
            | "activeAssetData"
            | "twapStates"
            | "userFills"
    ) {
        return Ok(());
    }
    let data = value
        .get("data")
        .ok_or_else(|| anyhow::anyhow!("{channel} WebSocket message missing data"))?;
    let now = now_ms();
    match channel {
        "bbo" => apply_bbo(state, data, now).await?,
        "l2Book" => apply_l2_book(state, data, now).await?,
        "clearinghouseState" => {
            let envelope = ws_user_envelope(data, "clearinghouseState", expected_user)?;
            let dex = ws_dex(envelope, "clearinghouseState")?;
            let snapshot = ws_user_payload(envelope, "clearinghouseState")?;
            let mut state = state.write().await;
            let mut candidate = state.clone();
            if dex.trim().is_empty() {
                apply_account(&mut candidate, snapshot, now, now)?;
            }
            apply_positions_for_watched(&mut candidate, snapshot, now, now, Some(dex), watched)?;
            *state = candidate;
        }
        "allDexsClearinghouseState" => {
            ws_user_envelope(data, "allDexsClearinghouseState", expected_user)?;
            apply_all_dexs_clearinghouse_state(state, data, now, watched).await?;
        }
        "spotState" => {
            let envelope = ws_user_envelope(data, "spotState", expected_user)?;
            let snapshot = ws_user_payload(envelope, "spotState")?;
            let mut state = state.write().await;
            let mut candidate = state.clone();
            apply_spot_balances(&mut candidate, snapshot, None, now, now)?;
            *state = candidate;
        }
        "allMids" => {
            let mut state = state.write().await;
            apply_spot_mids(&mut state, data)?;
        }
        "openOrders" => {
            let envelope = ws_user_envelope(data, "openOrders", expected_user)?;
            let dex = ws_dex(envelope, "openOrders")?;
            let mut state = state.write().await;
            let symbols = watched.iter().map(String::as_str).collect::<Vec<_>>();
            apply_open_orders_for_symbols(&mut state, data, now, now, Some(dex), &symbols)?;
        }
        "activeAssetData" => {
            ws_user_envelope(data, "activeAssetData", expected_user)?;
            let coin = data
                .get("coin")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|coin| !coin.is_empty())
                .ok_or_else(|| anyhow::anyhow!("activeAssetData missing coin"))?;
            let parsed = parse_active_asset_data(data)?;
            let mut state = state.write().await;
            let symbol = canonical_symbol_for_coin(&state, coin);
            anyhow::ensure!(
                state.markets.contains_key(&symbol),
                "activeAssetData references unknown market {coin}"
            );
            state.apply_active_asset(&symbol, parsed, now, Some(now));
        }
        "twapStates" => {
            ws_user_envelope(data, "twapStates", expected_user)?;
            apply_twap_states(state, data, now).await?;
        }
        "userFills" => {
            ws_user_envelope(data, "userFills", expected_user)?;
            apply_user_fills(state, data).await?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn ws_user_envelope<'a>(
    data: &'a Value,
    channel: &str,
    expected_user: Option<&str>,
) -> anyhow::Result<&'a serde_json::Map<String, Value>> {
    let envelope = data
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("{channel} WebSocket data must be an object"))?;
    let user = envelope
        .get("user")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|user| !user.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{channel} WebSocket data missing user"))?;
    let expected_user = expected_user
        .ok_or_else(|| anyhow::anyhow!("unexpected {channel} user WebSocket message"))?;
    anyhow::ensure!(
        user.eq_ignore_ascii_case(expected_user.trim()),
        "{channel} WebSocket user does not match subscription"
    );
    Ok(envelope)
}

fn ws_user_payload<'a>(
    envelope: &'a serde_json::Map<String, Value>,
    key: &str,
) -> anyhow::Result<&'a Value> {
    envelope
        .get(key)
        .filter(|payload| payload.is_object())
        .ok_or_else(|| anyhow::anyhow!("{key} WebSocket data missing {key} payload"))
}

fn ws_dex<'a>(
    envelope: &'a serde_json::Map<String, Value>,
    channel: &str,
) -> anyhow::Result<&'a str> {
    envelope
        .get("dex")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("{channel} WebSocket data missing dex"))
}

async fn apply_all_dexs_clearinghouse_state(
    state: &Arc<RwLock<TradingState>>,
    data: &Value,
    now: u64,
    watched: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let snapshots = clearinghouse_states(data)?;
    let mut state = state.write().await;
    let mut candidate = state.clone();
    for (snapshot, dex) in snapshots {
        if dex.trim().is_empty() {
            apply_account(&mut candidate, snapshot, now, now)?;
        }
        apply_positions_for_watched(&mut candidate, snapshot, now, now, Some(dex), watched)?;
    }
    let missing = watched
        .iter()
        .filter(|symbol| {
            candidate
                .markets
                .get(*symbol)
                .is_some_and(|market| market.kind != MarketKind::Spot)
                && !candidate.positions.contains_key(*symbol)
        })
        .cloned()
        .collect::<Vec<_>>();
    for symbol in missing {
        candidate.apply_position_snapshot(Position::empty(symbol), now, Some(now));
    }
    *state = candidate;
    Ok(())
}

async fn confirm_watched_books(
    state: &Arc<RwLock<TradingState>>,
    watched: &BTreeSet<String>,
    now: u64,
) {
    let mut state = state.write().await;
    for symbol in watched {
        state.confirm_book_subscription(symbol, now);
    }
}

fn application_pong(text: &str) -> anyhow::Result<bool> {
    Ok(serde_json::from_str::<Value>(text)?
        .get("channel")
        .and_then(Value::as_str)
        == Some("pong"))
}

fn clearinghouse_states(value: &Value) -> anyhow::Result<Vec<(&Value, &str)>> {
    value
        .get("clearinghouseStates")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("allDexsClearinghouseState missing clearinghouseStates"))?
        .iter()
        .map(|entry| {
            let parts = entry
                .as_array()
                .filter(|parts| parts.len() == 2)
                .ok_or_else(|| {
                    anyhow::anyhow!("allDexsClearinghouseState entry must be [dex, state]")
                })?;
            let dex = parts[0]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("allDexsClearinghouseState tuple dex invalid"))?;
            anyhow::ensure!(
                parts[1].is_object(),
                "allDexsClearinghouseState state invalid"
            );
            Ok((&parts[1], dex))
        })
        .collect()
}

async fn apply_user_fills(state: &Arc<RwLock<TradingState>>, data: &Value) -> anyhow::Result<()> {
    let fills = data
        .get("fills")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("userFills missing fills"))?;
    let snapshot = state.read().await;
    let mut parsed = Vec::with_capacity(fills.len());
    for fill in fills {
        let symbol = canonical_symbol_for_coin(
            &snapshot,
            fill.get("coin")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("fill missing coin"))?,
        );
        let time_ms = fill
            .get("time")
            .and_then(value_u64)
            .ok_or_else(|| anyhow::anyhow!("fill missing time"))?;
        let is_buy = match fill.get("side").and_then(Value::as_str) {
            Some("B") => true,
            Some("A") => false,
            _ => anyhow::bail!("fill side invalid"),
        };
        parsed.push(Fill {
            seq: 0,
            tid: fill
                .get("tid")
                .and_then(value_u64)
                .ok_or_else(|| anyhow::anyhow!("fill missing tid"))?,
            oid: fill
                .get("oid")
                .and_then(value_u64)
                .ok_or_else(|| anyhow::anyhow!("fill missing oid"))?,
            symbol,
            is_buy,
            size: decimal_field(fill, "sz")?,
            price: decimal_field(fill, "px")?,
            closed_pnl: decimal_field(fill, "closedPnl")?,
            time_ms,
        });
    }
    drop(snapshot);
    let mut state = state.write().await;
    for fill in parsed {
        state.record_fill(fill);
    }
    Ok(())
}

async fn apply_bbo(
    state: &Arc<RwLock<TradingState>>,
    data: &Value,
    now: u64,
) -> anyhow::Result<()> {
    let symbol = data
        .get("coin")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("bbo missing coin"))?
        .to_ascii_uppercase();
    let bbo = data
        .get("bbo")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("bbo missing tuple"))?;
    let bid = bbo_price(bbo.first())?;
    let ask = bbo_price(bbo.get(1))?;
    let exchange_ms = data.get("time").and_then(Value::as_u64).or(Some(now));
    let mut state = state.write().await;
    let symbol = canonical_symbol_for_coin(&state, &symbol);
    state.apply_book(&symbol, Book { bid, ask }, now, exchange_ms);
    Ok(())
}

fn bbo_price(value: Option<&Value>) -> anyhow::Result<Decimal> {
    value
        .and_then(|value| value.get("px"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("bbo missing price"))
        .and_then(|raw| raw.parse::<Decimal>().map_err(Into::into))
}

async fn apply_l2_book(
    state: &Arc<RwLock<TradingState>>,
    data: &Value,
    now: u64,
) -> anyhow::Result<()> {
    let symbol = data
        .get("coin")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("l2Book missing coin"))?
        .to_ascii_uppercase();
    let levels = data
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("l2Book missing levels"))?;
    let bid = book_level_price(levels.first(), "bid")?;
    let ask = book_level_price(levels.get(1), "ask")?;
    let exchange_ms = data.get("time").and_then(Value::as_u64).or(Some(now));
    let mut state = state.write().await;
    let symbol = canonical_symbol_for_coin(&state, &symbol);
    state.apply_book(&symbol, Book { bid, ask }, now, exchange_ms);
    Ok(())
}

fn book_level_price(side: Option<&Value>, name: &str) -> anyhow::Result<Decimal> {
    side.and_then(Value::as_array)
        .and_then(|levels| levels.first())
        .and_then(|level| level.get("px"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("l2Book missing {name}"))
        .and_then(|raw| raw.parse::<Decimal>().map_err(Into::into))
}

async fn apply_twap_states(
    state: &Arc<RwLock<TradingState>>,
    data: &Value,
    now: u64,
) -> anyhow::Result<()> {
    let dex = data
        .get("dex")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("twapStates missing dex"))?
        .trim()
        .to_string();
    let states = data
        .get("states")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("twapStates missing states"))?;
    let mut exchange_ms = None;
    let mut twaps = states
        .iter()
        .map(|entry| parse_twap_entry(entry, &dex, &mut exchange_ms))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut state = state.write().await;
    for (_, twap) in &mut twaps {
        twap.symbol = canonical_symbol_for_coin_in_dex(&state, &twap.symbol, Some(&dex));
    }
    state.replace_twaps_for_dex(&dex, twaps, now, exchange_ms.or(Some(now)));
    Ok(())
}

fn parse_twap_entry(
    entry: &Value,
    dex: &str,
    exchange_ms: &mut Option<u64>,
) -> anyhow::Result<(u64, Twap)> {
    let parts = entry
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("twap state entry must be [id, state]"))?;
    let id = parts
        .first()
        .and_then(value_u64)
        .ok_or_else(|| anyhow::anyhow!("twap state missing id"))?;
    let state = parts
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("twap state missing payload"))?;
    let submitted_ms = state.get("timestamp").and_then(value_u64);
    if let Some(ts) = submitted_ms {
        *exchange_ms = Some(exchange_ms.map_or(ts, |prev| prev.max(ts)));
    }
    Ok((
        id,
        Twap {
            symbol: state
                .get("coin")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("twap state missing coin"))?
                .trim()
                .to_ascii_uppercase(),
            dex: dex.to_string(),
            is_buy: parse_twap_side(state.get("side"))?,
            size: decimal_field(state, "sz")?,
            executed_size: decimal_field(state, "executedSz")?,
            minutes: state
                .get("minutes")
                .and_then(value_u64)
                .ok_or_else(|| anyhow::anyhow!("twap state missing minutes"))?,
            reduce_only: bool_field(state, "reduceOnly")?,
            randomize: bool_field(state, "randomize")?,
            submitted_ms,
        },
    ))
}

fn parse_twap_side(value: Option<&Value>) -> anyhow::Result<bool> {
    match value
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "buy" | "b" | "bid" => Ok(true),
        "sell" | "s" | "ask" => Ok(false),
        other => Err(anyhow::anyhow!("unknown twap side {other}")),
    }
}

fn decimal_field(value: &Value, key: &str) -> anyhow::Result<Decimal> {
    value
        .get(key)
        .and_then(value_decimal)
        .ok_or_else(|| anyhow::anyhow!("twap state missing {key}"))
}

fn bool_field(value: &Value, key: &str) -> anyhow::Result<bool> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow::anyhow!("twap state missing {key}"))
}

fn value_decimal(value: &Value) -> Option<Decimal> {
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_f64().map(|n| n.to_string()))
        .and_then(|raw| raw.parse().ok())
}

fn value_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.trim().parse().ok())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use crate::protocol::AssetId;

    #[tokio::test]
    async fn user_fill_identity_is_required_and_recorded_atomically() {
        let state = Arc::new(RwLock::new(TradingState::new("BTC")));
        let missing_oid = json!({"fills": [{
            "coin": "BTC", "side": "B", "sz": "0.01", "px": "50000",
            "closedPnl": "0", "time": 100, "tid": 1
        }]});
        assert!(apply_user_fills(&state, &missing_oid).await.is_err());
        assert!(state.read().await.recent_fills.is_empty());

        let identified = json!({"fills": [{
            "coin": "BTC", "side": "B", "sz": "0.01", "px": "50000",
            "closedPnl": "0", "time": 100, "tid": 1, "oid": 42
        }]});
        apply_user_fills(&state, &identified).await.unwrap();
        let state = state.read().await;
        assert_eq!(state.recent_fills.len(), 1);
        assert_eq!(state.recent_fills[0].oid, 42);
    }

    #[tokio::test]
    async fn application_pong_confirms_only_watched_book_subscriptions() {
        let mut state = TradingState::new("BTC");
        for (index, symbol, bid, ask) in [(0, "BTC", "100", "102"), (1, "ETH", "200", "202")] {
            state.set_market(crate::state::Market {
                symbol: symbol.to_string(),
                wire_symbol: symbol.to_string(),
                dex: None,
                asset: AssetId::native_perp(index),
                kind: MarketKind::Perp,
                size_decimals: 5,
                max_leverage: Some(40),
                delisted: false,
                open_interest_cap: false,
            });
            state.apply_book(
                symbol,
                Book {
                    bid: bid.parse().unwrap(),
                    ask: ask.parse().unwrap(),
                },
                1,
                Some(1),
            );
        }
        let watched = BTreeSet::from(["BTC".to_string()]);
        let state = Arc::new(RwLock::new(state));

        assert!(application_pong(r#"{"channel":"pong"}"#).unwrap());
        assert!(!application_pong(r#"{"channel":"allMids"}"#).unwrap());
        confirm_watched_books(&state, &watched, 10).await;
        let state = state.read().await;
        assert_eq!(state.book["BTC"].local_ms, 10);
        assert_eq!(state.book["ETH"].local_ms, 1);
    }
}
