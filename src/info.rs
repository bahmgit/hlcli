use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::{RwLock, watch};

use crate::{
    core::{ActionReconciler, ReconcileFuture, ReconcileOutcome},
    exchange::Network,
    execution::OrderStatus,
    metrics::Metrics,
    protocol::{Action, AssetId, MarketKind, OrderTarget, TimeInForce},
    state::{
        Account, AccountMode, ActiveAssetData, BalanceSummary, Book, BorrowLend,
        LOCAL_ORDER_OID_MIN, Market, Order, OrderKind, Position, PositionDetail, SpotBalance,
        TradingState,
    },
};

#[derive(Clone)]
pub struct InfoClient {
    http: reqwest::Client,
    network: Network,
    metrics: Metrics,
}

pub struct InfoReconciler {
    client: InfoClient,
    user: String,
    state: Arc<RwLock<TradingState>>,
}

impl InfoReconciler {
    pub fn new(client: InfoClient, user: String, state: Arc<RwLock<TradingState>>) -> Self {
        Self {
            client,
            user,
            state,
        }
    }

    async fn order_status(&self, oid: Value) -> Result<AuthoritativeOrder, String> {
        let value = self
            .client
            .post(json!({ "type": "orderStatus", "user": self.user, "oid": oid }))
            .await
            .map_err(|err| err.to_string())?;
        parse_authoritative_order(&value)
    }

    async fn reconcile_orders(
        &self,
        orders: &[crate::protocol::OrderRequest],
    ) -> Result<ReconcileOutcome, String> {
        let mut authoritative = Vec::new();
        for attempt in 0..3 {
            authoritative.clear();
            for order in orders {
                authoritative.push(
                    self.order_status(Value::String(order.cloid.to_string()))
                        .await?,
                );
            }
            if authoritative
                .iter()
                .all(|status| !matches!(status, AuthoritativeOrder::Unknown))
            {
                break;
            }
            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        if authoritative
            .iter()
            .any(|status| matches!(status, AuthoritativeOrder::Unknown))
        {
            return Ok(ReconcileOutcome::Inconclusive {
                detail: "one or more order cloids remained unknown; absence is not proof the action cannot still apply"
                    .to_string(),
            });
        }
        let statuses = authoritative
            .into_iter()
            .map(authoritative_order_status)
            .collect();
        Ok(ReconcileOutcome::Applied {
            detail: "reconciled order action by cloid".to_string(),
            statuses,
        })
    }

    async fn reconcile_cancel_targets(
        &self,
        targets: Vec<Value>,
    ) -> Result<ReconcileOutcome, String> {
        let mut statuses = Vec::with_capacity(targets.len());
        let mut open = 0_usize;
        for target in targets {
            let status = self.order_status(target).await?;
            statuses.push(match status {
                AuthoritativeOrder::Order { status, .. } if status == "open" => {
                    open += 1;
                    OrderStatus::Error {
                        message: "cancel did not apply; order remains open".to_string(),
                    }
                }
                AuthoritativeOrder::Order { status, .. } => OrderStatus::AlreadyTerminal {
                    message: format!("authoritative order status is {status}"),
                },
                AuthoritativeOrder::Unknown => OrderStatus::AlreadyTerminal {
                    message: "order is absent from authoritative order status".to_string(),
                },
            });
        }
        if open == statuses.len() {
            Ok(ReconcileOutcome::NotApplied {
                detail: "all target orders remain authoritatively open".to_string(),
            })
        } else {
            Ok(ReconcileOutcome::Applied {
                detail: "reconciled cancel action by target order status".to_string(),
                statuses,
            })
        }
    }

    async fn reconcile_action(&self, action: &Action) -> Result<ReconcileOutcome, String> {
        match action {
            Action::Order(batch) => self.reconcile_orders(&batch.orders).await,
            Action::Cancel(batch) => {
                self.reconcile_cancel_targets(
                    batch
                        .cancels
                        .iter()
                        .map(|cancel| Value::from(cancel.oid))
                        .collect(),
                )
                .await
            }
            Action::CancelByCloid(batch) => {
                self.reconcile_cancel_targets(
                    batch
                        .cancels
                        .iter()
                        .map(|cancel| Value::String(cancel.cloid.to_string()))
                        .collect(),
                )
                .await
            }
            Action::BatchModify(batch) => {
                let orders = batch
                    .modifies
                    .iter()
                    .map(|modify| modify.order.clone())
                    .collect::<Vec<_>>();
                let result = self.reconcile_orders(&orders).await?;
                if !matches!(result, ReconcileOutcome::Inconclusive { .. }) {
                    return Ok(result);
                }
                let targets = batch
                    .modifies
                    .iter()
                    .map(|modify| match &modify.oid {
                        OrderTarget::Oid(oid) => Value::from(*oid),
                        OrderTarget::Cloid(cloid) => Value::String(cloid.to_string()),
                    })
                    .collect::<Vec<_>>();
                let mut all_open = true;
                for target in targets {
                    all_open &= matches!(
                        self.order_status(target).await?,
                        AuthoritativeOrder::Order { status, .. } if status == "open"
                    );
                }
                Ok(ReconcileOutcome::Inconclusive {
                    detail: if all_open {
                        "new cloids are unknown and original orders are still open; the modify may still apply"
                            .to_string()
                    } else {
                        "new modify cloids are unknown and original orders are not all open"
                            .to_string()
                    },
                })
            }
            Action::UpdateLeverage(update) => {
                let market = self
                    .state
                    .read()
                    .await
                    .markets
                    .values()
                    .find(|market| market.asset.0 == update.asset)
                    .cloned()
                    .ok_or_else(|| format!("unknown asset {}", update.asset))?;
                let value = self
                    .client
                    .post(json!({
                        "type": "activeAssetData",
                        "user": self.user,
                        "coin": market.wire_symbol,
                    }))
                    .await
                    .map_err(|err| err.to_string())?;
                let actual = parse_active_asset_data(&value).map_err(|err| err.to_string())?;
                Ok(
                    if actual.leverage == update.leverage
                        && actual.leverage_cross == update.is_cross
                    {
                        ReconcileOutcome::Applied {
                            detail: "active asset leverage matches requested value".to_string(),
                            statuses: vec![OrderStatus::Success],
                        }
                    } else {
                        ReconcileOutcome::Inconclusive {
                            detail: "active asset leverage does not yet match requested value"
                                .to_string(),
                        }
                    },
                )
            }
            Action::AgentSetAbstraction(update) => {
                let value = self
                    .client
                    .post(json!({ "type": "userAbstraction", "user": self.user }))
                    .await
                    .map_err(|err| err.to_string())?;
                let actual = value
                    .as_str()
                    .or_else(|| value.get("userAbstraction").and_then(Value::as_str))
                    .ok_or_else(|| "userAbstraction response invalid".to_string())?;
                let expected = match update.abstraction.as_str() {
                    "i" => "disabled",
                    "u" => "unifiedAccount",
                    "p" => "portfolioMargin",
                    other => return Err(format!("unknown requested abstraction {other}")),
                };
                Ok(
                    if actual == expected || (update.abstraction == "i" && actual == "default") {
                        ReconcileOutcome::Applied {
                            detail: "account abstraction matches requested mode".to_string(),
                            statuses: vec![OrderStatus::Success],
                        }
                    } else {
                        ReconcileOutcome::Inconclusive {
                            detail: format!(
                                "account abstraction does not yet match: expected={expected} actual={actual}"
                            ),
                        }
                    },
                )
            }
            _ => Ok(ReconcileOutcome::Inconclusive {
                detail: "action family has no authoritative reconciliation contract".to_string(),
            }),
        }
    }
}

impl ActionReconciler for InfoReconciler {
    fn reconcile<'a>(&'a self, action: &'a Action) -> ReconcileFuture<'a> {
        Box::pin(async move { self.reconcile_action(action).await })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthoritativeOrder {
    Unknown,
    Order {
        status: String,
        oid: u64,
        cloid: Option<String>,
    },
}

fn parse_authoritative_order(value: &Value) -> Result<AuthoritativeOrder, String> {
    match value.get("status").and_then(Value::as_str) {
        Some("unknownOid") => Ok(AuthoritativeOrder::Unknown),
        Some("order") => {
            let wrapper = value
                .get("order")
                .ok_or_else(|| "orderStatus missing order".to_string())?;
            let order = wrapper
                .get("order")
                .ok_or_else(|| "orderStatus missing nested order".to_string())?;
            Ok(AuthoritativeOrder::Order {
                status: wrapper
                    .get("status")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "orderStatus missing terminal status".to_string())?
                    .to_string(),
                oid: order
                    .get("oid")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| "orderStatus missing oid".to_string())?,
                cloid: order
                    .get("cloid")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            })
        }
        other => Err(format!("unexpected orderStatus response status {other:?}")),
    }
}

fn authoritative_order_status(status: AuthoritativeOrder) -> OrderStatus {
    match status {
        AuthoritativeOrder::Unknown => OrderStatus::Error {
            message: "order cloid remained unknown during partial reconciliation".to_string(),
        },
        AuthoritativeOrder::Order { status, oid, cloid } if status == "open" => {
            OrderStatus::Resting { oid, cloid }
        }
        AuthoritativeOrder::Order { status, .. }
            if matches!(status.as_str(), "filled" | "canceled" | "triggered") =>
        {
            OrderStatus::Success
        }
        AuthoritativeOrder::Order { status, .. } => OrderStatus::Error {
            message: format!("authoritative order status is {status}"),
        },
    }
}

impl InfoClient {
    pub fn new(network: Network) -> Self {
        Self {
            http: reqwest::Client::new(),
            network,
            metrics: Metrics::default(),
        }
    }

    pub fn with_metrics(network: Network, metrics: Metrics) -> Self {
        Self {
            http: reqwest::Client::new(),
            network,
            metrics,
        }
    }

    pub async fn seed_state(
        &self,
        default_symbol: &str,
        allowed_symbols: &BTreeSet<String>,
        user: Option<&str>,
    ) -> anyhow::Result<TradingState> {
        let default_symbol = default_symbol.to_ascii_uppercase();
        let mut state = TradingState::new(default_symbol.clone());
        state.fill_session_start_ms = now_ms();
        let (markets, spot_price_wires) = self.markets(allowed_symbols).await?;
        state.spot_price_wires = spot_price_wires;
        for market in markets {
            state.set_market(market);
        }
        anyhow::ensure!(
            state.markets.contains_key(&default_symbol),
            "default symbol {default_symbol} missing from live market metadata"
        );
        let wire_symbol = active_market(&state, &default_symbol)?.wire_symbol.clone();
        let book = self.book(&wire_symbol).await?;
        let now = now_ms();
        state.apply_book(&default_symbol, book, now, Some(now));
        if let Some(user) = user {
            self.apply_user_state(&mut state, user, &default_symbol)
                .await?;
        }
        Ok(state)
    }

    pub async fn refresh_market(
        &self,
        state: &Arc<RwLock<TradingState>>,
        user: Option<&str>,
        symbol: &str,
    ) -> anyhow::Result<()> {
        let market = {
            let state = state.read().await;
            active_market(&state, symbol)?.clone()
        };
        let book = self.book(&market.wire_symbol).await?;
        let user_values = if let Some(user) = user {
            let request_ms = now_ms();
            let account = self
                .post(user_scoped("clearinghouseState", user, None))
                .await?;
            let position_state = if market.dex.is_some() {
                self.post(user_scoped(
                    "clearinghouseState",
                    user,
                    market.dex.as_deref(),
                ))
                .await?
            } else {
                account.clone()
            };
            let orders = self
                .post(user_scoped(
                    "frontendOpenOrders",
                    user,
                    market.dex.as_deref(),
                ))
                .await?;
            let account_mode = self
                .post(json!({ "type": "userAbstraction", "user": user }))
                .await?;
            let spot_state = self
                .post(json!({ "type": "spotClearinghouseState", "user": user }))
                .await?;
            let all_mids = self.post(json!({ "type": "allMids" })).await?;
            let borrow_lend = self
                .post(json!({ "type": "borrowLendUserState", "user": user }))
                .await?;
            let active_asset = if market.kind == MarketKind::Spot {
                None
            } else {
                Some(
                    self.post(json!({
                        "type": "activeAssetData",
                        "user": user,
                        "coin": market.wire_symbol,
                    }))
                    .await?,
                )
            };
            Some((
                request_ms,
                account,
                position_state,
                orders,
                account_mode,
                spot_state,
                all_mids,
                borrow_lend,
                active_asset,
            ))
        } else {
            None
        };
        let now = now_ms();
        let mut state = state.write().await;
        anyhow::ensure!(
            state.markets.contains_key(symbol),
            "market removed during refresh: {symbol}"
        );
        let mut candidate = state.clone();
        candidate.apply_book(symbol, book, now, Some(now));
        if let Some((
            request_ms,
            account,
            position_state,
            orders,
            account_mode,
            spot_state,
            all_mids,
            borrow_lend,
            active_asset,
        )) = user_values
        {
            apply_account(&mut candidate, &account, now, request_ms)?;
            apply_account_mode(&mut candidate, &account_mode, now, Some(request_ms))?;
            apply_spot_balances(
                &mut candidate,
                &spot_state,
                Some(&all_mids),
                now,
                request_ms,
            )?;
            apply_borrow_lend(&mut candidate, &borrow_lend, now, request_ms)?;
            let dex_symbols = symbols_in_dex(&candidate, market.dex.as_deref());
            if market.kind != MarketKind::Spot {
                apply_positions_for_watched(
                    &mut candidate,
                    &position_state,
                    now,
                    request_ms,
                    market.dex.as_deref(),
                    &dex_symbols,
                )?;
            }
            let dex_symbol_refs = dex_symbols.iter().map(String::as_str).collect::<Vec<_>>();
            apply_open_orders_for_symbols(
                &mut candidate,
                &orders,
                now,
                request_ms,
                market.dex.as_deref(),
                &dex_symbol_refs,
            )?;
            if let Some(active_asset) = active_asset {
                candidate.apply_active_asset(
                    symbol,
                    parse_active_asset_data(&active_asset)?,
                    now,
                    Some(request_ms),
                );
            }
        }
        *state = candidate;
        Ok(())
    }

    async fn markets(
        &self,
        allowed_symbols: &BTreeSet<String>,
    ) -> anyhow::Result<(Vec<Market>, BTreeMap<u64, String>)> {
        let mut markets = parse_markets(
            &self.post(json!({ "type": "metaAndAssetCtxs" })).await?,
            allowed_symbols,
            None,
        )?;
        for dex in parse_perp_dexs(&self.post(json!({ "type": "perpDexs" })).await?)? {
            let value = self
                .post(json!({ "type": "metaAndAssetCtxs", "dex": dex.name.as_str() }))
                .await?;
            markets.extend(parse_markets(&value, allowed_symbols, Some(&dex))?);
        }
        let spots =
            parse_spot_markets(&self.post(json!({ "type": "spotMetaAndAssetCtxs" })).await?)?;
        let (spot_markets, spot_price_wires) = select_spot_markets(spots, allowed_symbols);
        markets.extend(spot_markets);
        Ok((markets, spot_price_wires))
    }

    async fn book(&self, symbol: &str) -> anyhow::Result<Book> {
        let value = self
            .post(json!({ "type": "l2Book", "coin": symbol }))
            .await?;
        parse_book(&value)
    }

    async fn apply_user_state(
        &self,
        state: &mut TradingState,
        user: &str,
        active: &str,
    ) -> anyhow::Result<()> {
        let request_ms = now_ms();
        let market = active_market(state, active)?.clone();
        let account = self
            .post(user_scoped("clearinghouseState", user, None))
            .await?;
        let position_state = if market.dex.is_some() {
            self.post(user_scoped(
                "clearinghouseState",
                user,
                market.dex.as_deref(),
            ))
            .await?
        } else {
            account.clone()
        };
        let orders = self
            .post(user_scoped(
                "frontendOpenOrders",
                user,
                market.dex.as_deref(),
            ))
            .await?;
        let account_mode = self
            .post(json!({ "type": "userAbstraction", "user": user }))
            .await?;
        let spot_state = self
            .post(json!({ "type": "spotClearinghouseState", "user": user }))
            .await?;
        let all_mids = self.post(json!({ "type": "allMids" })).await?;
        let borrow_lend = self
            .post(json!({ "type": "borrowLendUserState", "user": user }))
            .await?;
        let active_asset = if market.kind == MarketKind::Spot {
            None
        } else {
            Some(
                self.post(
                    json!({ "type": "activeAssetData", "user": user, "coin": market.wire_symbol }),
                )
                .await?,
            )
        };
        let now = now_ms();
        apply_account(state, &account, now, request_ms)?;
        apply_account_mode(state, &account_mode, now, Some(request_ms))?;
        apply_spot_balances(state, &spot_state, Some(&all_mids), now, request_ms)?;
        apply_borrow_lend(state, &borrow_lend, now, request_ms)?;
        let dex_symbols = symbols_in_dex(state, market.dex.as_deref());
        if market.kind != MarketKind::Spot {
            apply_positions_for_watched(
                state,
                &position_state,
                now,
                request_ms,
                market.dex.as_deref(),
                &dex_symbols,
            )?;
        }
        let dex_symbol_refs = dex_symbols.iter().map(String::as_str).collect::<Vec<_>>();
        apply_open_orders_for_symbols(
            state,
            &orders,
            now,
            request_ms,
            market.dex.as_deref(),
            &dex_symbol_refs,
        )?;
        if let Some(active_asset) = active_asset {
            state.apply_active_asset(
                active,
                parse_active_asset_data(&active_asset)?,
                now,
                Some(request_ms),
            );
        }
        Ok(())
    }

    async fn post(&self, body: Value) -> anyhow::Result<Value> {
        self.metrics.rest_call();
        let response = self
            .http
            .post(self.info_url())
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }

    fn info_url(&self) -> &'static str {
        match self.network {
            Network::Mainnet => "https://api.hyperliquid.xyz/info",
            Network::Testnet => "https://api.hyperliquid-testnet.xyz/info",
        }
    }
}

pub fn spawn_info_refresh(
    network: Network,
    user: Option<String>,
    state: Arc<RwLock<TradingState>>,
    metrics: Metrics,
    interval: Duration,
    markets: watch::Receiver<BTreeSet<String>>,
) {
    tokio::spawn(async move {
        let client = InfoClient::with_metrics(network, metrics);
        let mut ticker = tokio::time::interval(interval);
        let mut cursor = 0_usize;
        loop {
            ticker.tick().await;
            let (active, wire_symbol, dex, kind, watched) = {
                let state = state.read().await;
                let watched = markets.borrow().clone();
                let symbols = watched.iter().cloned().collect::<Vec<_>>();
                if symbols.is_empty() {
                    continue;
                }
                let active = &symbols[cursor % symbols.len()];
                cursor = cursor.wrapping_add(1);
                let market = match active_market(&state, active) {
                    Ok(market) => market,
                    Err(_) => continue,
                };
                (
                    active.clone(),
                    market.wire_symbol.clone(),
                    market.dex.clone(),
                    market.kind,
                    watched,
                )
            };
            let refresh = async {
                let book = match client.book(&wire_symbol).await {
                    Ok(book) => Some(book),
                    Err(err) => {
                        eprintln!("info book refresh failed for {active}: {err:#}");
                        None
                    }
                };
                let user_values = if let Some(user) = user.as_deref() {
                    let request_ms = now_ms();
                    let account = client
                        .post(user_scoped("clearinghouseState", user, None))
                        .await?;
                    let position_state = if dex.is_some() {
                        client
                            .post(user_scoped("clearinghouseState", user, dex.as_deref()))
                            .await?
                    } else {
                        account.clone()
                    };
                    let orders = client
                        .post(user_scoped("frontendOpenOrders", user, dex.as_deref()))
                        .await?;
                    let account_mode = client
                        .post(json!({ "type": "userAbstraction", "user": user }))
                        .await?;
                    let (spot_state, active_asset) = if kind == MarketKind::Spot {
                        (
                            Some(
                            client
                                .post(json!({ "type": "spotClearinghouseState", "user": user }))
                                .await?,
                            ),
                            None,
                        )
                    } else {
                        (
                            None,
                            Some(client.post(
                                json!({ "type": "activeAssetData", "user": user, "coin": wire_symbol }),
                            ).await?),
                        )
                    };
                    let all_mids = if spot_state.is_some() {
                        Some(client.post(json!({ "type": "allMids" })).await?)
                    } else {
                        None
                    };
                    Some((
                        request_ms,
                        account,
                        position_state,
                        orders,
                        account_mode,
                        spot_state,
                        active_asset,
                        all_mids,
                    ))
                } else {
                    None
                };
                Ok::<_, anyhow::Error>((book, user_values))
            }
            .await;
            let (book, user_values) = match refresh {
                Ok(values) => values,
                Err(err) => {
                    eprintln!("info user refresh failed for {active}: {err:#}");
                    continue;
                }
            };
            let now = now_ms();
            let mut state = state.write().await;
            if let Some(book) = book {
                state.apply_book(&active, book, now, Some(now));
            }
            if let Some((
                request_ms,
                account,
                position_state,
                orders,
                account_mode,
                spot_state,
                active_asset,
                all_mids,
            )) = user_values
            {
                let mut candidate = state.clone();
                let applied = (|| -> anyhow::Result<()> {
                    apply_account(&mut candidate, &account, now, request_ms)?;
                    apply_account_mode(&mut candidate, &account_mode, now, Some(request_ms))?;
                    if let Some(spot_state) = spot_state {
                        apply_spot_balances(
                            &mut candidate,
                            &spot_state,
                            all_mids.as_ref(),
                            now,
                            request_ms,
                        )?;
                    } else {
                        apply_positions_for_watched(
                            &mut candidate,
                            &position_state,
                            now,
                            request_ms,
                            dex.as_deref(),
                            &watched,
                        )?;
                    }
                    let watched_symbols = watched.iter().map(String::as_str).collect::<Vec<_>>();
                    apply_open_orders_for_symbols(
                        &mut candidate,
                        &orders,
                        now,
                        request_ms,
                        dex.as_deref(),
                        &watched_symbols,
                    )?;
                    if let Some(active_asset) = active_asset {
                        candidate.apply_active_asset(
                            &active,
                            parse_active_asset_data(&active_asset)?,
                            now,
                            Some(request_ms),
                        );
                    }
                    Ok(())
                })();
                if applied.is_ok() {
                    *state = candidate;
                } else if let Err(err) = applied {
                    eprintln!("info snapshot rejected for {active}: {err:#}");
                }
            }
        }
    });
}

#[derive(Clone)]
struct PerpDex {
    index: u32,
    name: String,
}

#[derive(Clone)]
struct SpotToken {
    name: String,
    size_decimals: i64,
}

struct SpotMarket {
    market: Market,
    base_token: u64,
    quote: String,
}

fn parse_perp_dexs(value: &Value) -> anyhow::Result<Vec<PerpDex>> {
    value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("perpDexs response must be array"))?
        .iter()
        .enumerate()
        .filter_map(|(index, row)| row.as_object().map(|_| (index, row)))
        .map(|(index, row)| {
            Ok(PerpDex {
                index: index as u32,
                name: row
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("perpDexs dex missing name"))?
                    .to_string(),
            })
        })
        .collect()
}

fn parse_spot_markets(value: &Value) -> anyhow::Result<Vec<SpotMarket>> {
    let meta = value
        .as_array()
        .and_then(|items| items.first())
        .ok_or_else(|| anyhow::anyhow!("spotMetaAndAssetCtxs missing meta"))?;
    let mut tokens = BTreeMap::new();
    for token in meta
        .get("tokens")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("spotMetaAndAssetCtxs missing tokens"))?
    {
        let name = nonempty_str(token, "name")?;
        let size_decimals = value_u64(token, "szDecimals")?;
        anyhow::ensure!(
            size_decimals <= 8,
            "spot token {name} invalid szDecimals {size_decimals}"
        );
        tokens.insert(
            value_u64(token, "index")?,
            SpotToken {
                name: name.to_ascii_uppercase(),
                size_decimals: size_decimals as i64,
            },
        );
    }
    let universe = meta
        .get("universe")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("spotMetaAndAssetCtxs missing universe"))?;
    universe
        .iter()
        .map(|row| {
            let token_ids = row
                .get("tokens")
                .and_then(Value::as_array)
                .filter(|tokens| tokens.len() == 2)
                .ok_or_else(|| anyhow::anyhow!("spotMetaAndAssetCtxs market missing token pair"))?;
            let raw_name = nonempty_str(row, "name")?.to_string();
            let spot_index = value_u64(row, "index")?.try_into()?;
            let base_token = token_ids[0].as_u64().ok_or_else(|| {
                anyhow::anyhow!("spotMetaAndAssetCtxs market {raw_name} missing base token")
            })?;
            let base = tokens.get(&base_token).ok_or_else(|| {
                anyhow::anyhow!("spotMetaAndAssetCtxs market {raw_name} base token unknown")
            })?;
            let quote = tokens
                .get(&token_ids[1].as_u64().ok_or_else(|| {
                    anyhow::anyhow!("spotMetaAndAssetCtxs market {raw_name} missing quote token")
                })?)
                .ok_or_else(|| {
                    anyhow::anyhow!("spotMetaAndAssetCtxs market {raw_name} quote token unknown")
                })?;
            let symbol = format!("SPOT:{}/{}", base.name, quote.name);
            Ok(SpotMarket {
                market: Market {
                    symbol,
                    wire_symbol: raw_name,
                    dex: None,
                    asset: AssetId::spot(spot_index),
                    kind: MarketKind::Spot,
                    size_decimals: base.size_decimals,
                    max_leverage: None,
                    delisted: false,
                    open_interest_cap: false,
                },
                base_token,
                quote: quote.name.clone(),
            })
        })
        .collect()
}

fn select_spot_markets(
    spots: Vec<SpotMarket>,
    allowed_symbols: &BTreeSet<String>,
) -> (Vec<Market>, BTreeMap<u64, String>) {
    let prices = spots
        .iter()
        .filter(|spot| spot.quote.eq_ignore_ascii_case("USDC"))
        .map(|spot| (spot.base_token, spot.market.wire_symbol.clone()))
        .collect();
    let markets = spots
        .into_iter()
        .map(|spot| spot.market)
        .filter(|market| allowed_symbols.is_empty() || allowed_symbols.contains(&market.symbol))
        .collect();
    (markets, prices)
}

fn parse_markets(
    value: &Value,
    allowed_symbols: &BTreeSet<String>,
    dex: Option<&PerpDex>,
) -> anyhow::Result<Vec<Market>> {
    let meta = value
        .as_array()
        .and_then(|items| items.first())
        .ok_or_else(|| anyhow::anyhow!("metaAndAssetCtxs missing meta"))?;
    let universe = meta
        .get("universe")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("metaAndAssetCtxs missing universe"))?;
    universe
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let wire_symbol = row.get("name")?.as_str()?.to_string();
            let symbol = wire_symbol.to_ascii_uppercase();
            (allowed_symbols.is_empty() || allowed_symbols.contains(&symbol)).then_some((
                index,
                symbol,
                wire_symbol,
                row,
            ))
        })
        .map(|(index, symbol, wire_symbol, row)| {
            let size_decimals = row
                .get("szDecimals")
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow::anyhow!("{symbol} missing szDecimals"))?;
            anyhow::ensure!(
                (0..=6).contains(&size_decimals),
                "{symbol} invalid szDecimals {size_decimals}"
            );
            let max_leverage = row
                .get("maxLeverage")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| anyhow::anyhow!("{symbol} missing valid maxLeverage"))?;
            Ok(Market {
                wire_symbol,
                dex: dex.map(|dex| dex.name.clone()),
                asset: dex.map_or_else(
                    || AssetId::native_perp(index as u32),
                    |dex| AssetId::builder_perp(dex.index, index as u32),
                ),
                kind: if dex.is_some() {
                    MarketKind::BuilderPerp
                } else {
                    MarketKind::Perp
                },
                symbol,
                size_decimals,
                max_leverage: Some(max_leverage),
                delisted: bool_field(row, &["isDelisted", "delisted"]),
                open_interest_cap: bool_field(row, &["openInterestCap", "oiCap"]),
            })
        })
        .collect()
}

pub(crate) fn canonical_symbol_for_coin(state: &TradingState, coin: &str) -> String {
    canonical_symbol_for_coin_in_dex(state, coin, None)
}

pub(crate) fn canonical_symbol_for_coin_in_dex(
    state: &TradingState,
    coin: &str,
    dex: Option<&str>,
) -> String {
    let coin = coin.trim();
    let dex = dex.map(str::trim).filter(|dex| !dex.is_empty());
    if let Some(market) = dex.and_then(|dex| {
        let prefixed = (!coin.contains(':')).then(|| format!("{dex}:{coin}"));
        state.markets.values().find(|market| {
            market
                .dex
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(dex))
                && (market.symbol.eq_ignore_ascii_case(coin)
                    || market.wire_symbol.eq_ignore_ascii_case(coin)
                    || prefixed.as_deref().is_some_and(|prefixed| {
                        market.symbol.eq_ignore_ascii_case(prefixed)
                            || market.wire_symbol.eq_ignore_ascii_case(prefixed)
                    }))
        })
    }) {
        return market.symbol.clone();
    }
    state
        .markets
        .values()
        .find(|market| {
            market.symbol.eq_ignore_ascii_case(coin)
                || market.wire_symbol.eq_ignore_ascii_case(coin)
        })
        .map(|market| market.symbol.clone())
        .unwrap_or_else(|| {
            if let Some(dex) = dex
                && !coin.contains(':')
            {
                format!("{}:{}", dex.to_ascii_uppercase(), coin.to_ascii_uppercase())
            } else {
                coin.to_ascii_uppercase()
            }
        })
}

fn user_scoped(kind: &str, user: &str, dex: Option<&str>) -> Value {
    let mut value = json!({ "type": kind, "user": user });
    if let Some(dex) = dex.filter(|dex| !dex.is_empty()) {
        value["dex"] = Value::String(dex.to_string());
    }
    value
}

fn symbol_in_dex(state: &TradingState, symbol: &str, dex: Option<&str>) -> bool {
    state
        .markets
        .get(symbol)
        .is_some_and(|market| same_dex(market.dex.as_deref(), dex))
}

fn symbols_in_dex(state: &TradingState, dex: Option<&str>) -> BTreeSet<String> {
    state
        .markets
        .values()
        .filter(|market| same_dex(market.dex.as_deref(), dex))
        .map(|market| market.symbol.clone())
        .collect()
}

fn same_dex(left: Option<&str>, right: Option<&str>) -> bool {
    left.unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case(right.unwrap_or_default().trim())
}

fn active_market<'a>(state: &'a TradingState, symbol: &str) -> anyhow::Result<&'a Market> {
    state
        .markets
        .get(symbol)
        .ok_or_else(|| anyhow::anyhow!("unknown active market {symbol}"))
}

fn parse_book(value: &Value) -> anyhow::Result<Book> {
    let levels = value
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("l2Book missing levels"))?;
    Ok(Book {
        bid: level_price(levels.first(), "bid")?,
        ask: level_price(levels.get(1), "ask")?,
    })
}

pub(crate) fn apply_account(
    state: &mut TradingState,
    value: &Value,
    local_ms: u64,
    exchange_ms: u64,
) -> anyhow::Result<()> {
    let summary = value
        .get("marginSummary")
        .or_else(|| value.get("crossMarginSummary"))
        .ok_or_else(|| anyhow::anyhow!("clearinghouseState missing margin summary"))?;
    let account = Account {
        value_usd: decimal_field(summary, &["accountValue", "totalRawUsd"])?,
        available_margin_usd: decimal_field(value, &["withdrawable"])?,
        margin_used_usd: decimal_field(summary, &["totalMarginUsed"])?,
        notional_usd: decimal_field(summary, &["totalNtlPos"])?,
    };
    state.apply_account(account, local_ms, Some(exchange_ms));
    Ok(())
}

pub(crate) fn apply_positions(
    state: &mut TradingState,
    value: &Value,
    local_ms: u64,
    exchange_ms: u64,
    active: &str,
    dex: Option<&str>,
) -> anyhow::Result<bool> {
    let mut has_active = false;
    let mut present = BTreeSet::new();
    let rows = value
        .get("assetPositions")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("clearinghouseState missing assetPositions"))?;
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let position = row
            .get("position")
            .ok_or_else(|| anyhow::anyhow!("asset position missing position"))?;
        let symbol = position
            .get("coin")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("position missing coin"))?;
        let symbol = canonical_symbol_for_coin_in_dex(state, symbol, dex);
        anyhow::ensure!(
            present.insert(symbol.clone()),
            "duplicate position for {symbol}"
        );
        let size = decimal_field(position, &["szi", "size"])?;
        let entry_price = position
            .get("entryPx")
            .and_then(Value::as_str)
            .map(decimal)
            .transpose()?;
        has_active |= symbol == active;
        parsed.push(Position {
            symbol,
            size,
            entry_price,
            detail: Some(position_detail(position)?),
        });
    }
    for position in parsed {
        state.apply_position_snapshot(position, local_ms, Some(exchange_ms));
    }
    state.reconcile_positions_for_dex(&present, dex, exchange_ms);
    Ok(has_active)
}

pub(crate) fn apply_positions_for_watched(
    state: &mut TradingState,
    value: &Value,
    local_ms: u64,
    exchange_ms: u64,
    dex: Option<&str>,
    watched: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let representative = watched
        .iter()
        .find(|symbol| {
            state.markets.get(*symbol).is_some_and(|market| {
                market.kind != MarketKind::Spot && same_dex(market.dex.as_deref(), dex)
            })
        })
        .cloned()
        .or_else(|| {
            state
                .markets
                .values()
                .find(|market| {
                    market.kind != MarketKind::Spot && same_dex(market.dex.as_deref(), dex)
                })
                .map(|market| market.symbol.clone())
        })
        .unwrap_or_default();
    apply_positions(state, value, local_ms, exchange_ms, &representative, dex)?;
    let missing = watched
        .iter()
        .filter(|symbol| {
            state.markets.get(*symbol).is_some_and(|market| {
                market.kind != MarketKind::Spot && same_dex(market.dex.as_deref(), dex)
            }) && !state.positions.contains_key(*symbol)
        })
        .cloned()
        .collect::<Vec<_>>();
    for symbol in missing {
        state.apply_position_snapshot(Position::empty(symbol), local_ms, Some(exchange_ms));
    }
    Ok(())
}

pub(crate) fn parse_active_asset_data(value: &Value) -> anyhow::Result<ActiveAssetData> {
    let leverage = value
        .get("leverage")
        .ok_or_else(|| anyhow::anyhow!("activeAssetData missing leverage"))?;
    let leverage_value = leverage
        .get("value")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| anyhow::anyhow!("activeAssetData leverage value invalid"))?;
    let leverage_type = leverage
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("activeAssetData leverage type invalid"))?;
    anyhow::ensure!(
        matches!(leverage_type, "cross" | "isolated"),
        "activeAssetData leverage type unsupported"
    );
    let max_trade_sizes = decimal_pair(value, "maxTradeSzs")?;
    let available_to_trade = decimal_pair(value, "availableToTrade")?;
    let mark_price = decimal_field(value, &["markPx"])?;
    anyhow::ensure!(
        max_trade_sizes.iter().all(|value| *value >= Decimal::ZERO)
            && available_to_trade
                .iter()
                .all(|value| *value >= Decimal::ZERO),
        "activeAssetData capacity must be nonnegative"
    );
    anyhow::ensure!(
        leverage_value > 0 && mark_price > Decimal::ZERO,
        "activeAssetData leverage and mark price must be positive"
    );
    Ok(ActiveAssetData {
        leverage: leverage_value,
        leverage_cross: leverage_type == "cross",
        max_trade_sizes,
        available_to_trade,
        mark_price,
    })
}

fn decimal_pair(value: &Value, key: &str) -> anyhow::Result<[Decimal; 2]> {
    let values = value
        .get(key)
        .and_then(Value::as_array)
        .filter(|values| values.len() == 2)
        .ok_or_else(|| anyhow::anyhow!("activeAssetData {key} must contain two values"))?;
    Ok([
        value_decimal(&values[0])
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("activeAssetData {key}[0] invalid"))?,
        value_decimal(&values[1])
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("activeAssetData {key}[1] invalid"))?,
    ])
}

fn position_detail(position: &Value) -> anyhow::Result<PositionDetail> {
    let leverage = position
        .get("leverage")
        .ok_or_else(|| anyhow::anyhow!("position missing leverage"))?;
    let leverage_value = leverage
        .get("value")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow::anyhow!("position leverage value invalid"))?;
    let leverage_type = leverage
        .get("type")
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "cross" | "isolated"))
        .ok_or_else(|| anyhow::anyhow!("position leverage type invalid"))?;
    Ok(PositionDetail {
        unrealized_pnl: decimal_field(position, &["unrealizedPnl"])?,
        return_on_equity: decimal_field(position, &["returnOnEquity"])?,
        liquidation_px: position
            .get("liquidationPx")
            .and_then(Value::as_str)
            .map(decimal)
            .transpose()?,
        margin_used: decimal_field(position, &["marginUsed"])?,
        position_value: decimal_field(position, &["positionValue"])?,
        leverage: leverage_value,
        leverage_cross: leverage_type == "cross",
    })
}

pub(crate) fn apply_spot_balances(
    state: &mut TradingState,
    value: &Value,
    all_mids: Option<&Value>,
    local_ms: u64,
    exchange_ms: u64,
) -> anyhow::Result<()> {
    if let Some(all_mids) = all_mids {
        replace_spot_marks(state, all_mids)?;
    }
    let available_after_maintenance = token_decimal_map(value, "tokenToAvailableAfterMaintenance")?;
    let portfolio_borrow_ratio = token_decimal_map(value, "tokenToPortfolioBorrowRatio")?;
    let has_available_after_maintenance = value.get("tokenToAvailableAfterMaintenance").is_some();
    let has_portfolio_borrow_ratio = value.get("tokenToPortfolioBorrowRatio").is_some();
    let balances = spot_balance_rows(value)
        .ok_or_else(|| anyhow::anyhow!("spotClearinghouseState missing balances"))?
        .iter()
        .map(|row| {
            let token = row.get("token").and_then(Value::as_u64);
            let coin = nonempty_str(row, "coin")?.to_string();
            let previous = state.spot_balance(&coin).map(|balance| &balance.value);
            Ok(SpotBalance {
                coin,
                token,
                total: decimal_field(row, &["total"])?,
                hold: decimal_field(row, &["hold"])?,
                entry_ntl: decimal_field(row, &["entryNtl"])?,
                ltv: optional_decimal_update(row, "ltv", previous.and_then(|value| value.ltv))?,
                supplied: optional_decimal_update(
                    row,
                    "supplied",
                    previous.and_then(|value| value.supplied),
                )?,
                available_after_maintenance: if has_available_after_maintenance {
                    token.and_then(|token| available_after_maintenance.get(&token).copied())
                } else {
                    previous.and_then(|value| value.available_after_maintenance)
                },
                portfolio_borrow_ratio: if has_portfolio_borrow_ratio {
                    token.and_then(|token| portfolio_borrow_ratio.get(&token).copied())
                } else {
                    previous.and_then(|value| value.portfolio_borrow_ratio)
                },
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let (spot_value_usd, spot_available_usd, spot_unpriced_count) =
        spot_balance_values(state, balances.iter());
    let summary = BalanceSummary {
        portfolio_margin_enabled: optional_bool(value, &["portfolioMarginEnabled"]).unwrap_or_else(
            || {
                state
                    .balance_summary
                    .as_ref()
                    .is_some_and(|summary| summary.value.portfolio_margin_enabled)
            },
        ),
        portfolio_margin_ratio: optional_decimal_update(
            value,
            "portfolioMarginRatio",
            state
                .balance_summary
                .as_ref()
                .and_then(|summary| summary.value.portfolio_margin_ratio),
        )?,
        spot_value_usd: Some(spot_value_usd),
        spot_available_usd: Some(spot_available_usd),
        spot_unpriced_count,
        borrow_lend_health: state
            .balance_summary
            .as_ref()
            .and_then(|summary| summary.value.borrow_lend_health.clone()),
        borrow_lend_health_factor: state
            .balance_summary
            .as_ref()
            .and_then(|summary| summary.value.borrow_lend_health_factor),
    };
    state.apply_spot_balances(balances, summary, local_ms, Some(exchange_ms));
    if has_available_after_maintenance
        && optional_bool(value, &["portfolioMarginEnabled"]) == Some(true)
    {
        state.apply_spot_capacity(local_ms, Some(exchange_ms));
    }
    Ok(())
}

fn spot_balance_values<'a>(
    state: &TradingState,
    balances: impl IntoIterator<Item = &'a SpotBalance>,
) -> (Decimal, Decimal, usize) {
    let mut total = Decimal::ZERO;
    let mut available = Decimal::ZERO;
    let mut unpriced = 0;
    for balance in balances {
        if balance.total == Decimal::ZERO && balance.available() == Decimal::ZERO {
            continue;
        }
        let Some(price) = spot_balance_mark_price(state, balance) else {
            unpriced += 1;
            continue;
        };
        total += balance.total * price;
        available += balance.available() * price;
    }
    (total.normalize(), available.normalize(), unpriced)
}

fn spot_balance_mark_price(state: &TradingState, balance: &SpotBalance) -> Option<Decimal> {
    if balance.coin.eq_ignore_ascii_case("USDC") {
        return Some(Decimal::ONE);
    }
    balance
        .token
        .and_then(|token| state.spot_marks_usd.get(&token).copied())
}

pub(crate) fn apply_spot_mids(state: &mut TradingState, value: &Value) -> anyhow::Result<()> {
    replace_spot_marks(state, value)?;
    let (value, available, unpriced) = spot_balance_values(
        state,
        state.spot_balances.values().map(|balance| &balance.value),
    );
    if let Some(summary) = state.balance_summary.as_mut() {
        summary.value.spot_value_usd = Some(value);
        summary.value.spot_available_usd = Some(available);
        summary.value.spot_unpriced_count = unpriced;
    }
    Ok(())
}

fn replace_spot_marks(state: &mut TradingState, value: &Value) -> anyhow::Result<()> {
    let mids = value
        .get("mids")
        .unwrap_or(value)
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("allMids response must be object"))?;
    let mut marks = BTreeMap::new();
    for (token, wire) in &state.spot_price_wires {
        if let Some(value) = mids.get(wire) {
            marks.insert(
                *token,
                value_decimal(value)
                    .transpose()?
                    .ok_or_else(|| anyhow::anyhow!("allMids {wire} value invalid"))?,
            );
        }
    }
    state.spot_marks_usd = marks;
    Ok(())
}

pub(crate) fn apply_borrow_lend(
    state: &mut TradingState,
    value: &Value,
    local_ms: u64,
    exchange_ms: u64,
) -> anyhow::Result<()> {
    let entries = value
        .get("tokenToState")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let parts = row
                        .as_array()
                        .filter(|parts| parts.len() == 2)
                        .ok_or_else(|| anyhow::anyhow!("borrowLendUserState token row invalid"))?;
                    let token = parts[0]
                        .as_u64()
                        .ok_or_else(|| anyhow::anyhow!("borrowLendUserState token invalid"))?;
                    let state = &parts[1];
                    Ok(BorrowLend {
                        token,
                        borrow_value: decimal_field(
                            state.get("borrow").ok_or_else(|| {
                                anyhow::anyhow!("borrowLendUserState missing borrow")
                            })?,
                            &["value"],
                        )?,
                        supply_value: decimal_field(
                            state.get("supply").ok_or_else(|| {
                                anyhow::anyhow!("borrowLendUserState missing supply")
                            })?,
                            &["value"],
                        )?,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    let health = value
        .get("health")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let health_factor = optional_decimal(value, "healthFactor")?;
    state.apply_borrow_lend(entries, health, health_factor, local_ms, Some(exchange_ms));
    Ok(())
}

pub(crate) fn apply_open_orders_for_symbols(
    state: &mut TradingState,
    value: &Value,
    local_ms: u64,
    exchange_ms: u64,
    dex: Option<&str>,
    requested_symbols: &[&str],
) -> anyhow::Result<()> {
    let rows = open_order_rows(value)
        .ok_or_else(|| anyhow::anyhow!("openOrders response must be array"))?;
    let mut by_symbol: BTreeMap<String, Vec<Order>> = BTreeMap::new();
    for row in rows {
        let coin = nonempty_str(row, "coin")?;
        let symbol = canonical_symbol_for_coin_in_dex(state, coin, dex);
        let oid = row
            .get("oid")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("open order missing oid"))?;
        let cloid = row
            .get("cloid")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let previous = previous_order(state, oid, cloid.as_deref());
        let is_trigger = bool_field(row, &["isTrigger", "is_trigger"]);
        let kind = if is_trigger {
            order_kind(row)
                .or_else(|| previous.map(|existing| existing.kind.clone()))
                .filter(|kind| *kind != OrderKind::Limit)
                .ok_or_else(|| anyhow::anyhow!("trigger order missing TP/SL classification"))?
        } else {
            previous
                .filter(|existing| existing.kind != OrderKind::Limit)
                .map(|existing| existing.kind.clone())
                .unwrap_or(OrderKind::Limit)
        };
        let tif = order_tif(row, &kind)?;
        let previous_tif = previous.and_then(|existing| existing.tif.clone());
        let is_buy = match row.get("side").and_then(Value::as_str) {
            Some("B") => true,
            Some("A") => false,
            _ => anyhow::bail!("open order side invalid"),
        };
        let reduce_only = optional_bool(row, &["reduceOnly", "reduce_only"])
            .or_else(|| previous.map(|existing| existing.reduce_only))
            .ok_or_else(|| anyhow::anyhow!("open order missing reduceOnly"))?;
        let order = Order {
            symbol: symbol.clone(),
            oid,
            cloid,
            is_buy,
            price: order_price(row, &kind, previous)?,
            size: decimal_field(row, &["sz", "origSz", "size"])?,
            reduce_only,
            tif: if kind == OrderKind::Limit {
                tif.or(previous_tif)
            } else {
                None
            },
            kind,
        };
        by_symbol.entry(symbol).or_default().push(order);
    }
    let mut symbols: BTreeSet<String> = by_symbol.keys().cloned().collect();
    symbols.extend(
        requested_symbols
            .iter()
            .filter(|symbol| symbol_in_dex(state, symbol, dex))
            .map(|symbol| (*symbol).to_string()),
    );
    symbols.extend(
        state
            .orders
            .values()
            .filter(|order| {
                order.value.oid < LOCAL_ORDER_OID_MIN
                    && symbol_in_dex(state, &order.value.symbol, dex)
            })
            .map(|order| order.value.symbol.clone()),
    );
    for symbol in symbols {
        let orders = by_symbol.remove(&symbol).unwrap_or_default();
        state.apply_orders_snapshot(&symbol, orders, local_ms, Some(exchange_ms));
    }
    Ok(())
}

pub(crate) fn apply_account_mode(
    state: &mut TradingState,
    value: &Value,
    local_ms: u64,
    exchange_ms: Option<u64>,
) -> anyhow::Result<()> {
    let raw = value
        .as_str()
        .or_else(|| value.get("userAbstraction").and_then(Value::as_str))
        .ok_or_else(|| anyhow::anyhow!("userAbstraction response must be string"))?;
    state.apply_account_mode(
        AccountMode::parse(raw).map_err(anyhow::Error::msg)?,
        local_ms,
        exchange_ms,
    );
    Ok(())
}

fn previous_order<'a>(state: &'a TradingState, oid: u64, cloid: Option<&str>) -> Option<&'a Order> {
    state
        .orders
        .get(&oid)
        .map(|order| &order.value)
        .or_else(|| {
            cloid.and_then(|cloid| {
                state
                    .orders
                    .values()
                    .find(|order| order.value.cloid.as_deref() == Some(cloid))
                    .map(|order| &order.value)
            })
        })
}

fn order_kind(row: &Value) -> Option<OrderKind> {
    if !bool_field(row, &["isTrigger", "is_trigger"]) {
        return None;
    }
    let trigger = row
        .get("triggerCondition")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let order_type = row
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if trigger.contains("take")
        || trigger.contains("tp")
        || order_type.contains("take")
        || order_type.contains("tp")
    {
        Some(OrderKind::TakeProfit)
    } else if trigger.contains("stop")
        || trigger.contains("sl")
        || order_type.contains("stop")
        || order_type.contains("sl")
    {
        Some(OrderKind::StopLoss)
    } else {
        None
    }
}

fn order_tif(row: &Value, kind: &OrderKind) -> anyhow::Result<Option<TimeInForce>> {
    if *kind != OrderKind::Limit {
        return Ok(None);
    }
    let raw = row
        .pointer("/orderType/limit/tif")
        .or_else(|| row.get("tif"))
        .and_then(Value::as_str);
    match raw.map(str::to_ascii_lowercase).as_deref() {
        Some("alo") => Ok(Some(TimeInForce::Alo)),
        Some("ioc") => Ok(Some(TimeInForce::Ioc)),
        Some("gtc") => Ok(Some(TimeInForce::Gtc)),
        None => Ok(None),
        Some(other) => anyhow::bail!("unknown limit tif {other}"),
    }
}

fn order_price(row: &Value, kind: &OrderKind, previous: Option<&Order>) -> anyhow::Result<Decimal> {
    if *kind == OrderKind::Limit {
        return decimal_field(row, &["limitPx", "px"]);
    }
    if let Some(raw) = row.get("triggerPx").and_then(Value::as_str) {
        return decimal(raw);
    }
    previous
        .filter(|existing| existing.kind != OrderKind::Limit)
        .map(|existing| existing.price)
        .ok_or_else(|| anyhow::anyhow!("trigger order missing triggerPx"))
}

fn open_order_rows(value: &Value) -> Option<&Vec<Value>> {
    value
        .as_array()
        .or_else(|| value.get("orders").and_then(Value::as_array))
        .or_else(|| value.get("openOrders").and_then(Value::as_array))
}

fn spot_balance_rows(value: &Value) -> Option<&Vec<Value>> {
    value.get("balances").and_then(Value::as_array)
}

fn token_decimal_map(value: &Value, key: &str) -> anyhow::Result<BTreeMap<u64, Decimal>> {
    value
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|row| {
            let parts = row
                .as_array()
                .filter(|parts| parts.len() == 2)
                .ok_or_else(|| anyhow::anyhow!("{key} row invalid"))?;
            let token = parts[0]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("{key} token invalid"))?;
            let value = value_decimal(&parts[1])
                .transpose()?
                .ok_or_else(|| anyhow::anyhow!("{key} value invalid"))?;
            Ok((token, value))
        })
        .collect()
}

fn decimal_field(value: &Value, keys: &[&str]) -> anyhow::Result<Decimal> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(value_decimal))
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("missing decimal field {}", keys.join("|")))
}

fn optional_decimal(value: &Value, key: &str) -> anyhow::Result<Option<Decimal>> {
    value.get(key).and_then(value_decimal).transpose()
}

fn value_decimal(value: &Value) -> Option<anyhow::Result<Decimal>> {
    value
        .as_str()
        .map(decimal)
        .or_else(|| value.as_f64().map(|value| decimal(&value.to_string())))
}

fn decimal(raw: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(raw)
        .map(|value| value.normalize())
        .map_err(Into::into)
}

fn level_price(level: Option<&Value>, side: &str) -> anyhow::Result<Decimal> {
    level
        .and_then(Value::as_array)
        .and_then(|levels| levels.first())
        .and_then(|row| row.get("px"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("l2Book missing {side}"))
        .and_then(decimal)
}

fn optional_bool(value: &Value, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_bool))
}

fn optional_decimal_update(
    value: &Value,
    key: &str,
    previous: Option<Decimal>,
) -> anyhow::Result<Option<Decimal>> {
    if value.get(key).is_some() {
        optional_decimal(value, key)
    } else {
        Ok(previous)
    }
}

fn bool_field(value: &Value, keys: &[&str]) -> bool {
    optional_bool(value, keys).unwrap_or(false)
}

fn nonempty_str<'a>(value: &'a Value, key: &str) -> anyhow::Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing nonempty {key}"))
}

fn value_u64(value: &Value, key: &str) -> anyhow::Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("missing integer {key}"))
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

    #[test]
    fn perp_metadata_requires_and_preserves_positive_max_leverage() {
        let allowed = BTreeSet::from(["BTC".to_string()]);
        let missing = parse_markets(
            &json!([{"universe": [{"name": "BTC", "szDecimals": 5}]}]),
            &allowed,
            None,
        )
        .unwrap_err();
        assert!(missing.to_string().contains("maxLeverage"));

        let markets = parse_markets(
            &json!([{"universe": [{
                "name": "BTC", "szDecimals": 5, "maxLeverage": 40
            }]}]),
            &allowed,
            None,
        )
        .unwrap();
        assert_eq!(markets[0].max_leverage, Some(40));
    }

    #[test]
    fn dex_position_snapshot_keeps_every_watched_flat_market_available() {
        let mut state = TradingState::new("BTC");
        for (index, symbol) in ["BTC", "ETH"].into_iter().enumerate() {
            state.set_market(Market {
                symbol: symbol.to_string(),
                wire_symbol: symbol.to_string(),
                dex: None,
                asset: AssetId::native_perp(index as u32),
                kind: MarketKind::Perp,
                size_decimals: 5,
                max_leverage: Some(40),
                delisted: false,
                open_interest_cap: false,
            });
            state.apply_position_snapshot(Position::empty(symbol), 1, Some(1));
        }
        let watched = symbols_in_dex(&state, None);
        assert_eq!(
            watched,
            BTreeSet::from(["BTC".to_string(), "ETH".to_string()])
        );

        apply_positions_for_watched(
            &mut state,
            &json!({"assetPositions": []}),
            200,
            190,
            None,
            &watched,
        )
        .unwrap();

        for symbol in watched {
            let position = state.position(&symbol).expect("watched flat position");
            assert!(position.value.flat());
            assert_eq!(position.local_ms, 200);
            assert_eq!(position.exchange_ms, Some(190));
        }
    }
}
