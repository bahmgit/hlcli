use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    command::{Chase, Command, Protection, ProtectionKind, Scale, Side as CommandSide, Tif, Trade},
    core::{CommandExecutor, ExecuteFuture, PlanExecutor, SwitchBlockFuture},
    execution::{ExecutionControl, ExecutionStatus, OrderStatus, SubmitReceipt},
    planner::{
        ActionPlan, Planner, resolve_scale_price_fields_for, resolve_scale_risk_size_for,
        resolve_trade_price_fields_for,
    },
    protocol::{
        Action, BatchModify, Cloid, MarketKind, Modify, OrderRequest, OrderTarget, OrderType, Side,
        TimeInForce, TpSl,
    },
    scope::ScopeRegistry,
    state::{FreshnessLimits, Market, Order, OrderKind, TradingState},
};

pub struct ManagedExecutor {
    state: Arc<RwLock<TradingState>>,
    inner: Arc<dyn PlanExecutor>,
    transition_lock: Mutex<()>,
    chases: Mutex<Vec<ActiveChase>>,
    trailings: Mutex<Vec<ActiveTrailing>>,
    entry_protections: Mutex<Vec<EntryProtection>>,
    persistence: Option<ManagedPersistence>,
    persist_lock: Mutex<()>,
    wake: Notify,
    interval: Duration,
    local_ids: AtomicU64,
    scopes: Option<Arc<ScopeRegistry>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ActiveChase {
    symbol: String,
    is_buy: bool,
    cloid: Cloid,
    distance: ChaseDistance,
    tif: TimeInForce,
    next_move_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ActiveTrailing {
    symbol: String,
    cloid: Cloid,
    side: Side,
    distance: TrailDistance,
    trigger: Decimal,
    next_move_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EntryProtection {
    id: Cloid,
    symbol: String,
    intended_is_buy: bool,
    entry_orders: Vec<EntryOrder>,
    stop_loss: Option<ManagedProtectionLeg>,
    take_profit: Option<ManagedProtectionLeg>,
    trailing: Option<ManagedProtectionLeg>,
    #[serde(default)]
    entry_cancel_pending: bool,
    next_attempt_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EntryOrder {
    cloid: Cloid,
    oid: Option<u64>,
    receipt_filled: Decimal,
    feed_filled: Decimal,
    seen_fill_tids: std::collections::BTreeSet<u64>,
    terminal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManagedProtectionLeg {
    value: String,
    cloid: Option<Cloid>,
    size: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ChaseDistance {
    Quote,
    Absolute(Decimal),
    Percent(Decimal),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum TrailDistance {
    Absolute(Decimal),
    Percent(Decimal),
}

enum PollResult {
    Keep,
    Disarm,
    Throttle { cloid: Cloid, next_move_ms: u64 },
    Moved { cloid: Cloid, next_move_ms: u64 },
}

enum TrailPollResult {
    Keep,
    Disarm,
    Throttle {
        cloid: Cloid,
        next_move_ms: u64,
    },
    Moved {
        cloid: Cloid,
        trigger: Decimal,
        next_move_ms: u64,
    },
}

#[derive(Debug, Clone)]
struct ManagedPersistence {
    path: PathBuf,
    execution: ExecutionControl,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManagedSnapshot {
    version: u8,
    chases: Vec<ActiveChase>,
    trailings: Vec<ActiveTrailing>,
    entry_protections: Vec<EntryProtection>,
}

impl ManagedExecutor {
    pub fn spawn(
        state: Arc<RwLock<TradingState>>,
        inner: Arc<dyn PlanExecutor>,
        interval: Duration,
    ) -> Arc<Self> {
        Self::start(state, inner, interval, None, ManagedSnapshot::empty(), None)
    }

    pub async fn spawn_persisted(
        state: Arc<RwLock<TradingState>>,
        inner: Arc<dyn PlanExecutor>,
        interval: Duration,
        path: PathBuf,
        execution: ExecutionControl,
    ) -> anyhow::Result<Arc<Self>> {
        Self::spawn_persisted_with_scopes(state, inner, interval, path, execution, None).await
    }

    pub async fn spawn_persisted_with_scopes(
        state: Arc<RwLock<TradingState>>,
        inner: Arc<dyn PlanExecutor>,
        interval: Duration,
        path: PathBuf,
        execution: ExecutionControl,
        scopes: Option<Arc<ScopeRegistry>>,
    ) -> anyhow::Result<Arc<Self>> {
        let snapshot = ManagedSnapshot::load(&path)?;
        let symbols = snapshot.symbols();
        {
            let state = state.read().await;
            for symbol in &symbols {
                anyhow::ensure!(
                    state.markets.contains_key(symbol),
                    "managed state references unknown market {symbol}"
                );
            }
        }
        if let Some(scopes) = &scopes {
            scopes.set_managed(symbols).await;
        }
        Ok(Self::start(
            state,
            inner,
            interval,
            Some(ManagedPersistence { path, execution }),
            snapshot,
            scopes,
        ))
    }

    fn start(
        state: Arc<RwLock<TradingState>>,
        inner: Arc<dyn PlanExecutor>,
        interval: Duration,
        persistence: Option<ManagedPersistence>,
        snapshot: ManagedSnapshot,
        scopes: Option<Arc<ScopeRegistry>>,
    ) -> Arc<Self> {
        let executor = Arc::new(Self {
            state,
            inner,
            transition_lock: Mutex::new(()),
            chases: Mutex::new(snapshot.chases),
            trailings: Mutex::new(snapshot.trailings),
            entry_protections: Mutex::new(snapshot.entry_protections),
            persistence,
            persist_lock: Mutex::new(()),
            wake: Notify::new(),
            interval,
            local_ids: AtomicU64::new(1_000_000_000),
            scopes,
        });
        tokio::spawn(executor.clone().run());
        executor
    }

    async fn persist(&self) -> Result<(), String> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        let _guard = self.persist_lock.lock().await;
        let snapshot = ManagedSnapshot {
            version: 2,
            chases: self.chases.lock().await.clone(),
            trailings: self.trailings.lock().await.clone(),
            entry_protections: self.entry_protections.lock().await.clone(),
        };
        let data = serde_json::to_vec_pretty(&snapshot).map_err(|err| err.to_string())?;
        let path = persistence.path.clone();
        let result = tokio::task::spawn_blocking(move || write_snapshot(&path, &data))
            .await
            .map_err(|err| format!("managed-state persistence task failed: {err}"))?
            .map_err(|err| format!("persist managed state: {err:#}"));
        if let Err(err) = &result {
            persistence.execution.halt(err.clone()).await;
        } else if let Some(scopes) = &self.scopes {
            scopes.set_managed(snapshot.symbols()).await;
        }
        result
    }

    async fn run(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.interval) => {}
                _ = self.wake.notified() => {}
            }
            self.poll_chases().await;
            self.poll_entry_protections().await;
            self.poll_trailings().await;
        }
    }

    async fn start_trade_chase(&self, symbol: &str, trade: Trade) -> Result<SubmitReceipt, String> {
        if trade.size.starts_with('r') || trade.size.starts_with('R') {
            return Err("Risk sizing is not supported with chase entries".to_string());
        }
        if trade.price.is_some() {
            return Err("chase cannot be combined with at <price>".to_string());
        }
        let trade = {
            let state = self.state.read().await;
            let (trade, _) = resolve_trade_price_fields_for(&state, symbol, trade)?;
            require_perp_entry_protection_for(
                &state,
                symbol,
                trade.stop_loss.is_some()
                    || trade.take_profit.is_some()
                    || trade.trailing.is_some(),
            )?;
            if trade.stop_loss.is_some() || trade.take_profit.is_some() || trade.trailing.is_some()
            {
                require_flat_protected_entry(&state, symbol)?;
            }
            trade
        };
        let distance = trade
            .chase
            .clone()
            .ok_or_else(|| "missing chase distance".to_string())?;
        let chase = Chase {
            side: trade.side.clone(),
            size: trade.size.clone(),
            distance,
            reduce_only: trade.reduce_only,
            tif: trade.tif.clone(),
            post_only: trade.post_only,
        };
        if trade.stop_loss.is_none() && trade.take_profit.is_none() && trade.trailing.is_none() {
            return self.start_chase(symbol, chase).await;
        }
        let (plan, active) = self.prepare_chase(symbol, chase).await?;
        let protection = trade_entry_protection(symbol, &trade, &plan)?;
        let cloid = active.cloid;
        self.stage_chase_protection(active, protection.clone())
            .await?;
        let receipt = match self.inner.execute_plan(plan).await {
            Ok(receipt) => receipt,
            Err(err) => {
                self.chases.lock().await.retain(|item| item.cloid != cloid);
                self.remove_pending_protection(protection.id).await?;
                return Err(err);
            }
        };
        if receipt.status == ExecutionStatus::Rejected
            || (accepted(&receipt) && resting_oid(&receipt).is_none())
        {
            self.chases.lock().await.retain(|item| item.cloid != cloid);
        }
        self.after_entry(receipt, protection).await
    }

    async fn start_trade_with_entry_protection(
        &self,
        symbol: &str,
        trade: Trade,
    ) -> Result<SubmitReceipt, String> {
        if (trade.size.starts_with('r') || trade.size.starts_with('R')) && trade.stop_loss.is_none()
        {
            return Err(
                "Risk sizing requires a fixed stop-loss (sl/stop), not trailing".to_string(),
            );
        }
        let (trade, plan) = {
            let state = self.state.read().await;
            let (trade, _) = resolve_trade_price_fields_for(&state, symbol, trade)?;
            require_perp_entry_protection_for(&state, symbol, true)?;
            require_flat_protected_entry(&state, symbol)?;
            let mut validation_trade = trade.clone();
            validation_trade.trailing = None;
            let validated =
                Planner::default().plan_for(&state, symbol, Command::Trade(validation_trade))?;
            let Action::Order(validated) = validated.action else {
                return Err("protected trade did not produce an order batch".to_string());
            };
            let resolved_size = validated
                .orders
                .first()
                .ok_or_else(|| "protected trade produced an empty order batch".to_string())?
                .size;
            let entry = Trade {
                side: trade.side.clone(),
                size: resolved_size.to_string(),
                price: trade.price.clone(),
                reduce_only: trade.reduce_only,
                stop_loss: None,
                take_profit: None,
                trailing: None,
                chase: None,
                tif: trade.tif.clone(),
                post_only: trade.post_only,
            };
            let plan = Planner::default().plan_for(&state, symbol, Command::Trade(entry))?;
            (trade, plan)
        };
        let protection = trade_entry_protection(symbol, &trade, &plan)?;
        self.store_pending_protection(protection.clone()).await?;
        let receipt = match self.inner.execute_plan(plan).await {
            Ok(receipt) => receipt,
            Err(err) => {
                self.remove_pending_protection(protection.id).await?;
                return Err(err);
            }
        };
        self.after_entry(receipt, protection).await
    }

    async fn start_scale_with_entry_protection(
        &self,
        symbol: &str,
        scale: Scale,
    ) -> Result<SubmitReceipt, String> {
        let (scale, plan) = {
            let state = self.state.read().await;
            let (scale, _) = resolve_scale_price_fields_for(&state, symbol, scale)?;
            require_perp_entry_protection_for(&state, symbol, true)?;
            require_flat_protected_entry(&state, symbol)?;
            let resolved_size = resolve_scale_risk_size_for(&state, symbol, &scale)?;
            let entry = Scale {
                side: scale.side.clone(),
                size: resolved_size.unwrap_or_else(|| scale.size.clone()),
                legs: scale.legs,
                start_price: scale.start_price.clone(),
                end_price: scale.end_price.clone(),
                reduce_only: scale.reduce_only,
                stop_loss: None,
                take_profit: None,
                trailing: None,
                tif: scale.tif.clone(),
                post_only: scale.post_only,
            };
            let plan = Planner::default().plan_for(&state, symbol, Command::Scale(entry))?;
            (scale, plan)
        };
        let protection = scale_entry_protection(symbol, &scale, &plan)?;
        self.store_pending_protection(protection.clone()).await?;
        let receipt = match self.inner.execute_plan(plan).await {
            Ok(receipt) => receipt,
            Err(err) => {
                self.remove_pending_protection(protection.id).await?;
                return Err(err);
            }
        };
        self.after_entry(receipt, protection).await
    }

    async fn start_chase(&self, symbol: &str, chase: Chase) -> Result<SubmitReceipt, String> {
        let (plan, active) = self.prepare_chase(symbol, chase).await?;
        let cloid = active.cloid;
        self.chases.lock().await.push(active);
        if let Err(err) = self.persist().await {
            self.chases.lock().await.retain(|item| item.cloid != cloid);
            return Err(err);
        }
        let receipt = match self.inner.execute_plan(plan).await {
            Ok(receipt) => receipt,
            Err(err) => {
                self.chases.lock().await.retain(|item| item.cloid != cloid);
                self.persist().await?;
                return Err(err);
            }
        };
        if receipt.status == ExecutionStatus::Rejected
            || (accepted(&receipt) && resting_oid(&receipt).is_none())
        {
            self.chases.lock().await.retain(|item| item.cloid != cloid);
            self.persist().await?;
        } else {
            self.wake.notify_one();
        }
        Ok(receipt)
    }

    async fn prepare_chase(
        &self,
        symbol: &str,
        chase: Chase,
    ) -> Result<(ActionPlan, ActiveChase), String> {
        let tif = command_tif(chase.tif.clone(), chase.post_only);
        if tif == TimeInForce::Ioc {
            return Err("chase supports only ALO/GTC".to_string());
        }
        let distance = parse_distance(&chase.distance)?;
        let is_buy = chase.side == CommandSide::Buy;
        let (plan, cloid) = {
            let state = self.state.read().await;
            let plan = Planner::default().plan_for(&state, symbol, Command::ChasePlace(chase))?;
            let Action::Order(batch) = &plan.action else {
                return Err("chase plan did not produce an order".to_string());
            };
            let cloid = batch
                .orders
                .first()
                .ok_or_else(|| "chase plan missing order".to_string())?
                .cloid;
            (plan, cloid)
        };
        if self
            .chases
            .lock()
            .await
            .iter()
            .any(|active| active.symbol == symbol && active.is_buy == is_buy)
        {
            return Err(format!(
                "a {} chase is already active; cancel it explicitly with chase cancel before starting a replacement",
                if is_buy { "buy" } else { "sell" }
            ));
        }

        Ok((
            plan,
            ActiveChase {
                symbol: symbol.to_string(),
                is_buy,
                cloid,
                distance,
                tif,
                next_move_ms: next_chase_move_ms(self.interval),
            },
        ))
    }

    async fn cancel_chases(&self, symbol: &str) -> Result<SubmitReceipt, String> {
        let ids = self
            .chases
            .lock()
            .await
            .iter()
            .filter(|chase| chase.symbol == symbol)
            .map(|chase| chase.cloid.to_string())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(self.local_receipt());
        }
        let receipt = self
            .inner
            .execute_command_for(symbol, Command::CancelCloid { ids: ids.clone() })
            .await?;
        if accepted(&receipt) {
            self.chases
                .lock()
                .await
                .retain(|chase| !ids.contains(&chase.cloid.to_string()));
            self.clear_all_pending(symbol).await;
            self.persist().await?;
        }
        Ok(receipt)
    }

    async fn start_trailing(
        &self,
        symbol: &str,
        protection: Protection,
    ) -> Result<SubmitReceipt, String> {
        let distance = parse_trail_distance(&protection.value)?;
        let (plan, active) = {
            let state = self.state.read().await;
            if self
                .trailings
                .lock()
                .await
                .iter()
                .any(|active| active.symbol == symbol)
            {
                return Err("trailing stop already active; cancel it before replacing".to_string());
            }
            let plan =
                Planner::default().plan_for(&state, symbol, Command::ProtectionSet(protection))?;
            let active = active_trailing_from_plan(symbol, &plan, distance, self.interval)?;
            (plan, active)
        };
        let cloid = active.cloid;
        self.trailings.lock().await.push(active);
        if let Err(err) = self.persist().await {
            self.trailings
                .lock()
                .await
                .retain(|item| item.cloid != cloid);
            return Err(err);
        }
        let receipt = match self.inner.execute_plan(plan).await {
            Ok(receipt) => receipt,
            Err(err) => {
                self.trailings
                    .lock()
                    .await
                    .retain(|item| item.cloid != cloid);
                self.persist().await?;
                return Err(err);
            }
        };
        if receipt.status == ExecutionStatus::Rejected {
            self.trailings
                .lock()
                .await
                .retain(|item| item.cloid != cloid);
            self.persist().await?;
        } else if accepted(&receipt) {
            self.wake.notify_one();
        }
        Ok(receipt)
    }

    async fn cancel_protection(
        &self,
        symbol: &str,
        kind: ProtectionKind,
    ) -> Result<SubmitReceipt, String> {
        let cleared_pending = self.clear_pending_protection(symbol, &kind).await?;
        let trailing_cloids = self
            .trailings
            .lock()
            .await
            .iter()
            .filter(|trailing| trailing.symbol == symbol)
            .map(|trailing| trailing.cloid.to_string())
            .collect::<std::collections::BTreeSet<_>>();
        let (cloids, oids) = {
            let state = self.state.read().await;
            let target = match kind {
                ProtectionKind::StopLoss | ProtectionKind::TrailingStop => OrderKind::StopLoss,
                ProtectionKind::TakeProfit => OrderKind::TakeProfit,
            };
            let orders = state
                .orders_for(symbol)
                .filter(|order| order.value.kind == target)
                .filter(|order| match kind {
                    ProtectionKind::TrailingStop => order
                        .value
                        .cloid
                        .as_ref()
                        .is_some_and(|cloid| trailing_cloids.contains(cloid)),
                    ProtectionKind::StopLoss => order
                        .value
                        .cloid
                        .as_ref()
                        .is_none_or(|cloid| !trailing_cloids.contains(cloid)),
                    ProtectionKind::TakeProfit => true,
                });
            let mut cloids = Vec::new();
            let mut oids = Vec::new();
            for order in orders {
                match &order.value.cloid {
                    Some(cloid) => cloids.push(cloid.clone()),
                    None => oids.push(order.value.oid),
                }
            }
            (cloids, oids)
        };
        if cloids.is_empty() && oids.is_empty() {
            if cleared_pending {
                return Ok(self.local_receipt());
            }
            return Err("no matching protection orders".to_string());
        }
        let receipt = self.cancel_targets(symbol, cloids, oids).await?;
        if accepted(&receipt) && kind == ProtectionKind::TrailingStop {
            self.trailings
                .lock()
                .await
                .retain(|trailing| trailing.symbol != symbol);
            self.persist().await?;
        }
        Ok(receipt)
    }

    async fn cancel_targets(
        &self,
        symbol: &str,
        cloids: Vec<String>,
        oids: Vec<u64>,
    ) -> Result<SubmitReceipt, String> {
        let mut statuses = Vec::new();
        for command in [
            (!cloids.is_empty()).then_some(Command::CancelCloid { ids: cloids }),
            (!oids.is_empty()).then_some(Command::CancelOid { ids: oids }),
        ]
        .into_iter()
        .flatten()
        {
            let receipt = self.inner.execute_command_for(symbol, command).await?;
            statuses.extend(receipt.statuses.clone());
            if !accepted(&receipt) {
                return Ok(self.aggregate_protection_receipt(&receipt, statuses));
            }
        }
        Ok(self.aggregate_receipt(ExecutionStatus::Accepted, statuses))
    }

    async fn after_entry(
        &self,
        receipt: SubmitReceipt,
        mut protection: EntryProtection,
    ) -> Result<SubmitReceipt, String> {
        protection.apply_receipt(&receipt)?;
        if protection.filled_size() == Decimal::ZERO
            && protection.entry_orders.iter().all(|order| order.terminal)
        {
            self.remove_pending_protection(protection.id).await?;
            return Ok(receipt);
        }
        self.sync_pending_protection(&protection).await?;
        match self.reconcile_entry_protection(&mut protection).await {
            Ok(Some(armed)) => Ok(merge_receipts(receipt, armed)),
            Ok(None) => Ok(receipt),
            Err(err) => Err(format!(
                "entry accepted but protection reconciliation failed: {err}"
            )),
        }
    }

    async fn store_pending_protection(&self, protection: EntryProtection) -> Result<(), String> {
        debug_assert!(!protection.is_empty());
        if self
            .entry_protections
            .lock()
            .await
            .iter()
            .any(|item| item.symbol == protection.symbol && item.id != protection.id)
        {
            return Err(format!(
                "a protected entry is already active for {}; wait for it to finish or cancel its protection",
                protection.symbol
            ));
        }
        self.sync_pending_protection(&protection).await?;
        self.wake.notify_one();
        Ok(())
    }

    async fn stage_chase_protection(
        &self,
        active: ActiveChase,
        protection: EntryProtection,
    ) -> Result<(), String> {
        let cloid = active.cloid;
        if self
            .entry_protections
            .lock()
            .await
            .iter()
            .any(|item| item.symbol == protection.symbol)
        {
            return Err(format!(
                "a protected entry is already active for {}",
                protection.symbol
            ));
        }
        self.chases.lock().await.push(active);
        self.entry_protections.lock().await.push(protection.clone());
        if let Err(err) = self.persist().await {
            self.chases.lock().await.retain(|item| item.cloid != cloid);
            self.entry_protections
                .lock()
                .await
                .retain(|item| item.id != protection.id);
            return Err(err);
        }
        self.wake.notify_one();
        Ok(())
    }

    async fn sync_pending_protection(&self, protection: &EntryProtection) -> Result<(), String> {
        {
            let mut pending = self.entry_protections.lock().await;
            pending.retain(|item| item.id != protection.id);
            if !protection.is_empty() {
                pending.push(protection.clone());
            }
        }
        self.persist().await?;
        Ok(())
    }

    async fn clear_pending_protection(
        &self,
        symbol: &str,
        kind: &ProtectionKind,
    ) -> Result<bool, String> {
        let mut cleared = false;
        {
            let mut pending = self.entry_protections.lock().await;
            for protection in pending.iter_mut().filter(|item| item.symbol == symbol) {
                let was_cleared = match kind {
                    ProtectionKind::StopLoss => protection.stop_loss.take().is_some(),
                    ProtectionKind::TakeProfit => protection.take_profit.take().is_some(),
                    ProtectionKind::TrailingStop => protection.trailing.take().is_some(),
                };
                if was_cleared {
                    protection.next_attempt_ms = 0;
                }
                cleared |= was_cleared;
            }
            pending.retain(|protection| !protection.is_empty());
        }
        if cleared {
            self.persist().await?;
        }
        Ok(cleared)
    }

    async fn clear_all_pending(&self, symbol: &str) {
        self.entry_protections
            .lock()
            .await
            .retain(|protection| protection.symbol != symbol);
    }

    async fn remove_pending_protection(&self, id: Cloid) -> Result<(), String> {
        self.entry_protections
            .lock()
            .await
            .retain(|protection| protection.id != id);
        self.persist().await
    }

    async fn reconcile_entry_protection(
        &self,
        protection: &mut EntryProtection,
    ) -> Result<Option<SubmitReceipt>, String> {
        let (target_size, entry_complete, protection_gone) =
            self.refresh_entry_tracking(protection).await?;
        self.sync_pending_protection(protection).await?;
        if protection_gone {
            return self.terminate_protected_entry(protection).await;
        }
        if target_size == Decimal::ZERO {
            if !entry_complete && protection.filled_size() > Decimal::ZERO {
                return self.terminate_protected_entry(protection).await;
            } else if entry_complete {
                self.remove_pending_protection(protection.id).await?;
            }
            return Ok(None);
        }
        let mut statuses = Vec::new();
        for kind in [
            ProtectionKind::StopLoss,
            ProtectionKind::TakeProfit,
            ProtectionKind::TrailingStop,
        ] {
            let Some(leg) = protection.leg(&kind).cloned() else {
                continue;
            };
            let receipt = if let Some(cloid) = leg.cloid {
                if leg.size == target_size {
                    continue;
                }
                self.inner
                    .execute_command_for(
                        &protection.symbol,
                        Command::BatchResizeCloid {
                            ids: vec![cloid.to_string()],
                            size: target_size.to_string(),
                        },
                    )
                    .await?
            } else if kind == ProtectionKind::TrailingStop {
                self.start_trailing(
                    &protection.symbol,
                    Protection {
                        kind: kind.clone(),
                        value: leg.value.clone(),
                        size: Some(target_size.to_string()),
                    },
                )
                .await?
            } else {
                let (plan, cloid) = self
                    .plan_direct_protection(
                        &protection.symbol,
                        kind.clone(),
                        leg.value.clone(),
                        target_size,
                    )
                    .await?;
                protection.leg_mut(&kind).expect("leg exists").cloid = Some(cloid);
                self.sync_pending_protection(protection).await?;
                self.inner.execute_plan(plan).await?
            };
            statuses.extend(receipt.statuses.clone());
            if !accepted(&receipt) {
                protection.next_attempt_ms = u64::MAX;
                self.sync_pending_protection(protection).await?;
                if let Some(cancel) = self.cancel_remaining_entry_orders(protection).await? {
                    statuses.extend(cancel.statuses);
                }
                return Ok(Some(self.aggregate_protection_receipt(&receipt, statuses)));
            }
            if kind == ProtectionKind::TrailingStop && leg.cloid.is_none() {
                let cloid = self
                    .trailings
                    .lock()
                    .await
                    .iter()
                    .find(|trailing| trailing.symbol == protection.symbol)
                    .map(|trailing| trailing.cloid)
                    .ok_or_else(|| {
                        "managed trailing was accepted without active state".to_string()
                    })?;
                protection.leg_mut(&kind).expect("leg exists").cloid = Some(cloid);
            }
            protection.leg_mut(&kind).expect("leg exists").size = target_size;
            self.sync_pending_protection(protection).await?;
        }
        if entry_complete {
            self.remove_pending_protection(protection.id).await?;
        }
        Ok((!statuses.is_empty())
            .then(|| self.aggregate_receipt(ExecutionStatus::Accepted, statuses)))
    }

    async fn cancel_remaining_entry_orders(
        &self,
        protection: &mut EntryProtection,
    ) -> Result<Option<SubmitReceipt>, String> {
        let entry_ids = protection
            .entry_orders
            .iter()
            .filter(|entry| !entry.terminal)
            .map(|entry| entry.cloid.to_string())
            .collect::<Vec<_>>();
        if entry_ids.is_empty() {
            protection.entry_cancel_pending = false;
            self.sync_pending_protection(protection).await?;
            return Ok(None);
        }
        let next_after_cancel = protection.next_attempt_ms;
        protection.entry_cancel_pending = true;
        protection.next_attempt_ms = next_entry_cancel_ms(self.interval);
        self.sync_pending_protection(protection).await?;
        let receipt = self
            .inner
            .execute_command_for(&protection.symbol, Command::CancelCloid { ids: entry_ids })
            .await?;
        if accepted(&receipt) {
            for entry in &mut protection.entry_orders {
                entry.terminal = true;
            }
            protection.entry_cancel_pending = false;
            protection.next_attempt_ms = next_after_cancel;
        }
        self.sync_pending_protection(protection).await?;
        Ok(Some(receipt))
    }

    async fn terminate_protected_entry(
        &self,
        protection: &EntryProtection,
    ) -> Result<Option<SubmitReceipt>, String> {
        let mut ids = protection
            .entry_orders
            .iter()
            .filter(|entry| !entry.terminal)
            .map(|entry| entry.cloid.to_string())
            .chain(
                [
                    protection.stop_loss.as_ref(),
                    protection.take_profit.as_ref(),
                    protection.trailing.as_ref(),
                ]
                .into_iter()
                .flatten()
                .filter_map(|leg| leg.cloid.map(|cloid| cloid.to_string())),
            )
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        let receipt = if ids.is_empty() {
            None
        } else {
            let receipt = self
                .inner
                .execute_command_for(
                    &protection.symbol,
                    Command::CancelCloid { ids: ids.clone() },
                )
                .await?;
            if !accepted(&receipt) {
                return Ok(Some(receipt));
            }
            Some(receipt)
        };
        self.trailings
            .lock()
            .await
            .retain(|trailing| !ids.contains(&trailing.cloid.to_string()));
        self.remove_pending_protection(protection.id).await?;
        Ok(receipt)
    }

    async fn refresh_entry_tracking(
        &self,
        protection: &mut EntryProtection,
    ) -> Result<(Decimal, bool, bool), String> {
        let state = self.state.read().await;
        protection.recover_entry_outcomes(&state);
        protection.observe_fills(&state.recent_fills);
        let orders_ready = state.orders_ready(
            &protection.symbol,
            now_ms(),
            FreshnessLimits::default().orders_ms,
        );
        for entry in &mut protection.entry_orders {
            if entry.terminal {
                continue;
            }
            let cloid = entry.cloid.to_string();
            let live = state.orders_for(&protection.symbol).any(|order| {
                entry.oid == Some(order.value.oid)
                    || order.value.cloid.as_deref() == Some(cloid.as_str())
            });
            if !live && orders_ready && entry.oid.is_some() {
                entry.terminal = true;
            }
        }
        let filled = protection.filled_size();
        let entry_complete = protection.entry_orders.iter().all(|entry| entry.terminal);
        let mut protection_gone = false;
        if orders_ready {
            for leg in [
                protection.stop_loss.as_mut(),
                protection.take_profit.as_mut(),
                protection.trailing.as_mut(),
            ]
            .into_iter()
            .flatten()
            {
                let Some(cloid) = leg.cloid else {
                    continue;
                };
                let cloid = cloid.to_string();
                if !state
                    .orders_for(&protection.symbol)
                    .any(|order| order.value.cloid.as_deref() == Some(cloid.as_str()))
                {
                    protection_gone = true;
                }
            }
        }
        let Some(position) = state.position(&protection.symbol) else {
            return Ok((Decimal::ZERO, entry_complete, protection_gone));
        };
        if position.value.flat() {
            return Ok((Decimal::ZERO, entry_complete, protection_gone));
        }
        if (position.value.size > Decimal::ZERO) != protection.intended_is_buy {
            return Err(format!(
                "protected entry position side changed unexpectedly for {}",
                protection.symbol
            ));
        }
        Ok((
            filled.min(position.value.size.abs()),
            entry_complete,
            protection_gone,
        ))
    }

    async fn plan_direct_protection(
        &self,
        symbol: &str,
        kind: ProtectionKind,
        value: String,
        size: Decimal,
    ) -> Result<(ActionPlan, Cloid), String> {
        let state = self.state.read().await;
        let plan = Planner::default().plan_for(
            &state,
            symbol,
            Command::ProtectionSet(Protection {
                kind,
                value,
                size: Some(size.to_string()),
            }),
        )?;
        let Action::Order(batch) = &plan.action else {
            return Err("protection plan did not produce an order".to_string());
        };
        let cloid = batch
            .orders
            .first()
            .ok_or_else(|| "protection plan missing order".to_string())?
            .cloid;
        Ok((plan, cloid))
    }

    async fn cancel_twaps(&self, symbol: &str, id: Option<u64>) -> Result<SubmitReceipt, String> {
        let Some(id) = id else {
            return self.cancel_all_twaps(symbol).await;
        };
        self.inner
            .execute_command_for(symbol, Command::TwapCancel { id: Some(id) })
            .await
    }

    async fn cancel_all_twaps(&self, symbol: &str) -> Result<SubmitReceipt, String> {
        let ids = self
            .state
            .read()
            .await
            .twaps_for(symbol)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(self.local_receipt());
        }

        let mut statuses = Vec::new();
        let mut errors = Vec::new();
        for id in ids {
            let receipt = self
                .inner
                .execute_command_for(symbol, Command::TwapCancel { id: Some(id) })
                .await?;
            statuses.extend(receipt.statuses.clone());
            if !accepted(&receipt) {
                errors.push(format!("id {id}: {}", receipt_error(&receipt)));
            }
        }
        Ok(SubmitReceipt {
            id: self.local_ids.fetch_add(1, Ordering::Relaxed),
            status: if errors.is_empty() {
                ExecutionStatus::Accepted
            } else {
                ExecutionStatus::Rejected
            },
            statuses,
            error: (!errors.is_empty()).then(|| errors.join("; ")),
        })
    }

    async fn poll_chases(&self) {
        let _transition = self.transition_lock.lock().await;
        let chases = self.chases.lock().await.clone();
        for chase in chases {
            let result = self.poll_chase(chase.clone()).await;
            let mut active = self.chases.lock().await;
            let changed = match result {
                PollResult::Keep => false,
                PollResult::Disarm => {
                    active.retain(|item| item.cloid != chase.cloid);
                    true
                }
                PollResult::Throttle {
                    cloid,
                    next_move_ms,
                } => {
                    if let Some(item) = active
                        .iter_mut()
                        .find(|item| item.symbol == chase.symbol && item.cloid == cloid)
                    {
                        item.next_move_ms = next_move_ms;
                    }
                    true
                }
                PollResult::Moved {
                    cloid,
                    next_move_ms,
                } => {
                    if let Some(item) = active
                        .iter_mut()
                        .find(|item| item.symbol == chase.symbol && item.cloid == cloid)
                    {
                        item.next_move_ms = next_move_ms;
                    }
                    true
                }
            };
            drop(active);
            if changed && let Err(err) = self.persist().await {
                eprintln!("{err}");
            }
        }
    }

    async fn poll_trailings(&self) {
        let _transition = self.transition_lock.lock().await;
        let trailings = self.trailings.lock().await.clone();
        for trailing in trailings {
            let result = self.poll_trailing(trailing.clone()).await;
            let mut active = self.trailings.lock().await;
            let changed = match result {
                TrailPollResult::Keep => false,
                TrailPollResult::Disarm => {
                    active.retain(|item| item.cloid != trailing.cloid);
                    true
                }
                TrailPollResult::Throttle {
                    cloid,
                    next_move_ms,
                } => {
                    if let Some(item) = active.iter_mut().find(|item| item.cloid == cloid) {
                        item.next_move_ms = next_move_ms;
                    }
                    true
                }
                TrailPollResult::Moved {
                    cloid,
                    trigger,
                    next_move_ms,
                } => {
                    if let Some(item) = active.iter_mut().find(|item| item.cloid == cloid) {
                        item.trigger = trigger;
                        item.next_move_ms = next_move_ms;
                    }
                    true
                }
            };
            drop(active);
            if changed && let Err(err) = self.persist().await {
                eprintln!("{err}");
            }
        }
    }

    async fn poll_entry_protections(&self) {
        let _transition = self.transition_lock.lock().await;
        let now = now_ms();
        let protections = self.entry_protections.lock().await.clone();
        for mut protection in protections {
            if protection.next_attempt_ms > now {
                continue;
            }
            if protection.entry_cancel_pending {
                match self.cancel_remaining_entry_orders(&mut protection).await {
                    Ok(Some(receipt)) if !accepted(&receipt) => eprintln!(
                        "managed entry cancellation rejected for {}: {}",
                        protection.symbol,
                        receipt_error(&receipt)
                    ),
                    Ok(_) => {}
                    Err(err) => eprintln!(
                        "managed entry cancellation failed for {}: {err}",
                        protection.symbol
                    ),
                }
                continue;
            }
            match self.reconcile_entry_protection(&mut protection).await {
                Ok(Some(receipt)) if !accepted(&receipt) => {
                    let message = format!(
                        "managed protection rejected for {}: {}",
                        protection.symbol,
                        receipt_error(&receipt)
                    );
                    eprintln!("{message}");
                }
                Ok(_) => {}
                Err(err) => {
                    if let Some(item) = self
                        .entry_protections
                        .lock()
                        .await
                        .iter_mut()
                        .find(|item| item.id == protection.id)
                    {
                        item.next_attempt_ms = now.saturating_add(5_000);
                    }
                    if let Err(err) = self.persist().await {
                        eprintln!("{err}");
                    }
                    eprintln!(
                        "managed protection reconciliation failed for {}: {err}",
                        protection.symbol
                    );
                }
            }
        }
    }

    async fn poll_trailing(&self, trailing: ActiveTrailing) -> TrailPollResult {
        let now = now_ms();
        if now < trailing.next_move_ms {
            return TrailPollResult::Keep;
        }
        let planned = {
            let state = self.state.read().await;
            let cloid = trailing.cloid.to_string();
            let Some(order) = state
                .orders_for(&trailing.symbol)
                .find(|order| order.value.cloid.as_deref() == Some(cloid.as_str()))
            else {
                return TrailPollResult::Disarm;
            };
            if order.value.kind != OrderKind::StopLoss {
                return TrailPollResult::Disarm;
            }
            let Some(market) = state.markets.get(&trailing.symbol) else {
                return TrailPollResult::Disarm;
            };
            let target = match trailing_target(&state, &trailing) {
                Ok(target) => target,
                Err(_) => return TrailPollResult::Keep,
            };
            if !trail_should_move(&trailing, target) {
                return TrailPollResult::Keep;
            }
            match trailing_modify_plan(&trailing, market, &order.value, target) {
                Ok(plan) => (plan, target),
                Err(_) => return TrailPollResult::Keep,
            }
        };
        match self.inner.execute_plan(planned.0).await {
            Ok(receipt) if accepted(&receipt) => TrailPollResult::Moved {
                cloid: trailing.cloid,
                trigger: planned.1,
                next_move_ms: next_trailing_move_ms(self.interval),
            },
            _ => TrailPollResult::Throttle {
                cloid: trailing.cloid,
                next_move_ms: next_trailing_move_ms(self.interval),
            },
        }
    }

    async fn poll_chase(&self, chase: ActiveChase) -> PollResult {
        let now = now_ms();
        if now < chase.next_move_ms {
            return PollResult::Keep;
        }
        let plan = {
            let state = self.state.read().await;
            let cloid = chase.cloid.to_string();
            let Some(order) = state
                .orders_for(&chase.symbol)
                .find(|order| order.value.cloid.as_deref() == Some(cloid.as_str()))
                .map(|order| &order.value)
            else {
                return PollResult::Disarm;
            };
            if order.kind != OrderKind::Limit {
                return PollResult::Disarm;
            }
            let Some(market) = state.markets.get(&chase.symbol) else {
                return PollResult::Disarm;
            };
            let target = match chase_target(&state, &chase) {
                Ok(target) => target,
                Err(_) => return PollResult::Keep,
            };
            if target == order.price {
                return PollResult::Keep;
            }
            match chase_modify_plan(&chase, market, order, target) {
                Ok(plan) => plan,
                Err(_) => return PollResult::Keep,
            }
        };

        match self.inner.execute_plan(plan).await {
            Ok(receipt) if accepted(&receipt) => PollResult::Moved {
                cloid: chase.cloid,
                next_move_ms: next_chase_move_ms(self.interval),
            },
            _ => PollResult::Throttle {
                cloid: chase.cloid,
                next_move_ms: next_chase_move_ms(self.interval),
            },
        }
    }

    fn local_receipt(&self) -> SubmitReceipt {
        self.aggregate_receipt(ExecutionStatus::Accepted, Vec::new())
    }

    fn aggregate_receipt(
        &self,
        status: ExecutionStatus,
        statuses: Vec<OrderStatus>,
    ) -> SubmitReceipt {
        SubmitReceipt {
            id: self.local_ids.fetch_add(1, Ordering::Relaxed),
            error: (status != ExecutionStatus::Accepted)
                .then(|| "entry protection rejected".to_string()),
            status,
            statuses,
        }
    }

    fn aggregate_protection_receipt(
        &self,
        receipt: &SubmitReceipt,
        statuses: Vec<OrderStatus>,
    ) -> SubmitReceipt {
        SubmitReceipt {
            id: self.local_ids.fetch_add(1, Ordering::Relaxed),
            status: receipt.status.clone(),
            statuses,
            error: receipt.error.clone(),
        }
    }
}

impl CommandExecutor for ManagedExecutor {
    fn execute_command<'a>(&'a self, command: Command) -> ExecuteFuture<'a> {
        Box::pin(async move {
            let symbol = self.state.read().await.active.clone();
            self.execute_command_for(&symbol, command).await
        })
    }

    fn execute_command_for<'a>(&'a self, symbol: &'a str, command: Command) -> ExecuteFuture<'a> {
        Box::pin(async move {
            let _transition = self.transition_lock.lock().await;
            match command {
                Command::ChasePlace(chase) => self.start_chase(symbol, chase).await,
                Command::ChaseCancel => self.cancel_chases(symbol).await,
                Command::ProtectionSet(protection)
                    if protection.kind == ProtectionKind::TrailingStop =>
                {
                    self.start_trailing(symbol, protection).await
                }
                Command::ProtectionCancel { kind } => self.cancel_protection(symbol, kind).await,
                Command::Trade(trade) if trade.chase.is_some() => {
                    self.start_trade_chase(symbol, trade).await
                }
                Command::Trade(trade)
                    if trade.stop_loss.is_some()
                        || trade.take_profit.is_some()
                        || trade.trailing.is_some() =>
                {
                    self.start_trade_with_entry_protection(symbol, trade).await
                }
                Command::Scale(scale)
                    if scale.stop_loss.is_some()
                        || scale.take_profit.is_some()
                        || scale.trailing.is_some() =>
                {
                    self.start_scale_with_entry_protection(symbol, scale).await
                }
                Command::TwapCancel { id } => self.cancel_twaps(symbol, id).await,
                other => self.inner.execute_command_for(symbol, other).await,
            }
        })
    }

    fn switch_blocker<'a>(&'a self, symbol: &'a str) -> SwitchBlockFuture<'a> {
        Box::pin(async move {
            if self
                .chases
                .lock()
                .await
                .iter()
                .any(|active| active.symbol == symbol)
            {
                return Some("managed chase active".to_string());
            }
            if self
                .trailings
                .lock()
                .await
                .iter()
                .any(|active| active.symbol == symbol)
            {
                return Some("managed trailing stop active".to_string());
            }
            if self
                .entry_protections
                .lock()
                .await
                .iter()
                .any(|active| active.symbol == symbol)
            {
                return Some("deferred entry protection active".to_string());
            }
            None
        })
    }
}

fn command_tif(tif: Option<Tif>, post_only: bool) -> TimeInForce {
    match tif {
        Some(Tif::Alo) => TimeInForce::Alo,
        Some(Tif::Ioc) => TimeInForce::Ioc,
        Some(Tif::Gtc) | None if post_only => TimeInForce::Alo,
        Some(Tif::Gtc) | None => TimeInForce::Gtc,
    }
}

fn accepted(receipt: &SubmitReceipt) -> bool {
    receipt.status == ExecutionStatus::Accepted
        && receipt.error.is_none()
        && receipt
            .statuses
            .iter()
            .all(|status| status.error().is_none())
}

fn resting_oid(receipt: &SubmitReceipt) -> Option<u64> {
    receipt.statuses.iter().find_map(|status| match status {
        OrderStatus::Resting { oid, .. } => Some(*oid),
        _ => None,
    })
}

fn receipt_error(receipt: &SubmitReceipt) -> String {
    receipt
        .error
        .clone()
        .or_else(|| {
            receipt
                .statuses
                .iter()
                .find_map(OrderStatus::error)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| format!("{:?}", receipt.status))
}

impl EntryProtection {
    fn is_empty(&self) -> bool {
        self.stop_loss.is_none() && self.take_profit.is_none() && self.trailing.is_none()
    }

    fn leg(&self, kind: &ProtectionKind) -> Option<&ManagedProtectionLeg> {
        match kind {
            ProtectionKind::StopLoss => self.stop_loss.as_ref(),
            ProtectionKind::TakeProfit => self.take_profit.as_ref(),
            ProtectionKind::TrailingStop => self.trailing.as_ref(),
        }
    }

    fn leg_mut(&mut self, kind: &ProtectionKind) -> Option<&mut ManagedProtectionLeg> {
        match kind {
            ProtectionKind::StopLoss => self.stop_loss.as_mut(),
            ProtectionKind::TakeProfit => self.take_profit.as_mut(),
            ProtectionKind::TrailingStop => self.trailing.as_mut(),
        }
    }

    fn filled_size(&self) -> Decimal {
        self.entry_orders
            .iter()
            .map(|entry| entry.receipt_filled.max(entry.feed_filled))
            .sum::<Decimal>()
            .normalize()
    }

    fn apply_receipt(&mut self, receipt: &SubmitReceipt) -> Result<(), String> {
        if receipt.status == ExecutionStatus::Rejected && receipt.statuses.is_empty() {
            for entry in &mut self.entry_orders {
                entry.terminal = true;
            }
            return Ok(());
        }
        if receipt.status == ExecutionStatus::Ambiguous && receipt.statuses.is_empty() {
            return Ok(());
        }
        if receipt.statuses.len() != self.entry_orders.len() {
            return Err(format!(
                "entry receipt status count mismatch: expected={} actual={}",
                self.entry_orders.len(),
                receipt.statuses.len()
            ));
        }
        for (entry, status) in self.entry_orders.iter_mut().zip(&receipt.statuses) {
            match status {
                OrderStatus::Resting { oid, .. } => entry.oid = Some(*oid),
                OrderStatus::Filled {
                    oid, total_size, ..
                } => {
                    entry.oid = Some(*oid);
                    entry.receipt_filled = total_size.parse::<Decimal>().map_err(|_| {
                        format!("invalid filled size in entry receipt: {total_size}")
                    })?;
                    entry.terminal = true;
                }
                OrderStatus::Error { .. } | OrderStatus::AlreadyTerminal { .. } => {
                    entry.terminal = true;
                }
                OrderStatus::WaitingForFill | OrderStatus::WaitingForTrigger => {}
                OrderStatus::Success | OrderStatus::TwapRunning { .. } => {
                    return Err(format!("unexpected protected entry status: {status:?}"));
                }
            }
        }
        Ok(())
    }

    fn recover_entry_outcomes(&mut self, state: &TradingState) {
        for entry in &mut self.entry_orders {
            let cloid = entry.cloid.to_string();
            if let Some(outcome) = state.recovered_order_outcomes.get(&cloid) {
                entry.oid = outcome.oid.or(entry.oid);
                entry.receipt_filled = entry.receipt_filled.max(outcome.filled_size);
                entry.terminal |= outcome.terminal;
            }
            if entry.oid.is_none()
                && let Some(order) = state
                    .orders_for(&self.symbol)
                    .find(|order| order.value.cloid.as_deref() == Some(cloid.as_str()))
            {
                entry.oid = Some(order.value.oid);
            }
        }
    }

    fn observe_fills(&mut self, fills: &std::collections::VecDeque<crate::state::Fill>) {
        for fill in fills {
            if fill.symbol != self.symbol || fill.is_buy != self.intended_is_buy {
                continue;
            }
            if let Some(entry) = self
                .entry_orders
                .iter_mut()
                .find(|entry| entry.oid == Some(fill.oid))
                && entry.seen_fill_tids.insert(fill.tid)
            {
                entry.feed_filled += fill.size;
            }
        }
    }
}

impl ManagedSnapshot {
    fn empty() -> Self {
        Self {
            version: 2,
            chases: Vec::new(),
            trailings: Vec::new(),
            entry_protections: Vec::new(),
        }
    }

    fn load(path: &Path) -> anyhow::Result<Self> {
        let text = match fs::read_to_string(path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Self::empty()),
            Err(err) => return Err(err.into()),
        };
        let mut value: serde_json::Value = serde_json::from_str(&text)?;
        if value.get("version").and_then(serde_json::Value::as_u64) == Some(1) {
            anyhow::ensure!(
                value
                    .get("entryProtections")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(Vec::is_empty),
                "managed-state v1 contains an active protected entry that cannot be identified safely; cancel its entry/protection orders before upgrading"
            );
            value["version"] = serde_json::Value::from(2);
        }
        let snapshot: Self = serde_json::from_value(value)?;
        anyhow::ensure!(snapshot.version == 2, "unsupported managed-state version");
        let mut chase_keys = std::collections::BTreeSet::new();
        for chase in &snapshot.chases {
            anyhow::ensure!(
                !chase.symbol.trim().is_empty(),
                "managed chase symbol is empty"
            );
            anyhow::ensure!(
                chase_keys.insert((chase.symbol.clone(), chase.is_buy)),
                "duplicate managed chase for {}",
                chase.symbol
            );
        }
        let mut trailing_symbols = std::collections::BTreeSet::new();
        for trailing in &snapshot.trailings {
            anyhow::ensure!(
                !trailing.symbol.trim().is_empty(),
                "managed trailing symbol is empty"
            );
            anyhow::ensure!(
                trailing_symbols.insert(trailing.symbol.clone()),
                "duplicate managed trailing for {}",
                trailing.symbol
            );
        }
        let mut protection_symbols = std::collections::BTreeSet::new();
        let mut protection_ids = std::collections::BTreeSet::new();
        for protection in &snapshot.entry_protections {
            anyhow::ensure!(
                !protection.symbol.trim().is_empty()
                    && !protection.is_empty()
                    && !protection.entry_orders.is_empty(),
                "invalid deferred entry protection"
            );
            anyhow::ensure!(
                protection_ids.insert(protection.id.to_string()),
                "duplicate protected entry id {}",
                protection.id
            );
            anyhow::ensure!(
                protection_symbols.insert(protection.symbol.clone()),
                "duplicate deferred entry protection for {}",
                protection.symbol
            );
        }
        Ok(snapshot)
    }

    fn symbols(&self) -> std::collections::BTreeSet<String> {
        self.chases
            .iter()
            .map(|item| item.symbol.clone())
            .chain(self.trailings.iter().map(|item| item.symbol.clone()))
            .chain(
                self.entry_protections
                    .iter()
                    .map(|item| item.symbol.clone()),
            )
            .collect()
    }
}

fn write_snapshot(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    static WRITE_SEQ: AtomicU64 = AtomicU64::new(1);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("managed-state path missing UTF-8 file name"))?;
    let temporary = path.with_file_name(format!(
        ".{name}.tmp-{}-{}",
        std::process::id(),
        WRITE_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| -> anyhow::Result<()> {
        let mut file = options.open(&temporary)?;
        file.write_all(data)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn trade_entry_protection(
    symbol: &str,
    trade: &Trade,
    plan: &ActionPlan,
) -> Result<EntryProtection, String> {
    if trade.reduce_only
        && (trade.stop_loss.is_some() || trade.take_profit.is_some() || trade.trailing.is_some())
    {
        return Err(
            "attached TP/SL and trailing are not supported with reduce-only entries".to_string(),
        );
    }
    if trade.trailing.is_some() && trade.stop_loss.is_some() {
        return Err("Specify only one stop type: sl/stop or trailing/tsl".to_string());
    }
    let Action::Order(batch) = &plan.action else {
        return Err("protected entry did not produce an order batch".to_string());
    };
    let entry = batch
        .orders
        .first()
        .ok_or_else(|| "protected entry produced an empty order batch".to_string())?;
    let stop = trade
        .stop_loss
        .as_deref()
        .map(|value| decimal("stop loss", value))
        .transpose()?;
    let take_profit = trade
        .take_profit
        .as_deref()
        .map(|value| decimal("take profit", value))
        .transpose()?;
    match trade.side {
        CommandSide::Buy => {
            if stop.is_some_and(|stop| stop >= entry.price) {
                return Err("invalid SL for buy: stop must be below the entry price".to_string());
            }
            if take_profit.is_some_and(|tp| tp <= entry.price) {
                return Err(
                    "invalid TP for buy: take profit must be above the entry price".to_string(),
                );
            }
        }
        CommandSide::Sell => {
            if stop.is_some_and(|stop| stop <= entry.price) {
                return Err("invalid SL for sell: stop must be above the entry price".to_string());
            }
            if take_profit.is_some_and(|tp| tp >= entry.price) {
                return Err(
                    "invalid TP for sell: take profit must be below the entry price".to_string(),
                );
            }
        }
    }
    let entry_orders = entry_orders(plan)?;
    Ok(EntryProtection {
        id: entry_orders[0].cloid,
        symbol: symbol.to_string(),
        intended_is_buy: trade.side == CommandSide::Buy,
        entry_orders,
        stop_loss: trade.stop_loss.clone().map(managed_leg),
        take_profit: trade.take_profit.clone().map(managed_leg),
        trailing: trade.trailing.clone().map(managed_leg),
        entry_cancel_pending: false,
        next_attempt_ms: 0,
    })
}

fn require_perp_entry_protection_for(
    state: &TradingState,
    symbol: &str,
    has_protection: bool,
) -> Result<(), String> {
    if has_protection
        && state
            .markets
            .get(symbol)
            .is_some_and(|market| market.kind == MarketKind::Spot)
    {
        return Err("spot orders do not support position protection or risk sizing".to_string());
    }
    Ok(())
}

fn require_flat_protected_entry(state: &TradingState, symbol: &str) -> Result<(), String> {
    match state.position(symbol) {
        Some(position) if position.age_ms(now_ms()) > FreshnessLimits::default().position_ms => {
            Err(format!(
                "fresh position required for protected entry on {symbol}"
            ))
        }
        Some(position) if !position.value.flat() => Err(format!(
            "attached entry protection requires a flat starting position on {symbol}; use explicit position protection for an existing position"
        )),
        Some(_) => Ok(()),
        None => Err(format!(
            "position unavailable for protected entry on {symbol}"
        )),
    }
}

fn scale_entry_protection(
    symbol: &str,
    scale: &Scale,
    plan: &ActionPlan,
) -> Result<EntryProtection, String> {
    if scale.reduce_only
        && (scale.stop_loss.is_some() || scale.take_profit.is_some() || scale.trailing.is_some())
    {
        return Err(
            "attached TP/SL and trailing are not supported with reduce-only entries".to_string(),
        );
    }
    if scale.trailing.is_some() && scale.stop_loss.is_some() {
        return Err("Specify only one stop type: sl/stop or trailing/tsl".to_string());
    }
    validate_scale_prices(scale)?;
    let entry_orders = entry_orders(plan)?;
    Ok(EntryProtection {
        id: entry_orders[0].cloid,
        symbol: symbol.to_string(),
        intended_is_buy: scale.side == CommandSide::Buy,
        entry_orders,
        stop_loss: scale.stop_loss.clone().map(managed_leg),
        take_profit: scale.take_profit.clone().map(managed_leg),
        trailing: scale.trailing.clone().map(managed_leg),
        entry_cancel_pending: false,
        next_attempt_ms: 0,
    })
}

fn managed_leg(value: String) -> ManagedProtectionLeg {
    ManagedProtectionLeg {
        value,
        cloid: None,
        size: Decimal::ZERO,
    }
}

fn entry_orders(plan: &ActionPlan) -> Result<Vec<EntryOrder>, String> {
    let Action::Order(batch) = &plan.action else {
        return Err("protected entry did not produce an order batch".to_string());
    };
    if batch.orders.is_empty() {
        return Err("protected entry produced an empty order batch".to_string());
    }
    Ok(batch
        .orders
        .iter()
        .map(|order| EntryOrder {
            cloid: order.cloid,
            oid: None,
            receipt_filled: Decimal::ZERO,
            feed_filled: Decimal::ZERO,
            seen_fill_tids: std::collections::BTreeSet::new(),
            terminal: false,
        })
        .collect())
}

fn validate_scale_prices(scale: &Scale) -> Result<(), String> {
    let start = decimal("start price", &scale.start_price)?;
    let end = decimal("end price", &scale.end_price)?;
    let low = start.min(end);
    let high = start.max(end);
    let stop = scale
        .stop_loss
        .as_deref()
        .map(|value| decimal("stop loss", value))
        .transpose()?;
    let take_profit = scale
        .take_profit
        .as_deref()
        .map(|value| decimal("take profit", value))
        .transpose()?;
    match scale.side {
        CommandSide::Buy => {
            if let Some(stop) = stop
                && stop >= low
            {
                return Err(
                    "Invalid SL for scale buy: sl must be below the lowest entry".to_string(),
                );
            }
            if let Some(take_profit) = take_profit
                && take_profit <= high
            {
                return Err(
                    "Invalid TP for scale buy: tp must be above the highest entry".to_string(),
                );
            }
            if let (Some(stop), Some(take_profit)) = (stop, take_profit)
                && stop >= take_profit
            {
                return Err("Invalid TP/SL: expected sl < tp".to_string());
            }
        }
        CommandSide::Sell => {
            if let Some(stop) = stop
                && stop <= high
            {
                return Err(
                    "Invalid SL for scale sell: sl must be above the highest entry".to_string(),
                );
            }
            if let Some(take_profit) = take_profit
                && take_profit >= low
            {
                return Err(
                    "Invalid TP for scale sell: tp must be below the lowest entry".to_string(),
                );
            }
            if let (Some(stop), Some(take_profit)) = (stop, take_profit)
                && take_profit >= stop
            {
                return Err("Invalid TP/SL: expected tp < sl".to_string());
            }
        }
    }
    Ok(())
}

fn merge_receipts(mut entry: SubmitReceipt, protection: SubmitReceipt) -> SubmitReceipt {
    entry.statuses.extend(protection.statuses);
    if protection.status != ExecutionStatus::Accepted || protection.error.is_some() {
        entry.status = protection.status;
        entry.error = protection.error;
    }
    entry
}

fn parse_distance(raw: &str) -> Result<ChaseDistance, String> {
    if raw.eq_ignore_ascii_case("quote") {
        return Ok(ChaseDistance::Quote);
    }
    if let Some(pct) = raw.strip_suffix('%') {
        let pct = decimal("chase percent", pct.trim())?;
        if pct <= Decimal::ZERO {
            return Err("chase percent must be positive".to_string());
        }
        return Ok(ChaseDistance::Percent(pct));
    }
    let value = decimal("chase distance", raw.trim_end_matches('$').trim())?;
    if value <= Decimal::ZERO {
        return Err("chase distance must be positive".to_string());
    }
    Ok(ChaseDistance::Absolute(value))
}

fn chase_target(state: &TradingState, chase: &ActiveChase) -> Result<Decimal, String> {
    let book = state
        .book
        .get(&chase.symbol)
        .ok_or_else(|| "book unavailable for chase".to_string())?;
    if book.age_ms(now_ms()) > FreshnessLimits::default().book_ms {
        return Err("fresh book required for chase".to_string());
    }
    let book = book.value;
    let mid = ((book.bid + book.ask) / Decimal::TWO).normalize();
    let raw = match chase.distance {
        ChaseDistance::Quote => mid,
        ChaseDistance::Absolute(distance) if chase.is_buy => mid - distance,
        ChaseDistance::Absolute(distance) => mid + distance,
        ChaseDistance::Percent(percent) if chase.is_buy => {
            mid * (Decimal::ONE - percent / Decimal::from(100))
        }
        ChaseDistance::Percent(percent) => mid * (Decimal::ONE + percent / Decimal::from(100)),
    }
    .normalize();
    if raw <= Decimal::ZERO {
        return Err("chase target price must be positive".to_string());
    }
    let market = state
        .markets
        .get(&chase.symbol)
        .ok_or_else(|| format!("unknown market {}", chase.symbol))?;
    let side = if chase.is_buy { Side::Bid } else { Side::Ask };
    market
        .tick()
        .round_for_side(side, raw, true)
        .map(|price| price.normalize())
        .ok_or_else(|| format!("invalid price for {}", chase.symbol))
}

fn chase_modify_plan(
    chase: &ActiveChase,
    market: &Market,
    order: &Order,
    price: Decimal,
) -> Result<ActionPlan, String> {
    Ok(ActionPlan {
        kind: "chase_move",
        market: chase.symbol.clone(),
        action_label: "chase_move".to_string(),
        requires_book: true,
        action: Action::BatchModify(BatchModify {
            modifies: vec![Modify {
                oid: OrderTarget::Cloid(chase.cloid),
                order: OrderRequest {
                    asset: market.asset.0,
                    is_buy: chase.is_buy,
                    price,
                    size: order.size,
                    reduce_only: order.reduce_only,
                    order_type: OrderType::Limit {
                        tif: chase.tif.clone(),
                    },
                    cloid: chase.cloid,
                },
            }],
        }),
    })
}

fn active_trailing_from_plan(
    symbol: &str,
    plan: &ActionPlan,
    distance: TrailDistance,
    interval: Duration,
) -> Result<ActiveTrailing, String> {
    let Action::Order(batch) = &plan.action else {
        return Err("trailing plan did not produce an order".to_string());
    };
    let order = batch
        .orders
        .first()
        .ok_or_else(|| "trailing plan missing order".to_string())?;
    let trigger = match &order.order_type {
        OrderType::Trigger { trigger_px, .. } => *trigger_px,
        OrderType::Limit { .. } => {
            return Err("trailing plan did not produce a trigger".to_string());
        }
    };
    Ok(ActiveTrailing {
        symbol: symbol.to_string(),
        cloid: order.cloid,
        side: if order.is_buy { Side::Bid } else { Side::Ask },
        distance,
        trigger,
        next_move_ms: next_trailing_move_ms(interval),
    })
}

fn parse_trail_distance(raw: &str) -> Result<TrailDistance, String> {
    if let Some(pct) = raw.strip_suffix('%') {
        let pct = decimal("trailing percent", pct.trim())?;
        if pct <= Decimal::ZERO {
            return Err("trailing percent must be positive".to_string());
        }
        Ok(TrailDistance::Percent(pct))
    } else {
        let value = decimal("trailing distance", raw.trim_end_matches('$').trim())?;
        if value <= Decimal::ZERO {
            return Err("trailing distance must be positive".to_string());
        }
        Ok(TrailDistance::Absolute(value))
    }
}

fn trailing_target(state: &TradingState, trailing: &ActiveTrailing) -> Result<Decimal, String> {
    let book = state
        .book
        .get(&trailing.symbol)
        .ok_or_else(|| "book unavailable for trailing".to_string())?;
    if book.age_ms(now_ms()) > FreshnessLimits::default().book_ms {
        return Err("fresh book required for trailing".to_string());
    }
    let book = book.value;
    let mid = ((book.bid + book.ask) / Decimal::TWO).normalize();
    let distance = match trailing.distance {
        TrailDistance::Absolute(distance) => distance,
        TrailDistance::Percent(percent) => (mid * percent / Decimal::from(100)).normalize(),
    };
    let raw = match trailing.side {
        Side::Ask => mid - distance,
        Side::Bid => mid + distance,
    }
    .normalize();
    if raw <= Decimal::ZERO {
        return Err("trailing trigger must be positive".to_string());
    }
    let market = state
        .markets
        .get(&trailing.symbol)
        .ok_or_else(|| format!("unknown market {}", trailing.symbol))?;
    market
        .tick()
        .round_for_side(trailing.side, raw, true)
        .map(|price| price.normalize())
        .ok_or_else(|| format!("invalid trigger for {}", trailing.symbol))
}

fn trail_should_move(trailing: &ActiveTrailing, target: Decimal) -> bool {
    match trailing.side {
        Side::Ask => target > trailing.trigger,
        Side::Bid => target < trailing.trigger,
    }
}

fn trailing_modify_plan(
    trailing: &ActiveTrailing,
    market: &Market,
    order: &Order,
    trigger_px: Decimal,
) -> Result<ActionPlan, String> {
    Ok(ActionPlan {
        kind: "trailing_move",
        market: trailing.symbol.clone(),
        action_label: "trailing_move".to_string(),
        requires_book: true,
        action: Action::BatchModify(BatchModify {
            modifies: vec![Modify {
                oid: OrderTarget::Cloid(trailing.cloid),
                order: OrderRequest {
                    asset: market.asset.0,
                    is_buy: trailing.side == Side::Bid,
                    price: trigger_px,
                    size: order.size,
                    reduce_only: true,
                    order_type: OrderType::Trigger {
                        is_market: true,
                        trigger_px,
                        tpsl: TpSl::Sl,
                    },
                    cloid: trailing.cloid,
                },
            }],
        }),
    })
}

fn next_chase_move_ms(interval: Duration) -> u64 {
    let delay = interval
        .as_millis()
        .saturating_mul(120)
        .min(u128::from(u64::MAX)) as u64;
    now_ms().saturating_add(delay)
}

fn next_trailing_move_ms(interval: Duration) -> u64 {
    let delay = interval
        .as_millis()
        .saturating_mul(20)
        .min(u128::from(u64::MAX)) as u64;
    now_ms().saturating_add(delay)
}

fn next_entry_cancel_ms(interval: Duration) -> u64 {
    let delay = interval
        .as_millis()
        .saturating_mul(20)
        .min(u128::from(u64::MAX)) as u64;
    now_ms().saturating_add(delay)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn decimal(name: &str, raw: &str) -> Result<Decimal, String> {
    raw.parse::<Decimal>()
        .map(|value| value.normalize())
        .map_err(|_| format!("invalid {name}: {raw}"))
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use crate::state::{Fill, RecoveredOrderOutcome};

    #[test]
    fn persisted_trade_ids_make_fill_replay_idempotent_across_restart() {
        let cloid = Cloid::from_u128(1).unwrap();
        let mut protection = EntryProtection {
            id: cloid,
            symbol: "BTC".to_string(),
            intended_is_buy: true,
            entry_orders: vec![EntryOrder {
                cloid,
                oid: Some(42),
                receipt_filled: Decimal::ZERO,
                feed_filled: Decimal::ZERO,
                seen_fill_tids: std::collections::BTreeSet::new(),
                terminal: false,
            }],
            stop_loss: Some(managed_leg("48000".to_string())),
            take_profit: None,
            trailing: None,
            entry_cancel_pending: false,
            next_attempt_ms: 0,
        };
        let fill = |tid| Fill {
            seq: tid,
            tid,
            oid: 42,
            symbol: "BTC".to_string(),
            is_buy: true,
            size: "0.005".parse().unwrap(),
            price: "49000".parse().unwrap(),
            closed_pnl: Decimal::ZERO,
            time_ms: 1,
        };
        protection.observe_fills(&std::collections::VecDeque::from([fill(7)]));
        let encoded = serde_json::to_vec(&protection).unwrap();
        let mut restored: EntryProtection = serde_json::from_slice(&encoded).unwrap();
        restored.observe_fills(&std::collections::VecDeque::from([fill(7), fill(8)]));
        assert_eq!(restored.filled_size(), "0.01".parse().unwrap());
    }

    #[test]
    fn recovered_filled_entry_relinks_persisted_cloid_to_exact_oid() {
        let cloid = Cloid::from_u128(2).unwrap();
        let mut protection = EntryProtection {
            id: cloid,
            symbol: "BTC".to_string(),
            intended_is_buy: true,
            entry_orders: vec![EntryOrder {
                cloid,
                oid: None,
                receipt_filled: Decimal::ZERO,
                feed_filled: Decimal::ZERO,
                seen_fill_tids: std::collections::BTreeSet::new(),
                terminal: false,
            }],
            stop_loss: Some(managed_leg("48000".to_string())),
            take_profit: None,
            trailing: None,
            entry_cancel_pending: false,
            next_attempt_ms: 0,
        };
        let mut state = TradingState::new("BTC");
        state.record_recovered_order_outcome(
            cloid.to_string(),
            RecoveredOrderOutcome {
                oid: Some(42),
                filled_size: "0.01".parse().unwrap(),
                terminal: true,
            },
        );

        protection.recover_entry_outcomes(&state);

        assert_eq!(protection.entry_orders[0].oid, Some(42));
        assert_eq!(protection.filled_size(), "0.01".parse().unwrap());
        assert!(protection.entry_orders[0].terminal);
    }
}
