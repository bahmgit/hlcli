use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::Address;
use alloy_signer::SignerSync;
use rust_decimal::Decimal;

use crate::{
    command::Command,
    exchange::{ExchangeResponse, WsActionClient},
    execution::{
        ActionJournal, ExecutionControl, ExecutionKernel, ExecutionStatus, JournalContext,
        OrderStatus, Reconciliation, SubmitReceipt, TransportError,
    },
    metrics::Metrics,
    planner::{ActionPlan, Planner},
    protocol::{Action, ActionRequest, Chain, OrderRequest, OrderTarget, OrderType, TpSl},
    state::{
        AccountMode, Fresh, FreshnessLimits, Order, OrderKind, Position, Readiness,
        RecoveredOrderOutcome, TradingState, Twap,
    },
};
use tokio::sync::{Mutex, RwLock};

pub type TransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ExchangeResponse, TransportError>> + Send + 'a>>;
pub type ExecuteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SubmitReceipt, String>> + Send + 'a>>;
pub type SwitchBlockFuture<'a> = Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>>;
pub type ReconcileFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReconcileOutcome, String>> + Send + 'a>>;
pub type RecoveryFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileOutcome {
    Applied {
        detail: String,
        statuses: Vec<OrderStatus>,
    },
    NotApplied {
        detail: String,
    },
    Inconclusive {
        detail: String,
    },
}

pub trait ActionReconciler: Send + Sync {
    fn reconcile<'a>(&'a self, action: &'a Action) -> ReconcileFuture<'a>;
}

pub trait ActionTransport: Send + Sync {
    fn post_action<'a>(&'a self, request: ActionRequest, timeout: Duration) -> TransportFuture<'a>;
}

pub trait CommandExecutor: Send + Sync {
    fn execute_command<'a>(&'a self, command: Command) -> ExecuteFuture<'a>;
    fn execute_command_for<'a>(&'a self, _symbol: &'a str, _command: Command) -> ExecuteFuture<'a> {
        Box::pin(async { Err("market-scoped execution unsupported by this executor".to_string()) })
    }

    fn switch_blocker<'a>(&'a self, _symbol: &'a str) -> SwitchBlockFuture<'a> {
        Box::pin(async { None })
    }
}

pub trait PlanExecutor: CommandExecutor {
    fn execute_plan<'a>(&'a self, plan: ActionPlan) -> ExecuteFuture<'a>;
}

pub trait ExecutionRecovery: Send + Sync {
    fn recover_execution<'a>(&'a self) -> RecoveryFuture<'a>;
}

impl ActionTransport for WsActionClient {
    fn post_action<'a>(&'a self, request: ActionRequest, timeout: Duration) -> TransportFuture<'a> {
        Box::pin(async move { self.post_action(request, timeout).await })
    }
}

pub struct Core<S, T> {
    state: Arc<RwLock<TradingState>>,
    planner: Planner,
    kernel: ExecutionKernel,
    signer: S,
    transport: T,
    chain: Chain,
    vault_address: Option<Address>,
    timeout: Duration,
    metrics: Metrics,
    nonce: AtomicU64,
    execution_lock: Mutex<()>,
    execution_control: ExecutionControl,
    reconciler: Option<Arc<dyn ActionReconciler>>,
    recovery_complete: AtomicBool,
}

impl<S, T> Core<S, T>
where
    S: SignerSync + Send + Sync,
    T: ActionTransport,
{
    pub fn new(
        state: TradingState,
        journal: ActionJournal,
        signer: S,
        transport: T,
        chain: Chain,
        metrics: Metrics,
    ) -> Self {
        Self::from_shared_state(
            Arc::new(RwLock::new(state)),
            journal,
            signer,
            transport,
            chain,
            metrics,
        )
    }

    pub fn from_shared_state(
        state: Arc<RwLock<TradingState>>,
        journal: ActionJournal,
        signer: S,
        transport: T,
        chain: Chain,
        metrics: Metrics,
    ) -> Self {
        Self {
            state,
            planner: Planner::default(),
            kernel: ExecutionKernel::new(journal),
            signer,
            transport,
            chain,
            vault_address: None,
            timeout: Duration::from_secs(15),
            metrics,
            nonce: AtomicU64::new(0),
            execution_lock: Mutex::new(()),
            execution_control: ExecutionControl::default(),
            reconciler: None,
            recovery_complete: AtomicBool::new(false),
        }
    }

    pub fn with_vault(mut self, vault_address: Address) -> Self {
        self.vault_address = Some(vault_address);
        self
    }

    pub fn with_market_cross_bps(mut self, market_cross_bps: u32) -> Self {
        self.planner = Planner::with_market_cross_bps(market_cross_bps);
        self
    }

    pub fn with_reconciler(mut self, reconciler: Arc<dyn ActionReconciler>) -> Self {
        self.reconciler = Some(reconciler);
        self
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn journal(&self) -> &ActionJournal {
        self.kernel.journal()
    }

    pub fn state(&self) -> Arc<RwLock<TradingState>> {
        self.state.clone()
    }

    pub fn execution_control(&self) -> ExecutionControl {
        self.execution_control.clone()
    }

    pub async fn recover(&self) -> Result<(), String> {
        let _execution = self.execution_lock.lock().await;
        self.recover_or_halt().await
    }

    pub async fn execute(&self, command: Command) -> Result<SubmitReceipt, String> {
        let symbol = self.state.read().await.active.clone();
        self.execute_for(&symbol, command).await
    }

    pub async fn execute_for(
        &self,
        symbol: &str,
        command: Command,
    ) -> Result<SubmitReceipt, String> {
        let _execution = self.execution_lock.lock().await;
        self.recover_or_halt().await?;
        self.execution_control.ensure_running().await?;
        let state = self.state.read().await;
        let plan = self.planner.plan_for(&state, symbol, command)?;
        validate_plan(&state, &plan)?;
        validate_perp_capacity(&state, &plan)?;
        let readiness = readiness(&state, &plan);
        drop(state);
        if !readiness.ready {
            return Err(readiness.reasons.join("; "));
        }
        self.submit(plan).await
    }

    pub async fn execute_plan(&self, plan: ActionPlan) -> Result<SubmitReceipt, String> {
        let _execution = self.execution_lock.lock().await;
        self.recover_or_halt().await?;
        self.execution_control.ensure_running().await?;
        let state = self.state.read().await;
        validate_plan(&state, &plan)?;
        validate_perp_capacity(&state, &plan)?;
        let readiness = readiness(&state, &plan);
        drop(state);
        if !readiness.ready {
            return Err(readiness.reasons.join("; "));
        }
        self.submit(plan).await
    }

    async fn submit(&self, plan: ActionPlan) -> Result<SubmitReceipt, String> {
        let before = self.metrics.snapshot();
        let nonce = self.next_nonce();
        let action = plan.action.clone();
        let action_json = serde_json::to_string(&action).map_err(|err| err.to_string())?;
        let reconciliation_context = JournalContext {
            session_id: self.kernel.journal().session_id().to_string(),
            nonce: Some(nonce),
            action_json: Some(action_json.clone()),
        };
        let request = plan
            .action
            .sign(&self.signer, nonce, self.vault_address, None, self.chain)
            .map_err(|err| err.to_string())?;
        self.metrics.sign();

        let transport = &self.transport;
        let metrics = self.metrics.clone();
        let timeout = self.timeout;
        let response_action = action.clone();
        let mut receipt = self
            .kernel
            .submit_with_context(
                plan.kind,
                &plan.market,
                &plan.action_label,
                Some(nonce),
                Some(action_json),
                move |_| {
                    let request = request.clone();
                    let action = response_action.clone();
                    async move {
                        metrics.ws_post();
                        let response = transport.post_action(request, timeout).await;
                        match response {
                            Ok(response) => {
                                metrics.ws_ack();
                                response.statuses_for(&action).and_then(|statuses| {
                                    validate_response_statuses(&action, statuses)
                                })
                            }
                            Err(err) => {
                                match &err {
                                    TransportError::Timeout { .. } => metrics.ws_timeout(),
                                    TransportError::ClosedBeforeSend { .. }
                                    | TransportError::StreamClosed { .. } => metrics.ws_closed(),
                                    _ => {}
                                }
                                Err(err)
                            }
                        }
                    }
                },
            )
            .await;
        if receipt.status == ExecutionStatus::Ambiguous
            && let Some(reconciler) = &self.reconciler
        {
            receipt = match reconciler.reconcile(&action).await {
                Ok(ReconcileOutcome::Applied { detail, statuses }) => self.kernel.reconcile(
                    Reconciliation {
                        id: receipt.id,
                        kind: plan.kind,
                        market: &plan.market,
                        action: &plan.action_label,
                        applied: true,
                        detail: &detail,
                        context: reconciliation_context.clone(),
                    },
                    statuses,
                ),
                Ok(ReconcileOutcome::NotApplied { detail }) => self.kernel.reconcile(
                    Reconciliation {
                        id: receipt.id,
                        kind: plan.kind,
                        market: &plan.market,
                        action: &plan.action_label,
                        applied: false,
                        detail: &detail,
                        context: reconciliation_context.clone(),
                    },
                    Vec::new(),
                ),
                Ok(ReconcileOutcome::Inconclusive { detail }) => {
                    receipt.error = Some(format!(
                        "{}; reconciliation: {detail}",
                        receipt
                            .error
                            .as_deref()
                            .unwrap_or("ambiguous exchange outcome")
                    ));
                    receipt
                }
                Err(err) => {
                    receipt.error = Some(format!(
                        "{}; reconciliation failed: {err}",
                        receipt
                            .error
                            .as_deref()
                            .unwrap_or("ambiguous exchange outcome")
                    ));
                    receipt
                }
            };
        }
        if receipt.status == ExecutionStatus::Ambiguous {
            self.execution_control
                .halt(format!(
                    "unresolved {} execution id={}: {}",
                    plan.action_label,
                    receipt.id,
                    receipt
                        .error
                        .as_deref()
                        .unwrap_or("ambiguous exchange outcome")
                ))
                .await;
        } else if receipt
            .error
            .as_deref()
            .is_some_and(|error| error.contains("journal_terminal"))
        {
            self.execution_control
                .halt(format!(
                    "journal durability failure after {} execution id={}",
                    plan.action_label, receipt.id
                ))
                .await;
        }
        self.apply_receipt(&plan.market, &action, &receipt).await;
        let delta = self.metrics.snapshot().delta(before);
        debug_assert!(delta.forbidden_submit_path_activity().is_empty());
        Ok(receipt)
    }

    async fn recover_unresolved(&self) -> Result<(), String> {
        if self.recovery_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        let unresolved = self
            .kernel
            .journal()
            .unresolved()
            .map_err(|err| format!("read unresolved execution journal: {err}"))?;
        if unresolved.is_empty() {
            self.recovery_complete.store(true, Ordering::Release);
            return Ok(());
        }
        let Some(reconciler) = &self.reconciler else {
            let reason = "unresolved journal actions require an authoritative reconciler";
            self.execution_control.halt(reason).await;
            return Err(reason.to_string());
        };
        for record in unresolved {
            let context = record
                .context
                .clone()
                .ok_or_else(|| "unresolved journal record missing context".to_string())?;
            let action_json = context
                .action_json
                .as_deref()
                .ok_or_else(|| "unresolved journal record missing action JSON".to_string())?;
            let action: Action = serde_json::from_str(action_json)
                .map_err(|err| format!("decode unresolved journal action: {err}"))?;
            let receipt = match reconciler.reconcile(&action).await {
                Ok(ReconcileOutcome::Applied { detail, statuses }) => self.kernel.reconcile(
                    Reconciliation {
                        id: record.id,
                        kind: &record.kind,
                        market: &record.market,
                        action: &record.action,
                        applied: true,
                        detail: &detail,
                        context,
                    },
                    statuses,
                ),
                Ok(ReconcileOutcome::NotApplied { detail }) => self.kernel.reconcile(
                    Reconciliation {
                        id: record.id,
                        kind: &record.kind,
                        market: &record.market,
                        action: &record.action,
                        applied: false,
                        detail: &detail,
                        context,
                    },
                    Vec::new(),
                ),
                Ok(ReconcileOutcome::Inconclusive { detail }) => {
                    let reason = format!(
                        "unresolved prior execution session={} id={}: {detail}",
                        context.session_id, record.id
                    );
                    self.execution_control.halt(reason.clone()).await;
                    return Err(reason);
                }
                Err(err) => {
                    let reason = format!(
                        "prior execution reconciliation failed session={} id={}: {err}",
                        context.session_id, record.id
                    );
                    self.execution_control.halt(reason.clone()).await;
                    return Err(reason);
                }
            };
            if let Some(error) = receipt.error {
                let reason = format!(
                    "journal resolution failed for prior execution id={}: {error}",
                    record.id
                );
                self.execution_control.halt(reason.clone()).await;
                return Err(reason);
            }
            self.apply_receipt(&record.market, &action, &receipt).await;
            self.record_recovered_order_outcomes(&action, &receipt)
                .await;
        }
        self.recovery_complete.store(true, Ordering::Release);
        Ok(())
    }

    async fn recover_or_halt(&self) -> Result<(), String> {
        let result = self.recover_unresolved().await;
        if let Err(reason) = &result {
            self.execution_control.halt(reason.clone()).await;
        }
        result
    }

    async fn apply_receipt(&self, market: &str, action: &Action, receipt: &SubmitReceipt) {
        if receipt.error.is_some() {
            return;
        }
        let now = now_ms();
        let mut state = self.state.write().await;
        match action {
            Action::Order(batch) => {
                for (request, status) in batch.orders.iter().zip(&receipt.statuses) {
                    apply_order_status(&mut state, market, request, status, now);
                    apply_fill_position(&mut state, market, request, status, now);
                }
            }
            Action::Cancel(batch) => {
                for (cancel, status) in batch.cancels.iter().zip(&receipt.statuses) {
                    if cancel_status_clears_local(status) {
                        state.apply_cancel_receipt(market, cancel.oid, now);
                    }
                }
            }
            Action::CancelByCloid(batch) => {
                for (cancel, status) in batch.cancels.iter().zip(&receipt.statuses) {
                    if !cancel_status_clears_local(status) {
                        continue;
                    }
                    let cloid = cancel.cloid.to_string();
                    let oid = state
                        .orders_for(market)
                        .find(|order| order.value.cloid.as_deref() == Some(cloid.as_str()))
                        .map(|order| order.value.oid);
                    if let Some(oid) = oid {
                        state.apply_cancel_receipt(market, oid, now);
                    }
                }
            }
            Action::BatchModify(batch) => {
                for (modify, status) in batch.modifies.iter().zip(&receipt.statuses) {
                    if matches!(status, OrderStatus::Success) {
                        if let Some(oid) = modify_oid(&state, market, &modify.oid) {
                            state.apply_resting_receipt(
                                order_from_request(
                                    market,
                                    oid,
                                    Some(modify.order.cloid.to_string()),
                                    &modify.order,
                                ),
                                now,
                                now,
                            );
                        }
                        continue;
                    }
                    if matches!(
                        status,
                        OrderStatus::WaitingForTrigger | OrderStatus::Resting { .. }
                    ) && let OrderTarget::Cloid(cloid) = &modify.oid
                        && let Some(old_oid) = oid_for_cloid(&state, market, cloid)
                    {
                        state.apply_cancel_receipt(market, old_oid, now);
                    }
                    apply_order_status(&mut state, market, &modify.order, status, now);
                    if let (OrderTarget::Oid(old_oid), OrderStatus::Resting { oid, .. }) =
                        (&modify.oid, status)
                        && old_oid != oid
                    {
                        state.apply_cancel_receipt(market, *old_oid, now);
                    }
                }
            }
            Action::AgentSetAbstraction(action) if accepted(receipt) => {
                if let Ok(mode) = account_mode_from_agent_code(&action.abstraction) {
                    state.apply_account_mode(mode, now, Some(now));
                }
            }
            Action::TwapOrder(action) if accepted(receipt) => {
                for status in &receipt.statuses {
                    if let OrderStatus::TwapRunning { twap_id } = status {
                        state.apply_twap(
                            *twap_id,
                            Twap {
                                symbol: market.to_string(),
                                dex: String::new(),
                                is_buy: action.twap.is_buy,
                                size: action.twap.size,
                                executed_size: Decimal::ZERO,
                                minutes: action.twap.minutes,
                                reduce_only: action.twap.reduce_only,
                                randomize: action.twap.randomize,
                                submitted_ms: Some(now),
                            },
                            now,
                            Some(now),
                        );
                    }
                }
            }
            Action::TwapCancel(action) if accepted(receipt) => {
                state.forget_twap(action.twap_id);
            }
            _ => {}
        }
    }

    async fn record_recovered_order_outcomes(&self, action: &Action, receipt: &SubmitReceipt) {
        let Action::Order(batch) = action else {
            return;
        };
        let mut state = self.state.write().await;
        if receipt.status == ExecutionStatus::Rejected && receipt.statuses.is_empty() {
            for request in &batch.orders {
                state.record_recovered_order_outcome(
                    request.cloid.to_string(),
                    RecoveredOrderOutcome {
                        oid: None,
                        filled_size: Decimal::ZERO,
                        terminal: true,
                    },
                );
            }
            return;
        }
        for (request, status) in batch.orders.iter().zip(&receipt.statuses) {
            let outcome = match status {
                OrderStatus::Resting { oid, .. } => RecoveredOrderOutcome {
                    oid: Some(*oid),
                    filled_size: Decimal::ZERO,
                    terminal: false,
                },
                OrderStatus::Filled {
                    oid, total_size, ..
                } => {
                    let Ok(filled_size) = total_size.parse::<Decimal>() else {
                        continue;
                    };
                    RecoveredOrderOutcome {
                        oid: Some(*oid),
                        filled_size,
                        terminal: true,
                    }
                }
                OrderStatus::Error { .. } | OrderStatus::AlreadyTerminal { .. } => {
                    RecoveredOrderOutcome {
                        oid: None,
                        filled_size: Decimal::ZERO,
                        terminal: true,
                    }
                }
                OrderStatus::WaitingForFill | OrderStatus::WaitingForTrigger => {
                    RecoveredOrderOutcome {
                        oid: None,
                        filled_size: Decimal::ZERO,
                        terminal: false,
                    }
                }
                OrderStatus::Success | OrderStatus::TwapRunning { .. } => continue,
            };
            state.record_recovered_order_outcome(request.cloid.to_string(), outcome);
        }
    }

    fn next_nonce(&self) -> u64 {
        let mut candidate = now_ms();
        loop {
            let previous = self.nonce.load(Ordering::Relaxed);
            if candidate <= previous {
                candidate = previous + 1;
            }
            if self
                .nonce
                .compare_exchange(previous, candidate, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return candidate;
            }
        }
    }
}

impl<S, T> CommandExecutor for Core<S, T>
where
    S: SignerSync + Send + Sync,
    T: ActionTransport,
{
    fn execute_command<'a>(&'a self, command: Command) -> ExecuteFuture<'a> {
        Box::pin(async move { self.execute(command).await })
    }

    fn execute_command_for<'a>(&'a self, symbol: &'a str, command: Command) -> ExecuteFuture<'a> {
        Box::pin(async move { self.execute_for(symbol, command).await })
    }
}

impl<S, T> PlanExecutor for Core<S, T>
where
    S: SignerSync + Send + Sync,
    T: ActionTransport,
{
    fn execute_plan<'a>(&'a self, plan: ActionPlan) -> ExecuteFuture<'a> {
        Box::pin(async move { self.execute_plan(plan).await })
    }
}

impl<S, T> ExecutionRecovery for Core<S, T>
where
    S: SignerSync + Send + Sync,
    T: ActionTransport,
{
    fn recover_execution<'a>(&'a self) -> RecoveryFuture<'a> {
        Box::pin(async move { self.recover().await })
    }
}

fn apply_order_status(
    state: &mut TradingState,
    market: &str,
    request: &OrderRequest,
    status: &OrderStatus,
    now: u64,
) {
    match status {
        OrderStatus::Resting { oid, cloid } => {
            state.apply_resting_receipt(
                order_from_request(
                    market,
                    *oid,
                    cloid.clone().or_else(|| Some(request.cloid.to_string())),
                    request,
                ),
                now,
                now,
            );
        }
        OrderStatus::WaitingForTrigger | OrderStatus::WaitingForFill
            if matches!(request.order_type, OrderType::Trigger { .. }) =>
        {
            state.apply_resting_receipt(
                order_from_request(
                    market,
                    next_local_oid(),
                    Some(request.cloid.to_string()),
                    request,
                ),
                now,
                now,
            );
        }
        _ => {}
    }
}

fn cancel_status_clears_local(status: &OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Success | OrderStatus::AlreadyTerminal { .. }
    )
}

fn terminal_cancel_error(message: &str) -> bool {
    message.starts_with("Order was never placed, already canceled, or filled.")
}

fn validate_response_statuses(
    action: &Action,
    mut statuses: Vec<OrderStatus>,
) -> Result<Vec<OrderStatus>, TransportError> {
    let expected = match action {
        Action::Order(batch) => batch.orders.len(),
        Action::BatchModify(batch) => batch.modifies.len(),
        Action::Cancel(batch) => batch.cancels.len(),
        Action::CancelByCloid(batch) => batch.cancels.len(),
        _ => 1,
    };
    if statuses.len() != expected {
        return Err(TransportError::Decode(format!(
            "exchange status cardinality mismatch expected={expected} actual={}",
            statuses.len()
        )));
    }
    if matches!(action, Action::Cancel(_) | Action::CancelByCloid(_)) {
        for status in &mut statuses {
            let OrderStatus::Error { message } = status else {
                continue;
            };
            if terminal_cancel_error(message) {
                *status = OrderStatus::AlreadyTerminal {
                    message: message.clone(),
                };
            }
        }
    }
    Ok(statuses)
}

fn apply_fill_position(
    state: &mut TradingState,
    market: &str,
    request: &OrderRequest,
    status: &OrderStatus,
    now: u64,
) {
    let OrderStatus::Filled {
        total_size,
        average_price,
        ..
    } = status
    else {
        return;
    };
    let Ok(size) = total_size.parse::<Decimal>() else {
        return;
    };
    let Ok(fill_price) = average_price.parse::<Decimal>() else {
        return;
    };
    let current = state
        .positions
        .get(market)
        .map(|position| position.value.clone())
        .unwrap_or_else(|| Position::empty(market));
    let delta = if request.is_buy { size } else { -size };
    let next_size = (current.size + delta).normalize();
    let entry_price = if next_size == Decimal::ZERO {
        None
    } else if current.size == Decimal::ZERO
        || (current.size < Decimal::ZERO) != (next_size < Decimal::ZERO)
        || next_size.abs() > current.size.abs()
    {
        Some(fill_price)
    } else {
        current.entry_price
    };
    state.apply_fill_position(
        Position {
            symbol: market.to_string(),
            size: next_size,
            entry_price,
            detail: None,
        },
        now,
        now,
    );
}

fn accepted(receipt: &SubmitReceipt) -> bool {
    receipt.status == crate::execution::ExecutionStatus::Accepted
        && receipt.error.is_none()
        && receipt
            .statuses
            .iter()
            .all(|status| status.error().is_none())
}

fn account_mode_from_agent_code(raw: &str) -> Result<AccountMode, String> {
    match raw {
        "i" => Ok(AccountMode::Standard),
        "u" => Ok(AccountMode::UnifiedAccount),
        "p" => Ok(AccountMode::PortfolioMargin),
        _ => Err(format!("unknown agent abstraction code {raw}")),
    }
}

fn modify_oid(state: &TradingState, market: &str, target: &OrderTarget) -> Option<u64> {
    match target {
        OrderTarget::Oid(oid) => Some(*oid),
        OrderTarget::Cloid(cloid) => oid_for_cloid(state, market, cloid),
    }
}

fn oid_for_cloid(
    state: &TradingState,
    market: &str,
    cloid: &crate::protocol::Cloid,
) -> Option<u64> {
    let cloid = cloid.to_string();
    state
        .orders_for(market)
        .find(|order| order.value.cloid.as_deref() == Some(cloid.as_str()))
        .map(|order| order.value.oid)
}

fn order_from_request(
    market: &str,
    oid: u64,
    cloid: Option<String>,
    request: &OrderRequest,
) -> Order {
    let tif = match &request.order_type {
        OrderType::Limit { tif } => Some(tif.clone()),
        OrderType::Trigger { .. } => None,
    };
    Order {
        symbol: market.to_string(),
        oid,
        cloid,
        is_buy: request.is_buy,
        price: request.price,
        size: request.size,
        reduce_only: request.reduce_only,
        kind: order_kind(&request.order_type),
        tif,
    }
}

fn next_local_oid() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(crate::state::LOCAL_ORDER_OID_MIN);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn order_kind(order_type: &OrderType) -> OrderKind {
    match order_type {
        OrderType::Limit { .. } => OrderKind::Limit,
        OrderType::Trigger { tpsl: TpSl::Tp, .. } => OrderKind::TakeProfit,
        OrderType::Trigger { tpsl: TpSl::Sl, .. } => OrderKind::StopLoss,
    }
}

fn exposure_increasing(plan: &ActionPlan) -> bool {
    match &plan.action {
        Action::Order(batch) => batch.orders.iter().any(|order| !order.reduce_only),
        Action::BatchModify(batch) => batch
            .modifies
            .iter()
            .any(|modify| !modify.order.reduce_only),
        Action::TwapOrder(action) => !action.twap.reduce_only,
        _ => false,
    }
}

fn validate_perp_capacity(state: &TradingState, plan: &ActionPlan) -> Result<(), String> {
    let Some(market) = state.markets.get(&plan.market) else {
        return Err(format!("unknown planned market {}", plan.market));
    };
    if !matches!(
        market.kind,
        crate::protocol::MarketKind::Perp | crate::protocol::MarketKind::BuilderPerp
    ) || !exposure_increasing(plan)
    {
        return Ok(());
    }
    let capacity = state
        .active_assets
        .get(&plan.market)
        .ok_or_else(|| "activeAssetData unavailable".to_string())?;
    let age_ms = capacity.age_ms(now_ms());
    if age_ms > FreshnessLimits::default().active_asset_ms {
        return Err(format!("activeAssetData stale age_ms={age_ms}"));
    }
    let mut required = [Decimal::ZERO; 2];
    let mut add = |order: &OrderRequest, released: Decimal| {
        if !order.reduce_only {
            let index = usize::from(!order.is_buy);
            required[index] += (order.size - released).max(Decimal::ZERO);
        }
    };
    match &plan.action {
        Action::Order(batch) => {
            for order in &batch.orders {
                add(order, Decimal::ZERO);
            }
        }
        Action::BatchModify(batch) => {
            for modify in &batch.modifies {
                let released = modify_oid(state, &plan.market, &modify.oid)
                    .and_then(|oid| state.orders.get(&oid))
                    .filter(|existing| existing.value.is_buy == modify.order.is_buy)
                    .map(|existing| existing.value.size)
                    .unwrap_or(Decimal::ZERO);
                add(&modify.order, released);
            }
        }
        Action::TwapOrder(action) if !action.twap.reduce_only => {
            let index = usize::from(!action.twap.is_buy);
            required[index] = action.twap.size;
        }
        _ => {}
    }
    for (index, required) in required.into_iter().enumerate() {
        let available = capacity.value.max_trade_sizes[index];
        if required > available {
            let side = if index == 0 { "buy" } else { "sell" };
            return Err(format!(
                "insufficient {side} perp capacity: required={required} available={available}"
            ));
        }
    }
    Ok(())
}

fn validate_plan(state: &TradingState, plan: &ActionPlan) -> Result<(), String> {
    plan.validate(
        state
            .markets
            .get(&plan.market)
            .ok_or_else(|| format!("unknown planned market {}", plan.market))?,
    )
}

fn needs_orders(plan: &ActionPlan) -> bool {
    matches!(
        plan.action,
        Action::Order(_) | Action::BatchModify(_) | Action::TwapOrder(_)
    )
}

fn readiness(state: &TradingState, plan: &ActionPlan) -> crate::state::Readiness {
    let now = now_ms();
    let limits = FreshnessLimits::default();
    let mut reasons = Vec::new();
    let symbol = plan.market.as_str();
    let Some(market) = state.markets.get(symbol) else {
        return Readiness {
            ready: false,
            reasons: vec![format!("unknown planned market {symbol}")],
        };
    };
    if market.delisted {
        reasons.push(format!("{symbol} delisted"));
    }
    if exposure_increasing(plan) && market.open_interest_cap {
        reasons.push(format!("{symbol} at open-interest cap"));
    }
    if applies_account_mode_guard(plan)
        && let Some(required) = state.required_account_mode
    {
        match state.account_mode {
            Some(actual) if actual.age_ms(now) > limits.account_ms => {
                reasons.push(format!("account_mode stale age_ms={}", actual.age_ms(now)))
            }
            Some(actual) if actual.value != required => reasons.push(format!(
                "account_mode_mismatch actual={} required={}",
                actual.value.as_str(),
                required.as_str()
            )),
            Some(_) => {}
            None => reasons.push(format!(
                "account_mode unavailable required={}",
                required.as_str()
            )),
        }
    }
    if matches!(plan.action, Action::AgentSetAbstraction(_)) {
        require_fresh(
            &mut reasons,
            "account_mode",
            state.account_mode.as_ref(),
            now,
            limits.account_ms,
        );
    }
    if needs_book(plan) {
        require_fresh(
            &mut reasons,
            "book",
            state.book.get(symbol),
            now,
            limits.book_ms,
        );
    }
    if exposure_increasing(plan) {
        require_fresh(
            &mut reasons,
            "account",
            state.account.as_ref(),
            now,
            limits.account_ms,
        );
        if matches!(
            market.kind,
            crate::protocol::MarketKind::Perp | crate::protocol::MarketKind::BuilderPerp
        ) {
            require_fresh(
                &mut reasons,
                "position",
                state.positions.get(symbol),
                now,
                limits.position_ms,
            );
            require_fresh(
                &mut reasons,
                "activeAssetData",
                state.active_assets.get(symbol),
                now,
                limits.active_asset_ms,
            );
        } else {
            require_fresh(
                &mut reasons,
                "spot balances",
                state.balance_summary.as_ref(),
                now,
                limits.account_ms,
            );
        }
    } else if needs_fresh_position(plan) {
        require_fresh(
            &mut reasons,
            "position",
            state.positions.get(symbol),
            now,
            limits.position_ms,
        );
    } else if needs_position(plan) && state.position(symbol).is_none() {
        reasons.push("unknown position for reduce-only action".to_string());
    }
    if needs_orders(plan) && !orders_fresh(state, symbol, now, limits.orders_ms) {
        reasons.push("orders unavailable or stale".to_string());
    }
    Readiness {
        ready: reasons.is_empty(),
        reasons,
    }
}

fn applies_account_mode_guard(plan: &ActionPlan) -> bool {
    matches!(
        plan.action,
        Action::Order(_)
            | Action::TwapOrder(_)
            | Action::UpdateLeverage(_)
            | Action::UpdateIsolatedMargin(_)
    )
}

fn needs_book(plan: &ActionPlan) -> bool {
    plan.requires_book
}

fn needs_position(plan: &ActionPlan) -> bool {
    matches!(plan.action, Action::Order(_))
}

fn needs_fresh_position(plan: &ActionPlan) -> bool {
    matches!(plan.action, Action::UpdateIsolatedMargin(_))
}

fn orders_fresh(state: &TradingState, symbol: &str, now_ms: u64, max_age_ms: u64) -> bool {
    state.orders_ready(symbol, now_ms, max_age_ms)
}

fn require_fresh<T>(
    reasons: &mut Vec<String>,
    name: &str,
    value: Option<&Fresh<T>>,
    now_ms: u64,
    max_age_ms: u64,
) {
    match value {
        Some(value) if value.age_ms(now_ms) <= max_age_ms => {}
        Some(value) => reasons.push(format!("{name} stale age_ms={}", value.age_ms(now_ms))),
        None => reasons.push(format!("{name} unavailable")),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
