mod support;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use hl_v2::{
    command::{self, Command},
    core::{CommandExecutor, ExecuteFuture, PlanExecutor},
    execution::{ExecutionStatus, OrderStatus, SubmitReceipt},
    managed::ManagedExecutor,
    planner::{ActionPlan, Planner},
    protocol::{Action, OrderRequest, OrderType, TpSl},
    state::{Fill, Order, OrderKind, Position, TradingState, Twap},
};
use rust_decimal::Decimal;
use tokio::sync::{Notify, RwLock};

use support::{dec, now_ms, ready_perp};

struct RecordingPlanExecutor {
    state: Arc<RwLock<TradingState>>,
    plans: Mutex<Vec<ActionPlan>>,
    commands: Mutex<Vec<Command>>,
    next_oid: AtomicU64,
    reject_protection: AtomicBool,
    reject_cancel_once: AtomicBool,
    block_chase_modify: AtomicBool,
    chase_modify_started: Notify,
    release_chase_modify: Notify,
    lifecycle_events: Mutex<Vec<&'static str>>,
}

impl RecordingPlanExecutor {
    fn new(state: Arc<RwLock<TradingState>>) -> Self {
        Self {
            state,
            plans: Mutex::new(Vec::new()),
            commands: Mutex::new(Vec::new()),
            next_oid: AtomicU64::new(100),
            reject_protection: AtomicBool::new(false),
            reject_cancel_once: AtomicBool::new(false),
            block_chase_modify: AtomicBool::new(false),
            chase_modify_started: Notify::new(),
            release_chase_modify: Notify::new(),
            lifecycle_events: Mutex::new(Vec::new()),
        }
    }

    fn plan_kinds(&self) -> Vec<String> {
        self.plans
            .lock()
            .unwrap()
            .iter()
            .map(|plan| plan.kind.to_string())
            .collect()
    }
}

impl CommandExecutor for RecordingPlanExecutor {
    fn execute_command<'a>(&'a self, command: Command) -> ExecuteFuture<'a> {
        Box::pin(async move {
            let symbol = self.state.read().await.active.clone();
            self.execute_command_for(&symbol, command).await
        })
    }

    fn execute_command_for<'a>(&'a self, symbol: &'a str, command: Command) -> ExecuteFuture<'a> {
        self.commands.lock().unwrap().push(command.clone());
        Box::pin(async move {
            let state = self.state.read().await;
            let plan = Planner::default().plan_for(&state, symbol, command)?;
            drop(state);
            self.execute_plan(plan).await
        })
    }
}

impl PlanExecutor for RecordingPlanExecutor {
    fn execute_plan<'a>(&'a self, plan: ActionPlan) -> ExecuteFuture<'a> {
        self.plans.lock().unwrap().push(plan.clone());
        Box::pin(async move {
            match &plan.action {
                Action::Order(batch) => {
                    if self.reject_protection.load(Ordering::Relaxed)
                        && batch
                            .orders
                            .iter()
                            .any(|order| matches!(order.order_type, OrderType::Trigger { .. }))
                    {
                        return Ok(SubmitReceipt {
                            id: 1,
                            status: ExecutionStatus::Rejected,
                            statuses: vec![OrderStatus::Error {
                                message: "rejected by exchange".to_string(),
                            }],
                            error: Some("entry protection rejected".to_string()),
                        });
                    }
                    let mut statuses = Vec::with_capacity(batch.orders.len());
                    let mut state = self.state.write().await;
                    let now = now_ms();
                    for request in &batch.orders {
                        let oid = self.next_oid.fetch_add(1, Ordering::Relaxed);
                        state.apply_resting_receipt(
                            order_from(&plan.market, oid, request),
                            now,
                            now,
                        );
                        statuses.push(OrderStatus::Resting {
                            oid,
                            cloid: Some(request.cloid.to_string()),
                        });
                    }
                    Ok(receipt(statuses))
                }
                Action::BatchModify(batch) => {
                    if plan.kind == "chase_move" && self.block_chase_modify.load(Ordering::Relaxed)
                    {
                        self.chase_modify_started.notify_one();
                        self.release_chase_modify.notified().await;
                        self.lifecycle_events.lock().unwrap().push("modify");
                    }
                    let mut statuses = Vec::with_capacity(batch.modifies.len());
                    let mut state = self.state.write().await;
                    let now = now_ms();
                    for modify in &batch.modifies {
                        let existing = state
                            .orders_for(&plan.market)
                            .find(|order| {
                                order.value.cloid.as_deref()
                                    == Some(modify.order.cloid.to_string().as_str())
                            })
                            .map(|order| order.value.oid)
                            .ok_or_else(|| "modify target missing".to_string())?;
                        state.apply_resting_receipt(
                            order_from(&plan.market, existing, &modify.order),
                            now,
                            now,
                        );
                        statuses.push(OrderStatus::Resting {
                            oid: existing,
                            cloid: Some(modify.order.cloid.to_string()),
                        });
                    }
                    Ok(receipt(statuses))
                }
                Action::CancelByCloid(batch) => {
                    self.lifecycle_events.lock().unwrap().push("cancel");
                    if self.reject_cancel_once.swap(false, Ordering::Relaxed) {
                        return Ok(SubmitReceipt {
                            id: 1,
                            status: ExecutionStatus::Rejected,
                            statuses: vec![OrderStatus::Error {
                                message: "cancel rejected by exchange".to_string(),
                            }],
                            error: Some("entry cancellation rejected".to_string()),
                        });
                    }
                    let mut state = self.state.write().await;
                    let now = now_ms();
                    for cancel in &batch.cancels {
                        let cloid = cancel.cloid.to_string();
                        let oid = {
                            state
                                .orders_for(&plan.market)
                                .find(|order| order.value.cloid.as_deref() == Some(cloid.as_str()))
                                .map(|order| order.value.oid)
                        };
                        if let Some(oid) = oid {
                            state.apply_cancel_receipt(&plan.market, oid, now);
                        }
                    }
                    Ok(receipt(vec![OrderStatus::Success; batch.cancels.len()]))
                }
                Action::Cancel(batch) => {
                    let mut state = self.state.write().await;
                    let now = now_ms();
                    for cancel in &batch.cancels {
                        state.apply_cancel_receipt(&plan.market, cancel.oid, now);
                    }
                    Ok(receipt(vec![OrderStatus::Success; batch.cancels.len()]))
                }
                _ => Ok(receipt(vec![OrderStatus::Success])),
            }
        })
    }
}

fn order_from(symbol: &str, oid: u64, request: &OrderRequest) -> Order {
    let kind = match request.order_type {
        OrderType::Limit { .. } => OrderKind::Limit,
        OrderType::Trigger { tpsl: TpSl::Sl, .. } => OrderKind::StopLoss,
        OrderType::Trigger { tpsl: TpSl::Tp, .. } => OrderKind::TakeProfit,
    };
    Order {
        symbol: symbol.to_string(),
        oid,
        cloid: Some(request.cloid.to_string()),
        is_buy: request.is_buy,
        price: request.price,
        size: request.size,
        reduce_only: request.reduce_only,
        kind,
        tif: match &request.order_type {
            OrderType::Limit { tif } => Some(tif.clone()),
            OrderType::Trigger { .. } => None,
        },
    }
}

fn receipt(statuses: Vec<OrderStatus>) -> SubmitReceipt {
    SubmitReceipt {
        id: 1,
        status: ExecutionStatus::Accepted,
        statuses,
        error: None,
    }
}

async fn record_fill(
    state: &Arc<RwLock<TradingState>>,
    oid: u64,
    size: &str,
    net_position: &str,
    tid: u64,
) {
    let now = now_ms();
    let mut state = state.write().await;
    state.record_fill(Fill {
        seq: 0,
        tid,
        oid,
        symbol: "BTC".to_string(),
        is_buy: true,
        size: dec(size),
        price: dec("49000"),
        closed_pnl: Decimal::ZERO,
        time_ms: now,
    });
    state.apply_fill_position(
        Position {
            symbol: "BTC".to_string(),
            size: dec(net_position),
            entry_price: Some(dec("49000")),
            detail: None,
        },
        now,
        now,
    );
}

async fn wait_for_protection_size(state: &Arc<RwLock<TradingState>>, expected: &str) {
    for _ in 0..200 {
        if state.read().await.orders_for("BTC").any(|order| {
            order.value.kind == OrderKind::StopLoss && order.value.size == dec(expected)
        }) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("stop-loss size never reached {expected}");
}

#[tokio::test]
async fn protected_scale_tracks_only_its_entry_fills_and_resizes_until_terminal() {
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let inner = Arc::new(RecordingPlanExecutor::new(state.clone()));
    let managed = ManagedExecutor::spawn(state.clone(), inner.clone(), Duration::from_millis(5));

    managed
        .execute_command(command::parse(
            "scale buy 0.02 into 2 from 49000 to 48000 sl 47000",
        ))
        .await
        .unwrap();
    let entry_oids = state
        .read()
        .await
        .orders_for("BTC")
        .filter(|order| order.value.kind == OrderKind::Limit)
        .map(|order| order.value.oid)
        .collect::<Vec<_>>();
    assert_eq!(entry_oids.len(), 2);

    record_fill(&state, entry_oids[0], "0.005", "0.005", 1).await;
    wait_for_protection_size(&state, "0.005").await;

    record_fill(&state, 999_999, "0.02", "0.025", 2).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(state.read().await.orders_for("BTC").any(|order| {
        order.value.kind == OrderKind::StopLoss && order.value.size == dec("0.005")
    }));

    record_fill(&state, entry_oids[1], "0.005", "0.03", 3).await;
    wait_for_protection_size(&state, "0.01").await;

    let stop_oid = state
        .read()
        .await
        .orders_for("BTC")
        .find(|order| order.value.kind == OrderKind::StopLoss)
        .unwrap()
        .value
        .oid;
    state
        .write()
        .await
        .apply_cancel_receipt("BTC", stop_oid, now_ms());
    for _ in 0..100 {
        if !state
            .read()
            .await
            .orders_for("BTC")
            .any(|order| order.value.kind == OrderKind::Limit)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        !state
            .read()
            .await
            .orders_for("BTC")
            .any(|order| order.value.kind == OrderKind::Limit)
    );
    assert!(managed.switch_blocker("BTC").await.is_none());

    let second = managed
        .execute_command(command::parse("buy 0.01 at 47000 sl 46000"))
        .await
        .unwrap_err();
    assert!(second.contains("flat starting position") || second.contains("already active"));
}

#[tokio::test]
async fn fixed_trade_protection_is_dependent_and_cancel_families_are_isolated() {
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let inner = Arc::new(RecordingPlanExecutor::new(state.clone()));
    let managed = ManagedExecutor::spawn(state.clone(), inner.clone(), Duration::from_secs(60));

    managed
        .execute_command(command::parse("buy 0.01 at 49000 sl 48000"))
        .await
        .unwrap();
    assert_eq!(
        inner.plan_kinds()[0],
        "order",
        "entry must not use an unsafe native bracket"
    );

    record_fill(&state, 100, "0.01", "0.01", 10).await;
    wait_for_protection_size(&state, "0.01").await;
    managed
        .execute_command(command::parse("trail 1%"))
        .await
        .unwrap();

    managed
        .execute_command(command::parse("sl cancel"))
        .await
        .unwrap();
    let orders = state.read().await;
    assert_eq!(
        orders
            .orders_for("BTC")
            .filter(|order| order.value.kind == OrderKind::StopLoss)
            .count(),
        1,
        "fixed SL cancel must retain managed trailing"
    );
    drop(orders);
    managed
        .execute_command(command::parse("trail cancel"))
        .await
        .unwrap();
    assert!(
        !state
            .read()
            .await
            .orders_for("BTC")
            .any(|order| order.value.kind == OrderKind::StopLoss)
    );
}

#[tokio::test]
async fn trailing_trade_attachment_reaches_managed_protection_after_fill() {
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let inner = Arc::new(RecordingPlanExecutor::new(state.clone()));
    let managed = ManagedExecutor::spawn(state.clone(), inner.clone(), Duration::from_millis(5));

    managed
        .execute_command(command::parse("buy 0.01 at 49000 trail 1% tp 51000"))
        .await
        .unwrap();
    assert_eq!(inner.plan_kinds(), ["order"]);

    record_fill(&state, 100, "0.01", "0.01", 11).await;
    for _ in 0..200 {
        let orders = state.read().await;
        let trailing = orders
            .orders_for("BTC")
            .any(|order| order.value.kind == OrderKind::StopLoss);
        let take_profit = orders
            .orders_for("BTC")
            .any(|order| order.value.kind == OrderKind::TakeProfit);
        if trailing && take_profit {
            return;
        }
        drop(orders);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("trailing and take-profit attachments were not both armed");
}

#[tokio::test]
async fn rejected_protection_cancels_remaining_entry_without_losing_degraded_intent() {
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let inner = Arc::new(RecordingPlanExecutor::new(state.clone()));
    let managed = ManagedExecutor::spawn(state.clone(), inner.clone(), Duration::from_millis(5));

    managed
        .execute_command(command::parse("buy 0.02 at 49000 sl 48000"))
        .await
        .unwrap();
    inner.reject_protection.store(true, Ordering::Relaxed);
    inner.reject_cancel_once.store(true, Ordering::Relaxed);
    record_fill(&state, 100, "0.01", "0.01", 20).await;

    for _ in 0..200 {
        if !state
            .read()
            .await
            .orders_for("BTC")
            .any(|order| order.value.kind == OrderKind::Limit)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        !state
            .read()
            .await
            .orders_for("BTC")
            .any(|order| order.value.kind == OrderKind::Limit),
        "remaining entry exposure must be cancelled after protection rejection"
    );
    assert!(
        managed
            .switch_blocker("BTC")
            .await
            .is_some_and(|reason| reason.contains("deferred entry protection")),
        "the rejected protection intent must remain visible for operator resolution"
    );
    assert!(
        inner
            .commands
            .lock()
            .unwrap()
            .iter()
            .filter(|command| matches!(command, Command::CancelCloid { .. }))
            .count()
            >= 2,
        "a rejected entry cancellation must be retried without retrying protection"
    );
    assert_eq!(
        inner
            .plan_kinds()
            .iter()
            .filter(|kind| kind.as_str() == "protection")
            .count(),
        1,
        "the deterministically rejected protection leg must not be retried"
    );
}

#[tokio::test]
async fn stale_book_never_generates_a_managed_chase_modify() {
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let inner = Arc::new(RecordingPlanExecutor::new(state.clone()));
    let managed = ManagedExecutor::spawn(state.clone(), inner.clone(), Duration::from_millis(1));
    managed
        .execute_command(command::parse("chase buy 0.01 quote"))
        .await
        .unwrap();
    state.write().await.book.get_mut("BTC").unwrap().local_ms = 0;
    tokio::time::sleep(Duration::from_millis(180)).await;
    assert!(!inner.plan_kinds().iter().any(|kind| kind == "chase_move"));
}

#[tokio::test]
async fn explicit_chase_cancel_is_terminal_against_an_in_flight_poll() {
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let inner = Arc::new(RecordingPlanExecutor::new(state.clone()));
    let managed = ManagedExecutor::spawn(state.clone(), inner.clone(), Duration::from_millis(1));
    managed
        .execute_command(command::parse("chase buy 0.01 quote"))
        .await
        .unwrap();

    inner.block_chase_modify.store(true, Ordering::Relaxed);
    let started = inner.chase_modify_started.notified();
    let now = now_ms();
    state.write().await.apply_book(
        "BTC",
        hl_v2::state::Book {
            bid: dec("49990"),
            ask: dec("49991"),
        },
        now,
        Some(now),
    );
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("chase poll did not reach the modify boundary");

    let cancel = tokio::spawn({
        let managed = managed.clone();
        async move {
            managed
                .execute_command(command::parse("chase cancel"))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    inner.release_chase_modify.notify_one();
    cancel.await.unwrap().unwrap();

    assert_eq!(
        inner.lifecycle_events.lock().unwrap().as_slice(),
        ["modify", "cancel"],
        "a planned manager move must finish before explicit cancellation, never after it"
    );
    assert!(!state.read().await.orders_for("BTC").any(|_| true));
    assert!(managed.switch_blocker("BTC").await.is_none());
}

#[tokio::test]
async fn managed_cancel_all_commands_target_only_their_active_family() {
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let inner = Arc::new(RecordingPlanExecutor::new(state.clone()));
    let managed = ManagedExecutor::spawn(state.clone(), inner.clone(), Duration::from_secs(60));

    managed
        .execute_command(command::parse("chase buy 0.01 quote"))
        .await
        .unwrap();
    assert!(managed.switch_blocker("BTC").await.is_some());
    managed
        .execute_command(command::parse("chase cancel"))
        .await
        .unwrap();
    assert!(managed.switch_blocker("BTC").await.is_none());

    let now = now_ms();
    let twap = |id| Twap {
        symbol: "BTC".to_string(),
        dex: String::new(),
        is_buy: id == 7,
        size: dec("0.01"),
        executed_size: Decimal::ZERO,
        minutes: 5,
        reduce_only: false,
        randomize: false,
        submitted_ms: Some(now),
    };
    state.write().await.apply_twap(7, twap(7), now, Some(now));
    state.write().await.apply_twap(8, twap(8), now, Some(now));
    managed
        .execute_command(command::parse("twap cancel all"))
        .await
        .unwrap();
    let mut ids = inner
        .commands
        .lock()
        .unwrap()
        .iter()
        .filter_map(|command| match command {
            Command::TwapCancel { id: Some(id) } => Some(*id),
            _ => None,
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![7, 8]);
}
