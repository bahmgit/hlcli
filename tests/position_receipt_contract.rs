mod support;

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use hl_v2::{
    command,
    core::{
        ActionReconciler, ActionTransport, Core, ReconcileFuture, ReconcileOutcome, TransportFuture,
    },
    execution::{ActionJournal, ExecutionStatus, OrderStatus, TransportError},
    metrics::Metrics,
    protocol::{Action, ActionRequest, Chain},
    state::{Position, TradingState},
};
use std::{sync::Arc, time::Duration};
use support::{dec, now_ms, ready_perp};
use tokio::sync::RwLock;

struct FillTransport {
    state: Arc<RwLock<TradingState>>,
    snapshot: Option<&'static str>,
    timeout: bool,
}

impl ActionTransport for FillTransport {
    fn post_action<'a>(&'a self, request: ActionRequest, _: Duration) -> TransportFuture<'a> {
        Box::pin(async move {
            if let Some(size) = self.snapshot {
                let now = now_ms();
                self.state.write().await.apply_position_snapshot(
                    Position {
                        symbol: "BTC".into(),
                        size: dec(size),
                        entry_price: (dec(size) != dec("0")).then(|| dec("49999")),
                        detail: None,
                    },
                    now,
                    Some(now),
                );
            }
            if self.timeout {
                return Err(TransportError::Timeout {
                    id: 1,
                    timeout_ms: 100,
                });
            }
            let Action::Order(batch) = request.action else {
                panic!("expected order")
            };
            let statuses: Vec<_> = batch.orders.iter().enumerate().map(|(i, order)| {
                serde_json::json!({"filled":{"oid":i+1,"totalSz":order.size.to_string(),"avgPx":"50000"}})
            }).collect();
            Ok(serde_json::from_value(serde_json::json!({"status":"ok", "response":{"type":"order","data":{"statuses":statuses}}})).unwrap())
        })
    }
}

fn core(
    state: Arc<RwLock<TradingState>>,
    path: &std::path::Path,
    snapshot: Option<&'static str>,
    timeout: bool,
) -> Core<PrivateKeySigner, FillTransport> {
    Core::from_shared_state(
        state.clone(),
        ActionJournal::new(path),
        PrivateKeySigner::from_bytes(&B256::repeat_byte(1)).unwrap(),
        FillTransport {
            state,
            snapshot,
            timeout,
        },
        Chain::Mainnet,
        Metrics::default(),
    )
}

#[tokio::test]
async fn fill_ack_does_not_double_apply_concurrent_positions_or_guess_conflicting_state() {
    for (i, initial, command, snapshot, expected) in [
        (0, "0.01", "close", None, Some("0")),
        (1, "0.01", "close", Some("0"), Some("0")),
        (2, "0", "buy 0.01", None, Some("0.01")),
        (3, "0", "buy 0.01", Some("0.01"), Some("0.01")),
        // Even a same-value snapshot may predate the fill. Do not guess its ordering.
        (4, "0", "buy 0.01", Some("0"), None),
        (5, "0.01", "close", Some("0.02"), None),
        (
            6,
            "0",
            "batch buy 0.01@50000 0.02@50000 tif ioc",
            None,
            Some("0.03"),
        ),
        (
            7,
            "0",
            "batch buy 0.01@50000 0.02@50000 tif ioc",
            Some("0.03"),
            Some("0.03"),
        ),
    ] {
        let path = std::env::temp_dir().join(format!(
            "hl-position-race-{}-{i}-{}.jsonl",
            std::process::id(),
            now_ms()
        ));
        let state = Arc::new(RwLock::new(ready_perp(initial)));
        let engine = core(state.clone(), &path, snapshot, false);
        let receipt = engine.execute(command::parse(command)).await.unwrap();
        assert_eq!(receipt.status, ExecutionStatus::Accepted);
        let shared = state;
        let mut state = shared.write().await;
        assert_eq!(
            state.position("BTC").map(|p| p.value.size),
            expected.map(dec),
            "case {i}"
        );
        if i == 3 {
            assert_eq!(
                state.position("BTC").unwrap().value.entry_price,
                Some(dec("49999")),
                "preserve authoritative details"
            );
        }
        let now = now_ms();
        assert!(
            !state.apply_position_snapshot(Position::empty("BTC"), now, Some(1)),
            "older snapshots cannot undo the acknowledged result"
        );
        if expected.is_none() {
            drop(state);
            let error = engine
                .execute(command::parse("buy 0.01"))
                .await
                .unwrap_err();
            assert!(error.contains("position"), "{error}");
            state = shared.write().await;
            let now = now_ms();
            assert!(!state.apply_position_snapshot(Position::empty("BTC"), now, Some(1)));
            assert!(state.apply_position_snapshot(Position::empty("BTC"), now + 1, Some(now + 1)));
            assert!(state.position("BTC").unwrap().value.flat());
        }
        std::fs::remove_file(path).unwrap();
    }
}

struct FilledReconciler;
impl ActionReconciler for FilledReconciler {
    fn reconcile<'a>(&'a self, _: &'a Action) -> ReconcileFuture<'a> {
        Box::pin(async {
            Ok(ReconcileOutcome::Applied {
                detail: "historical close filled".into(),
                statuses: vec![OrderStatus::Filled {
                    oid: 1,
                    total_size: "0.01".into(),
                    average_price: "50000".into(),
                }],
            })
        })
    }
}

#[tokio::test]
async fn recovered_historical_fill_does_not_change_current_authoritative_position() {
    let path = std::env::temp_dir().join(format!(
        "hl-position-restart-{}-{}.jsonl",
        std::process::id(),
        now_ms()
    ));
    let state = Arc::new(RwLock::new(ready_perp("0.01")));
    let engine = core(state, &path, None, true);
    assert_eq!(
        engine
            .execute(command::parse("close"))
            .await
            .unwrap()
            .status,
        ExecutionStatus::Ambiguous
    );
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let restarted =
        core(state.clone(), &path, None, false).with_reconciler(Arc::new(FilledReconciler));
    restarted.recover().await.unwrap();
    assert!(state.read().await.position("BTC").unwrap().value.flat());
    assert!(ActionJournal::new(&path).unresolved().unwrap().is_empty());
    std::fs::remove_file(path).unwrap();
}
