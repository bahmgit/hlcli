mod support;

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use hl_v2::{
    command,
    core::{ActionTransport, Core, TransportFuture},
    exchange::{ExchangeResponse, OkResponse},
    execution::{ActionJournal, ExecutionStatus, JournalPhase, OrderStatus, TransportError},
    metrics::Metrics,
    planner::Planner,
    protocol::{
        Action, ActionRequest, BatchCancel, Cancel, Chain, Retracement, TrailingStopAction,
    },
    state::{Order, OrderKind},
};
use rust_decimal::Decimal;
use std::{
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};
use support::{dec, now_ms, ready_perp};
use tokio::sync::RwLock;

fn trailing() -> Action {
    Action::TrailingStop(Box::new(TrailingStopAction {
        asset: 0,
        is_buy: false,
        size: dec("0.01"),
        reduce_only: true,
        retracement: Retracement::Pct("1%".into()),
        activation_px: None,
    }))
}

#[test]
fn native_trailing_and_fast_cancel_wire_are_exact() {
    assert_eq!(
        serde_json::to_value(trailing()).unwrap(),
        serde_json::json!({
            "type":"trailingStop", "asset":0,"isBuy":false,"sz":"0.01","reduceOnly":true,"retracement":{"pct":"1%"},"activationPx":null
        })
    );
    // Independently encoded MessagePack map, in the official frontend action field order.
    let expected = "87a474797065ac747261696c696e6753746f70a5617373657400a56973427579c2a2737aa4302e3031aa7265647563654f6e6c79c3ab726574726163656d656e7481a3706374a23125ac61637469766174696f6e5078c0";
    let bytes = rmp_serde::to_vec_named(&trailing()).unwrap();
    assert_eq!(
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        expected
    );
    let ordinary = Action::Cancel(BatchCancel {
        cancels: vec![Cancel { asset: 0, oid: 42 }],
        fast: false,
    });
    let fast = Action::Cancel(BatchCancel {
        cancels: vec![Cancel { asset: 0, oid: 42 }],
        fast: true,
    });
    assert_eq!(
        serde_json::to_value(&ordinary).unwrap(),
        serde_json::json!({"type":"cancel","cancels":[{"a":0,"o":42}]})
    );
    assert_eq!(
        rmp_serde::to_vec_named(&ordinary).unwrap(),
        [
            0x82, 0xa4, b't', b'y', b'p', b'e', 0xa6, b'c', b'a', b'n', b'c', b'e', b'l', 0xa7,
            b'c', b'a', b'n', b'c', b'e', b'l', b's', 0x91, 0x82, 0xa1, b'a', 0, 0xa1, b'o', 42
        ]
    );
    assert_eq!(serde_json::to_value(&fast).unwrap()["f"], true);
    assert_ne!(
        ordinary.hash(1700000000123, None, None).unwrap(),
        fast.hash(1700000000123, None, None).unwrap()
    );
    let response: ExchangeResponse = serde_json::from_value(serde_json::json!({"status":"ok","response":{"type":"trailingStop","data":{"oid":77738308}}})).unwrap();
    assert_eq!(
        response.statuses_for(&trailing()).unwrap(),
        [OrderStatus::Resting {
            oid: 77738308,
            cloid: None
        }]
    );
    assert!(
        ExchangeResponse::Ok(OkResponse::TrailingStop { oid: 0 })
            .statuses_for(&trailing())
            .unwrap_err()
            .is_ambiguous()
    );
}

#[test]
fn trailing_uses_fresh_mark_and_rejects_watermark_resetting_amendments() {
    let mut state = ready_perp("0.02");
    state.book.get_mut("BTC").unwrap().local_ms = 0;
    let plan = Planner::default()
        .plan(&state, command::parse("trail 1% size 50% activate 52000"))
        .unwrap();
    assert!(!plan.requires_book);
    let Action::TrailingStop(action) = plan.action else {
        panic!("not native");
    };
    assert_eq!(action.size, dec("0.01"));
    assert_eq!(action.activation_px.as_deref(), Some("52000"));
    assert!(!action.is_buy && action.reduce_only);
    let now = now_ms();
    state.apply_resting_receipt(
        Order {
            symbol: "BTC".into(),
            oid: 42,
            cloid: None,
            is_buy: false,
            price: Decimal::ZERO,
            size: dec("0.01"),
            reduce_only: true,
            kind: OrderKind::TrailingStop,
            tif: None,
            fast_cancel_eligible: false,
            trailing: None,
        },
        now,
        now,
    );
    for input in ["move 42 to 49500", "resize 42 to 0.005"] {
        let error = Planner::default()
            .plan(&state, command::parse(input))
            .unwrap_err();
        assert!(error.contains("cannot be moved or resized"), "{error}");
    }
    for input in [
        "trail 0%",
        "trail 100%",
        "trail -1",
        "trail 1.00001%",
        "trail 1% activate 0",
    ] {
        assert!(
            Planner::default()
                .plan(&state, command::parse(input))
                .is_err(),
            "{input}"
        );
    }
    for position in ["0.0001", "-0.0001"] {
        let tiny = ready_perp(position);
        assert!(
            Planner::default()
                .plan(&tiny, command::parse("trail 1%"))
                .unwrap_err()
                .contains("10 USDC"),
            "full-position native trails still require the exchange minimum"
        );
    }
    assert!(
        Planner::default()
            .plan(&ready_perp("0.0002"), command::parse("trail 1%"))
            .is_ok()
    );
    state.mark_prices.get_mut("BTC").unwrap().local_ms = 0;
    assert!(
        Planner::default()
            .plan(&state, command::parse("trail 1%"))
            .unwrap_err()
            .contains("fresh mark")
    );
}

#[test]
fn fast_cancel_requires_positive_eligibility_for_entire_batch() {
    let mut state = ready_perp("0.02");
    let now = now_ms();
    for (oid, eligible, kind) in [(1, true, OrderKind::Limit), (2, false, OrderKind::StopLoss)] {
        state.apply_resting_receipt(
            Order {
                symbol: "BTC".into(),
                oid,
                cloid: Some(format!("0x{oid:032x}")),
                is_buy: false,
                price: dec("49000"),
                size: dec("0.01"),
                reduce_only: oid == 2,
                kind,
                tif: Some(hl_v2::protocol::TimeInForce::Gtc),
                fast_cancel_eligible: eligible,
                trailing: None,
            },
            now,
            now,
        );
    }
    for (input, expected) in [
        ("cancel 1", true),
        ("cancel 1,2", false),
        ("cancel 1,999", false),
        ("sl cancel", false),
        ("cancel cloid 0x00000000000000000000000000000001", true),
    ] {
        let plan = Planner::default()
            .plan(&state, command::parse(input))
            .unwrap();
        let fast = match plan.action {
            Action::Cancel(batch) => batch.fast,
            Action::CancelByCloid(batch) => batch.fast,
            _ => panic!(),
        };
        assert_eq!(fast, expected, "{input}");
    }
}

#[test]
fn conditional_twap_grammar_validation_and_unconditional_wire_compatibility() {
    let state = ready_perp("0");
    let plan = |input| Planner::default().plan(&state, command::parse(input));
    let action = plan("twap buy 0.1 over 30 trigger below 50000 max 51000")
        .unwrap()
        .action;
    assert_eq!(
        serde_json::to_value(action).unwrap(),
        serde_json::json!({"type":"twapOrder","twap":{"a":0,"b":true,"s":"0.1","r":false,"m":30,"t":false},"details":{"t":{"p":"50000","a":false},"s":"51000"}})
    );
    let action = plan("twap sell 0.1 over 30 trigger above 50000 min 49000")
        .unwrap()
        .action;
    assert_eq!(
        serde_json::to_value(action).unwrap()["details"],
        serde_json::json!({"t":{"p":"50000","a":true},"s":"49000"})
    );
    let conditional = plan("twap buy 0.1 over 30 trigger below 50000 max 51000")
        .unwrap()
        .action;
    let expected = "83a474797065a9747761704f72646572a47477617086a16100a162c3a173a3302e31a172c2a16d1ea174c2a764657461696c7382a17482a170a53530303030a161c2a173a53531303030";
    assert_eq!(
        rmp_serde::to_vec_named(&conditional)
            .unwrap()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        expected,
        "conditional TWAP signing field order"
    );
    let unconditional = plan("twap buy 0.1 over 30").unwrap().action;
    assert_eq!(
        serde_json::to_value(unconditional).unwrap(),
        serde_json::json!({"type":"twapOrder","twap":{"a":0,"b":true,"s":"0.1","r":false,"m":30,"t":false}})
    );
    for input in [
        "twap buy 0.1 over 4",
        "twap buy 0.1 over 10081",
        "twap buy 0.1 over 30 trigger above 0",
        "twap buy 0.1 over 30 max 50000",
        "twap sell 0.1 over 30 min 50000",
        "twap buy 0.1 over 30 trigger above 51000 max 51000",
        "twap buy 0.1 over 30 min 49000",
        "twap buy 0.1 over 30 trigger above 51000 trigger below 49000",
    ] {
        assert!(plan(input).is_err(), "{input}");
    }
    let value =
        serde_json::to_value(plan("twap buy 0.1 over 30 max 51000").unwrap().action).unwrap();
    assert_eq!(value["details"], serde_json::json!({"t":null,"s":"51000"}));
}

struct FakeTransport {
    requests: Arc<Mutex<Vec<ActionRequest>>>,
    response: Result<ExchangeResponse, TransportError>,
}
impl ActionTransport for FakeTransport {
    fn post_action<'a>(&'a self, request: ActionRequest, _: Duration) -> TransportFuture<'a> {
        self.requests.lock().unwrap().push(request);
        let response = self.response.clone();
        Box::pin(async move { response })
    }
}
fn signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::repeat_byte(1)).unwrap()
}

#[tokio::test]
async fn builder_twap_receipt_survives_native_dex_snapshots_until_its_own_terminal_state() {
    let path = std::env::temp_dir().join(format!(
        "hlcli-builder-twap-{}-{}.jsonl",
        std::process::id(),
        now_ms()
    ));
    let mut state = ready_perp("0");
    let symbol = "XYZ:XYZ100";
    let mut market = state.markets["BTC"].clone();
    market.symbol = symbol.into();
    market.wire_symbol = "xyz:XYZ100".into();
    market.dex = Some("xyz".into());
    market.kind = hl_v2::protocol::MarketKind::BuilderPerp;
    market.asset = hl_v2::protocol::AssetId::builder_perp(2, 0);
    state.set_market(market);
    let now = now_ms();
    state.apply_book(symbol, state.book["BTC"].value, now, Some(now));
    state.apply_active_asset(symbol, state.active_assets["BTC"].value, now, Some(now));
    state.apply_position_snapshot(hl_v2::state::Position::empty(symbol), now, Some(now));
    state.apply_orders_snapshot(symbol, Vec::new(), now, Some(now));
    let state = Arc::new(RwLock::new(state));
    let response = serde_json::from_value(serde_json::json!({
        "status":"ok", "response":{"type":"twapOrder","data":{"status":{"running":{"twapId":42}}}}
    }))
    .unwrap();
    let core = Core::from_shared_state(
        state.clone(),
        ActionJournal::new(&path),
        signer(),
        FakeTransport {
            requests: Arc::new(Mutex::new(Vec::new())),
            response: Ok(response),
        },
        Chain::Mainnet,
        Metrics::default(),
    );
    let receipt = core
        .execute_for(
            symbol,
            command::parse("twap buy 0.01 over 5 trigger above 51000 max 52000"),
        )
        .await
        .unwrap();
    assert_eq!(receipt.status, ExecutionStatus::Accepted);
    let mut state = state.write().await;
    assert_eq!(state.twaps[&42].value.dex, "xyz");
    state.replace_twaps_for_dex("", Vec::new(), now + 1, Some(now + 1));
    assert_eq!(
        state.twaps_for(symbol).count(),
        1,
        "native snapshots cannot hide acknowledged builder TWAPs from cancellation"
    );
    assert_eq!(state.twaps_for("BTC").count(), 0);
    state.replace_twaps_for_dex("xyz", Vec::new(), now + 2, Some(now + 2));
    assert_eq!(state.twaps_for(symbol).count(), 0);
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn acknowledged_native_oid_recovers_without_recreating_active_orders() {
    let path = std::env::temp_dir().join(format!(
        "hlcli-native-ack-{}-{}.jsonl",
        std::process::id(),
        now_ms()
    ));
    let state = Arc::new(RwLock::new(ready_perp("0.02")));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let core = Core::from_shared_state(
        state.clone(),
        ActionJournal::new(&path),
        signer(),
        FakeTransport {
            requests: requests.clone(),
            response: Ok(ExchangeResponse::Ok(OkResponse::TrailingStop { oid: 42 })),
        },
        Chain::Mainnet,
        Metrics::default(),
    );
    let mut plan = Planner::default()
        .plan(&*state.read().await, command::parse("trail 1% size 0.01"))
        .unwrap();
    plan.action_label = "attached_trail:fixture:0".into();
    assert_eq!(
        core.execute_plan(plan).await.unwrap().status,
        ExecutionStatus::Accepted
    );
    let records = ActionJournal::new(&path).read_all().unwrap();
    assert_eq!(
        records[1].statuses,
        [OrderStatus::Resting {
            oid: 42,
            cloid: None
        }]
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
    // Simulate restart after the journal acknowledgement, before the managed snapshot saves oid.
    let restart_state = Arc::new(RwLock::new(ready_perp("0.02")));
    let restart = Core::from_shared_state(
        restart_state.clone(),
        ActionJournal::new(&path),
        signer(),
        FakeTransport {
            requests: requests.clone(),
            response: Ok(ExchangeResponse::Ok(OkResponse::Default)),
        },
        Chain::Mainnet,
        Metrics::default(),
    );
    restart.recover().await.unwrap();
    let state = restart_state.read().await;
    assert_eq!(
        state
            .recovered_trailing_placements
            .get("attached_trail:fixture:0"),
        Some(&Some(42))
    );
    assert_eq!(
        state.open_orders().count(),
        0,
        "historical acknowledgement must not invent active orders"
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn malformed_native_ack_halts_and_pending_restart_never_guesses_ownership() {
    let path = std::env::temp_dir().join(format!(
        "hlcli-native-lost-{}-{}.jsonl",
        std::process::id(),
        now_ms()
    ));
    let journal = ActionJournal::new(&path);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let core = Core::from_shared_state(
        Arc::new(RwLock::new(ready_perp("0.02"))),
        journal.clone(),
        signer(),
        FakeTransport {
            requests: requests.clone(),
            response: Ok(ExchangeResponse::Ok(OkResponse::Default)),
        },
        Chain::Mainnet,
        Metrics::default(),
    );
    let receipt = core.execute(command::parse("trail 1%")).await.unwrap();
    assert_eq!(receipt.status, ExecutionStatus::Ambiguous);
    assert_eq!(
        journal.read_all().unwrap()[1].phase,
        JournalPhase::Ambiguous
    );
    assert!(core.execute(command::parse("trail 1%")).await.is_err());
    assert_eq!(requests.lock().unwrap().len(), 1);
    let restart = Core::from_shared_state(
        Arc::new(RwLock::new(ready_perp("0.02"))),
        ActionJournal::new(&path),
        signer(),
        FakeTransport {
            requests,
            response: Ok(ExchangeResponse::Ok(OkResponse::Default)),
        },
        Chain::Mainnet,
        Metrics::default(),
    );
    assert!(restart.recover().await.is_err());
    fs::remove_file(path).unwrap();
}
