mod support;

use hl_v2::{
    command::{self, Command},
    planner::Planner,
    protocol::{Action, OrderGrouping, OrderType, TimeInForce, TpSl},
    state::{AccountMode, Order, OrderKind, PositionDetail, TradingState, Twap},
};
use rust_decimal::Decimal;
use support::{dec, now_ms, ready_perp};

fn plan(state: &TradingState, input: &str) -> hl_v2::planner::ActionPlan {
    Planner::default()
        .plan_for(state, "BTC", command::parse(input))
        .unwrap_or_else(|error| panic!("{input:?} failed to plan: {error}"))
}

#[test]
fn executable_families_produce_their_exact_action_class() {
    let flat = ready_perp("0");
    let long = ready_perp("1");
    let mut managed = long.clone();
    let now = now_ms();
    managed.apply_resting_receipt(
        Order {
            symbol: "BTC".to_string(),
            oid: 1,
            cloid: Some("0x00000000000000000000000000000001".to_string()),
            is_buy: true,
            price: dec("49000"),
            size: dec("0.01"),
            reduce_only: false,
            kind: OrderKind::Limit,
            tif: Some(TimeInForce::Gtc),
        },
        now,
        now,
    );
    managed.apply_resting_receipt(
        Order {
            symbol: "BTC".to_string(),
            oid: 2,
            cloid: Some("0x00000000000000000000000000000002".to_string()),
            is_buy: false,
            price: dec("48000"),
            size: dec("1"),
            reduce_only: true,
            kind: OrderKind::StopLoss,
            tif: None,
        },
        now,
        now,
    );
    managed.apply_twap(
        7,
        Twap {
            symbol: "BTC".to_string(),
            dex: String::new(),
            is_buy: true,
            size: dec("0.01"),
            executed_size: Decimal::ZERO,
            minutes: 5,
            reduce_only: false,
            randomize: false,
            submitted_ms: Some(now),
        },
        now,
        Some(now),
    );
    let mut isolated = long.clone();
    isolated.positions.get_mut("BTC").unwrap().value.detail = Some(PositionDetail {
        unrealized_pnl: Decimal::ZERO,
        return_on_equity: Decimal::ZERO,
        liquidation_px: None,
        margin_used: dec("100"),
        position_value: dec("1000"),
        leverage: 5,
        leverage_cross: false,
    });
    let cases = [
        (&flat, "buy 0.01", "order"),
        (&flat, "scale buy 0.02 into 2 from 49000 to 48000", "scale"),
        (&flat, "batch buy 0.01@49000 0.01@48000", "batch"),
        (&flat, "chase buy 0.01 quote", "chase_start"),
        (&flat, "twap buy 0.01 over 5", "twap"),
        (&long, "sl 48000", "protection"),
        (&managed, "sl cancel", "protection_cancel"),
        (&managed, "cancel", "cancel_all"),
        (&managed, "cancel 1", "cancel_oid"),
        (
            &managed,
            "cancel cloid 0x00000000000000000000000000000001",
            "cancel_cloid",
        ),
        (&managed, "move 1 to 48500", "modify"),
        (&managed, "resize 1 to 0.02", "modify"),
        (&managed, "batch move 1 to 48500", "modify"),
        (&managed, "batch resize 1 to 0.02", "modify"),
        (
            &managed,
            "batch move cloid 0x00000000000000000000000000000001 to 48500",
            "modify",
        ),
        (
            &managed,
            "batch resize cloid 0x00000000000000000000000000000001 to 0.02",
            "modify",
        ),
        (&long, "close", "close"),
        (&managed, "twap cancel 7", "twap_cancel"),
        (&long, "leverage cross 20", "leverage"),
        (&isolated, "margin add 1", "isolated_margin"),
        (&flat, "account mode set unified", "account_mode"),
    ];
    for (state, input, kind) in cases {
        assert_eq!(plan(state, input).kind, kind, "{input}");
    }
}

#[test]
fn order_semantics_preserve_side_size_tif_and_trigger_direction() {
    let state = ready_perp("0");
    let trade = plan(&state, "buy 0.02 at 49000 tif alo post sl 48000 tp 51000");
    let Action::Order(batch) = trade.action else {
        panic!("expected order batch")
    };
    assert_eq!(batch.grouping, OrderGrouping::NormalTpsl);
    assert_eq!(batch.orders.len(), 3);
    assert!(batch.orders[0].is_buy);
    assert_eq!(batch.orders[0].size, dec("0.02"));
    assert!(!batch.orders[0].reduce_only);
    assert!(matches!(
        batch.orders[1].order_type,
        OrderType::Trigger {
            tpsl: TpSl::Sl,
            trigger_px,
            ..
        } if trigger_px == dec("48000")
    ));
    assert!(matches!(
        batch.orders[2].order_type,
        OrderType::Trigger {
            tpsl: TpSl::Tp,
            trigger_px,
            ..
        } if trigger_px == dec("51000")
    ));
}

#[test]
fn exposure_reducing_and_account_invariants_fail_before_action_creation() {
    let planner = Planner::default();
    for (state, input, message) in [
        (ready_perp("0"), "sell 1 reduce", "flat position"),
        (ready_perp("1"), "buy 1 reduce", "would not reduce"),
        (ready_perp("1"), "sell 2 reduce", "exceeds position"),
        (ready_perp("0"), "buy 1 sl 51000", "invalid SL"),
        (ready_perp("0"), "buy 1 tp 49000", "invalid TP"),
        (ready_perp("0"), "leverage cross 41", "market maximum"),
    ] {
        let error = planner
            .plan_for(&state, "BTC", command::parse(input))
            .expect_err(input);
        assert!(error.contains(message), "{input}: {error}");
    }

    let mut unified = ready_perp("0");
    unified.apply_account_mode(AccountMode::UnifiedAccount, now_ms(), Some(now_ms()));
    let error = planner
        .plan_for(
            &unified,
            "BTC",
            command::parse("account mode set portfolioMargin"),
        )
        .unwrap_err();
    assert!(error.contains("require current mode standard"), "{error}");

    let error = planner
        .plan_for(&unified, "BTC", command::parse("account mode set standard"))
        .unwrap_err();
    assert!(error.contains("requires the main user signer"), "{error}");

    let error = planner
        .plan_for(
            &ready_perp("0"),
            "BTC",
            command::parse("account mode set standard"),
        )
        .unwrap_err();
    assert!(error.contains("already standard"), "{error}");
}

#[test]
fn protection_percent_is_position_percent_and_isolated_margin_is_exact() {
    let mut state = ready_perp("2");
    let protection = plan(&state, "sl 48000 size 25%");
    let Action::Order(batch) = protection.action else {
        panic!("expected protection order")
    };
    assert_eq!(batch.orders[0].size, dec("0.5"));

    state.positions.get_mut("BTC").unwrap().value.detail = Some(PositionDetail {
        unrealized_pnl: Decimal::ZERO,
        return_on_equity: Decimal::ZERO,
        liquidation_px: None,
        margin_used: dec("100"),
        position_value: dec("100000"),
        leverage: 5,
        leverage_cross: false,
    });
    let margin = plan(&state, "margin remove 1.25");
    assert!(matches!(
        margin.action,
        Action::UpdateIsolatedMargin(action) if action.ntli == -1_250_000 && action.is_buy
    ));
}

#[test]
fn non_executable_commands_never_plan_as_exchange_actions() {
    let state = ready_perp("0");
    for command in [Command::Status, Command::Quit, Command::Doctor] {
        assert!(Planner::default().plan_for(&state, "BTC", command).is_err());
    }
}

#[test]
fn exchange_minimums_fail_locally_after_precision_rounding() {
    let state = ready_perp("0");
    let market = state.markets.get("BTC").unwrap();
    let below_order_minimum = Planner::default()
        .plan_for(&state, "BTC", command::parse("buy 10$ at 78327"))
        .unwrap();
    assert!(
        below_order_minimum
            .validate(market)
            .unwrap_err()
            .contains("10 USDC exchange minimum")
    );

    let twap_error = Planner::default()
        .plan_for(&state, "BTC", command::parse("twap buy 99$ over 5"))
        .unwrap_err();
    assert!(twap_error.contains("100 USDC exchange minimum"));

    let partial_close_error = Planner::default()
        .plan_for(&ready_perp("0.00025"), "BTC", command::parse("close 50%"))
        .unwrap_err();
    assert!(
        partial_close_error.contains("only an exact full-position close is exempt"),
        "{partial_close_error}"
    );

    Planner::default()
        .plan_for(&ready_perp("0.0001"), "BTC", command::parse("close"))
        .expect("an exact full-position close remains available below the order minimum");
}
