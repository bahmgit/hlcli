use std::time::{SystemTime, UNIX_EPOCH};

use hl_v2::{
    protocol::{AssetId, MarketKind},
    state::{Account, AccountMode, ActiveAssetData, Book, Market, Position, TradingState},
};
use rust_decimal::Decimal;

pub fn dec(raw: &str) -> Decimal {
    raw.parse().expect("valid test decimal")
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as u64
}

#[allow(dead_code)]
pub fn ready_perp(position_size: &str) -> TradingState {
    let now = now_ms();
    let mut state = TradingState::new("BTC");
    state.set_market(Market {
        symbol: "BTC".to_string(),
        wire_symbol: "BTC".to_string(),
        dex: None,
        asset: AssetId::native_perp(0),
        kind: MarketKind::Perp,
        size_decimals: 5,
        max_leverage: Some(40),
        delisted: false,
        open_interest_cap: false,
    });
    state.apply_book(
        "BTC",
        Book {
            bid: dec("50000"),
            ask: dec("50001"),
        },
        now,
        Some(now),
    );
    state.apply_account(
        Account {
            value_usd: dec("1000"),
            available_margin_usd: dec("900"),
            margin_used_usd: dec("100"),
            notional_usd: dec("100"),
        },
        now,
        Some(now),
    );
    let size = dec(position_size);
    state.apply_position_snapshot(
        Position {
            symbol: "BTC".to_string(),
            size,
            entry_price: (size != Decimal::ZERO).then(|| dec("49000")),
            detail: None,
        },
        now,
        Some(now),
    );
    state.apply_active_asset(
        "BTC",
        ActiveAssetData {
            leverage: 10,
            leverage_cross: true,
            max_trade_sizes: [dec("100"), dec("100")],
            available_to_trade: [dec("100"), dec("100")],
            mark_price: dec("50000"),
        },
        now,
        Some(now),
    );
    state.apply_orders_snapshot("BTC", Vec::new(), now, Some(now));
    state.apply_account_mode(AccountMode::Standard, now, Some(now));
    state
}
