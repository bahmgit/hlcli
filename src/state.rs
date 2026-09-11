use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::protocol::{AssetId, MarketKind, PriceTick, TimeInForce};

pub const LOCAL_ORDER_OID_MIN: u64 = 1 << 63;
const FILL_HISTORY_CAP: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Market {
    pub symbol: String,
    pub wire_symbol: String,
    pub dex: Option<String>,
    pub asset: AssetId,
    pub kind: MarketKind,
    pub size_decimals: i64,
    pub max_leverage: Option<u32>,
    pub delisted: bool,
    pub open_interest_cap: bool,
}

impl Market {
    pub fn tick(&self) -> PriceTick {
        PriceTick::new(self.kind, self.size_decimals)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fresh<T> {
    pub value: T,
    pub local_ms: u64,
    pub exchange_ms: Option<u64>,
}

impl<T> Fresh<T> {
    pub fn age_ms(&self, now_ms: u64) -> u64 {
        now_ms.saturating_sub(self.local_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Book {
    pub bid: Decimal,
    pub ask: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub value_usd: Decimal,
    pub available_margin_usd: Decimal,
    pub margin_used_usd: Decimal,
    pub notional_usd: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountOverview {
    pub source: String,
    pub value_usd: Option<Decimal>,
    pub available_usd: Option<Decimal>,
    pub margin_used_usd: Option<Decimal>,
    pub notional_usd: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ActiveAssetData {
    pub leverage: u32,
    pub leverage_cross: bool,
    pub max_trade_sizes: [Decimal; 2],
    pub available_to_trade: [Decimal; 2],
    pub mark_price: Decimal,
}

impl Account {
    pub fn exposure(&self) -> Option<Decimal> {
        (self.value_usd > Decimal::ZERO).then(|| self.notional_usd / self.value_usd)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AccountMode {
    Standard,
    UnifiedAccount,
    PortfolioMargin,
    DexAbstraction,
}

impl AccountMode {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let normalized = raw
            .trim()
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .map(|ch| ch.to_ascii_lowercase())
            .collect::<String>();
        match normalized.as_str() {
            "default" | "disabled" | "standard" | "classic" | "i" => Ok(Self::Standard),
            "unified" | "unifiedaccount" | "u" => Ok(Self::UnifiedAccount),
            "portfolio" | "portfoliomargin" | "pm" | "p" => Ok(Self::PortfolioMargin),
            "dex" | "dexabstraction" => Ok(Self::DexAbstraction),
            _ => Err(format!("unsupported account mode {raw}")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::UnifiedAccount => "unifiedAccount",
            Self::PortfolioMargin => "portfolioMargin",
            Self::DexAbstraction => "dexAbstraction",
        }
    }

    pub fn agent_code(self) -> Result<&'static str, String> {
        match self {
            Self::Standard => Ok("i"),
            Self::UnifiedAccount => Ok("u"),
            Self::PortfolioMargin => Ok("p"),
            Self::DexAbstraction => Err(
                "dexAbstraction cannot be set with agentSetAbstraction; choose standard, unifiedAccount, or portfolioMargin"
                    .to_string(),
            ),
        }
    }

    pub fn account_value_source(self) -> &'static str {
        match self {
            Self::UnifiedAccount | Self::PortfolioMargin => "spotClearinghouseState",
            Self::Standard | Self::DexAbstraction => "clearinghouseState",
        }
    }

    pub fn agent_transition_code_from(self, current: Self) -> Result<&'static str, String> {
        if self == current {
            return Err(format!("account mode is already {}", self.as_str()));
        }
        if self == Self::Standard {
            return Err(
                "returning to standard account mode requires the main user signer; hld intentionally holds only an API wallet"
                    .to_string(),
            );
        }
        if current != Self::Standard {
            return Err(format!(
                "API-wallet account mode transitions require current mode standard; current={}. Changing an already abstracted account requires the main user signer",
                current.as_str()
            ));
        }
        self.agent_code()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PositionDetail {
    pub unrealized_pnl: Decimal,
    pub return_on_equity: Decimal,
    pub liquidation_px: Option<Decimal>,
    pub margin_used: Decimal,
    pub position_value: Decimal,
    pub leverage: u32,
    pub leverage_cross: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub size: Decimal,
    pub entry_price: Option<Decimal>,
    pub detail: Option<PositionDetail>,
}

impl Position {
    pub fn empty(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            size: Decimal::ZERO,
            entry_price: None,
            detail: None,
        }
    }

    pub fn flat(&self) -> bool {
        self.size == Decimal::ZERO
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub seq: u64,
    pub tid: u64,
    pub oid: u64,
    pub symbol: String,
    pub is_buy: bool,
    pub size: Decimal,
    pub price: Decimal,
    pub closed_pnl: Decimal,
    pub time_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoveredOrderOutcome {
    pub oid: Option<u64>,
    pub filled_size: Decimal,
    pub terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotBalance {
    pub coin: String,
    pub token: Option<u64>,
    pub total: Decimal,
    pub hold: Decimal,
    pub entry_ntl: Decimal,
    pub ltv: Option<Decimal>,
    pub supplied: Option<Decimal>,
    pub available_after_maintenance: Option<Decimal>,
    pub portfolio_borrow_ratio: Option<Decimal>,
}

impl SpotBalance {
    pub fn available(&self) -> Decimal {
        (self.total - self.hold).normalize()
    }

    pub fn nonzero(&self) -> bool {
        self.total != Decimal::ZERO
            || self.hold != Decimal::ZERO
            || self.supplied.is_some_and(|value| value != Decimal::ZERO)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BorrowLend {
    pub token: u64,
    pub borrow_value: Decimal,
    pub supply_value: Decimal,
}

impl BorrowLend {
    pub fn nonzero(&self) -> bool {
        self.borrow_value != Decimal::ZERO || self.supply_value != Decimal::ZERO
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BalanceSummary {
    pub portfolio_margin_enabled: bool,
    pub portfolio_margin_ratio: Option<Decimal>,
    pub spot_value_usd: Option<Decimal>,
    pub spot_available_usd: Option<Decimal>,
    pub spot_unpriced_count: usize,
    pub borrow_lend_health: Option<String>,
    pub borrow_lend_health_factor: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Twap {
    pub symbol: String,
    pub dex: String,
    pub is_buy: bool,
    pub size: Decimal,
    pub executed_size: Decimal,
    pub minutes: u64,
    pub reduce_only: bool,
    pub randomize: bool,
    pub submitted_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderKind {
    Limit,
    StopLoss,
    TakeProfit,
    TrailingStop,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub symbol: String,
    pub oid: u64,
    pub cloid: Option<String>,
    pub is_buy: bool,
    pub price: Decimal,
    pub size: Decimal,
    pub reduce_only: bool,
    pub kind: OrderKind,
    pub tif: Option<TimeInForce>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessLimits {
    pub book_ms: u64,
    pub account_ms: u64,
    pub position_ms: u64,
    pub orders_ms: u64,
    pub active_asset_ms: u64,
}

impl Default for FreshnessLimits {
    fn default() -> Self {
        Self {
            book_ms: 5_000,
            account_ms: 10_000,
            position_ms: 10_000,
            orders_ms: 10_000,
            active_asset_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Readiness {
    pub ready: bool,
    pub reasons: Vec<String>,
}

impl Readiness {
    fn ready() -> Self {
        Self {
            ready: true,
            reasons: Vec::new(),
        }
    }

    fn reject(reasons: Vec<String>) -> Self {
        Self {
            ready: false,
            reasons,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradingState {
    pub active: String,
    pub markets: BTreeMap<String, Market>,
    pub book: BTreeMap<String, Fresh<Book>>,
    pub account: Option<Fresh<Account>>,
    pub positions: BTreeMap<String, Fresh<Position>>,
    pub orders: BTreeMap<u64, Fresh<Order>>,
    pub twaps: BTreeMap<u64, Fresh<Twap>>,
    pub spot_balances: BTreeMap<String, Fresh<SpotBalance>>,
    pub spot_price_wires: BTreeMap<u64, String>,
    pub spot_marks_usd: BTreeMap<u64, Decimal>,
    pub borrow_lend: BTreeMap<u64, Fresh<BorrowLend>>,
    pub balance_summary: Option<Fresh<BalanceSummary>>,
    pub spot_capacity_ms: Option<Fresh<()>>,
    pub active_assets: BTreeMap<String, Fresh<ActiveAssetData>>,
    pub account_mode: Option<Fresh<AccountMode>>,
    pub required_account_mode: Option<AccountMode>,
    pub recent_fills: VecDeque<Fill>,
    #[serde(default)]
    pub recovered_order_outcomes: BTreeMap<String, RecoveredOrderOutcome>,
    pub fill_session_start_ms: u64,
    position_floor_ms: BTreeMap<String, u64>,
    order_floor_ms: BTreeMap<String, u64>,
    fill_seq: u64,
    fill_seen: HashSet<u64>,
}

impl TradingState {
    pub fn new(active: impl Into<String>) -> Self {
        Self {
            active: active.into(),
            markets: BTreeMap::new(),
            book: BTreeMap::new(),
            account: None,
            positions: BTreeMap::new(),
            orders: BTreeMap::new(),
            twaps: BTreeMap::new(),
            spot_balances: BTreeMap::new(),
            spot_price_wires: BTreeMap::new(),
            spot_marks_usd: BTreeMap::new(),
            borrow_lend: BTreeMap::new(),
            balance_summary: None,
            spot_capacity_ms: None,
            active_assets: BTreeMap::new(),
            account_mode: None,
            required_account_mode: None,
            recent_fills: VecDeque::new(),
            recovered_order_outcomes: BTreeMap::new(),
            fill_session_start_ms: 0,
            position_floor_ms: BTreeMap::new(),
            order_floor_ms: BTreeMap::new(),
            fill_seq: 0,
            fill_seen: HashSet::new(),
        }
    }

    pub fn set_market(&mut self, market: Market) {
        self.markets.insert(market.symbol.clone(), market);
    }

    pub fn switch_active(&mut self, symbol: impl Into<String>) -> Result<(), String> {
        let symbol = symbol.into();
        if !self.markets.contains_key(&symbol) {
            return Err(format!("unknown market {symbol}"));
        }
        if self.active != symbol {
            self.book.remove(&symbol);
            self.positions.remove(&symbol);
            self.active_assets.remove(&symbol);
            self.position_floor_ms.remove(&symbol);
            self.order_floor_ms.remove(&symbol);
            self.orders.retain(|_, order| order.value.symbol != symbol);
        }
        self.active = symbol;
        Ok(())
    }

    pub fn switch_active_preserving_state(
        &mut self,
        symbol: impl Into<String>,
    ) -> Result<(), String> {
        let symbol = symbol.into();
        if !self.markets.contains_key(&symbol) {
            return Err(format!("unknown market {symbol}"));
        }
        self.active = symbol;
        Ok(())
    }

    pub fn apply_book(
        &mut self,
        symbol: &str,
        book: Book,
        local_ms: u64,
        exchange_ms: Option<u64>,
    ) {
        self.book.insert(
            symbol.to_string(),
            Fresh {
                value: book,
                local_ms,
                exchange_ms,
            },
        );
    }

    pub fn confirm_book_subscription(&mut self, symbol: &str, local_ms: u64) -> bool {
        let Some(book) = self.book.get_mut(symbol) else {
            return false;
        };
        book.local_ms = local_ms;
        true
    }

    pub fn apply_account(&mut self, account: Account, local_ms: u64, exchange_ms: Option<u64>) {
        self.account = Some(Fresh {
            value: account,
            local_ms,
            exchange_ms,
        });
    }

    pub fn apply_account_mode(
        &mut self,
        mode: AccountMode,
        local_ms: u64,
        exchange_ms: Option<u64>,
    ) {
        if self
            .account_mode
            .as_ref()
            .is_none_or(|current| current.value != mode)
        {
            self.spot_capacity_ms = None;
        }
        self.account_mode = Some(Fresh {
            value: mode,
            local_ms,
            exchange_ms,
        });
    }

    pub fn set_required_account_mode(&mut self, mode: Option<AccountMode>) {
        self.required_account_mode = mode;
    }

    pub fn apply_active_asset(
        &mut self,
        symbol: &str,
        data: ActiveAssetData,
        local_ms: u64,
        exchange_ms: Option<u64>,
    ) {
        self.active_assets.insert(
            symbol.to_string(),
            Fresh {
                value: data,
                local_ms,
                exchange_ms,
            },
        );
    }

    pub fn apply_spot_capacity(&mut self, local_ms: u64, exchange_ms: Option<u64>) {
        self.spot_capacity_ms = Some(Fresh {
            value: (),
            local_ms,
            exchange_ms,
        });
    }

    pub fn apply_position_snapshot(
        &mut self,
        position: Position,
        local_ms: u64,
        exchange_ms: Option<u64>,
    ) -> bool {
        if self
            .markets
            .get(&position.symbol)
            .is_some_and(|market| market.kind == MarketKind::Spot)
        {
            self.positions.remove(&position.symbol);
            return false;
        }
        if self.is_stale(&self.position_floor_ms, &position.symbol, exchange_ms) {
            return false;
        }
        if position.flat() {
            self.orders.retain(|_, order| {
                order.value.symbol != position.symbol || order.value.oid < LOCAL_ORDER_OID_MIN
            });
        }
        self.positions.insert(
            position.symbol.clone(),
            Fresh {
                value: position,
                local_ms,
                exchange_ms,
            },
        );
        true
    }

    pub fn reconcile_positions_for_dex(
        &mut self,
        present: &BTreeSet<String>,
        dex: Option<&str>,
        exchange_ms: u64,
    ) {
        let closed: Vec<String> = self
            .positions
            .keys()
            .filter(|symbol| {
                !present.contains(*symbol)
                    && self.markets.get(*symbol).is_some_and(|market| {
                        matches!(market.kind, MarketKind::Perp | MarketKind::BuilderPerp)
                            && same_dex(market.dex.as_deref(), dex)
                    })
                    && self
                        .position_floor_ms
                        .get(*symbol)
                        .is_none_or(|floor| exchange_ms >= *floor)
            })
            .cloned()
            .collect();
        for symbol in closed {
            self.positions.remove(&symbol);
        }
    }

    pub fn apply_fill_position(&mut self, position: Position, local_ms: u64, exchange_ms: u64) {
        if self
            .markets
            .get(&position.symbol)
            .is_some_and(|market| market.kind == MarketKind::Spot)
        {
            self.positions.remove(&position.symbol);
            return;
        }
        self.position_floor_ms
            .insert(position.symbol.clone(), exchange_ms);
        if position.flat() {
            self.orders.retain(|_, order| {
                order.value.symbol != position.symbol || order.value.oid < LOCAL_ORDER_OID_MIN
            });
        }
        self.positions.insert(
            position.symbol.clone(),
            Fresh {
                value: position,
                local_ms,
                exchange_ms: Some(exchange_ms),
            },
        );
    }

    pub fn apply_orders_snapshot(
        &mut self,
        symbol: &str,
        orders: Vec<Order>,
        local_ms: u64,
        exchange_ms: Option<u64>,
    ) -> bool {
        if self.is_stale(&self.order_floor_ms, symbol, exchange_ms) {
            return false;
        }
        self.order_floor_ms
            .insert(symbol.to_string(), exchange_ms.unwrap_or(local_ms));
        self.orders.retain(|_, order| {
            order.value.symbol != symbol || order.value.oid >= LOCAL_ORDER_OID_MIN
        });
        for order in orders {
            if let Some(cloid) = order.cloid.as_deref() {
                self.orders.retain(|_, existing| {
                    existing.value.symbol != symbol
                        || existing.value.oid < LOCAL_ORDER_OID_MIN
                        || existing.value.cloid.as_deref() != Some(cloid)
                });
            }
            self.orders.insert(
                order.oid,
                Fresh {
                    value: order,
                    local_ms,
                    exchange_ms,
                },
            );
        }
        true
    }

    pub fn apply_resting_receipt(&mut self, order: Order, local_ms: u64, exchange_ms: u64) {
        self.order_floor_ms
            .insert(order.symbol.clone(), exchange_ms);
        self.orders.insert(
            order.oid,
            Fresh {
                value: order,
                local_ms,
                exchange_ms: Some(exchange_ms),
            },
        );
    }

    pub fn apply_cancel_receipt(&mut self, symbol: &str, oid: u64, exchange_ms: u64) {
        self.order_floor_ms.insert(symbol.to_string(), exchange_ms);
        self.orders.remove(&oid);
    }

    pub fn apply_twap(&mut self, id: u64, twap: Twap, local_ms: u64, exchange_ms: Option<u64>) {
        self.twaps.insert(
            id,
            Fresh {
                value: twap,
                local_ms,
                exchange_ms,
            },
        );
    }

    pub fn replace_twaps_for_dex(
        &mut self,
        dex: &str,
        twaps: Vec<(u64, Twap)>,
        local_ms: u64,
        exchange_ms: Option<u64>,
    ) {
        self.twaps.retain(|_, twap| twap.value.dex != dex);
        for (id, twap) in twaps {
            self.apply_twap(id, twap, local_ms, exchange_ms);
        }
    }

    pub fn forget_twap(&mut self, id: u64) {
        self.twaps.remove(&id);
    }

    pub fn apply_spot_balances(
        &mut self,
        balances: Vec<SpotBalance>,
        summary: BalanceSummary,
        local_ms: u64,
        exchange_ms: Option<u64>,
    ) {
        self.spot_balances.clear();
        for balance in balances {
            self.spot_balances.insert(
                balance_key(&balance),
                Fresh {
                    value: balance,
                    local_ms,
                    exchange_ms,
                },
            );
        }
        self.balance_summary = Some(Fresh {
            value: summary,
            local_ms,
            exchange_ms,
        });
    }

    pub fn apply_borrow_lend(
        &mut self,
        entries: Vec<BorrowLend>,
        health: Option<String>,
        health_factor: Option<Decimal>,
        local_ms: u64,
        exchange_ms: Option<u64>,
    ) {
        self.borrow_lend.clear();
        for entry in entries {
            self.borrow_lend.insert(
                entry.token,
                Fresh {
                    value: entry,
                    local_ms,
                    exchange_ms,
                },
            );
        }
        if let Some(summary) = &mut self.balance_summary {
            summary.value.borrow_lend_health = health;
            summary.value.borrow_lend_health_factor = health_factor;
            summary.local_ms = local_ms;
            summary.exchange_ms = exchange_ms;
        } else {
            self.balance_summary = Some(Fresh {
                value: BalanceSummary {
                    portfolio_margin_enabled: false,
                    portfolio_margin_ratio: None,
                    spot_value_usd: None,
                    spot_available_usd: None,
                    spot_unpriced_count: 0,
                    borrow_lend_health: health,
                    borrow_lend_health_factor: health_factor,
                },
                local_ms,
                exchange_ms,
            });
        }
    }

    pub fn record_fill(&mut self, mut fill: Fill) {
        if fill.time_ms < self.fill_session_start_ms {
            return;
        }
        if !self.fill_seen.insert(fill.tid) {
            return;
        }
        self.fill_seq += 1;
        fill.seq = self.fill_seq;
        self.recent_fills.push_back(fill);
        while self.recent_fills.len() > FILL_HISTORY_CAP {
            self.recent_fills.pop_front();
        }
    }

    pub fn record_recovered_order_outcome(
        &mut self,
        cloid: String,
        outcome: RecoveredOrderOutcome,
    ) {
        self.recovered_order_outcomes.insert(cloid, outcome);
    }

    pub fn fills_after(&self, seq: u64) -> Vec<Fill> {
        self.recent_fills
            .iter()
            .filter(|fill| fill.seq > seq)
            .cloned()
            .collect()
    }

    pub fn active_position(&self) -> Option<&Fresh<Position>> {
        self.position(&self.active)
    }

    pub fn position(&self, symbol: &str) -> Option<&Fresh<Position>> {
        self.positions.get(symbol)
    }

    pub fn spot_balance(&self, coin: &str) -> Option<&Fresh<SpotBalance>> {
        self.spot_balances
            .values()
            .find(|balance| balance.value.coin.eq_ignore_ascii_case(coin))
    }

    pub fn account_value_usd(&self) -> Option<Decimal> {
        let value = self.account_overview()?.value_usd?;
        (value > Decimal::ZERO).then_some(value)
    }

    pub fn account_overview(&self) -> Option<AccountOverview> {
        let source = self
            .account_mode
            .map(|mode| mode.value.account_value_source())
            .unwrap_or("clearinghouseState");
        let account = self.account.as_ref().map(|account| account.value);
        let overview = if source == "spotClearinghouseState" {
            let balance = self.balance_summary.as_ref().map(|summary| &summary.value);
            AccountOverview {
                source: source.to_string(),
                value_usd: balance.and_then(|summary| summary.spot_value_usd),
                available_usd: balance.and_then(|summary| summary.spot_available_usd),
                margin_used_usd: account.map(|account| account.margin_used_usd),
                notional_usd: account.map(|account| account.notional_usd),
            }
        } else {
            AccountOverview {
                source: source.to_string(),
                value_usd: account.map(|account| account.value_usd),
                available_usd: account.map(|account| account.available_margin_usd),
                margin_used_usd: account.map(|account| account.margin_used_usd),
                notional_usd: account.map(|account| account.notional_usd),
            }
        };
        (overview.value_usd.is_some()
            || overview.available_usd.is_some()
            || overview.margin_used_usd.is_some()
            || overview.notional_usd.is_some())
        .then_some(overview)
    }

    pub fn active_orders(&self) -> impl Iterator<Item = &Fresh<Order>> {
        self.orders_for(&self.active)
    }

    pub fn orders_for<'a>(&'a self, symbol: &'a str) -> impl Iterator<Item = &'a Fresh<Order>> {
        self.orders
            .values()
            .filter(move |order| order.value.symbol == symbol)
    }

    pub fn active_orders_ready(&self, now_ms: u64, max_age_ms: u64) -> bool {
        self.orders_ready(&self.active, now_ms, max_age_ms)
    }

    pub fn orders_ready(&self, symbol: &str, now_ms: u64, max_age_ms: u64) -> bool {
        let mut has_order = false;
        for order in self.orders_for(symbol) {
            has_order = true;
            if order.value.oid < LOCAL_ORDER_OID_MIN && order.age_ms(now_ms) > max_age_ms {
                return false;
            }
        }
        if has_order {
            return true;
        }
        self.order_floor_ms
            .get(symbol)
            .is_some_and(|updated_ms| now_ms.saturating_sub(*updated_ms) <= max_age_ms)
    }

    pub fn active_sync_reasons(&self, now_ms: u64, limits: FreshnessLimits) -> Vec<String> {
        self.sync_reasons(&self.active, now_ms, limits)
    }

    pub fn sync_reasons(&self, symbol: &str, now_ms: u64, limits: FreshnessLimits) -> Vec<String> {
        let mut reasons = Vec::new();
        let Some(market) = self.markets.get(symbol) else {
            return vec![format!("unknown market {symbol}")];
        };
        require_fresh(
            &mut reasons,
            "book",
            self.book.get(symbol),
            now_ms,
            limits.book_ms,
        );
        if matches!(market.kind, MarketKind::Perp | MarketKind::BuilderPerp) {
            require_fresh(
                &mut reasons,
                "position",
                self.positions.get(symbol),
                now_ms,
                limits.position_ms,
            );
            require_fresh(
                &mut reasons,
                "activeAssetData",
                self.active_assets.get(symbol),
                now_ms,
                limits.active_asset_ms,
            );
        }
        if !self.orders_ready(symbol, now_ms, limits.orders_ms) {
            reasons.push("orders unavailable or stale".to_string());
        }
        reasons
    }

    pub fn open_orders(&self) -> impl Iterator<Item = &Fresh<Order>> {
        self.orders.values()
    }

    pub fn open_positions(&self) -> impl Iterator<Item = &Fresh<Position>> {
        self.positions
            .values()
            .filter(|position| !position.value.flat())
    }

    pub fn active_twaps(&self) -> impl Iterator<Item = (&u64, &Fresh<Twap>)> {
        self.twaps_for(&self.active)
    }

    pub fn twaps_for<'a>(
        &'a self,
        symbol: &'a str,
    ) -> impl Iterator<Item = (&'a u64, &'a Fresh<Twap>)> {
        self.twaps
            .iter()
            .filter(move |(_, twap)| twap.value.symbol == symbol)
    }

    pub fn readiness(
        &self,
        now_ms: u64,
        limits: FreshnessLimits,
        needs_orders: bool,
        exposure_increasing: bool,
    ) -> Readiness {
        self.readiness_for(
            &self.active,
            now_ms,
            limits,
            needs_orders,
            exposure_increasing,
        )
    }

    pub fn readiness_for(
        &self,
        symbol: &str,
        now_ms: u64,
        limits: FreshnessLimits,
        needs_orders: bool,
        exposure_increasing: bool,
    ) -> Readiness {
        let mut reasons = Vec::new();
        let Some(market) = self.markets.get(symbol) else {
            return Readiness::reject(vec![format!("unknown market {symbol}")]);
        };
        if market.delisted {
            reasons.push(format!("{symbol} delisted"));
        }
        if let Some(required) = self.required_account_mode {
            match self.account_mode {
                Some(actual) if actual.age_ms(now_ms) > limits.account_ms => reasons.push(format!(
                    "account_mode stale age_ms={}",
                    actual.age_ms(now_ms)
                )),
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
        if exposure_increasing && market.open_interest_cap {
            reasons.push(format!("{symbol} at open-interest cap"));
        }
        require_fresh(
            &mut reasons,
            "book",
            self.book.get(symbol),
            now_ms,
            limits.book_ms,
        );
        if exposure_increasing {
            require_fresh(
                &mut reasons,
                "account",
                self.account.as_ref(),
                now_ms,
                limits.account_ms,
            );
            if matches!(market.kind, MarketKind::Perp | MarketKind::BuilderPerp) {
                require_fresh(
                    &mut reasons,
                    "position",
                    self.positions.get(symbol),
                    now_ms,
                    limits.position_ms,
                );
                require_fresh(
                    &mut reasons,
                    "activeAssetData",
                    self.active_assets.get(symbol),
                    now_ms,
                    limits.active_asset_ms,
                );
            } else {
                require_fresh(
                    &mut reasons,
                    "spot balances",
                    self.balance_summary.as_ref(),
                    now_ms,
                    limits.account_ms,
                );
                if self
                    .account_mode
                    .is_some_and(|mode| mode.value == AccountMode::PortfolioMargin)
                {
                    require_fresh(
                        &mut reasons,
                        "spot portfolio capacity",
                        self.spot_capacity_ms.as_ref(),
                        now_ms,
                        limits.account_ms,
                    );
                }
            }
        } else if self.position(symbol).is_none() {
            reasons.push("unknown position for reduce-only action".to_string());
        }
        if needs_orders && !self.orders_ready(symbol, now_ms, limits.orders_ms) {
            reasons.push("orders unavailable or stale".to_string());
        }
        if reasons.is_empty() {
            Readiness::ready()
        } else {
            Readiness::reject(reasons)
        }
    }

    fn is_stale(
        &self,
        floors: &BTreeMap<String, u64>,
        symbol: &str,
        exchange_ms: Option<u64>,
    ) -> bool {
        match (floors.get(symbol), exchange_ms) {
            (Some(floor), Some(exchange_ms)) => exchange_ms < *floor,
            _ => false,
        }
    }
}

fn same_dex(left: Option<&str>, right: Option<&str>) -> bool {
    left.unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case(right.unwrap_or_default().trim())
}

fn balance_key(balance: &SpotBalance) -> String {
    balance
        .token
        .map(|token| format!("#{token}"))
        .unwrap_or_else(|| balance.coin.trim().to_ascii_uppercase())
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
