//! Versioned trading IPC contracts shared by hld and independent native clients.
//! No signing, exchange connectivity, or execution policy belongs in this crate.
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub const VERSION: u32 = 1;
pub const MAX_FRAME: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Hello,
    Open {
        instance_id: String,
        market: String,
    },
    Snapshot {
        instance_id: String,
        session_id: u64,
        after_fill_seq: u64,
    },
    Submit {
        instance_id: String,
        session_id: u64,
        sequence: u64,
        market: String,
        command: String,
    },
    Get {
        instance_id: String,
        command_id: u64,
    },
    Close {
        instance_id: String,
        session_id: u64,
    },
}

/// The outer envelope matches the existing IPC request/response framing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestEnvelope {
    Trading { request: Request },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum ResponseEnvelope {
    Trading(Response),
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Hello {
        version: u32,
        instance_id: String,
        network: String,
        account: Option<String>,
        read_only: bool,
        markets: Vec<Market>,
    },
    Opened {
        instance_id: String,
        session_id: u64,
        market: Market,
    },
    Snapshot(Box<Snapshot>),
    Submitted {
        instance_id: String,
        command_id: u64,
        sequence: u64,
        queue_depth: u32,
    },
    Command {
        instance_id: String,
        record: Box<CommandRecord>,
    },
    Closed,
    Error {
        instance_id: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Market {
    pub symbol: String,
    pub wire_symbol: String,
    pub kind: MarketKind,
    pub size_decimals: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    Perp,
    BuilderPerp,
    Spot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Timed<T> {
    pub value: T,
    pub local_ms: u64,
    pub exchange_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Health {
    pub ready: bool,
    pub halted: bool,
    pub reasons: Vec<String>,
    pub action_connected: Option<bool>,
    pub state_connected: Option<bool>,
    pub orders_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub instance_id: String,
    pub session_id: u64,
    pub observed_ms: u64,
    pub market: Market,
    pub health: Health,
    pub orders: Vec<Timed<Order>>,
    pub position: Option<Timed<Position>>,
    pub mark: Option<Timed<Decimal>>,
    pub balances: Vec<Timed<Balance>>,
    pub fills: Vec<Fill>,
    pub oldest_fill_seq: Option<u64>,
    pub latest_fill_seq: u64,
    pub queue_depth: u32,
    pub confirmations: Vec<Confirmation>,
    pub twaps: Vec<Timed<Twap>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Position {
    pub size: Decimal,
    pub entry_price: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub liquidation_price: Option<Decimal>,
    pub leverage: Option<u32>,
    pub cross: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Balance {
    pub coin: String,
    pub total: Decimal,
    pub hold: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Order {
    pub oid: u64,
    pub cloid: Option<String>,
    pub is_buy: bool,
    pub price: Decimal,
    pub size: Decimal,
    pub reduce_only: bool,
    pub kind: OrderKind,
    pub trailing: Option<Trailing>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    Limit,
    StopLoss,
    TakeProfit,
    TrailingStop,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trailing {
    pub retracement: String,
    pub activation: Option<Decimal>,
    pub best: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Twap {
    pub id: u64,
    pub is_buy: bool,
    pub size: Decimal,
    pub executed_size: Decimal,
    pub minutes: u64,
    pub reduce_only: bool,
    pub trigger_price: Option<Decimal>,
    pub trigger_above: Option<bool>,
    pub stop_price: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Fill {
    pub seq: u64,
    pub tid: u64,
    pub oid: u64,
    pub is_buy: bool,
    pub size: Decimal,
    pub price: Decimal,
    pub closed_pnl: Decimal,
    pub time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Confirmation {
    pub scope: String,
    pub summary: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandRecord {
    pub command_id: u64,
    pub command: String,
    pub sequence: Option<u64>,
    pub state: CommandState,
    pub submitted_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub response: Option<CommandOutput>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandState {
    Queued,
    Running,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandOutput {
    pub ok: bool,
    pub lines: Vec<String>,
    pub receipts: Vec<Receipt>,
    pub confirmation: Option<Confirmation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub id: u64,
    pub status: ExecutionStatus,
    pub items: Vec<OrderOutcome>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Accepted,
    Rejected,
    Ambiguous,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OrderOutcome {
    Success,
    AlreadyTerminal {
        message: String,
    },
    Resting {
        oid: u64,
        cloid: Option<String>,
    },
    Filled {
        oid: u64,
        total_size: String,
        average_price: String,
    },
    TwapRunning {
        twap_id: u64,
    },
    WaitingForFill,
    WaitingForTrigger,
    Error {
        message: String,
    },
}
