use std::{path::Path, sync::Arc, time::Duration};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

use crate::{
    exchange::ActionConnectionHealth,
    execution::ExecutionControl,
    metrics::{CountersSnapshot, Metrics},
    runtime::{
        CommandExecutionRecord, CommandQueue, CommandResultsResponse, CommandSubmitResponse,
        MarketSessionResponse,
    },
    state::{
        BalanceSummary, BorrowLend, Fill, FreshnessLimits, Order, Position, SpotBalance,
        TradingState,
    },
};

const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IpcRequest {
    Health,
    MarketSessionHealth { session_id: u64 },
    Status,
    Orders,
    Position,
    Balances,
    Markets,
    Metrics,
    Keybinds,
    Fills { after_seq: u64 },
    FillsScoped { session_id: u64, after_seq: u64 },
    MarketSessionOpen { market: String },
    MarketSessionGet { session_id: u64 },
    MarketSessionClose { session_id: u64 },
    CommandSubmit { command: String },
    CommandSubmitScoped { session_id: u64, command: String },
    CommandResults { after_command_id: u64, limit: u32 },
    CommandGet { command_id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum IpcResponse {
    Health(HealthPayload),
    Status(StatusPayload),
    Orders(OrdersPayload),
    Position(PositionPayload),
    Balances(BalancesPayload),
    Markets(MarketsPayload),
    Metrics(MetricsPayload),
    Keybinds(KeybindsPayload),
    Fills(FillsPayload),
    MarketSession(MarketSessionResponse),
    MarketSessionClosed,
    CommandSubmit(CommandSubmitResponse),
    CommandResults(CommandResultsResponse),
    CommandGet(CommandExecutionRecord),
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HealthPayload {
    pub ok: bool,
    pub active: String,
    pub reasons: Vec<String>,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub action_connected: Option<bool>,
    pub state_feed_connected: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatusPayload {
    pub active: String,
    pub market_count: usize,
    pub open_orders: usize,
    pub twap_ids: Vec<u64>,
    pub position: Option<Position>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OrdersPayload {
    pub orders: Vec<Order>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FillsPayload {
    pub fills: Vec<Fill>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PositionPayload {
    pub positions: Vec<Position>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BalancesPayload {
    pub summary: Option<BalanceSummary>,
    pub spot_balances: Vec<SpotBalance>,
    pub borrow_lend: Vec<BorrowLend>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MarketsPayload {
    pub markets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MetricsPayload {
    pub counters: CountersSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KeybindsPayload {
    pub keybinds: std::collections::BTreeMap<String, String>,
}

#[derive(Clone)]
pub struct IpcApp {
    state: Arc<tokio::sync::RwLock<TradingState>>,
    metrics: Metrics,
    queue: Arc<CommandQueue>,
    execution: ExecutionControl,
    action_connection: Option<ActionConnectionHealth>,
}

impl IpcApp {
    pub fn new(
        state: Arc<tokio::sync::RwLock<TradingState>>,
        metrics: Metrics,
        queue: Arc<CommandQueue>,
    ) -> Self {
        Self {
            state,
            metrics,
            queue,
            execution: ExecutionControl::default(),
            action_connection: None,
        }
    }

    pub fn with_execution_control(mut self, execution: ExecutionControl) -> Self {
        self.execution = execution;
        self
    }

    pub fn with_action_connection(
        mut self,
        action_connection: Option<ActionConnectionHealth>,
    ) -> Self {
        self.action_connection = action_connection;
        self
    }

    async fn health_payload(
        &self,
        active: String,
        readiness: crate::state::Readiness,
    ) -> HealthPayload {
        let execution = self.execution.health().await;
        let action_connected = self
            .action_connection
            .as_ref()
            .map(ActionConnectionHealth::connected);
        let state_feed_connected = self.metrics.state_feed_connected();
        let mut reasons = readiness.reasons;
        if let Some(reason) = execution.reason.as_deref() {
            reasons.push(format!("execution halted: {reason}"));
        }
        if action_connected == Some(false) {
            reasons.push("action feed disconnected".to_string());
        }
        if state_feed_connected == Some(false) {
            reasons.push("state feed disconnected".to_string());
        }
        HealthPayload {
            ok: readiness.ready
                && !execution.halted
                && action_connected.unwrap_or(true)
                && state_feed_connected.unwrap_or(true),
            active,
            reasons,
            halted: execution.halted,
            halt_reason: execution.reason,
            action_connected,
            state_feed_connected,
        }
    }

    pub async fn dispatch(&self, request: IpcRequest) -> IpcResponse {
        match request {
            IpcRequest::Health => {
                let state = self.state.read().await;
                let readiness = readiness(&state);
                let active = state.active.clone();
                drop(state);
                IpcResponse::Health(self.health_payload(active, readiness).await)
            }
            IpcRequest::MarketSessionHealth { session_id } => {
                let session = match self.queue.market_session(session_id).await {
                    Ok(session) => session,
                    Err(message) => return IpcResponse::Error { message },
                };
                let state = self.state.read().await;
                let readiness = readiness_for(&state, &session.active);
                drop(state);
                IpcResponse::Health(self.health_payload(session.active, readiness).await)
            }
            IpcRequest::Status => {
                let state = self.state.read().await;
                let active = state.active.clone();
                let market_count = state.markets.len();
                let open_orders = state.open_orders().count();
                let twap_ids = state.active_twaps().map(|(id, _)| *id).collect();
                let position = state
                    .active_position()
                    .map(|position| position.value.clone());
                drop(state);
                IpcResponse::Status(StatusPayload {
                    active,
                    market_count,
                    open_orders,
                    twap_ids,
                    position,
                })
            }
            IpcRequest::Orders => {
                let state = self.state.read().await;
                IpcResponse::Orders(OrdersPayload {
                    orders: state
                        .open_orders()
                        .map(|order| order.value.clone())
                        .collect(),
                })
            }
            IpcRequest::Position => {
                let state = self.state.read().await;
                IpcResponse::Position(PositionPayload {
                    positions: state
                        .open_positions()
                        .map(|position| position.value.clone())
                        .collect(),
                })
            }
            IpcRequest::Balances => {
                let state = self.state.read().await;
                IpcResponse::Balances(BalancesPayload {
                    summary: state
                        .balance_summary
                        .as_ref()
                        .map(|summary| summary.value.clone()),
                    spot_balances: state
                        .spot_balances
                        .values()
                        .filter(|balance| balance.value.nonzero())
                        .map(|balance| balance.value.clone())
                        .collect(),
                    borrow_lend: state
                        .borrow_lend
                        .values()
                        .filter(|entry| entry.value.nonzero())
                        .map(|entry| entry.value.clone())
                        .collect(),
                })
            }
            IpcRequest::Markets => {
                let state = self.state.read().await;
                IpcResponse::Markets(MarketsPayload {
                    markets: state.markets.keys().cloned().collect(),
                })
            }
            IpcRequest::Fills { after_seq } => {
                let state = self.state.read().await;
                IpcResponse::Fills(FillsPayload {
                    fills: state.fills_after(after_seq),
                })
            }
            IpcRequest::FillsScoped {
                session_id,
                after_seq,
            } => {
                let symbol = match self.queue.market_session(session_id).await {
                    Ok(session) => session.active,
                    Err(message) => return IpcResponse::Error { message },
                };
                let state = self.state.read().await;
                IpcResponse::Fills(FillsPayload {
                    fills: state
                        .fills_after(after_seq)
                        .into_iter()
                        .filter(|fill| fill.symbol == symbol)
                        .collect(),
                })
            }
            IpcRequest::MarketSessionOpen { market } => {
                match self.queue.open_market_session(market).await {
                    Ok(session) => IpcResponse::MarketSession(session),
                    Err(message) => IpcResponse::Error { message },
                }
            }
            IpcRequest::MarketSessionGet { session_id } => {
                match self.queue.market_session(session_id).await {
                    Ok(session) => IpcResponse::MarketSession(session),
                    Err(message) => IpcResponse::Error { message },
                }
            }
            IpcRequest::MarketSessionClose { session_id } => {
                match self.queue.close_market_session(session_id).await {
                    Ok(()) => IpcResponse::MarketSessionClosed,
                    Err(message) => IpcResponse::Error { message },
                }
            }
            IpcRequest::Metrics => IpcResponse::Metrics(MetricsPayload {
                counters: self.metrics.snapshot(),
            }),
            IpcRequest::Keybinds => IpcResponse::Keybinds(KeybindsPayload {
                keybinds: self.queue.keybinds().await,
            }),
            IpcRequest::CommandSubmit { command } => {
                IpcResponse::CommandSubmit(self.queue.submit(command).await)
            }
            IpcRequest::CommandSubmitScoped {
                session_id,
                command,
            } => match self.queue.submit_scoped(session_id, command).await {
                Ok(response) => IpcResponse::CommandSubmit(response),
                Err(message) => IpcResponse::Error { message },
            },
            IpcRequest::CommandResults {
                after_command_id,
                limit,
            } => IpcResponse::CommandResults(self.queue.results(after_command_id, limit).await),
            IpcRequest::CommandGet { command_id } => match self.queue.get(command_id).await {
                Some(record) => IpcResponse::CommandGet(record),
                None => IpcResponse::Error {
                    message: format!("unknown command id {command_id}"),
                },
            },
        }
    }
}

pub async fn serve(path: &Path, app: IpcApp, max_frame_bytes: usize) -> anyhow::Result<()> {
    let listener = bind(path).await?;
    serve_bound(listener, app, max_frame_bytes).await
}

pub async fn bind(path: &Path) -> anyhow::Result<UnixListener> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                anyhow::ensure!(
                    metadata.file_type().is_socket(),
                    "refusing to remove non-socket IPC path {}",
                    path.display()
                );
            }
            match UnixStream::connect(path).await {
                Ok(_) => anyhow::bail!("IPC socket is already active at {}", path.display()),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    tokio::fs::remove_file(path).await?;
                }
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("probe existing IPC socket {}", path.display()));
                }
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    let listener = UnixListener::bind(path).with_context(|| format!("bind {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(listener)
}

pub async fn serve_bound(
    listener: UnixListener,
    app: IpcApp,
    max_frame_bytes: usize,
) -> anyhow::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, app, max_frame_bytes.max(1024)).await {
                eprintln!("ipc connection error: {err:#}");
            }
        });
    }
}

fn readiness(state: &TradingState) -> crate::state::Readiness {
    state.readiness(
        now_ms(),
        FreshnessLimits::default(),
        true,
        state
            .active_position()
            .is_none_or(|position| position.value.flat()),
    )
}

fn readiness_for(state: &TradingState, symbol: &str) -> crate::state::Readiness {
    state.readiness_for(
        symbol,
        now_ms(),
        FreshnessLimits::default(),
        true,
        state
            .position(symbol)
            .is_none_or(|position| position.value.flat()),
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn handle_connection(
    mut stream: UnixStream,
    app: IpcApp,
    max_frame_bytes: usize,
) -> anyhow::Result<()> {
    loop {
        let payload = match tokio::time::timeout(
            IDLE_TIMEOUT,
            read_frame(&mut stream, max_frame_bytes),
        )
        .await
        {
            Ok(Ok(payload)) => payload,
            Ok(Err(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => return Ok(()),
        };
        let response = match rmp_serde::from_slice::<IpcRequest>(&payload) {
            Ok(request) => app.dispatch(request).await,
            Err(err) => IpcResponse::Error {
                message: format!("decode request: {err}"),
            },
        };
        tokio::time::timeout(
            WRITE_TIMEOUT,
            write_frame(
                &mut stream,
                &rmp_serde::to_vec_named(&response)?,
                max_frame_bytes,
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("ipc write timeout"))??;
    }
}

pub async fn request(
    path: &Path,
    request: &IpcRequest,
    max_frame_bytes: usize,
) -> anyhow::Result<IpcResponse> {
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        let mut stream = UnixStream::connect(path).await?;
        write_frame(
            &mut stream,
            &rmp_serde::to_vec_named(request)?,
            max_frame_bytes,
        )
        .await?;
        let payload = read_frame(&mut stream, max_frame_bytes).await?;
        rmp_serde::from_slice(&payload).context("decode ipc response")
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "IPC request timed out after {}ms",
            REQUEST_TIMEOUT.as_millis()
        )
    })?
}

async fn read_frame(stream: &mut UnixStream, max_frame_bytes: usize) -> std::io::Result<Vec<u8>> {
    let len = stream.read_u32().await? as usize;
    if len == 0 || len > max_frame_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("ipc frame length {len} exceeds {max_frame_bytes}"),
        ));
    }
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn write_frame(
    stream: &mut UnixStream,
    payload: &[u8],
    max_frame_bytes: usize,
) -> std::io::Result<()> {
    if payload.is_empty() || payload.len() > max_frame_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "ipc frame length {} exceeds {max_frame_bytes}",
                payload.len()
            ),
        ));
    }
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}
