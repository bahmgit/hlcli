use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    catalog,
    config::Config,
    exchange::ActionConnectionHealth,
    execution::ExecutionControl,
    metrics::{CountersSnapshot, Metrics},
    runtime::{CommandPendingResponse, CommandQueue, CommandResultsResponse},
    state::{
        AccountOverview, BalanceSummary, BorrowLend, FreshnessLimits, Order, Position, Readiness,
        SpotBalance, TradingState,
    },
};

#[derive(Clone)]
pub struct AppState {
    state: Arc<RwLock<TradingState>>,
    metrics: Metrics,
    token: Option<String>,
    queue: Option<Arc<CommandQueue>>,
    execution: ExecutionControl,
    action_connection: Option<ActionConnectionHealth>,
}

impl AppState {
    pub fn new(state: TradingState, metrics: Metrics, cfg: &Config) -> Self {
        Self::from_shared_state(Arc::new(RwLock::new(state)), metrics, cfg)
    }

    pub fn from_shared_state(
        state: Arc<RwLock<TradingState>>,
        metrics: Metrics,
        cfg: &Config,
    ) -> Self {
        Self {
            state,
            metrics,
            token: cfg.backend_token.clone(),
            queue: None,
            execution: ExecutionControl::default(),
            action_connection: None,
        }
    }

    pub fn with_queue(mut self, queue: Arc<CommandQueue>) -> Self {
        self.queue = Some(queue);
        self
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

    pub async fn update(&self, state: TradingState) {
        *self.state.write().await = state;
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/status", get(status))
        .route("/v1/portfolio", get(portfolio))
        .route("/v1/balances", get(balances))
        .route("/v1/metrics", get(metrics))
        .route("/v1/exposure", get(exposure))
        .route("/v1/snapshot", get(snapshot))
        .route("/v1/orders", get(orders))
        .route("/v1/position", get(position))
        .route("/v1/markets", get(markets))
        .route("/v1/pending", get(pending))
        .route("/v1/commands", get(commands))
        .route("/v1/keybinds", get(keybinds))
        .with_state(state)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    pub ok: bool,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub action_connected: Option<bool>,
    pub state_feed_connected: Option<bool>,
    pub readiness: ReadinessView,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadinessView {
    pub ready: bool,
    pub reasons: Vec<String>,
}

impl From<Readiness> for ReadinessView {
    fn from(value: Readiness) -> Self {
        Self {
            ready: value.ready,
            reasons: value.reasons,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusResponse {
    pub active: String,
    pub readiness: ReadinessView,
    pub market_count: usize,
    pub open_orders: usize,
    pub twap_ids: Vec<u64>,
    pub position: Option<Position>,
    pub account_mode: Option<String>,
    pub required_account_mode: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsResponse {
    pub execution_counters: CountersSnapshot,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilitiesResponse {
    pub commands: Vec<&'static str>,
    pub market_filters: Vec<&'static str>,
    pub routes: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrdersResponse {
    pub orders: Vec<Order>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionResponse {
    pub positions: Vec<Position>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioResponse {
    pub account: Option<AccountOverview>,
    pub positions: Vec<Position>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BalancesResponse {
    pub summary: Option<BalanceSummary>,
    pub spot_balances: Vec<SpotBalance>,
    pub borrow_lend: Vec<BorrowLend>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketsResponse {
    pub markets: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExposureResponse {
    pub active: String,
    pub net_delta: String,
    pub gross_notional_usd: Option<String>,
    pub open_orders: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotResponse {
    pub status: StatusResponse,
    pub orders: Vec<Order>,
    pub markets: Vec<String>,
    pub metrics: CountersSnapshot,
}

#[derive(Debug, Deserialize)]
struct MarketQuery {
    kind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CommandsQuery {
    after: Option<u64>,
    limit: Option<u32>,
}

async fn health(State(app): State<AppState>) -> Json<HealthResponse> {
    let state = app.state.read().await;
    let readiness = readiness(&state);
    drop(state);
    let execution = app.execution.health().await;
    let action_connected = app
        .action_connection
        .as_ref()
        .map(ActionConnectionHealth::connected);
    let state_feed_connected = app.metrics.state_feed_connected();
    Json(HealthResponse {
        ok: readiness.ready
            && !execution.halted
            && action_connected.unwrap_or(true)
            && state_feed_connected.unwrap_or(true),
        halted: execution.halted,
        halt_reason: execution.reason,
        action_connected,
        state_feed_connected,
        readiness: readiness.into(),
    })
}

async fn capabilities(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<CapabilitiesResponse>, ApiError> {
    authorize(&app, &headers)?;
    Ok(Json(CapabilitiesResponse {
        commands: catalog::command_names(),
        market_filters: vec!["all", "perps", "hip3", "spot"],
        routes: vec![
            "/v1/health",
            "/v1/capabilities",
            "/v1/status",
            "/v1/portfolio",
            "/v1/balances",
            "/v1/metrics",
            "/v1/exposure",
            "/v1/snapshot",
            "/v1/orders",
            "/v1/position",
            "/v1/markets",
            "/v1/pending",
            "/v1/commands",
            "/v1/keybinds",
        ],
    }))
}

async fn status(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, ApiError> {
    authorize(&app, &headers)?;
    let state = app.state.read().await;
    let readiness = readiness(&state);
    let open_orders = state.open_orders().count();
    let twap_ids = active_twap_ids(&state);
    let position = state
        .active_position()
        .map(|position| position.value.clone());
    let account_mode = state
        .account_mode
        .map(|mode| mode.value.as_str().to_string());
    let required_account_mode = state
        .required_account_mode
        .map(|mode| mode.as_str().to_string());
    let active = state.active.clone();
    let market_count = state.markets.len();
    drop(state);
    Ok(Json(StatusResponse {
        active,
        readiness: readiness.into(),
        market_count,
        open_orders,
        twap_ids,
        position,
        account_mode,
        required_account_mode,
    }))
}

async fn commands(
    State(app): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CommandsQuery>,
) -> Result<Json<CommandResultsResponse>, ApiError> {
    authorize(&app, &headers)?;
    let queue = app.queue.as_ref().ok_or_else(queue_unavailable)?;
    Ok(Json(
        queue
            .results(query.after.unwrap_or(0), query.limit.unwrap_or(50))
            .await,
    ))
}

async fn portfolio(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PortfolioResponse>, ApiError> {
    authorize(&app, &headers)?;
    let state = app.state.read().await;
    Ok(Json(PortfolioResponse {
        account: state.account_overview(),
        positions: state
            .open_positions()
            .map(|position| position.value.clone())
            .collect(),
    }))
}

async fn balances(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<BalancesResponse>, ApiError> {
    authorize(&app, &headers)?;
    let state = app.state.read().await;
    Ok(Json(BalancesResponse {
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
    }))
}

async fn pending(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<CommandPendingResponse>, ApiError> {
    authorize(&app, &headers)?;
    let queue = app.queue.as_ref().ok_or_else(queue_unavailable)?;
    Ok(Json(queue.pending().await))
}

async fn keybinds(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<KeybindsResponse>, ApiError> {
    authorize(&app, &headers)?;
    let queue = app.queue.as_ref().ok_or_else(queue_unavailable)?;
    Ok(Json(KeybindsResponse {
        keybinds: queue.keybinds().await,
    }))
}

async fn metrics(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<MetricsResponse>, ApiError> {
    authorize(&app, &headers)?;
    Ok(Json(MetricsResponse {
        execution_counters: app.metrics.snapshot(),
    }))
}

async fn orders(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<OrdersResponse>, ApiError> {
    authorize(&app, &headers)?;
    let state = app.state.read().await;
    Ok(Json(OrdersResponse {
        orders: state
            .open_orders()
            .map(|order| order.value.clone())
            .collect(),
    }))
}

async fn exposure(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ExposureResponse>, ApiError> {
    authorize(&app, &headers)?;
    let state = app.state.read().await;
    let position = state
        .active_position()
        .map(|position| position.value.clone());
    let net_delta = position
        .as_ref()
        .map(|position| position.size)
        .unwrap_or_default();
    let gross_notional_usd = position
        .as_ref()
        .and_then(|position| {
            state
                .book
                .get(&position.symbol)
                .map(|book| (position, book))
        })
        .map(|(position, book)| {
            let mid = (book.value.bid + book.value.ask) / rust_decimal::Decimal::TWO;
            (position.size.abs() * mid).normalize().to_string()
        });
    Ok(Json(ExposureResponse {
        active: state.active.clone(),
        net_delta: net_delta.normalize().to_string(),
        gross_notional_usd,
        open_orders: state.active_orders().count(),
    }))
}

async fn snapshot(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<SnapshotResponse>, ApiError> {
    authorize(&app, &headers)?;
    let state = app.state.read().await;
    let readiness = readiness(&state);
    let orders = state
        .open_orders()
        .map(|order| order.value.clone())
        .collect::<Vec<_>>();
    let markets = state
        .markets
        .values()
        .map(|market| market.symbol.clone())
        .collect::<Vec<_>>();
    let active = state.active.clone();
    let market_count = state.markets.len();
    let twap_ids = active_twap_ids(&state);
    let position = state
        .active_position()
        .map(|position| position.value.clone());
    let account_mode = state
        .account_mode
        .map(|mode| mode.value.as_str().to_string());
    let required_account_mode = state
        .required_account_mode
        .map(|mode| mode.as_str().to_string());
    drop(state);
    let status = StatusResponse {
        active,
        readiness: readiness.into(),
        market_count,
        open_orders: orders.len(),
        twap_ids,
        position,
        account_mode,
        required_account_mode,
    };
    Ok(Json(SnapshotResponse {
        status,
        orders,
        markets,
        metrics: app.metrics.snapshot(),
    }))
}

async fn position(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PositionResponse>, ApiError> {
    authorize(&app, &headers)?;
    let state = app.state.read().await;
    Ok(Json(PositionResponse {
        positions: state
            .open_positions()
            .map(|position| position.value.clone())
            .collect(),
    }))
}

async fn markets(
    State(app): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MarketQuery>,
) -> Result<Json<MarketsResponse>, ApiError> {
    authorize(&app, &headers)?;
    let state = app.state.read().await;
    let kind = query.kind.as_deref().unwrap_or("all").to_ascii_lowercase();
    if !matches!(
        kind.as_str(),
        "all" | "perps" | "perp" | "hip3" | "hip-3" | "spot" | "spots"
    ) {
        return Err(ApiError::bad_request(format!("unknown market kind {kind}")));
    }
    let markets = state
        .markets
        .values()
        .filter(|market| match kind.as_str() {
            "all" => true,
            "perps" | "perp" => matches!(
                market.kind,
                crate::protocol::MarketKind::Perp | crate::protocol::MarketKind::BuilderPerp
            ),
            "hip3" | "hip-3" => market.kind == crate::protocol::MarketKind::BuilderPerp,
            "spot" | "spots" => market.kind == crate::protocol::MarketKind::Spot,
            _ => false,
        })
        .map(|market| market.symbol.clone())
        .collect();
    Ok(Json(MarketsResponse { markets }))
}

fn active_twap_ids(state: &TradingState) -> Vec<u64> {
    state.active_twaps().map(|(id, _)| *id).collect()
}

fn readiness(state: &TradingState) -> Readiness {
    state.readiness(
        now_ms(),
        FreshnessLimits::default(),
        true,
        state
            .active_position()
            .is_none_or(|position| position.value.flat()),
    )
}

fn authorize(app: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(token) = &app.token else {
        return Ok(());
    };
    let header = headers
        .get("x-hl-v2-token")
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
        });
    if header == Some(token.as_str()) {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "unauthorized".to_string(),
        ))
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeybindsResponse {
    pub keybinds: std::collections::BTreeMap<String, String>,
}

fn queue_unavailable() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "command queue unavailable".to_string(),
    )
}

#[derive(Debug)]
pub struct ApiError(StatusCode, String);

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, message.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
