use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    catalog,
    command::{self, Command, MarketFilter},
    config::Config,
    core::CommandExecutor,
    exchange::ActionConnectionHealth,
    execution::{ExecutionControl, ExecutionStatus, OrderStatus, SubmitReceipt},
    metrics::Metrics,
    operator::{ChainStep, KeybindStore, Variables, parse_chain},
    protocol::{MarketKind, TimeInForce},
    scope::ScopeRegistry,
    state::{
        AccountMode, Book, BorrowLend, Fresh, FreshnessLimits, Market, Order, OrderKind, Position,
        SpotBalance, TradingState, Twap,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandResponse {
    pub ok: bool,
    #[serde(default)]
    pub clear: bool,
    #[serde(default)]
    pub exit: bool,
    pub lines: Vec<String>,
}

impl CommandResponse {
    fn ok(lines: impl Into<Vec<String>>) -> Self {
        Self {
            ok: true,
            clear: false,
            exit: false,
            lines: lines.into(),
        }
    }

    fn err(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            clear: false,
            exit: false,
            lines: vec![message.into()],
        }
    }

    fn clear() -> Self {
        Self {
            ok: true,
            clear: true,
            exit: false,
            lines: Vec::new(),
        }
    }

    fn exit() -> Self {
        Self {
            ok: true,
            clear: false,
            exit: true,
            lines: Vec::new(),
        }
    }

    fn append(&mut self, other: Self) {
        self.ok &= other.ok;
        if other.clear {
            self.clear = true;
            self.lines.clear();
        }
        self.exit |= other.exit;
        self.lines.extend(other.lines);
    }
}

pub type RefreshFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

pub trait StateRefresher: Send + Sync {
    fn refresh_market<'a>(&'a self, symbol: &'a str) -> RefreshFuture<'a>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeConfig {
    network: String,
    default_symbol: String,
    allowed_symbols: Vec<String>,
    symbol_aliases: BTreeMap<String, String>,
    bind: String,
    allow_remote: bool,
    vault: bool,
    required_account_mode: Option<String>,
    market_cross_bps: u32,
}

impl RuntimeConfig {
    fn from_config(cfg: &Config) -> Self {
        Self {
            network: format!("{:?}", cfg.network),
            default_symbol: cfg.default_symbol.clone(),
            allowed_symbols: cfg.allowed_symbols.iter().cloned().collect(),
            symbol_aliases: cfg.symbol_aliases.clone(),
            bind: cfg.bind.to_string(),
            allow_remote: cfg.allow_remote,
            vault: cfg.vault_address.is_some(),
            required_account_mode: cfg.required_account_mode.clone(),
            market_cross_bps: cfg.market_cross_bps,
        }
    }

    fn lines(&self) -> Vec<String> {
        vec![
            format!("network={}", self.network),
            format!("default_symbol={}", self.default_symbol),
            format!(
                "allowed_symbols={}",
                if self.allowed_symbols.is_empty() {
                    "live".to_string()
                } else {
                    self.allowed_symbols.join(",")
                }
            ),
            format!("symbol_aliases={}", self.aliases_line()),
            format!("bind={}", self.bind),
            format!("allow_remote={}", self.allow_remote),
            format!("vault={}", self.vault),
            format!(
                "required_account_mode={}",
                self.required_account_mode.as_deref().unwrap_or("any")
            ),
            format!("market_cross_bps={}", self.market_cross_bps),
        ]
    }

    fn get(&self, key: &str) -> Option<String> {
        match key {
            "network" => Some(self.network.clone()),
            "default-symbol" | "default_symbol" => Some(self.default_symbol.clone()),
            "allowed-symbols" | "allowed_symbols" => Some(if self.allowed_symbols.is_empty() {
                "live".to_string()
            } else {
                self.allowed_symbols.join(",")
            }),
            "symbol-aliases" | "symbol_aliases" | "aliases" => Some(self.aliases_line()),
            "bind" => Some(self.bind.clone()),
            "allow-remote" | "allow_remote" => Some(self.allow_remote.to_string()),
            "vault" | "vault-address" | "vault_address" => Some(self.vault.to_string()),
            "required-account-mode" | "required_account_mode" => Some(
                self.required_account_mode
                    .clone()
                    .unwrap_or_else(|| "any".to_string()),
            ),
            "market-cross-bps" | "market_cross_bps" => Some(self.market_cross_bps.to_string()),
            _ => None,
        }
    }

    fn risk_lines(&self) -> Vec<String> {
        vec![
            format!("market_cross_bps={}", self.market_cross_bps),
            format!(
                "required_account_mode={}",
                self.required_account_mode.as_deref().unwrap_or("any")
            ),
            format!(
                "allowed_symbols={}",
                if self.allowed_symbols.is_empty() {
                    "live".to_string()
                } else {
                    self.allowed_symbols.join(",")
                }
            ),
            format!("symbol_aliases={}", self.aliases_line()),
        ]
    }

    fn aliases_line(&self) -> String {
        if self.symbol_aliases.is_empty() {
            "none".to_string()
        } else {
            self.symbol_aliases
                .iter()
                .map(|(alias, target)| format!("{alias}={target}"))
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn resolve_symbol(&self, symbol: &str) -> Option<String> {
        self.symbol_aliases.get(symbol).cloned()
    }

    fn aliases_for(&self, symbol: &str) -> Vec<&str> {
        self.symbol_aliases
            .iter()
            .filter_map(|(alias, target)| (target == symbol).then_some(alias.as_str()))
            .collect()
    }
}

pub struct CommandService {
    state: Arc<RwLock<TradingState>>,
    executor: Arc<dyn CommandExecutor>,
    variables: Mutex<Variables>,
    keybinds: Mutex<KeybindStore>,
    config: Option<RuntimeConfig>,
    refresher: Option<Arc<dyn StateRefresher>>,
    scopes: Option<Arc<ScopeRegistry>>,
    diagnostics: Option<Diagnostics>,
    confirmations: Mutex<BTreeMap<Option<u64>, PendingConfirmation>>,
}

#[derive(Clone)]
struct Diagnostics {
    metrics: Metrics,
    execution: ExecutionControl,
    action_connection: Option<ActionConnectionHealth>,
}

#[derive(Debug, Clone)]
struct PendingConfirmation {
    command: Command,
    summary: String,
    expires_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PendingConfirmationView {
    pub scope: String,
    pub summary: String,
    pub expires_at_ms: u64,
}

const CONFIRMATION_TTL_MS: u64 = 30_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MarketSessionResponse {
    pub session_id: u64,
    pub active: String,
    pub sync_reasons: Vec<String>,
}

impl CommandService {
    pub fn new(
        state: Arc<RwLock<TradingState>>,
        executor: Arc<dyn CommandExecutor>,
        keybinds: KeybindStore,
    ) -> Self {
        Self {
            state,
            executor,
            variables: Mutex::new(Variables::default()),
            keybinds: Mutex::new(keybinds),
            config: None,
            refresher: None,
            scopes: None,
            diagnostics: None,
            confirmations: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn with_config(mut self, cfg: &Config) -> Self {
        self.config = Some(RuntimeConfig::from_config(cfg));
        self
    }

    pub fn with_refresher(mut self, refresher: Arc<dyn StateRefresher>) -> Self {
        self.refresher = Some(refresher);
        self
    }

    pub fn with_scopes(mut self, scopes: Arc<ScopeRegistry>) -> Self {
        self.scopes = Some(scopes);
        self
    }

    pub fn with_diagnostics(
        mut self,
        metrics: Metrics,
        execution: ExecutionControl,
        action_connection: Option<ActionConnectionHealth>,
    ) -> Self {
        self.diagnostics = Some(Diagnostics {
            metrics,
            execution,
            action_connection,
        });
        self
    }

    pub async fn execute_line(&self, input: &str) -> CommandResponse {
        self.execute_line_in(None, input).await
    }

    pub async fn execute_line_scoped(&self, session_id: u64, input: &str) -> CommandResponse {
        self.execute_line_in(Some(session_id), input).await
    }

    async fn execute_line_in(&self, session_id: Option<u64>, input: &str) -> CommandResponse {
        let steps = match parse_chain(input) {
            Ok(steps) => steps,
            Err(err) => return CommandResponse::err(err.to_string()),
        };
        let mut out = CommandResponse::ok(Vec::new());
        for step in steps {
            let response = match step {
                ChainStep::Sleep(delay) => {
                    tokio::time::sleep(delay).await;
                    CommandResponse::ok(Vec::new())
                }
                ChainStep::Command(command) => self.execute_one(session_id, &command).await,
            };
            let stop = !response.ok || response.exit;
            out.append(response);
            if stop || self.pending_confirmation(session_id).await.is_some() {
                break;
            }
        }
        out
    }

    pub async fn keybinds(&self) -> BTreeMap<String, String> {
        self.keybinds.lock().await.binds().clone()
    }

    pub async fn open_market_session(
        &self,
        requested: String,
    ) -> Result<MarketSessionResponse, String> {
        let requested = requested.trim().to_ascii_uppercase();
        let symbol = self
            .config
            .as_ref()
            .and_then(|config| config.resolve_symbol(&requested))
            .unwrap_or(requested);
        {
            let state = self.state.read().await;
            if !state.markets.contains_key(&symbol) {
                return Err(format!("unknown market {symbol}"));
            }
        }
        let scopes = self
            .scopes
            .as_ref()
            .ok_or_else(|| "market sessions unavailable".to_string())?;
        let session_id = scopes.open(symbol.clone()).await;
        if let Some(refresher) = &self.refresher
            && let Err(err) = refresher.refresh_market(&symbol).await
        {
            scopes.close(session_id).await?;
            return Err(format!("initial refresh failed for {symbol}: {err}"));
        }
        let state = self.state.read().await;
        Ok(MarketSessionResponse {
            session_id,
            active: symbol.clone(),
            sync_reasons: state.sync_reasons(&symbol, now_ms(), FreshnessLimits::default()),
        })
    }

    pub async fn market_session(&self, session_id: u64) -> Result<MarketSessionResponse, String> {
        let scopes = self
            .scopes
            .as_ref()
            .ok_or_else(|| "market sessions unavailable".to_string())?;
        let active = scopes.live_market(session_id).await?;
        let state = self.state.read().await;
        Ok(MarketSessionResponse {
            session_id,
            active: active.clone(),
            sync_reasons: state.sync_reasons(&active, now_ms(), FreshnessLimits::default()),
        })
    }

    pub async fn close_market_session(&self, session_id: u64) -> Result<(), String> {
        self.confirmations.lock().await.remove(&Some(session_id));
        self.scopes
            .as_ref()
            .ok_or_else(|| "market sessions unavailable".to_string())?
            .close(session_id)
            .await
    }

    async fn execute_one(&self, session_id: Option<u64>, input: &str) -> CommandResponse {
        let scopes = match session_id {
            Some(_) => match self.scopes.as_deref() {
                Some(scopes) => Some(scopes),
                None => return CommandResponse::err("market sessions unavailable"),
            },
            None => None,
        };
        if let Some(response) = self.confirmation_input(session_id, input).await {
            return response;
        }
        if let Some(pending) = self.pending_confirmation(session_id).await {
            return CommandResponse::err(format!(
                "confirmation pending: {}; reply yes or no before another command",
                pending.summary
            ));
        }
        let command = match self.parse_runtime_command(session_id, input).await {
            Ok(command) => command,
            Err(message) => return CommandResponse::err(message),
        };
        match command {
            Command::Empty => CommandResponse::ok(Vec::new()),
            Command::Reject { message } => CommandResponse::err(message),
            Command::Unknown { input } => CommandResponse::err(format!("unknown command: {input}")),
            Command::Status => self.status(session_id).await,
            Command::Doctor => self.doctor(session_id).await,
            Command::RiskShow => self.risk_show().await,
            Command::ConfigShow => self.config_show().await,
            Command::ConfigGet { key } => self.config_get(key).await,
            Command::Refresh => self.refresh(session_id).await,
            Command::Portfolio => self.portfolio().await,
            Command::Balances => self.balances().await,
            Command::Orders => self.orders(session_id).await,
            Command::Position => self.position(session_id).await,
            Command::AccountModeShow => self.account_mode().await,
            Command::AccountModeRequire { mode } => self.require_account_mode(mode).await,
            Command::MarketList { filter } => self.markets(filter).await,
            Command::InstrumentShow => self.instrument(session_id).await,
            Command::InstrumentUse { symbol } => self.switch_instrument(session_id, symbol).await,
            Command::SetVar { name, value } => {
                let result = match session_id {
                    Some(id) => {
                        scopes
                            .expect("scoped execution established registry")
                            .set_variable(id, name.clone(), value)
                            .await
                    }
                    None => self.variables.lock().await.set(name.clone(), value),
                };
                match result {
                    Ok(()) => CommandResponse::ok(vec![format!("set {name}")]),
                    Err(err) => CommandResponse::err(err),
                }
            }
            Command::PrintVar { name } => self.print_var(session_id, name).await,
            Command::UnsetVar { name } => {
                let removed = match session_id {
                    Some(id) => match scopes
                        .expect("scoped execution established registry")
                        .unset_variable(id, &name)
                        .await
                    {
                        Ok(removed) => removed,
                        Err(err) => return CommandResponse::err(err),
                    },
                    None => self.variables.lock().await.unset(&name),
                };
                CommandResponse::ok(vec![if removed {
                    format!("unset {name}")
                } else {
                    format!("{name} not set")
                }])
            }
            Command::Keybinds => self.render_keybinds().await,
            Command::Bind { key, action } => {
                let result = self.keybinds.lock().await.bind(key.clone(), action.clone());
                match result {
                    Ok(()) => CommandResponse::ok(vec![format!("bind {key} -> {action}")]),
                    Err(err) => CommandResponse::err(err.to_string()),
                }
            }
            Command::Unbind { key } => {
                let result = self.keybinds.lock().await.unbind(&key);
                match result {
                    Ok(true) => CommandResponse::ok(vec![format!("unbind {key}")]),
                    Ok(false) => CommandResponse::ok(vec![format!("{key} not bound")]),
                    Err(err) => CommandResponse::err(err.to_string()),
                }
            }
            Command::Help { topic } => CommandResponse::ok(catalog::help_lines(topic.as_deref())),
            Command::Clear => CommandResponse::clear(),
            Command::Quit => CommandResponse::exit(),
            command @ Command::AccountModeSet { .. } => {
                self.stage_confirmation(session_id, command).await
            }
            command @ (Command::Trade(_)
            | Command::Scale(_)
            | Command::BatchPlace(_)
            | Command::ProtectionSet(_)
            | Command::ProtectionCancel { .. }
            | Command::ChasePlace(_)
            | Command::ChaseCancel
            | Command::CancelAll
            | Command::CancelOid { .. }
            | Command::CancelCloid { .. }
            | Command::MoveOid { .. }
            | Command::ResizeOid { .. }
            | Command::BatchMoveOid { .. }
            | Command::BatchResizeOid { .. }
            | Command::BatchMoveCloid { .. }
            | Command::BatchResizeCloid { .. }
            | Command::Close { .. }
            | Command::Leverage { .. }
            | Command::IsolatedMargin { .. }
            | Command::TwapPlace(_)
            | Command::TwapCancel { .. }) => self.execute_action(session_id, command).await,
        }
    }

    async fn execute_action(&self, session_id: Option<u64>, command: Command) -> CommandResponse {
        match session_id {
            Some(id) => {
                let Some(scopes) = self.scopes.as_deref() else {
                    return CommandResponse::err("market sessions unavailable");
                };
                let symbol = match scopes.market(id).await {
                    Ok(symbol) => symbol,
                    Err(err) => return CommandResponse::err(err),
                };
                match self.executor.execute_command_for(&symbol, command).await {
                    Ok(receipt) => response_from_receipt(receipt),
                    Err(err) => CommandResponse::err(err),
                }
            }
            None => match self.executor.execute_command(command).await {
                Ok(receipt) => response_from_receipt(receipt),
                Err(err) => CommandResponse::err(err),
            },
        }
    }

    async fn stage_confirmation(
        &self,
        session_id: Option<u64>,
        command: Command,
    ) -> CommandResponse {
        let summary = match &command {
            Command::AccountModeSet { mode } => {
                let mode = match AccountMode::parse(mode) {
                    Ok(mode) => mode,
                    Err(err) => return CommandResponse::err(err),
                };
                let current = self
                    .state
                    .read()
                    .await
                    .account_mode
                    .as_ref()
                    .map(|current| {
                        let age = current.age_ms(now_ms());
                        if age > FreshnessLimits::default().account_ms {
                            Err(format!("account mode stale age_ms={age}"))
                        } else {
                            Ok(current.value)
                        }
                    })
                    .unwrap_or_else(|| Err("account mode unavailable".to_string()));
                let current = match current {
                    Ok(current) => current,
                    Err(err) => return CommandResponse::err(err),
                };
                if let Err(err) = mode.agent_transition_code_from(current) {
                    return CommandResponse::err(err);
                }
                format!("set account mode to {}", mode.as_str())
            }
            _ => return CommandResponse::err("command does not require confirmation"),
        };
        let expires_at_ms = now_ms().saturating_add(CONFIRMATION_TTL_MS);
        let mut confirmations = self.confirmations.lock().await;
        let now = now_ms();
        confirmations.retain(|_, pending| pending.expires_at_ms >= now);
        if let Some((owner, pending)) = confirmations.first_key_value() {
            let owner = owner
                .map(|id| format!("marketSession:{id}"))
                .unwrap_or_else(|| "global".to_string());
            return CommandResponse::err(format!(
                "account-wide confirmation already pending in {owner}: {}; its owner must reply yes or no",
                pending.summary
            ));
        }
        confirmations.insert(
            session_id,
            PendingConfirmation {
                command,
                summary: summary.clone(),
                expires_at_ms,
            },
        );
        drop(confirmations);
        CommandResponse::ok(vec![format!(
            "confirm {summary}? reply yes or no within {} seconds",
            CONFIRMATION_TTL_MS / 1_000
        )])
    }

    async fn confirmation_input(
        &self,
        session_id: Option<u64>,
        input: &str,
    ) -> Option<CommandResponse> {
        let answer = input.trim().to_ascii_lowercase();
        if !matches!(answer.as_str(), "y" | "yes" | "n" | "no") {
            return None;
        }
        let pending = self.confirmations.lock().await.remove(&session_id)?;
        if pending.expires_at_ms < now_ms() {
            return Some(CommandResponse::err(
                "confirmation expired; submit the command again",
            ));
        }
        if matches!(answer.as_str(), "n" | "no") {
            return Some(CommandResponse::ok(vec![format!(
                "cancelled {}",
                pending.summary
            )]));
        }
        Some(self.execute_action(session_id, pending.command).await)
    }

    async fn pending_confirmation(&self, session_id: Option<u64>) -> Option<PendingConfirmation> {
        let mut confirmations = self.confirmations.lock().await;
        let now = now_ms();
        confirmations.retain(|_, pending| pending.expires_at_ms >= now);
        confirmations.get(&session_id).cloned()
    }

    async fn pending_confirmations(&self) -> Vec<PendingConfirmationView> {
        let mut confirmations = self.confirmations.lock().await;
        let now = now_ms();
        confirmations.retain(|_, pending| pending.expires_at_ms >= now);
        confirmations
            .iter()
            .map(|(session_id, pending)| PendingConfirmationView {
                scope: session_id
                    .map(|id| format!("marketSession:{id}"))
                    .unwrap_or_else(|| "global".to_string()),
                summary: pending.summary.clone(),
                expires_at_ms: pending.expires_at_ms,
            })
            .collect()
    }

    async fn parse_runtime_command(
        &self,
        session_id: Option<u64>,
        input: &str,
    ) -> Result<Command, String> {
        let parsed = command::parse(input);
        match parsed {
            Command::SetVar { .. }
            | Command::Bind { .. }
            | Command::Unbind { .. }
            | Command::PrintVar { .. }
            | Command::UnsetVar { .. }
            | Command::Reject { .. }
            | Command::Unknown { .. }
            | Command::Empty => Ok(parsed),
            _ => {
                let expanded = self.expand_line(session_id, input).await?;
                Ok(command::parse(&expanded))
            }
        }
    }

    async fn expand_line(&self, session_id: Option<u64>, input: &str) -> Result<String, String> {
        let tokens = command::tokenize(input)?;
        let expanded = match session_id {
            Some(id) => {
                let scopes = self
                    .scopes
                    .as_ref()
                    .ok_or_else(|| "market sessions unavailable".to_string())?;
                let mut expanded = Vec::with_capacity(tokens.len());
                for token in tokens {
                    expanded.push(scopes.expand(id, &token).await?);
                }
                expanded
            }
            None => {
                let vars = self.variables.lock().await;
                tokens
                    .into_iter()
                    .map(|token| vars.expand_token(&token))
                    .collect::<Result<Vec<_>, _>>()?
            }
        };
        Ok(expanded.join(" "))
    }

    async fn selected_symbol(&self, session_id: Option<u64>) -> Result<String, String> {
        match session_id {
            Some(id) => {
                self.scopes
                    .as_ref()
                    .ok_or_else(|| "market sessions unavailable".to_string())?
                    .market(id)
                    .await
            }
            None => Ok(self.state.read().await.active.clone()),
        }
    }

    async fn status(&self, session_id: Option<u64>) -> CommandResponse {
        let symbol = match self.selected_symbol(session_id).await {
            Ok(symbol) => symbol,
            Err(err) => return CommandResponse::err(err),
        };
        let state = self.state.read().await;
        let price = mark_for(&state, &symbol)
            .map(|price| price.to_string())
            .unwrap_or_else(|| "-".to_string());
        let twaps = state
            .twaps_for(&symbol)
            .map(|(id, twap)| render_twap(*id, &twap.value))
            .collect::<Vec<_>>()
            .join(",");
        let position = if state
            .markets
            .get(&symbol)
            .is_some_and(|market| market.kind == MarketKind::Spot)
        {
            "n/a".to_string()
        } else {
            state
                .position(&symbol)
                .map(|position| position.value.size.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        };
        let mut lines = vec![format!(
            "active={} price={} markets={} open_orders_all={} twaps=[{}] position={}",
            symbol,
            price,
            state.markets.len(),
            state.open_orders().count(),
            twaps,
            position
        )];
        let sync = state.sync_reasons(&symbol, now_ms(), FreshnessLimits::default());
        if !sync.is_empty() {
            lines.push(format!("syncing={}", sync.join(",")));
        }
        lines.push(render_account(&state));
        CommandResponse::ok(lines)
    }

    async fn doctor(&self, session_id: Option<u64>) -> CommandResponse {
        let symbol = match self.selected_symbol(session_id).await {
            Ok(symbol) => symbol,
            Err(err) => return CommandResponse::err(err),
        };
        let readiness = {
            let state = self.state.read().await;
            state.readiness_for(
                &symbol,
                now_ms(),
                FreshnessLimits::default(),
                true,
                state
                    .position(&symbol)
                    .is_none_or(|position| position.value.flat()),
            )
        };
        let Some(diagnostics) = &self.diagnostics else {
            return CommandResponse::err("diagnostics unavailable");
        };
        let execution = diagnostics.execution.health().await;
        let action = diagnostics
            .action_connection
            .as_ref()
            .map(ActionConnectionHealth::connected);
        let state_feed = diagnostics.metrics.state_feed_connected();
        let mut reasons = readiness.reasons;
        if let Some(reason) = execution.reason.as_deref() {
            reasons.push(format!("execution halted: {reason}"));
        }
        if action == Some(false) {
            reasons.push("action feed disconnected".to_string());
        }
        if state_feed == Some(false) {
            reasons.push("state feed disconnected".to_string());
        }
        let ok = readiness.ready
            && !execution.halted
            && action.unwrap_or(true)
            && state_feed.unwrap_or(true);
        let mut lines = vec![format!(
            "doctor active={symbol} ready={ok} action_feed={} state_feed={} execution_halted={}",
            optional_health(action),
            optional_health(state_feed),
            execution.halted
        )];
        if reasons.is_empty() {
            lines.push("no issues".to_string());
        } else {
            lines.extend(reasons.into_iter().map(|reason| format!("issue: {reason}")));
        }
        CommandResponse {
            ok,
            clear: false,
            exit: false,
            lines,
        }
    }

    async fn refresh(&self, session_id: Option<u64>) -> CommandResponse {
        let Some(refresher) = &self.refresher else {
            return CommandResponse::err("refresh unavailable");
        };
        let symbol = match self.selected_symbol(session_id).await {
            Ok(symbol) => symbol,
            Err(err) => return CommandResponse::err(err),
        };
        match refresher.refresh_market(&symbol).await {
            Ok(()) => CommandResponse::ok(vec!["refresh complete".to_string()]),
            Err(err) => CommandResponse::err(format!("refresh failed: {err}")),
        }
    }

    async fn config_show(&self) -> CommandResponse {
        match &self.config {
            Some(config) => CommandResponse::ok(config.lines()),
            None => CommandResponse::err("config unavailable"),
        }
    }

    async fn config_get(&self, key: String) -> CommandResponse {
        match &self.config {
            Some(config) => match config.get(&key) {
                Some(value) => CommandResponse::ok(vec![format!("{key}={value}")]),
                None => CommandResponse::err(format!("unknown config key {key}")),
            },
            None => CommandResponse::err("config unavailable"),
        }
    }

    async fn risk_show(&self) -> CommandResponse {
        match &self.config {
            Some(config) => CommandResponse::ok(config.risk_lines()),
            None => CommandResponse::err("risk config unavailable"),
        }
    }

    async fn portfolio(&self) -> CommandResponse {
        let state = self.state.read().await;
        let mut lines = vec![render_account(&state)];
        let positions = state
            .open_positions()
            .map(|position| render_position(position, mark_for(&state, &position.value.symbol)))
            .collect::<Vec<_>>();
        if positions.is_empty() {
            lines.push("no positions".to_string());
        } else {
            lines.extend(positions);
        }
        CommandResponse::ok(lines)
    }

    async fn orders(&self, session_id: Option<u64>) -> CommandResponse {
        let symbol = match self.selected_symbol(session_id).await {
            Ok(symbol) => symbol,
            Err(err) => return CommandResponse::err(err),
        };
        let state = self.state.read().await;
        let mut lines = orders_sync_lines(&state, &symbol);
        let orders: Box<dyn Iterator<Item = &Fresh<Order>> + '_> = if session_id.is_some() {
            Box::new(state.orders_for(&symbol))
        } else {
            Box::new(state.open_orders())
        };
        lines.extend(orders.map(|order| {
            let order = &order.value;
            render_order(
                order,
                state
                    .positions
                    .get(&order.symbol)
                    .map(|position| &position.value),
                mark_for(&state, &order.symbol),
            )
        }));
        CommandResponse::ok(if lines.is_empty() {
            vec!["no orders".to_string()]
        } else {
            lines
        })
    }

    async fn position(&self, session_id: Option<u64>) -> CommandResponse {
        let symbol = match self.selected_symbol(session_id).await {
            Ok(symbol) => symbol,
            Err(err) => return CommandResponse::err(err),
        };
        let state = self.state.read().await;
        let mut lines = position_sync_lines(&state, &symbol);
        let positions: Box<dyn Iterator<Item = &Fresh<Position>> + '_> = if session_id.is_some() {
            Box::new(
                state
                    .position(&symbol)
                    .into_iter()
                    .filter(|position| !position.value.flat()),
            )
        } else {
            Box::new(state.open_positions())
        };
        lines.extend(
            positions.map(|position| {
                render_position(position, mark_for(&state, &position.value.symbol))
            }),
        );
        CommandResponse::ok(if lines.is_empty() {
            vec!["no positions".to_string()]
        } else {
            lines
        })
    }

    async fn balances(&self) -> CommandResponse {
        let state = self.state.read().await;
        let mut lines = Vec::new();
        if let Some(summary) = &state.balance_summary {
            let summary = &summary.value;
            lines.push(format!(
                "balances portfolio_margin={} ratio={} value_usd={} available_usd={} unpriced={} borrow_lend_health={} health_factor={}",
                summary.portfolio_margin_enabled,
                summary
                    .portfolio_margin_ratio
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                summary
                    .spot_value_usd
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                summary
                    .spot_available_usd
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                summary.spot_unpriced_count,
                summary.borrow_lend_health.as_deref().unwrap_or("-"),
                summary
                    .borrow_lend_health_factor
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            ));
        }
        let mut rendered_tokens = BTreeSet::new();
        for balance in state
            .spot_balances
            .values()
            .filter(|balance| balance.value.nonzero())
        {
            if let Some(token) = balance.value.token {
                rendered_tokens.insert(token);
            }
            lines.push(render_balance(
                &balance.value,
                balance
                    .value
                    .token
                    .and_then(|token| state.borrow_lend.get(&token))
                    .map(|entry| &entry.value),
            ));
        }
        lines.extend(
            state
                .borrow_lend
                .values()
                .filter(|entry| {
                    entry.value.nonzero() && !rendered_tokens.contains(&entry.value.token)
                })
                .map(|entry| render_borrow_lend(&entry.value)),
        );
        CommandResponse::ok(if lines.is_empty() {
            vec!["no balances".to_string()]
        } else {
            lines
        })
    }

    async fn markets(&self, filter: MarketFilter) -> CommandResponse {
        let state = self.state.read().await;
        let lines = state
            .markets
            .values()
            .filter(|market| match filter {
                MarketFilter::All => true,
                MarketFilter::Perps => matches!(
                    market.kind,
                    crate::protocol::MarketKind::Perp | crate::protocol::MarketKind::BuilderPerp
                ),
                MarketFilter::Hip3 => market.kind == crate::protocol::MarketKind::BuilderPerp,
                MarketFilter::Spot => market.kind == crate::protocol::MarketKind::Spot,
            })
            .map(|market| render_market(market, self.market_aliases(&market.symbol)))
            .collect::<Vec<_>>();
        CommandResponse::ok(lines)
    }

    async fn instrument(&self, session_id: Option<u64>) -> CommandResponse {
        match self.selected_symbol(session_id).await {
            Ok(symbol) => CommandResponse::ok(vec![format!("active instrument {symbol}")]),
            Err(err) => CommandResponse::err(err),
        }
    }

    async fn switch_instrument(&self, session_id: Option<u64>, symbol: String) -> CommandResponse {
        let (input, symbol) = match self
            .config
            .as_ref()
            .and_then(|cfg| cfg.resolve_symbol(&symbol))
        {
            Some(target) => (Some(symbol), target),
            None => (None, symbol),
        };
        let active = match self.selected_symbol(session_id).await {
            Ok(active) => active,
            Err(err) => return CommandResponse::err(err),
        };
        {
            let state = self.state.read().await;
            if active == symbol {
                return CommandResponse::ok(vec![render_active_instrument(
                    &symbol,
                    input.as_deref(),
                )]);
            }
            if !state.markets.contains_key(&symbol) {
                return CommandResponse::err(match input {
                    Some(alias) => format!("alias {alias} targets unknown market {symbol}"),
                    None => format!("unknown market {symbol}"),
                });
            }
        }
        if let Some(reason) = self.executor.switch_blocker(&active).await {
            return CommandResponse::err(format!(
                "cannot switch instrument while {reason} on {active}; cancel managed work first"
            ));
        }
        match session_id {
            Some(id) => {
                let Some(scopes) = &self.scopes else {
                    return CommandResponse::err("market sessions unavailable");
                };
                if let Err(err) = scopes.switch(id, symbol.clone()).await {
                    return CommandResponse::err(err);
                }
            }
            None => {
                let preserve_target = match &self.scopes {
                    Some(scopes) => scopes.keeps_market_alive(&symbol).await,
                    None => false,
                };
                let mut state = self.state.write().await;
                let switched = if preserve_target {
                    state.switch_active_preserving_state(symbol.clone())
                } else {
                    state.switch_active(symbol.clone())
                };
                if let Err(err) = switched {
                    return CommandResponse::err(err);
                }
                drop(state);
                if let Some(scopes) = &self.scopes {
                    scopes.set_default(symbol.clone()).await;
                }
            }
        }
        let refresh_error = if let Some(refresher) = &self.refresher {
            refresher.refresh_market(&symbol).await.err()
        } else {
            None
        };
        let state = self.state.read().await;
        let mut lines = vec![render_active_instrument(&symbol, input.as_deref())];
        if let Some(err) = refresh_error {
            lines.push(format!("refresh failed: {err}"));
        }
        lines.extend(sync_lines(&state, &symbol));
        CommandResponse::ok(lines)
    }

    async fn account_mode(&self) -> CommandResponse {
        let state = self.state.read().await;
        let required = state
            .required_account_mode
            .map(AccountMode::as_str)
            .unwrap_or("any");
        let Some(mode) = state.account_mode else {
            return CommandResponse::err(format!("account mode unavailable required={required}"));
        };
        CommandResponse::ok(vec![format!(
            "account mode={} required={} source={}",
            mode.value.as_str(),
            required,
            mode.value.account_value_source()
        )])
    }

    async fn require_account_mode(&self, mode: Option<String>) -> CommandResponse {
        let required = match mode {
            Some(mode) => match AccountMode::parse(&mode) {
                Ok(mode) => Some(mode),
                Err(err) => return CommandResponse::err(err),
            },
            None => None,
        };
        self.state.write().await.set_required_account_mode(required);
        CommandResponse::ok(vec![format!(
            "account mode requirement={}",
            required.map(AccountMode::as_str).unwrap_or("any")
        )])
    }

    async fn print_var(&self, session_id: Option<u64>, name: Option<String>) -> CommandResponse {
        if let Some(id) = session_id {
            let Some(scopes) = &self.scopes else {
                return CommandResponse::err("market sessions unavailable");
            };
            let values = match scopes.variables(id).await {
                Ok(values) => values,
                Err(err) => return CommandResponse::err(err),
            };
            return match name {
                Some(name) => match values.iter().find(|(candidate, _)| candidate == &name) {
                    Some((_, value)) => CommandResponse::ok(vec![format!("{name}={value}")]),
                    None => CommandResponse::err(format!("unknown variable {name}")),
                },
                None => CommandResponse::ok(
                    values
                        .into_iter()
                        .map(|(name, value)| format!("{name}={value}"))
                        .collect::<Vec<_>>(),
                ),
            };
        }
        let vars = self.variables.lock().await;
        match name {
            Some(name) => match vars.get(&name) {
                Some(value) => CommandResponse::ok(vec![format!("{name}={value}")]),
                None => CommandResponse::err(format!("unknown variable {name}")),
            },
            None => CommandResponse::ok(
                vars.iter()
                    .map(|(name, value)| format!("{name}={value}"))
                    .collect::<Vec<_>>(),
            ),
        }
    }

    async fn render_keybinds(&self) -> CommandResponse {
        let keybinds = self.keybinds.lock().await;
        let lines = keybinds
            .binds()
            .iter()
            .map(|(key, action)| format!("{key} -> {action}"))
            .collect::<Vec<_>>();
        CommandResponse::ok(if lines.is_empty() {
            vec!["no keybinds".to_string()]
        } else {
            lines
        })
    }

    fn market_aliases(&self, symbol: &str) -> Vec<&str> {
        self.config
            .as_ref()
            .map(|config| config.aliases_for(symbol))
            .unwrap_or_default()
    }
}

fn response_from_receipt(receipt: SubmitReceipt) -> CommandResponse {
    let ok = receipt.status == ExecutionStatus::Accepted && receipt.error.is_none();
    let mut lines = vec![format!(
        "execution id={} status={:?}",
        receipt.id, receipt.status
    )];
    lines.extend(receipt.statuses.iter().enumerate().map(render_order_status));
    if let Some(error) = receipt.error {
        lines.push(format!("error={error}"));
    }
    CommandResponse {
        ok,
        clear: false,
        exit: false,
        lines,
    }
}

fn render_order_status((idx, status): (usize, &OrderStatus)) -> String {
    let n = idx + 1;
    match status {
        OrderStatus::Success => format!("order[{n}]=success"),
        OrderStatus::AlreadyTerminal { message } => {
            format!("order[{n}]=already_terminal {message}")
        }
        OrderStatus::Resting { oid, cloid } => match cloid {
            Some(cloid) => format!("order[{n}]=resting oid={oid} cloid={cloid}"),
            None => format!("order[{n}]=resting oid={oid}"),
        },
        OrderStatus::Filled {
            oid,
            total_size,
            average_price,
        } => format!("order[{n}]=filled oid={oid} size={total_size} avg_px={average_price}"),
        OrderStatus::TwapRunning { twap_id } => format!("order[{n}]=twap_running id={twap_id}"),
        OrderStatus::WaitingForFill => format!("order[{n}]=waiting_for_fill"),
        OrderStatus::WaitingForTrigger => format!("order[{n}]=waiting_for_trigger"),
        OrderStatus::Error { message } => format!("order[{n}]=error {message}"),
    }
}

fn sync_lines(state: &TradingState, symbol: &str) -> Vec<String> {
    let reasons = state.sync_reasons(symbol, now_ms(), FreshnessLimits::default());
    if reasons.is_empty() {
        Vec::new()
    } else {
        vec![format!("{symbol} syncing {}", reasons.join(","))]
    }
}

fn orders_sync_lines(state: &TradingState, symbol: &str) -> Vec<String> {
    if state.orders_ready(symbol, now_ms(), FreshnessLimits::default().orders_ms) {
        Vec::new()
    } else {
        vec![format!("{symbol} orders syncing")]
    }
}

fn position_sync_lines(state: &TradingState, symbol: &str) -> Vec<String> {
    let Some(market) = state.markets.get(symbol) else {
        return vec![format!("unknown market {symbol}")];
    };
    if matches!(
        market.kind,
        crate::protocol::MarketKind::Perp | crate::protocol::MarketKind::BuilderPerp
    ) && state.position(symbol).is_none()
    {
        vec![format!("{symbol} position syncing")]
    } else {
        Vec::new()
    }
}

fn render_active_instrument(symbol: &str, alias: Option<&str>) -> String {
    match alias {
        Some(alias) => format!("active instrument {symbol} alias={alias}"),
        None => format!("active instrument {symbol}"),
    }
}

fn render_market(market: &Market, aliases: Vec<&str>) -> String {
    let aliases = if aliases.is_empty() {
        String::new()
    } else {
        format!(" aliases={}", aliases.join(","))
    };
    format!(
        "{}{} kind={:?} asset={} wire={} szDecimals={} maxLeverage={}",
        market.symbol,
        aliases,
        market.kind,
        market.asset.0,
        market.wire_symbol,
        market.size_decimals,
        market
            .max_leverage
            .map_or_else(|| "-".to_string(), |value| value.to_string())
    )
}

fn render_account(state: &TradingState) -> String {
    let Some(account) = state.account_overview() else {
        return "account unavailable".to_string();
    };
    let mut line = format!(
        "account source={} value_usd={} available_usd={} margin_used_usd={} notional_usd={}",
        account.source,
        decimal_or_dash(account.value_usd),
        decimal_or_dash(account.available_usd),
        decimal_or_dash(account.margin_used_usd),
        decimal_or_dash(account.notional_usd)
    );
    if let (Some(value), Some(notional)) = (account.value_usd, account.notional_usd)
        && value > Decimal::ZERO
    {
        line.push_str(&format!(" exposure={}x", (notional / value).round_dp(2)));
    }
    if let Some(summary) = state.balance_summary.as_ref().map(|summary| &summary.value)
        && summary.spot_unpriced_count > 0
    {
        line.push_str(&format!(
            " unpriced_spot_balances={}",
            summary.spot_unpriced_count
        ));
    }
    let unrealized = unrealized_pnl(state);
    line.push_str(&format!(" uPnL={}", signed(unrealized)));
    line
}

fn decimal_or_dash(value: Option<Decimal>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn unrealized_pnl(state: &TradingState) -> Decimal {
    state
        .open_positions()
        .filter_map(|position| position.value.detail.map(|detail| detail.unrealized_pnl))
        .sum()
}

fn render_position(position: &Fresh<Position>, mark: Option<Decimal>) -> String {
    let position = &position.value;
    let mut line = format!(
        "{} size={} entry={}",
        position.symbol,
        position.size,
        position
            .entry_price
            .map(|price| price.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    if let Some(mark) = mark {
        line.push_str(&format!(" mark={mark}"));
    }
    if let Some(detail) = position.detail {
        line.push_str(&format!(
            " uPnL={} roe={}% liq={} lev={}x({}) notional={} margin={}",
            signed(detail.unrealized_pnl),
            signed((detail.return_on_equity * Decimal::from(100)).round_dp(2)),
            detail
                .liquidation_px
                .map(|price| price.to_string())
                .unwrap_or_else(|| "-".to_string()),
            detail.leverage,
            if detail.leverage_cross {
                "cross"
            } else {
                "iso"
            },
            detail.position_value,
            detail.margin_used
        ));
    }
    line
}

fn render_order(order: &Order, position: Option<&Position>, mark: Option<Decimal>) -> String {
    let side = if order.is_buy { "buy" } else { "sell" };
    let mut line = if order.kind == OrderKind::Limit {
        format!(
            "{} oid={} {} {} @ {}",
            order.symbol, order.oid, side, order.size, order.price
        )
    } else {
        format!(
            "{} oid={} {} {} trg {}",
            order.symbol, order.oid, side, order.size, order.price
        )
    };
    line.push(' ');
    line.push_str(order_kind_label(&order.kind));
    if let Some(tif) = &order.tif {
        line.push_str(&format!(" tif={}", tif_label(tif)));
    }
    if order.reduce_only {
        line.push_str(" reduce");
    }
    if let Some(pnl) = position.and_then(|position| projected_pnl(order, position)) {
        line.push_str(&format!(" pnl={}", signed(pnl)));
    }
    if let Some(distance) = mark.and_then(|mark| distance_pct(order.price, mark)) {
        line.push_str(&format!(" dist={}%", signed(distance)));
    }
    if let Some(cloid) = &order.cloid {
        line.push_str(&format!(" cloid={cloid}"));
    }
    line
}

fn render_balance(balance: &SpotBalance, borrow_lend: Option<&BorrowLend>) -> String {
    let mut line = format!(
        "balance {} total={} available={} hold={} entryNtl={}",
        balance.coin,
        balance.total,
        balance.available(),
        balance.hold,
        balance.entry_ntl
    );
    push_optional(&mut line, "ltv", balance.ltv);
    push_optional(&mut line, "supplied", balance.supplied);
    push_optional(
        &mut line,
        "availableAfterMaintenance",
        balance.available_after_maintenance,
    );
    push_optional(
        &mut line,
        "portfolioBorrowRatio",
        balance.portfolio_borrow_ratio,
    );
    if let Some(entry) = borrow_lend {
        line.push_str(&format!(
            " borrowValue={} supplyValue={}",
            entry.borrow_value, entry.supply_value
        ));
    }
    line
}

fn render_borrow_lend(entry: &BorrowLend) -> String {
    format!(
        "borrow_lend token={} borrowValue={} supplyValue={}",
        entry.token, entry.borrow_value, entry.supply_value
    )
}

fn push_optional(line: &mut String, label: &str, value: Option<Decimal>) {
    if let Some(value) = value {
        line.push_str(&format!(" {label}={value}"));
    }
}

fn render_twap(id: u64, twap: &Twap) -> String {
    let side = if twap.is_buy { "buy" } else { "sell" };
    let pct = if twap.size > Decimal::ZERO {
        (twap.executed_size / twap.size * Decimal::from(100)).round_dp(0)
    } else {
        Decimal::ZERO
    };
    format!(
        "{id}:{side} {}/{}({pct}%) {}m",
        twap.executed_size, twap.size, twap.minutes
    )
}

fn mark_for(state: &TradingState, symbol: &str) -> Option<Decimal> {
    state.book.get(symbol).map(|book| mid(book.value))
}

fn mid(book: Book) -> Decimal {
    (book.bid + book.ask) / Decimal::from(2)
}

fn signed(value: Decimal) -> String {
    if value.is_sign_negative() {
        value.to_string()
    } else {
        format!("+{value}")
    }
}

fn optional_health(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "connected",
        Some(false) => "disconnected",
        None => "disabled",
    }
}

fn distance_pct(price: Decimal, mark: Decimal) -> Option<Decimal> {
    (mark > Decimal::ZERO).then(|| ((price - mark) / mark * Decimal::from(100)).round_dp(2))
}

fn projected_pnl(order: &Order, position: &Position) -> Option<Decimal> {
    let entry = position.entry_price?;
    if position.flat() {
        return None;
    }
    let reduces = (position.size > Decimal::ZERO && !order.is_buy)
        || (position.size < Decimal::ZERO && order.is_buy);
    if !(order.reduce_only || reduces) {
        return None;
    }
    let size = order.size.min(position.size.abs());
    let direction = if position.size > Decimal::ZERO {
        Decimal::ONE
    } else {
        -Decimal::ONE
    };
    Some((size * (order.price - entry) * direction).round_dp(2))
}

fn order_kind_label(kind: &OrderKind) -> &'static str {
    match kind {
        OrderKind::Limit => "limit",
        OrderKind::StopLoss => "SL",
        OrderKind::TakeProfit => "TP",
        OrderKind::TrailingStop => "trail",
    }
}

fn tif_label(tif: &TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Alo => "alo",
        TimeInForce::Ioc => "ioc",
        TimeInForce::Gtc => "gtc",
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandExecutionState {
    Queued,
    Running,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandExecutionRecord {
    pub command_id: u64,
    pub command: String,
    pub state: CommandExecutionState,
    pub submitted_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub response: Option<CommandResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandSubmitResponse {
    pub command_id: u64,
    pub accepted_at_ms: u64,
    pub queued_ahead: u32,
    pub queue_depth: u32,
    pub running_command_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandResultsResponse {
    pub latest_command_id: u64,
    pub queue_depth: u32,
    pub running_command_id: Option<u64>,
    pub records: Vec<CommandExecutionRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandPendingResponse {
    pub queue_depth: u32,
    pub running_command_id: Option<u64>,
    pub confirmations: Vec<PendingConfirmationView>,
}

pub struct CommandQueue {
    service: Arc<CommandService>,
    state: Mutex<QueueState>,
    notify: Notify,
    history_limit: usize,
}

#[derive(Default)]
struct QueueState {
    next_id: u64,
    running: Option<u64>,
    queue: VecDeque<u64>,
    jobs: BTreeMap<u64, CommandExecutionRecord>,
    session_ids: BTreeMap<u64, u64>,
    completed: VecDeque<u64>,
}

impl CommandQueue {
    pub fn new(service: Arc<CommandService>) -> Arc<Self> {
        let queue = Arc::new(Self {
            service,
            state: Mutex::new(QueueState::default()),
            notify: Notify::new(),
            history_limit: 200,
        });
        tokio::spawn(queue.clone().worker());
        queue
    }

    pub async fn submit(&self, command: String) -> CommandSubmitResponse {
        self.enqueue(command, None).await
    }

    pub async fn submit_scoped(
        &self,
        session_id: u64,
        command: String,
    ) -> Result<CommandSubmitResponse, String> {
        let scopes = self
            .service
            .scopes
            .as_ref()
            .ok_or_else(|| "market sessions unavailable".to_string())?;
        scopes.retain_job(session_id).await?;
        Ok(self.enqueue(command, Some(session_id)).await)
    }

    async fn enqueue(&self, command: String, session_id: Option<u64>) -> CommandSubmitResponse {
        let mut state = self.state.lock().await;
        state.next_id = state.next_id.saturating_add(1);
        let command_id = state.next_id;
        let queued_ahead = queue_depth(&state);
        let accepted_at_ms = now_ms();
        state.queue.push_back(command_id);
        if let Some(session_id) = session_id {
            state.session_ids.insert(command_id, session_id);
        }
        state.jobs.insert(
            command_id,
            CommandExecutionRecord {
                command_id,
                command,
                state: CommandExecutionState::Queued,
                submitted_at_ms: accepted_at_ms,
                started_at_ms: None,
                finished_at_ms: None,
                response: None,
            },
        );
        let response = CommandSubmitResponse {
            command_id,
            accepted_at_ms,
            queued_ahead,
            queue_depth: queue_depth(&state),
            running_command_id: state.running,
        };
        drop(state);
        self.notify.notify_one();
        response
    }

    pub async fn get(&self, command_id: u64) -> Option<CommandExecutionRecord> {
        self.state.lock().await.jobs.get(&command_id).cloned()
    }

    pub async fn keybinds(&self) -> BTreeMap<String, String> {
        self.service.keybinds().await
    }

    pub async fn open_market_session(
        &self,
        market: String,
    ) -> Result<MarketSessionResponse, String> {
        self.service.open_market_session(market).await
    }

    pub async fn market_session(&self, session_id: u64) -> Result<MarketSessionResponse, String> {
        self.service.market_session(session_id).await
    }

    pub async fn close_market_session(&self, session_id: u64) -> Result<(), String> {
        self.service.close_market_session(session_id).await
    }

    pub async fn pending(&self) -> CommandPendingResponse {
        let state = self.state.lock().await;
        let queue_depth = queue_depth(&state);
        let running_command_id = state.running;
        drop(state);
        let confirmations = self.service.pending_confirmations().await;
        CommandPendingResponse {
            queue_depth,
            running_command_id,
            confirmations,
        }
    }

    pub async fn results(&self, after_command_id: u64, limit: u32) -> CommandResultsResponse {
        let state = self.state.lock().await;
        let mut records = Vec::new();
        for id in &state.completed {
            if *id > after_command_id
                && let Some(record) = state.jobs.get(id)
            {
                records.push(record.clone());
                if records.len() >= limit.max(1) as usize {
                    break;
                }
            }
        }
        CommandResultsResponse {
            latest_command_id: state.completed.back().copied().unwrap_or(0),
            queue_depth: queue_depth(&state),
            running_command_id: state.running,
            records,
        }
    }

    async fn worker(self: Arc<Self>) {
        loop {
            let Some((id, command, session_id)) = self.next_job().await else {
                continue;
            };
            let response = match session_id {
                Some(session_id) => self.service.execute_line_scoped(session_id, &command).await,
                None => self.service.execute_line(&command).await,
            };
            if let Some(session_id) = session_id
                && let Some(scopes) = &self.service.scopes
            {
                scopes.release_job(session_id).await;
            }
            let mut state = self.state.lock().await;
            state.running = None;
            if let Some(job) = state.jobs.get_mut(&id) {
                job.state = CommandExecutionState::Completed;
                job.finished_at_ms = Some(now_ms());
                job.response = Some(response);
            }
            state.completed.push_back(id);
            while state.completed.len() > self.history_limit {
                if let Some(old) = state.completed.pop_front() {
                    state.jobs.remove(&old);
                    state.session_ids.remove(&old);
                }
            }
            drop(state);
            self.notify.notify_one();
        }
    }

    async fn next_job(&self) -> Option<(u64, String, Option<u64>)> {
        loop {
            let notified = self.notify.notified();
            {
                let mut state = self.state.lock().await;
                if state.running.is_none()
                    && let Some(id) = state.queue.pop_front()
                {
                    state.running = Some(id);
                    let session_id = state.session_ids.get(&id).copied();
                    if let Some(job) = state.jobs.get_mut(&id) {
                        job.state = CommandExecutionState::Running;
                        job.started_at_ms = Some(now_ms());
                        return Some((id, job.command.clone(), session_id));
                    }
                }
            }
            notified.await;
        }
    }
}

fn queue_depth(state: &QueueState) -> u32 {
    (state.queue.len() + usize::from(state.running.is_some())) as u32
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
