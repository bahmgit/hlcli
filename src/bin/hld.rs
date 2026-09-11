use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use clap::Parser;
use hl_v2::{
    config::Config,
    core::{CommandExecutor, Core, ExecuteFuture, ExecutionRecovery, PlanExecutor},
    exchange::{ActionConnectionHealth, Network, WsActionClient},
    execution::{ActionJournal, ExecutionControl},
    feed::spawn_state_feed,
    info::{InfoClient, InfoReconciler, spawn_info_refresh},
    ipc::{self, IpcApp},
    managed::ManagedExecutor,
    metrics::Metrics,
    operator::KeybindStore,
    protocol::Chain,
    runtime::{CommandQueue, CommandService, RefreshFuture, StateRefresher},
    scope::ScopeRegistry,
    security,
    server::{self, AppState},
    state::{AccountMode, TradingState},
};
use tokio::sync::RwLock;
use zeroize::Zeroize;

const INFO_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Parser)]
#[command(name = "hld", about = "Hyperliquid v2 daemon")]
struct Args {
    #[arg(long, env = "HL_V2_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[arg(long, conflicts_with = "testnet")]
    mainnet: bool,
    #[arg(long, conflicts_with = "mainnet")]
    testnet: bool,
    #[arg(long, env = "HL_V2_BIND")]
    bind: Option<std::net::SocketAddr>,
    #[arg(long, env = "HL_V2_ALLOW_REMOTE")]
    allow_remote: bool,
    #[arg(long, env = "HL_V2_IPC_PATH")]
    ipc_path: Option<PathBuf>,
    #[arg(long, env = "HL_V2_BACKEND_TOKEN")]
    backend_token: Option<String>,
    #[arg(long, env = "HL_V2_WALLET_PROFILE")]
    wallet_profile: Option<String>,
    #[arg(long, env = "HL_V2_PASSWORD_FILE")]
    password_file: Option<PathBuf>,
    #[arg(long)]
    read_only: bool,
    #[arg(long, env = "HL_V2_USER")]
    user: Option<String>,
}

struct Session {
    executor: Arc<dyn CommandExecutor>,
    managed_inner: Option<Arc<dyn PlanExecutor>>,
    recovery: Option<Arc<dyn ExecutionRecovery>>,
    user: Option<String>,
    execution: ExecutionControl,
    action_connection: Option<ActionConnectionHealth>,
}

struct ActiveRefresher {
    client: InfoClient,
    state: Arc<RwLock<TradingState>>,
    user: Option<String>,
    metrics: Metrics,
}

impl StateRefresher for ActiveRefresher {
    fn refresh_market<'a>(&'a self, symbol: &'a str) -> RefreshFuture<'a> {
        Box::pin(async move {
            self.metrics.refresh_state();
            self.client
                .refresh_market(&self.state, self.user.as_deref(), symbol)
                .await
                .map_err(|err| err.to_string())
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut cfg = Config::load(args.data_dir.clone())?;
    apply_overrides(&mut cfg, &args)?;
    let metrics = Metrics::default();
    let state = Arc::new(RwLock::new(TradingState::new(cfg.default_symbol.clone())));
    let session = session(&cfg, &args, state.clone(), metrics.clone()).await?;
    let mut seeded = InfoClient::with_metrics(cfg.network, metrics.clone())
        .seed_state(
            &cfg.default_symbol,
            &cfg.allowed_symbols,
            session.user.as_deref(),
        )
        .await?;
    seeded.set_required_account_mode(required_account_mode(&cfg)?);
    *state.write().await = seeded;
    if let Some(recovery) = &session.recovery
        && let Err(err) = recovery.recover_execution().await
    {
        eprintln!("execution recovery halted trading: {err}");
    }
    let scopes = ScopeRegistry::new(state.read().await.active.clone())?;
    let executor: Arc<dyn CommandExecutor> = match session.managed_inner.clone() {
        Some(inner) => {
            ManagedExecutor::spawn_persisted_with_scopes(
                state.clone(),
                inner,
                std::time::Duration::from_millis(250),
                cfg.managed_state_path(),
                session.execution.clone(),
                Some(scopes.clone()),
            )
            .await?
        }
        None => session.executor.clone(),
    };
    spawn_state_feed(
        cfg.network,
        session.user.clone(),
        state.clone(),
        metrics.clone(),
        scopes.subscribe(),
    );
    spawn_info_refresh(
        cfg.network,
        session.user.clone(),
        state.clone(),
        metrics.clone(),
        INFO_REFRESH_INTERVAL,
        scopes.subscribe(),
    );
    let refresher = Arc::new(ActiveRefresher {
        client: InfoClient::with_metrics(cfg.network, metrics.clone()),
        state: state.clone(),
        user: session.user.clone(),
        metrics: metrics.clone(),
    });
    let service = Arc::new(
        CommandService::new(
            state.clone(),
            executor,
            KeybindStore::load(cfg.keybinds_path())?,
        )
        .with_config(&cfg)
        .with_refresher(refresher)
        .with_scopes(scopes.clone())
        .with_diagnostics(
            metrics.clone(),
            session.execution.clone(),
            session.action_connection.clone(),
        ),
    );
    tokio::spawn({
        let scopes = scopes.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                scopes.expire_idle().await;
            }
        }
    });
    let queue = CommandQueue::new(service);
    let server_queue = queue.clone();
    let ipc_path = args
        .ipc_path
        .unwrap_or_else(|| cfg.runtime_dir().join("backend.sock"));
    let http_listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    let ipc_listener = ipc::bind(&ipc_path).await?;
    let ipc_app = IpcApp::new(state.clone(), metrics.clone(), queue)
        .with_execution_control(session.execution.clone())
        .with_action_connection(session.action_connection.clone());
    let http_app = server::router(
        AppState::from_shared_state(state, metrics, &cfg)
            .with_queue(server_queue)
            .with_execution_control(session.execution)
            .with_action_connection(session.action_connection),
    );
    println!("hld http {}", cfg.bind);
    println!("hld ipc {}", ipc_path.display());
    tokio::select! {
        result = ipc::serve_bound(ipc_listener, ipc_app, 8 * 1024 * 1024) => result,
        result = axum::serve(http_listener, http_app) => result.map_err(Into::into),
    }
}

fn apply_overrides(cfg: &mut Config, args: &Args) -> anyhow::Result<()> {
    if args.mainnet {
        cfg.network = Network::Mainnet;
    }
    if args.testnet {
        cfg.network = Network::Testnet;
    }
    if let Some(bind) = args.bind {
        cfg.bind = bind;
    }
    if args.allow_remote {
        cfg.allow_remote = true;
    }
    if let Some(token) = args.backend_token.as_ref() {
        anyhow::ensure!(!token.trim().is_empty(), "backend token must not be empty");
        cfg.backend_token = Some(token.clone());
    }
    Ok(())
}

async fn session(
    cfg: &Config,
    args: &Args,
    state: Arc<RwLock<TradingState>>,
    metrics: Metrics,
) -> anyhow::Result<Session> {
    if args.read_only {
        return Ok(Session {
            executor: Arc::new(ReadOnlyExecutor),
            managed_inner: None,
            recovery: None,
            user: args.user.clone(),
            execution: ExecutionControl::default(),
            action_connection: None,
        });
    }
    let profile = security::selected_profile_id(&cfg.data_dir, args.wallet_profile.as_deref())?;
    let credentials = if security::credentials_exist(&cfg.data_dir, &profile)? {
        with_password(credential_password(args, false)?, |password| {
            security::load_credentials(&cfg.data_dir, Some(&profile), password)
        })?
    } else {
        let credentials = collect_wallet_credentials(cfg.network, &profile)?;
        with_password(credential_password(args, true)?, |password| {
            security::store_credentials(&cfg.data_dir, &profile, password, &credentials)
        })?;
        println!("created wallet profile {profile}");
        credentials
    };
    ensure_network(cfg.network, &credentials.network)?;
    let signer: PrivateKeySigner = credentials.api_private_key.parse()?;
    let journal = ActionJournal::new(cfg.journal_path());
    let user = cfg
        .vault_address
        .clone()
        .unwrap_or_else(|| credentials.main_wallet.clone());
    let action_client = WsActionClient::connect(cfg.network).await?;
    let action_connection = action_client.connection_health();
    let mut core = Core::from_shared_state(
        state.clone(),
        journal,
        signer,
        action_client,
        chain(cfg.network),
        metrics.clone(),
    );
    if let Some(vault) = cfg.vault_address.as_deref() {
        core = core.with_vault(vault.parse::<Address>()?);
    }
    core = core.with_market_cross_bps(cfg.market_cross_bps);
    core = core.with_reconciler(Arc::new(InfoReconciler::new(
        InfoClient::with_metrics(cfg.network, metrics.clone()),
        user.clone(),
        state.clone(),
    )));
    let core = Arc::new(core);
    let execution = core.execution_control();
    Ok(Session {
        executor: core.clone(),
        managed_inner: Some(core.clone()),
        recovery: Some(core),
        user: Some(user),
        execution,
        action_connection: Some(action_connection),
    })
}

fn credential_password(args: &Args, create: bool) -> anyhow::Result<String> {
    if let Some(path) = args.password_file.as_ref() {
        let password = std::fs::read_to_string(path)?.trim().to_string();
        anyhow::ensure!(
            !create || !password.is_empty(),
            "credentials password must not be empty"
        );
        return Ok(password);
    }
    if !create {
        return prompt_password("Enter credentials password: ");
    }
    let mut password = prompt_password("Create credentials password: ")?;
    anyhow::ensure!(
        !password.is_empty(),
        "credentials password must not be empty"
    );
    let mut confirm = prompt_password("Confirm credentials password: ")?;
    let matches = password == confirm;
    confirm.zeroize();
    if !matches {
        password.zeroize();
        anyhow::bail!("credentials password confirmation mismatch");
    }
    Ok(password)
}

fn with_password<T>(
    mut password: String,
    op: impl FnOnce(&[u8]) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let result = op(password.as_bytes());
    password.zeroize();
    result
}

fn collect_wallet_credentials(
    network: Network,
    profile: &str,
) -> anyhow::Result<security::Credentials> {
    eprintln!("setting up wallet profile {profile}");
    let wallet = prompt_line("Main wallet address (0x...): ")?;
    let wallet: Address = wallet
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid main wallet address"))?;
    let mut api_private_key = prompt_password("API private key (hex): ")?;
    if api_private_key.parse::<PrivateKeySigner>().is_err() {
        api_private_key.zeroize();
        anyhow::bail!("invalid API private key");
    }
    Ok(security::Credentials {
        main_wallet: wallet.to_string(),
        api_private_key,
        network: match network {
            Network::Mainnet => "Mainnet".to_string(),
            Network::Testnet => "Testnet".to_string(),
        },
    })
}

fn prompt_line(prompt: &str) -> anyhow::Result<String> {
    print!("{prompt}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let line = line.trim().to_string();
    anyhow::ensure!(!line.is_empty(), "input must not be empty");
    Ok(line)
}

fn prompt_password(prompt: &str) -> anyhow::Result<String> {
    #[cfg(not(unix))]
    {
        let _ = prompt;
        anyhow::bail!("provide --password-file or HL_V2_PASSWORD_FILE");
    }

    #[cfg(unix)]
    {
        let mut tty = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")
            .map_err(|_| {
                anyhow::anyhow!("secure password prompt requires a TTY; provide --password-file")
            })?;
        tty.write_all(prompt.as_bytes())?;
        tty.flush()?;
        let _guard = EchoGuard::disable(&tty)?;
        let mut line = String::new();
        BufReader::new(tty.try_clone()?).read_line(&mut line)?;
        tty.write_all(b"\n")?;
        Ok(line.trim_end_matches(&['\r', '\n'][..]).to_string())
    }
}

#[cfg(unix)]
struct EchoGuard;

#[cfg(unix)]
impl EchoGuard {
    fn disable(tty: &std::fs::File) -> anyhow::Result<Self> {
        let status = Command::new("stty")
            .arg("-echo")
            .stdin(Stdio::from(tty.try_clone()?))
            .status()?;
        anyhow::ensure!(
            status.success(),
            "failed to secure password input (echo disabled unavailable)"
        );
        Ok(Self)
    }
}

#[cfg(unix)]
impl Drop for EchoGuard {
    fn drop(&mut self) {
        if let Ok(tty) = std::fs::OpenOptions::new().read(true).open("/dev/tty") {
            let _ = Command::new("stty")
                .arg("echo")
                .stdin(Stdio::from(tty))
                .status();
        }
    }
}

fn ensure_network(network: Network, credential_network: &str) -> anyhow::Result<()> {
    let expected = match network {
        Network::Mainnet => "mainnet",
        Network::Testnet => "testnet",
    };
    anyhow::ensure!(
        credential_network.eq_ignore_ascii_case(expected),
        "credential network mismatch: stored={} active={expected}",
        credential_network
    );
    Ok(())
}

fn required_account_mode(cfg: &Config) -> anyhow::Result<Option<AccountMode>> {
    cfg.required_account_mode
        .as_deref()
        .map(AccountMode::parse)
        .transpose()
        .map_err(anyhow::Error::msg)
}

fn chain(network: Network) -> Chain {
    match network {
        Network::Mainnet => Chain::Mainnet,
        Network::Testnet => Chain::Testnet,
    }
}

struct ReadOnlyExecutor;

impl CommandExecutor for ReadOnlyExecutor {
    fn execute_command<'a>(&'a self, _: hl_v2::command::Command) -> ExecuteFuture<'a> {
        Box::pin(async { Err("daemon is read-only".to_string()) })
    }

    fn execute_command_for<'a>(
        &'a self,
        _: &'a str,
        _: hl_v2::command::Command,
    ) -> ExecuteFuture<'a> {
        Box::pin(async { Err("daemon is read-only".to_string()) })
    }
}
