mod support;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use hl_v2::{
    command::Command,
    config::Config,
    core::{CommandExecutor, ExecuteFuture},
    execution::{ExecutionControl, ExecutionStatus, SubmitReceipt},
    metrics::Metrics,
    operator::KeybindStore,
    runtime::{CommandExecutionState, CommandQueue, CommandService, RefreshFuture, StateRefresher},
    scope::ScopeRegistry,
    state::{AccountMode, BalanceSummary},
};
use tokio::sync::RwLock;

use support::{dec, now_ms, ready_perp};

#[derive(Default)]
struct RecordingExecutor {
    commands: Mutex<Vec<(Option<String>, Command)>>,
}

struct SuccessfulRefresher;

impl StateRefresher for SuccessfulRefresher {
    fn refresh_market<'a>(&'a self, _symbol: &'a str) -> RefreshFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

impl CommandExecutor for RecordingExecutor {
    fn execute_command<'a>(&'a self, command: Command) -> ExecuteFuture<'a> {
        self.commands.lock().unwrap().push((None, command));
        accepted()
    }

    fn execute_command_for<'a>(&'a self, symbol: &'a str, command: Command) -> ExecuteFuture<'a> {
        self.commands
            .lock()
            .unwrap()
            .push((Some(symbol.to_string()), command));
        accepted()
    }
}

fn accepted<'a>() -> ExecuteFuture<'a> {
    Box::pin(async {
        Ok(SubmitReceipt {
            id: 1,
            status: ExecutionStatus::Accepted,
            statuses: Vec::new(),
            error: None,
        })
    })
}

fn temp_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("hl-v2-{label}-{}-{}", std::process::id(), now_ms()))
}

fn service(executor: Arc<RecordingExecutor>) -> Arc<CommandService> {
    Arc::new(CommandService::new(
        Arc::new(RwLock::new(ready_perp("0"))),
        executor,
        KeybindStore::load(temp_path("keybinds")).unwrap(),
    ))
}

#[tokio::test]
async fn account_wide_actions_require_typed_confirmation() {
    let executor = Arc::new(RecordingExecutor::default());
    let service = service(executor.clone());

    let prompt = service.execute_line("account mode set unified").await;
    assert!(prompt.ok);
    assert!(prompt.lines[0].contains("reply yes or no"));
    assert!(executor.commands.lock().unwrap().is_empty());

    let blocked = service.execute_line("buy 0.01").await;
    assert!(!blocked.ok);
    assert!(blocked.lines[0].contains("confirmation pending"));

    assert!(service.execute_line("no").await.ok);
    assert!(executor.commands.lock().unwrap().is_empty());
}

#[tokio::test]
async fn impossible_account_mode_transition_never_stages_confirmation() {
    let executor = Arc::new(RecordingExecutor::default());
    let mut state = ready_perp("0");
    let now = now_ms();
    state.apply_account_mode(AccountMode::UnifiedAccount, now, Some(now));
    let state = Arc::new(RwLock::new(state));
    let service = CommandService::new(
        state.clone(),
        executor.clone(),
        KeybindStore::load(temp_path("account-mode-boundary-keybinds")).unwrap(),
    );

    let response = service.execute_line("account mode set standard").await;
    assert!(!response.ok);
    assert!(response.lines[0].contains("requires the main user signer"));
    assert!(executor.commands.lock().unwrap().is_empty());

    state
        .write()
        .await
        .apply_account_mode(AccountMode::Standard, 0, None);
    let response = service.execute_line("account mode set unified").await;
    assert!(!response.ok);
    assert!(response.lines[0].contains("account mode stale"));
    assert!(executor.commands.lock().unwrap().is_empty());
}

#[tokio::test]
async fn account_wide_confirmation_is_unique_across_market_sessions() {
    let executor = Arc::new(RecordingExecutor::default());
    let scopes = ScopeRegistry::new("BTC".to_string()).unwrap();
    let service = CommandService::new(
        Arc::new(RwLock::new(ready_perp("0"))),
        executor,
        KeybindStore::load(temp_path("confirmation-scope-keybinds")).unwrap(),
    )
    .with_scopes(scopes.clone());
    let first = scopes.open("BTC".to_string()).await;
    let second = scopes.open("BTC".to_string()).await;

    assert!(
        service
            .execute_line_scoped(first, "account mode set unified")
            .await
            .ok
    );
    let blocked = service
        .execute_line_scoped(second, "account mode set portfolioMargin")
        .await;
    assert!(!blocked.ok);
    assert!(blocked.lines[0].contains("account-wide confirmation already pending"));
    assert!(blocked.lines[0].contains("its owner must reply"));
    service.close_market_session(first).await.unwrap();
    let replacement = service
        .execute_line_scoped(second, "account mode set portfolioMargin")
        .await;
    assert!(replacement.ok, "{replacement:?}");
    assert!(service.execute_line_scoped(second, "no").await.ok);
}

#[tokio::test]
async fn quit_clear_and_chain_control_are_typed_not_text_conventions() {
    let service = service(Arc::new(RecordingExecutor::default()));
    let quit = service.execute_line("status; q; status").await;
    assert!(quit.ok);
    assert!(quit.exit);
    assert_eq!(
        quit.lines.len(),
        2,
        "second status must not execute after quit"
    );

    let clear = service.execute_line("status; clear").await;
    assert!(clear.ok);
    assert!(clear.clear);
    assert!(clear.lines.is_empty());

    let wait = service.execute_line("wait 1ms; status").await;
    assert!(wait.ok);
    assert!(wait.lines.iter().any(|line| line.starts_with("active=BTC")));
}

#[tokio::test]
async fn every_read_policy_and_shell_family_executes_through_runtime() {
    let keybind_path = temp_path("surface-keybinds");
    let service = CommandService::new(
        Arc::new(RwLock::new(ready_perp("0"))),
        Arc::new(RecordingExecutor::default()),
        KeybindStore::load(&keybind_path).unwrap(),
    )
    .with_config(&Config::defaults(temp_path("surface-config")))
    .with_refresher(Arc::new(SuccessfulRefresher))
    .with_diagnostics(Metrics::default(), ExecutionControl::default(), None);
    for command in [
        "help",
        "help all",
        "status",
        "portfolio",
        "balances",
        "orders",
        "position",
        "risk",
        "config",
        "config get market-cross-bps",
        "doctor",
        "refresh",
        "markets",
        "markets perps",
        "markets hip3",
        "markets spot",
        "instrument",
        "instrument BTC",
        "account mode",
        "account mode require any",
        "set @size 0.01",
        "print @size",
        "print",
        "unset @size",
        "keybinds",
        "bind k status",
        "unbind k",
    ] {
        let response = service.execute_line(command).await;
        assert!(response.ok, "{command:?}: {response:?}");
    }
    let _ = std::fs::remove_file(keybind_path);
}

#[tokio::test]
async fn retained_scoped_job_finishes_after_session_close() {
    let executor = Arc::new(RecordingExecutor::default());
    let scopes = ScopeRegistry::new("BTC".to_string()).unwrap();
    let service = Arc::new(
        CommandService::new(
            Arc::new(RwLock::new(ready_perp("0"))),
            executor,
            KeybindStore::load(temp_path("scope-keybinds")).unwrap(),
        )
        .with_scopes(scopes.clone()),
    );
    let queue = CommandQueue::new(service);
    let session = queue.open_market_session("BTC".to_string()).await.unwrap();
    let submitted = queue
        .submit_scoped(session.session_id, "sleep 50ms; status".to_string())
        .await
        .unwrap();
    queue
        .close_market_session(session.session_id)
        .await
        .unwrap();
    assert!(queue.market_session(session.session_id).await.is_err());

    for _ in 0..100 {
        if let Some(record) = queue.get(submitted.command_id).await
            && record.state == CommandExecutionState::Completed
        {
            let response = record.response.unwrap();
            assert!(response.ok, "{response:?}");
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("retained scoped job did not complete");
}

#[tokio::test]
async fn account_summary_and_doctor_report_the_authoritative_source_and_causes() {
    let mut seeded = ready_perp("0");
    let now = now_ms();
    seeded.apply_account_mode(AccountMode::UnifiedAccount, now, Some(now));
    seeded.apply_spot_balances(
        Vec::new(),
        BalanceSummary {
            portfolio_margin_enabled: false,
            portfolio_margin_ratio: None,
            spot_value_usd: Some(dec("1234")),
            spot_available_usd: Some(dec("1200")),
            spot_unpriced_count: 0,
            borrow_lend_health: None,
            borrow_lend_health_factor: None,
        },
        now,
        Some(now),
    );
    let state = Arc::new(RwLock::new(seeded));
    assert_eq!(
        state.read().await.account_overview().unwrap().source,
        "spotClearinghouseState"
    );

    let metrics = Metrics::default();
    metrics.state_ws_disconnected();
    let execution = ExecutionControl::default();
    execution.halt("ambiguous action".to_string()).await;
    let service = CommandService::new(
        state,
        Arc::new(RecordingExecutor::default()),
        KeybindStore::load(temp_path("doctor-keybinds")).unwrap(),
    )
    .with_diagnostics(metrics, execution, None);
    let doctor = service.execute_line("doctor").await;
    assert!(!doctor.ok);
    assert!(
        doctor
            .lines
            .iter()
            .any(|line| line.contains("state feed disconnected"))
    );
    assert!(
        doctor
            .lines
            .iter()
            .any(|line| line.contains("ambiguous action"))
    );
}
