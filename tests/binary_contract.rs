mod support;

use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};

use hl_v2::{
    command::Command,
    core::{CommandExecutor, ExecuteFuture},
    execution::{ExecutionStatus, SubmitReceipt},
    ipc::{self, IpcApp},
    metrics::Metrics,
    operator::KeybindStore,
    runtime::{CommandQueue, CommandService},
    scope::ScopeRegistry,
};
use tokio::sync::RwLock;

use support::{now_ms, ready_perp};

struct NoopExecutor;

impl CommandExecutor for NoopExecutor {
    fn execute_command<'a>(&'a self, _command: Command) -> ExecuteFuture<'a> {
        accepted()
    }

    fn execute_command_for<'a>(&'a self, _symbol: &'a str, _command: Command) -> ExecuteFuture<'a> {
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

fn temp_dir() -> PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "hl-v2-binary-{}-{}-{}",
        std::process::id(),
        now_ms(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn run_hl(data_dir: &Path, socket: &Path, args: &[&str]) -> std::process::Output {
    ProcessCommand::new(env!("CARGO_BIN_EXE_hl"))
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--ipc-path")
        .arg(socket)
        .args(args)
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_client_enforces_arity_health_exit_and_typed_quit() {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("backend.sock");
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let metrics = Metrics::default();
    let service = Arc::new(
        CommandService::new(
            state.clone(),
            Arc::new(NoopExecutor),
            KeybindStore::load(dir.join("keybinds.json")).unwrap(),
        )
        .with_scopes(ScopeRegistry::new("BTC".to_string()).unwrap()),
    );
    let app = IpcApp::new(state, metrics.clone(), CommandQueue::new(service));
    let socket_for_server = socket.clone();
    let server = tokio::spawn(async move {
        ipc::serve(&socket_for_server, app, 1_048_576)
            .await
            .unwrap()
    });
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(socket.exists());

    let healthy = run_hl(&dir, &socket, &["health"]);
    assert!(
        healthy.status.success(),
        "{}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    assert!(String::from_utf8_lossy(&healthy.stdout).contains("\"ok\": true"));

    let scoped_health = run_hl(&dir, &socket, &["--market", "BTC", "health"]);
    assert!(
        scoped_health.status.success(),
        "{}",
        String::from_utf8_lossy(&scoped_health.stderr)
    );
    let metrics_output = run_hl(&dir, &socket, &["MeTrIcS"]);
    assert!(metrics_output.status.success());

    let exec = run_hl(&dir, &socket, &["exec", "status"]);
    assert!(exec.status.success());
    assert!(String::from_utf8_lossy(&exec.stdout).contains("active=BTC"));

    let bind = run_hl(&dir, &socket, &["bind", "k", "status"]);
    assert!(bind.status.success());
    let bound = run_hl(&dir, &socket, &["press", "k"]);
    assert!(bound.status.success());
    assert!(String::from_utf8_lossy(&bound.stdout).contains("active=BTC"));

    let clear = run_hl(&dir, &socket, &["clear"]);
    assert!(clear.status.success());
    assert!(clear.stdout.starts_with(b"\x1b[2J\x1b[H"));

    let extra = run_hl(&dir, &socket, &["health", "extra"]);
    assert!(!extra.status.success());
    assert!(String::from_utf8_lossy(&extra.stderr).contains("does not accept arguments"));

    let unbound = run_hl(&dir, &socket, &["press", "u"]);
    assert!(!unbound.status.success());
    assert!(String::from_utf8_lossy(&unbound.stderr).contains("not bound"));
    let press_extra = run_hl(&dir, &socket, &["press", "k", "extra"]);
    assert!(!press_extra.status.success());
    let command_extra = run_hl(&dir, &socket, &["command", "get", "1", "extra"]);
    assert!(!command_extra.status.success());
    let unknown_command = run_hl(&dir, &socket, &["command", "get", "999999"]);
    assert!(!unknown_command.status.success());

    let quit = run_hl(&dir, &socket, &["q"]);
    assert!(
        quit.status.success(),
        "{}",
        String::from_utf8_lossy(&quit.stderr)
    );

    metrics.state_ws_disconnected();
    let unhealthy = run_hl(&dir, &socket, &["health"]);
    assert!(!unhealthy.status.success());
    assert!(String::from_utf8_lossy(&unhealthy.stdout).contains("\"ok\": false"));

    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interactive_shell_restores_the_exact_prior_terminal_state() {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("backend.sock");
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let service = Arc::new(CommandService::new(
        state.clone(),
        Arc::new(NoopExecutor),
        KeybindStore::load(dir.join("keybinds.json")).unwrap(),
    ));
    let app = IpcApp::new(state, Metrics::default(), CommandQueue::new(service));
    let socket_for_server = socket.clone();
    let server = tokio::spawn(async move {
        ipc::serve(&socket_for_server, app, 1_048_576)
            .await
            .unwrap()
    });
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut master = 0;
    let mut slave = 0;
    // SAFETY: openpty initializes both descriptors on success; they are immediately owned by Files.
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        },
        0
    );
    // SAFETY: openpty returned unique live descriptors above.
    let mut master = unsafe { std::fs::File::from_raw_fd(master) };
    // SAFETY: openpty returned unique live descriptors above.
    let slave = unsafe { std::fs::File::from_raw_fd(slave) };
    let mut expected = std::mem::MaybeUninit::<libc::termios>::uninit();
    // SAFETY: tcgetattr initializes expected on success for the live PTY slave descriptor.
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), expected.as_mut_ptr()) },
        0
    );
    // SAFETY: tcgetattr succeeded and initialized the value.
    let mut expected = unsafe { expected.assume_init() };
    expected.c_lflag &= !libc::ECHO;
    expected.c_cc[libc::VMIN] = 2;
    expected.c_cc[libc::VTIME] = 1;
    // SAFETY: expected is a valid termios value for the live PTY slave descriptor.
    assert_eq!(
        unsafe { libc::tcsetattr(slave.as_raw_fd(), libc::TCSANOW, &expected) },
        0
    );

    let duplicate = |fd| {
        // SAFETY: fd is the live PTY slave descriptor and dup returns independent ownership.
        let duplicated = unsafe { libc::dup(fd) };
        assert!(duplicated >= 0);
        // SAFETY: duplicated is a new descriptor owned by the returned File.
        unsafe { std::fs::File::from_raw_fd(duplicated) }
    };
    let mut child = ProcessCommand::new(env!("CARGO_BIN_EXE_hl"))
        .arg("--data-dir")
        .arg(&dir)
        .arg("--ipc-path")
        .arg(&socket)
        .stdin(Stdio::from(duplicate(slave.as_raw_fd())))
        .stdout(Stdio::from(duplicate(slave.as_raw_fd())))
        .stderr(Stdio::from(duplicate(slave.as_raw_fd())))
        .spawn()
        .unwrap();
    master.write_all(b"q\n").unwrap();

    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(status.success());

    let mut actual = std::mem::MaybeUninit::<libc::termios>::uninit();
    // SAFETY: tcgetattr initializes actual on success for the still-live PTY slave descriptor.
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), actual.as_mut_ptr()) },
        0
    );
    // SAFETY: tcgetattr succeeded and initialized the value.
    let actual = unsafe { actual.assume_init() };
    assert_eq!(actual.c_iflag, expected.c_iflag);
    assert_eq!(actual.c_oflag, expected.c_oflag);
    assert_eq!(actual.c_cflag, expected.c_cflag);
    // macOS sets PENDIN as transient kernel state when queued PTY input crosses a
    // canonical-mode transition; it is not a user-configured terminal setting.
    assert_eq!(
        actual.c_lflag & !libc::PENDIN,
        expected.c_lflag & !libc::PENDIN
    );
    assert_eq!(actual.c_cc, expected.c_cc);

    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(dir).unwrap();
}
