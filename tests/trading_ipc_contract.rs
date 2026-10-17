mod support;

use hl_v2::{
    command::Command,
    core::{CommandExecutor, ExecuteFuture},
    execution::{ExecutionStatus, OrderStatus, SubmitReceipt},
    ipc::{self, IpcApp},
    metrics::Metrics,
    operator::KeybindStore,
    runtime::{CommandQueue, CommandService},
    scope::ScopeRegistry,
};
use hl_wire::{Request, RequestEnvelope, Response, ResponseEnvelope};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::{now_ms, ready_perp};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::RwLock,
};

#[derive(Default)]
struct Executor(AtomicUsize);
impl CommandExecutor for Executor {
    fn execute_command<'a>(&'a self, _: Command) -> ExecuteFuture<'a> {
        self.execute_command_for("BTC", Command::Status)
    }
    fn execute_command_for<'a>(&'a self, market: &'a str, _: Command) -> ExecuteFuture<'a> {
        assert_eq!(market, "BTC");
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(SubmitReceipt {
                id: 42,
                status: ExecutionStatus::Accepted,
                statuses: vec![
                    OrderStatus::Resting {
                        oid: 99,
                        cloid: Some("owned".into()),
                    },
                    OrderStatus::Error {
                        message: "minimum size".into(),
                    },
                ],
                error: None,
            })
        })
    }
}

async fn rpc(path: &Path, request: Request) -> Response {
    let mut stream = UnixStream::connect(path).await.unwrap();
    send(&mut stream, request).await;
    let len = stream.read_u32().await.unwrap();
    let mut bytes = vec![0; len as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    match rmp_serde::from_slice::<ResponseEnvelope>(&bytes).unwrap() {
        ResponseEnvelope::Trading(response) => response,
        other => panic!("unexpected response {other:?}"),
    }
}

async fn send(stream: &mut UnixStream, request: Request) {
    let bytes = rmp_serde::to_vec_named(&RequestEnvelope::Trading { request }).unwrap();
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identified_uds_submission_survives_lost_reply_and_preserves_typed_partial_outcomes() {
    let dir = std::env::temp_dir().join(format!(
        "hl-trading-ipc-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let socket = dir.join("backend.sock");
    let mut seeded = ready_perp("0");
    let mut eth = seeded.markets["BTC"].clone();
    eth.symbol = "ETH".into();
    eth.wire_symbol = "ETH".into();
    seeded.markets.insert("ETH".into(), eth);
    let state = Arc::new(RwLock::new(seeded));
    let executor = Arc::new(Executor::default());
    let service = Arc::new(
        CommandService::new(
            state.clone(),
            executor.clone(),
            KeybindStore::load(dir.join("binds")).unwrap(),
        )
        .with_scopes(ScopeRegistry::new("BTC".into()).unwrap()),
    );
    let queue = CommandQueue::new(service).unwrap();
    let app = IpcApp::new(state, Metrics::default(), queue.clone()).with_identity(
        "Testnet".into(),
        Some("fixture-account".into()),
        false,
    );
    let listener = ipc::bind(&socket).await.unwrap();
    let server = tokio::spawn(ipc::serve_bound(listener, app, hl_wire::MAX_FRAME));
    let instance = match rpc(&socket, Request::Hello).await {
        Response::Hello {
            version,
            instance_id,
            network,
            read_only,
            ..
        } => {
            assert_eq!(version, hl_wire::VERSION);
            assert_eq!(network, "Testnet");
            assert!(!read_only);
            instance_id
        }
        other => panic!("{other:?}"),
    };
    let session = match rpc(
        &socket,
        Request::Open {
            instance_id: instance.clone(),
            market: "BTC".into(),
        },
    )
    .await
    {
        Response::Opened {
            session_id, market, ..
        } => {
            assert_eq!(market.symbol, "BTC");
            session_id
        }
        other => panic!("{other:?}"),
    };
    let request = Request::Submit {
        instance_id: instance.clone(),
        session_id: session,
        sequence: 1,
        market: "BTC".into(),
        command: "buy 0.01".into(),
    };
    // Drop the transport after the complete request, before receiving its acknowledgement.
    let mut lost = UnixStream::connect(&socket).await.unwrap();
    send(&mut lost, request.clone()).await;
    drop(lost);
    let (a, b) = tokio::join!(rpc(&socket, request.clone()), rpc(&socket, request.clone()));
    let id = match (a, b) {
        (Response::Submitted { command_id: a, .. }, Response::Submitted { command_id: b, .. }) => {
            assert_eq!(a, b);
            a
        }
        other => panic!("{other:?}"),
    };
    let record = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Response::Command { record, .. } = rpc(
                &socket,
                Request::Get {
                    instance_id: instance.clone(),
                    command_id: id,
                },
            )
            .await
                && record.state == hl_wire::CommandState::Completed
            {
                break record;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    let output = record.response.unwrap();
    assert_eq!(output.receipts[0].id, 42);
    assert_eq!(
        output.receipts[0].items[0],
        hl_wire::OrderOutcome::Resting {
            oid: 99,
            cloid: Some("owned".into())
        }
    );
    assert!(matches!(
        output.receipts[0].items[1],
        hl_wire::OrderOutcome::Error { .. }
    ));
    let conflict = Request::Submit {
        instance_id: instance.clone(),
        session_id: session,
        sequence: 1,
        market: "BTC".into(),
        command: "sell 0.01".into(),
    };
    assert!(matches!(
        rpc(&socket, conflict).await,
        Response::Error { .. }
    ));
    let stale = Request::Submit {
        instance_id: "previous-daemon".into(),
        session_id: session,
        sequence: 2,
        market: "BTC".into(),
        command: "sell 0.01".into(),
    };
    assert!(matches!(rpc(&socket, stale).await, Response::Error { .. }));
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    // Fixed sessions cannot be retargeted by a command, including one in a chain.
    let switch = Request::Submit {
        instance_id: instance.clone(),
        session_id: session,
        sequence: 2,
        market: "BTC".into(),
        command: "instrument ETH; buy 0.01".into(),
    };
    let switched_id = match rpc(&socket, switch).await {
        Response::Submitted { command_id, .. } => command_id,
        other => panic!("{other:?}"),
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let r = queue.get(switched_id).await.unwrap();
            if let Some(response) = r.response {
                assert!(!response.ok);
                assert!(response.lines[0].contains("market is fixed"));
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    let snap = rpc(
        &socket,
        Request::Snapshot {
            instance_id: instance,
            session_id: session,
            after_fill_seq: 0,
        },
    )
    .await;
    assert!(
        matches!(snap, Response::Snapshot(ref s) if s.market.symbol == "BTC" && s.health.orders_ready)
    );
    server.abort();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn retired_sequences_never_enqueue_again_and_unknown_daemon_ids_never_alias() {
    let dir = std::env::temp_dir().join(format!("hl-sequence-{}-{}", std::process::id(), now_ms()));
    let state = Arc::new(RwLock::new(ready_perp("0")));
    let service = Arc::new(
        CommandService::new(
            state.clone(),
            Arc::new(Executor::default()),
            KeybindStore::load(dir.join("bindings")).unwrap(),
        )
        .with_scopes(ScopeRegistry::new("BTC".into()).unwrap()),
    );
    let queue = CommandQueue::new(service).unwrap();
    let session = queue
        .open_trading_session("BTC".into())
        .await
        .unwrap()
        .session_id;
    assert!(
        queue
            .submit_identified(session, 0, "BTC", "orders".into())
            .await
            .is_err()
    );
    assert!(
        queue
            .submit_identified(session, 2, "BTC", "orders".into())
            .await
            .is_err()
    );
    assert!(
        queue
            .submit_identified(session, 1, "ETH", "orders".into())
            .await
            .is_err()
    );
    for sequence in 1..=202 {
        let id = queue
            .submit_identified(session, sequence, "BTC", "orders".into())
            .await
            .unwrap()
            .command_id;
        tokio::time::timeout(Duration::from_secs(2), async {
            while queue.get(id).await.unwrap().state
                != hl_v2::runtime::CommandExecutionState::Completed
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let before = queue.results(0, 1000).await;
    let retired = queue
        .submit_identified(session, 1, "BTC", "orders".into())
        .await
        .unwrap_err();
    assert!(retired.contains("retired"));
    assert_eq!(
        queue.results(0, 1000).await.latest_command_id,
        before.latest_command_id
    );
    let replay = queue
        .submit_identified(session, 202, "BTC", "orders".into())
        .await
        .unwrap();
    assert_eq!(replay.command_id, before.latest_command_id);
    let other_service = Arc::new(CommandService::new(
        state,
        Arc::new(Executor::default()),
        KeybindStore::load(dir.join("other-bindings")).unwrap(),
    ));
    let other = CommandQueue::new(other_service).unwrap();
    assert_ne!(queue.instance_id(), other.instance_id());
    let _ = std::fs::remove_dir_all(dir);
}
