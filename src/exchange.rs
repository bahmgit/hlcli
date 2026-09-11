use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, mpsc, oneshot},
    time::Instant,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    execution::{OrderStatus, TransportError},
    protocol::{Action, ActionRequest, Chain},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Network {
    Mainnet,
    Testnet,
}

impl From<Chain> for Network {
    fn from(value: Chain) -> Self {
        if value.is_mainnet() {
            Self::Mainnet
        } else {
            Self::Testnet
        }
    }
}

impl Network {
    pub fn ws_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://api.hyperliquid.xyz/ws",
            Self::Testnet => "wss://api.hyperliquid-testnet.xyz/ws",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", content = "response", rename_all = "camelCase")]
pub enum ExchangeResponse {
    Ok(OkResponse),
    Err(String),
}

impl ExchangeResponse {
    pub fn order_statuses(self) -> Result<Vec<OrderStatus>, TransportError> {
        match self {
            Self::Err(err) => Err(TransportError::Exchange(err)),
            Self::Ok(OkResponse::Order { statuses })
            | Self::Ok(OkResponse::Cancel { statuses }) => Ok(statuses),
            Self::Ok(OkResponse::TwapOrder { status }) => status.into_statuses(),
            Self::Ok(OkResponse::TwapCancel { status }) => status.into_statuses(),
            Self::Ok(OkResponse::UpdateIsolatedMargin { status }) => status.into_statuses(),
            Self::Ok(OkResponse::Default) => Ok(vec![OrderStatus::Success]),
            Self::Ok(other) => Err(TransportError::Unexpected(format!("{other:?}"))),
        }
    }

    pub fn statuses_for(self, action: &Action) -> Result<Vec<OrderStatus>, TransportError> {
        match (action, self) {
            (_, Self::Err(err)) => Err(TransportError::Exchange(err)),
            (
                Action::Order(_) | Action::BatchModify(_),
                Self::Ok(OkResponse::Order { statuses }),
            )
            | (
                Action::Cancel(_) | Action::CancelByCloid(_),
                Self::Ok(OkResponse::Cancel { statuses }),
            ) => Ok(statuses),
            (Action::TwapOrder(_), Self::Ok(OkResponse::TwapOrder { status })) => {
                status.into_statuses()
            }
            (Action::TwapCancel(_), Self::Ok(OkResponse::TwapCancel { status })) => {
                status.into_statuses()
            }
            (
                Action::UpdateIsolatedMargin(_),
                Self::Ok(OkResponse::UpdateIsolatedMargin { status }),
            ) => status.into_statuses(),
            (
                Action::UpdateLeverage(_)
                | Action::UpdateIsolatedMargin(_)
                | Action::AgentSetAbstraction(_)
                | Action::ReserveRequestWeight { .. }
                | Action::Noop,
                Self::Ok(OkResponse::Default),
            ) => Ok(vec![OrderStatus::Success]),
            (_, response) => Err(TransportError::Unexpected(format!(
                "response {response:?} does not match action {action:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "data", rename_all = "camelCase")]
pub enum OkResponse {
    Order { statuses: Vec<OrderStatus> },
    Cancel { statuses: Vec<OrderStatus> },
    TwapOrder { status: TwapOrderStatus },
    TwapCancel { status: TwapCancelStatus },
    UpdateIsolatedMargin { status: ActionStatus },
    SpotUser(serde_json::Value),
    Default,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TwapRunning {
    pub twap_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TwapOrderStatus {
    Running { running: TwapRunning },
    Error { error: String },
    Unknown(serde_json::Value),
}

impl TwapOrderStatus {
    fn into_statuses(self) -> Result<Vec<OrderStatus>, TransportError> {
        match self {
            Self::Running { running } => Ok(vec![OrderStatus::TwapRunning {
                twap_id: running.twap_id,
            }]),
            Self::Error { error } => Ok(vec![OrderStatus::Error { message: error }]),
            Self::Unknown(value) => Err(TransportError::Unexpected(format!(
                "twapOrder status {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TwapCancelSuccess {
    Success,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TwapCancelStatus {
    Success(TwapCancelSuccess),
    Error { error: String },
    Unknown(serde_json::Value),
}

impl TwapCancelStatus {
    fn into_statuses(self) -> Result<Vec<OrderStatus>, TransportError> {
        match self {
            Self::Success(TwapCancelSuccess::Success) => Ok(vec![OrderStatus::Success]),
            Self::Error { error } if is_terminal_twap_cancel_error(&error) => {
                Ok(vec![OrderStatus::Success])
            }
            Self::Error { error } => Ok(vec![OrderStatus::Error { message: error }]),
            Self::Unknown(value) => Err(TransportError::Unexpected(format!(
                "twapCancel status {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActionSuccess {
    Success,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ActionStatus {
    Success(ActionSuccess),
    Error { error: String },
    Unknown(serde_json::Value),
}

impl ActionStatus {
    fn into_statuses(self) -> Result<Vec<OrderStatus>, TransportError> {
        match self {
            Self::Success(ActionSuccess::Success) => Ok(vec![OrderStatus::Success]),
            Self::Error { error } => Ok(vec![OrderStatus::Error { message: error }]),
            Self::Unknown(value) => {
                Err(TransportError::Unexpected(format!("action status {value}")))
            }
        }
    }
}

fn is_terminal_twap_cancel_error(error: &str) -> bool {
    error == "TWAP was never placed, already canceled, or filled."
}

#[derive(Debug, Serialize)]
#[serde(tag = "method", rename_all = "camelCase")]
enum Outgoing {
    Post { id: u64, request: PostRequest },
    Ping,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum PostRequest {
    Action { payload: ActionRequest },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "channel", content = "data", rename_all = "camelCase")]
enum Incoming {
    Post(PostChannel),
    Error(serde_json::Value),
    Ping,
    Pong,
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PostChannel {
    id: u64,
    response: PostResponse,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum PostResponse {
    Action { payload: ExchangeResponse },
    Error { payload: String },
}

type PendingMap =
    Arc<Mutex<BTreeMap<u64, oneshot::Sender<Result<ExchangeResponse, TransportError>>>>>;

const MAX_POSTS_PER_SOCKET: u64 = 4;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(45);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
enum WorkerCommand {
    Post { id: u64, request: ActionRequest },
    Reset,
}

#[derive(Debug, Clone, Copy)]
struct SocketPolicy {
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    max_posts: u64,
}

impl Default for SocketPolicy {
    fn default() -> Self {
        Self {
            heartbeat_interval: HEARTBEAT_INTERVAL,
            heartbeat_timeout: HEARTBEAT_TIMEOUT,
            max_posts: MAX_POSTS_PER_SOCKET,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SocketExit {
    Shutdown,
    Rotate,
    Closed,
}

pub struct WsActionClient {
    next_id: AtomicU64,
    tx: mpsc::UnboundedSender<WorkerCommand>,
    pending: PendingMap,
    connection: ActionConnectionHealth,
}

#[derive(Debug, Clone)]
pub struct ActionConnectionHealth {
    connected: Arc<AtomicBool>,
}

impl ActionConnectionHealth {
    pub fn connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }
}

impl WsActionClient {
    pub async fn connect(network: Network) -> anyhow::Result<Self> {
        let (mut ws, _) = connect_async(network.ws_url()).await?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pending: PendingMap = Arc::new(Mutex::new(BTreeMap::new()));
        let worker_pending = pending.clone();
        let connection = ActionConnectionHealth {
            connected: Arc::new(AtomicBool::new(true)),
        };
        let worker_connection = connection.clone();

        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(250);
            loop {
                let exit =
                    run_socket(&mut ws, &mut rx, &worker_pending, SocketPolicy::default()).await;
                match exit {
                    SocketExit::Shutdown => {
                        worker_connection.connected.store(false, Ordering::Release);
                        break;
                    }
                    SocketExit::Rotate => {
                        match tokio::time::timeout(
                            HEARTBEAT_TIMEOUT,
                            connect_async(network.ws_url()),
                        )
                        .await
                        {
                            Ok(Ok((next, _))) => {
                                let mut previous = std::mem::replace(&mut ws, next);
                                let _ =
                                    tokio::time::timeout(HEARTBEAT_TIMEOUT, previous.close(None))
                                        .await;
                            }
                            Ok(Err(err)) => {
                                eprintln!(
                                    "action websocket rotation failed; retaining current socket: {err}"
                                );
                            }
                            Err(_) => {
                                eprintln!(
                                    "action websocket rotation timed out; retaining current socket"
                                );
                            }
                        }
                        backoff = Duration::from_millis(250);
                        continue;
                    }
                    SocketExit::Closed => {
                        worker_connection.connected.store(false, Ordering::Release);
                        eprintln!("action websocket disconnected; reconnecting");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(10));
                    }
                }
                loop {
                    match connect_async(network.ws_url()).await {
                        Ok((next, _)) => {
                            ws = next;
                            worker_connection.connected.store(true, Ordering::Release);
                            backoff = Duration::from_millis(250);
                            break;
                        }
                        Err(err) => {
                            eprintln!("action websocket reconnect failed: {err}");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(10));
                        }
                    }
                }
            }
        });

        Ok(Self {
            next_id: AtomicU64::new(1),
            tx,
            pending,
            connection,
        })
    }

    pub fn connection_health(&self) -> ActionConnectionHealth {
        self.connection.clone()
    }

    pub async fn post_action(
        &self,
        request: ActionRequest,
        timeout: Duration,
    ) -> Result<ExchangeResponse, TransportError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        if self.tx.send(WorkerCommand::Post { id, request }).is_err() {
            self.pending.lock().await.remove(&id);
            return Err(TransportError::ClosedBeforeSend { id });
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => Err(TransportError::StreamClosed { id }),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                let _ = self.tx.send(WorkerCommand::Reset);
                Err(TransportError::Timeout {
                    id,
                    timeout_ms: timeout.as_millis() as u64,
                })
            }
        }
    }
}

async fn run_socket<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    rx: &mut mpsc::UnboundedReceiver<WorkerCommand>,
    pending: &PendingMap,
    policy: SocketPolicy,
) -> SocketExit
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut posts = 0_u64;
    let mut in_flight = BTreeSet::new();
    let mut heartbeat = tokio::time::interval_at(
        Instant::now() + policy.heartbeat_interval,
        policy.heartbeat_interval,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pong_deadline = None;
    loop {
        tokio::select! {
            biased;
            _ = wait_for_deadline(pong_deadline), if pong_deadline.is_some() => {
                fail_in_flight(pending, &mut in_flight).await;
                return SocketExit::Closed;
            }
            _ = heartbeat.tick(), if pong_deadline.is_none() => {
                let text = serde_json::to_string(&Outgoing::Ping)
                    .expect("heartbeat serialization is infallible");
                if ws.send(Message::Text(text.into())).await.is_err() {
                    fail_in_flight(pending, &mut in_flight).await;
                    return SocketExit::Closed;
                }
                pong_deadline = Some(Instant::now() + policy.heartbeat_timeout);
            }
            outgoing = rx.recv() => {
                let Some(outgoing) = outgoing else {
                    fail_in_flight(pending, &mut in_flight).await;
                    return SocketExit::Shutdown;
                };
                let WorkerCommand::Post { id, request: payload } = outgoing else {
                    fail_in_flight(pending, &mut in_flight).await;
                    let _ = ws.close(None).await;
                    return SocketExit::Closed;
                };
                if !is_pending(pending, id).await {
                    continue;
                }
                let text = match serde_json::to_string(&Outgoing::Post {
                    id,
                    request: PostRequest::Action { payload },
                }) {
                    Ok(text) => text,
                    Err(err) => {
                        finish(pending, id, Err(TransportError::Unexpected(err.to_string()))).await;
                        continue;
                    }
                };
                if ws.send(Message::Text(text.into())).await.is_err() {
                    finish(pending, id, Err(TransportError::StreamClosed { id })).await;
                    return SocketExit::Closed;
                }
                in_flight.insert(id);
                posts += 1;
            }
            incoming = ws.next() => {
                let Some(Ok(message)) = incoming else {
                    fail_in_flight(pending, &mut in_flight).await;
                    return SocketExit::Closed;
                };
                match message {
                    Message::Text(text) => {
                        if is_application_pong(&text) {
                            pong_deadline = None;
                        }
                        let id = post_id(&text);
                        handle_text(pending, &text).await;
                        if let Some(id) = id {
                            in_flight.remove(&id);
                        }
                    }
                    Message::Ping(bytes) => {
                        if ws.send(Message::Pong(bytes)).await.is_err() {
                            fail_in_flight(pending, &mut in_flight).await;
                            return SocketExit::Closed;
                        }
                    }
                    Message::Close(_) => {
                        fail_in_flight(pending, &mut in_flight).await;
                        return SocketExit::Closed;
                    }
                    _ => {}
                }
                if posts >= policy.max_posts && in_flight.is_empty() {
                    return SocketExit::Rotate;
                }
            }
        }
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn is_application_pong(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("channel")
                .and_then(serde_json::Value::as_str)
                .map(|channel| channel == "pong")
        })
        .unwrap_or(false)
}

async fn handle_text(pending: &PendingMap, text: &str) {
    match serde_json::from_str::<Incoming>(text) {
        Ok(Incoming::Post(post)) => {
            let result = match post.response {
                PostResponse::Action { payload } => Ok(payload),
                PostResponse::Error { payload } => Err(TransportError::Exchange(payload)),
            };
            finish(pending, post.id, result).await;
        }
        Ok(Incoming::Ping) | Ok(Incoming::Pong) | Ok(Incoming::Other) => {}
        Ok(Incoming::Error(error)) => {
            let _ = error;
        }
        Err(err) => {
            if let Some(id) = post_id(text) {
                finish(
                    pending,
                    id,
                    Err(TransportError::Decode(format!(
                        "{}: {}",
                        err,
                        truncate(text, 1_000)
                    ))),
                )
                .await;
            }
        }
    }
}

async fn finish(pending: &PendingMap, id: u64, result: Result<ExchangeResponse, TransportError>) {
    if let Some(tx) = pending.lock().await.remove(&id) {
        let _ = tx.send(result);
    }
}

async fn is_pending(pending: &PendingMap, id: u64) -> bool {
    pending.lock().await.contains_key(&id)
}

fn post_id(text: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()?
        .get("data")?
        .get("id")?
        .as_u64()
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        text.to_string()
    } else {
        format!("{}...", &text[..max])
    }
}

async fn fail_in_flight(pending: &PendingMap, in_flight: &mut BTreeSet<u64>) {
    let mut pending = pending.lock().await;
    for id in std::mem::take(in_flight) {
        let Some(tx) = pending.remove(&id) else {
            continue;
        };
        let _ = tx.send(Err(TransportError::StreamClosed { id }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};
    use tokio::io::duplex;
    use tokio_tungstenite::tungstenite::protocol::Role;

    async fn socket_pair() -> (
        tokio_tungstenite::WebSocketStream<tokio::io::DuplexStream>,
        tokio_tungstenite::WebSocketStream<tokio::io::DuplexStream>,
    ) {
        let (client, server) = duplex(16 * 1024);
        tokio::join!(
            tokio_tungstenite::WebSocketStream::from_raw_socket(client, Role::Client, None),
            tokio_tungstenite::WebSocketStream::from_raw_socket(server, Role::Server, None),
        )
    }

    #[tokio::test]
    async fn heartbeat_accepts_pong_without_blocking_action_posts() {
        let (mut client, mut server) = socket_pair().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pending: PendingMap = Arc::new(Mutex::new(BTreeMap::new()));
        let policy = SocketPolicy {
            heartbeat_interval: Duration::from_millis(10),
            heartbeat_timeout: Duration::from_millis(100),
            max_posts: 4,
        };
        let task_pending = pending.clone();
        let task =
            tokio::spawn(
                async move { run_socket(&mut client, &mut rx, &task_pending, policy).await },
            );

        let heartbeat = tokio::time::timeout(Duration::from_millis(100), server.next())
            .await
            .expect("heartbeat timeout")
            .expect("heartbeat stream closed")
            .expect("heartbeat read failed");
        assert_eq!(heartbeat.into_text().unwrap(), r#"{"method":"ping"}"#);

        let (response, _rx) = oneshot::channel();
        pending.lock().await.insert(7, response);
        tx.send(WorkerCommand::Post {
            id: 7,
            request: ActionRequest {
                action: Action::Noop,
                nonce: 1,
                signature: crate::protocol::Signature {
                    r: U256::ZERO,
                    s: U256::ZERO,
                    v: 27,
                },
                vault_address: Some(Address::ZERO),
                expires_after: None,
            },
        })
        .unwrap();
        let post = tokio::time::timeout(Duration::from_millis(30), server.next())
            .await
            .expect("action post was delayed behind heartbeat")
            .expect("socket closed before action post")
            .expect("action post read failed")
            .into_text()
            .unwrap();
        assert!(post.contains(r#""method":"post""#));

        server
            .send(Message::Text(r#"{"channel":"pong"}"#.into()))
            .await
            .unwrap();
        drop(tx);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), task)
                .await
                .unwrap()
                .unwrap(),
            SocketExit::Shutdown
        );
    }

    #[tokio::test]
    async fn missing_application_pong_retires_socket() {
        let (mut client, mut server) = socket_pair().await;
        let (_tx, mut rx) = mpsc::unbounded_channel();
        let pending: PendingMap = Arc::new(Mutex::new(BTreeMap::new()));
        let policy = SocketPolicy {
            heartbeat_interval: Duration::from_millis(10),
            heartbeat_timeout: Duration::from_millis(20),
            max_posts: 4,
        };
        let task =
            tokio::spawn(async move { run_socket(&mut client, &mut rx, &pending, policy).await });

        let heartbeat = tokio::time::timeout(Duration::from_millis(100), server.next())
            .await
            .expect("heartbeat timeout")
            .expect("heartbeat stream closed")
            .expect("heartbeat read failed");
        assert_eq!(heartbeat.into_text().unwrap(), r#"{"method":"ping"}"#);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), task)
                .await
                .unwrap()
                .unwrap(),
            SocketExit::Closed
        );
    }

    #[tokio::test]
    async fn planned_rotation_keeps_the_acknowledged_socket_open_for_hot_handoff() {
        let (mut client, mut server) = socket_pair().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pending: PendingMap = Arc::new(Mutex::new(BTreeMap::new()));
        let policy = SocketPolicy {
            heartbeat_interval: Duration::from_secs(60),
            heartbeat_timeout: Duration::from_secs(1),
            max_posts: 1,
        };
        let (response, _response_rx) = oneshot::channel();
        pending.lock().await.insert(7, response);
        tx.send(WorkerCommand::Post {
            id: 7,
            request: ActionRequest {
                action: Action::Noop,
                nonce: 1,
                signature: crate::protocol::Signature {
                    r: U256::ZERO,
                    s: U256::ZERO,
                    v: 27,
                },
                vault_address: Some(Address::ZERO),
                expires_after: None,
            },
        })
        .unwrap();

        let task_pending = pending.clone();
        let task = tokio::spawn(async move {
            let exit = run_socket(&mut client, &mut rx, &task_pending, policy).await;
            (exit, client)
        });
        let post = tokio::time::timeout(Duration::from_millis(100), server.next())
            .await
            .expect("action post timeout")
            .expect("socket closed before action post")
            .expect("action post read failed");
        assert!(post.into_text().unwrap().contains(r#""id":7"#));
        server
            .send(Message::Text(
                r#"{"channel":"post","data":{"id":7,"response":{"type":"action","payload":{"status":"ok","response":{"type":"default"}}}}}"#
                    .into(),
            ))
            .await
            .unwrap();

        let (exit, mut client) = tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .expect("rotation timeout")
            .unwrap();
        assert_eq!(exit, SocketExit::Rotate);
        client
            .send(Message::Ping(Vec::new().into()))
            .await
            .expect("planned rotation closed the old socket before replacement");
        let ping = tokio::time::timeout(Duration::from_millis(100), server.next())
            .await
            .expect("open socket did not carry post-rotation ping")
            .expect("socket closed after rotation")
            .expect("post-rotation ping read failed");
        assert!(matches!(ping, Message::Ping(_)));
    }
}
