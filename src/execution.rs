use std::{
    fmt,
    fs::OpenOptions,
    future::Future,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalPhase {
    Pending,
    Accepted,
    Rejected,
    Ambiguous,
    ReconciledAccepted,
    ReconciledRejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JournalRecord {
    pub id: u64,
    pub ts_ms: u64,
    pub phase: JournalPhase,
    pub kind: String,
    pub market: String,
    pub action: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<JournalContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JournalContext {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_json: Option<String>,
}

pub struct Reconciliation<'a> {
    pub id: u64,
    pub kind: &'a str,
    pub market: &'a str,
    pub action: &'a str,
    pub applied: bool,
    pub detail: &'a str,
    pub context: JournalContext,
}

#[derive(Debug, Clone)]
pub struct ActionJournal {
    path: PathBuf,
    session_id: String,
    writer: Arc<Mutex<()>>,
}

impl ActionJournal {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        static SESSION_SEQ: AtomicU64 = AtomicU64::new(1);
        Self {
            path: path.into(),
            session_id: format!(
                "{:x}-{}-{}",
                now_ms(),
                std::process::id(),
                SESSION_SEQ.fetch_add(1, Ordering::Relaxed)
            ),
            writer: Arc::new(Mutex::new(())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn append(&self, record: &JournalRecord) -> io::Result<()> {
        let mut encoded = serde_json::to_vec(record).map_err(io::Error::other)?;
        encoded.push(b'\n');
        let _writer = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("journal writer lock poisoned"))?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&self.path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = file.metadata()?.permissions();
            if permissions.mode() & 0o077 != 0 {
                permissions.set_mode(0o600);
                file.set_permissions(permissions)?;
            }
        }
        file.write_all(&encoded)?;
        file.sync_data()
    }

    pub fn read_all(&self) -> io::Result<Vec<JournalRecord>> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).map_err(io::Error::other))
            .collect()
    }

    pub fn unresolved(&self) -> io::Result<Vec<JournalRecord>> {
        let mut latest = std::collections::BTreeMap::new();
        for record in self.read_all()? {
            let Some(context) = record.context.as_ref() else {
                continue;
            };
            latest.insert((context.session_id.clone(), record.id), record);
        }
        Ok(latest
            .into_values()
            .filter(|record| {
                matches!(
                    record.phase,
                    JournalPhase::Pending | JournalPhase::Ambiguous
                )
            })
            .collect())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionHealth {
    pub halted: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionControl {
    halt: Arc<tokio::sync::RwLock<Option<String>>>,
}

impl ExecutionControl {
    pub async fn health(&self) -> ExecutionHealth {
        let reason = self.halt.read().await.clone();
        ExecutionHealth {
            halted: reason.is_some(),
            reason,
        }
    }

    pub async fn ensure_running(&self) -> Result<(), String> {
        match self.halt.read().await.as_ref() {
            Some(reason) => Err(format!("execution halted: {reason}")),
            None => Ok(()),
        }
    }

    pub async fn halt(&self, reason: impl Into<String>) {
        let mut current = self.halt.write().await;
        if current.is_none() {
            *current = Some(reason.into());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    Sign(String),
    ClosedBeforeSend { id: u64 },
    StreamClosed { id: u64 },
    Timeout { id: u64, timeout_ms: u64 },
    Exchange(String),
    Decode(String),
    Unexpected(String),
}

impl TransportError {
    pub fn is_ambiguous(&self) -> bool {
        matches!(
            self,
            Self::StreamClosed { .. } | Self::Timeout { .. } | Self::Decode(_)
        )
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sign(msg) => write!(f, "sign action: {msg}"),
            Self::ClosedBeforeSend { id } => write!(f, "exchange post channel closed id={id}"),
            Self::StreamClosed { id } => write!(f, "exchange post stream terminated id={id}"),
            Self::Timeout { id, timeout_ms } => {
                write!(f, "exchange post timeout after {timeout_ms}ms id={id}")
            }
            Self::Exchange(msg) => write!(f, "exchange rejected action: {msg}"),
            Self::Decode(msg) => write!(f, "decode exchange response: {msg}"),
            Self::Unexpected(msg) => write!(f, "unexpected exchange response: {msg}"),
        }
    }
}

impl std::error::Error for TransportError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderStatus {
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

impl OrderStatus {
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Error { message } => Some(message),
            _ => None,
        }
    }
}

impl Serialize for OrderStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Success => serializer.serialize_str("success"),
            Self::AlreadyTerminal { message } => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("alreadyTerminal", message)?;
                map.end()
            }
            Self::WaitingForFill => serializer.serialize_str("waitingForFill"),
            Self::WaitingForTrigger => serializer.serialize_str("waitingForTrigger"),
            Self::Resting { oid, cloid } => {
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct Resting<'a> {
                    oid: u64,
                    #[serde(skip_serializing_if = "Option::is_none")]
                    cloid: &'a Option<String>,
                }
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("resting", &Resting { oid: *oid, cloid })?;
                map.end()
            }
            Self::Filled {
                oid,
                total_size,
                average_price,
            } => {
                #[derive(Serialize)]
                struct Filled<'a> {
                    oid: u64,
                    #[serde(rename = "totalSz")]
                    total_size: &'a str,
                    #[serde(rename = "avgPx")]
                    average_price: &'a str,
                }
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(
                    "filled",
                    &Filled {
                        oid: *oid,
                        total_size,
                        average_price,
                    },
                )?;
                map.end()
            }
            Self::TwapRunning { twap_id } => {
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct Running {
                    twap_id: u64,
                }
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("twapRunning", &Running { twap_id: *twap_id })?;
                map.end()
            }
            Self::Error { message } => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("error", message)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for OrderStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if let Some(text) = value.as_str() {
            return match text {
                "success" => Ok(Self::Success),
                "waitingForFill" => Ok(Self::WaitingForFill),
                "waitingForTrigger" => Ok(Self::WaitingForTrigger),
                other => Err(de::Error::custom(format!("unknown order status {other}"))),
            };
        }
        let object = value
            .as_object()
            .ok_or_else(|| de::Error::custom("order status must be string or object"))?;
        if let Some(resting) = object.get("resting") {
            return Ok(Self::Resting {
                oid: resting
                    .get("oid")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| de::Error::custom("resting status missing oid"))?,
                cloid: resting
                    .get("cloid")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned),
            });
        }
        if let Some(filled) = object.get("filled") {
            return Ok(Self::Filled {
                oid: filled
                    .get("oid")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| de::Error::custom("filled status missing oid"))?,
                total_size: get_string(filled, &["totalSz", "totalSize"])?,
                average_price: get_string(filled, &["avgPx", "averagePrice"])?,
            });
        }
        if let Some(running) = object.get("twapRunning") {
            return Ok(Self::TwapRunning {
                twap_id: running
                    .get("twapId")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| de::Error::custom("twap running status missing twapId"))?,
            });
        }
        if let Some(error) = object.get("error").and_then(serde_json::Value::as_str) {
            return Ok(Self::Error {
                message: error.to_string(),
            });
        }
        if let Some(message) = object
            .get("alreadyTerminal")
            .and_then(serde_json::Value::as_str)
        {
            return Ok(Self::AlreadyTerminal {
                message: message.to_string(),
            });
        }
        Err(de::Error::custom("unknown order status object"))
    }
}

fn get_string<E: de::Error>(value: &serde_json::Value, keys: &[&str]) -> Result<String, E> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
        .ok_or_else(|| E::custom(format!("missing one of {}", keys.join(", "))))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionStatus {
    Accepted,
    Rejected,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitReceipt {
    pub id: u64,
    pub status: ExecutionStatus,
    pub statuses: Vec<OrderStatus>,
    pub error: Option<String>,
}

pub struct ExecutionKernel {
    journal: ActionJournal,
    next_id: AtomicU64,
}

#[derive(Clone, Copy)]
struct SubmitMeta<'a> {
    kind: &'a str,
    market: &'a str,
    action: &'a str,
}

impl ExecutionKernel {
    pub fn new(journal: ActionJournal) -> Self {
        Self {
            journal,
            next_id: AtomicU64::new(1),
        }
    }

    pub fn journal(&self) -> &ActionJournal {
        &self.journal
    }

    pub fn reconcile(
        &self,
        reconciliation: Reconciliation<'_>,
        statuses: Vec<OrderStatus>,
    ) -> SubmitReceipt {
        let rejected =
            !reconciliation.applied || statuses.iter().any(|status| status.error().is_some());
        let phase = if rejected {
            JournalPhase::ReconciledRejected
        } else {
            JournalPhase::ReconciledAccepted
        };
        let append = self.append(
            reconciliation.id,
            phase,
            SubmitMeta {
                kind: reconciliation.kind,
                market: reconciliation.market,
                action: reconciliation.action,
            },
            reconciliation.detail,
            Some(&reconciliation.context),
        );
        SubmitReceipt {
            id: reconciliation.id,
            status: if rejected {
                ExecutionStatus::Rejected
            } else {
                ExecutionStatus::Accepted
            },
            statuses,
            error: append.err().map(|err| format!("journal_terminal: {err}")),
        }
    }

    pub async fn submit<F, Fut>(
        &self,
        kind: &str,
        market: &str,
        action: &str,
        post: F,
    ) -> SubmitReceipt
    where
        F: FnOnce(u64) -> Fut,
        Fut: Future<Output = Result<Vec<OrderStatus>, TransportError>>,
    {
        self.submit_with_context(kind, market, action, None, None, post)
            .await
    }

    pub async fn submit_with_context<F, Fut>(
        &self,
        kind: &str,
        market: &str,
        action: &str,
        nonce: Option<u64>,
        action_json: Option<String>,
        post: F,
    ) -> SubmitReceipt
    where
        F: FnOnce(u64) -> Fut,
        Fut: Future<Output = Result<Vec<OrderStatus>, TransportError>>,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let meta = SubmitMeta {
            kind,
            market,
            action,
        };
        let context = JournalContext {
            session_id: self.journal.session_id.clone(),
            nonce,
            action_json,
        };
        if let Err(err) = self.append(id, JournalPhase::Pending, meta, "pending", Some(&context)) {
            return SubmitReceipt {
                id,
                status: ExecutionStatus::Rejected,
                statuses: Vec::new(),
                error: Some(format!("journal_before_post: {err}")),
            };
        }

        match post(id).await {
            Ok(statuses) => {
                if let Some(message) = statuses
                    .iter()
                    .find_map(OrderStatus::error)
                    .map(ToOwned::to_owned)
                {
                    self.terminal(
                        id,
                        JournalPhase::Rejected,
                        meta,
                        &message,
                        statuses,
                        Some(&context),
                    )
                } else {
                    self.terminal(
                        id,
                        JournalPhase::Accepted,
                        meta,
                        "accepted",
                        statuses,
                        Some(&context),
                    )
                }
            }
            Err(err) => {
                let phase = if err.is_ambiguous() {
                    JournalPhase::Ambiguous
                } else {
                    JournalPhase::Rejected
                };
                let status = if err.is_ambiguous() {
                    ExecutionStatus::Ambiguous
                } else {
                    ExecutionStatus::Rejected
                };
                let message = err.to_string();
                let journal_error = self
                    .append(id, phase, meta, &message, Some(&context))
                    .err()
                    .map(|journal| format!("; journal_terminal: {journal}"))
                    .unwrap_or_default();
                SubmitReceipt {
                    id,
                    status,
                    statuses: Vec::new(),
                    error: Some(format!("{message}{journal_error}")),
                }
            }
        }
    }

    fn terminal(
        &self,
        id: u64,
        phase: JournalPhase,
        meta: SubmitMeta<'_>,
        detail: &str,
        statuses: Vec<OrderStatus>,
        context: Option<&JournalContext>,
    ) -> SubmitReceipt {
        let append = self.append(id, phase, meta, detail, context);
        let status = match phase {
            JournalPhase::Accepted | JournalPhase::ReconciledAccepted => ExecutionStatus::Accepted,
            JournalPhase::Rejected | JournalPhase::Pending | JournalPhase::ReconciledRejected => {
                ExecutionStatus::Rejected
            }
            JournalPhase::Ambiguous => ExecutionStatus::Ambiguous,
        };
        SubmitReceipt {
            id,
            status,
            statuses,
            error: append.err().map(|err| format!("journal_terminal: {err}")),
        }
    }

    fn append(
        &self,
        id: u64,
        phase: JournalPhase,
        meta: SubmitMeta<'_>,
        detail: &str,
        context: Option<&JournalContext>,
    ) -> io::Result<()> {
        self.journal.append(&JournalRecord {
            id,
            ts_ms: now_ms(),
            phase,
            kind: meta.kind.to_string(),
            market: meta.market.to_string(),
            action: meta.action.to_string(),
            detail: detail.to_string(),
            context: context.cloned(),
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
