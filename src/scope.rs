use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::{Mutex, watch};

use crate::operator::Variables;

const SESSION_IDLE_MS: u64 = 120_000;

#[derive(Debug)]
struct Session {
    symbol: String,
    variables: Variables,
    last_seen_ms: u64,
    jobs: u32,
    closed: bool,
}

#[derive(Debug)]
struct ScopeState {
    default_symbol: String,
    sessions: BTreeMap<u64, Session>,
    managed_symbols: BTreeSet<String>,
}

pub struct ScopeRegistry {
    state: Mutex<ScopeState>,
    next_session_id: AtomicU64,
    markets: watch::Sender<BTreeSet<String>>,
}

impl ScopeRegistry {
    pub fn new(default_symbol: String) -> anyhow::Result<Arc<Self>> {
        let mut seed = [0_u8; 8];
        getrandom::fill(&mut seed)?;
        let seed = u64::from_le_bytes(seed).max(1);
        let initial = BTreeSet::from([default_symbol.clone()]);
        let (markets, _) = watch::channel(initial);
        Ok(Arc::new(Self {
            state: Mutex::new(ScopeState {
                default_symbol,
                sessions: BTreeMap::new(),
                managed_symbols: BTreeSet::new(),
            }),
            next_session_id: AtomicU64::new(seed),
            markets,
        }))
    }

    pub fn subscribe(&self) -> watch::Receiver<BTreeSet<String>> {
        self.markets.subscribe()
    }

    pub async fn open(&self, symbol: String) -> u64 {
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().await;
        state.sessions.insert(
            id,
            Session {
                symbol,
                variables: Variables::default(),
                last_seen_ms: now_ms(),
                jobs: 0,
                closed: false,
            },
        );
        self.publish(&state);
        id
    }

    pub async fn close(&self, id: u64) -> Result<(), String> {
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(&id)
            .ok_or_else(|| unknown_session(id))?;
        if session.jobs == 0 {
            state.sessions.remove(&id);
        } else {
            session.closed = true;
        }
        self.publish(&state);
        Ok(())
    }

    pub async fn market(&self, id: u64) -> Result<String, String> {
        let mut state = self.state.lock().await;
        let session = session_mut(&mut state, id)?;
        session.last_seen_ms = now_ms();
        Ok(session.symbol.clone())
    }

    pub async fn live_market(&self, id: u64) -> Result<String, String> {
        let mut state = self.state.lock().await;
        let session = live_session_mut(&mut state, id)?;
        session.last_seen_ms = now_ms();
        Ok(session.symbol.clone())
    }

    pub async fn switch(&self, id: u64, symbol: String) -> Result<(), String> {
        let mut state = self.state.lock().await;
        let session = session_mut(&mut state, id)?;
        session.symbol = symbol;
        session.last_seen_ms = now_ms();
        self.publish(&state);
        Ok(())
    }

    pub async fn retain_job(&self, id: u64) -> Result<(), String> {
        let mut state = self.state.lock().await;
        let session = live_session_mut(&mut state, id)?;
        session.jobs = session.jobs.saturating_add(1);
        session.last_seen_ms = now_ms();
        Ok(())
    }

    pub async fn release_job(&self, id: u64) {
        let mut state = self.state.lock().await;
        let remove = state.sessions.get_mut(&id).is_some_and(|session| {
            session.jobs = session.jobs.saturating_sub(1);
            session.closed && session.jobs == 0
        });
        if remove {
            state.sessions.remove(&id);
            self.publish(&state);
        }
    }

    pub async fn expand(&self, id: u64, token: &str) -> Result<String, String> {
        let mut state = self.state.lock().await;
        let session = session_mut(&mut state, id)?;
        session.last_seen_ms = now_ms();
        session.variables.expand_token(token)
    }

    pub async fn set_variable(&self, id: u64, name: String, value: String) -> Result<(), String> {
        let mut state = self.state.lock().await;
        let session = session_mut(&mut state, id)?;
        session.last_seen_ms = now_ms();
        session.variables.set(name, value)
    }

    pub async fn unset_variable(&self, id: u64, name: &str) -> Result<bool, String> {
        let mut state = self.state.lock().await;
        let session = session_mut(&mut state, id)?;
        session.last_seen_ms = now_ms();
        Ok(session.variables.unset(name))
    }

    pub async fn variables(&self, id: u64) -> Result<Vec<(String, String)>, String> {
        let mut state = self.state.lock().await;
        let session = session_mut(&mut state, id)?;
        session.last_seen_ms = now_ms();
        Ok(session
            .variables
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect())
    }

    pub async fn set_default(&self, symbol: String) {
        let mut state = self.state.lock().await;
        state.default_symbol = symbol;
        self.publish(&state);
    }

    pub async fn set_managed(&self, symbols: BTreeSet<String>) {
        let mut state = self.state.lock().await;
        state.managed_symbols = symbols;
        self.publish(&state);
    }

    pub async fn keeps_market_alive(&self, symbol: &str) -> bool {
        let state = self.state.lock().await;
        state
            .sessions
            .values()
            .any(|session| session.symbol == symbol)
            || state.managed_symbols.contains(symbol)
    }

    pub async fn expire_idle(&self) {
        let cutoff = now_ms().saturating_sub(SESSION_IDLE_MS);
        let mut state = self.state.lock().await;
        let before = state.sessions.len();
        state.sessions.retain(|_, session| {
            session.jobs > 0 || (!session.closed && session.last_seen_ms >= cutoff)
        });
        if state.sessions.len() != before {
            self.publish(&state);
        }
    }

    fn publish(&self, state: &ScopeState) {
        let mut markets = BTreeSet::from([state.default_symbol.clone()]);
        markets.extend(
            state
                .sessions
                .values()
                .map(|session| session.symbol.clone()),
        );
        markets.extend(state.managed_symbols.iter().cloned());
        self.markets.send_replace(markets);
    }
}

fn live_session_mut(state: &mut ScopeState, id: u64) -> Result<&mut Session, String> {
    state
        .sessions
        .get_mut(&id)
        .filter(|session| !session.closed)
        .ok_or_else(|| unknown_session(id))
}

fn session_mut(state: &mut ScopeState, id: u64) -> Result<&mut Session, String> {
    state
        .sessions
        .get_mut(&id)
        .ok_or_else(|| unknown_session(id))
}

fn unknown_session(id: u64) -> String {
    format!("unknown or expired market session {id}")
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
