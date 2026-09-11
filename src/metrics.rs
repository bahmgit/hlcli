use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountersSnapshot {
    pub action_signs: u64,
    pub action_ws_posts: u64,
    pub action_ws_acks: u64,
    pub action_ws_timeouts: u64,
    pub action_ws_closed: u64,
    pub action_http_posts: u64,
    pub state_ws_connects: u64,
    pub state_ws_disconnects: u64,
    pub rest_calls: u64,
    pub sleeps: u64,
    pub parser_calls: u64,
    pub refresh_state_calls: u64,
    pub cache_refreshes: u64,
}

impl CountersSnapshot {
    pub fn delta(self, previous: Self) -> Self {
        Self {
            action_signs: self.action_signs.saturating_sub(previous.action_signs),
            action_ws_posts: self
                .action_ws_posts
                .saturating_sub(previous.action_ws_posts),
            action_ws_acks: self.action_ws_acks.saturating_sub(previous.action_ws_acks),
            action_ws_timeouts: self
                .action_ws_timeouts
                .saturating_sub(previous.action_ws_timeouts),
            action_ws_closed: self
                .action_ws_closed
                .saturating_sub(previous.action_ws_closed),
            action_http_posts: self
                .action_http_posts
                .saturating_sub(previous.action_http_posts),
            state_ws_connects: self
                .state_ws_connects
                .saturating_sub(previous.state_ws_connects),
            state_ws_disconnects: self
                .state_ws_disconnects
                .saturating_sub(previous.state_ws_disconnects),
            rest_calls: self.rest_calls.saturating_sub(previous.rest_calls),
            sleeps: self.sleeps.saturating_sub(previous.sleeps),
            parser_calls: self.parser_calls.saturating_sub(previous.parser_calls),
            refresh_state_calls: self
                .refresh_state_calls
                .saturating_sub(previous.refresh_state_calls),
            cache_refreshes: self
                .cache_refreshes
                .saturating_sub(previous.cache_refreshes),
        }
    }

    pub fn forbidden_submit_path_activity(self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.action_http_posts != 0 {
            out.push("action_http_posts");
        }
        if self.sleeps != 0 {
            out.push("sleeps");
        }
        if self.parser_calls != 0 {
            out.push("parser_calls");
        }
        if self.refresh_state_calls != 0 {
            out.push("refresh_state_calls");
        }
        if self.cache_refreshes != 0 {
            out.push("cache_refreshes");
        }
        out
    }
}

#[derive(Debug, Default)]
struct Counters {
    action_signs: AtomicU64,
    action_ws_posts: AtomicU64,
    action_ws_acks: AtomicU64,
    action_ws_timeouts: AtomicU64,
    action_ws_closed: AtomicU64,
    action_http_posts: AtomicU64,
    state_ws_connects: AtomicU64,
    state_ws_disconnects: AtomicU64,
    state_feed_status: AtomicU8,
    rest_calls: AtomicU64,
    sleeps: AtomicU64,
    parser_calls: AtomicU64,
    refresh_state_calls: AtomicU64,
    cache_refreshes: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct Metrics(Arc<Counters>);

impl Metrics {
    pub fn snapshot(&self) -> CountersSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        CountersSnapshot {
            action_signs: load(&self.0.action_signs),
            action_ws_posts: load(&self.0.action_ws_posts),
            action_ws_acks: load(&self.0.action_ws_acks),
            action_ws_timeouts: load(&self.0.action_ws_timeouts),
            action_ws_closed: load(&self.0.action_ws_closed),
            action_http_posts: load(&self.0.action_http_posts),
            state_ws_connects: load(&self.0.state_ws_connects),
            state_ws_disconnects: load(&self.0.state_ws_disconnects),
            rest_calls: load(&self.0.rest_calls),
            sleeps: load(&self.0.sleeps),
            parser_calls: load(&self.0.parser_calls),
            refresh_state_calls: load(&self.0.refresh_state_calls),
            cache_refreshes: load(&self.0.cache_refreshes),
        }
    }

    pub fn sign(&self) {
        self.0.action_signs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ws_post(&self) {
        self.0.action_ws_posts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ws_ack(&self) {
        self.0.action_ws_acks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ws_timeout(&self) {
        self.0.action_ws_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ws_closed(&self) {
        self.0.action_ws_closed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn http_post(&self) {
        self.0.action_http_posts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn start_state_feed(&self) {
        self.0.state_feed_status.store(1, Ordering::Release);
    }

    pub fn state_ws_connected(&self) {
        self.0.state_ws_connects.fetch_add(1, Ordering::Relaxed);
        self.0.state_feed_status.store(2, Ordering::Release);
    }

    pub fn state_ws_disconnected(&self) {
        self.0.state_ws_disconnects.fetch_add(1, Ordering::Relaxed);
        self.state_feed_down();
    }

    pub fn state_feed_down(&self) {
        self.0.state_feed_status.store(1, Ordering::Release);
    }

    pub fn state_feed_connected(&self) -> Option<bool> {
        match self.0.state_feed_status.load(Ordering::Acquire) {
            0 => None,
            1 => Some(false),
            2 => Some(true),
            _ => unreachable!(),
        }
    }

    pub fn rest_call(&self) {
        self.0.rest_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sleep(&self) {
        self.0.sleeps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn parser_call(&self) {
        self.0.parser_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn refresh_state(&self) {
        self.0.refresh_state_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_refresh(&self) {
        self.0.cache_refreshes.fetch_add(1, Ordering::Relaxed);
    }
}
