//! Read-only startup snapshots and process-local REST budgeting. Never used to submit actions.
use crate::{exchange::Network, metrics::Metrics};
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::OnceLock,
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::time::{self, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WINDOW: Duration = Duration::from_secs(60);
// Reserve headroom below the exchange's 1200/IP limit. Other processes/IP users remain external.
const READ_BUDGET: u32 = 1000;
#[derive(Default)]
struct Budget {
    spent: VecDeque<(Instant, u32)>,
    cooldown: Option<Instant>,
}
impl Budget {
    fn reserve(&mut self, now: Instant, weight: u32) -> Option<Duration> {
        while self
            .spent
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= WINDOW)
        {
            self.spent.pop_front();
        }
        if let Some(until) = self.cooldown.filter(|until| *until > now) {
            return Some(until - now);
        }
        if self.spent.iter().map(|(_, weight)| weight).sum::<u32>() + weight > READ_BUDGET {
            return Some(
                WINDOW - now.duration_since(self.spent.front().expect("budget occupied").0),
            );
        }
        self.spent.push_back((now, weight));
        None
    }
}
fn budget(network: Network) -> &'static Mutex<Budget> {
    static BUDGETS: OnceLock<[Mutex<Budget>; 2]> = OnceLock::new();
    &BUDGETS.get_or_init(|| [Mutex::new(Budget::default()), Mutex::new(Budget::default())])
        [match network {
            Network::Mainnet => 0,
            Network::Testnet => 1,
        }]
}
fn weight(kind: &str) -> u32 {
    match kind {
        "l2Book"
        | "allMids"
        | "clearinghouseState"
        | "orderStatus"
        | "spotClearinghouseState"
        | "exchangeStatus" => 2,
        "userRole" => 60,
        _ => 20,
    }
}
pub(crate) async fn post(
    http: &reqwest::Client,
    network: Network,
    url: &str,
    body: Value,
    metrics: &Metrics,
) -> anyhow::Result<Value> {
    post_budgeted(http, url, body, metrics, budget(network)).await
}

async fn post_budgeted(
    http: &reqwest::Client,
    url: &str,
    body: Value,
    metrics: &Metrics,
    shared: &Mutex<Budget>,
) -> anyhow::Result<Value> {
    let kind = body
        .get("type")
        .and_then(Value::as_str)
        .context("info request missing type")?;
    let label = format!(
        "info {kind}{}",
        body.get("dex")
            .and_then(Value::as_str)
            .map(|dex| format!(" dex={dex}"))
            .unwrap_or_default()
    );
    for attempt in 0..3 {
        loop {
            let wait = shared.lock().await.reserve(Instant::now(), weight(kind));
            match wait {
                Some(wait) => time::sleep(wait).await,
                None => break,
            }
        }
        metrics.rest_call();
        let response = http
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("{label}: {error}"))?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60)
                .max(60);
            let until = Instant::now()
                .checked_add(Duration::from_secs(retry_after))
                .context("info Retry-After exceeds clock range")?;
            let mut state = shared.lock().await;
            state.cooldown = Some(state.cooldown.map_or(until, |old| old.max(until)));
            drop(state);
            if attempt < 2 && retry_after <= 120 {
                eprintln!(
                    "{label}: rate limited; retry {}/2 after shared cooldown (at least {retry_after}s)",
                    attempt + 1
                );
                continue;
            }
        }
        return response
            .error_for_status()
            .map_err(|error| anyhow::anyhow!("{label}: {error}"))?
            .json()
            .await
            .map_err(|error| anyhow::anyhow!("{label}: invalid JSON: {error}"));
    }
    unreachable!("last attempt returns its response")
}

struct OrderSnapshots {
    user: String,
    pending: BTreeSet<String>,
    values: BTreeMap<String, Value>,
}
impl OrderSnapshots {
    fn accept(&mut self, message: Value) -> anyhow::Result<()> {
        match message.get("channel").and_then(Value::as_str) {
            Some("subscriptionResponse") | Some("pong") => return Ok(()),
            Some("error") => {
                anyhow::bail!("openOrders initialization: exchange subscription error")
            }
            Some("openOrders") => {}
            _ => anyhow::bail!("openOrders initialization: unexpected channel"),
        }
        let data = message
            .get("data")
            .context("openOrders snapshot missing data")?;
        anyhow::ensure!(
            data.get("user")
                .and_then(Value::as_str)
                .is_some_and(|user| user.eq_ignore_ascii_case(&self.user)),
            "openOrders snapshot user mismatch"
        );
        let dex = data
            .get("dex")
            .and_then(Value::as_str)
            .context("openOrders snapshot missing dex")?;
        anyhow::ensure!(
            self.pending.contains(dex) || self.values.contains_key(dex),
            "openOrders snapshot unexpected dex"
        );
        anyhow::ensure!(
            data.get("orders").is_some_and(Value::is_array),
            "openOrders snapshot missing orders"
        );
        self.pending.remove(dex);
        self.values.insert(dex.to_owned(), data.clone());
        Ok(())
    }
}

pub(crate) async fn order_snapshots(
    network: Network,
    user: &str,
    dexes: BTreeSet<String>,
) -> anyhow::Result<BTreeMap<String, Value>> {
    order_snapshots_at(network.ws_url(), user, dexes).await
}

async fn order_snapshots_at(
    url: &str,
    user: &str,
    dexes: BTreeSet<String>,
) -> anyhow::Result<BTreeMap<String, Value>> {
    if dexes.is_empty() {
        return Ok(BTreeMap::new());
    }
    anyhow::ensure!(
        dexes.len() <= 1000,
        "openOrders initialization exceeds WebSocket subscription limit"
    );
    time::timeout(Duration::from_secs(30), async {
        let (mut socket, _) = connect_async(url).await?;
        for dex in &dexes {
            socket.send(Message::Text(json!({"method":"subscribe","subscription":{"type":"openOrders","user":user,"dex":dex}}).to_string().into())).await?;
        }
        let mut snapshots = OrderSnapshots { user: user.to_owned(), pending: dexes, values: BTreeMap::new() };
        while !snapshots.pending.is_empty() {
            match socket.next().await.context("openOrders snapshot connection ended")?? {
                Message::Text(text) => snapshots.accept(serde_json::from_str(text.as_ref())?)?,
                Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
                Message::Pong(_) => {},
                Message::Close(_) => anyhow::bail!("openOrders snapshot connection closed before all DEXes responded"),
                _ => anyhow::bail!("openOrders snapshot unexpected frame"),
            }
        }
        socket.close(None).await?;
        Ok::<_, anyhow::Error>(snapshots.values)
    }).await.map_err(|_| anyhow::anyhow!("openOrders initialization timed out; complete DEX snapshots required"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn read_budget_is_weighted_rolling_and_honors_shared_cooldown() {
        let now = Instant::now();
        let mut b = Budget::default();
        for _ in 0..50 {
            assert_eq!(b.reserve(now, weight("allPerpMetas")), None);
        }
        assert_eq!(b.reserve(now, 2), Some(WINDOW));
        assert_eq!(b.reserve(now + WINDOW, 2), None);
        b.cooldown = Some(now + WINDOW + Duration::from_secs(70));
        assert_eq!(b.reserve(now + WINDOW, 20), Some(Duration::from_secs(70)));
        assert_eq!(b.reserve(now + WINDOW + Duration::from_secs(70), 20), None);
    }
    #[test]
    fn snapshots_require_every_dex_and_validate_identity_including_empty_results() {
        let mut s = OrderSnapshots {
            user: "0xabc".into(),
            pending: BTreeSet::from(["".into(), "xyz".into()]),
            values: BTreeMap::new(),
        };
        let message = |user, dex, orders| json!({"channel":"openOrders","data":{"user":user,"dex":dex,"orders":orders}});
        assert!(s.accept(message("0xwrong", "", json!([]))).is_err());
        assert!(s.accept(message("0xabc", "other", json!([]))).is_err());
        assert!(s.accept(message("0xabc", "", Value::Null)).is_err());
        assert_eq!(s.pending.len(), 2);
        s.accept(message("0xABC", "", json!([]))).unwrap();
        assert_eq!(s.pending, BTreeSet::from(["xyz".into()]));
        let orders = json!([{"oid":9007199254740993u64,"coin":"xyz:BTC","reduceOnly":true,"orderType":"Trailing Stop Market","triggerCondition":"retracement 1%, best 100"}]);
        s.accept(message("0xabc", "xyz", orders.clone())).unwrap();
        assert!(s.pending.is_empty());
        assert_eq!(s.values["xyz"]["orders"], orders);
    }
    #[tokio::test]
    async fn snapshot_transport_requires_complete_valid_responses_before_returning() {
        for scenario in ["complete", "missing", "wrong-user"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                for dex in ["", "xyz"] {
                    let message = ws.next().await.unwrap().unwrap();
                    let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                    assert_eq!(
                        value,
                        json!({"method":"subscribe","subscription":{"type":"openOrders","user":"0xabc","dex":dex}})
                    );
                }
                let who = if scenario == "wrong-user" {
                    "0xother"
                } else {
                    "0xabc"
                };
                let payload = |dex| {
                    tokio_tungstenite::tungstenite::Message::Text(
                        json!({"channel":"openOrders","data":{"user":who,"dex":dex,"orders":[]}})
                            .to_string()
                            .into(),
                    )
                };
                ws.send(payload("xyz")).await.unwrap();
                if scenario == "complete" {
                    ws.send(payload("")).await.unwrap();
                }
                ws.close(None).await.unwrap();
            });
            let result =
                order_snapshots_at(&url, "0xabc", BTreeSet::from(["".into(), "xyz".into()])).await;
            if scenario == "complete" {
                assert_eq!(result.unwrap().len(), 2);
            } else {
                assert!(result.is_err());
            }
            server.await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn http_429_retries_only_reads_with_cooldown_and_bounded_attempts() {
        use axum::{
            Json, Router,
            http::{HeaderMap, StatusCode},
            routing::post,
        };
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        for statuses in [[429, 200, 200], [429, 429, 429], [500, 200, 200]] {
            let calls = Arc::new(AtomicUsize::new(0));
            let count = calls.clone();
            let router = Router::new().route(
                "/",
                post(move |Json(body): Json<Value>| {
                    let index = count.fetch_add(1, Ordering::SeqCst);
                    async move {
                        assert_eq!(body, json!({"type":"allPerpMetas"}));
                        let mut headers = HeaderMap::new();
                        headers.insert("retry-after", "70".parse().unwrap());
                        (
                            StatusCode::from_u16(statuses[index]).unwrap(),
                            headers,
                            Json(json!([])),
                        )
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            let before = Instant::now();
            let result = post_budgeted(
                &reqwest::Client::new(),
                &url,
                json!({"type":"allPerpMetas"}),
                &Metrics::default(),
                &Mutex::new(Budget::default()),
            )
            .await;
            match statuses {
                [429, 200, _] => {
                    assert_eq!(result.unwrap(), json!([]));
                    assert_eq!(calls.load(Ordering::SeqCst), 2);
                    assert!(before.elapsed() >= Duration::from_secs(70));
                }
                [429, 429, 429] => {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("info allPerpMetas") && error.contains("429"));
                    assert_eq!(calls.load(Ordering::SeqCst), 3);
                    assert!(before.elapsed() >= Duration::from_secs(140));
                }
                _ => {
                    assert!(result.is_err());
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                }
            }
            server.abort();
        }
    }
}
