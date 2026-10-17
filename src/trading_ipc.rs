//! Native-client observation and identified submissions. Exchange execution remains in CommandQueue.
use crate::{ipc::IpcApp, protocol, runtime, state};
use hl_wire as wire;

impl IpcApp {
    pub(crate) async fn dispatch_trading(&self, request: wire::Request) -> wire::Response {
        let result = self.trading_request(request).await;
        result.unwrap_or_else(|error| wire::Response::Error {
            instance_id: self.queue.instance_id().to_string(),
            message: error.to_string(),
        })
    }

    async fn trading_request(&self, request: wire::Request) -> anyhow::Result<wire::Response> {
        let instance_id = self.queue.instance_id().to_string();
        if let wire::Request::Hello = request {
            let (network, account, read_only) = self
                .identity
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("daemon identity unavailable"))?;
            let state = self.state.read().await;
            let markets = state
                .markets
                .values()
                .map(market)
                .collect::<anyhow::Result<_>>()?;
            return Ok(wire::Response::Hello {
                version: wire::VERSION,
                instance_id,
                network: network.clone(),
                account: account.clone(),
                read_only: *read_only,
                markets,
            });
        }
        let expected = match &request {
            wire::Request::Open { instance_id, .. }
            | wire::Request::Snapshot { instance_id, .. }
            | wire::Request::Submit { instance_id, .. }
            | wire::Request::Get { instance_id, .. }
            | wire::Request::Close { instance_id, .. } => instance_id,
            wire::Request::Hello => unreachable!(),
        };
        anyhow::ensure!(
            *expected == instance_id,
            "daemon instance changed; reconcile outstanding submissions before reopening"
        );
        match request {
            wire::Request::Hello => unreachable!(),
            wire::Request::Open { market: symbol, .. } => {
                let session = self
                    .queue
                    .open_trading_session(symbol)
                    .await
                    .map_err(anyhow::Error::msg)?;
                let state = self.state.read().await;
                let m = state
                    .markets
                    .get(&session.active)
                    .ok_or_else(|| anyhow::anyhow!("market disappeared"))?;
                Ok(wire::Response::Opened {
                    instance_id,
                    session_id: session.session_id,
                    market: market(m)?,
                })
            }
            wire::Request::Close { session_id, .. } => {
                self.queue
                    .close_market_session(session_id)
                    .await
                    .map_err(anyhow::Error::msg)?;
                Ok(wire::Response::Closed)
            }
            wire::Request::Submit {
                session_id,
                sequence,
                market,
                command,
                ..
            } => {
                let submitted = self
                    .queue
                    .submit_identified(session_id, sequence, &market, command)
                    .await
                    .map_err(anyhow::Error::msg)?;
                Ok(wire::Response::Submitted {
                    instance_id,
                    command_id: submitted.command_id,
                    sequence,
                    queue_depth: submitted.queue_depth,
                })
            }
            wire::Request::Get { command_id, .. } => {
                let record = self.queue.get(command_id).await.ok_or_else(|| {
                    anyhow::anyhow!("command result unavailable; reconcile instead of resubmitting")
                })?;
                Ok(wire::Response::Command {
                    instance_id,
                    record: Box::new(command_record(record)),
                })
            }
            wire::Request::Snapshot {
                session_id,
                after_fill_seq,
                ..
            } => {
                let session = self
                    .queue
                    .market_session(session_id)
                    .await
                    .map_err(anyhow::Error::msg)?;
                let symbol = session.active;
                let now = now_ms();
                let state = self.state.read().await;
                let limits = state::FreshnessLimits::default();
                let readiness = state.readiness_for(
                    &symbol,
                    now,
                    limits,
                    true,
                    state.position(&symbol).is_none_or(|p| p.value.flat()),
                );
                let orders_ready = state.orders_ready(&symbol, now, limits.orders_ms);
                let market = market(
                    state
                        .markets
                        .get(&symbol)
                        .ok_or_else(|| anyhow::anyhow!("market disappeared"))?,
                )?;
                let orders = state
                    .orders_for(&symbol)
                    .map(|o| timed(o, order(&o.value)))
                    .collect();
                let position = state.position(&symbol).map(|p| {
                    timed(
                        p,
                        wire::Position {
                            size: p.value.size,
                            entry_price: p.value.entry_price,
                            unrealized_pnl: p.value.detail.map(|d| d.unrealized_pnl),
                            liquidation_price: p.value.detail.and_then(|d| d.liquidation_px),
                            leverage: p.value.detail.map(|d| d.leverage),
                            cross: p.value.detail.map(|d| d.leverage_cross),
                        },
                    )
                });
                let balances = state
                    .spot_balances
                    .values()
                    .map(|b| {
                        timed(
                            b,
                            wire::Balance {
                                coin: b.value.coin.clone(),
                                total: b.value.total,
                                hold: b.value.hold,
                            },
                        )
                    })
                    .collect();
                let retained = state.fills_after(0);
                let oldest_fill_seq = retained.first().map(|f| f.seq);
                let latest_fill_seq = retained.last().map_or(0, |f| f.seq);
                let fills = retained
                    .into_iter()
                    .filter(|f| f.seq > after_fill_seq && f.symbol == symbol)
                    .map(|f| wire::Fill {
                        seq: f.seq,
                        tid: f.tid,
                        oid: f.oid,
                        is_buy: f.is_buy,
                        size: f.size,
                        price: f.price,
                        closed_pnl: f.closed_pnl,
                        time_ms: f.time_ms,
                    })
                    .collect();
                let mark = state.mark_prices.get(&symbol).map(|p| timed(p, p.value));
                let twaps = state
                    .twaps_for(&symbol)
                    .map(|(id, t)| {
                        let trigger = t.value.details.as_ref().and_then(|d| d.t.as_ref());
                        let stop_price = t
                            .value
                            .details
                            .as_ref()
                            .and_then(|d| d.s.as_ref())
                            .map(|s| s.parse())
                            .transpose()?;
                        Ok(timed(
                            t,
                            wire::Twap {
                                id: *id,
                                is_buy: t.value.is_buy,
                                size: t.value.size,
                                executed_size: t.value.executed_size,
                                minutes: t.value.minutes,
                                reduce_only: t.value.reduce_only,
                                trigger_price: trigger.map(|t| t.p),
                                trigger_above: trigger.map(|t| t.a),
                                stop_price,
                            },
                        ))
                    })
                    .collect::<anyhow::Result<_>>()?;
                drop(state);
                let health = self.health_payload(symbol, readiness).await;
                let pending = self.queue.pending().await;
                Ok(wire::Response::Snapshot(Box::new(wire::Snapshot {
                    instance_id,
                    session_id,
                    observed_ms: now,
                    market,
                    health: wire::Health {
                        ready: health.ok,
                        halted: health.halted,
                        reasons: health.reasons,
                        action_connected: health.action_connected,
                        state_connected: health.state_feed_connected,
                        orders_ready,
                    },
                    orders,
                    position,
                    mark,
                    balances,
                    fills,
                    oldest_fill_seq,
                    latest_fill_seq,
                    queue_depth: pending.queue_depth,
                    confirmations: pending
                        .confirmations
                        .into_iter()
                        .map(confirmation)
                        .collect(),
                    twaps,
                })))
            }
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn market(m: &state::Market) -> anyhow::Result<wire::Market> {
    Ok(wire::Market {
        symbol: m.symbol.clone(),
        wire_symbol: m.wire_symbol.clone(),
        kind: match m.kind {
            protocol::MarketKind::Perp => wire::MarketKind::Perp,
            protocol::MarketKind::BuilderPerp => wire::MarketKind::BuilderPerp,
            protocol::MarketKind::Spot => wire::MarketKind::Spot,
        },
        size_decimals: u32::try_from(m.size_decimals)?,
    })
}

fn timed<T, U>(source: &state::Fresh<T>, value: U) -> wire::Timed<U> {
    wire::Timed {
        value,
        local_ms: source.local_ms,
        exchange_ms: source.exchange_ms,
    }
}

fn order(o: &state::Order) -> wire::Order {
    wire::Order {
        oid: o.oid,
        cloid: o.cloid.clone(),
        is_buy: o.is_buy,
        price: o.price,
        size: o.size,
        reduce_only: o.reduce_only,
        kind: match o.kind {
            state::OrderKind::Limit => wire::OrderKind::Limit,
            state::OrderKind::StopLoss => wire::OrderKind::StopLoss,
            state::OrderKind::TakeProfit => wire::OrderKind::TakeProfit,
            state::OrderKind::TrailingStop => wire::OrderKind::TrailingStop,
        },
        trailing: o.trailing.as_ref().map(|t| wire::Trailing {
            retracement: match &t.retracement {
                protocol::Retracement::Px(p) => p.to_string(),
                protocol::Retracement::Pct(p) => format!("{p}%"),
            },
            activation: t.activation,
            best: t.best,
        }),
    }
}

fn confirmation(c: runtime::PendingConfirmationView) -> wire::Confirmation {
    wire::Confirmation {
        scope: c.scope,
        summary: c.summary,
        expires_at_ms: c.expires_at_ms,
    }
}

fn command_record(r: runtime::CommandExecutionRecord) -> wire::CommandRecord {
    wire::CommandRecord {
        command_id: r.command_id,
        command: r.command,
        sequence: r.submission_sequence,
        state: match r.state {
            runtime::CommandExecutionState::Queued => wire::CommandState::Queued,
            runtime::CommandExecutionState::Running => wire::CommandState::Running,
            runtime::CommandExecutionState::Completed => wire::CommandState::Completed,
        },
        submitted_at_ms: r.submitted_at_ms,
        started_at_ms: r.started_at_ms,
        finished_at_ms: r.finished_at_ms,
        response: r.response.map(|r| wire::CommandOutput {
            ok: r.ok,
            lines: r.lines,
            confirmation: r.confirmation.map(confirmation),
            receipts: r.receipts.into_iter().map(receipt).collect(),
        }),
    }
}

fn receipt(r: crate::execution::SubmitReceipt) -> wire::Receipt {
    use crate::execution::{ExecutionStatus as S, OrderStatus as O};
    wire::Receipt {
        id: r.id,
        error: r.error,
        status: match r.status {
            S::Accepted => wire::ExecutionStatus::Accepted,
            S::Rejected => wire::ExecutionStatus::Rejected,
            S::Ambiguous => wire::ExecutionStatus::Ambiguous,
        },
        items: r
            .statuses
            .into_iter()
            .map(|o| match o {
                O::Success => wire::OrderOutcome::Success,
                O::AlreadyTerminal { message } => wire::OrderOutcome::AlreadyTerminal { message },
                O::Resting { oid, cloid } => wire::OrderOutcome::Resting { oid, cloid },
                O::Filled {
                    oid,
                    total_size,
                    average_price,
                } => wire::OrderOutcome::Filled {
                    oid,
                    total_size,
                    average_price,
                },
                O::TwapRunning { twap_id } => wire::OrderOutcome::TwapRunning { twap_id },
                O::WaitingForFill => wire::OrderOutcome::WaitingForFill,
                O::WaitingForTrigger => wire::OrderOutcome::WaitingForTrigger,
                O::Error { message } => wire::OrderOutcome::Error { message },
            })
            .collect(),
    }
}
