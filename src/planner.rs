use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::{
    command::{
        BatchPlace, Chase, Command, Protection, ProtectionKind, Scale, Side as CmdSide, Tif, Trade,
        TwapPlace,
    },
    protocol::{
        Action, AgentSetAbstraction, BatchCancel, BatchCancelCloid, BatchModify, BatchOrder,
        Cancel, CancelByCloid, Cloid, MarketKind, Modify, OrderGrouping, OrderRequest, OrderTarget,
        OrderType, Side, Size, TimeInForce, TpSl, TwapCancelAction, TwapOrder, TwapOrderAction,
        UpdateIsolatedMarginAction, UpdateLeverageAction,
    },
    state::{
        AccountMode, Book, Fresh, FreshnessLimits, LOCAL_ORDER_OID_MIN, Market, Order, OrderKind,
        TradingState,
    },
};

#[derive(Debug, Clone, PartialEq)]
pub struct ActionPlan {
    pub kind: &'static str,
    pub market: String,
    pub action_label: String,
    pub requires_book: bool,
    pub action: Action,
}

impl ActionPlan {
    pub fn validate(&self, market: &Market) -> Result<(), String> {
        let precision = market.tick();
        let validate = |order: &OrderRequest| {
            if order.asset != market.asset.0 {
                return Err(format!(
                    "action asset {} does not match {} asset {}",
                    order.asset, market.symbol, market.asset.0
                ));
            }
            if !precision.accepts(order.price) {
                return Err(format!(
                    "invalid {} wire price {}",
                    market.symbol, order.price
                ));
            }
            if let OrderType::Trigger { trigger_px, .. } = &order.order_type
                && !precision.accepts(*trigger_px)
            {
                return Err(format!(
                    "invalid {} trigger price {}",
                    market.symbol, trigger_px
                ));
            }
            let notional = (order.price * order.size).normalize();
            if !order.reduce_only && notional < Decimal::from(10) {
                return Err(format!(
                    "{} order notional is below the 10 USDC exchange minimum after precision rounding: {notional}",
                    market.symbol
                ));
            }
            Ok(())
        };
        match &self.action {
            Action::Order(batch) => batch.orders.iter().try_for_each(validate),
            Action::BatchModify(batch) => batch
                .modifies
                .iter()
                .map(|modify| &modify.order)
                .try_for_each(validate),
            _ => Ok(()),
        }
    }
}

pub struct Planner {
    market_cross_pct: Decimal,
}

impl Default for Planner {
    fn default() -> Self {
        Self {
            market_cross_pct: Decimal::new(50, 4),
        }
    }
}

impl Planner {
    pub fn with_market_cross_bps(market_cross_bps: u32) -> Self {
        Self {
            market_cross_pct: Decimal::new(i64::from(market_cross_bps), 4),
        }
    }

    pub fn plan(&self, state: &TradingState, command: Command) -> Result<ActionPlan, String> {
        self.plan_for(state, &state.active, command)
    }

    pub fn plan_for(
        &self,
        state: &TradingState,
        symbol: &str,
        command: Command,
    ) -> Result<ActionPlan, String> {
        let plan = match command {
            Command::Trade(trade) => self.trade(state, symbol, trade),
            Command::Scale(scale) => self.scale(state, symbol, scale),
            Command::BatchPlace(batch) => self.batch(state, symbol, batch),
            Command::ProtectionSet(protection) => self.protection(state, symbol, protection),
            Command::ProtectionCancel { kind } => self.protection_cancel(state, symbol, kind),
            Command::ChasePlace(chase) => self.chase(state, symbol, chase),
            Command::Close { size, price } => self.close(state, symbol, size, price),
            Command::CancelAll => self.cancel_all(state, symbol),
            Command::CancelOid { ids } => self.cancel_oids(state, symbol, ids),
            Command::CancelCloid { ids } => self.cancel_cloids(state, symbol, ids),
            Command::MoveOid { id, price } => {
                self.modify_oids(state, symbol, vec![id], Some(price), None, "move")
            }
            Command::ResizeOid { id, size } => {
                self.modify_oids(state, symbol, vec![id], None, Some(size), "resize")
            }
            Command::BatchMoveOid { ids, price } => {
                self.modify_oids(state, symbol, ids, Some(price), None, "batch_move")
            }
            Command::BatchResizeOid { ids, size } => {
                self.modify_oids(state, symbol, ids, None, Some(size), "batch_resize")
            }
            Command::BatchMoveCloid { ids, price } => {
                self.modify_cloids(state, symbol, ids, Some(price), None, "batch_move_cloid")
            }
            Command::BatchResizeCloid { ids, size } => {
                self.modify_cloids(state, symbol, ids, None, Some(size), "batch_resize_cloid")
            }
            Command::TwapPlace(twap) => self.twap(state, symbol, twap),
            Command::TwapCancel { id: Some(id) } => self.twap_cancel(state, symbol, id),
            Command::TwapCancel { id: None } => {
                Err("twap cancel all requires live twap state".to_string())
            }
            Command::Leverage { cross, value } => self.leverage(state, symbol, cross, value),
            Command::IsolatedMargin { add, amount } => {
                self.isolated_margin(state, symbol, add, &amount)
            }
            Command::AccountModeSet { mode } => self.account_mode(state, symbol, &mode),
            other => Err(format!(
                "command is not directly executable as an exchange action: {other:?}"
            )),
        }?;
        validate_reduce_only_plan(state, &plan)?;
        Ok(plan)
    }

    fn trade(
        &self,
        state: &TradingState,
        symbol: &str,
        trade: Trade,
    ) -> Result<ActionPlan, String> {
        let (trade, price_requires_book) = resolve_trade_price_fields_for(state, symbol, trade)?;
        let market = market(state, symbol)?;
        let side = side(trade.side.clone());
        let requires_book = trade.price.is_none() || price_requires_book;
        let price = match trade.price.as_deref() {
            Some(raw) => rounded_price(market, side, parse_decimal("price", raw)?, true)?,
            None => rounded_price(
                market,
                side,
                market_price(state, symbol, side, self.market_cross_pct)?,
                false,
            )?,
        };
        let stop_loss = trade
            .stop_loss
            .as_deref()
            .map(|stop| {
                rounded_price(
                    market,
                    opposite(side),
                    parse_decimal("stop loss", stop)?,
                    true,
                )
            })
            .transpose()?;
        let take_profit = trade
            .take_profit
            .as_deref()
            .map(|tp| {
                rounded_price(
                    market,
                    opposite(side),
                    parse_decimal("take profit", tp)?,
                    true,
                )
            })
            .transpose()?;
        validate_protection_levels(side, price, stop_loss, take_profit)?;
        let size_input = parse_size(&trade.size)?;
        reject_unsupported_trade_modifiers(market, &trade, size_input)?;
        let size = size_at_price(
            state,
            market,
            size_input,
            price,
            RiskContext {
                side,
                stop: stop_loss,
                label: "Risk sizing requires a stop-loss (sl/stop ...)",
            },
        )?;
        let mut orders = vec![order(
            market.asset.0,
            side,
            price,
            size,
            trade.reduce_only,
            tif(trade.tif, trade.post_only),
            next_cloid(),
        )];
        let trigger_side = opposite(side);
        if let Some(stop) = stop_loss {
            orders.push(trigger(market.asset.0, trigger_side, size, stop, TpSl::Sl));
        }
        if let Some(tp) = take_profit {
            orders.push(trigger(market.asset.0, trigger_side, size, tp, TpSl::Tp));
        }
        ensure_spot_capacity(state, market, side, [(size, price)])?;
        Ok(ActionPlan {
            kind: if orders.len() == 1 {
                "order"
            } else {
                "bracket"
            },
            market: symbol.to_string(),
            action_label: "trade".to_string(),
            requires_book,
            action: Action::Order(BatchOrder {
                grouping: if orders.len() == 1 {
                    OrderGrouping::Na
                } else {
                    OrderGrouping::NormalTpsl
                },
                orders,
            }),
        })
    }

    fn batch(
        &self,
        state: &TradingState,
        symbol: &str,
        batch: BatchPlace,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        let side = side(batch.side);
        reject_spot_reduce_only(market, batch.reduce_only)?;
        let mut orders = Vec::with_capacity(batch.orders.len());
        let mut requires_book = false;
        for leg in batch.orders {
            let price = price_level(state, symbol, "price", &leg.price)?;
            requires_book |= price.requires_book;
            let price = rounded_price(market, side, price.value, true)?;
            let size = size_at_price_no_risk(state, market, side, &leg.size, price)?;
            orders.push(order(
                market.asset.0,
                side,
                price,
                size,
                batch.reduce_only,
                tif(batch.tif.clone(), batch.post_only),
                next_cloid(),
            ));
        }
        ensure_spot_capacity(
            state,
            market,
            side,
            orders.iter().map(|order| (order.size, order.price)),
        )?;
        Ok(ActionPlan {
            kind: "batch",
            market: symbol.to_string(),
            action_label: "batch".to_string(),
            requires_book,
            action: Action::Order(BatchOrder {
                orders,
                grouping: OrderGrouping::Na,
            }),
        })
    }

    fn scale(
        &self,
        state: &TradingState,
        symbol: &str,
        scale: Scale,
    ) -> Result<ActionPlan, String> {
        let size_input = parse_size(&scale.size)?;
        reject_unsupported_scale_modifiers(market(state, symbol)?, &scale, size_input)?;
        let (scale, requires_book) = resolve_scale_price_fields_for(state, symbol, scale)?;
        let market = market(state, symbol)?;
        let side = side(scale.side);
        let start = parse_decimal("start price", &scale.start_price)?;
        let end = parse_decimal("end price", &scale.end_price)?;
        let step = if scale.legs == 1 {
            Decimal::ZERO
        } else {
            (end - start) / Decimal::from(scale.legs - 1)
        };
        let prices = (0..scale.legs)
            .map(|idx| {
                rounded_price(
                    market,
                    side,
                    (start + step * Decimal::from(idx)).normalize(),
                    true,
                )
            })
            .collect::<Result<Vec<_>, String>>()?;
        let sizes = scale_sizes(state, market, side, size_input, &prices)?;
        let orders = prices
            .into_iter()
            .zip(sizes)
            .map(|(price, size)| {
                Ok(order(
                    market.asset.0,
                    side,
                    price,
                    size,
                    scale.reduce_only,
                    tif(scale.tif.clone(), scale.post_only),
                    next_cloid(),
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        ensure_spot_capacity(
            state,
            market,
            side,
            orders.iter().map(|order| (order.size, order.price)),
        )?;
        Ok(ActionPlan {
            kind: "scale",
            market: symbol.to_string(),
            action_label: "scale".to_string(),
            requires_book,
            action: Action::Order(BatchOrder {
                orders,
                grouping: OrderGrouping::Na,
            }),
        })
    }

    fn protection(
        &self,
        state: &TradingState,
        symbol: &str,
        protection: Protection,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        require_perp_market(market, "position protection")?;
        let position = state
            .position(symbol)
            .ok_or_else(|| "unknown active position".to_string())?
            .value
            .clone();
        if position.flat() {
            return Err("cannot place protection without an active position".to_string());
        }
        let side = if position.size > Decimal::ZERO {
            Side::Ask
        } else {
            Side::Bid
        };
        let tpsl = match protection.kind {
            ProtectionKind::StopLoss | ProtectionKind::TrailingStop => TpSl::Sl,
            ProtectionKind::TakeProfit => TpSl::Tp,
        };
        let trigger_px = if protection.kind == ProtectionKind::TrailingStop {
            PriceLevel {
                value: trailing_trigger(state, symbol, side, &protection.value)?,
                requires_book: true,
            }
        } else {
            price_level(state, symbol, "trigger", &protection.value)?
        };
        let trigger_px = rounded_price(market, side, trigger_px.value, true)?;
        let reference = fresh_book(state, symbol)
            .map(|book| (book.bid + book.ask) / Decimal::TWO)
            .ok_or_else(|| "fresh book required to validate protection trigger".to_string())?;
        validate_position_trigger(position.size > Decimal::ZERO, reference, trigger_px, &tpsl)?;
        let size = protection_size(market, &position, protection.size, trigger_px)?;
        Ok(ActionPlan {
            kind: "protection",
            market: symbol.to_string(),
            action_label: "protection".to_string(),
            requires_book: true,
            action: Action::Order(BatchOrder {
                grouping: OrderGrouping::PositionTpsl,
                orders: vec![trigger(market.asset.0, side, size, trigger_px, tpsl)],
            }),
        })
    }

    fn protection_cancel(
        &self,
        state: &TradingState,
        symbol: &str,
        kind: ProtectionKind,
    ) -> Result<ActionPlan, String> {
        let target = match kind {
            ProtectionKind::StopLoss | ProtectionKind::TrailingStop => OrderKind::StopLoss,
            ProtectionKind::TakeProfit => OrderKind::TakeProfit,
        };
        let market = market(state, symbol)?;
        require_perp_market(market, "position protection")?;
        let orders = state
            .orders_for(symbol)
            .filter(|order| order.value.kind == target)
            .collect::<Vec<_>>();
        if orders.is_empty() {
            require_orders_ready(state, symbol)?;
            return Err("no matching protection orders".to_string());
        }
        if let Some(cloid_cancels) = cloid_cancels_for_local_orders(market.asset.0, &orders)? {
            return Ok(ActionPlan {
                kind: "protection_cancel",
                market: symbol.to_string(),
                action_label: "protection_cancel".to_string(),
                requires_book: false,
                action: Action::CancelByCloid(BatchCancelCloid {
                    cancels: cloid_cancels,
                }),
            });
        }
        Ok(ActionPlan {
            kind: "protection_cancel",
            market: symbol.to_string(),
            action_label: "protection_cancel".to_string(),
            requires_book: false,
            action: Action::Cancel(BatchCancel {
                cancels: orders
                    .into_iter()
                    .map(|order| Cancel {
                        asset: market.asset.0,
                        oid: order.value.oid,
                    })
                    .collect(),
            }),
        })
    }

    fn chase(
        &self,
        state: &TradingState,
        symbol: &str,
        chase: Chase,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        let side = side(chase.side);
        reject_spot_reduce_only(market, chase.reduce_only)?;
        let price = rounded_price(
            market,
            side,
            chase_target(state, symbol, side, &chase.distance)?,
            true,
        )?;
        let size = size_at_price_no_risk(state, market, side, &chase.size, price)?;
        ensure_spot_capacity(state, market, side, [(size, price)])?;
        Ok(ActionPlan {
            kind: "chase_start",
            market: symbol.to_string(),
            action_label: format!("chase:{}", chase.distance),
            requires_book: true,
            action: Action::Order(BatchOrder {
                grouping: OrderGrouping::Na,
                orders: vec![order(
                    market.asset.0,
                    side,
                    price,
                    size,
                    chase.reduce_only,
                    tif(chase.tif, chase.post_only),
                    next_cloid(),
                )],
            }),
        })
    }

    fn cancel_all(&self, state: &TradingState, symbol: &str) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        require_orders_ready(state, symbol)?;
        let orders = cancelable_orders(state, symbol);
        if orders.is_empty() {
            return Err("no active non-protection limit orders to cancel".to_string());
        }
        if let Some(cloid_cancels) = cloid_cancels_for_local_orders(market.asset.0, &orders)? {
            return Ok(ActionPlan {
                kind: "cancel_all",
                market: symbol.to_string(),
                action_label: "cancel_all".to_string(),
                requires_book: false,
                action: Action::CancelByCloid(BatchCancelCloid {
                    cancels: cloid_cancels,
                }),
            });
        }
        Ok(ActionPlan {
            kind: "cancel_all",
            market: symbol.to_string(),
            action_label: "cancel_all".to_string(),
            requires_book: false,
            action: Action::Cancel(BatchCancel {
                cancels: orders
                    .into_iter()
                    .map(|order| Cancel {
                        asset: market.asset.0,
                        oid: order.value.oid,
                    })
                    .collect(),
            }),
        })
    }

    fn cancel_oids(
        &self,
        state: &TradingState,
        symbol: &str,
        ids: Vec<u64>,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        Ok(ActionPlan {
            kind: "cancel_oid",
            market: symbol.to_string(),
            action_label: "cancel_oid".to_string(),
            requires_book: false,
            action: Action::Cancel(BatchCancel {
                cancels: ids
                    .into_iter()
                    .map(|oid| Cancel {
                        asset: market.asset.0,
                        oid,
                    })
                    .collect(),
            }),
        })
    }

    fn cancel_cloids(
        &self,
        state: &TradingState,
        symbol: &str,
        ids: Vec<String>,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        let cancels = ids
            .into_iter()
            .map(|id| {
                Cloid::from_str(&id).map(|cloid| CancelByCloid {
                    asset: market.asset.0,
                    cloid,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|err| err.to_string())?;
        Ok(ActionPlan {
            kind: "cancel_cloid",
            market: symbol.to_string(),
            action_label: "cancel_cloid".to_string(),
            requires_book: false,
            action: Action::CancelByCloid(BatchCancelCloid { cancels }),
        })
    }

    fn modify_oids(
        &self,
        state: &TradingState,
        symbol: &str,
        ids: Vec<u64>,
        price: Option<String>,
        size: Option<String>,
        label: &'static str,
    ) -> Result<ActionPlan, String> {
        self.modify(
            state,
            symbol,
            ids.into_iter().map(OrderTarget::Oid).collect(),
            price,
            size,
            label,
        )
    }

    fn modify_cloids(
        &self,
        state: &TradingState,
        symbol: &str,
        ids: Vec<String>,
        price: Option<String>,
        size: Option<String>,
        label: &'static str,
    ) -> Result<ActionPlan, String> {
        let targets = ids
            .into_iter()
            .map(|id| Cloid::from_str(&id).map(OrderTarget::Cloid))
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|err| err.to_string())?;
        self.modify(state, symbol, targets, price, size, label)
    }

    fn modify(
        &self,
        state: &TradingState,
        symbol: &str,
        targets: Vec<OrderTarget>,
        price: Option<String>,
        size: Option<String>,
        label: &'static str,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        let price = price
            .as_deref()
            .map(|raw| price_level(state, symbol, "price", raw))
            .transpose()?;
        let requires_book = price.is_some_and(|price| price.requires_book);
        let size = size.as_deref().map(parse_size).transpose()?;
        let modifies = targets
            .into_iter()
            .map(|target| {
                let existing = find_order(state, symbol, &target)?;
                let side = if existing.is_buy {
                    Side::Bid
                } else {
                    Side::Ask
                };
                let raw_price = price.map_or(existing.price, |price| price.value);
                let planned_price = rounded_price(market, side, raw_price, true)?;
                let planned_size = match size {
                    Some(SizeInput::Percent(percent)) if market.kind == MarketKind::Spot => {
                        let released = if side == Side::Bid {
                            existing.size * existing.price
                        } else {
                            existing.size
                        };
                        rounded_spot_percent_size(
                            state,
                            market,
                            side,
                            percent,
                            planned_price,
                            released,
                        )?
                    }
                    Some(size) => {
                        rounded_size_input_no_risk(state, market, side, size, planned_price)?
                    }
                    None => existing.size,
                };
                Ok(Modify {
                    oid: target,
                    order: request_from_order(
                        market,
                        market.asset.0,
                        existing,
                        raw_price,
                        planned_size,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ActionPlan {
            kind: "modify",
            market: symbol.to_string(),
            action_label: label.to_string(),
            requires_book,
            action: Action::BatchModify(BatchModify { modifies }),
        })
    }

    fn close(
        &self,
        state: &TradingState,
        symbol: &str,
        size: Option<String>,
        price: Option<String>,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        let (side, available, reduce_only) = if market.kind == MarketKind::Spot {
            let available = spot_capacity(state, market, Side::Ask)?;
            if available <= Decimal::ZERO {
                return Err(format!(
                    "no available {} spot balance to close",
                    spot_pair(market)?.0
                ));
            }
            (Side::Ask, available, false)
        } else {
            let position = &state
                .position(symbol)
                .ok_or_else(|| "unknown active position".to_string())?
                .value;
            if position.flat() {
                return Err("active position is flat".to_string());
            }
            (
                if position.size > Decimal::ZERO {
                    Side::Ask
                } else {
                    Side::Bid
                },
                position.size.abs(),
                true,
            )
        };
        let explicit_price = price.is_some();
        let price = match price {
            Some(raw) => {
                let price = price_level(state, symbol, "price", &raw)?;
                (
                    rounded_price(market, side, price.value, true)?,
                    price.requires_book,
                )
            }
            None => rounded_price(
                market,
                side,
                market_price(state, symbol, side, self.market_cross_pct)?,
                false,
            )
            .map(|price| (price, true))?,
        };
        let (price, requires_book) = price;
        if explicit_price {
            reject_non_marketable_ioc_close(state, symbol, side, price)?;
        }
        let size = match size {
            Some(raw) => close_size_at_price(market, available, &raw, price)?,
            None => rounded_size(market, available)?,
        };
        Ok(ActionPlan {
            kind: "close",
            market: symbol.to_string(),
            action_label: "close".to_string(),
            requires_book,
            action: Action::Order(BatchOrder {
                grouping: OrderGrouping::Na,
                orders: vec![order(
                    market.asset.0,
                    side,
                    price,
                    size,
                    reduce_only,
                    TimeInForce::Ioc,
                    next_cloid(),
                )],
            }),
        })
    }

    fn twap(
        &self,
        state: &TradingState,
        symbol: &str,
        twap: TwapPlace,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        let side = side(twap.side);
        reject_spot_reduce_only(market, twap.reduce_only)?;
        let price = rounded_price(
            market,
            side,
            market_price(state, symbol, side, self.market_cross_pct)?,
            false,
        )?;
        let size = size_at_price_no_risk(state, market, side, &twap.size, price)?;
        ensure_spot_capacity(state, market, side, [(size, price)])?;
        let notional = (size * price).normalize();
        if notional < Decimal::from(100) {
            return Err(format!(
                "TWAP notional is below the 100 USDC exchange minimum after precision rounding: {notional}"
            ));
        }
        Ok(ActionPlan {
            kind: "twap",
            market: symbol.to_string(),
            action_label: "twap".to_string(),
            requires_book: true,
            action: Action::TwapOrder(TwapOrderAction {
                twap: TwapOrder {
                    asset: market.asset.0,
                    is_buy: side == Side::Bid,
                    size,
                    reduce_only: twap.reduce_only,
                    minutes: twap.minutes,
                    randomize: twap.randomize,
                },
            }),
        })
    }

    fn twap_cancel(
        &self,
        state: &TradingState,
        symbol: &str,
        id: u64,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        Ok(ActionPlan {
            kind: "twap_cancel",
            market: symbol.to_string(),
            action_label: "twap_cancel".to_string(),
            requires_book: false,
            action: Action::TwapCancel(TwapCancelAction {
                asset: market.asset.0,
                twap_id: id,
            }),
        })
    }

    fn leverage(
        &self,
        state: &TradingState,
        symbol: &str,
        cross: bool,
        value: u32,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        require_perp_market(market, "leverage")?;
        if value == 0 {
            return Err("leverage must be positive".to_string());
        }
        let maximum = market
            .max_leverage
            .ok_or_else(|| format!("maximum leverage unavailable for {symbol}"))?;
        if value > maximum {
            return Err(format!(
                "leverage exceeds market maximum: requested={value} maximum={maximum}"
            ));
        }
        Ok(ActionPlan {
            kind: "leverage",
            market: symbol.to_string(),
            action_label: "leverage".to_string(),
            requires_book: false,
            action: Action::UpdateLeverage(UpdateLeverageAction {
                asset: market.asset.0,
                is_cross: cross,
                leverage: value,
            }),
        })
    }

    fn isolated_margin(
        &self,
        state: &TradingState,
        symbol: &str,
        add: bool,
        amount: &str,
    ) -> Result<ActionPlan, String> {
        let market = market(state, symbol)?;
        if !matches!(
            market.kind,
            crate::protocol::MarketKind::Perp | crate::protocol::MarketKind::BuilderPerp
        ) {
            return Err("isolated margin requires an active perp market".to_string());
        }
        let position = state
            .position(symbol)
            .ok_or_else(|| "unknown active position for isolated margin".to_string())?
            .value
            .clone();
        if position.flat() {
            return Err("cannot adjust isolated margin without an active position".to_string());
        }
        let detail = position
            .detail
            .ok_or_else(|| "position detail unavailable for isolated margin".to_string())?;
        if detail.leverage_cross {
            return Err("isolated margin requires an isolated/no-cross position".to_string());
        }
        Ok(ActionPlan {
            kind: "isolated_margin",
            market: symbol.to_string(),
            action_label: if add {
                "margin_add".to_string()
            } else {
                "margin_remove".to_string()
            },
            requires_book: false,
            action: Action::UpdateIsolatedMargin(UpdateIsolatedMarginAction {
                asset: market.asset.0,
                is_buy: position.size > Decimal::ZERO,
                ntli: margin_ntli(add, amount)?,
            }),
        })
    }

    fn account_mode(
        &self,
        state: &TradingState,
        symbol: &str,
        mode: &str,
    ) -> Result<ActionPlan, String> {
        let requested = AccountMode::parse(mode)?;
        let current = state
            .account_mode
            .as_ref()
            .ok_or_else(|| "account mode unavailable".to_string())?
            .value;
        let abstraction = requested.agent_transition_code_from(current)?;
        Ok(ActionPlan {
            kind: "account_mode",
            market: symbol.to_string(),
            action_label: "account_mode".to_string(),
            requires_book: false,
            action: Action::AgentSetAbstraction(AgentSetAbstraction {
                abstraction: abstraction.to_string(),
            }),
        })
    }
}

fn reject_unsupported_trade_modifiers(
    market: &Market,
    trade: &Trade,
    size: SizeInput,
) -> Result<(), String> {
    if market.kind == MarketKind::Spot {
        reject_spot_reduce_only(market, trade.reduce_only)?;
        if size.is_risk()
            || trade.stop_loss.is_some()
            || trade.take_profit.is_some()
            || trade.trailing.is_some()
        {
            return Err(
                "spot orders do not support position protection or risk sizing".to_string(),
            );
        }
    }
    if size.is_risk() && trade.reduce_only {
        return Err("Risk sizing is not supported with reduce-only".to_string());
    }
    if trade.trailing.is_some() {
        return Err("trailing stop requires the managed trailing engine".to_string());
    }
    if trade.chase.is_some() {
        return Err("trade chase modifier requires the managed chase engine".to_string());
    }
    Ok(())
}

fn reject_unsupported_scale_modifiers(
    market: &Market,
    scale: &Scale,
    size: SizeInput,
) -> Result<(), String> {
    if market.kind == MarketKind::Spot {
        reject_spot_reduce_only(market, scale.reduce_only)?;
        if size.is_risk()
            || scale.stop_loss.is_some()
            || scale.take_profit.is_some()
            || scale.trailing.is_some()
        {
            return Err(
                "spot orders do not support position protection or risk sizing".to_string(),
            );
        }
    }
    if size.is_risk() {
        if scale.reduce_only {
            return Err("Risk sizing is not supported with reduce-only".to_string());
        }
        if scale.trailing.is_some() {
            return Err(
                "Risk sizing requires a fixed stop-loss (sl/stop), not trailing".to_string(),
            );
        }
        if scale.stop_loss.is_none() {
            return Err("Risk sizing requires a stop-loss (sl/stop ...)".to_string());
        }
    }
    if scale.stop_loss.is_some() || scale.take_profit.is_some() {
        return Err(
            "scale TP/SL attachments require dependent protection orchestration".to_string(),
        );
    }
    if scale.trailing.is_some() {
        return Err("scale trailing attachment requires the managed trailing engine".to_string());
    }
    Ok(())
}

fn market<'a>(state: &'a TradingState, symbol: &str) -> Result<&'a Market, String> {
    state
        .markets
        .get(symbol)
        .ok_or_else(|| format!("unknown market {symbol}"))
}

fn require_perp_market(market: &Market, action: &str) -> Result<(), String> {
    if market.kind == MarketKind::Spot {
        return Err(format!("{action} requires an active perp market"));
    }
    Ok(())
}

fn reject_spot_reduce_only(market: &Market, reduce_only: bool) -> Result<(), String> {
    if market.kind == MarketKind::Spot && reduce_only {
        return Err("reduce-only is invalid for spot trading".to_string());
    }
    Ok(())
}

fn validate_protection_levels(
    entry_side: Side,
    entry: Decimal,
    stop_loss: Option<Decimal>,
    take_profit: Option<Decimal>,
) -> Result<(), String> {
    let is_long = entry_side == Side::Bid;
    if let Some(stop) = stop_loss {
        validate_position_trigger(is_long, entry, stop, &TpSl::Sl)?;
    }
    if let Some(take) = take_profit {
        validate_position_trigger(is_long, entry, take, &TpSl::Tp)?;
    }
    Ok(())
}

fn validate_position_trigger(
    is_long: bool,
    reference: Decimal,
    trigger: Decimal,
    kind: &TpSl,
) -> Result<(), String> {
    let valid = match (is_long, kind) {
        (true, TpSl::Sl) => trigger < reference,
        (true, TpSl::Tp) => trigger > reference,
        (false, TpSl::Sl) => trigger > reference,
        (false, TpSl::Tp) => trigger < reference,
    };
    if valid {
        Ok(())
    } else {
        let side = if is_long { "long" } else { "short" };
        let kind = if *kind == TpSl::Sl { "SL" } else { "TP" };
        Err(format!(
            "invalid {kind} for {side}: trigger={trigger} reference={reference}"
        ))
    }
}

fn protection_size(
    market: &Market,
    position: &crate::state::Position,
    raw: Option<String>,
    trigger_px: Decimal,
) -> Result<Decimal, String> {
    let available = position.size.abs();
    let size = match raw {
        None => rounded_size(market, available)?,
        Some(raw) => match parse_size(&raw)? {
            SizeInput::Base(size) => rounded_size(market, size)?,
            SizeInput::Quote(notional) => rounded_size(market, notional / trigger_px)?,
            SizeInput::Percent(percent) => {
                rounded_size(market, available * percent / Decimal::from(100))?
            }
            SizeInput::RiskUsd(_) | SizeInput::RiskPercent(_) => {
                return Err("protection size does not accept risk sizing".to_string());
            }
        },
    };
    if size > available {
        return Err(format!(
            "protection size exceeds position: requested={size} available={available}"
        ));
    }
    Ok(size)
}

fn validate_reduce_only_plan(state: &TradingState, plan: &ActionPlan) -> Result<(), String> {
    let market = market(state, &plan.market)?;
    let (orders, aggregate) = match &plan.action {
        Action::Order(batch) if batch.grouping == OrderGrouping::NormalTpsl => {
            (batch.orders.first().into_iter().collect::<Vec<_>>(), false)
        }
        Action::Order(batch) => (
            batch.orders.iter().collect::<Vec<_>>(),
            batch.grouping == OrderGrouping::Na,
        ),
        Action::BatchModify(batch) => (
            batch
                .modifies
                .iter()
                .map(|modify| &modify.order)
                .collect::<Vec<_>>(),
            true,
        ),
        Action::TwapOrder(action) if action.twap.reduce_only => {
            return validate_reduce_only_order(state, market, action.twap.is_buy, action.twap.size);
        }
        _ => return Ok(()),
    };
    let reducing = orders
        .into_iter()
        .filter(|order| order.reduce_only)
        .collect::<Vec<_>>();
    if reducing.is_empty() {
        return Ok(());
    }
    for order in &reducing {
        validate_reduce_only_order(state, market, order.is_buy, order.size)?;
        validate_reduce_only_minimum(state, market, order)?;
    }
    if aggregate {
        let total = reducing.iter().map(|order| order.size).sum::<Decimal>();
        let available = state
            .position(&plan.market)
            .map(|position| position.value.size.abs())
            .unwrap_or_default();
        if total > available {
            return Err(format!(
                "aggregate reduce-only size exceeds position: requested={total} available={available}"
            ));
        }
    }
    Ok(())
}

fn validate_reduce_only_minimum(
    state: &TradingState,
    market: &Market,
    order: &OrderRequest,
) -> Result<(), String> {
    let notional = (order.price * order.size).normalize();
    if notional >= Decimal::from(10) {
        return Ok(());
    }
    let available = state
        .position(&market.symbol)
        .map(|position| position.value.size.abs())
        .unwrap_or_default();
    if order.size == available {
        return Ok(());
    }
    Err(format!(
        "{} partial reduce-only order notional is below the 10 USDC exchange minimum after precision rounding: {notional}; only an exact full-position close is exempt",
        market.symbol
    ))
}

fn validate_reduce_only_order(
    state: &TradingState,
    market: &Market,
    is_buy: bool,
    size: Decimal,
) -> Result<(), String> {
    require_perp_market(market, "reduce-only")?;
    let position = state
        .position(&market.symbol)
        .ok_or_else(|| "unknown position for reduce-only order".to_string())?;
    if position.value.flat() {
        return Err("cannot place reduce-only order on a flat position".to_string());
    }
    let correct_side = (position.value.size > Decimal::ZERO && !is_buy)
        || (position.value.size < Decimal::ZERO && is_buy);
    if !correct_side {
        return Err("reduce-only side would not reduce the active position".to_string());
    }
    if size > position.value.size.abs() {
        return Err(format!(
            "reduce-only size exceeds position: requested={size} available={}",
            position.value.size.abs()
        ));
    }
    Ok(())
}

fn side(side: CmdSide) -> Side {
    match side {
        CmdSide::Buy => Side::Bid,
        CmdSide::Sell => Side::Ask,
    }
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Bid => Side::Ask,
        Side::Ask => Side::Bid,
    }
}

fn tif(tif: Option<Tif>, post_only: bool) -> TimeInForce {
    match (tif, post_only) {
        (Some(Tif::Ioc), _) => TimeInForce::Ioc,
        (Some(Tif::Gtc), false) => TimeInForce::Gtc,
        _ if post_only => TimeInForce::Alo,
        (Some(Tif::Alo), _) => TimeInForce::Alo,
        _ => TimeInForce::Gtc,
    }
}

fn order(
    asset: u32,
    side: Side,
    price: Decimal,
    size: Decimal,
    reduce_only: bool,
    tif: TimeInForce,
    cloid: Cloid,
) -> OrderRequest {
    OrderRequest {
        asset,
        is_buy: side == Side::Bid,
        price,
        size,
        reduce_only,
        order_type: OrderType::Limit { tif },
        cloid,
    }
}

fn trigger(asset: u32, side: Side, size: Decimal, trigger_px: Decimal, tpsl: TpSl) -> OrderRequest {
    OrderRequest {
        asset,
        is_buy: side == Side::Bid,
        price: trigger_px,
        size,
        reduce_only: true,
        order_type: OrderType::Trigger {
            is_market: true,
            trigger_px,
            tpsl,
        },
        cloid: next_cloid(),
    }
}

fn request_from_order(
    market: &Market,
    asset: u32,
    existing: &Order,
    price: Decimal,
    size: Decimal,
) -> Result<OrderRequest, String> {
    let side = if existing.is_buy {
        Side::Bid
    } else {
        Side::Ask
    };
    let price = rounded_price(market, side, price, true)?;
    let size = rounded_size(market, size)?;
    let cloid = existing
        .cloid
        .as_deref()
        .map(Cloid::from_str)
        .transpose()
        .map_err(|err| err.to_string())?
        .unwrap_or_else(next_cloid);
    let order_type = match existing.kind {
        OrderKind::Limit => OrderType::Limit {
            tif: existing
                .tif
                .clone()
                .ok_or_else(|| "cannot modify limit order with unknown tif".to_string())?,
        },
        OrderKind::StopLoss | OrderKind::TrailingStop => OrderType::Trigger {
            is_market: true,
            trigger_px: price,
            tpsl: TpSl::Sl,
        },
        OrderKind::TakeProfit => OrderType::Trigger {
            is_market: true,
            trigger_px: price,
            tpsl: TpSl::Tp,
        },
    };
    Ok(OrderRequest {
        asset,
        is_buy: side == Side::Bid,
        price,
        size,
        reduce_only: existing.reduce_only,
        order_type,
        cloid,
    })
}

fn find_order<'a>(
    state: &'a TradingState,
    symbol: &str,
    target: &OrderTarget,
) -> Result<&'a Order, String> {
    let found = match target {
        OrderTarget::Oid(oid) => state
            .orders
            .get(oid)
            .filter(|order| order.value.symbol == symbol)
            .map(|order| &order.value),
        OrderTarget::Cloid(cloid) => {
            let cloid = cloid.to_string();
            state
                .orders
                .values()
                .filter(|order| order.value.symbol == symbol)
                .find(|order| order.value.cloid.as_deref() == Some(cloid.as_str()))
                .map(|order| &order.value)
        }
    };
    if let Some(order) = found {
        return Ok(order);
    }
    require_orders_ready(state, symbol)?;
    match target {
        OrderTarget::Oid(oid) => Err(format!("unknown active order oid={oid}")),
        OrderTarget::Cloid(cloid) => Err(format!("unknown active order cloid={cloid}")),
    }
}

fn require_orders_ready(state: &TradingState, symbol: &str) -> Result<(), String> {
    if state.orders_ready(symbol, now_ms(), FreshnessLimits::default().orders_ms) {
        Ok(())
    } else {
        Err(format!("{symbol} orders syncing"))
    }
}

fn cancelable_orders<'a>(state: &'a TradingState, symbol: &str) -> Vec<&'a Fresh<Order>> {
    state
        .orders
        .values()
        .filter(|order| order.value.symbol == symbol)
        .filter(|order| order.value.kind == OrderKind::Limit)
        .collect()
}

fn cloid_cancels_for_local_orders(
    asset: u32,
    orders: &[&Fresh<Order>],
) -> Result<Option<Vec<CancelByCloid>>, String> {
    if !orders
        .iter()
        .any(|order| order.value.oid >= LOCAL_ORDER_OID_MIN)
    {
        return Ok(None);
    }
    orders
        .iter()
        .map(|order| {
            let cloid = order
                .value
                .cloid
                .as_deref()
                .ok_or_else(|| "local protection order missing cloid".to_string())?;
            Cloid::from_str(cloid)
                .map(|cloid| CancelByCloid { asset, cloid })
                .map_err(|err| err.to_string())
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn market_price(
    state: &TradingState,
    symbol: &str,
    side: Side,
    cross_pct: Decimal,
) -> Result<Decimal, String> {
    let book = state
        .book
        .get(symbol)
        .ok_or_else(|| "book unavailable for market emulation".to_string())?
        .value;
    let one = Decimal::ONE;
    Ok(match side {
        Side::Bid => book.ask * (one + cross_pct),
        Side::Ask => book.bid * (one - cross_pct),
    })
}

fn reject_non_marketable_ioc_close(
    state: &TradingState,
    symbol: &str,
    side: Side,
    price: Decimal,
) -> Result<(), String> {
    let Some(book) = fresh_book(state, symbol) else {
        return Ok(());
    };
    match side {
        Side::Ask if price > book.bid => Err(format!(
            "close at {price} would not immediately fill: sell IOC close must be <= bid {}; use an explicit reduce-only sell limit for a resting exit",
            book.bid
        )),
        Side::Bid if price < book.ask => Err(format!(
            "close at {price} would not immediately fill: buy IOC close must be >= ask {}; use an explicit reduce-only buy limit for a resting exit",
            book.ask
        )),
        _ => Ok(()),
    }
}

fn fresh_book(state: &TradingState, symbol: &str) -> Option<Book> {
    let book = state.book.get(symbol)?;
    (book.age_ms(now_ms()) <= FreshnessLimits::default().book_ms).then_some(book.value)
}

#[derive(Debug, Clone, Copy)]
struct PriceLevel {
    value: Decimal,
    requires_book: bool,
}

pub(crate) fn resolve_trade_price_fields_for(
    state: &TradingState,
    symbol: &str,
    mut trade: Trade,
) -> Result<(Trade, bool), String> {
    let mut requires_book = false;
    requires_book |= resolve_optional_price_field(state, symbol, "price", &mut trade.price)?;
    requires_book |=
        resolve_optional_price_field(state, symbol, "stop loss", &mut trade.stop_loss)?;
    requires_book |=
        resolve_optional_price_field(state, symbol, "take profit", &mut trade.take_profit)?;
    Ok((trade, requires_book))
}

pub(crate) fn resolve_scale_price_fields_for(
    state: &TradingState,
    symbol: &str,
    mut scale: Scale,
) -> Result<(Scale, bool), String> {
    let mut requires_book = false;
    requires_book |=
        resolve_required_price_field(state, symbol, "start price", &mut scale.start_price)?;
    requires_book |=
        resolve_required_price_field(state, symbol, "end price", &mut scale.end_price)?;
    requires_book |=
        resolve_optional_price_field(state, symbol, "stop loss", &mut scale.stop_loss)?;
    requires_book |=
        resolve_optional_price_field(state, symbol, "take profit", &mut scale.take_profit)?;
    Ok((scale, requires_book))
}

fn resolve_required_price_field(
    state: &TradingState,
    symbol: &str,
    name: &str,
    raw: &mut String,
) -> Result<bool, String> {
    let price = price_level(state, symbol, name, raw)?;
    *raw = price_token(price.value);
    Ok(price.requires_book)
}

fn resolve_optional_price_field(
    state: &TradingState,
    symbol: &str,
    name: &str,
    raw: &mut Option<String>,
) -> Result<bool, String> {
    match raw {
        Some(raw) => resolve_required_price_field(state, symbol, name, raw),
        None => Ok(false),
    }
}

fn price_level(
    state: &TradingState,
    symbol: &str,
    name: &str,
    raw: &str,
) -> Result<PriceLevel, String> {
    let token = raw.trim();
    if token.is_empty() {
        return Err(format!("invalid {name}: {raw}"));
    }
    if token.eq_ignore_ascii_case("$entry") {
        let value = state
            .position(symbol)
            .and_then(|position| position.value.entry_price)
            .ok_or_else(|| format!("entry price unavailable for {name}"))?;
        return Ok(PriceLevel {
            value: positive_price(name, value)?,
            requires_book: false,
        });
    }
    if let Some(percent) = token.strip_suffix('%') {
        let Some((sign, percent)) = signed_tail(percent.trim()) else {
            return Err(format!(
                "{name} relative percent requires explicit + or -: {raw}"
            ));
        };
        let percent_name = format!("{name} percent");
        let reference = relative_reference(state, symbol, name)?;
        let delta =
            reference * parse_positive_decimal(&percent_name, percent.trim())? / Decimal::from(100);
        return relative_price(name, reference, sign, delta);
    }
    if let Some((sign, amount)) = signed_tail(token) {
        return relative_price(
            name,
            relative_reference(state, symbol, name)?,
            sign,
            parse_positive_decimal(name, amount.trim())?,
        );
    }
    Ok(PriceLevel {
        value: positive_price(name, parse_decimal(name, token)?)?,
        requires_book: false,
    })
}

fn relative_reference(state: &TradingState, symbol: &str, name: &str) -> Result<Decimal, String> {
    let book = fresh_book(state, symbol)
        .ok_or_else(|| format!("fresh book unavailable for relative {name}"))?;
    positive_price(name, ((book.bid + book.ask) / Decimal::TWO).normalize())
}

fn relative_price(
    name: &str,
    reference: Decimal,
    sign: char,
    delta: Decimal,
) -> Result<PriceLevel, String> {
    let value = match sign {
        '+' => reference + delta,
        '-' => reference - delta,
        _ => unreachable!("signed_tail returns only + or -"),
    };
    Ok(PriceLevel {
        value: positive_price(name, value)?,
        requires_book: true,
    })
}

fn signed_tail(token: &str) -> Option<(char, &str)> {
    let sign = token.chars().next()?;
    matches!(sign, '+' | '-').then_some((sign, &token[sign.len_utf8()..]))
}

fn positive_price(name: &str, value: Decimal) -> Result<Decimal, String> {
    if value <= Decimal::ZERO {
        Err(format!("{name} must be positive"))
    } else {
        Ok(value.normalize())
    }
}

fn price_token(value: Decimal) -> String {
    value.normalize().to_string()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn trailing_trigger(
    state: &TradingState,
    symbol: &str,
    side: Side,
    distance: &str,
) -> Result<Decimal, String> {
    let book = state
        .book
        .get(symbol)
        .ok_or_else(|| "book unavailable for trailing stop".to_string())?
        .value;
    let mid = ((book.bid + book.ask) / Decimal::TWO).normalize();
    let distance = trailing_distance(mid, distance)?;
    let trigger = match side {
        Side::Ask => mid - distance,
        Side::Bid => mid + distance,
    }
    .normalize();
    if trigger <= Decimal::ZERO {
        return Err("trailing stop trigger must be positive".to_string());
    }
    Ok(trigger)
}

fn trailing_distance(reference: Decimal, raw: &str) -> Result<Decimal, String> {
    if let Some(pct) = raw.strip_suffix('%') {
        let pct = parse_decimal("trailing percent", pct.trim())?;
        if pct <= Decimal::ZERO {
            return Err("trailing percent must be positive".to_string());
        }
        Ok((reference * pct / Decimal::from(100)).normalize())
    } else {
        let distance = parse_decimal("trailing distance", raw)?;
        if distance <= Decimal::ZERO {
            return Err("trailing distance must be positive".to_string());
        }
        Ok(distance)
    }
}

fn chase_target(
    state: &TradingState,
    symbol: &str,
    side: Side,
    distance: &str,
) -> Result<Decimal, String> {
    let book = state
        .book
        .get(symbol)
        .ok_or_else(|| "book unavailable for chase".to_string())?
        .value;
    let mid = ((book.bid + book.ask) / Decimal::TWO).normalize();
    if distance.eq_ignore_ascii_case("quote") {
        return Ok(mid);
    }
    let distance = if let Some(pct) = distance.strip_suffix('%') {
        let pct = parse_decimal("chase percent", pct.trim())?;
        if pct <= Decimal::ZERO {
            return Err("chase percent must be positive".to_string());
        }
        (mid * pct / Decimal::from(100)).normalize()
    } else {
        let distance = parse_decimal("chase distance", distance.trim_end_matches('$').trim())?;
        if distance <= Decimal::ZERO {
            return Err("chase distance must be positive".to_string());
        }
        distance
    };
    let target = match side {
        Side::Bid => mid - distance,
        Side::Ask => mid + distance,
    }
    .normalize();
    if target <= Decimal::ZERO {
        return Err("chase target price must be positive".to_string());
    }
    Ok(target)
}

fn parse_decimal(name: &str, raw: &str) -> Result<Decimal, String> {
    raw.parse::<Decimal>()
        .map(|value| value.normalize())
        .map_err(|_| format!("invalid {name}: {raw}"))
}

#[derive(Debug, Clone, Copy)]
enum SizeInput {
    Base(Decimal),
    Quote(Decimal),
    Percent(Decimal),
    RiskUsd(Decimal),
    RiskPercent(Decimal),
}

impl SizeInput {
    fn is_risk(self) -> bool {
        matches!(self, Self::RiskUsd(_) | Self::RiskPercent(_))
    }
}

#[derive(Debug, Clone, Copy)]
struct RiskContext {
    side: Side,
    stop: Option<Decimal>,
    label: &'static str,
}

fn parse_size(raw: &str) -> Result<SizeInput, String> {
    let raw = raw.trim();
    if let Some(risk) = raw.strip_prefix('r').or_else(|| raw.strip_prefix('R')) {
        let Some(value) = risk.strip_suffix('$') else {
            let Some(value) = risk.strip_suffix('%') else {
                return Err("Invalid risk size: expected r<amount>$ or r<percent>%".to_string());
            };
            return parse_positive_decimal("risk percent", value.trim()).and_then(|percent| {
                if percent > Decimal::from(100) {
                    Err("Risk percent must be <= 100".to_string())
                } else {
                    Ok(SizeInput::RiskPercent(percent))
                }
            });
        };
        return parse_positive_decimal("risk size", value.trim()).map(SizeInput::RiskUsd);
    }
    match (raw.strip_prefix('$'), raw.strip_suffix('$')) {
        (Some(_), Some(_)) => Err(format!("invalid size: {raw}")),
        (Some(value), None) | (None, Some(value)) => {
            parse_positive_decimal("size", value.trim()).map(SizeInput::Quote)
        }
        (None, None) => {
            if let Some(value) = raw.strip_suffix('%') {
                parse_positive_decimal("size percent", value.trim()).map(SizeInput::Percent)
            } else {
                parse_positive_decimal("size", raw).map(SizeInput::Base)
            }
        }
    }
}

fn parse_positive_decimal(name: &str, raw: &str) -> Result<Decimal, String> {
    let value = parse_decimal(name, raw)?;
    if value <= Decimal::ZERO {
        Err(format!("{name} must be positive"))
    } else {
        Ok(value)
    }
}

fn margin_ntli(add: bool, raw: &str) -> Result<i64, String> {
    let amount = parse_margin_amount(raw)?;
    let scaled = (amount * Decimal::from(1_000_000)).normalize();
    if scaled.scale() != 0 {
        return Err("margin amount supports at most 6 decimals".to_string());
    }
    let ntli = scaled
        .to_i64()
        .ok_or_else(|| "margin amount too large".to_string())?;
    if add { Ok(ntli) } else { Ok(-ntli) }
}

fn parse_margin_amount(raw: &str) -> Result<Decimal, String> {
    let token = raw.trim();
    let lower = token.to_ascii_lowercase();
    let unit_markers = token.matches('$').count() + if lower.ends_with("usdc") { 1 } else { 0 };
    if unit_markers > 1 {
        return Err(format!("invalid margin amount: {raw}"));
    }
    let amount = if lower.ends_with("usdc") {
        &token[..token.len() - 4]
    } else if let Some(amount) = token.strip_prefix('$') {
        amount
    } else if let Some(amount) = token.strip_suffix('$') {
        amount
    } else {
        token
    };
    parse_positive_decimal("margin amount", amount.trim())
}

fn size_at_price_no_risk(
    state: &TradingState,
    market: &Market,
    side: Side,
    raw: &str,
    price: Decimal,
) -> Result<Decimal, String> {
    rounded_size_input_no_risk(state, market, side, parse_size(raw)?, price)
}

fn rounded_size_input_no_risk(
    state: &TradingState,
    market: &Market,
    side: Side,
    input: SizeInput,
    price: Decimal,
) -> Result<Decimal, String> {
    rounded_size_input(
        state,
        market,
        input,
        price,
        RiskContext {
            side,
            stop: None,
            label: "risk sizing is not supported on this command",
        },
    )
}

fn close_size_at_price(
    market: &Market,
    available: Decimal,
    raw: &str,
    price: Decimal,
) -> Result<Decimal, String> {
    let size = match parse_size(raw)? {
        SizeInput::Base(size) => size,
        SizeInput::Quote(quote) => {
            if price <= Decimal::ZERO {
                return Err("quote close size requires a positive reference price".to_string());
            }
            (quote / price).normalize()
        }
        SizeInput::Percent(percent) => (available * percent / Decimal::from(100)).normalize(),
        SizeInput::RiskUsd(_) | SizeInput::RiskPercent(_) => {
            return Err("risk sizing is not supported on close".to_string());
        }
    };
    rounded_size(market, size.min(available))
}

fn size_at_price(
    state: &TradingState,
    market: &Market,
    input: SizeInput,
    price: Decimal,
    risk: RiskContext,
) -> Result<Decimal, String> {
    rounded_size_input(state, market, input, price, risk)
}

fn rounded_size_input(
    state: &TradingState,
    market: &Market,
    input: SizeInput,
    price: Decimal,
    risk: RiskContext,
) -> Result<Decimal, String> {
    match input {
        SizeInput::Base(size) => rounded_size(market, size),
        SizeInput::Quote(quote) => {
            if price <= Decimal::ZERO {
                return Err("quote size requires a positive reference price".to_string());
            }
            rounded_size(market, (quote / price).normalize())
        }
        SizeInput::Percent(percent) => {
            if price <= Decimal::ZERO {
                return Err("percent size requires a positive reference price".to_string());
            }
            if market.kind == MarketKind::Spot {
                rounded_spot_percent_size(state, market, risk.side, percent, price, Decimal::ZERO)
            } else {
                rounded_size(
                    market,
                    (account_value(state)? * percent / Decimal::from(100) / price).normalize(),
                )
            }
        }
        SizeInput::RiskUsd(budget) => {
            require_perp_market(market, "risk sizing")?;
            rounded_risk_size(
                market,
                budget,
                price,
                risk.stop.ok_or(risk.label)?,
                risk.side,
            )
        }
        SizeInput::RiskPercent(percent) => {
            require_perp_market(market, "risk sizing")?;
            rounded_risk_size(
                market,
                (account_value(state)? * percent / Decimal::from(100)).normalize(),
                price,
                risk.stop.ok_or(risk.label)?,
                risk.side,
            )
        }
    }
}

fn account_value(state: &TradingState) -> Result<Decimal, String> {
    state
        .account_value_usd()
        .ok_or_else(|| "account value unavailable for percent sizing".to_string())
}

fn rounded_risk_size(
    market: &Market,
    budget: Decimal,
    entry: Decimal,
    stop: Decimal,
    side: Side,
) -> Result<Decimal, String> {
    let unit_risk = match side {
        Side::Bid => entry - stop,
        Side::Ask => stop - entry,
    };
    if unit_risk <= Decimal::ZERO {
        return Err(match side {
            Side::Bid => "Invalid stop: for buys, stop must be below entry",
            Side::Ask => "Invalid stop: for sells, stop must be above entry",
        }
        .to_string());
    }
    rounded_size(market, (budget / unit_risk).normalize())
}

fn scale_sizes(
    state: &TradingState,
    market: &Market,
    side: Side,
    input: SizeInput,
    prices: &[Decimal],
) -> Result<Vec<Decimal>, String> {
    let legs = Decimal::from(prices.len());
    match input {
        SizeInput::Base(size) => {
            let leg_size = rounded_size(market, (size / legs).normalize())?;
            Ok(vec![leg_size; prices.len()])
        }
        SizeInput::Quote(quote) => split_quote_scale(state, market, side, quote, prices),
        SizeInput::Percent(percent) if market.kind == MarketKind::Spot && side == Side::Ask => {
            let total = spot_capacity(state, market, side)? * percent / Decimal::from(100);
            let leg_size = rounded_size(market, (total / legs).normalize())?;
            Ok(vec![leg_size; prices.len()])
        }
        SizeInput::Percent(percent) => {
            let quote = if market.kind == MarketKind::Spot {
                spot_capacity(state, market, side)?
            } else {
                account_value(state)?
            };
            split_quote_scale(
                state,
                market,
                side,
                (quote * percent / Decimal::from(100)).normalize(),
                prices,
            )
        }
        SizeInput::RiskUsd(_) | SizeInput::RiskPercent(_) => {
            Err("scale risk sizing requires managed attached-protection orchestration".to_string())
        }
    }
}

fn split_quote_scale(
    state: &TradingState,
    market: &Market,
    side: Side,
    quote: Decimal,
    prices: &[Decimal],
) -> Result<Vec<Decimal>, String> {
    let legs = Decimal::from(prices.len());
    prices
        .iter()
        .map(|price| {
            rounded_size_input_no_risk(
                state,
                market,
                side,
                SizeInput::Quote((quote / legs).normalize()),
                *price,
            )
        })
        .collect()
}

fn spot_pair(market: &Market) -> Result<(&str, &str), String> {
    market
        .symbol
        .strip_prefix("SPOT:")
        .and_then(|pair| pair.split_once('/'))
        .ok_or_else(|| format!("invalid spot market symbol {}", market.symbol))
}

fn spot_capacity(state: &TradingState, market: &Market, side: Side) -> Result<Decimal, String> {
    let (base, quote) = spot_pair(market)?;
    let coin = if side == Side::Bid { quote } else { base };
    let balance = &state
        .spot_balance(coin)
        .ok_or_else(|| format!("{coin} spot balance unavailable"))?
        .value;
    let portfolio_margin = state
        .account_mode
        .is_some_and(|mode| mode.value == AccountMode::PortfolioMargin);
    let available = if side == Side::Bid {
        if portfolio_margin {
            let freshness = state
                .spot_capacity_ms
                .as_ref()
                .ok_or_else(|| "spot portfolio capacity unavailable".to_string())?;
            let age_ms = freshness.age_ms(now_ms());
            if age_ms > FreshnessLimits::default().account_ms {
                return Err(format!("spot portfolio capacity stale age_ms={age_ms}"));
            }
            balance.available_after_maintenance.ok_or_else(|| {
                format!("{coin} portfolio available-after-maintenance unavailable")
            })?
        } else {
            balance.available()
        }
    } else {
        balance.available()
    };
    Ok(available.max(Decimal::ZERO))
}

fn rounded_spot_percent_size(
    state: &TradingState,
    market: &Market,
    side: Side,
    percent: Decimal,
    price: Decimal,
    released: Decimal,
) -> Result<Decimal, String> {
    let capacity = spot_capacity(state, market, side)? + released;
    let size = if side == Side::Bid {
        capacity / price
    } else {
        capacity
    };
    rounded_size(market, (size * percent / Decimal::from(100)).normalize())
}

fn ensure_spot_capacity(
    state: &TradingState,
    market: &Market,
    side: Side,
    orders: impl IntoIterator<Item = (Decimal, Decimal)>,
) -> Result<(), String> {
    if market.kind != MarketKind::Spot {
        return Ok(());
    }
    let required = orders
        .into_iter()
        .map(|(size, price)| {
            if side == Side::Bid {
                size * price
            } else {
                size
            }
        })
        .sum::<Decimal>()
        .normalize();
    let available = spot_capacity(state, market, side)?;
    if required > available {
        let coin = if side == Side::Bid {
            spot_pair(market)?.1
        } else {
            spot_pair(market)?.0
        };
        return Err(format!(
            "insufficient {coin} spot capacity: required={required} available={available}"
        ));
    }
    Ok(())
}

pub fn resolve_scale_risk_size(
    state: &TradingState,
    scale: &Scale,
) -> Result<Option<String>, String> {
    resolve_scale_risk_size_for(state, &state.active, scale)
}

pub fn resolve_scale_risk_size_for(
    state: &TradingState,
    symbol: &str,
    scale: &Scale,
) -> Result<Option<String>, String> {
    let (scale, _) = resolve_scale_price_fields_for(state, symbol, scale.clone())?;
    let input = parse_size(&scale.size)?;
    if !input.is_risk() {
        return Ok(None);
    }
    if scale.reduce_only {
        return Err("Risk sizing is not supported with reduce-only".to_string());
    }
    if scale.trailing.is_some() {
        return Err("Risk sizing requires a fixed stop-loss (sl/stop), not trailing".to_string());
    }
    let market = market(state, symbol)?;
    let side = side(scale.side.clone());
    let start = rounded_price(
        market,
        side,
        parse_decimal("start price", &scale.start_price)?,
        true,
    )?;
    let end = rounded_price(
        market,
        side,
        parse_decimal("end price", &scale.end_price)?,
        true,
    )?;
    let stop = scale
        .stop_loss
        .as_deref()
        .ok_or("Risk sizing requires a stop-loss (sl/stop ...)")?;
    let stop = rounded_price(
        market,
        opposite(side),
        parse_decimal("stop loss", stop)?,
        true,
    )?;
    let entry_worst = match side {
        Side::Bid => start.max(end),
        Side::Ask => start.min(end),
    };
    let size = size_at_price(
        state,
        market,
        input,
        entry_worst,
        RiskContext {
            side,
            stop: Some(stop),
            label: "Risk sizing requires a stop-loss (sl/stop ...)",
        },
    )?;
    Ok(Some(size.to_string()))
}

fn next_cloid() -> Cloid {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX));
    let seq = u128::from(SEQ.fetch_add(1, Ordering::Relaxed));
    Cloid::from_u128_unchecked((now << 64) | seq)
}

fn rounded_size(market: &Market, size: Decimal) -> Result<Decimal, String> {
    Size(size)
        .truncate(market.size_decimals as u32)
        .map(|size| size.0)
        .map_err(|err| err.to_string())
}

fn rounded_price(
    market: &Market,
    side: Side,
    price: Decimal,
    conservative: bool,
) -> Result<Decimal, String> {
    market
        .tick()
        .round_for_side(side, price, conservative)
        .map(|price| price.normalize())
        .ok_or_else(|| format!("invalid price for {}", market.symbol))
}
