use serde::{Deserialize, Serialize};

use crate::catalog;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    fn parse(raw: &str) -> Option<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "buy" | "b" => Some(Self::Buy),
            "sell" | "s" => Some(Self::Sell),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tif {
    Gtc,
    Ioc,
    Alo,
}

impl Tif {
    fn parse(raw: &str) -> Option<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "gtc" => Some(Self::Gtc),
            "ioc" => Some(Self::Ioc),
            "alo" | "post" | "postonly" | "post-only" => Some(Self::Alo),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketFilter {
    All,
    Perps,
    Hip3,
    Spot,
}

impl MarketFilter {
    fn parse(raw: Option<&str>) -> Option<Self> {
        match raw.unwrap_or("all").to_ascii_lowercase().as_str() {
            "all" => Some(Self::All),
            "perp" | "perps" => Some(Self::Perps),
            "hip3" | "hip-3" => Some(Self::Hip3),
            "spot" | "spots" => Some(Self::Spot),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Command {
    Help {
        topic: Option<String>,
    },
    Status,
    Portfolio,
    Balances,
    Orders,
    Position,
    RiskShow,
    ConfigShow,
    ConfigGet {
        key: String,
    },
    Doctor,
    Refresh,
    InstrumentShow,
    MarketList {
        filter: MarketFilter,
    },
    InstrumentUse {
        symbol: String,
    },
    AccountModeShow,
    AccountModeRequire {
        mode: Option<String>,
    },
    AccountModeSet {
        mode: String,
    },
    CancelAll,
    CancelOid {
        ids: Vec<u64>,
    },
    CancelCloid {
        ids: Vec<String>,
    },
    MoveOid {
        id: u64,
        price: String,
    },
    ResizeOid {
        id: u64,
        size: String,
    },
    BatchMoveOid {
        ids: Vec<u64>,
        price: String,
    },
    BatchResizeOid {
        ids: Vec<u64>,
        size: String,
    },
    BatchMoveCloid {
        ids: Vec<String>,
        price: String,
    },
    BatchResizeCloid {
        ids: Vec<String>,
        size: String,
    },
    Close {
        size: Option<String>,
        price: Option<String>,
    },
    Leverage {
        cross: bool,
        value: u32,
    },
    IsolatedMargin {
        add: bool,
        amount: String,
    },
    Trade(Trade),
    Scale(Scale),
    ProtectionSet(Protection),
    ProtectionCancel {
        kind: ProtectionKind,
    },
    ChasePlace(Chase),
    ChaseCancel,
    BatchPlace(BatchPlace),
    TwapPlace(TwapPlace),
    TwapCancel {
        id: Option<u64>,
    },
    Keybinds,
    Bind {
        key: String,
        action: String,
    },
    Unbind {
        key: String,
    },
    SetVar {
        name: String,
        value: String,
    },
    PrintVar {
        name: Option<String>,
    },
    UnsetVar {
        name: String,
    },
    Clear,
    Quit,
    Empty,
    Reject {
        message: String,
    },
    Unknown {
        input: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub side: Side,
    pub size: String,
    pub price: Option<String>,
    pub reduce_only: bool,
    pub stop_loss: Option<String>,
    pub take_profit: Option<String>,
    pub trailing: Option<String>,
    pub chase: Option<String>,
    pub tif: Option<Tif>,
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scale {
    pub side: Side,
    pub size: String,
    pub legs: u16,
    pub start_price: String,
    pub end_price: String,
    pub reduce_only: bool,
    pub stop_loss: Option<String>,
    pub take_profit: Option<String>,
    pub trailing: Option<String>,
    pub tif: Option<Tif>,
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtectionKind {
    StopLoss,
    TakeProfit,
    TrailingStop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Protection {
    pub kind: ProtectionKind,
    pub value: String,
    pub size: Option<String>,
    #[serde(default)]
    pub activation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chase {
    pub side: Side,
    pub size: String,
    pub distance: String,
    pub reduce_only: bool,
    pub tif: Option<Tif>,
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchPlace {
    pub side: Side,
    pub orders: Vec<BatchLeg>,
    pub reduce_only: bool,
    pub tif: Option<Tif>,
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchLeg {
    pub size: String,
    pub price: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TwapPlace {
    #[serde(default)]
    pub trigger: Option<(bool, String)>,
    #[serde(default)]
    pub stop: Option<String>,
    pub side: Side,
    pub size: String,
    pub minutes: u64,
    pub reduce_only: bool,
    pub randomize: bool,
}

pub fn parse(input: &str) -> Command {
    let input = input.trim();
    if input.is_empty() {
        return Command::Empty;
    }
    let words = match tokenize(input) {
        Ok(words) => words,
        Err(message) => return Command::Reject { message },
    };
    match parse_words(input, &words) {
        Command::Reject { message } => Command::Reject {
            message: catalog::usage_with_note(input, &message).unwrap_or(message),
        },
        command => command,
    }
}

pub fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote = false;
    for ch in input.chars() {
        match ch {
            '"' => quote = !quote,
            ch if ch.is_whitespace() && !quote => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(ch),
        }
    }
    if quote {
        return Err("unterminated quote".to_string());
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    Ok(out)
}

fn parse_words(input: &str, words: &[String]) -> Command {
    let lower = words
        .iter()
        .map(|word| word.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if matches!(lower.first().map(String::as_str), Some("help" | "h")) {
        return Command::Help {
            topic: (words.len() > 1).then(|| words[1..].join(" ")),
        };
    }
    match lower.as_slice() {
        [one] => match one.as_str() {
            "status" | "st" => return Command::Status,
            "portfolio" | "pf" => return Command::Portfolio,
            "balance" | "balances" | "bal" | "assets" | "borrow" | "lend" => {
                return Command::Balances;
            }
            "orders" | "ord" | "o" => return Command::Orders,
            "position" | "pos" | "p" => return Command::Position,
            "risk" => return Command::RiskShow,
            "config" => return Command::ConfigShow,
            "doctor" | "preflight" => return Command::Doctor,
            "refresh" | "r" => return Command::Refresh,
            "instrument" | "inst" | "instr" | "coin" => return Command::InstrumentShow,
            "markets" => {
                return Command::MarketList {
                    filter: MarketFilter::All,
                };
            }
            "cancel" | "c" => return Command::CancelAll,
            "close" => {
                return Command::Close {
                    size: None,
                    price: None,
                };
            }
            "keybinds" | "binds" => return Command::Keybinds,
            "quit" | "exit" | "q" => return Command::Quit,
            "clear" | "cls" => return Command::Clear,
            _ => {}
        },
        [first, second] => match (first.as_str(), second.as_str()) {
            ("risk", "show") => return Command::RiskShow,
            ("config", "show" | "list") => return Command::ConfigShow,
            ("setup", "check") => return Command::Doctor,
            ("account", "mode") => return Command::AccountModeShow,
            ("chase", "cancel") => return Command::ChaseCancel,
            ("tp", "cancel") => {
                return Command::ProtectionCancel {
                    kind: ProtectionKind::TakeProfit,
                };
            }
            ("sl", "cancel") => {
                return Command::ProtectionCancel {
                    kind: ProtectionKind::StopLoss,
                };
            }
            ("trail" | "tsl", "cancel") => {
                return Command::ProtectionCancel {
                    kind: ProtectionKind::TrailingStop,
                };
            }
            _ => {}
        },
        _ => {}
    }

    match lower.first().map(String::as_str) {
        Some("buy" | "sell" | "b" | "s") => parse_trade(words),
        Some("scale") => parse_scale(words),
        Some("batch") => parse_batch(words),
        Some("chase") => parse_chase(words),
        Some("twap") => parse_twap(words),
        Some("cancel" | "c") => parse_cancel(words),
        Some("move") => parse_move(words),
        Some("resize") => parse_resize(words),
        Some("close") => parse_close(words),
        Some("tp" | "sl" | "trail" | "tsl") => parse_protection(words),
        Some("leverage" | "lev") => parse_leverage(words),
        Some("margin") => parse_margin(words),
        Some("risk") => parse_risk(words),
        Some("config") => parse_config(words),
        Some("requests" | "rate-limit") => reject(
            "request-weight reserve requires a funded main user signer and is unavailable in API-wallet-only hld",
        ),
        Some("account") => parse_account(words),
        Some("instrument" | "inst" | "instr" | "coin") => parse_instrument(words),
        Some("markets" | "market") => parse_market_list(words),
        Some("set") => parse_set_var(words),
        Some("print") => parse_print_var(words),
        Some("unset") => parse_unset_var(words),
        Some("bind") => parse_bind(words),
        Some("unbind") => parse_unbind(words),
        _ => Command::Unknown {
            input: input.to_string(),
        },
    }
}

fn parse_trade(words: &[String]) -> Command {
    let Some(side) = Side::parse(&words[0]) else {
        return reject("trade side must be buy or sell");
    };
    if words.len() < 2 {
        return reject("trade requires size");
    }
    let size = words[1].clone();
    let mut mods = Mods::default();
    let mut i = 2;
    while i < words.len() {
        match words[i].to_ascii_lowercase().as_str() {
            "at" => {
                let Some(price) = words.get(i + 1) else {
                    return reject("at requires price");
                };
                if mods.price.replace(price.clone()).is_some() {
                    return reject("duplicate price");
                }
                i += 2;
            }
            token => match parse_mod(&mut mods, token, words, &mut i) {
                ModResult::Done => {}
                ModResult::Unknown => return reject(format!("unknown trade token '{}'", words[i])),
                ModResult::Reject(message) => return reject(message),
            },
        }
    }
    Command::Trade(Trade {
        side,
        size,
        price: mods.price,
        reduce_only: mods.reduce_only,
        stop_loss: mods.stop_loss,
        take_profit: mods.take_profit,
        trailing: mods.trailing,
        chase: mods.chase,
        tif: mods.tif,
        post_only: mods.post_only,
    })
}

fn parse_scale(words: &[String]) -> Command {
    if words.len() < 9 {
        return reject("scale requires side, size, legs, start, and end");
    }
    let Some(side) = Side::parse(&words[1]) else {
        return reject("scale side must be buy or sell");
    };
    let size = words[2].clone();
    let mut mods = Mods::default();
    let mut legs: Option<u16> = None;
    let mut start_price: Option<String> = None;
    let mut end_price: Option<String> = None;
    let mut i = 3;
    while i < words.len() {
        match words[i].to_ascii_lowercase().as_str() {
            "into" => {
                let Some(raw) = words.get(i + 1) else {
                    return reject("into requires leg count");
                };
                let Some(parsed) = parse_u16(raw) else {
                    return reject("invalid scale leg count");
                };
                if legs.replace(parsed).is_some() {
                    return reject("duplicate scale leg count");
                }
                i += 2;
            }
            "from" => {
                let Some(raw) = words.get(i + 1) else {
                    return reject("from requires start price");
                };
                if start_price.replace(raw.clone()).is_some() {
                    return reject("duplicate scale start price");
                }
                i += 2;
            }
            "to" => {
                let Some(raw) = words.get(i + 1) else {
                    return reject("to requires end price");
                };
                if end_price.replace(raw.clone()).is_some() {
                    return reject("duplicate scale end price");
                }
                i += 2;
            }
            token => match parse_mod(&mut mods, token, words, &mut i) {
                ModResult::Done => {}
                ModResult::Unknown => return reject(format!("unknown scale token '{}'", words[i])),
                ModResult::Reject(message) => return reject(message),
            },
        }
    }
    let Some(legs) = legs else {
        return reject("scale requires into <legs>");
    };
    let Some(start_price) = start_price else {
        return reject("scale requires from <price>");
    };
    let Some(end_price) = end_price else {
        return reject("scale requires to <price>");
    };
    if mods.chase.is_some() {
        return reject("scale does not accept chase");
    }
    Command::Scale(Scale {
        side,
        size,
        legs,
        start_price,
        end_price,
        reduce_only: mods.reduce_only,
        stop_loss: mods.stop_loss,
        take_profit: mods.take_profit,
        trailing: mods.trailing,
        tif: mods.tif,
        post_only: mods.post_only,
    })
}

fn parse_batch(words: &[String]) -> Command {
    if words.len() >= 6 && eq(words, 2, "cloid") && eq(words, 1, "move") {
        return parse_batch_adjust(words, true, true);
    }
    if words.len() >= 6 && eq(words, 2, "cloid") && eq(words, 1, "resize") {
        return parse_batch_adjust(words, false, true);
    }
    if words.len() >= 5 && eq(words, 1, "move") {
        return parse_batch_adjust(words, true, false);
    }
    if words.len() >= 5 && eq(words, 1, "resize") {
        return parse_batch_adjust(words, false, false);
    }
    if words.len() < 3 {
        return reject("batch requires side and at least one size@price leg");
    }
    let Some(side) = Side::parse(&words[1]) else {
        return reject("batch side must be buy or sell");
    };
    let mut mods = Mods::default();
    let mut legs = Vec::new();
    let mut i = 2;
    while i < words.len() {
        let token = words[i].to_ascii_lowercase();
        match parse_mod(&mut mods, &token, words, &mut i) {
            ModResult::Done => continue,
            ModResult::Unknown => {}
            ModResult::Reject(message) => return reject(message),
        }
        let Some((size, price)) = split_once(&words[i], '@') else {
            return reject(format!("invalid batch leg '{}'", words[i]));
        };
        legs.push(BatchLeg { size, price });
        i += 1;
    }
    if legs.is_empty() {
        return reject("batch requires at least one size@price leg");
    }
    if mods.stop_loss.is_some()
        || mods.take_profit.is_some()
        || mods.trailing.is_some()
        || mods.chase.is_some()
    {
        return reject("batch place accepts only TIF, post-only, and reduce-only modifiers");
    }
    Command::BatchPlace(BatchPlace {
        side,
        orders: legs,
        reduce_only: mods.reduce_only,
        tif: mods.tif,
        post_only: mods.post_only,
    })
}

fn parse_batch_adjust(words: &[String], is_move: bool, by_cloid: bool) -> Command {
    let id_index = if by_cloid { 3 } else { 2 };
    let Some(ids) = words.get(id_index) else {
        return reject("batch adjustment requires ids");
    };
    if !eq(words, id_index + 1, "to") {
        return reject("batch adjustment requires to <value>");
    }
    let Some(value) = words.get(id_index + 2) else {
        return reject("batch adjustment requires value");
    };
    if words.len() != id_index + 3 {
        return reject("batch adjustment has unexpected extra tokens");
    }
    if by_cloid {
        let Some(ids) = parse_csv_strings(ids) else {
            return reject("invalid batch cloid list");
        };
        return if is_move {
            Command::BatchMoveCloid {
                ids,
                price: value.clone(),
            }
        } else {
            Command::BatchResizeCloid {
                ids,
                size: value.clone(),
            }
        };
    }
    let Some(ids) = parse_u64_csv(ids) else {
        return reject("invalid batch oid list");
    };
    if is_move {
        Command::BatchMoveOid {
            ids,
            price: value.clone(),
        }
    } else {
        Command::BatchResizeOid {
            ids,
            size: value.clone(),
        }
    }
}

fn parse_chase(words: &[String]) -> Command {
    if words.len() == 2 && eq(words, 1, "cancel") {
        return Command::ChaseCancel;
    }
    if words.len() < 4 {
        return reject("chase requires side, size, and distance");
    }
    let Some(side) = Side::parse(&words[1]) else {
        return reject("chase side must be buy or sell");
    };
    let mut mods = Mods::default();
    let size = words[2].clone();
    let distance = words[3].clone();
    let mut i = 4;
    while i < words.len() {
        let token = words[i].to_ascii_lowercase();
        match parse_mod(&mut mods, &token, words, &mut i) {
            ModResult::Done => {}
            ModResult::Unknown => return reject(format!("unknown chase token '{}'", words[i])),
            ModResult::Reject(message) => return reject(message),
        }
    }
    if mods.stop_loss.is_some()
        || mods.take_profit.is_some()
        || mods.trailing.is_some()
        || mods.chase.is_some()
    {
        return reject("chase entry accepts no chase or protection modifiers");
    }
    Command::ChasePlace(Chase {
        side,
        size,
        distance,
        reduce_only: mods.reduce_only,
        tif: mods.tif,
        post_only: mods.post_only,
    })
}

fn parse_twap(words: &[String]) -> Command {
    if words.len() == 3 && eq(words, 1, "cancel") {
        return if words[2] == "*" || eq(words, 2, "all") {
            Command::TwapCancel { id: None }
        } else if let Some(id) = parse_u64(&words[2]) {
            Command::TwapCancel { id: Some(id) }
        } else {
            reject("invalid twap id")
        };
    }
    if words.len() < 5 {
        return reject("twap requires side, size, over, and minutes");
    }
    let Some(side) = Side::parse(&words[1]) else {
        return reject("twap side must be buy or sell");
    };
    if !eq(words, 3, "over") {
        return reject("twap requires over <minutes>");
    }
    let Some(minutes) = parse_u64(&words[4]) else {
        return reject("invalid twap minutes");
    };
    let mut reduce_only = false;
    let mut randomize = false;
    let mut trigger = None;
    let mut stop = None;
    let mut i = 5;
    while i < words.len() {
        match words[i].to_ascii_lowercase().as_str() {
            "reduce" | "reduce-only" if !reduce_only => reduce_only = true,
            "randomize" | "randomise" if !randomize => randomize = true,
            "trigger" => {
                if trigger.is_some() {
                    return reject("duplicate TWAP trigger");
                }
                let above = if eq(words, i + 1, "above") {
                    true
                } else if eq(words, i + 1, "below") {
                    false
                } else {
                    return reject("TWAP trigger requires above|below <price>");
                };
                let Some(price) = words.get(i + 2) else {
                    return reject("TWAP trigger requires price");
                };
                trigger = Some((above, price.clone()));
                i += 2;
            }
            "max" | "min" => {
                if (eq(words, i, "max") && side != Side::Buy)
                    || (eq(words, i, "min") && side != Side::Sell)
                {
                    return reject("buy TWAP accepts max; sell TWAP accepts min");
                }
                let Some(price) = words.get(i + 1) else {
                    return reject("TWAP stop requires price");
                };
                if stop.replace(price.clone()).is_some() {
                    return reject("duplicate TWAP stop");
                }
                i += 1;
            }
            other => return reject(format!("unknown or duplicate twap token '{other}'")),
        }
        i += 1;
    }
    Command::TwapPlace(TwapPlace {
        trigger,
        stop,
        side,
        size: words[2].clone(),
        minutes,
        reduce_only,
        randomize,
    })
}

fn parse_cancel(words: &[String]) -> Command {
    if words.len() < 2 {
        return Command::CancelAll;
    }
    if eq(words, 1, "cloid") {
        let Some(raw) = words.get(2) else {
            return reject("cancel cloid requires id");
        };
        if words.len() != 3 {
            return reject("cancel cloid has unexpected extra tokens");
        }
        let Some(ids) = parse_csv_strings(raw) else {
            return reject("invalid cloid list");
        };
        return Command::CancelCloid { ids };
    }
    if words.len() != 2 {
        return reject("cancel expects oid list or cloid list");
    }
    let Some(ids) = parse_u64_csv(&words[1]) else {
        return reject("invalid cancel oid list");
    };
    Command::CancelOid { ids }
}

fn parse_move(words: &[String]) -> Command {
    if words.len() != 4 || !eq(words, 2, "to") {
        return reject("move requires <oid> to <price>");
    }
    let Some(id) = parse_u64(&words[1]) else {
        return reject("invalid order id");
    };
    Command::MoveOid {
        id,
        price: words[3].clone(),
    }
}

fn parse_resize(words: &[String]) -> Command {
    if words.len() != 4 || !eq(words, 2, "to") {
        return reject("resize requires <oid> to <size>");
    }
    let Some(id) = parse_u64(&words[1]) else {
        return reject("invalid order id");
    };
    Command::ResizeOid {
        id,
        size: words[3].clone(),
    }
}

fn parse_close(words: &[String]) -> Command {
    match words.len() {
        1 => Command::Close {
            size: None,
            price: None,
        },
        2 => Command::Close {
            size: Some(words[1].clone()),
            price: None,
        },
        4 if eq(words, 2, "at") => Command::Close {
            size: Some(words[1].clone()),
            price: Some(words[3].clone()),
        },
        _ => reject("close accepts no args, <size>, or <size> at <price>"),
    }
}

fn parse_protection(words: &[String]) -> Command {
    let kind = match words[0].to_ascii_lowercase().as_str() {
        "tp" => ProtectionKind::TakeProfit,
        "sl" => ProtectionKind::StopLoss,
        "trail" | "tsl" => ProtectionKind::TrailingStop,
        _ => return reject("unknown protection command"),
    };
    if words.len() >= 2 && eq(words, 1, "cancel") {
        return if words.len() == 2 {
            Command::ProtectionCancel { kind }
        } else {
            reject("protection cancel does not accept extra tokens")
        };
    }
    let value_index = if words.len() >= 2 && (eq(words, 1, "set") || eq(words, 1, "modify")) {
        2
    } else {
        1
    };
    let Some(value) = words.get(value_index) else {
        return reject("protection command requires value");
    };
    let mut size = None;
    let mut activation = None;
    let mut i = value_index + 1;
    while i < words.len() {
        let Some(raw) = words.get(i + 1) else {
            return reject("protection modifier requires value");
        };
        if eq(words, i, "size") {
            if size.replace(raw.clone()).is_some() {
                return reject("duplicate protection size");
            }
        } else if eq(words, i, "activate") && kind == ProtectionKind::TrailingStop {
            if activation.replace(raw.clone()).is_some() {
                return reject("duplicate trailing activation");
            }
        } else {
            return reject(format!("unknown protection token '{}'", words[i]));
        }
        i += 2;
    }
    Command::ProtectionSet(Protection {
        kind,
        value: value.clone(),
        size,
        activation,
    })
}

fn parse_leverage(words: &[String]) -> Command {
    if words.len() != 3 {
        return reject("leverage requires cross|isolated and value");
    }
    let cross = match words[1].to_ascii_lowercase().as_str() {
        "cross" => true,
        "isolated" | "iso" => false,
        _ => return reject("leverage mode must be cross or isolated"),
    };
    let Some(value) = parse_u32(&words[2]) else {
        return reject("invalid leverage value");
    };
    if value == 0 {
        return reject("leverage must be positive");
    }
    Command::Leverage { cross, value }
}

fn parse_margin(words: &[String]) -> Command {
    if words.len() != 3 {
        return reject("margin requires add|remove and amount");
    }
    let add = match words[1].to_ascii_lowercase().as_str() {
        "add" => true,
        "remove" | "rm" => false,
        _ => return reject("margin side must be add or remove"),
    };
    Command::IsolatedMargin {
        add,
        amount: words[2].clone(),
    }
}

fn parse_risk(words: &[String]) -> Command {
    match words {
        [one] if eq_one(one, "risk") => Command::RiskShow,
        [one, two] if eq_one(one, "risk") && eq_one(two, "show") => Command::RiskShow,
        _ => reject("risk supports show"),
    }
}

fn parse_config(words: &[String]) -> Command {
    match words {
        [one] if eq_one(one, "config") => Command::ConfigShow,
        [one, two] if eq_one(one, "config") && (eq_one(two, "show") || eq_one(two, "list")) => {
            Command::ConfigShow
        }
        [one, two, key] if eq_one(one, "config") && eq_one(two, "get") => Command::ConfigGet {
            key: key.to_ascii_lowercase(),
        },
        _ => reject("config supports show and get <key>"),
    }
}

fn parse_account(words: &[String]) -> Command {
    match words {
        [one, two] if eq_one(one, "account") && eq_one(two, "mode") => Command::AccountModeShow,
        [one, two, three, mode]
            if eq_one(one, "account") && eq_one(two, "mode") && eq_one(three, "require") =>
        {
            Command::AccountModeRequire {
                mode: if eq_one(mode, "any") {
                    None
                } else {
                    Some(mode.clone())
                },
            }
        }
        [one, two, three, mode]
            if eq_one(one, "account") && eq_one(two, "mode") && eq_one(three, "set") =>
        {
            Command::AccountModeSet { mode: mode.clone() }
        }
        _ => reject("account mode supports show, require <mode>, and set <mode>"),
    }
}

fn parse_instrument(words: &[String]) -> Command {
    if words.len() == 1 {
        return Command::InstrumentShow;
    }
    if eq(words, 1, "list") {
        return match MarketFilter::parse(words.get(2).map(String::as_str)) {
            Some(filter) if words.len() <= 3 => Command::MarketList { filter },
            _ => reject("instrument list expects all, perps, hip3, or spot"),
        };
    }
    if (eq(words, 1, "use") || eq(words, 1, "switch")) && words.len() == 3 {
        return Command::InstrumentUse {
            symbol: canonical_symbol(&words[2]),
        };
    }
    if words.len() == 2 {
        return Command::InstrumentUse {
            symbol: canonical_symbol(&words[1]),
        };
    }
    reject("instrument supports show, list, and use <symbol>")
}

fn parse_market_list(words: &[String]) -> Command {
    match words {
        [one] if eq_one(one, "markets") => Command::MarketList {
            filter: MarketFilter::All,
        },
        [one, two] if eq_one(one, "markets") => match MarketFilter::parse(Some(two)) {
            Some(filter) => Command::MarketList { filter },
            None => reject("unknown market filter"),
        },
        [one, two] if eq_one(one, "market") && eq_one(two, "list") => Command::MarketList {
            filter: MarketFilter::All,
        },
        [one, two, three] if eq_one(one, "market") && eq_one(two, "list") => {
            match MarketFilter::parse(Some(three)) {
                Some(filter) => Command::MarketList { filter },
                None => reject("unknown market filter"),
            }
        }
        _ => reject("markets expects optional filter"),
    }
}

fn parse_set_var(words: &[String]) -> Command {
    if words.len() < 3 {
        return reject("set requires @name and value");
    }
    if !words[1].starts_with('@') {
        return reject("variable names must start with @");
    }
    Command::SetVar {
        name: words[1].clone(),
        value: words[2..].join(" "),
    }
}

fn parse_print_var(words: &[String]) -> Command {
    match words {
        [_] => Command::PrintVar { name: None },
        [_, name] if name.starts_with('@') => Command::PrintVar {
            name: Some(name.clone()),
        },
        _ => reject("print accepts optional @name"),
    }
}

fn parse_unset_var(words: &[String]) -> Command {
    match words {
        [_, name] if name.starts_with('@') => Command::UnsetVar { name: name.clone() },
        _ => reject("unset requires @name"),
    }
}

fn parse_bind(words: &[String]) -> Command {
    if words.len() < 3 {
        return reject("bind requires key and action");
    }
    if !valid_key(&words[1]) {
        return reject("bind key must be one printable ASCII character");
    }
    Command::Bind {
        key: words[1].clone(),
        action: words[2..].join(" "),
    }
}

fn parse_unbind(words: &[String]) -> Command {
    match words {
        [_, key] if valid_key(key) => Command::Unbind { key: key.clone() },
        _ => reject("unbind requires one printable ASCII character"),
    }
}

fn valid_key(key: &str) -> bool {
    key.len() == 1 && key.as_bytes()[0].is_ascii_graphic()
}

#[derive(Default)]
struct Mods {
    price: Option<String>,
    reduce_only: bool,
    stop_loss: Option<String>,
    take_profit: Option<String>,
    trailing: Option<String>,
    chase: Option<String>,
    tif: Option<Tif>,
    post_only: bool,
    reduce_seen: bool,
    post_seen: bool,
}

enum ModResult {
    Done,
    Unknown,
    Reject(String),
}

fn parse_mod(mods: &mut Mods, token: &str, words: &[String], i: &mut usize) -> ModResult {
    match token {
        "reduce" | "reduce-only" => {
            if mods.reduce_seen {
                return ModResult::Reject("duplicate reduce-only modifier".to_string());
            }
            mods.reduce_seen = true;
            mods.reduce_only = true;
            *i += 1;
        }
        "post" | "post-only" | "postonly" => {
            if mods.post_seen {
                return ModResult::Reject("duplicate post-only modifier".to_string());
            }
            if mods.tif.as_ref().is_some_and(|tif| *tif != Tif::Alo) {
                return ModResult::Reject("post-only conflicts with non-ALO TIF".to_string());
            }
            mods.post_seen = true;
            mods.post_only = true;
            mods.tif.get_or_insert(Tif::Alo);
            *i += 1;
        }
        "tif" => {
            let Some(raw) = words.get(*i + 1) else {
                return ModResult::Unknown;
            };
            let Some(tif) = Tif::parse(raw) else {
                return ModResult::Unknown;
            };
            if mods.tif.is_some()
                && !(mods.post_seen && mods.tif == Some(Tif::Alo) && tif == Tif::Alo)
            {
                return ModResult::Reject("duplicate TIF modifier".to_string());
            }
            if mods.post_only && tif != Tif::Alo {
                return ModResult::Reject("post-only conflicts with non-ALO TIF".to_string());
            }
            mods.post_only |= tif == Tif::Alo;
            mods.tif = Some(tif);
            *i += 2;
        }
        "sl" | "stop" | "stop-loss" => {
            let Some(value) = words.get(*i + 1) else {
                return ModResult::Unknown;
            };
            if mods.stop_loss.replace(value.clone()).is_some() {
                return ModResult::Reject("duplicate stop-loss modifier".to_string());
            }
            *i += 2;
        }
        "tp" | "take-profit" => {
            let Some(value) = words.get(*i + 1) else {
                return ModResult::Unknown;
            };
            if mods.take_profit.replace(value.clone()).is_some() {
                return ModResult::Reject("duplicate take-profit modifier".to_string());
            }
            *i += 2;
        }
        "trail" | "tsl" | "trailing" => {
            let Some(value) = words.get(*i + 1) else {
                return ModResult::Unknown;
            };
            if mods.trailing.replace(value.clone()).is_some() {
                return ModResult::Reject("duplicate trailing modifier".to_string());
            }
            *i += 2;
        }
        "chase" => {
            let Some(value) = words.get(*i + 1) else {
                return ModResult::Unknown;
            };
            if mods.chase.replace(value.clone()).is_some() {
                return ModResult::Reject("duplicate chase modifier".to_string());
            }
            *i += 2;
        }
        _ => return ModResult::Unknown,
    }
    ModResult::Done
}

fn reject(message: impl Into<String>) -> Command {
    Command::Reject {
        message: message.into(),
    }
}

fn eq(words: &[String], index: usize, value: &str) -> bool {
    words
        .get(index)
        .is_some_and(|word| word.eq_ignore_ascii_case(value))
}

fn eq_one(word: &str, value: &str) -> bool {
    word.eq_ignore_ascii_case(value)
}

fn parse_u64(raw: &str) -> Option<u64> {
    raw.parse::<u64>().ok()
}

fn parse_u32(raw: &str) -> Option<u32> {
    raw.parse::<u32>().ok()
}

fn parse_u16(raw: &str) -> Option<u16> {
    raw.parse::<u16>().ok().filter(|value| *value > 0)
}

fn parse_u64_csv(raw: &str) -> Option<Vec<u64>> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        if part.is_empty() {
            return None;
        }
        out.push(parse_u64(part)?);
    }
    (!out.is_empty()).then_some(out)
}

fn parse_csv_strings(raw: &str) -> Option<Vec<String>> {
    let ids = raw.split(',').map(ToOwned::to_owned).collect::<Vec<_>>();
    (!ids.is_empty() && ids.iter().all(|id| !id.is_empty())).then_some(ids)
}

fn split_once(raw: &str, needle: char) -> Option<(String, String)> {
    let (left, right) = raw.split_once(needle)?;
    if left.is_empty() || right.is_empty() {
        return None;
    }
    Some((left.to_string(), right.to_string()))
}

fn canonical_symbol(raw: &str) -> String {
    raw.to_ascii_uppercase()
}
