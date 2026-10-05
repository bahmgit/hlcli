#[derive(Debug, Clone, Copy)]
pub struct CommandDoc {
    pub group: &'static str,
    pub name: &'static str,
    pub summary: &'static str,
    pub aliases: &'static [&'static str],
    pub examples: &'static [&'static str],
    pub notes: &'static [&'static str],
    pub usage_tokens: &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
pub struct CommandGroup {
    pub key: &'static str,
    pub title: &'static str,
    pub summary: &'static str,
}

pub const COMMAND_GROUPS: &[CommandGroup] = &[
    CommandGroup {
        key: "read",
        title: "read",
        summary: "state, balances, markets, and refresh",
    },
    CommandGroup {
        key: "trade",
        title: "trade",
        summary: "entries, exits, TWAP, scale, and batch",
    },
    CommandGroup {
        key: "manage",
        title: "manage",
        summary: "orders, protection, and chase",
    },
    CommandGroup {
        key: "account",
        title: "account",
        summary: "instrument, leverage, isolated margin, and account mode",
    },
    CommandGroup {
        key: "shell",
        title: "shell",
        summary: "variables, keybinds, help, and session controls",
    },
];

pub const COMMAND_DOCS: &[CommandDoc] = &[
    CommandDoc {
        group: "shell",
        name: "help",
        summary: "show this command catalogue",
        aliases: &["help", "h"],
        examples: &["help", "help trade", "help twap", "help all"],
        notes: &["default help shows categories; help all shows the full catalogue"],
        usage_tokens: &["help", "h"],
    },
    CommandDoc {
        group: "read",
        name: "status",
        summary: "show active market, readiness, orders, TWAP IDs, and position",
        aliases: &["status", "st", "refresh", "r"],
        examples: &["status", "refresh"],
        notes: &["status includes active TWAP IDs; cancel them with `twap cancel <id|all>`"],
        usage_tokens: &["status", "st", "refresh", "r"],
    },
    CommandDoc {
        group: "read",
        name: "portfolio",
        summary: "show account value, margin, balances, borrow/lend, and positions",
        aliases: &[
            "portfolio",
            "pf",
            "balance",
            "balances",
            "bal",
            "assets",
            "borrow",
            "lend",
        ],
        examples: &["portfolio", "balances", "bal"],
        notes: &["spot balances are shown here, not as positions"],
        usage_tokens: &[
            "portfolio",
            "pf",
            "balance",
            "balances",
            "bal",
            "assets",
            "borrow",
            "lend",
        ],
    },
    CommandDoc {
        group: "read",
        name: "orders/position",
        summary: "show open orders or open perp positions",
        aliases: &["orders", "ord", "o", "position", "pos", "p"],
        examples: &["orders", "position"],
        notes: &[
            "orders and position are all-scope by default and market-scoped under `hl --market`",
        ],
        usage_tokens: &["orders", "ord", "o", "position", "pos", "p"],
    },
    CommandDoc {
        group: "account",
        name: "markets/instrument",
        summary: "list markets or switch the active trading instrument",
        aliases: &["markets", "market", "instrument", "inst", "instr", "coin"],
        examples: &[
            "markets",
            "markets hip3",
            "markets spot",
            "instrument",
            "instr ETH",
            "instrument use ETH",
            "instrument use BUILDER:ASSET",
        ],
        notes: &[
            "market filters: all, perps, hip3, spot",
            "configured allow-list aliases are accepted by instrument switching and shown in markets",
            "active instrument scopes trading, close, TP/SL, chase, TWAP, and default cancel commands",
            "launch independent asset terminals with `hl --market <symbol-or-alias>`; all share one daemon execution queue",
            "instrument switching is blocked only by active backend-managed work on the current instrument",
        ],
        usage_tokens: &["markets", "market", "instrument", "inst", "instr", "coin"],
    },
    CommandDoc {
        group: "trade",
        name: "buy/sell",
        summary: "buy or sell the active market with strict modifiers",
        aliases: &["buy", "sell", "b", "s"],
        examples: &[
            "buy 0.01",
            "sell 100$ at 50500 reduce",
            "buy 0.01 at 50500 tif alo post",
            "buy 5% sl 49000 tp 52000",
            "buy 0.01 chase quote tp 52000",
            "buy r100$ sl 49000",
            "buy r0.5% sl 49000",
        ],
        notes: &[
            "size tokens include base size, quote notional like 100$, percent like 5%, variables, and risk size like r100$",
            "price levels accept absolutes, signed offsets like -100, signed percents like -0.5%, and $entry when an entry is known",
            "risk sizing requires a stop loss",
            "chase <distance> enables managed quote-following entry; distance can be quote, absolute, or percent",
        ],
        usage_tokens: &["buy", "sell", "b", "s"],
    },
    CommandDoc {
        group: "trade",
        name: "scale",
        summary: "split a size across limit orders from one price to another",
        aliases: &["scale"],
        examples: &[
            "scale buy 0.05 into 5 from 40000 to 39500",
            "scale sell 0.1 reduce into 10 from 42000 to 44000",
            "scale sell 0.1 into 10 from 42000 to 44000 tif alo post",
            "scale buy 100000$ into 10 from -0.01% to -1%",
            "scale buy r100$ sl 48000 into 5 from 50000 to 49000",
        ],
        notes: &[
            "from/to/TP/SL price levels use the same absolute, signed-offset, signed-percent, and $entry syntax as buy/sell",
            "attached TP/SL/trailing are rejected with reduce-only scale orders",
        ],
        usage_tokens: &["scale"],
    },
    CommandDoc {
        group: "trade",
        name: "batch",
        summary: "place, move, or resize multiple orders",
        aliases: &["batch"],
        examples: &[
            "batch buy 0.01@40000 0.02@39500 tif gtc",
            "batch sell 0.01@41000 0.01@41500 reduce",
            "batch move 1,2,3 to 40500",
            "batch resize 1,2,3 to 0.005",
            "batch move cloid 0xabc,0xdef to 40500",
            "batch resize cloid 0xabc,0xdef to 0.005",
        ],
        notes: &["any invalid batch item rejects the full batch"],
        usage_tokens: &["batch"],
    },
    CommandDoc {
        group: "manage",
        name: "protection",
        summary: "set or cancel position take-profit, stop-loss, or trailing stop",
        aliases: &["tp", "sl", "trail", "tsl"],
        examples: &[
            "tp 43000",
            "sl 39500",
            "tp cancel",
            "sl cancel",
            "trail 150",
            "trail set 0.4%",
            "trail 120 size 0.005",
            "trail 1% size 50% activate 52000",
            "trail cancel",
        ],
        notes: &[
            "native trailing uses exchange mark-price watermarks and continues offline; generic move/resize is rejected",
            "attached trailing adds independent native protection per owned fill increment",
        ],
        usage_tokens: &["tp", "sl", "trail", "tsl"],
    },
    CommandDoc {
        group: "manage",
        name: "cancel/modify",
        summary: "cancel non-protection limits, move, or resize existing orders",
        aliases: &["cancel", "c", "move", "resize"],
        examples: &[
            "cancel",
            "c 123",
            "c 1,2,3",
            "cancel cloid 0x1234",
            "move 123 to 40500",
            "resize 123 to 0.005",
        ],
        notes: &[
            "IDs are Hyperliquid order IDs",
            "plain cancel skips TP/SL/trailing protection; use tp cancel, sl cancel, or trail cancel",
        ],
        usage_tokens: &["cancel", "c", "move", "resize"],
    },
    CommandDoc {
        group: "manage",
        name: "chase",
        summary: "submit or cancel a managed quote-following limit order",
        aliases: &["chase"],
        examples: &[
            "chase buy 0.01 quote",
            "chase buy 0.01 50",
            "chase sell 0.02 0.2%",
            "chase buy 100$ quote tif alo post",
            "chase cancel",
        ],
        notes: &["start a replacement chase with explicit `chase cancel` first"],
        usage_tokens: &["chase"],
    },
    CommandDoc {
        group: "trade",
        name: "close",
        summary: "close the active position fully or partially",
        aliases: &["close"],
        examples: &["close", "close 0.01", "close 50% at 42000"],
        notes: &[
            "close submits a reduce-only IOC for perps, or sells available base inventory for spot",
            "use an explicit reduce-only buy/sell limit for a resting exit",
        ],
        usage_tokens: &["close"],
    },
    CommandDoc {
        group: "trade",
        name: "twap",
        summary: "submit or cancel exchange TWAP orders",
        aliases: &["twap"],
        examples: &[
            "twap buy 0.1 over 30",
            "twap sell 0.2 over 10 reduce randomize",
            "twap buy 0.1 over 30 trigger above 50000 max 51000",
            "twap sell 0.1 over 30 trigger below 50000 min 49000",
            "twap cancel 12",
            "twap cancel all",
        ],
        notes: &[
            "use `status` to see active TWAP IDs and waiting/running conditions",
            "trigger direction is explicit; buy accepts max, sell accepts min; Hyperliquid owns activation and stopping",
        ],
        usage_tokens: &["twap"],
    },
    CommandDoc {
        group: "account",
        name: "leverage",
        summary: "set active-market leverage mode and value",
        aliases: &["leverage", "lev"],
        examples: &["leverage cross 5", "lev iso 3"],
        notes: &[],
        usage_tokens: &["leverage", "lev"],
    },
    CommandDoc {
        group: "account",
        name: "isolated margin",
        summary: "add or remove USDC margin on the active isolated position",
        aliases: &["margin"],
        examples: &["margin add 100", "margin remove 25", "margin rm $10"],
        notes: &["requires an active isolated/no-cross perp position"],
        usage_tokens: &["margin"],
    },
    CommandDoc {
        group: "account",
        name: "account mode",
        summary: "show, require, or change account abstraction mode",
        aliases: &["account mode"],
        examples: &[
            "account mode",
            "account mode require standard",
            "account mode require portfolioMargin",
            "account mode require any",
            "account mode set unified",
        ],
        notes: &[
            "require sets a session guard; trading fails closed unless live mode matches",
            "an API wallet can set unified or portfolio mode only from standard; returning or changing an abstracted account requires the main user signer",
        ],
        usage_tokens: &["account"],
    },
    CommandDoc {
        group: "read",
        name: "risk/config",
        summary: "inspect runtime policy and config",
        aliases: &["risk", "config", "doctor", "preflight", "setup check"],
        examples: &[
            "risk",
            "config",
            "config get market-cross-bps",
            "doctor",
            "preflight",
            "setup check",
        ],
        notes: &["risk and config are read-only"],
        usage_tokens: &["risk", "config", "doctor", "preflight", "setup"],
    },
    CommandDoc {
        group: "shell",
        name: "variables",
        summary: "set, print, and unset shell variables",
        aliases: &["set", "print", "unset"],
        examples: &["set @size 0.01", "print @size", "print", "unset @size"],
        notes: &["variables expand before parser dispatch for normal commands"],
        usage_tokens: &["set", "print", "unset"],
    },
    CommandDoc {
        group: "shell",
        name: "keybinds",
        summary: "list and manage one-character shell key bindings",
        aliases: &["keybinds", "binds", "bind", "unbind"],
        examples: &["keybinds", "bind k buy 0.01", "unbind k"],
        notes: &[],
        usage_tokens: &["keybinds", "binds", "bind", "unbind"],
    },
    CommandDoc {
        group: "shell",
        name: "session",
        summary: "clear shell output or quit the shell",
        aliases: &["clear", "cls", "quit", "exit", "q"],
        examples: &["clear", "quit"],
        notes: &[],
        usage_tokens: &["clear", "cls", "quit", "exit", "q"],
    },
];

pub fn command_names() -> Vec<&'static str> {
    COMMAND_DOCS.iter().map(|doc| doc.name).collect()
}

pub fn help_lines(topic: Option<&str>) -> Vec<String> {
    let Some(topic) = topic.map(str::trim).filter(|topic| !topic.is_empty()) else {
        return overview_lines();
    };
    if topic.eq_ignore_ascii_case("all") {
        return catalogue_lines(COMMAND_DOCS.iter().collect());
    }
    if let Some(group) = group_for_topic(topic) {
        return group_lines(group);
    }
    if let Some(doc) = doc_for_topic(topic) {
        return doc_lines(doc);
    }
    let mut lines = overview_lines();
    lines.insert(0, format!("unknown help topic '{topic}'"));
    lines
}

pub fn usage_with_note(input: &str, note: &str) -> Option<String> {
    let doc = doc_for_input(input)?;
    let mut lines = vec![note.to_string(), format!("Usage: {}", doc.name)];
    lines.extend(doc.examples.iter().map(|example| format!("  {example}")));
    Some(lines.join("\n"))
}

fn overview_lines() -> Vec<String> {
    let mut lines = vec![
        "Hyperliquid operator commands".to_string(),
        "Use `help <category|command>` for details, or `help all` for every command.".to_string(),
        "Chains: cmd1; sleep 2s; cmd2. Chains stop on failure.".to_string(),
        String::new(),
        "Categories:".to_string(),
    ];
    lines.extend(COMMAND_GROUPS.iter().map(|group| {
        let names = docs_in_group(group.key)
            .iter()
            .map(|doc| doc.name)
            .collect::<Vec<_>>()
            .join(", ");
        format!("  {:<8} {:<52} {}", group.key, names, group.summary)
    }));
    lines.push(String::new());
    lines.push("Examples: help trade | help twap | help variables | help all".to_string());
    lines
}

fn group_lines(group: &CommandGroup) -> Vec<String> {
    let docs = docs_in_group(group.key);
    let mut lines = vec![format!("{}: {}", group.title, group.summary), String::new()];
    lines.extend(
        docs.iter()
            .map(|doc| format!("  {:<16} {}", doc.name, doc.summary)),
    );
    lines.push(String::new());
    lines.push(format!(
        "Try: {}",
        docs.iter()
            .flat_map(|doc| doc.usage_tokens.first())
            .take(5)
            .map(|topic| format!("help {topic}"))
            .collect::<Vec<_>>()
            .join(" | ")
    ));
    lines
}

fn catalogue_lines(docs: Vec<&CommandDoc>) -> Vec<String> {
    let mut lines = vec![
        "Hyperliquid operator commands".to_string(),
        "Chains: cmd1; sleep 2s; cmd2. Chains stop on failure.".to_string(),
        String::new(),
    ];
    lines.extend(
        docs.into_iter()
            .flat_map(|doc| doc_lines(doc).into_iter().chain([String::new()])),
    );
    lines.pop();
    lines
}

fn doc_lines(doc: &CommandDoc) -> Vec<String> {
    let mut lines = vec![format!("{}: {}", doc.name, doc.summary)];
    if !doc.aliases.is_empty() {
        lines.push(format!("  aliases: {}", doc.aliases.join(", ")));
    }
    if !doc.examples.is_empty() {
        lines.push("  examples:".to_string());
        lines.extend(doc.examples.iter().map(|example| format!("    {example}")));
    }
    lines.extend(doc.notes.iter().map(|note| format!("  note: {note}")));
    lines
}

fn docs_in_group(group: &str) -> Vec<&'static CommandDoc> {
    COMMAND_DOCS
        .iter()
        .filter(|doc| doc.group == group)
        .collect()
}

fn group_for_topic(topic: &str) -> Option<&'static CommandGroup> {
    COMMAND_GROUPS
        .iter()
        .find(|group| group.key.eq_ignore_ascii_case(topic))
}

fn doc_for_topic(topic: &str) -> Option<&'static CommandDoc> {
    COMMAND_DOCS.iter().find(|doc| {
        doc.name.eq_ignore_ascii_case(topic)
            || doc
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(topic))
            || doc
                .usage_tokens
                .iter()
                .any(|token| token.eq_ignore_ascii_case(topic))
    })
}

fn doc_for_input(input: &str) -> Option<&'static CommandDoc> {
    let first = input.split_whitespace().next()?;
    COMMAND_DOCS.iter().find(|doc| {
        doc.usage_tokens
            .iter()
            .any(|token| first.eq_ignore_ascii_case(token))
    })
}
