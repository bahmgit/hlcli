use hl_v2::{
    catalog::COMMAND_DOCS,
    command::{self, Command, ProtectionKind, Side, Tif},
};

fn variant(command: &Command) -> &'static str {
    match command {
        Command::Help { .. } => "help",
        Command::Status => "status",
        Command::Portfolio => "portfolio",
        Command::Balances => "balances",
        Command::Orders => "orders",
        Command::Position => "position",
        Command::RiskShow => "risk_show",
        Command::ConfigShow => "config_show",
        Command::ConfigGet { .. } => "config_get",
        Command::Doctor => "doctor",
        Command::Refresh => "refresh",
        Command::InstrumentShow => "instrument_show",
        Command::MarketList { .. } => "market_list",
        Command::InstrumentUse { .. } => "instrument_use",
        Command::AccountModeShow => "account_mode_show",
        Command::AccountModeRequire { .. } => "account_mode_require",
        Command::AccountModeSet { .. } => "account_mode_set",
        Command::CancelAll => "cancel_all",
        Command::CancelOid { .. } => "cancel_oid",
        Command::CancelCloid { .. } => "cancel_cloid",
        Command::MoveOid { .. } => "move_oid",
        Command::ResizeOid { .. } => "resize_oid",
        Command::BatchMoveOid { .. } => "batch_move_oid",
        Command::BatchResizeOid { .. } => "batch_resize_oid",
        Command::BatchMoveCloid { .. } => "batch_move_cloid",
        Command::BatchResizeCloid { .. } => "batch_resize_cloid",
        Command::Close { .. } => "close",
        Command::Leverage { .. } => "leverage",
        Command::IsolatedMargin { .. } => "isolated_margin",
        Command::Trade(_) => "trade",
        Command::Scale(_) => "scale",
        Command::ProtectionSet(_) => "protection_set",
        Command::ProtectionCancel { .. } => "protection_cancel",
        Command::ChasePlace(_) => "chase_place",
        Command::ChaseCancel => "chase_cancel",
        Command::BatchPlace(_) => "batch_place",
        Command::TwapPlace(_) => "twap_place",
        Command::TwapCancel { .. } => "twap_cancel",
        Command::Keybinds => "keybinds",
        Command::Bind { .. } => "bind",
        Command::Unbind { .. } => "unbind",
        Command::SetVar { .. } => "set_var",
        Command::PrintVar { .. } => "print_var",
        Command::UnsetVar { .. } => "unset_var",
        Command::Clear => "clear",
        Command::Quit => "quit",
        Command::Empty => "empty",
        Command::Reject { .. } => "reject",
        Command::Unknown { .. } => "unknown",
    }
}

fn family(command: &Command) -> &'static str {
    match command {
        Command::Help { .. } => "help",
        Command::Status | Command::Refresh => "status",
        Command::Portfolio | Command::Balances => "portfolio",
        Command::Orders | Command::Position => "orders/position",
        Command::MarketList { .. } | Command::InstrumentShow | Command::InstrumentUse { .. } => {
            "markets/instrument"
        }
        Command::Trade(_) => "buy/sell",
        Command::Scale(_) => "scale",
        Command::BatchPlace(_)
        | Command::BatchMoveOid { .. }
        | Command::BatchResizeOid { .. }
        | Command::BatchMoveCloid { .. }
        | Command::BatchResizeCloid { .. } => "batch",
        Command::ProtectionSet(_) | Command::ProtectionCancel { .. } => "protection",
        Command::CancelAll
        | Command::CancelOid { .. }
        | Command::CancelCloid { .. }
        | Command::MoveOid { .. }
        | Command::ResizeOid { .. } => "cancel/modify",
        Command::ChasePlace(_) | Command::ChaseCancel => "chase",
        Command::Close { .. } => "close",
        Command::TwapPlace(_) | Command::TwapCancel { .. } => "twap",
        Command::Leverage { .. } => "leverage",
        Command::IsolatedMargin { .. } => "isolated margin",
        Command::AccountModeShow
        | Command::AccountModeRequire { .. }
        | Command::AccountModeSet { .. } => "account mode",
        Command::RiskShow | Command::ConfigShow | Command::ConfigGet { .. } | Command::Doctor => {
            "risk/config"
        }
        Command::SetVar { .. } | Command::PrintVar { .. } | Command::UnsetVar { .. } => "variables",
        Command::Keybinds | Command::Bind { .. } | Command::Unbind { .. } => "keybinds",
        Command::Clear | Command::Quit => "session",
        Command::Empty => "empty",
        Command::Reject { .. } => "reject",
        Command::Unknown { .. } => "unknown",
    }
}

#[test]
fn every_catalogue_example_is_valid_grammar() {
    for doc in COMMAND_DOCS {
        for example in doc.examples {
            let parsed = command::parse(example);
            assert_eq!(
                family(&parsed),
                doc.name,
                "catalogue example maps to the wrong typed family: {example:?} -> {parsed:?}"
            );
        }
    }
}

#[test]
fn every_typed_command_variant_has_representative_grammar() {
    let cases = [
        ("help", "help"),
        ("status", "status"),
        ("portfolio", "portfolio"),
        ("balances", "balances"),
        ("orders", "orders"),
        ("position", "position"),
        ("risk", "risk_show"),
        ("config", "config_show"),
        ("config get market-cross-bps", "config_get"),
        ("doctor", "doctor"),
        ("refresh", "refresh"),
        ("instrument", "instrument_show"),
        ("markets", "market_list"),
        ("instrument BTC", "instrument_use"),
        ("account mode", "account_mode_show"),
        ("account mode require any", "account_mode_require"),
        ("account mode set unified", "account_mode_set"),
        ("cancel", "cancel_all"),
        ("cancel 1", "cancel_oid"),
        ("cancel cloid 0x1", "cancel_cloid"),
        ("move 1 to 2", "move_oid"),
        ("resize 1 to 2", "resize_oid"),
        ("batch move 1 to 2", "batch_move_oid"),
        ("batch resize 1 to 2", "batch_resize_oid"),
        ("batch move cloid 0x1 to 2", "batch_move_cloid"),
        ("batch resize cloid 0x1 to 2", "batch_resize_cloid"),
        ("close", "close"),
        ("leverage cross 1", "leverage"),
        ("margin add 1", "isolated_margin"),
        ("buy 1", "trade"),
        ("scale buy 1 into 1 from 1 to 1", "scale"),
        ("sl 1", "protection_set"),
        ("sl cancel", "protection_cancel"),
        ("chase buy 1 quote", "chase_place"),
        ("chase cancel", "chase_cancel"),
        ("batch buy 1@2", "batch_place"),
        ("twap buy 1 over 5", "twap_place"),
        ("twap cancel all", "twap_cancel"),
        ("keybinds", "keybinds"),
        ("bind k status", "bind"),
        ("unbind k", "unbind"),
        ("set @x 1", "set_var"),
        ("print", "print_var"),
        ("unset @x", "unset_var"),
        ("clear", "clear"),
        ("quit", "quit"),
        ("", "empty"),
        ("buy", "reject"),
        ("not-a-command", "unknown"),
    ];
    let mut seen = std::collections::BTreeSet::new();
    for (input, expected) in cases {
        let parsed = command::parse(input);
        assert_eq!(variant(&parsed), expected, "{input:?} -> {parsed:?}");
        assert!(seen.insert(expected), "duplicate variant case: {expected}");
    }
    assert_eq!(seen.len(), 49, "typed command inventory changed");
}

#[test]
fn every_documented_alias_maps_to_its_typed_family() {
    let cases = [
        ("help", "help"),
        ("h", "help"),
        ("status", "status"),
        ("st", "status"),
        ("refresh", "status"),
        ("r", "status"),
        ("portfolio", "portfolio"),
        ("pf", "portfolio"),
        ("balance", "portfolio"),
        ("balances", "portfolio"),
        ("bal", "portfolio"),
        ("assets", "portfolio"),
        ("borrow", "portfolio"),
        ("lend", "portfolio"),
        ("orders", "orders/position"),
        ("ord", "orders/position"),
        ("o", "orders/position"),
        ("position", "orders/position"),
        ("pos", "orders/position"),
        ("p", "orders/position"),
        ("markets", "markets/instrument"),
        ("market list", "markets/instrument"),
        ("instrument", "markets/instrument"),
        ("inst", "markets/instrument"),
        ("instr", "markets/instrument"),
        ("coin", "markets/instrument"),
        ("buy 1", "buy/sell"),
        ("sell 1", "buy/sell"),
        ("b 1", "buy/sell"),
        ("s 1", "buy/sell"),
        ("scale buy 1 into 1 from 1 to 1", "scale"),
        ("batch buy 1@1", "batch"),
        ("tp 2", "protection"),
        ("sl 1", "protection"),
        ("trail 1", "protection"),
        ("tsl 1", "protection"),
        ("cancel", "cancel/modify"),
        ("c 1", "cancel/modify"),
        ("move 1 to 2", "cancel/modify"),
        ("resize 1 to 2", "cancel/modify"),
        ("chase cancel", "chase"),
        ("close", "close"),
        ("twap cancel all", "twap"),
        ("leverage cross 1", "leverage"),
        ("lev iso 1", "leverage"),
        ("margin add 1", "isolated margin"),
        ("account mode", "account mode"),
        ("risk", "risk/config"),
        ("config", "risk/config"),
        ("doctor", "risk/config"),
        ("preflight", "risk/config"),
        ("setup check", "risk/config"),
        ("set @x 1", "variables"),
        ("print", "variables"),
        ("unset @x", "variables"),
        ("keybinds", "keybinds"),
        ("binds", "keybinds"),
        ("bind k status", "keybinds"),
        ("unbind k", "keybinds"),
        ("clear", "session"),
        ("cls", "session"),
        ("quit", "session"),
        ("exit", "session"),
        ("q", "session"),
    ];
    let expected_aliases = COMMAND_DOCS
        .iter()
        .map(|doc| doc.aliases.len())
        .sum::<usize>();
    assert_eq!(
        cases.len(),
        expected_aliases,
        "alias case table is incomplete"
    );
    for (input, expected) in cases {
        let parsed = command::parse(input);
        assert_eq!(
            family(&parsed),
            expected,
            "alias failed: {input:?} -> {parsed:?}"
        );
    }
}

#[test]
fn aliases_and_all_command_families_map_to_typed_intent() {
    let cases = [
        ("h trade", "help"),
        ("st", "status"),
        ("pf", "portfolio"),
        ("assets", "balances"),
        ("ord", "orders"),
        ("pos", "position"),
        ("preflight", "doctor"),
        ("r", "refresh"),
        ("market list hip-3", "markets"),
        ("coin eth", "instrument"),
        ("account mode require pm", "account-require"),
        ("b 0.01", "trade"),
        ("scale sell 1 into 2 from 2 to 3", "scale"),
        ("batch buy 1@2 2@1", "batch-place"),
        ("sl 1", "protection"),
        ("tsl cancel", "protection-cancel"),
        ("c 1,2", "cancel"),
        ("move 1 to 2", "move"),
        ("resize 1 to 2", "resize"),
        ("chase sell 1 quote", "chase"),
        ("close 50%", "close"),
        ("twap buy 1 over 5", "twap"),
        ("lev iso 3", "leverage"),
        ("margin rm 1", "margin"),
        ("set @x 1", "set"),
        ("print @x", "print"),
        ("unset @x", "unset"),
        ("bind k buy 1", "bind"),
        ("unbind k", "unbind"),
        ("cls", "clear"),
        ("q", "quit"),
    ];
    for (input, family) in cases {
        let parsed = command::parse(input);
        assert!(
            !matches!(parsed, Command::Unknown { .. } | Command::Reject { .. }),
            "{family} grammar failed for {input:?}: {parsed:?}"
        );
    }

    assert!(matches!(
        command::parse("buy 1 tif alo post reduce"),
        Command::Trade(hl_v2::command::Trade {
            side: Side::Buy,
            tif: Some(Tif::Alo),
            post_only: true,
            reduce_only: true,
            ..
        })
    ));
    assert!(matches!(
        command::parse("buy 1 post tif alo"),
        Command::Trade(_)
    ));
    assert!(matches!(
        command::parse("trail cancel"),
        Command::ProtectionCancel {
            kind: ProtectionKind::TrailingStop
        }
    ));
}

#[test]
fn ambiguous_or_removed_grammar_fails_closed() {
    for input in [
        "buy 1 reduce reduce",
        "buy 1 post post",
        "buy 1 tif ioc post",
        "buy 1 sl 10 sl 9",
        "buy 1 tp 10 tp 11",
        "buy 1 chase 1 chase 2",
        "scale buy 1 into 2 into 3 from 1 to 2",
        "batch buy 1@2 trail 1",
        "cancel cloid 0x1,,0x2",
        "leverage cross 0",
        "bind ü status",
        "unbind ü",
        "requests reserve 1",
        "rate-limit reserve 1",
    ] {
        assert!(
            matches!(command::parse(input), Command::Reject { .. }),
            "unsafe grammar accepted: {input}"
        );
    }
    assert!(matches!(
        command::parse("transfer spot to perp 1"),
        Command::Unknown { .. }
    ));
}
