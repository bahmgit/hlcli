mod support;

use std::{collections::BTreeMap, fs, path::PathBuf};

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use hl_v2::{
    config::Config,
    exchange::Network,
    metrics::Metrics,
    protocol::{
        Action, BatchOrder, Chain, Cloid, OrderGrouping, OrderRequest, OrderType, TimeInForce,
    },
    security::{Credentials, load_credentials, store_credentials},
    server::{AppState, router},
    state::{AccountMode, BalanceSummary},
};
use tower::ServiceExt;

use support::{dec, now_ms, ready_perp};

fn temp_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("hl-v2-{label}-{}-{}", std::process::id(), now_ms()))
}

fn signing_action() -> Action {
    Action::Order(BatchOrder {
        grouping: OrderGrouping::Na,
        orders: vec![OrderRequest {
            asset: 0,
            is_buy: true,
            price: dec("50000.12"),
            size: dec("0.01"),
            reduce_only: false,
            order_type: OrderType::Limit {
                tif: TimeInForce::Gtc,
            },
            cloid: Cloid::from_uuid_text("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
        }],
    })
}

#[test]
fn l1_hash_signature_and_envelope_remain_wire_stable() {
    let action = signing_action();
    let nonce = 1_700_000_000_123_u64;
    let expires_after = 1_700_000_010_123_u64;
    let vault: Address = "0x1111111111111111111111111111111111111111"
        .parse()
        .unwrap();
    assert_eq!(
        action
            .hash(nonce, Some(vault), Some(expires_after))
            .unwrap()
            .to_string(),
        "0x2302ed30802276d1a804896cd67395c2561ee9f53d6822fd363a9e5337a697d1"
    );
    // Public deterministic fixture, never an account key.
    let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x01)).unwrap();
    let signed = action
        .sign(
            &signer,
            nonce,
            Some(vault),
            Some(expires_after),
            Chain::Mainnet,
        )
        .unwrap();
    assert_eq!(
        signed.signature.to_string(),
        "0xf68911f9c4f24e0e3ec2e87d911f7563fb4fd58268503d51b1ae5c59a13097c9249cb66ad992e3af8d341fff23b60572dbaa2281858d9626aed149fcf6a42fe21b"
    );
    assert_eq!(signed.vault_address, Some(vault));
    assert_eq!(signed.expires_after, Some(expires_after));
}

#[test]
fn config_is_strict_and_remote_bind_requires_both_opt_ins() {
    let dir = temp_dir("config");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("hl-v2.json"),
        r#"{
          "network":"Mainnet", "defaultSymbol":"btc", "allowedSymbols":["btc"],
          "symbolAliases":{"x":"btc"}, "bind":"0.0.0.0:8080", "backendToken":"secret"
        }"#,
    )
    .unwrap();
    assert!(Config::load(Some(dir.clone())).is_err());

    let text = fs::read_to_string(dir.join("hl-v2.json")).unwrap();
    fs::write(
        dir.join("hl-v2.json"),
        text.replace(
            "\"backendToken\":\"secret\"",
            "\"backendToken\":\"secret\", \"allowRemote\":true",
        ),
    )
    .unwrap();
    let config = Config::load(Some(dir.clone())).unwrap();
    assert_eq!(config.network, Network::Mainnet);
    assert_eq!(
        config.symbol_aliases,
        BTreeMap::from([("X".to_string(), "BTC".to_string())])
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn credential_profile_round_trip_is_encrypted_and_permission_restricted() {
    let dir = temp_dir("credentials");
    let credentials = Credentials {
        main_wallet: "0x1111111111111111111111111111111111111111".to_string(),
        api_private_key: "private-key-material".to_string(),
        network: "Mainnet".to_string(),
    };
    store_credentials(&dir, "Trader_A", b"correct password", &credentials).unwrap();

    let profile_dir = dir.join("wallets/trader_a");
    let blob = fs::read(profile_dir.join("credentials.bin")).unwrap();
    assert!(
        !blob
            .windows(credentials.api_private_key.len())
            .any(|window| { window == credentials.api_private_key.as_bytes() })
    );
    assert!(load_credentials(&dir, Some("trader_a"), b"wrong password").is_err());
    let loaded = load_credentials(&dir, None, b"correct password").unwrap();
    assert_eq!(loaded.main_wallet, credentials.main_wallet);
    assert_eq!(loaded.api_private_key, credentials.api_private_key);
    assert_eq!(loaded.network, credentials.network);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        assert_eq!(
            fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(profile_dir.join("credentials.bin"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn http_auth_and_portfolio_use_the_same_account_source_as_cli_sizing() {
    let mut state = ready_perp("0");
    let now = now_ms();
    state.apply_account_mode(AccountMode::UnifiedAccount, now, Some(now));
    state.apply_spot_balances(
        Vec::new(),
        BalanceSummary {
            portfolio_margin_enabled: false,
            portfolio_margin_ratio: None,
            spot_value_usd: Some(dec("2222")),
            spot_available_usd: Some(dec("2000")),
            spot_unpriced_count: 0,
            borrow_lend_health: None,
            borrow_lend_health_factor: None,
        },
        now,
        Some(now),
    );
    let mut config = Config::defaults(temp_dir("server"));
    config.backend_token = Some("secret".to_string());
    let app = router(AppState::new(state, Metrics::default(), &config));

    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/portfolio")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/portfolio")
                .header("x-hl-v2-token", "secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["account"]["source"], "spotClearinghouseState");
    assert_eq!(json["account"]["valueUsd"], "2222");
}
