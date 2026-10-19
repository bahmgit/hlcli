//! Explicit read-only acceptance against public exchange APIs; never signs or submits actions.
use hl_v2::{exchange::Network, info::InfoClient};
use std::{collections::BTreeSet, time::Instant};

#[tokio::test]
#[ignore = "live public network acceptance: run explicitly"]
async fn complete_universe_and_order_snapshots_on_both_networks() {
    for network in [Network::Mainnet, Network::Testnet] {
        let start = Instant::now();
        let state = InfoClient::new(network)
            .seed_state(
                "BTC",
                &BTreeSet::new(),
                Some("0x0000000000000000000000000000000000000000"),
            )
            .await
            .unwrap();
        assert!(state.markets.contains_key("BTC"));
        println!(
            "{network:?}: {} markets, {} builder DEXes; complete initialization in {:?}",
            state.markets.len(),
            state
                .markets
                .values()
                .filter_map(|m| m.dex.as_ref())
                .collect::<BTreeSet<_>>()
                .len(),
            start.elapsed()
        );
    }
}
