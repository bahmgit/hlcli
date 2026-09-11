mod support;

use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use hl_v2::{
    exchange::{ExchangeResponse, OkResponse},
    execution::{
        ActionJournal, ExecutionKernel, ExecutionStatus, JournalPhase, OrderStatus, TransportError,
    },
    protocol::{
        Action, AssetId, BatchOrder, Cloid, OrderGrouping, OrderRequest, OrderType, TimeInForce,
        UpdateIsolatedMarginAction,
    },
};

use support::{dec, now_ms};

fn temp_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hl-v2-{label}-{}-{}.jsonl",
        std::process::id(),
        now_ms()
    ))
}

#[tokio::test]
async fn journal_pending_is_durable_before_transport_runs() {
    let path = temp_path("journal-order");
    let journal = ActionJournal::new(&path);
    let kernel = ExecutionKernel::new(journal.clone());
    let observed = Arc::new(AtomicBool::new(false));
    let observed_in_post = observed.clone();
    let journal_in_post = journal.clone();

    let receipt = kernel
        .submit("order", "BTC", "trade", move |_| async move {
            let records = journal_in_post.read_all().unwrap();
            observed_in_post.store(
                records.len() == 1 && records[0].phase == JournalPhase::Pending,
                Ordering::Relaxed,
            );
            Ok(vec![OrderStatus::Success])
        })
        .await;
    assert!(observed.load(Ordering::Relaxed));
    assert_eq!(receipt.status, ExecutionStatus::Accepted);
    assert_eq!(
        journal
            .read_all()
            .unwrap()
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![JournalPhase::Pending, JournalPhase::Accepted]
    );
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn journal_failure_prevents_any_transport_side_effect() {
    let parent_file = temp_path("not-a-directory");
    fs::write(&parent_file, b"file").unwrap();
    let kernel = ExecutionKernel::new(ActionJournal::new(parent_file.join("journal.jsonl")));
    let posted = Arc::new(AtomicBool::new(false));
    let posted_in_call = posted.clone();
    let receipt = kernel
        .submit("order", "BTC", "trade", move |_| async move {
            posted_in_call.store(true, Ordering::Relaxed);
            Ok(vec![OrderStatus::Success])
        })
        .await;
    assert_eq!(receipt.status, ExecutionStatus::Rejected);
    assert!(receipt.error.unwrap().contains("journal_before_post"));
    assert!(!posted.load(Ordering::Relaxed));
    fs::remove_file(parent_file).unwrap();
}

#[tokio::test]
async fn post_send_timeout_is_ambiguous_and_never_reported_as_rejection() {
    let path = temp_path("ambiguous");
    let journal = ActionJournal::new(&path);
    let kernel = ExecutionKernel::new(journal.clone());
    let receipt = kernel
        .submit("order", "BTC", "trade", |id| async move {
            Err(TransportError::Timeout { id, timeout_ms: 10 })
        })
        .await;
    assert_eq!(receipt.status, ExecutionStatus::Ambiguous);
    assert_eq!(
        journal.read_all().unwrap()[1].phase,
        JournalPhase::Ambiguous
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn exchange_response_type_mismatch_is_an_error_not_fabricated_success() {
    let action = Action::Order(BatchOrder {
        grouping: OrderGrouping::Na,
        orders: vec![OrderRequest {
            asset: AssetId::native_perp(0).0,
            is_buy: true,
            price: dec("50000"),
            size: dec("0.01"),
            reduce_only: false,
            order_type: OrderType::Limit {
                tif: TimeInForce::Gtc,
            },
            cloid: Cloid::from_u128(1).unwrap(),
        }],
    });
    let error = ExchangeResponse::Ok(OkResponse::Default)
        .statuses_for(&action)
        .unwrap_err();
    assert!(error.to_string().contains("does not match action"));

    let margin = Action::UpdateIsolatedMargin(UpdateIsolatedMarginAction {
        asset: AssetId::native_perp(0).0,
        is_buy: true,
        ntli: 100_000,
    });
    assert_eq!(
        ExchangeResponse::Ok(OkResponse::Default)
            .statuses_for(&margin)
            .unwrap(),
        [OrderStatus::Success]
    );
}
