//! Reader lifetime, cache semantics, and observability through real synchronous engine reads.

use std::sync::Arc;

use rstest::rstest;

use super::super::snapshot_crc::SnapshotCrc;
use crate::crc::{DomainMetadataState, SetTransactionState};
use crate::engine::sync::SyncEngine;
use crate::engine::KernelEngine;
use crate::metrics::MetricEvent;
use crate::unit_test_utils::{
    install_thread_local_metrics_reporter, CapturingReporter, TestCancellationToken,
};
use crate::{CancellationTokenRef, KernelError, Snapshot};

/// Independent readers neither inherit a completed build token nor alter another reader's scope.
#[test]
fn reader_scopes_are_independent_and_release_their_tokens() {
    let engine = SyncEngine::new();
    let build_token = Arc::new(TestCancellationToken::default());
    let snapshot = Snapshot::builder_for("tests/data/app-txn-no-checkpoint")
        .with_cancellation_token(build_token.clone() as CancellationTokenRef)
        .build(&engine)
        .unwrap();
    assert_eq!(Arc::strong_count(&build_token), 1);
    build_token.cancel();

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let weak = Arc::downgrade(&token);
    let cancelled = snapshot.reader().with_cancellation_token(token);
    let independent =
        snapshot.reader().with_cancellation_token(
            Arc::new(TestCancellationToken::default()) as CancellationTokenRef
        );
    assert!(matches!(
        cancelled.get_app_id_version("my-app", &engine),
        Err(KernelError::Cancelled)
    ));
    assert_eq!(
        independent.get_app_id_version("my-app", &engine).unwrap(),
        Some(3)
    );
    assert_eq!(
        snapshot
            .reader()
            .get_app_id_version("my-app", &engine)
            .unwrap(),
        Some(3)
    );
    let unscoped = cancelled.with_cancellation_token(None);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        unscoped.get_app_id_version("my-app", &engine).unwrap(),
        Some(3)
    );

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::default());
    let weak = Arc::downgrade(&token);
    let reader = snapshot.reader().with_cancellation_token(token);
    reader.get_app_id_version("my-app", &engine).unwrap();
    drop(reader);
    assert!(weak.upgrade().is_none());
}

/// An unscoped reader preserves cancellation already supplied by the original engine.
#[test]
fn unscoped_reader_preserves_engine_cancellation() {
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for("tests/data/app-txn-no-checkpoint")
        .build(&engine)
        .unwrap();
    let engine = KernelEngine::new(&engine, Arc::new(TestCancellationToken::cancelled()));
    assert!(matches!(
        snapshot.reader().get_app_id_version("my-app", &engine),
        Err(KernelError::Cancelled)
    ));
}

/// Complete CRC misses and all cache hits avoid I/O; partial misses still honor cancellation.
#[rstest]
fn reader_crc_cache_respects_completeness(
    #[values(false, true)] partial: bool,
    #[values(false, true)] hit: bool,
) {
    let engine = SyncEngine::new();
    let mut snapshot = Snapshot::builder_for("tests/data/crc-full")
        .build(&engine)
        .unwrap();
    if partial {
        let mut crc = snapshot.crc_at_version().unwrap().as_ref().clone();
        let DomainMetadataState::Complete(domains) = crc.domain_metadata_state else {
            panic!("fixture must have complete domains");
        };
        let SetTransactionState::Complete(transactions) = crc.set_transaction_state else {
            panic!("fixture must have complete transactions");
        };
        crc.domain_metadata_state = DomainMetadataState::Partial(domains);
        crc.set_transaction_state = SetTransactionState::Partial(transactions);
        Arc::get_mut(&mut snapshot).unwrap().crc =
            SnapshotCrc::try_new(Some(Arc::new(crc)), 0, None).unwrap();
    }
    let reader = snapshot.reader().with_cancellation_token(Arc::new(
        TestCancellationToken::cancelled(),
    ) as CancellationTokenRef);
    let domain =
        reader.get_domain_metadata(if hit { "myApp.metadata" } else { "missing" }, &engine);
    let transaction =
        reader.get_app_id_version(if hit { "spark-app-1" } else { "missing" }, &engine);
    if partial && !hit {
        assert!(matches!(domain, Err(KernelError::Cancelled)));
        assert!(matches!(transaction, Err(KernelError::Cancelled)));
    } else {
        assert_eq!(domain.unwrap(), hit.then(|| r#"{"key":"value"}"#.into()));
        assert_eq!(transaction.unwrap(), hit.then_some(42));
        assert_eq!(
            reader.get_row_tracking_high_water_mark(&engine).unwrap(),
            Some(9)
        );
    }
}

/// In-memory timestamp and feature-absent clustering reads do not reject a cancelled token.
#[test]
fn reader_no_io_getters_can_succeed_after_cancellation() {
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for("tests/data/table-without-dv-small")
        .build(&engine)
        .unwrap();
    let expected = snapshot.reader().get_timestamp(&engine).unwrap();
    let reader = snapshot.reader().with_cancellation_token(Arc::new(
        TestCancellationToken::cancelled(),
    ) as CancellationTokenRef);
    assert_eq!(reader.get_timestamp(&engine).unwrap(), expected);
    assert_eq!(reader.get_in_commit_timestamp(&engine).unwrap(), None);
    assert!(reader
        .get_clustering_column_infos(&engine)
        .unwrap()
        .is_none());
    assert_eq!(
        reader.get_physical_clustering_columns(&engine).unwrap(),
        None
    );
}

/// Reader delegation preserves metadata success/failure metrics and validation errors.
#[rstest]
fn reader_preserves_metadata_metrics_and_errors(#[values(false, true)] cancelled: bool) {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for("tests/data/app-txn-no-checkpoint")
        .with_correlation_id("reader-metadata")
        .build(&engine)
        .unwrap();
    let token = Arc::new(TestCancellationToken::default());
    if cancelled {
        token.cancel();
    }
    let reader = snapshot
        .reader()
        .with_cancellation_token(token as CancellationTokenRef);
    let domain = reader.get_domain_metadata("missing", &engine);
    let txn = reader.get_app_id_version("my-app", &engine);
    assert_eq!(domain.is_err(), cancelled);
    assert_eq!(txn.is_err(), cancelled);
    let error = reader
        .get_domain_metadata("delta.reserved", &engine)
        .unwrap_err();
    assert!(error.to_string().contains("system-controlled"));

    let events = reporter.events();
    let mut metadata_events = 0;
    for event in &events {
        let failed = match event {
            MetricEvent::DomainMetadataLoadSuccess(e) => {
                assert!(!e.from_cache);
                assert_eq!(e.num_domains_returned, 0);
                false
            }
            MetricEvent::SetTransactionLoadSuccess(e) => {
                assert!(!e.from_cache);
                assert!(e.found);
                false
            }
            MetricEvent::DomainMetadataLoadFailure | MetricEvent::SetTransactionLoadFailure => true,
            _ => continue,
        };
        assert_eq!(failed, cancelled);
        metadata_events += 1;
    }
    assert_eq!(metadata_events, 2);
}
