//! Snapshot cancellation, CRC fallback, and unscoped read regressions.

use std::sync::Arc;

use rstest::rstest;
use tempfile::TempDir;
use url::Url;

use super::IncrementalReplay;
use crate::engine::sync::SyncEngine;
use crate::metrics::{LogSegmentLoadType, MetricEvent, SnapshotLoadType};
use crate::unit_test_utils::recording_reads::{Read, RecordingReads};
use crate::unit_test_utils::{
    CapturingReporter, copy_test_table, install_thread_local_metrics_reporter,
};
use crate::{CancellationTokenRef, KernelError, Snapshot};

#[rstest]
#[case::json("table-without-dv-small", Read::Json)]
#[case::crc("crc-full", Read::Crc)]
#[case::parquet("parquet_row_group_skipping", Read::Parquet)]
fn snapshot_without_token_passes_none_to_read_handlers(#[case] table: &str, #[case] read: Read) {
    let path = format!("tests/data/{table}");
    let mut reads = RecordingReads::new(None);
    Arc::get_mut(&mut reads).unwrap().expected_token = None;
    let actual = Snapshot::builder_for(&path).build(&reads.engine()).unwrap();
    let expected = Snapshot::builder_for(&path)
        .build(reads.inner.as_ref())
        .unwrap();
    assert_eq!(actual.as_ref(), expected.as_ref());
    let events = reads.events.lock().unwrap();
    assert!(events.contains(&Read::Hint), "{events:?}");
    assert!(events.contains(&Read::List), "{events:?}");
    assert!(events.contains(&read), "{events:?}");
}

#[test]
fn ordinary_protocol_metadata_replay_propagates_cancellation_after_batch() {
    let (url, _temp) = ordinary_replay_table();
    let mut reads = RecordingReads::new(None);
    Arc::get_mut(&mut reads).unwrap().cancel_json_after_batch = true;
    let token: CancellationTokenRef = reads.token.clone();
    let result = Snapshot::builder_for(url)
        .with_cancellation_token(token)
        .build(&reads.engine());
    assert!(matches!(result, Err(KernelError::Cancelled)), "{result:?}");
    assert_eq!(
        reads
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|read| **read == Read::Json)
            .count(),
        1
    );
}

/// Propagates cancellation at JSON read entry or after a batch with a stale CRC.
#[rstest]
fn stale_crc_replay_keeps_builder_token(
    #[values(false, true)] incremental: bool,
    #[values(IncrementalReplay::Disabled, IncrementalReplay::Unlimited)] replay: IncrementalReplay,
    #[values(None, Some(false), Some(true))] cancel_after_batch: Option<bool>,
) {
    let (url, _temp) = stale_crc_table();
    let mut reads = RecordingReads::new((cancel_after_batch == Some(false)).then_some(Read::Json));
    Arc::get_mut(&mut reads).unwrap().cancel_json_after_batch = cancel_after_batch == Some(true);
    let engine = reads.engine();
    let token: CancellationTokenRef = reads.token.clone();
    let result = if incremental {
        let base = Snapshot::builder_for(&url)
            .at_version(0)
            .build(reads.inner.as_ref())
            .unwrap();
        Snapshot::builder_from(base)
            .with_incremental_crc_replay(replay)
            .with_cancellation_token(token)
            .build(&engine)
    } else {
        Snapshot::builder_for(&url)
            .with_incremental_crc_replay(replay)
            .with_cancellation_token(token)
            .build(&engine)
    };
    if cancel_after_batch.is_some() {
        assert!(matches!(result, Err(KernelError::Cancelled)));
    } else {
        let snapshot = result.unwrap();
        assert_eq!(snapshot.version(), 1);
        assert_eq!(
            snapshot.crc_at_version().map(|crc| crc.version),
            (replay == IncrementalReplay::Unlimited).then_some(1)
        );
    }
    let events = reads.events.lock().unwrap();
    assert_eq!(events.iter().filter(|read| **read == Read::Json).count(), 1);
}

/// Verifies JSON fallback for a malformed newer CRC during an incremental snapshot build.
/// A cancelled CRC read stops the build before reading its contents.
#[rstest]
#[case::malformed(false, false)]
#[case::engine_cancellation(true, false)]
#[case::caller_token_cancellation(true, true)]
fn only_cancelled_crc_read_blocks_incremental_snapshot_build(
    #[case] cancel_crc: bool,
    #[case] bind_token: bool,
) {
    let (url, _temp) = stale_crc_table();
    let base = Snapshot::builder_for(&url)
        .at_version(0)
        .build(&SyncEngine::new())
        .unwrap();
    assert_eq!(base.crc_at_version().unwrap().version, 0);
    std::fs::write(
        url.to_file_path()
            .unwrap()
            .join("_delta_log/00000000000000000001.crc"),
        b"malformed",
    )
    .unwrap();

    let mut reads = RecordingReads::new(cancel_crc.then_some(Read::Crc));
    let token = bind_token.then(|| reads.token.clone() as CancellationTokenRef);
    Arc::get_mut(&mut reads).unwrap().expected_token = token.clone();
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let result = Snapshot::builder_from(base)
        .with_cancellation_token(token)
        .with_incremental_crc_replay(IncrementalReplay::Unlimited)
        .with_correlation_id("incremental-crc-failure")
        .build(&reads.engine());
    let read_events = reads.events.lock().unwrap();
    assert!(read_events.contains(&Read::Crc));
    if !cancel_crc {
        let updated = result.unwrap();
        assert_eq!(updated.version(), 1);
        assert_eq!(updated.crc_at_version().unwrap().version, 1);
        assert!(read_events.contains(&Read::Json));
        return;
    }

    assert!(matches!(result, Err(KernelError::Cancelled)));
    assert_eq!(read_events.last(), Some(&Read::Crc));
    assert!(!read_events.contains(&Read::Json));
    let events = reporter.events();
    let failures: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                MetricEvent::ProtocolMetadataLoadFailure(_) | MetricEvent::SnapshotBuildFailure(_)
            )
        })
        .collect();
    let [
        MetricEvent::ProtocolMetadataLoadFailure(pm),
        MetricEvent::SnapshotBuildFailure(build),
    ] = failures.as_slice()
    else {
        panic!("expected one protocol/metadata and snapshot failure: {events:?}");
    };
    assert_eq!(pm.operation_id, build.operation_id);
    assert_eq!(
        pm.correlation_id.as_deref(),
        Some("incremental-crc-failure")
    );
    assert_eq!(pm.correlation_id, build.correlation_id);
    assert_eq!(pm.load_type, LogSegmentLoadType::Incremental);
    assert_eq!(build.load_type, SnapshotLoadType::Incremental);
    assert!(!events.iter().any(|event| matches!(
        event,
        MetricEvent::ProtocolMetadataLoadSuccess(_) | MetricEvent::SnapshotBuildSuccess(_)
    )));
}

/// Propagates an engine-reported CRC cancellation even without a caller-supplied token.
#[test]
fn engine_reported_crc_cancellation_is_not_optional() {
    let mut reads = RecordingReads::new(Some(Read::Crc));
    Arc::get_mut(&mut reads).unwrap().expected_token = None;
    let result = Snapshot::builder_for("tests/data/crc-full").build(&reads.engine());
    assert!(matches!(result, Err(KernelError::Cancelled)));
    assert_eq!(reads.events.lock().unwrap().last(), Some(&Read::Crc));
}

/// Creates a table with CRC at version zero and a newer commit to replay.
fn stale_crc_table() -> (Url, TempDir) {
    let (url, temp) = copy_test_table("crc-full").unwrap();
    std::fs::write(
        url.to_file_path()
            .unwrap()
            .join("_delta_log/00000000000000000001.json"),
        r#"{"commitInfo":{"operation":"WRITE"}}"#,
    )
    .unwrap();
    (url, temp)
}

fn ordinary_replay_table() -> (Url, TempDir) {
    let (url, temp) = copy_test_table("table-without-dv-small").unwrap();
    std::fs::write(
        url.to_file_path()
            .unwrap()
            .join("_delta_log/00000000000000000001.json"),
        r#"{"commitInfo":{"operation":"WRITE"}}"#,
    )
    .unwrap();
    (url, temp)
}
