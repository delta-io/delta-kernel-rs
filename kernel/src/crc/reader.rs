//! CRC file reading functionality.

use std::sync::Arc;

use tracing::instrument;

use super::{Crc, CrcWithProvenance};
use crate::metrics::CrcReadFailureReason;
use crate::metrics::events::CRC_READ_COMPLETED_SPAN;
use crate::path::{AsUrl as _, ParsedLogPath};
use crate::{Engine, KernelError, KernelResult};

/// Attempt to read and parse a CRC file.
///
/// Reads raw bytes via the storage handler and deserializes with serde_json.
///
/// Returns `Ok(Crc)` on success, `Err` on any failure (file not readable, corrupt JSON,
/// missing required fields). Cancellation must propagate; other failures may fall back to log
/// replay.
///
/// Reports metrics: `CrcReadSuccess` or `CrcReadFailure`.
#[instrument(name = CRC_READ_COMPLETED_SPAN, err(level = "warn"), skip_all, fields(report, enable_call_frame, bytes_read, failure_reason, path = ?crc_path.location.location))]
pub(crate) fn try_read_crc_file(
    engine: &dyn Engine,
    crc_path: &ParsedLogPath,
) -> KernelResult<Crc> {
    let storage = engine.storage_handler();
    let url = crc_path.location.as_url().clone();
    let data = storage
        .read_files(vec![(url, None)])
        .inspect_err(record_storage_failure)?
        .next()
        .ok_or_else(|| {
            record_failure_reason(CrcReadFailureReason::Empty);
            KernelError::generic("CRC file read returned no data")
        })?
        .inspect_err(record_storage_failure)?;
    tracing::Span::current().record("bytes_read", data.len() as u64);
    Crc::try_from_json_bytes(&data, crc_path.version).inspect_err(|_| {
        record_failure_reason(if data.is_empty() {
            CrcReadFailureReason::Empty
        } else {
            CrcReadFailureReason::Parsing
        });
    })
}

/// Read an optional CRC, propagating cancellation and ignoring other read failures.
///
/// CRC files are optional, so an unreadable one is not an error: the caller proceeds without
/// it. The failure is logged and metered by [`try_read_crc_file`]'s instrumentation.
pub(crate) fn read_crc_file_or_none(
    engine: &dyn Engine,
    crc_file: &ParsedLogPath,
) -> KernelResult<Option<CrcWithProvenance>> {
    match try_read_crc_file(engine, crc_file) {
        Ok(crc) => Ok(Some(CrcWithProvenance::from_file(Arc::new(crc)))),
        Err(error) if matches!(error.without_backtrace(), KernelError::Cancelled) => Err(error),
        Err(_) => Ok(None),
    }
}

fn record_storage_failure(error: &KernelError) {
    record_failure_reason(match error.without_backtrace() {
        KernelError::FileNotFound(_) => CrcReadFailureReason::NotFound,
        KernelError::Cancelled => CrcReadFailureReason::Cancelled,
        _ => CrcReadFailureReason::Error,
    });
}

fn record_failure_reason(reason: CrcReadFailureReason) {
    tracing::Span::current().record("failure_reason", reason.as_ref());
}

#[cfg(test)]
mod tests {
    use std::backtrace::Backtrace;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use bytes::Bytes;
    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::KernelResultIterator;
    use crate::actions::{Format, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::engine::{CancellableEngine, DelegatingEngine};
    use crate::metrics::MetricEvent;
    use crate::path::ParsedLogPath;
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::recording_reads::RecordingReads;
    use crate::unit_test_utils::{
        CapturingReporter, TestCancellationToken, install_thread_local_metrics_reporter,
    };

    fn test_table_root(dir: &str) -> url::Url {
        let path = std::fs::canonicalize(PathBuf::from(dir)).unwrap();
        url::Url::from_directory_path(path).unwrap()
    }

    #[rstest::rstest]
    #[case("crc-full", 0, Some(0), None)]
    #[case("crc-malformed", 0, None, Some(CrcReadFailureReason::Parsing))]
    #[case("crc-full", 999, None, Some(CrcReadFailureReason::NotFound))]
    fn optional_crc_read_preserves_cancellation(
        #[case] fixture: &str,
        #[case] version: u64,
        #[case] expected: Option<u64>,
        #[case] expected_reason: Option<CrcReadFailureReason>,
        #[values(false, true)] cancelled: bool,
    ) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let table_root = test_table_root(&format!("./tests/data/{fixture}/"));
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, version);
        let token = Arc::new(TestCancellationToken::default());
        if cancelled {
            token.cancel();
        }
        let engine = CancellableEngine::new(&SyncEngine::new(), token);
        let result = read_crc_file_or_none(&engine, &crc_path);
        if cancelled {
            assert!(matches!(result, Err(KernelError::Cancelled)), "{result:?}");
        } else {
            assert_eq!(result.unwrap().map(|crc| crc.version()), expected);
        }
        let reason = if cancelled {
            Some(CrcReadFailureReason::Cancelled)
        } else {
            expected_reason
        };
        if let Some(reason) = reason {
            assert_failure_reason(&reporter, reason);
        } else {
            assert!(matches!(
                reporter.events().as_slice(),
                [MetricEvent::CrcReadSuccess(_)]
            ));
        }
    }

    #[test]
    fn test_read_crc_file() {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());

        let engine = SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-full/");
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, 0);

        // Read and parse the CRC file
        let crc = try_read_crc_file(&engine, &crc_path).unwrap();

        // Verify basic fields
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.table_size_bytes(), 5259);
        assert_eq!(stats.num_files(), 10);
        assert_eq!(crc.in_commit_timestamp_opt, Some(1694758257000));

        // Verify protocol
        let expected_protocol = Protocol::new_unchecked(
            3,
            7,
            Some(vec![TableFeature::DeletionVectors]),
            Some(vec![
                TableFeature::DomainMetadata,
                TableFeature::ClusteredTable,
                TableFeature::DeletionVectors,
                TableFeature::RowTracking,
            ]),
        );
        assert_eq!(crc.protocol, expected_protocol);

        // Verify metadata
        let expected_metadata = Metadata::new_unchecked(
            "6ca3020b-3cd9-4048-82e3-1417a0abb98f",
            None,
            None,
            Format::default(),
            r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#,
            vec![],
            Some(1694758256009),
            HashMap::from([
                (
                    "delta.enableDeletionVectors".to_string(),
                    "true".to_string(),
                ),
                (
                    "delta.checkpoint.writeStatsAsStruct".to_string(),
                    "true".to_string(),
                ),
                ("delta.enableRowTracking".to_string(), "true".to_string()),
                (
                    "delta.checkpoint.writeStatsAsJson".to_string(),
                    "false".to_string(),
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName".to_string(),
                    "_row-commit-version-col-2f60dcc1-9e36-4424-95e7-799b707e4ddb".to_string(),
                ),
                (
                    "delta.rowTracking.materializedRowIdColumnName".to_string(),
                    "_row-id-col-4cbc7924-f662-4db1-aa59-22c23f59eb5d".to_string(),
                ),
            ]),
        );
        assert_eq!(crc.metadata, expected_metadata);

        // Verify domain metadatas
        let dms = crc.domain_metadata_state.expect_complete();
        assert_eq!(dms.len(), 3);

        assert!(
            dms["delta.clustering"]
                .configuration()
                .contains("clusteringColumns")
        );
        assert!(
            dms["delta.rowTracking"]
                .configuration()
                .contains("rowIdHighWaterMark")
        );
        assert!(dms["myApp.metadata"].configuration().contains("key"));

        // Verify set transactions
        let txns = crc.set_transaction_state.expect_complete();
        assert_eq!(txns.len(), 2);
        assert_eq!(txns["spark-app-1"].version, 42);
        assert_eq!(txns["spark-app-1"].last_updated, Some(1694758250000));
        assert_eq!(txns["streaming-job-abc"].version, 100);
        assert_eq!(txns["streaming-job-abc"].last_updated, Some(1694758255000));

        // Verify file size histogram was deserialized (all 10 files in bin 0, < 8KB)
        let hist = stats.file_size_histogram().unwrap();
        assert_eq!(hist.sorted_bin_boundaries.len(), 95);
        assert_eq!(hist.file_counts[0], 10);
        assert_eq!(hist.total_bytes[0], 5259);
        // All other bins should be zero
        assert!(hist.file_counts[1..].iter().all(|&c| c == 0));
        assert!(hist.total_bytes[1..].iter().all(|&b| b == 0));

        assert_eq!(
            crc.txn_id.as_deref(),
            Some("29ebf587-9705-4bb4-ac40-2f02324065c8")
        );
        let all_files = crc.all_files.as_ref().unwrap();
        assert_eq!(all_files.len(), 10);
        assert_eq!(all_files.iter().map(|add| add.size).sum::<i64>(), 5259);
        assert_eq!(crc.num_deleted_records_opt, Some(0));
        assert_eq!(crc.num_deletion_vectors_opt, Some(0));
        assert_eq!(
            crc.deleted_record_counts_histogram_opt
                .as_ref()
                .unwrap()
                .deleted_record_counts,
            [10, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );

        let events = reporter.events();
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, MetricEvent::CrcReadFailure(_))),
            "a valid CRC must not emit a failure"
        );
        let crc_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, MetricEvent::CrcReadSuccess(_)))
            .collect();
        assert_eq!(crc_events.len(), 1);
        assert!(matches!(&crc_events[0], MetricEvent::CrcReadSuccess(e) if e.bytes_read > 0));
    }

    #[rstest]
    #[case::empty_file(ReadBehavior::Data(b""), CrcReadFailureReason::Empty)]
    #[case::empty_iterator(ReadBehavior::EmptyIterator, CrcReadFailureReason::Empty)]
    #[case::whitespace(ReadBehavior::Data(b" \n"), CrcReadFailureReason::Parsing)]
    #[case::malformed(ReadBehavior::Data(b"invalid"), CrcReadFailureReason::Parsing)]
    #[case::missing_fields(ReadBehavior::Data(b"{}"), CrcReadFailureReason::Parsing)]
    fn invalid_crc_reports_reason_and_allows_fallback(
        #[case] behavior: ReadBehavior,
        #[case] reason: CrcReadFailureReason,
    ) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let engine = crc_engine(behavior);
        let root = Url::parse("memory:///").unwrap();
        let crc_path = ParsedLogPath::create_parsed_crc(&root, 0);

        assert!(read_crc_file_or_none(&engine, &crc_path).unwrap().is_none());
        assert_failure_reason(&reporter, reason);
    }

    #[rstest]
    fn storage_failure_reports_reason_and_preserves_cancellation(
        #[values(
            CrcReadFailureReason::NotFound,
            CrcReadFailureReason::Error,
            CrcReadFailureReason::Cancelled
        )]
        reason: CrcReadFailureReason,
        #[values(false, true)] in_iterator: bool,
        #[values(false, true)] backtraced: bool,
    ) {
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let engine = crc_engine(ReadBehavior::Error {
            reason,
            in_iterator,
            backtraced,
        });
        let root = Url::parse("memory:///").unwrap();
        let crc_path = ParsedLogPath::create_parsed_crc(&root, 0);

        let result = read_crc_file_or_none(&engine, &crc_path);
        if reason == CrcReadFailureReason::Cancelled {
            let error = result.unwrap_err();
            assert!(matches!(error.without_backtrace(), KernelError::Cancelled));
            assert_eq!(matches!(error, KernelError::Backtraced { .. }), backtraced);
        } else {
            assert!(result.unwrap().is_none());
        }
        assert_failure_reason(&reporter, reason);
    }

    fn crc_engine(behavior: ReadBehavior) -> DelegatingEngine {
        let mut reads = RecordingReads::new(None);
        let config = Arc::get_mut(&mut reads).unwrap();
        config.expected_token = None;
        config.override_byte_reads = Some(Box::new(move |_| behavior.read_files()));
        reads.engine()
    }

    fn assert_failure_reason(reporter: &CapturingReporter, expected: CrcReadFailureReason) {
        let events = reporter.events();
        assert!(
            matches!(events.as_slice(), [MetricEvent::CrcReadFailure(event)] if event.reason == expected),
            "expected one CRC failure with reason {expected}: {events:?}"
        );
    }

    #[derive(Clone, Copy)]
    enum ReadBehavior {
        Data(&'static [u8]),
        EmptyIterator,
        Error {
            reason: CrcReadFailureReason,
            in_iterator: bool,
            backtraced: bool,
        },
    }

    impl ReadBehavior {
        fn read_files(self) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>> {
            match self {
                ReadBehavior::Data(data) => {
                    Ok(Box::new(std::iter::once(Ok(Bytes::from_static(data)))))
                }
                ReadBehavior::EmptyIterator => Ok(Box::new(std::iter::empty())),
                ReadBehavior::Error {
                    reason,
                    in_iterator,
                    backtraced,
                } => {
                    let error = match reason {
                        CrcReadFailureReason::NotFound => KernelError::file_not_found("crc"),
                        CrcReadFailureReason::Cancelled => KernelError::Cancelled,
                        _ => KernelError::generic("read failed"),
                    };
                    let error = if backtraced {
                        KernelError::Backtraced {
                            source: Box::new(error),
                            backtrace: Box::new(Backtrace::disabled()),
                        }
                    } else {
                        error
                    };
                    if in_iterator {
                        Ok(Box::new(std::iter::once(Err(error))))
                    } else {
                        Err(error)
                    }
                }
            }
        }
    }
}
