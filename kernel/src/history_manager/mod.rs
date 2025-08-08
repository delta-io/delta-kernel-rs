//! This module provides functions for performing timestamp queries over the Delta Log, translating
//! between timestamps and Delta versions.
//!
//! # Usage
//!
//! Use this module to:
//! - Convert timestamps or timestamp ranges into Delta versions or version ranges
//! - Perform time travel queries using [`Snapshot::try_new`]
//! - Execute timestamp-based change data feed queries using [`TableChanges::try_new`]
//!
//! The history_manager module works with tables regardless of whether they have In-Commit
//! Timestamps enabled.
//!
//! # Limitations
//!
//!  All timestamp queries are limited to the state captured in the [`Snapshot`]
//! provided during construction.
//!
//! [`TableChanges::try_new`]: crate::table_changes::TableChanges::try_new
use std::num::NonZero;

use error::LogHistoryError;

use crate::log_segment::LogSegment;
use crate::snapshot::Snapshot;
use crate::Engine;

pub(crate) mod search;

pub mod error;

type Timestamp = i64;

/// Gets a [`LogSegment`] that has the same `end_version` as the provided [`Snapshot`].
///
/// # Parameters
///
/// * `engine` - The engine instance providing access to storage and other Delta Lake operations
/// * `snapshot` - The snapshot whose version will be used as the end version for the log segment
/// * `limit` - Optional maximum number of commits that will be fetched. If specified, only the
///   most recent `limit` commits up to the snapshot version will be included in the log segment.
///   For large tables, using a limit can provide significant performance improvements by avoiding
///   the need to list all files in the log.
///
/// # Returns
///
/// Returns a `Result<LogSegment, LogHistoryError>` containing either:
/// * `Ok(LogSegment)` - A log segment suitable for timestamp conversion operations
/// * `Err(LogHistoryError::FailedToBuildLogSegment)` - If the log segment construction fails
///
/// # Notes
///
/// * For large tables, this listing operation can be very expensive without a limit
/// * The log segment may be shorter than the specified limit if there are missing commits
#[allow(unused)]
fn get_log_segment_for_timestamp_conversion(
    engine: &dyn Engine,
    snapshot: &Snapshot,
    limit: Option<NonZero<usize>>,
) -> Result<LogSegment, LogHistoryError> {
    let log_segment = LogSegment::for_timestamp_conversion(
        engine.storage_handler().as_ref(),
        snapshot.log_segment().log_root.clone(),
        snapshot.version(),
        limit,
    )
    .map_err(|error| LogHistoryError::FailedToBuildLogSegment {
        version: snapshot.version(),
        error: Box::new(error),
    })?;
    Ok(log_segment)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::time::{Duration, SystemTime};

    use crate::actions::{CommitInfo, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::snapshot::Snapshot;
    use crate::table_features::WriterFeature;
    use crate::utils::test_utils::{Action, LocalMockTable};
    use crate::Version;
    use test_utils::delta_path_for_version;
    use url::Url;

    use super::*;

    // Helper to set the file modification timestamp of a file
    fn set_mod_time(mock_table: &LocalMockTable, commit_version: Version, timestamp: Timestamp) {
        let file_name = delta_path_for_version(commit_version, "json")
            .filename()
            .unwrap()
            .to_string();
        let path = mock_table.table_root().join("_delta_log/").join(file_name);
        let file = OpenOptions::new().write(true).open(path).unwrap();

        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp.try_into().unwrap());
        file.set_modified(time).unwrap();
    }

    async fn mock_table() -> LocalMockTable {
        let mut mock_table = LocalMockTable::new();

        // 0: Has file modification timestamp 50
        mock_table.commit([Action::Metadata(Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        }),
            Action::Protocol(Protocol::try_new(3, 7, Some(Vec::<String>::new()), Some(vec![WriterFeature::InCommitTimestamp])).unwrap())
        ]).await;
        set_mod_time(&mock_table, 0, 50);

        // 1: Has file modification timestamp 150
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 1, 150);

        // 2: Has file modification timestamp 250
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 2, 250);

        // 3: Has in-commit timestamp 300, file modification timestamp 350
        mock_table.commit([
            Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(300),
                ..Default::default()
            }),
            Action::Metadata(Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            configuration: HashMap::from_iter([(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            ),
                ("delta.inCommitTimestampEnablementVersion".to_string(), "3".to_string()),
                ("delta.inCommitTimestampEnablementTimestamp".to_string(), "300".to_string())]),
            ..Default::default()
        }),
            Action::Protocol(Protocol::try_new(3, 7, Some(Vec::<String>::new()), Some(vec![WriterFeature::InCommitTimestamp])).unwrap())
        ]).await;
        set_mod_time(&mock_table, 3, 350);

        // 4: Has in-commit timestamp 400, file modification timestamp 450
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(400),
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 4, 450);

        mock_table
    }

    #[tokio::test]
    async fn reading_in_commit_timestamp() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let log_segment =
            get_log_segment_for_timestamp_conversion(&engine, &snapshot, None).unwrap();
        let commits = log_segment.ascending_commit_files;

        // File that has no In-commit timestamps
        let mut res = commits[0].read_in_commit_timestamp(&engine);
        assert!(
            matches!(
                res,
                Err(LogHistoryError::InCommitTimestampNotFoundError { version: 0 })
            ),
            "{res:?} failed"
        );

        // File that doesn't exist
        let mut fake_log_path = commits[0].clone();

        let failing_path = if cfg!(windows) {
            "C:\\phony\\path"
        } else {
            "/phony/path"
        };

        fake_log_path.location.location = Url::from_file_path(failing_path).unwrap();
        res = fake_log_path.read_in_commit_timestamp(&engine);
        assert!(
            matches!(
                res,
                Err(LogHistoryError::FailedToReadTimestampForCommit {
                    version: 0,
                    error: _
                })
            ),
            "{res:?} failed"
        );

        // Files with In-commit timestamps
        res = commits[3].read_in_commit_timestamp(&engine);
        assert!(matches!(res, Ok(300)), "{res:?}");
        res = commits[4].read_in_commit_timestamp(&engine);
        assert!(matches!(res, Ok(400)), "{res:?}");
    }

    #[tokio::test]
    async fn file_modification_conversion() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let log_segment =
            get_log_segment_for_timestamp_conversion(&engine, &snapshot, None).unwrap();
        let commits = log_segment.ascending_commit_files;

        // Read the file modification timestamps
        let ts = commits[0].file_modification_timestamp();
        assert!(matches!(ts, 50));

        let ts = commits[1].file_modification_timestamp();
        assert!(matches!(ts, 150));

        let ts = commits[2].file_modification_timestamp();
        assert!(matches!(ts, 250));

        // Read the in-commit timestamps
        let ts = commits[3].read_in_commit_timestamp(&engine);
        assert!(matches!(ts, Ok(300)));
        let ts = commits[4].read_in_commit_timestamp(&engine);
        assert!(matches!(ts, Ok(400)));
    }
}
