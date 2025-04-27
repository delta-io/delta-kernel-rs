//! This module provides functions for performing timestamp queries over the Delta Log, translating
//! between timestamps and Delta versions.
//!
//! # Usage
//!
//! Use this module to:
//! - Convert timestamps or timestamp ranges into Delta versions or version ranges
//! - Perform time travel queries using [`Table::snapshot`]
//! - Execute timestamp-based change data feed queries using [`Table::table_changes`]
//!
//! The history_manager module works with tables regardless of whether they have In-Commit
//! Timestamps enabled.
//!
//! # Limitations
//!
//!  All timestamp queries are limited to the state captured in the [`Snapshot`]
//! provided during construction.
//!
//! [`Table::snapshot`]: crate::Table::snapshot
//! [`Table::table_changes`]: crate::Table::table_changes
use std::num::NonZero;

use error::LogHistoryError;

use crate::actions::visitors::InCommitTimestampVisitor;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::snapshot::Snapshot;
use crate::Engine;
use crate::Error as DeltaError;
use crate::RowVisitor;

pub(crate) mod search;

pub mod error;

type Timestamp = i64;

/// Gets the timestamp for the `commit_file`. If `read_ict` is false, this returns the file's
/// modification timestamp. If `read_ict` is true, this reads the file's In-commit timestamp.
#[allow(unused)]
fn commit_file_to_timestamp(
    latest_snapshot: &Snapshot,
    engine: &dyn Engine,
    commit_file: &ParsedLogPath,
    read_ict: bool,
) -> Result<Timestamp, LogHistoryError> {
    let commit_timestamp = if read_ict {
        read_in_commit_timestamp(engine, commit_file)?
    } else {
        // By default, the timestamp of a commit is its modification time
        commit_file.location.last_modified
    };

    Ok(commit_timestamp)
}

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
    limit: Option<NonZero<usize>>
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

/// Reads the in-commit timestamp for the given `commit_file`.
///
/// This returns a [`LogHistoryError::FailedToReadTimestampForCommit`] if this encounters an
/// error while reading the file or visiting the rows.
///
/// This returns a [`LogHistoryError::InCommitTimestampNotFoundError`] if the in-commit timestamp
/// is not present in the commit file, or if the CommitInfo is not the first action in the
/// commit.
#[allow(unused)]
fn read_in_commit_timestamp(
    engine: &dyn Engine,
    commit_file: &ParsedLogPath,
) -> Result<Timestamp, LogHistoryError> {
    debug_assert!(commit_file.is_commit(), "File should be a commit");
    let wrap_err = |error: DeltaError| LogHistoryError::FailedToReadTimestampForCommit {
        version: commit_file.version,
        error: Box::new(error),
    };

    // Get an iterator over the actions in the commit file
    let mut action_iter = engine
        .json_handler()
        .read_json_files(
            &[commit_file.location.clone()],
            InCommitTimestampVisitor::schema(),
            None,
        )
        .map_err(wrap_err)?;

    let not_found = || LogHistoryError::InCommitTimestampNotFoundError {
        version: commit_file.version,
    };

    // Take the first non-empty engine data batch
    match action_iter.next() {
        Some(Ok(batch)) => {
            // Visit the rows and get the in-commit timestamp if present
            let mut visitor = InCommitTimestampVisitor::default();
            visitor.visit_rows_of(batch.as_ref()).map_err(wrap_err)?;
            visitor.in_commit_timestamp.ok_or_else(not_found)
        }
        Some(Err(err)) => Err(wrap_err(err)),
        None => Err(not_found()),
    }
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
        let log_segment = get_log_segment_for_timestamp_conversion(&engine, &snapshot, None).unwrap();
        let commits = log_segment.ascending_commit_files;

        // File that has no In-commit timestamps
        let mut res = read_in_commit_timestamp(&engine, &commits[0]);
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
        res = read_in_commit_timestamp(&engine, &fake_log_path);
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
        res = read_in_commit_timestamp(&engine, &commits[3]);
        assert!(matches!(res, Ok(300)), "{res:?}");
        res = read_in_commit_timestamp(&engine, &commits[4]);
        assert!(matches!(res, Ok(400)), "{res:?}");
    }

    #[tokio::test]
    async fn file_modification_conversion() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let log_segment = get_log_segment_for_timestamp_conversion(&engine, &snapshot, None).unwrap();
        let commits = log_segment.ascending_commit_files;

        // Read the file modification timestamps
        let ts = commit_file_to_timestamp(&snapshot, &engine, &commits[0], false);
        assert!(matches!(ts, Ok(50)));

        let ts = commit_file_to_timestamp(&snapshot, &engine, &commits[1], false);
        assert!(matches!(ts, Ok(150)));

        let ts = commit_file_to_timestamp(&snapshot, &engine, &commits[2], false);
        assert!(matches!(ts, Ok(250)));

        // Read the in-commit timestamps
        let ts = commit_file_to_timestamp(&snapshot, &engine, &commits[3], true);
        assert!(matches!(ts, Ok(300)));
        let ts = commit_file_to_timestamp(&snapshot, &engine, &commits[4], true);
        assert!(matches!(ts, Ok(400)));
    }
}
