//! This module provides functions for performing timestamp queries over the Delta Log, translating
//! between timestamps and Delta versions.
//!
//! # Usage
//!
//! Use this module to:
//! - Convert timestamps or timestamp ranges into Delta versions or version ranges
//! - Perform time travel queries
//! - Execute timestamp-based change data feed queries
//!
//! The history_manager module works with tables regardless of whether they have In-Commit
//! Timestamps enabled.
//!
//! # Limitations
//!
//!  All timestamp queries are limited to the state captured in the [`Snapshot`]
//! provided during construction.
use error::{LogHistoryError, TimestampOutOfRangeError};
use search::{binary_search_by_key_with_bounds, Bound, SearchError};
use std::cmp::Ordering;
use std::fmt::Debug;

use crate::actions::visitors::InCommitTimestampVisitor;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::snapshot::Snapshot;
use crate::{Engine, Error as DeltaError, RowVisitor, Version};

pub(crate) mod search;

#[cfg(feature = "internal-api")]
pub mod error;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod error;

type Timestamp = i64;

#[allow(unused)]
#[derive(Debug)]
enum TimestampSearchBounds {
    ExactMatch(Version),
    ICTSearchStartingFrom(usize),
    FileModificationSearch,
    FileModificationSearchUntil {
        index: usize,
        ict_enablement_version: Version,
    },
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
            std::slice::from_ref(&commit_file.location),
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

/// Given a timestamp, this function determines the commit range that timestamp conversion
/// should search. A timestamp search may be conducted over one of two version ranges:
///     1) A range of commits whose timestamp is the file modification timestamp
///     2) A range of commits whose timestamp is an in-commit timestamp.
#[allow(unused)]
fn get_timestamp_search_bounds(
    snapshot: &Snapshot,
    log_segment: &LogSegment,
    timestamp: Timestamp,
) -> Result<TimestampSearchBounds, LogHistoryError> {
    debug_assert!(log_segment.end_version == snapshot.version());
    let table_config = snapshot.table_configuration();

    // Get the In-commit timestamp (ICT) enablement version and timestamp. If ICT is not
    // supported or enabled, we perform a regular file modification search.
    use crate::table_configuration::InCommitTimestampEnablement;
    let ict_enablement = table_config
        .in_commit_timestamp_enablement()
        .map_err(|err| LogHistoryError::InvalidTableConfiguration(Box::new(err)))?;

    let (ict_version, ict_timestamp) = match ict_enablement {
        InCommitTimestampEnablement::NotEnabled => {
            return Ok(TimestampSearchBounds::FileModificationSearch)
        }
        InCommitTimestampEnablement::Enabled {
            enablement: Some((v, t)),
        } => (v, t),
        InCommitTimestampEnablement::Enabled { enablement: None } => {
            // ICT was enabled from table creation - no enablement version
            return Ok(TimestampSearchBounds::FileModificationSearch);
        }
    };

    // Helper function to get the index at which ICT was enabled if it is present in this
    // LogSegment. We use binary_search_by to find the index where ict_version would be inserted
    // if it's not present. If found (Ok), we get the exact index. If not found (Err), we get
    // the insertion point, which is correct because we want the index of the first commit at or
    // after the ICT enablement version.
    let version_idx = || {
        log_segment
            .ascending_commit_files
            .binary_search_by(|x| x.version.cmp(&ict_version))
            .unwrap_or_else(|idx| idx)
    };

    // Based on the in-commit timestamp enablement, determine the search bounds. If the
    // desired timestamp is in the ICT range, we must _only_ search over commits with ICT
    // enabled. Otherwise, the timestamp range must only consist of commits that use file
    // modification time as the timestamp.
    let result = match timestamp.cmp(&ict_timestamp) {
        Ordering::Equal => TimestampSearchBounds::ExactMatch(ict_version),
        Ordering::Less => TimestampSearchBounds::FileModificationSearchUntil {
            index: version_idx(),
            ict_enablement_version: ict_version,
        },
        Ordering::Greater => TimestampSearchBounds::ICTSearchStartingFrom(version_idx()),
    };
    Ok(result)
}

/// Converts a timestamp to a version based on the specified bound type.
///
/// This function finds the appropriate commit version that corresponds to the given timestamp
/// according to the bound parameter:
///
/// - `Bound::GreatestLower`: Returns the version with the greatest timestamp that is less than or equal to
///   the given timestamp (the version immediately before or at the timestamp).
/// - `Bound::LeastUpper`: Returns the version with the smallest timestamp that is greater than or equal to
///   the given timestamp (the version immediately at or after the timestamp).
///
/// # Visual Example:
/// ```ignore
/// versions:    v0                    v1                    v2
///              |----------|----------|----------|----------|
/// timestamp:              t1                    t2
/// ```
///
/// # Results Table:
/// | Timestamp | GreatestLower | LeastUpper |
/// |-----------|---------------|------------|
/// | t1        | v0            | v1         |
/// | t2        | v1            | v2         |
///
/// # Errors
/// Returns [`LogHistoryError::TimestampOutOfRange`] if the timestamp is out of range. This can
/// happen in the following cases based on the bound:
/// - `Bound::GreatestLower`: There is no commit whose timestamp is lower than the given `timestamp`.
/// - `Bound::LeastUpper`: There is no commit whose timestamp is greater than the given `timestamp`.
#[allow(unused)]
fn timestamp_to_version(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    timestamp: Timestamp,
    bound: Bound,
) -> Result<Version, LogHistoryError> {
    // Get log segment for timestamp conversion
    let log_segment = LogSegment::for_timestamp_conversion(
        engine.storage_handler().as_ref(),
        snapshot.log_segment().log_root.clone(),
        snapshot.version(),
        None,
    )
    .map_err(|error| LogHistoryError::FailedToBuildLogSegment {
        version: snapshot.version(),
        error: Box::new(error),
    })?;
    debug_assert!(
        !log_segment.ascending_commit_files.is_empty(),
        "LogSegment should ensure that a segment is non-empty"
    );
    debug_assert!(log_segment.end_version == snapshot.log_segment().end_version);

    let len = log_segment.ascending_commit_files.len();

    // Determine the type of timestamp search. The search may either be over file modification
    // timestamps or over In-Commit Timestamps.
    let search_bounds = get_timestamp_search_bounds(snapshot, &log_segment, timestamp)?;
    let (lo, hi, read_ict) = match search_bounds {
        TimestampSearchBounds::ExactMatch(version) => return Ok(version),
        TimestampSearchBounds::ICTSearchStartingFrom(lo) => (lo, len, true),
        TimestampSearchBounds::FileModificationSearch => (0, len, false),
        TimestampSearchBounds::FileModificationSearchUntil { index: hi, .. } => (0, hi, false),
    };
    debug_assert!(lo < hi, "Index range should be non-empty");

    // Declare the key function of the search that finds the timestamp given a commit.
    let commit_to_ts = |commit: &ParsedLogPath| -> Result<Timestamp, LogHistoryError> {
        if read_ict {
            read_in_commit_timestamp(engine, commit)
        } else {
            Ok(commit.location.last_modified)
        }
    };

    // We only search in the range [lo..hi).
    let commit_range = &log_segment.ascending_commit_files[lo..hi];
    let search_result =
        binary_search_by_key_with_bounds(commit_range, timestamp, commit_to_ts, bound);

    // Get the index from the commit range if successful
    match search_result {
        Ok(relative_idx) => {
            // `relative_idx` holds for the range [lo..hi]. Add back `lo`
            let idx = lo + relative_idx;
            debug_assert!(idx < len, "Relative index should become a valid index");
            Ok(log_segment.ascending_commit_files[idx].version)
        }
        Err(SearchError::KeyFunctionError(error)) => {
            // Commit to timestamp conversion failed. Return the error
            Err(error)
        }
        Err(SearchError::OutOfRange) => match (search_bounds, bound) {
            // Special case: If searching for Least Upper Bound over file modification
            // timestamp range, and no version was found, then use the in-commit timestamp
            // enablement version. This is because the enablement version bounds all file
            // modification timestamps from above.
            (
                TimestampSearchBounds::FileModificationSearchUntil {
                    ict_enablement_version,
                    ..
                },
                Bound::LeastUpper,
            ) => Ok(ict_enablement_version),
            _ => Err(LogHistoryError::TimestampOutOfRange(
                TimestampOutOfRangeError::try_new(
                    commit_range,
                    timestamp,
                    commit_to_ts,
                    bound,
                    read_ict,
                )?,
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use crate::actions::{CommitInfo, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::snapshot::Snapshot;
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::{Action, LocalMockTable};
    use crate::Version;
    use test_utils::delta_path_for_version;
    use url::Url;

    use super::*;

    // Helper to create test schema
    fn get_test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]))
    }

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
        mock_table
            .commit([
                Action::Metadata(
                    Metadata::try_new(None, None, get_test_schema(), vec![], 0, HashMap::new())
                        .unwrap(),
                ),
                Action::Protocol(
                    Protocol::try_new(
                        3,
                        7,
                        Some(Vec::<String>::new()),
                        Some(vec![TableFeature::InCommitTimestamp]),
                    )
                    .unwrap(),
                ),
            ])
            .await;
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
        mock_table
            .commit([
                Action::CommitInfo(CommitInfo {
                    in_commit_timestamp: Some(300),
                    ..Default::default()
                }),
                Action::Metadata(
                    Metadata::try_new(
                        None,
                        None,
                        get_test_schema(),
                        vec![],
                        0,
                        HashMap::from_iter([
                            (
                                "delta.enableInCommitTimestamps".to_string(),
                                "true".to_string(),
                            ),
                            (
                                "delta.inCommitTimestampEnablementVersion".to_string(),
                                "3".to_string(),
                            ),
                            (
                                "delta.inCommitTimestampEnablementTimestamp".to_string(),
                                "300".to_string(),
                            ),
                        ]),
                    )
                    .unwrap(),
                ),
                Action::Protocol(
                    Protocol::try_new(
                        3,
                        7,
                        Some(Vec::<String>::new()),
                        Some(vec![TableFeature::InCommitTimestamp]),
                    )
                    .unwrap(),
                ),
            ])
            .await;
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
    async fn timestamp_search_bounds_no_ict() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();

        // Set the end version to a time before In-commit timestamps were enabled
        let snapshot = Snapshot::builder_for(path)
            .at_version(2)
            .build(&engine)
            .unwrap();
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            None,
        )
        .unwrap();

        // Before all commits
        let mut res = get_timestamp_search_bounds(&snapshot, &log_segment, 0);
        assert!(matches!(
            res,
            Ok(TimestampSearchBounds::FileModificationSearch)
        ));

        // Within the timestamp range
        res = get_timestamp_search_bounds(&snapshot, &log_segment, 100);
        assert!(matches!(
            res,
            Ok(TimestampSearchBounds::FileModificationSearch)
        ));

        // After all commits
        res = get_timestamp_search_bounds(&snapshot, &log_segment, 1000);
        assert!(matches!(
            res,
            Ok(TimestampSearchBounds::FileModificationSearch)
        ));
    }

    #[tokio::test]
    async fn timestamp_search_bounds_with_ict_range() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            None,
        )
        .unwrap();

        // Exact match only applies to in-commit timestamp enablement version
        let mut res = get_timestamp_search_bounds(&snapshot, &log_segment, 50);
        assert!(
            matches!(
                res,
                Ok(TimestampSearchBounds::FileModificationSearchUntil {
                    index: 3,
                    ict_enablement_version: 3
                })
            ),
            "{res:?}"
        );

        // Not exact match, file modification time range
        res = get_timestamp_search_bounds(&snapshot, &log_segment, 60);
        assert!(
            matches!(
                res,
                Ok(TimestampSearchBounds::FileModificationSearchUntil {
                    index: 3,
                    ict_enablement_version: 3
                })
            ),
            "{res:?}"
        );

        // Edge case: The last timestamp that is file modification time
        res = get_timestamp_search_bounds(&snapshot, &log_segment, 299);
        assert!(
            matches!(
                res,
                Ok(TimestampSearchBounds::FileModificationSearchUntil {
                    index: 3,
                    ict_enablement_version: 3
                })
            ),
            "{res:?}"
        );

        // Edge case: Timestamp is the enablement timestamp
        res = get_timestamp_search_bounds(&snapshot, &log_segment, 300);
        assert!(
            matches!(res, Ok(TimestampSearchBounds::ExactMatch(3))),
            "{res:?}"
        );

        // Edge case: The timestamp is 1 + enablement_timestamp. This returns the beginning of the
        // in-commit timestamp index range.
        res = get_timestamp_search_bounds(&snapshot, &log_segment, 301);
        assert!(
            matches!(res, Ok(TimestampSearchBounds::ICTSearchStartingFrom(3))),
            "{res:?}"
        );

        // Timestamp is much larger than the last in-commit timestamp
        res = get_timestamp_search_bounds(&snapshot, &log_segment, 1000);
        assert!(
            matches!(res, Ok(TimestampSearchBounds::ICTSearchStartingFrom(3))),
            "{res:?}"
        );
    }

    #[tokio::test]
    async fn reading_in_commit_timestamp() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            None,
        )
        .unwrap();
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
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            None,
        )
        .unwrap();
        let commits = log_segment.ascending_commit_files;

        // Read the file modification timestamps
        let ts: Result<Timestamp, LogHistoryError> = Ok(commits[0].location.last_modified);
        assert!(matches!(ts, Ok(50)));

        let ts: Result<Timestamp, LogHistoryError> = Ok(commits[1].location.last_modified);
        assert!(matches!(ts, Ok(150)));

        let ts: Result<Timestamp, LogHistoryError> = Ok(commits[2].location.last_modified);
        assert!(matches!(ts, Ok(250)));

        // Read the in-commit timestamps
        let ts = read_in_commit_timestamp(&engine, &commits[3]);
        assert!(matches!(ts, Ok(300)));
        let ts = read_in_commit_timestamp(&engine, &commits[4]);
        assert!(matches!(ts, Ok(400)));
    }

    #[tokio::test]
    async fn convert_timestamp_to_version() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        // Less than the lowest ICT
        let mut res = timestamp_to_version(&snapshot, &engine, 0, Bound::LeastUpper);
        assert!(matches!(res, Ok(0)), "{res:?}");

        // Negative timestamps are allowed:
        res = timestamp_to_version(&snapshot, &engine, -1, Bound::LeastUpper);
        assert!(matches!(res, Ok(0)), "{res:?}");

        // GreatestLower Bound on timestamp that is less that all commits fails
        res = timestamp_to_version(&snapshot, &engine, 0, Bound::GreatestLower);
        assert!(
            matches!(
                res,
                Err(LogHistoryError::TimestampOutOfRange(
                    TimestampOutOfRangeError {
                        timestamp: 0,
                        nearest_timestamp: 50,
                        nearest_version: 0,
                        bound: Bound::GreatestLower,
                        _read_ict: false
                    }
                ))
            ),
            "{res:?}"
        );

        // GreatestLower bound on timestamp that is greater than all commits succeeds
        res = timestamp_to_version(&snapshot, &engine, 1000, Bound::GreatestLower);
        assert!(matches!(res, Ok(4)), "{res:?}");

        // LeastUpper Bound on timestamp that is greater than all commits fails
        res = timestamp_to_version(&snapshot, &engine, 1000, Bound::LeastUpper);
        assert!(
            matches!(
                res,
                Err(LogHistoryError::TimestampOutOfRange(
                    TimestampOutOfRangeError {
                        timestamp: 1000,
                        nearest_timestamp: 400,
                        nearest_version: 4,
                        bound: Bound::LeastUpper,
                        _read_ict: true
                    }
                ))
            ),
            "{res:?}"
        );

        // Edge cases: timestamp is between file modification time and in-commit timestamp

        // Right after last file modification timestamp
        res = timestamp_to_version(&snapshot, &engine, 251, Bound::LeastUpper);
        assert!(matches!(res, Ok(3)), "{res:?}");
        res = timestamp_to_version(&snapshot, &engine, 251, Bound::GreatestLower);
        assert!(matches!(res, Ok(2)), "{res:?}");

        // Right before first in-commit timestamp
        res = timestamp_to_version(&snapshot, &engine, 299, Bound::LeastUpper);
        assert!(matches!(res, Ok(3)), "{res:?}");
        res = timestamp_to_version(&snapshot, &engine, 299, Bound::GreatestLower);
        assert!(matches!(res, Ok(2)), "{res:?}");

        // Right after first in-commit timestamp
        res = timestamp_to_version(&snapshot, &engine, 301, Bound::GreatestLower);
        assert!(matches!(res, Ok(3)), "{res:?}");
        res = timestamp_to_version(&snapshot, &engine, 301, Bound::LeastUpper);
        assert!(matches!(res, Ok(4)), "{res:?}");
    }
}
