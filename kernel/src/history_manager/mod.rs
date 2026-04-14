//! This module provides functions for performing timestamp queries over the Delta Log, translating
//! between timestamps and Delta versions.

use std::cmp::Ordering;

use error::LogHistoryError;
use search::{binary_search_by_key_with_bounds, Bound, SearchError};
use tracing::info;

use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::snapshot::Snapshot;
use crate::table_configuration::InCommitTimestampEnablement;
use crate::utils::require;
use crate::{Engine, Error as DeltaError, Version};

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
    let ict_enablement = table_config
        .in_commit_timestamp_enablement()
        .map_err(|e| LogHistoryError::internal("failed to read table configuration", e))?;

    let (ict_enablement_version, ict_timestamp) = match ict_enablement {
        InCommitTimestampEnablement::NotEnabled => {
            return Ok(TimestampSearchBounds::FileModificationSearch)
        }
        InCommitTimestampEnablement::Enabled {
            enablement: Some((v, t)),
        } => (v, t),
        InCommitTimestampEnablement::Enabled { enablement: None } => {
            // ICT was enabled from table creation - all commits have ICT
            return Ok(TimestampSearchBounds::ICTSearchStartingFrom(0));
        }
    };

    // Get the index of the ICT enablement version in the log segment. If the version is not
    // present, binary_search returns the insertion point (first commit at or after).
    let commit_count = log_segment.listed.ascending_commit_files.len();
    let ict_enablement_idx = log_segment
        .listed
        .ascending_commit_files
        .binary_search_by(|x| x.version.cmp(&ict_enablement_version))
        .unwrap_or_else(|idx| idx);

    // The ICT enablement index must be within the log segment. If it equals commit_count,
    // the ICT enablement version is beyond all commits in the segment.
    require!(
        ict_enablement_idx < commit_count,
        LogHistoryError::internal(
            "ICT enablement version beyond log segment",
            DeltaError::generic(format!(
                "ICT enablement version {} not in log segment (index {} >= count {})",
                ict_enablement_version, ict_enablement_idx, commit_count
            ))
        )
    );

    // Based on the in-commit timestamp enablement, determine the search bounds. If the
    // desired timestamp is in the ICT range, we must _only_ search over commits with ICT
    // enabled. Otherwise, the timestamp range must only consist of commits that use file
    // modification time as the timestamp.
    let result = match timestamp.cmp(&ict_timestamp) {
        Ordering::Equal => TimestampSearchBounds::ExactMatch(ict_enablement_version),
        Ordering::Less => TimestampSearchBounds::FileModificationSearchUntil {
            index: ict_enablement_idx,
            ict_enablement_version,
        },
        Ordering::Greater => TimestampSearchBounds::ICTSearchStartingFrom(ict_enablement_idx),
    };
    Ok(result)
}

/// Converts a timestamp to a version based on the specified bound type.
///
/// This function finds the appropriate commit version that corresponds to the given timestamp
/// according to the bound parameter:
///
/// - `Bound::GreatestLower`: Returns the version with the greatest timestamp that is less than or
///   equal to the given timestamp (the version immediately before or at the timestamp).
/// - `Bound::LeastUpper`: Returns the version with the smallest timestamp that is greater than or
///   equal to the given timestamp (the version immediately at or after the timestamp).
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
/// - `Bound::GreatestLower`: There is no commit whose timestamp is lower than the given
///   `timestamp`.
/// - `Bound::LeastUpper`: There is no commit whose timestamp is greater than the given `timestamp`.
#[allow(unused)]
pub(crate) fn timestamp_to_version(
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
    .map_err(|e| {
        LogHistoryError::internal("failed to build log segment for timestamp conversion", e)
    })?;
    debug_assert!(
        !log_segment.listed.ascending_commit_files.is_empty(),
        "LogSegment should ensure that a segment is non-empty"
    );
    debug_assert!(log_segment.end_version == snapshot.log_segment().end_version);

    let len = log_segment.listed.ascending_commit_files.len();

    // Determine the type of timestamp search. The search may either be over file modification
    // timestamps or over In-Commit Timestamps.
    let search_bounds = get_timestamp_search_bounds(snapshot, &log_segment, timestamp)?;
    let (lo, hi, read_ict) = match search_bounds {
        TimestampSearchBounds::ExactMatch(version) => return Ok(version),
        TimestampSearchBounds::ICTSearchStartingFrom(lo) => (lo, len, true),
        TimestampSearchBounds::FileModificationSearch => (0, len, false),
        TimestampSearchBounds::FileModificationSearchUntil { index: hi, .. } => (0, hi, false),
    };

    // If the search range is empty (lo >= hi), the timestamp is out of range. This can happen
    // when the log has been trimmed and the query timestamp falls before the available commits.
    if lo >= hi {
        return Err(LogHistoryError::TimestampOutOfRange { timestamp, bound });
    }

    let lo_version = log_segment.listed.ascending_commit_files[lo].version;
    let hi_version = log_segment.listed.ascending_commit_files[hi - 1].version;
    info!(
        ?search_bounds,
        lo_version, hi_version, "Timestamp search bounds"
    );

    // We only search in the range [lo..hi).
    let commit_range = &log_segment.listed.ascending_commit_files[lo..hi];

    // Key function that extracts the timestamp from a commit.
    let commit_to_ts = |commit: &ParsedLogPath| -> Result<Timestamp, LogHistoryError> {
        if read_ict {
            info!(version = commit.version, "Reading in-commit timestamp");
            commit
                .read_in_commit_timestamp(engine)
                .map_err(|e| LogHistoryError::internal("failed to read in-commit timestamp", e))
        } else {
            Ok(commit.location.last_modified)
        }
    };

    let search_result =
        binary_search_by_key_with_bounds(commit_range, timestamp, commit_to_ts, bound);

    match search_result {
        Ok(relative_idx) => {
            // `relative_idx` is for range [lo..hi]. Add back `lo` to get absolute index.
            let idx = lo + relative_idx;
            debug_assert!(idx < len, "Relative index should become a valid index");
            Ok(log_segment.listed.ascending_commit_files[idx].version)
        }
        Err(SearchError::KeyFunctionError(error)) => Err(error),
        // Special case: For LeastUpper search over file modification timestamps, the ICT
        // enablement version serves as the upper bound when no version was found in range.
        Err(SearchError::OutOfRange) => match (&search_bounds, bound) {
            (
                TimestampSearchBounds::FileModificationSearchUntil {
                    ict_enablement_version,
                    ..
                },
                Bound::LeastUpper,
            ) => Ok(*ict_enablement_version),
            _ => Err(LogHistoryError::TimestampOutOfRange { timestamp, bound }),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use test_utils::delta_path_for_version;
    use url::Url;

    use super::*;
    use crate::actions::{CommitInfo, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::snapshot::Snapshot;
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::{Action, LocalMockTable};
    use crate::Version;

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
    async fn test_timestamp_search_bounds_no_ict() {
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
    async fn test_timestamp_search_bounds_with_ict_range() {
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
    async fn test_reading_in_commit_timestamp() {
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
        let commits = log_segment.listed.ascending_commit_files;

        // File that has no In-commit timestamps
        let mut res = commits[0].read_in_commit_timestamp(&engine);
        assert!(res.is_err(), "Expected error for file without ICT: {res:?}");

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
            res.is_err(),
            "Expected error for non-existent file: {res:?}"
        );

        // Files with In-commit timestamps
        res = commits[3].read_in_commit_timestamp(&engine);
        assert!(matches!(res, Ok(300)), "{res:?}");
        res = commits[4].read_in_commit_timestamp(&engine);
        assert!(matches!(res, Ok(400)), "{res:?}");
    }

    #[tokio::test]
    async fn test_file_modification_conversion() {
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
        let commits = log_segment.listed.ascending_commit_files;

        // Read the file modification timestamps
        let ts: Result<Timestamp, LogHistoryError> = Ok(commits[0].location.last_modified);
        assert!(matches!(ts, Ok(50)));

        let ts: Result<Timestamp, LogHistoryError> = Ok(commits[1].location.last_modified);
        assert!(matches!(ts, Ok(150)));

        let ts: Result<Timestamp, LogHistoryError> = Ok(commits[2].location.last_modified);
        assert!(matches!(ts, Ok(250)));

        // Read the in-commit timestamps
        let ts = commits[3].read_in_commit_timestamp(&engine);
        assert!(matches!(ts, Ok(300)));
        let ts = commits[4].read_in_commit_timestamp(&engine);
        assert!(matches!(ts, Ok(400)));
    }

    #[tokio::test]
    async fn test_convert_timestamp_to_version() {
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

        // GreatestLower Bound on timestamp that is less than all commits fails
        res = timestamp_to_version(&snapshot, &engine, 0, Bound::GreatestLower);
        assert!(
            matches!(
                res,
                Err(LogHistoryError::TimestampOutOfRange {
                    timestamp: 0,
                    bound: Bound::GreatestLower,
                })
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
                Err(LogHistoryError::TimestampOutOfRange {
                    timestamp: 1000,
                    bound: Bound::LeastUpper,
                })
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
