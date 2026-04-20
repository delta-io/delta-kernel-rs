//! This module provides functions for performing timestamp queries over the Delta Log, translating
//! between timestamps and Delta versions.

use std::cmp::Ordering;

use error::LogHistoryError;
use search::{binary_search_by_key_with_bounds, Bound, SearchError};
use tracing::{info, warn};

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

/// Determines the search strategy for timestamp-to-version conversion based on ICT enablement.
#[allow(unused)]
#[derive(Debug, PartialEq, Eq)]
enum TimestampSearchBounds {
    /// The timestamp exactly matches the ICT enablement timestamp.
    ExactMatch(Version),
    /// Search over In-Commit Timestamps starting from the given index.
    ICTSearchStartingFrom(usize),
    /// Search over file modification timestamps (ICT not enabled).
    FileModificationSearch,
    /// Search over file modification timestamps up to the ICT enablement point.
    FileModificationSearchUntil {
        /// The index in the commit list where ICT begins.
        index: usize,
        /// The version at which ICT was enabled.
        ict_enablement_version: Version,
    },
}

/// Given a timestamp, this function determines the commit range that timestamp conversion
/// should search. A timestamp search may be conducted over one of two version ranges:
///     1) A range of commits whose timestamp is the file modification timestamp
///     2) A range of commits whose timestamp is an in-commit timestamp.
#[allow(unused)]
#[tracing::instrument(skip(snapshot, log_segment), ret)]
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

/// Performs a linear search over file modification timestamps with monotonization.
///
/// File modification timestamps are not guaranteed to be monotonically increasing due to clock
/// skew or filesystem limitations. This function ensures correctness by monotonizing timestamps
/// as it scans: if a commit's timestamp is less than or equal to the previous commit's timestamp,
/// it is adjusted to `previous_timestamp + 1`.
///
/// # Arguments
/// * `commits` - Slice of commits to search, ordered by version (ascending)
/// * `timestamp` - The target timestamp to search for
/// * `bound` - The type of bound to find (GreatestLower or LeastUpper)
///
/// # Returns
/// The version matching the bound criteria, or an error if no match exists.
//
// NOTE: Monotonization trade-offs
//
// File modification timestamps can have clock skew. We monotonize over visible history
// only, which may produce incorrect results for commits near the retention boundary if
// metadata cleanup removed skewed commits that affected monotonization.
//
// Since ICT is the correct long-term solution for timestamps, we avoid over-investing
// in file modification timestamp handling.
//
// TODO: Today we read full commit history for timestamp conversion, which is expensive.
// A future backwards-listing optimization would avoid reading full history - at that
// point, monotonizing over full history becomes a bad trade-off and should be revisited.
#[allow(unused)]
fn linear_search_file_mod_timestamps(
    commits: &[ParsedLogPath],
    timestamp: Timestamp,
    bound: Bound,
) -> Result<Version, LogHistoryError> {
    if commits.is_empty() {
        return Err(LogHistoryError::TimestampOutOfRange { timestamp, bound });
    }

    let lo_version = commits[0].version;
    let hi_version = commits[commits.len() - 1].version;
    info!(lo_version, hi_version, "File modification linear search");

    let mut result: Option<Version> = None;
    let mut prev_monotonic_ts = i64::MIN;

    for commit in commits {
        let raw_ts = commit.location.last_modified;
        // Monotonize: ensure each timestamp is strictly greater than the previous
        let monotonic_ts = raw_ts.max(prev_monotonic_ts + 1);
        if monotonic_ts != raw_ts {
            warn!(
                version = commit.version,
                raw_ts, monotonic_ts, "Adjusted non-monotonic timestamp"
            );
        }

        match bound {
            // GreatestLower: update result until we surpass `timestamp`
            Bound::GreatestLower if monotonic_ts <= timestamp => result = Some(commit.version),
            Bound::GreatestLower => break,
            // LeastUpper: return version upon first match
            Bound::LeastUpper if monotonic_ts >= timestamp => return Ok(commit.version),
            Bound::LeastUpper => {}
        }

        prev_monotonic_ts = monotonic_ts;
    }

    result.ok_or(LogHistoryError::TimestampOutOfRange { timestamp, bound })
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

    match search_bounds {
        TimestampSearchBounds::ExactMatch(version) => Ok(version),

        // ICT search: use binary search (ICT timestamps are guaranteed monotonic by protocol)
        TimestampSearchBounds::ICTSearchStartingFrom(lo) => {
            let commit_range = &log_segment.listed.ascending_commit_files[lo..];
            if commit_range.is_empty() {
                return Err(LogHistoryError::TimestampOutOfRange { timestamp, bound });
            }

            let lo_version = commit_range.first().map(|c| c.version).unwrap_or(0);
            let hi_version = commit_range.last().map(|c| c.version).unwrap_or(0);
            info!(
                lo_version,
                hi_version, "ICT binary search over version range"
            );

            let commit_to_ict = |commit: &ParsedLogPath| -> Result<Timestamp, LogHistoryError> {
                commit
                    .read_in_commit_timestamp(engine)
                    .map_err(|e| LogHistoryError::internal("failed to read in-commit timestamp", e))
            };

            let search_result =
                binary_search_by_key_with_bounds(commit_range, timestamp, commit_to_ict, bound);

            match search_result {
                Ok(relative_idx) => {
                    let idx = lo + relative_idx;
                    debug_assert!(idx < len, "Index should be valid");
                    Ok(log_segment.listed.ascending_commit_files[idx].version)
                }
                Err(SearchError::KeyFunctionError(error)) => Err(error),
                Err(SearchError::OutOfRange) => {
                    Err(LogHistoryError::TimestampOutOfRange { timestamp, bound })
                }
            }
        }

        // File modification search: use linear scan with monotonization
        // (file mod timestamps are NOT guaranteed monotonic due to clock skew)
        TimestampSearchBounds::FileModificationSearch => {
            let commits = &log_segment.listed.ascending_commit_files[..];
            linear_search_file_mod_timestamps(commits, timestamp, bound)
        }

        // File modification search up to ICT enablement point
        TimestampSearchBounds::FileModificationSearchUntil {
            index,
            ict_enablement_version,
        } => {
            let commits = &log_segment.listed.ascending_commit_files[..index];
            linear_search_file_mod_timestamps(commits, timestamp, bound).or_else(|e| {
                // For LeastUpper, if no version found, the ICT enablement version is the answer
                if bound == Bound::LeastUpper {
                    Ok(ict_enablement_version)
                } else {
                    Err(e)
                }
            })
        }
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

    fn get_test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]))
    }

    fn set_mod_time(mock_table: &LocalMockTable, version: Version, timestamp: Timestamp) {
        let file_name = delta_path_for_version(version, "json")
            .filename()
            .unwrap()
            .to_string();
        let path = mock_table.table_root().join("_delta_log/").join(file_name);
        let file = OpenOptions::new().write(true).open(path).unwrap();
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp.try_into().unwrap());
        file.set_modified(time).unwrap();
    }

    /// Creates a test table with specified timestamps.
    ///
    /// # Arguments
    /// * `timestamps` - List of `(file_mod_ts, Option<ict_ts>)` for each version
    /// * `ict_enablement_version` - Version where ICT is enabled:
    ///   - `None`: ICT never enabled (file mod only)
    ///   - `Some(0)`: ICT enabled from creation
    ///   - `Some(n)`: ICT enabled at version n
    async fn mock_table_with_timestamps(
        timestamps: &[(Timestamp, Option<Timestamp>)],
        ict_enablement_version: Option<Version>,
    ) -> LocalMockTable {
        let mut mock_table = LocalMockTable::new();

        for (version, (file_mod_ts, ict_ts)) in timestamps.iter().enumerate() {
            let version = version as Version;
            let is_first = version == 0;
            let is_ict_enablement = ict_enablement_version == Some(version);
            let ict_from_creation = ict_enablement_version == Some(0) && is_first;

            let mut actions: Vec<Action> = vec![];

            // CommitInfo with optional ICT (must be first when ICT is present)
            if ict_ts.is_some() {
                actions.push(Action::CommitInfo(CommitInfo {
                    in_commit_timestamp: *ict_ts,
                    ..Default::default()
                }));
            }

            // Metadata: on first commit, or when enabling ICT
            if is_first || is_ict_enablement {
                let config = if ict_from_creation {
                    // ICT enabled from creation: no enablement version/timestamp
                    HashMap::from_iter([(
                        "delta.enableInCommitTimestamps".to_string(),
                        "true".to_string(),
                    )])
                } else if is_ict_enablement {
                    // ICT enabled at this version
                    HashMap::from_iter([
                        (
                            "delta.enableInCommitTimestamps".to_string(),
                            "true".to_string(),
                        ),
                        (
                            "delta.inCommitTimestampEnablementVersion".to_string(),
                            version.to_string(),
                        ),
                        (
                            "delta.inCommitTimestampEnablementTimestamp".to_string(),
                            ict_ts.unwrap().to_string(),
                        ),
                    ])
                } else {
                    HashMap::new()
                };
                actions.push(Action::Metadata(
                    Metadata::try_new(None, None, get_test_schema(), vec![], 0, config).unwrap(),
                ));
            }

            // Protocol on first commit
            if is_first {
                let writer_features: Option<Vec<TableFeature>> = if ict_enablement_version.is_some()
                {
                    Some(vec![TableFeature::InCommitTimestamp])
                } else {
                    Some(vec![])
                };
                actions.push(Action::Protocol(
                    Protocol::try_new(3, 7, Some(Vec::<String>::new()), writer_features).unwrap(),
                ));
            }

            // If no actions yet (non-first commit without ICT), add empty CommitInfo
            if actions.is_empty() {
                actions.push(Action::CommitInfo(CommitInfo::default()));
            }

            mock_table.commit(actions).await;
            set_mod_time(&mock_table, version, *file_mod_ts);
        }

        mock_table
    }

    /// Helper to build snapshot and log segment
    async fn build_test_snapshot(
        timestamps: &[(Timestamp, Option<Timestamp>)],
        ict_enablement: Option<Version>,
        at_version: Option<Version>,
    ) -> (LocalMockTable, SyncEngine, Arc<Snapshot>, LogSegment) {
        let table = mock_table_with_timestamps(timestamps, ict_enablement).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let mut builder = Snapshot::builder_for(path);
        if let Some(v) = at_version {
            builder = builder.at_version(v);
        }
        let snapshot = builder.build(&engine).unwrap();
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            None,
        )
        .unwrap();
        (table, engine, snapshot, log_segment)
    }

    // Table: v0=50, v1=150, v2=250 (file mod only), snapshot at v2 (no ICT)
    #[rstest::rstest]
    #[case::before_all(0)]
    #[case::within_range(100)]
    #[case::after_all(1000)]
    #[tokio::test]
    async fn test_search_bounds_no_ict(#[case] timestamp: Timestamp) {
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let (_table, _engine, snapshot, log_segment) =
            build_test_snapshot(&timestamps, Some(3), Some(2)).await;

        let res = get_timestamp_search_bounds(&snapshot, &log_segment, timestamp).unwrap();
        assert!(
            matches!(res, TimestampSearchBounds::FileModificationSearch),
            "{res:?}"
        );
    }

    // Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT enabled at v3)
    #[rstest::rstest]
    #[case::file_mod_region(50, TimestampSearchBounds::FileModificationSearchUntil { index: 3, ict_enablement_version: 3 })]
    #[case::file_mod_between(60, TimestampSearchBounds::FileModificationSearchUntil { index: 3, ict_enablement_version: 3 })]
    #[case::file_mod_last(299, TimestampSearchBounds::FileModificationSearchUntil { index: 3, ict_enablement_version: 3 })]
    #[case::exact_enablement(300, TimestampSearchBounds::ExactMatch(3))]
    #[case::ict_after_enablement(301, TimestampSearchBounds::ICTSearchStartingFrom(3))]
    #[case::ict_far_future(1000, TimestampSearchBounds::ICTSearchStartingFrom(3))]
    #[tokio::test]
    async fn test_search_bounds_with_ict(
        #[case] timestamp: Timestamp,
        #[case] expected: TimestampSearchBounds,
    ) {
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let (_table, _engine, snapshot, log_segment) =
            build_test_snapshot(&timestamps, Some(3), None).await;

        let res = get_timestamp_search_bounds(&snapshot, &log_segment, timestamp).unwrap();
        assert_eq!(res, expected);
    }

    // Table: v0=100, v1=200, v2=300 (ICT from creation)
    #[rstest::rstest]
    #[case::before_all(50)]
    #[case::at_v0(100)]
    #[case::between_v0_v1(150)]
    #[case::after_all(1000)]
    #[tokio::test]
    async fn test_search_bounds_ict_from_creation(#[case] timestamp: Timestamp) {
        let timestamps = [(0, Some(100)), (0, Some(200)), (0, Some(300))];
        let (_table, _engine, snapshot, log_segment) =
            build_test_snapshot(&timestamps, Some(0), None).await;

        let res = get_timestamp_search_bounds(&snapshot, &log_segment, timestamp).unwrap();
        assert!(
            matches!(res, TimestampSearchBounds::ICTSearchStartingFrom(0)),
            "{res:?}"
        );
    }

    /// Tests reading timestamps from commits - both file modification and ICT.
    #[tokio::test]
    async fn test_reading_commit_timestamps() {
        // Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT at v3)
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let (_table, engine, _snapshot, log_segment) =
            build_test_snapshot(&timestamps, Some(3), None).await;
        let commits = &log_segment.listed.ascending_commit_files;

        // File modification timestamps are set correctly
        assert_eq!(commits[0].location.last_modified, 50);
        assert_eq!(commits[1].location.last_modified, 150);
        assert_eq!(commits[2].location.last_modified, 250);

        // Reading ICT from file without ICT returns error
        let res = commits[0].read_in_commit_timestamp(&engine);
        assert!(res.is_err(), "Expected error for file without ICT: {res:?}");

        // Reading ICT from non-existent file returns error
        let mut fake_log_path = commits[0].clone();
        let failing_path = if cfg!(windows) {
            "C:\\phony\\path"
        } else {
            "/phony/path"
        };
        fake_log_path.location.location = Url::from_file_path(failing_path).unwrap();
        let res = fake_log_path.read_in_commit_timestamp(&engine);
        assert!(
            res.is_err(),
            "Expected error for non-existent file: {res:?}"
        );

        // In-commit timestamps read correctly
        assert_eq!(commits[3].read_in_commit_timestamp(&engine).unwrap(), 300);
        assert_eq!(commits[4].read_in_commit_timestamp(&engine).unwrap(), 400);
    }

    // Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT at v3)
    #[rstest::rstest]
    #[case::before_all_lub(0, Bound::LeastUpper, Some(0))]
    #[case::before_all_glb(0, Bound::GreatestLower, None)]
    #[case::negative_lub(-1, Bound::LeastUpper, Some(0))]
    #[case::after_all_lub(1000, Bound::LeastUpper, None)]
    #[case::after_all_glb(1000, Bound::GreatestLower, Some(4))]
    #[case::exact_v0_lub(50, Bound::LeastUpper, Some(0))]
    #[case::exact_v0_glb(50, Bound::GreatestLower, Some(0))]
    #[case::exact_v1_lub(150, Bound::LeastUpper, Some(1))]
    #[case::exact_v1_glb(150, Bound::GreatestLower, Some(1))]
    #[case::exact_v3_ict_lub(300, Bound::LeastUpper, Some(3))]
    #[case::exact_v3_ict_glb(300, Bound::GreatestLower, Some(3))]
    #[case::between_v0_v1_lub(100, Bound::LeastUpper, Some(1))]
    #[case::between_v0_v1_glb(100, Bound::GreatestLower, Some(0))]
    #[case::just_after_last_file_mod_lub(251, Bound::LeastUpper, Some(3))]
    #[case::just_after_last_file_mod_glb(251, Bound::GreatestLower, Some(2))]
    #[case::just_before_ict_lub(299, Bound::LeastUpper, Some(3))]
    #[case::just_before_ict_glb(299, Bound::GreatestLower, Some(2))]
    #[case::just_after_ict_glb(301, Bound::GreatestLower, Some(3))]
    #[case::just_after_ict_lub(301, Bound::LeastUpper, Some(4))]
    #[tokio::test]
    async fn test_timestamp_to_version_standard_table(
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected: Option<Version>,
    ) {
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let (_table, engine, snapshot, _log_segment) =
            build_test_snapshot(&timestamps, Some(3), None).await;

        let res = timestamp_to_version(&snapshot, &engine, timestamp, bound);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(
                matches!(res, Err(LogHistoryError::TimestampOutOfRange { .. })),
                "{res:?}"
            ),
        }
    }

    // Table: v0=100, v1=200, v2=300 (ICT from creation)
    #[rstest::rstest]
    #[case::exact_v0_glb(100, Bound::GreatestLower, Some(0))]
    #[case::exact_v0_lub(100, Bound::LeastUpper, Some(0))]
    #[case::between_v0_v1_glb(150, Bound::GreatestLower, Some(0))]
    #[case::between_v0_v1_lub(150, Bound::LeastUpper, Some(1))]
    #[case::between_v1_v2_glb(250, Bound::GreatestLower, Some(1))]
    #[case::between_v1_v2_lub(250, Bound::LeastUpper, Some(2))]
    #[case::before_all_glb(50, Bound::GreatestLower, None)]
    #[case::before_all_lub(50, Bound::LeastUpper, Some(0))]
    #[tokio::test]
    async fn test_ict_from_creation(
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected: Option<Version>,
    ) {
        let mock_table =
            mock_table_with_timestamps(&[(0, Some(100)), (0, Some(200)), (0, Some(300))], Some(0))
                .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_to_version(&snapshot, &engine, timestamp, bound);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(
                matches!(res, Err(LogHistoryError::TimestampOutOfRange { .. })),
                "{res:?}"
            ),
        }
    }

    /// Single commit table: v0=100 (file mod only)
    #[rstest::rstest]
    #[case::exact_glb(100, Bound::GreatestLower, Some(0))]
    #[case::exact_lub(100, Bound::LeastUpper, Some(0))]
    #[case::before_glb(50, Bound::GreatestLower, None)]
    #[case::before_lub(50, Bound::LeastUpper, Some(0))]
    #[case::after_glb(150, Bound::GreatestLower, Some(0))]
    #[case::after_lub(150, Bound::LeastUpper, None)]
    #[tokio::test]
    async fn test_single_commit(
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected: Option<Version>,
    ) {
        let mock_table = mock_table_with_timestamps(&[(100, None)], None).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_to_version(&snapshot, &engine, timestamp, bound);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(
                matches!(res, Err(LogHistoryError::TimestampOutOfRange { .. })),
                "{res:?}"
            ),
        }
    }

    /// Non-monotonic file mod timestamps (clock skew):
    /// Raw: v0=100, v1=200, v2=150, v3=180, v4=300
    /// Monotonized: v0=100, v1=200, v2=201, v3=202, v4=300
    #[rstest::rstest]
    #[case::exact_v0_glb(100, Bound::GreatestLower, Some(0))]
    #[case::exact_v1_glb(200, Bound::GreatestLower, Some(1))]
    #[case::monotonized_v2_glb(201, Bound::GreatestLower, Some(2))]
    #[case::monotonized_v3_glb(202, Bound::GreatestLower, Some(3))]
    #[case::between_v3_v4_glb(250, Bound::GreatestLower, Some(3))]
    #[case::exact_v4_glb(300, Bound::GreatestLower, Some(4))]
    #[case::raw_v2_maps_to_v1_lub(150, Bound::LeastUpper, Some(1))]
    #[case::monotonized_v2_lub(201, Bound::LeastUpper, Some(2))]
    #[case::after_v3_monotonized_lub(203, Bound::LeastUpper, Some(4))]
    #[tokio::test]
    async fn test_non_monotonic_timestamps(
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected: Option<Version>,
    ) {
        let mock_table = mock_table_with_timestamps(
            &[
                (100, None),
                (200, None),
                (150, None),
                (180, None),
                (300, None),
            ],
            None,
        )
        .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_to_version(&snapshot, &engine, timestamp, bound);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(
                matches!(res, Err(LogHistoryError::TimestampOutOfRange { .. })),
                "{res:?}"
            ),
        }
    }

    /// ICT enabled at v3 but v4 is missing its ICT timestamp (error case).
    /// Table: v0=50, v1=150, v2=250 (file mod), v3=300 (ICT), v4=450 (missing ICT)
    #[tokio::test]
    async fn test_missing_ict_after_enablement() {
        // Create table where v4 is after ICT enablement but doesn't have an ICT
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)), // ICT enabled at v3
            (450, None),      // v4 missing ICT (bug in writer!)
        ];
        let table = mock_table_with_timestamps(&timestamps, Some(3)).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        // Search for timestamp 350 which would need to search in ICT range (v3+)
        // The search will try to read ICT from v4 which is missing
        let res = timestamp_to_version(&snapshot, &engine, 350, Bound::LeastUpper);
        assert!(
            matches!(res, Err(LogHistoryError::Internal { .. })),
            "Expected internal error for missing ICT: {res:?}"
        );
    }
}
