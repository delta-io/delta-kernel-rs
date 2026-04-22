// Low-level timestamp conversion internals. Made public in a later PR.
#![allow(unused)]

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
//! Timestamps (ICT) enabled. ICT is a Delta table feature that stores precise commit timestamps
//! in the commit metadata rather than relying on file modification times, which can be affected
//! by clock skew or filesystem limitations.
//!
//! # Limitations
//!
//! All timestamp queries are limited to the state captured in the [`Snapshot`] provided during
//! construction. For tables without ICT enabled, timestamp queries rely on file modification
//! timestamps, which are not guaranteed to be monotonically increasing across commits.

use std::cmp::Ordering;

use error::LogHistoryError;
use search::{binary_search_by_key_with_bounds, Bound, SearchError};
use tracing::{info, trace};

use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::snapshot::Snapshot;
use crate::table_configuration::InCommitTimestampEnablement;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error as DeltaError, Version};

pub(crate) mod search;

pub(crate) mod error;

/// A timestamp representing milliseconds since the Unix epoch (1970-01-01 00:00:00 UTC).
pub(crate) type Timestamp = i64;

/// Determines the search strategy for timestamp-to-version conversion based on ICT enablement.
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

/// Determines the search strategy for timestamp-to-version conversion based on ICT enablement.
///
/// Given a timestamp, this function determines which commit range to search and what timestamp
/// source to use (file modification time vs in-commit timestamp). A timestamp search may be
/// conducted over one of two version ranges:
///   1) A range of commits whose timestamp is the file modification timestamp
///   2) A range of commits whose timestamp is an in-commit timestamp
///
/// # Returns
///
/// - [`TimestampSearchBounds::FileModificationSearch`]: ICT not enabled; search all commits using
///   file modification timestamps.
/// - [`TimestampSearchBounds::ICTSearchStartingFrom`]: Search commits with ICT enabled using
///   in-commit timestamps (binary search).
/// - [`TimestampSearchBounds::FileModificationSearchUntil`]: Timestamp is before ICT enablement;
///   search pre-ICT commits using file modification timestamps.
/// - [`TimestampSearchBounds::ExactMatch`]: Timestamp exactly matches the ICT enablement timestamp.
///
/// # Errors
///
/// Returns [`LogHistoryError::Internal`] if:
/// - Failed to read table configuration
/// - ICT enablement version is beyond the log segment (indicates corrupted metadata)
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

    // Fast path: if the first commit is at or after ICT enablement (e.g., old commits cleaned
    // up), all commits in the segment have ICT - skip binary search.
    let first_version = log_segment.listed.ascending_commit_files[0].version;
    if first_version >= ict_enablement_version {
        return Ok(TimestampSearchBounds::ICTSearchStartingFrom(0));
    }

    // Get the index of the ICT enablement version in the log segment. If the version is not
    // present, binary_search returns the insertion point (first commit at or after).
    let commit_count = log_segment.listed.ascending_commit_files.len();
    let ict_enablement_idx = log_segment
        .listed
        .ascending_commit_files
        .binary_search_by(|x| x.version.cmp(&ict_enablement_version))
        .unwrap_or_else(|idx| idx);

    // Invariant: ICT enablement version must be within the log segment. If it equals
    // commit_count, the enablement version is beyond the table's latest version.
    require!(
        ict_enablement_idx < commit_count,
        LogHistoryError::internal_message(
            "ICT enablement version is beyond the table's latest version"
        )
    );

    // Per PROTOCOL.md section on In-Commit Timestamps:
    //   - ts >= enablementTs => consider only versions >= enablementVersion (ICT region)
    //   - ts <  enablementTs => consider only versions <  enablementVersion (file mod region)
    let result = match timestamp.cmp(&ict_timestamp) {
        // Equal: enablementTs IS the ICT of enablementVersion, so this version is the exact
        // answer for both bounds (GreatestLower and LeastUpper).
        Ordering::Equal => TimestampSearchBounds::ExactMatch(ict_enablement_version),
        // Less: Restrict search to the pre-ICT region via FileModificationSearchUntil. The
        // protocol guarantees enablementTs > prev_mod_time, so when the pre-ICT linear scan
        // fails for LeastUpper, enablementVersion is the true least upper bound.
        Ordering::Less => TimestampSearchBounds::FileModificationSearchUntil {
            index: ict_enablement_idx,
            ict_enablement_version,
        },
        // Greater: Restrict search to ICT commits only via ICTSearchStartingFrom.
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
fn linear_search_file_mod_timestamps(
    commits: &[ParsedLogPath],
    timestamp: Timestamp,
    bound: Bound,
) -> Result<Version, LogHistoryError> {
    if commits.is_empty() {
        return Err(LogHistoryError::TimestampOutOfRange {
            timestamp,
            reason: bound.out_of_range_reason(),
        });
    }

    let lo_version = commits[0].version;
    let hi_version = commits[commits.len() - 1].version;
    info!(lo_version, hi_version, "File modification linear search");

    let mut result: Option<Version> = None;
    let mut prev_monotonic_ts = i64::MIN;

    for commit in commits {
        let raw_ts = commit.location.last_modified;
        // Monotonize: ensure each timestamp is strictly greater than the previous
        let monotonic_ts = raw_ts.max(prev_monotonic_ts.saturating_add(1));
        if monotonic_ts != raw_ts {
            trace!(
                version = commit.version,
                raw_ts,
                monotonic_ts,
                "Adjusted non-monotonic timestamp"
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

    result.ok_or(LogHistoryError::TimestampOutOfRange {
        timestamp,
        reason: bound.out_of_range_reason(),
    })
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
/// - `Bound::GreatestLower`: There is no commit whose timestamp is less than or equal to the given
///   `timestamp`.
/// - `Bound::LeastUpper`: There is no commit whose timestamp is greater than or equal to the given
///   `timestamp`.
pub(crate) fn timestamp_to_version(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    timestamp: Timestamp,
    bound: Bound,
) -> Result<Version, LogHistoryError> {
    // TODO(#2443): Support staged commits (catalog-managed tables). Staged commits have ICT
    // (required by catalog-managed), so timestamps are available. However, we currently
    // rebuild the log segment from storage which doesn't include staged commits. To support
    // this, we'd need to pass the snapshot's log_tail into the log segment construction.
    let has_staged_commits = snapshot
        .log_segment()
        .listed
        .ascending_commit_files
        .last() // Use last since a log segment always ends with staged commits from `log_tail`
        .is_some_and(|c| c.file_type == crate::path::LogPathFileType::StagedCommit);
    if has_staged_commits {
        return Err(LogHistoryError::internal_message(
            "timestamp conversion not yet supported for snapshots with staged commits",
        ));
    }

    // Build a dedicated log segment for timestamp conversion. We cannot reuse
    // snapshot.log_segment() because it may include a checkpoint prefix with gaps in
    // the commit sequence. for_timestamp_conversion returns only contiguous commits,
    // which is required for correct timestamp-to-version mapping.
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
                return Err(LogHistoryError::TimestampOutOfRange {
                    timestamp,
                    reason: bound.out_of_range_reason(),
                });
            }

            let lo_version = commit_range[0].version;
            let hi_version = commit_range[commit_range.len() - 1].version;
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
                Err(SearchError::OutOfRange) => Err(LogHistoryError::TimestampOutOfRange {
                    timestamp,
                    reason: bound.out_of_range_reason(),
                }),
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
                // For LeastUpper, if no version found in pre-ICT region, the ICT enablement
                // version is the answer (protocol guarantees enablementTs > prev_mod_time)
                match (&e, bound) {
                    (LogHistoryError::TimestampOutOfRange { .. }, Bound::LeastUpper) => {
                        Ok(ict_enablement_version)
                    }
                    _ => Err(e),
                }
            })
        }
    }
}

/// Gets the latest version that occurs before or at the given `timestamp`.
///
/// This finds the version whose timestamp is less than or equal to `timestamp`.
/// If no such version exists, returns [`LogHistoryError::TimestampOutOfRange`].
///
/// # Examples
/// ```ignore
/// use delta_kernel::snapshot::Snapshot;
/// use delta_kernel::engine::default::DefaultEngine;
/// use delta_kernel::history_manager::latest_version_as_of;
///
/// let engine = DefaultEngine::try_new(...)?;
/// let snapshot = Snapshot::builder_for(table_uri).build(&engine)?;
///
/// // Get the latest version as of January 1, 2023
/// let timestamp = 1672531200000; // Milliseconds since epoch for 2023-01-01
/// let version = latest_version_as_of(&snapshot, &engine, timestamp)?;
/// ```
#[tracing::instrument(skip(snapshot, engine), ret, fields(latest_version = snapshot.version(), table_root = %snapshot.table_root()))]
pub fn latest_version_as_of(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    timestamp: Timestamp,
) -> DeltaResult<Version> {
    timestamp_to_version(snapshot, engine, timestamp, Bound::GreatestLower).map_err(Into::into)
}

/// Gets the first version that occurs at or after the given `timestamp`.
///
/// This finds the version whose timestamp is greater than or equal to `timestamp`.
/// If no such version exists, returns [`LogHistoryError::TimestampOutOfRange`].
///
/// # Examples
/// ```ignore
/// use delta_kernel::snapshot::Snapshot;
/// use delta_kernel::engine::default::DefaultEngine;
/// use delta_kernel::history_manager::first_version_after;
///
/// let engine = DefaultEngine::try_new(...)?;
/// let snapshot = Snapshot::builder_for(table_uri).build(&engine)?;
///
/// // Find the first version that occurred after January 1, 2023
/// let timestamp = 1672531200000; // Milliseconds since epoch for 2023-01-01
/// let version = first_version_after(&snapshot, &engine, timestamp)?;
/// ```
#[tracing::instrument(skip(snapshot, engine), ret, fields(latest_version = snapshot.version(), table_root = %snapshot.table_root()))]
pub fn first_version_after(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    timestamp: Timestamp,
) -> DeltaResult<Version> {
    timestamp_to_version(snapshot, engine, timestamp, Bound::LeastUpper).map_err(Into::into)
}

/// Converts a timestamp range to a corresponding version range.
///
/// This function finds the version range that corresponds to the given timestamp range.
/// The returned tuple contains:
/// - The first (earliest) version with a timestamp greater than or equal to `start_timestamp`
/// - If `end_timestamp` is provided, the version with a timestamp less than or equal to
///   `end_timestamp`.
///
/// # Arguments
/// * `snapshot` - The snapshot that defines the searchable version range
/// * `engine` - The engine used to access version history
/// * `start_timestamp` - The lower bound timestamp (inclusive), in milliseconds since Unix epoch
/// * `end_timestamp` - The optional upper bound timestamp (inclusive), or `None` to indicate no
///   upper bound
///
/// # Returns
/// A tuple containing the start version and optional end version (inclusive)
///
/// # Errors
/// Returns [`LogHistoryError::InvalidTimestampRange`] if `start_timestamp > end_timestamp`.
///
/// Returns [`LogHistoryError::TimestampOutOfRange`] if:
/// - No version exists at or after `start_timestamp`
/// - `end_timestamp` is provided and no version exists at or before it
///
/// Returns [`LogHistoryError::EmptyTimestampRange`] if the entire range falls between two commits.
///
/// # Examples
/// ```ignore
/// use delta_kernel::snapshot::Snapshot;
/// use delta_kernel::engine::default::DefaultEngine;
/// use delta_kernel::history_manager::timestamp_range_to_versions;
///
/// let engine = DefaultEngine::try_new(...)?;
/// let snapshot = Snapshot::builder_for(table_uri).build(&engine)?;
///
/// // Find versions between January 1, 2023 and March 1, 2023
/// let start_timestamp = 1672531200000; // Jan 1, 2023 (milliseconds since epoch)
/// let end_timestamp = 1677628800000;   // Mar 1, 2023 (milliseconds since epoch)
///
/// let (start_version, end_version) =
///     timestamp_range_to_versions(&snapshot, &engine, start_timestamp, Some(end_timestamp))?;
/// ```
#[tracing::instrument(skip(snapshot, engine), ret, fields(latest_version = snapshot.version(), table_root = %snapshot.table_root()))]
pub fn timestamp_range_to_versions(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    start_timestamp: Timestamp,
    end_timestamp: Option<Timestamp>,
) -> DeltaResult<(Version, Option<Version>)> {
    if let Some(end_timestamp) = end_timestamp {
        // The `start_timestamp` must be no greater than the `end_timestamp`.
        require!(
            start_timestamp <= end_timestamp,
            LogHistoryError::InvalidTimestampRange {
                start_timestamp,
                end_timestamp
            }
            .into()
        );
    }

    // Convert the start timestamp to version
    let start_version = first_version_after(snapshot, engine, start_timestamp)?;

    // If the end timestamp is present, convert it to an end version
    let end_version = end_timestamp
        .map(|end| {
            let end_version = latest_version_as_of(snapshot, engine, end)?;

            // Verify that the start version is no greater than the end version. This can
            // happen in the case that the entire timestamp range falls between two commits.
            // Consider the following history:
            // |-------------|--------------------|---------------|
            // v4       start_timestamp      end_timestamp       v5
            //
            // The latest version as of the end_timestamp is 4. The first version after the
            // start_timestamp is 5. Thus in the case where end_version < start_version, we
            // return an [`LogHistoryError::EmptyTimestampRange`].
            require!(
                start_version <= end_version,
                DeltaError::from(LogHistoryError::EmptyTimestampRange {
                    start_timestamp,
                    end_timestamp: end,
                    between_version: end_version,
                })
            );

            Ok(end_version)
        })
        .transpose()?;

    Ok((start_version, end_version))
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

    // ====================================================================================
    // Tests for low-level internal functions: get_timestamp_search_bounds,
    // timestamp_to_version, linear_search_file_mod_timestamps
    // ====================================================================================

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
    #[case::between_ict_commits(350, Bound::GreatestLower, Some(3))]
    #[case::between_ict_commits_lub(350, Bound::LeastUpper, Some(4))]
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

    // ====================================================================================
    // Tests for high-level public APIs: latest_version_as_of, first_version_after,
    // timestamp_range_to_versions
    // ====================================================================================

    // Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT at v3)
    // Matches test_timestamp_to_version_standard_table cases for GreatestLower bound
    #[rstest::rstest]
    #[case::before_all(0, None)]
    #[case::exact_v0(50, Some(0))]
    #[case::between_v0_v1(100, Some(0))]
    #[case::exact_v1(150, Some(1))]
    #[case::just_after_last_file_mod(251, Some(2))]
    #[case::just_before_ict(299, Some(2))]
    #[case::exact_ict_enablement(300, Some(3))]
    #[case::just_after_ict(301, Some(3))]
    #[case::between_ict_commits(350, Some(3))]
    #[case::after_all(1000, Some(4))]
    #[tokio::test]
    async fn test_latest_version_as_of(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<Version>,
    ) {
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let table = mock_table_with_timestamps(&timestamps, Some(3)).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = latest_version_as_of(&snapshot, &engine, timestamp);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    // Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT at v3)
    // Matches test_timestamp_to_version_standard_table cases for LeastUpper bound
    #[rstest::rstest]
    #[case::before_all(0, Some(0))]
    #[case::exact_v0(50, Some(0))]
    #[case::between_v0_v1(100, Some(1))]
    #[case::exact_v1(150, Some(1))]
    #[case::just_after_last_file_mod(251, Some(3))]
    #[case::just_before_ict(299, Some(3))]
    #[case::exact_ict_enablement(300, Some(3))]
    #[case::just_after_ict(301, Some(4))]
    #[case::between_ict_commits(350, Some(4))]
    #[case::after_all(1000, None)]
    #[tokio::test]
    async fn test_first_version_after(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<Version>,
    ) {
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let table = mock_table_with_timestamps(&timestamps, Some(3)).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = first_version_after(&snapshot, &engine, timestamp);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT at v3)
    #[rstest::rstest]
    #[case::basic_range(50, Some(300), Ok((0, Some(3))))]
    #[case::no_end(100, None, Ok((1, None)))]
    #[case::start_equals_end(150, Some(150), Ok((1, Some(1))))]
    #[case::spanning_ict_boundary(100, Some(350), Ok((1, Some(3))))]
    #[case::entire_table(0, Some(1000), Ok((0, Some(4))))]
    #[case::exact_v0_to_v1(50, Some(150), Ok((0, Some(1))))]
    #[case::exact_v1_to_v2(150, Some(250), Ok((1, Some(2))))]
    #[case::exact_v3_ict_to_v4(300, Some(400), Ok((3, Some(4))))]
    #[case::between_v0_v1_to_exact_v2(100, Some(250), Ok((1, Some(2))))]
    #[case::just_after_last_file_mod_no_end(251, None, Ok((3, None)))]
    #[case::just_before_ict_to_just_after(299, Some(301), Ok((3, Some(3))))]
    #[case::exact_ict_to_after_all(300, Some(1000), Ok((3, Some(4))))]
    #[case::between_ict_commits(350, Some(400), Ok((4, Some(4))))]
    #[case::just_after_ict_to_end(301, Some(1000), Ok((4, Some(4))))]
    #[case::end_before_all_fails(0, Some(40), Err(()))]
    #[case::start_after_all_fails(500, Some(1000), Err(()))]
    #[tokio::test]
    async fn test_timestamp_range_to_versions(
        #[case] start: Timestamp,
        #[case] end: Option<Timestamp>,
        #[case] expected: Result<(Version, Option<Version>), ()>,
    ) {
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let table = mock_table_with_timestamps(&timestamps, Some(3)).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_range_to_versions(&snapshot, &engine, start, end);
        match expected {
            Ok(v) => assert_eq!(res.unwrap(), v),
            Err(()) => assert!(res.is_err(), "{res:?}"),
        }
    }

    #[tokio::test]
    async fn test_timestamp_range_invalid_range() {
        // Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT at v3)
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let table = mock_table_with_timestamps(&timestamps, Some(3)).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_range_to_versions(&snapshot, &engine, 300, Some(100));
        assert!(
            matches!(
                res,
                Err(crate::Error::LogHistory(ref e))
                    if matches!(**e, LogHistoryError::InvalidTimestampRange { .. })
            ),
            "{res:?}"
        );
    }

    #[tokio::test]
    async fn test_timestamp_range_empty_range() {
        // Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT at v3)
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let table = mock_table_with_timestamps(&timestamps, Some(3)).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        // Timestamps 51-149 fall between v0 (ts=50) and v1 (ts=150)
        let res = timestamp_range_to_versions(&snapshot, &engine, 51, Some(149));
        assert!(
            matches!(
                res,
                Err(crate::Error::LogHistory(ref e))
                    if matches!(**e, LogHistoryError::EmptyTimestampRange { between_version: 0, .. })
            ),
            "{res:?}"
        );
    }

    // ====================================================================================
    // High-level API tests for additional table configurations
    // ====================================================================================

    /// File-mod-only table (no ICT): v0=100, v1=200, v2=300
    #[rstest::rstest]
    #[case::exact_match(200, Some(1))]
    #[case::between_commits(150, Some(0))]
    #[case::after_all(500, Some(2))]
    #[case::before_all(50, None)]
    #[tokio::test]
    async fn test_latest_version_as_of_file_mod_only(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<Version>,
    ) {
        let table =
            mock_table_with_timestamps(&[(100, None), (200, None), (300, None)], None).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = latest_version_as_of(&snapshot, &engine, timestamp);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// File-mod-only table (no ICT): v0=100, v1=200, v2=300
    #[rstest::rstest]
    #[case::exact_match(200, Some(1))]
    #[case::between_commits(150, Some(1))]
    #[case::before_all(50, Some(0))]
    #[case::after_all(500, None)]
    #[tokio::test]
    async fn test_first_version_after_file_mod_only(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<Version>,
    ) {
        let table =
            mock_table_with_timestamps(&[(100, None), (200, None), (300, None)], None).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = first_version_after(&snapshot, &engine, timestamp);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// ICT from creation: v0=100, v1=200, v2=300 (all ICT)
    #[rstest::rstest]
    #[case::exact_match(200, Some(1))]
    #[case::between_commits(150, Some(0))]
    #[case::after_all(500, Some(2))]
    #[case::before_all(50, None)]
    #[tokio::test]
    async fn test_latest_version_as_of_ict_from_creation(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<Version>,
    ) {
        let table =
            mock_table_with_timestamps(&[(0, Some(100)), (0, Some(200)), (0, Some(300))], Some(0))
                .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = latest_version_as_of(&snapshot, &engine, timestamp);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// ICT from creation: v0=100, v1=200, v2=300 (all ICT)
    #[rstest::rstest]
    #[case::exact_match(200, Some(1))]
    #[case::between_commits(150, Some(1))]
    #[case::before_all(50, Some(0))]
    #[case::after_all(500, None)]
    #[tokio::test]
    async fn test_first_version_after_ict_from_creation(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<Version>,
    ) {
        let table =
            mock_table_with_timestamps(&[(0, Some(100)), (0, Some(200)), (0, Some(300))], Some(0))
                .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = first_version_after(&snapshot, &engine, timestamp);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// Non-monotonic file mod timestamps (clock skew):
    /// Raw: v0=100, v1=200, v2=150, v3=180, v4=300
    /// Monotonized: v0=100, v1=200, v2=201, v3=202, v4=300
    #[rstest::rstest]
    #[case::exact_v1(200, Some(1))]
    #[case::monotonized_v2(201, Some(2))]
    #[case::monotonized_v3(202, Some(3))]
    #[case::between_v3_v4(250, Some(3))]
    #[case::raw_v2_between_v0_v1(150, Some(0))]
    #[tokio::test]
    async fn test_latest_version_as_of_non_monotonic(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<Version>,
    ) {
        let table = mock_table_with_timestamps(
            &[
                (100, None),
                (200, None),
                (150, None), // Clock skew - earlier than v1
                (180, None), // Clock skew - earlier than v1
                (300, None),
            ],
            None,
        )
        .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = latest_version_as_of(&snapshot, &engine, timestamp);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// Non-monotonic file mod timestamps (clock skew):
    /// Raw: v0=100, v1=200, v2=150, v3=180, v4=300
    /// Monotonized: v0=100, v1=200, v2=201, v3=202, v4=300
    #[rstest::rstest]
    #[case::exact_v1(200, Some(1))]
    #[case::monotonized_v2(201, Some(2))]
    #[case::monotonized_v3(202, Some(3))]
    #[case::after_v3_monotonized(203, Some(4))]
    #[case::raw_v2_maps_to_v1(150, Some(1))]
    #[tokio::test]
    async fn test_first_version_after_non_monotonic(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<Version>,
    ) {
        let table = mock_table_with_timestamps(
            &[
                (100, None),
                (200, None),
                (150, None), // Clock skew - earlier than v1
                (180, None), // Clock skew - earlier than v1
                (300, None),
            ],
            None,
        )
        .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = first_version_after(&snapshot, &engine, timestamp);
        match expected {
            Some(v) => assert_eq!(res.unwrap(), v),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// timestamp_range_to_versions with file-mod-only table (v0=100, v1=200, v2=300)
    #[rstest::rstest]
    #[case::basic_range(100, Some(200), Ok((0, Some(1))))]
    #[case::entire_table(50, Some(500), Ok((0, Some(2))))]
    #[case::no_end(150, None, Ok((1, None)))]
    #[case::exact_v0_to_v2(100, Some(300), Ok((0, Some(2))))]
    #[case::between_commits(150, Some(250), Ok((1, Some(1))))]
    #[case::start_equals_end_exact(200, Some(200), Ok((1, Some(1))))]
    #[case::end_before_all_fails(50, Some(80), Err(()))]
    #[case::start_after_all_fails(400, Some(500), Err(()))]
    #[tokio::test]
    async fn test_timestamp_range_file_mod_only(
        #[case] start: Timestamp,
        #[case] end: Option<Timestamp>,
        #[case] expected: Result<(Version, Option<Version>), ()>,
    ) {
        let table =
            mock_table_with_timestamps(&[(100, None), (200, None), (300, None)], None).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_range_to_versions(&snapshot, &engine, start, end);
        match expected {
            Ok(v) => assert_eq!(res.unwrap(), v),
            Err(()) => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// timestamp_range_to_versions with ICT from creation (v0=100, v1=200, v2=300)
    #[rstest::rstest]
    #[case::basic_range(100, Some(200), Ok((0, Some(1))))]
    #[case::entire_table(50, Some(500), Ok((0, Some(2))))]
    #[case::no_end(150, None, Ok((1, None)))]
    #[case::exact_v0_to_v2(100, Some(300), Ok((0, Some(2))))]
    #[case::between_commits(150, Some(250), Ok((1, Some(1))))]
    #[case::start_equals_end_exact(200, Some(200), Ok((1, Some(1))))]
    #[case::end_before_all_fails(50, Some(80), Err(()))]
    #[case::start_after_all_fails(400, Some(500), Err(()))]
    #[tokio::test]
    async fn test_timestamp_range_ict_from_creation(
        #[case] start: Timestamp,
        #[case] end: Option<Timestamp>,
        #[case] expected: Result<(Version, Option<Version>), ()>,
    ) {
        let table =
            mock_table_with_timestamps(&[(0, Some(100)), (0, Some(200)), (0, Some(300))], Some(0))
                .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_range_to_versions(&snapshot, &engine, start, end);
        match expected {
            Ok(v) => assert_eq!(res.unwrap(), v),
            Err(()) => assert!(res.is_err(), "{res:?}"),
        }
    }
}
