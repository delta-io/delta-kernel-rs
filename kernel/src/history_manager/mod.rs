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
//! All timestamp queries are limited to the state captured in the provided [`Snapshot`]. For
//! tables without ICT enabled, timestamp queries rely on file modification timestamps, which are
//! not guaranteed to be monotonically increasing across commits.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use error::{LogHistoryError, NearestTimestamp};
use itertools::Itertools;
use search::{binary_search_by_key_with_bounds, Bound, SearchError};
use tracing::{info, trace, warn};
use url::Url;

use crate::log_segment::LogSegment;
use crate::log_segment_files::list_from_storage;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::snapshot::Snapshot;
use crate::table_configuration::InCommitTimestampEnablement;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error as DeltaError, Version};

pub(crate) mod search;

pub mod error;

/// A timestamp representing milliseconds since the Unix epoch (1970-01-01 00:00:00 UTC).
///
/// This type is used throughout the history_manager module for timestamp-to-version conversion.
/// All timestamp values should be specified in milliseconds, not seconds or nanoseconds.
pub type Timestamp = i64;

/// A commit located by a timestamp query: the commit [`Version`] paired with its timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitAt {
    /// The commit version.
    pub version: Version,
    /// Timestamp (milliseconds since the Unix epoch) associated with this commit.
    ///
    /// This is the commit's in-commit timestamp (ICT) when ICT is enabled, otherwise the
    /// commit's file modification time.
    pub timestamp: Timestamp,
}

impl CommitAt {
    /// Creates a [`CommitAt`] pairing a commit `version` with its `timestamp`.
    pub fn new(version: Version, timestamp: Timestamp) -> Self {
        Self { version, timestamp }
    }
}

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
        /// The timestamp at which ICT was enabled.
        ict_enablement_timestamp: Timestamp,
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
            ict_enablement_timestamp: ict_timestamp,
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
/// The [`CommitAt`] matching the bound criteria, whose `timestamp` is the monotonized file
/// modification time, or an error if no match exists.
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
) -> Result<CommitAt, LogHistoryError> {
    if commits.is_empty() {
        return Err(LogHistoryError::out_of_range(
            timestamp,
            bound,
            NearestTimestamp::Unknown,
        ));
    }

    let lo_version = commits[0].version;
    let hi_version = commits[commits.len() - 1].version;
    info!(lo_version, hi_version, "File modification linear search");

    let mut result: Option<CommitAt> = None;
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
            Bound::GreatestLower if monotonic_ts <= timestamp => {
                result = Some(CommitAt::new(commit.version, monotonic_ts))
            }
            Bound::GreatestLower => break,
            // LeastUpper: return version upon first match
            Bound::LeastUpper if monotonic_ts >= timestamp => {
                return Ok(CommitAt::new(commit.version, monotonic_ts))
            }
            Bound::LeastUpper => {}
        }

        prev_monotonic_ts = monotonic_ts;
    }

    result.ok_or_else(|| {
        // OOR with non-empty commits: under GreatestLower, `timestamp` is below
        // the first commit's monotonic ts (which equals its raw `last_modified`
        // since no prior commit affects it). Under LeastUpper, `timestamp` is
        // above the last commit's monotonic ts (held in `prev_monotonic_ts`).
        // Using the monotonized values means a re-query at the suggested
        // `nearest` round-trips to the same boundary version.
        let nearest_ts = match bound {
            Bound::GreatestLower => commits[0].location.last_modified,
            Bound::LeastUpper => prev_monotonic_ts,
        };
        LogHistoryError::out_of_range(
            timestamp,
            bound,
            NearestTimestamp::from_boundary(bound, nearest_ts),
        )
    })
}

/// Binary search over commits with In-Commit Timestamps (ICT).
///
/// ICT timestamps are guaranteed monotonic by protocol, so binary search is safe.
fn binary_search_ict_timestamps(
    commits: &[ParsedLogPath],
    timestamp: Timestamp,
    bound: Bound,
    engine: &dyn Engine,
) -> Result<CommitAt, LogHistoryError> {
    if commits.is_empty() {
        return Err(LogHistoryError::out_of_range(
            timestamp,
            bound,
            NearestTimestamp::Unknown,
        ));
    }

    let lo_version = commits[0].version;
    let hi_version = commits[commits.len() - 1].version;
    info!(
        lo_version,
        hi_version, "ICT binary search over version range"
    );

    let commit_to_ict = |commit: &ParsedLogPath| -> Result<Timestamp, LogHistoryError> {
        commit
            .read_in_commit_timestamp(engine)
            .map_err(|e| LogHistoryError::internal("failed to read in-commit timestamp", e))
    };

    match binary_search_by_key_with_bounds(commits, timestamp, commit_to_ict, bound) {
        Ok(idx) => {
            let version = commits[idx].version;
            // TODO(#2687): reading in_commit_timestamp is an expensive operation since it require
            // an engine I/O. An optimization is to have the `binary_search_by_key_with_bounds`
            // return the search result's index and key value to avoid calling commit_to_ict
            let timestamp = commit_to_ict(&commits[idx])?;
            Ok(CommitAt::new(version, timestamp))
        }
        Err(SearchError::KeyFunctionError(error)) => Err(error),
        Err(SearchError::OutOfRange) => {
            // OutOfRange under `GreatestLower` means timestamp < commits[0]'s
            // ICT; under `LeastUpper` it means timestamp > commits.last()'s
            // ICT. The boundary timestamp is read via `commit_to_ict`; on
            // engine failure we degrade to `Unknown` so the original
            // out-of-range error is preserved.
            let nearest = bound
                .boundary_of(commits)
                .and_then(|boundary| {
                    commit_to_ict(boundary)
                        .inspect_err(|e| {
                            warn!(
                                error = ?e,
                                version = boundary.version,
                                "failed to read boundary ICT for nearest_timestamp hint",
                            );
                        })
                        .ok()
                        .map(|ts| NearestTimestamp::from_boundary(bound, ts))
                })
                .unwrap_or(NearestTimestamp::Unknown);
            Err(LogHistoryError::out_of_range(timestamp, bound, nearest))
        }
    }
}

/// Converts a timestamp to a [`CommitAt`] based on the specified bound type.
///
/// This function finds the appropriate commit that corresponds to the given timestamp
/// according to the bound parameter:
///
/// - `Bound::GreatestLower`: Returns the commit with the greatest timestamp that is less than or
///   equal to the given timestamp (the version immediately before or at the timestamp).
/// - `Bound::LeastUpper`: Returns the commit with the smallest timestamp that is greater than or
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
) -> Result<CommitAt, LogHistoryError> {
    // Short-circuit: compare against snapshot's timestamp to avoid log segment rebuild.
    // This optimization mirrors Delta Spark and Java Kernel behavior.
    let snap_ts = snapshot
        .get_timestamp(engine)
        .map_err(|e| LogHistoryError::internal("failed to get snapshot timestamp", e))?;
    match (timestamp.cmp(&snap_ts), bound) {
        // Exact match: snapshot version satisfies both bounds
        (Ordering::Equal, _) => return Ok(CommitAt::new(snapshot.version(), snap_ts)),
        // Timestamp after snapshot: for GreatestLower, snapshot is the answer
        (Ordering::Greater, Bound::GreatestLower) => {
            return Ok(CommitAt::new(snapshot.version(), snap_ts))
        }
        // Timestamp after snapshot: for LeastUpper, no version exists
        (Ordering::Greater, Bound::LeastUpper) => {
            return Err(LogHistoryError::out_of_range(
                timestamp,
                bound,
                NearestTimestamp::Latest(snap_ts),
            ));
        }
        // Timestamp before snapshot: need to search the log
        _ => {}
    }

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

    // Determine the type of timestamp search. The search may either be over file modification
    // timestamps or over In-Commit Timestamps.
    let commits = &log_segment.listed.ascending_commit_files;
    let search_bounds = get_timestamp_search_bounds(snapshot, &log_segment, timestamp)?;

    match search_bounds {
        TimestampSearchBounds::ExactMatch(version) => Ok(CommitAt::new(version, timestamp)),
        TimestampSearchBounds::ICTSearchStartingFrom(lo) => {
            binary_search_ict_timestamps(&commits[lo..], timestamp, bound, engine)
        }
        TimestampSearchBounds::FileModificationSearch => {
            linear_search_file_mod_timestamps(commits, timestamp, bound)
        }
        TimestampSearchBounds::FileModificationSearchUntil {
            index,
            ict_enablement_version,
            ict_enablement_timestamp,
        } => {
            linear_search_file_mod_timestamps(&commits[..index], timestamp, bound).or_else(|e| {
                // For LeastUpper, if no version found in pre-ICT region, the ICT enablement
                // version is the answer (protocol guarantees enablementTs > prev_mod_time)
                match (&e, bound) {
                    (LogHistoryError::TimestampOutOfRange { .. }, Bound::LeastUpper) => Ok(
                        CommitAt::new(ict_enablement_version, ict_enablement_timestamp),
                    ),
                    _ => Err(e),
                }
            })
        }
    }
}

/// Gets the latest [`CommitAt`] (version and timestamp) with a timestamp at or before `timestamp`.
///
/// Returns [`LogHistoryError::TimestampOutOfRange`] if no version exists at or before
/// the given timestamp.
///
/// # Examples
/// ```ignore
/// use delta_kernel::snapshot::Snapshot;
/// use test_utils::delta_kernel_default_engine::DefaultEngine;
/// use delta_kernel::history_manager::latest_version_as_of;
///
/// let engine = DefaultEngine::try_new(...)?;
/// let snapshot = Snapshot::builder_for(table_uri).build(&engine)?;
///
/// // Get the latest commit as of January 1, 2023
/// let timestamp = 1672531200000; // Milliseconds since epoch for 2023-01-01
/// let commit = latest_version_as_of(&snapshot, &engine, timestamp)?;
/// ```
#[tracing::instrument(skip(snapshot, engine), ret, fields(latest_version = snapshot.version(), table_root = %snapshot.table_root()))]
pub fn latest_version_as_of(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    timestamp: Timestamp,
) -> DeltaResult<CommitAt> {
    timestamp_to_version(snapshot, engine, timestamp, Bound::GreatestLower).map_err(Into::into)
}

/// Gets the first [`CommitAt`] (version and timestamp) with a timestamp at or after `timestamp`.
///
/// Returns [`LogHistoryError::TimestampOutOfRange`] if no version exists at or after
/// the given timestamp.
///
/// # Examples
/// ```ignore
/// use delta_kernel::snapshot::Snapshot;
/// use test_utils::delta_kernel_default_engine::DefaultEngine;
/// use delta_kernel::history_manager::first_version_after;
///
/// let engine = DefaultEngine::try_new(...)?;
/// let snapshot = Snapshot::builder_for(table_uri).build(&engine)?;
///
/// // Find the first commit that occurred at or after January 1, 2023
/// let timestamp = 1672531200000; // Milliseconds since epoch for 2023-01-01
/// let commit = first_version_after(&snapshot, &engine, timestamp)?;
/// ```
#[tracing::instrument(skip(snapshot, engine), ret, fields(latest_version = snapshot.version(), table_root = %snapshot.table_root()))]
pub fn first_version_after(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    timestamp: Timestamp,
) -> DeltaResult<CommitAt> {
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
/// use test_utils::delta_kernel_default_engine::DefaultEngine;
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

    // TODO(#2449): Optimize by reusing the log segment between the two timestamp conversions.
    // Currently, both first_version_after and latest_version_as_of each build their own
    // log segment via listing. We could build the segment once and pass it to a shared
    // helper (e.g., timestamp_to_version_in_segment) to cut listing time in half.

    // Convert the start timestamp to version
    let start_version = first_version_after(snapshot, engine, start_timestamp)?.version;

    // If the end timestamp is present, convert it to an end version
    let end_version = end_timestamp
        .map(|end| {
            let end_version = latest_version_as_of(snapshot, engine, end)?.version;

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

/// Returns the earliest commit version available on the file system for this table.
///
/// The returned version is the version of the lowest-numbered `*.json` commit file present in
/// `log_root`. The returned version is not guaranteed to exist by the time the caller
/// acts on it: a concurrent log-cleanup operation may delete the file.
///
/// # Parameters
/// - `engine`: kernel engine used to list `log_root`.
/// - `log_root`: URL of the table's `_delta_log/` directory (must end with `/`).
/// - `earliest_ratified_commit_version`: For catalog-managed tables, it is the earliest version the
///   catalog has ratified commit. Pass `None` for filesystem-only tables.
///
/// # Errors
/// - Propagates any error from listing the log directory.
/// - [`LogHistoryError::NoCommitsFound`] when the log directory contains no commits.
/// - [`DeltaError::Generic`] when there is no publised file-system commit and the earliest ratified
///   CCv2 commit is v0. For a catalog-managed table, v0 must be a published file-system commit
///   before the catalog exposes the table. Otherwise a filesystem-only client could list an empty
///   `_delta_log/` and "create" a table at the same location. An empty listing here therefore
///   indicates a broken invariant rather than a normal missing version.
#[tracing::instrument(skip(engine), ret, err)]
fn get_earliest_published_commit_version(
    engine: &dyn Engine,
    log_root: &Url,
    earliest_ratified_commit_version: Option<Version>,
) -> DeltaResult<Version> {
    list_from_storage(engine.storage_handler().as_ref(), log_root, 0, Version::MAX)?
        .filter_ok(|f| f.file_type == LogPathFileType::Commit)
        .next()
        .transpose()?
        .map(|f| f.version)
        .ok_or_else(|| {
            if earliest_ratified_commit_version == Some(0) {
                return DeltaError::generic(format!(
                    "expected a published v0 commit for catalog-managed table {log_root}, \
                       but the log listing returned no commits"
                ));
            }
            DeltaError::from(LogHistoryError::NoCommitsFound {
                log_root: log_root.clone(),
            })
        })
}

/// Returns the earliest table version that can be fully reconstructed from the log at
/// `log_root`. This is either commit version 0 (if `00...00.json` exists), or the version
/// of the earliest complete checkpoint. The returned version is not guaranteed to exist by the
/// time the caller acts on it: a concurrent log-cleanup operation may delete the file.
/// This method assumes that the commits are contiguous.
///
/// # Parameters
/// - `engine`: kernel engine used to list `log_root`.
/// - `log_root`: URL of the table's `_delta_log/` directory (must end with `/`).
/// - `earliest_ratified_commit_version`: For catalog-managed tables, it is the earliest version the
///   catalog has ratified commit. Pass `None` for filesystem-only tables.
///
/// # Errors
/// - Propagate any error from listing the log directory.
/// - [`LogHistoryError::NoCommitsFound`] if the log contains no commit files at all
/// (empty directory, or only checkpoint files) -- unless `earliest_ratified_commit_version`
/// is `Some(0)`, in which case it returns a generic [`Error`](crate::Error) flagging the
/// broken CCv2 invariant (ratified commit 0 with no published filesystem commit).
/// - [`LogHistoryError::NoRecreatableCommit`] if commits exist but neither
/// `00...00.json` nor a complete checkpoint that anchors the smallest commit is present.
#[tracing::instrument(skip(engine), err, ret)]
fn get_earliest_recreatable_commit(
    engine: &dyn Engine,
    log_root: &Url,
    earliest_ratified_commit_version: Option<Version>,
) -> DeltaResult<Version> {
    let mut last_complete_checkpoint: Option<Version> = None;
    // Tracks (version, num_parts) -> set of part numbers observed so far, for multi-part
    // checkpoint completeness.
    let mut multi_part_checkpoint_progress = HashMap::<(Version, u32), HashSet<u32>>::new();
    let mut smallest_commit_version = Version::MAX;

    let listing = list_from_storage(engine.storage_handler().as_ref(), log_root, 0, Version::MAX)?;
    for parsed_result in listing {
        let parsed_log_path = parsed_result?;
        match parsed_log_path.file_type {
            LogPathFileType::Commit => {
                if parsed_log_path.version == 0 {
                    return Ok(0);
                }
                smallest_commit_version = smallest_commit_version.min(parsed_log_path.version);

                if let Some(checkpoint_version) = last_complete_checkpoint {
                    if checkpoint_version + 1 >= smallest_commit_version {
                        // Given the contiguity assumption of delta_log commits,
                        // when a full checkpoint has contiguous commits starting before or at
                        // (checkpoint_version + 1) that table can be
                        // recreated at checkpoint_version.
                        return Ok(checkpoint_version);
                    }
                }
            }
            LogPathFileType::SinglePartCheckpoint | LogPathFileType::UuidCheckpoint => {
                last_complete_checkpoint = Some(parsed_log_path.version);
            }
            LogPathFileType::MultiPartCheckpoint {
                part_num,
                num_parts,
            } => {
                let parts = multi_part_checkpoint_progress
                    .entry((parsed_log_path.version, num_parts))
                    .or_default();
                parts.insert(part_num);
                if parts.len() == num_parts as usize {
                    last_complete_checkpoint = Some(parsed_log_path.version);
                }
            }
            LogPathFileType::StagedCommit
            | LogPathFileType::CompactedCommit { .. }
            | LogPathFileType::Crc
            | LogPathFileType::Unknown => {}
        }
    }

    let saw_any_commit = smallest_commit_version != Version::MAX;
    match last_complete_checkpoint {
        Some(checkpoint_version)
            if saw_any_commit && checkpoint_version >= smallest_commit_version =>
        {
            Ok(checkpoint_version)
        }
        _ if saw_any_commit => Err(DeltaError::from(LogHistoryError::NoRecreatableCommit {
            log_root: log_root.clone(),
        })),
        _ if earliest_ratified_commit_version == Some(0) => Err(DeltaError::generic(format!(
            "expected a published v0 commit for catalog-managed table {log_root}, \
                                    but the log listing returned no commits"
        ))),
        _ => Err(DeltaError::from(LogHistoryError::NoCommitsFound {
            log_root: log_root.clone(),
        })),
    }
}

/// Returns the earliest table version available on the file system at `log_root`. The returned
/// version is not guaranteed to exist by the time the caller acts on it: a concurrent log-cleanup
/// operation may delete the underlying file.
///
/// # Parameters
/// - `engine`: kernel engine used to list `log_root`.
/// - `log_root`: URL of the table's `_delta_log/` directory (must end with `/`).
/// - `earliest_ratified_commit_version`: For catalog-managed tables, the earliest version the
///   catalog has ratified a commit at. Pass `None` for filesystem-only tables.
/// - `must_be_recreatable`: When `true`, return the earliest version whose state can be fully
///   reconstructed (commit 0 or the earliest complete checkpoint). When `false`, return the
///   lowest-numbered published commit.
///
/// # Errors
/// - Propagates any error from listing the log directory.
/// - [`LogHistoryError::NoCommitsFound`] when the log directory contains no commits.
/// - [`LogHistoryError::NoRecreatableCommit`] when `must_be_recreatable` is `true`, commits exist,
///   but neither `00...00.json` nor a checkpoint anchoring the smallest commit is present.
/// - [`DeltaError::Generic`] when there is no published filesystem commit and
///   `earliest_ratified_commit_version` is `Some(0)`, flagging a broken catalog-managed invariant
///   (ratified commit 0 with no published filesystem commit).
#[tracing::instrument(skip(engine), err, ret)]
pub fn get_earliest_commit(
    engine: &dyn Engine,
    log_root: &Url,
    earliest_ratified_commit_version: Option<Version>,
    must_be_recreatable: bool,
) -> DeltaResult<Version> {
    if must_be_recreatable {
        get_earliest_recreatable_commit(engine, log_root, earliest_ratified_commit_version)
    } else {
        get_earliest_published_commit_version(engine, log_root, earliest_ratified_commit_version)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::{remove_file, OpenOptions};
    use std::ops::RangeInclusive;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use bytes::Bytes;
    use test_utils::delta_path_for_version;
    use url::Url;
    use uuid::Uuid;

    use super::*;
    use crate::actions::{CommitInfo, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
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
    #[case::file_mod_region(50, TimestampSearchBounds::FileModificationSearchUntil { index: 3, ict_enablement_version: 3, ict_enablement_timestamp: 300 })]
    #[case::file_mod_between(60, TimestampSearchBounds::FileModificationSearchUntil { index: 3, ict_enablement_version: 3, ict_enablement_timestamp: 300 })]
    #[case::file_mod_last(299, TimestampSearchBounds::FileModificationSearchUntil { index: 3, ict_enablement_version: 3, ict_enablement_timestamp: 300 })]
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

    /// Tests the fast path: when pre-ICT commits are cleaned up (e.g., via VACUUM), the first
    /// commit in the log segment is at or after ICT enablement. All visible commits have ICT,
    /// so we return ICTSearchStartingFrom(0) directly without binary search.
    #[rstest::rstest]
    #[case::before_all(50)]
    #[case::at_ict_enablement(300)]
    #[case::after_ict_enablement(350)]
    #[case::after_all(1000)]
    #[tokio::test]
    async fn test_search_bounds_fast_path_pre_ict_commits_cleaned_up(#[case] timestamp: Timestamp) {
        // Table: ICT enabled at v3 with ts=300, but only v3+ remain after cleanup
        let timestamps = [
            (50, None),
            (150, None),
            (250, None),
            (350, Some(300)),
            (450, Some(400)),
        ];
        let (table, engine, snapshot, _log_segment) =
            build_test_snapshot(&timestamps, Some(3), None).await;

        // Simulate cleanup: delete pre-ICT commit files (v0, v1, v2)
        let log_dir = table.table_root().join("_delta_log");
        for version in 0..3 {
            let file_name = format!("{:020}.json", version);
            std::fs::remove_file(log_dir.join(&file_name)).unwrap();
        }

        // Build a new log segment that only sees v3+ (simulating post-cleanup state)
        let cleaned_log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            None,
        )
        .unwrap();

        // Verify the log segment only contains v3+ (first_version=3 >= ict_enablement_version=3)
        assert_eq!(
            cleaned_log_segment.listed.ascending_commit_files[0].version,
            3
        );

        // The fast path should trigger: returns ICTSearchStartingFrom(0) for any timestamp
        let res = get_timestamp_search_bounds(&snapshot, &cleaned_log_segment, timestamp).unwrap();
        assert!(
            matches!(res, TimestampSearchBounds::ICTSearchStartingFrom(0)),
            "Expected fast path ICTSearchStartingFrom(0), got {res:?}"
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
    #[case::before_all_lub(0, Bound::LeastUpper, Some(CommitAt::new(0, 50)))]
    #[case::before_all_glb(0, Bound::GreatestLower, None)]
    #[case::negative_lub(-1, Bound::LeastUpper, Some(CommitAt::new(0, 50)))]
    #[case::after_all_lub(1000, Bound::LeastUpper, None)]
    #[case::after_all_glb(1000, Bound::GreatestLower, Some(CommitAt::new(4, 400)))]
    #[case::exact_v0_lub(50, Bound::LeastUpper, Some(CommitAt::new(0, 50)))]
    #[case::exact_v0_glb(50, Bound::GreatestLower, Some(CommitAt::new(0, 50)))]
    #[case::exact_v1_lub(150, Bound::LeastUpper, Some(CommitAt::new(1, 150)))]
    #[case::exact_v1_glb(150, Bound::GreatestLower, Some(CommitAt::new(1, 150)))]
    #[case::exact_v3_ict_lub(300, Bound::LeastUpper, Some(CommitAt::new(3, 300)))]
    #[case::exact_v3_ict_glb(300, Bound::GreatestLower, Some(CommitAt::new(3, 300)))]
    #[case::between_v0_v1_lub(100, Bound::LeastUpper, Some(CommitAt::new(1, 150)))]
    #[case::between_v0_v1_glb(100, Bound::GreatestLower, Some(CommitAt::new(0, 50)))]
    #[case::just_after_last_file_mod_lub(251, Bound::LeastUpper, Some(CommitAt::new(3, 300)))]
    #[case::just_after_last_file_mod_glb(251, Bound::GreatestLower, Some(CommitAt::new(2, 250)))]
    #[case::just_before_ict_lub(299, Bound::LeastUpper, Some(CommitAt::new(3, 300)))]
    #[case::just_before_ict_glb(299, Bound::GreatestLower, Some(CommitAt::new(2, 250)))]
    #[case::just_after_ict_glb(301, Bound::GreatestLower, Some(CommitAt::new(3, 300)))]
    #[case::just_after_ict_lub(301, Bound::LeastUpper, Some(CommitAt::new(4, 400)))]
    #[case::between_ict_commits(350, Bound::GreatestLower, Some(CommitAt::new(3, 300)))]
    #[case::between_ict_commits_lub(350, Bound::LeastUpper, Some(CommitAt::new(4, 400)))]
    #[tokio::test]
    async fn test_timestamp_to_version_standard_table(
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected: Option<CommitAt>,
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
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(
                matches!(res, Err(LogHistoryError::TimestampOutOfRange { .. })),
                "{res:?}"
            ),
        }
    }

    // Table: v0=100, v1=200, v2=300 (ICT from creation)
    #[rstest::rstest]
    #[case::exact_v0_glb(100, Bound::GreatestLower, Some(CommitAt::new(0, 100)))]
    #[case::exact_v0_lub(100, Bound::LeastUpper, Some(CommitAt::new(0, 100)))]
    #[case::between_v0_v1_glb(150, Bound::GreatestLower, Some(CommitAt::new(0, 100)))]
    #[case::between_v0_v1_lub(150, Bound::LeastUpper, Some(CommitAt::new(1, 200)))]
    #[case::between_v1_v2_glb(250, Bound::GreatestLower, Some(CommitAt::new(1, 200)))]
    #[case::between_v1_v2_lub(250, Bound::LeastUpper, Some(CommitAt::new(2, 300)))]
    #[case::before_all_glb(50, Bound::GreatestLower, None)]
    #[case::before_all_lub(50, Bound::LeastUpper, Some(CommitAt::new(0, 100)))]
    #[tokio::test]
    async fn test_ict_from_creation(
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected: Option<CommitAt>,
    ) {
        let mock_table =
            mock_table_with_timestamps(&[(0, Some(100)), (0, Some(200)), (0, Some(300))], Some(0))
                .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_to_version(&snapshot, &engine, timestamp, bound);
        match expected {
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(
                matches!(res, Err(LogHistoryError::TimestampOutOfRange { .. })),
                "{res:?}"
            ),
        }
    }

    /// Single commit table: v0=100 (file mod only)
    #[rstest::rstest]
    #[case::exact_glb(100, Bound::GreatestLower, Some(CommitAt::new(0, 100)))]
    #[case::exact_lub(100, Bound::LeastUpper, Some(CommitAt::new(0, 100)))]
    #[case::before_glb(50, Bound::GreatestLower, None)]
    #[case::before_lub(50, Bound::LeastUpper, Some(CommitAt::new(0, 100)))]
    #[case::after_glb(150, Bound::GreatestLower, Some(CommitAt::new(0, 100)))]
    #[case::after_lub(150, Bound::LeastUpper, None)]
    #[tokio::test]
    async fn test_single_commit(
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected: Option<CommitAt>,
    ) {
        let mock_table = mock_table_with_timestamps(&[(100, None)], None).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = timestamp_to_version(&snapshot, &engine, timestamp, bound);
        match expected {
            Some(commit) => assert_eq!(res.unwrap(), commit),
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
    #[case::exact_v0_glb(100, Bound::GreatestLower, Some(CommitAt::new(0, 100)))]
    #[case::exact_v1_glb(200, Bound::GreatestLower, Some(CommitAt::new(1, 200)))]
    #[case::monotonized_v2_glb(201, Bound::GreatestLower, Some(CommitAt::new(2, 201)))]
    #[case::monotonized_v3_glb(202, Bound::GreatestLower, Some(CommitAt::new(3, 202)))]
    #[case::between_v3_v4_glb(250, Bound::GreatestLower, Some(CommitAt::new(3, 202)))]
    #[case::exact_v4_glb(300, Bound::GreatestLower, Some(CommitAt::new(4, 300)))]
    #[case::raw_v2_maps_to_v1_lub(150, Bound::LeastUpper, Some(CommitAt::new(1, 200)))]
    #[case::monotonized_v2_lub(201, Bound::LeastUpper, Some(CommitAt::new(2, 201)))]
    #[case::after_v3_monotonized_lub(203, Bound::LeastUpper, Some(CommitAt::new(4, 300)))]
    #[tokio::test]
    async fn test_non_monotonic_timestamps(
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected: Option<CommitAt>,
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
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(
                matches!(res, Err(LogHistoryError::TimestampOutOfRange { .. })),
                "{res:?}"
            ),
        }
    }

    /// Verifies `nearest_timestamp` is populated on `TimestampOutOfRange`. Covers
    /// the three reachable populate sites: linear file-mod search out-of-range,
    /// ICT binary-search out-of-range, and the snapshot short-circuit.
    #[rstest::rstest]
    // Linear file-mod GLB: timestamp below earliest commit on the standard table.
    // nearest = Earliest(commits[0].last_modified) = Earliest(50).
    #[case::linear_glb_below_earliest(
        &[(50, None), (150, None), (250, None), (350, Some(300)), (450, Some(400))],
        Some(3),
        0,
        Bound::GreatestLower,
        NearestTimestamp::Earliest(50),
    )]
    // Snapshot short-circuit LUB: timestamp above the snapshot's ICT (snap_ts = 400).
    #[case::short_circuit_lub_after_snapshot(
        &[(50, None), (150, None), (250, None), (350, Some(300)), (450, Some(400))],
        Some(3),
        1000,
        Bound::LeastUpper,
        NearestTimestamp::Latest(400),
    )]
    // ICT binary-search GLB: timestamp below earliest ICT on an ICT-from-creation
    // table. nearest = Earliest(ICT(v0)) = Earliest(100).
    #[case::ict_glb_below_earliest(
        &[(0, Some(100)), (0, Some(200)), (0, Some(300))],
        Some(0),
        50,
        Bound::GreatestLower,
        NearestTimestamp::Earliest(100),
    )]
    #[tokio::test]
    async fn test_timestamp_out_of_range_nearest_timestamp(
        #[case] timestamps: &[(Timestamp, Option<Timestamp>)],
        #[case] ict_enablement: Option<Version>,
        #[case] timestamp: Timestamp,
        #[case] bound: Bound,
        #[case] expected_nearest: NearestTimestamp,
    ) {
        let (_table, engine, snapshot, _log_segment) =
            build_test_snapshot(timestamps, ict_enablement, None).await;

        let err = timestamp_to_version(&snapshot, &engine, timestamp, bound).unwrap_err();
        let LogHistoryError::TimestampOutOfRange {
            nearest_timestamp, ..
        } = err
        else {
            panic!("expected TimestampOutOfRange, got {err:?}");
        };
        assert_eq!(nearest_timestamp, expected_nearest);
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
    #[case::exact_v0(50, Some(CommitAt::new(0, 50)))]
    #[case::between_v0_v1(100, Some(CommitAt::new(0, 50)))]
    #[case::exact_v1(150, Some(CommitAt::new(1, 150)))]
    #[case::just_after_last_file_mod(251, Some(CommitAt::new(2, 250)))]
    #[case::just_before_ict(299, Some(CommitAt::new(2, 250)))]
    #[case::exact_ict_enablement(300, Some(CommitAt::new(3, 300)))]
    #[case::just_after_ict(301, Some(CommitAt::new(3, 300)))]
    #[case::between_ict_commits(350, Some(CommitAt::new(3, 300)))]
    #[case::after_all(1000, Some(CommitAt::new(4, 400)))]
    #[tokio::test]
    async fn test_latest_version_as_of(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<CommitAt>,
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
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    // Table: v0=50, v1=150, v2=250 (file mod), v3=300, v4=400 (ICT at v3)
    // Matches test_timestamp_to_version_standard_table cases for LeastUpper bound
    #[rstest::rstest]
    #[case::before_all(0, Some(CommitAt::new(0, 50)))]
    #[case::exact_v0(50, Some(CommitAt::new(0, 50)))]
    #[case::between_v0_v1(100, Some(CommitAt::new(1, 150)))]
    #[case::exact_v1(150, Some(CommitAt::new(1, 150)))]
    #[case::just_after_last_file_mod(251, Some(CommitAt::new(3, 300)))]
    #[case::just_before_ict(299, Some(CommitAt::new(3, 300)))]
    #[case::exact_ict_enablement(300, Some(CommitAt::new(3, 300)))]
    #[case::just_after_ict(301, Some(CommitAt::new(4, 400)))]
    #[case::between_ict_commits(350, Some(CommitAt::new(4, 400)))]
    #[case::after_all(1000, None)]
    #[tokio::test]
    async fn test_first_version_after(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<CommitAt>,
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
            Some(commit) => assert_eq!(res.unwrap(), commit),
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
    #[case::exact_match(200, Some(CommitAt::new(1, 200)))]
    #[case::between_commits(150, Some(CommitAt::new(0, 100)))]
    #[case::after_all(500, Some(CommitAt::new(2, 300)))]
    #[case::before_all(50, None)]
    #[tokio::test]
    async fn test_latest_version_as_of_file_mod_only(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<CommitAt>,
    ) {
        let table =
            mock_table_with_timestamps(&[(100, None), (200, None), (300, None)], None).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = latest_version_as_of(&snapshot, &engine, timestamp);
        match expected {
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// File-mod-only table (no ICT): v0=100, v1=200, v2=300
    #[rstest::rstest]
    #[case::exact_match(200, Some(CommitAt::new(1, 200)))]
    #[case::between_commits(150, Some(CommitAt::new(1, 200)))]
    #[case::before_all(50, Some(CommitAt::new(0, 100)))]
    #[case::after_all(500, None)]
    #[tokio::test]
    async fn test_first_version_after_file_mod_only(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<CommitAt>,
    ) {
        let table =
            mock_table_with_timestamps(&[(100, None), (200, None), (300, None)], None).await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = first_version_after(&snapshot, &engine, timestamp);
        match expected {
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// ICT from creation: v0=100, v1=200, v2=300 (all ICT)
    #[rstest::rstest]
    #[case::exact_match(200, Some(CommitAt::new(1, 200)))]
    #[case::between_commits(150, Some(CommitAt::new(0, 100)))]
    #[case::after_all(500, Some(CommitAt::new(2, 300)))]
    #[case::before_all(50, None)]
    #[tokio::test]
    async fn test_latest_version_as_of_ict_from_creation(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<CommitAt>,
    ) {
        let table =
            mock_table_with_timestamps(&[(0, Some(100)), (0, Some(200)), (0, Some(300))], Some(0))
                .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = latest_version_as_of(&snapshot, &engine, timestamp);
        match expected {
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// ICT from creation: v0=100, v1=200, v2=300 (all ICT)
    #[rstest::rstest]
    #[case::exact_match(200, Some(CommitAt::new(1, 200)))]
    #[case::between_commits(150, Some(CommitAt::new(1, 200)))]
    #[case::before_all(50, Some(CommitAt::new(0, 100)))]
    #[case::after_all(500, None)]
    #[tokio::test]
    async fn test_first_version_after_ict_from_creation(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<CommitAt>,
    ) {
        let table =
            mock_table_with_timestamps(&[(0, Some(100)), (0, Some(200)), (0, Some(300))], Some(0))
                .await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(table.table_root()).unwrap();
        let snapshot = Snapshot::builder_for(path).build(&engine).unwrap();

        let res = first_version_after(&snapshot, &engine, timestamp);
        match expected {
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// Non-monotonic file mod timestamps (clock skew):
    /// Raw: v0=100, v1=200, v2=150, v3=180, v4=300
    /// Monotonized: v0=100, v1=200, v2=201, v3=202, v4=300
    #[rstest::rstest]
    #[case::exact_v1(200, Some(CommitAt::new(1, 200)))]
    #[case::monotonized_v2(201, Some(CommitAt::new(2, 201)))]
    #[case::monotonized_v3(202, Some(CommitAt::new(3, 202)))]
    #[case::between_v3_v4(250, Some(CommitAt::new(3, 202)))]
    #[case::raw_v2_between_v0_v1(150, Some(CommitAt::new(0, 100)))]
    #[tokio::test]
    async fn test_latest_version_as_of_non_monotonic(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<CommitAt>,
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
            Some(commit) => assert_eq!(res.unwrap(), commit),
            None => assert!(res.is_err(), "{res:?}"),
        }
    }

    /// Non-monotonic file mod timestamps (clock skew):
    /// Raw: v0=100, v1=200, v2=150, v3=180, v4=300
    /// Monotonized: v0=100, v1=200, v2=201, v3=202, v4=300
    #[rstest::rstest]
    #[case::exact_v1(200, Some(CommitAt::new(1, 200)))]
    #[case::monotonized_v2(201, Some(CommitAt::new(2, 201)))]
    #[case::monotonized_v3(202, Some(CommitAt::new(3, 202)))]
    #[case::after_v3_monotonized(203, Some(CommitAt::new(4, 300)))]
    #[case::raw_v2_maps_to_v1(150, Some(CommitAt::new(1, 200)))]
    #[tokio::test]
    async fn test_first_version_after_non_monotonic(
        #[case] timestamp: Timestamp,
        #[case] expected: Option<CommitAt>,
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
            Some(commit) => assert_eq!(res.unwrap(), commit),
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

    /// Builds an in-memory store with a mock entry per given log file path and returns
    /// an engine wired to it plus the corresponding `_delta_log/` URL.
    fn engine_with_log_files(paths: &[String]) -> (SyncEngine, Url) {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let storage = engine.storage_handler();
        for path in paths {
            let url = Url::parse(&format!("memory:///{path}")).unwrap();
            storage
                .put(&url, Bytes::from_static(b"x"), false)
                .expect("put log file");
        }
        (engine, Url::parse("memory:///_delta_log/").unwrap())
    }

    fn commit_path(version_range: RangeInclusive<Version>) -> Vec<String> {
        version_range
            .map(|v| delta_path_for_version(v, "json").to_string())
            .collect()
    }

    fn single_part_checkpoint_path(v: Version) -> String {
        format!("_delta_log/{v:020}.checkpoint.parquet")
    }

    fn uuid_checkpoint_path(v: Version) -> String {
        format!("_delta_log/{v:020}.checkpoint.{}.parquet", Uuid::new_v4())
    }

    fn multi_part_checkpoint_paths(v: Version, num_parts: u32) -> Vec<String> {
        (1..=num_parts)
            .map(|part| format!("_delta_log/{v:020}.checkpoint.{part:010}.{num_parts:010}.parquet"))
            .collect()
    }

    /// Builds a "truncated log" layout: `checkpoint_paths` plus commits over `commit_range`.
    fn truncated_log(
        checkpoint_paths: Vec<String>,
        commit_range: RangeInclusive<Version>,
    ) -> Vec<String> {
        let mut paths: Vec<String> = checkpoint_paths;
        paths.extend(commit_path(commit_range));
        paths
    }

    #[rstest::rstest]
    #[case::v0_exists(commit_path(0..=5), None, Expected::Version(0))]
    #[case::truncated_single_part(truncated_log(vec![single_part_checkpoint_path(5)], 5..=9), None, Expected::Version(5))]
    #[case::truncated_uuid(truncated_log(vec![uuid_checkpoint_path(5)], 5..=9), None, Expected::Version(5))]
    #[case::truncated_multi_part(truncated_log(multi_part_checkpoint_paths(5, 3), 5..=9), None, Expected::Version(5))]
    #[case::ratified_zero_with_v0_present(commit_path(0..=3), Some(0), Expected::Version(0))]
    #[case::non_zero_ratified_falls_through_to_scan(commit_path(0..=3), Some(4), Expected::Version(0))]
    #[case::commit_cleanup_before_checkpoint(
        {
            let mut p = commit_path(2..=4);
            p.push(uuid_checkpoint_path(4));
            p
        },
        None,
        Expected::Version(4)
    )]
    #[case::picks_first_anchored_checkpoint(
        {
            let mut p = vec![single_part_checkpoint_path(3)];
            p.extend(multi_part_checkpoint_paths(7, 2));
            p.extend(commit_path(7..=8));
            p
        },
        None,
        Expected::Version(7),
    )]
    #[case::listing_with_multiple_full_checkpoints(
        {
            let mut p = vec![single_part_checkpoint_path(3)];
            p.extend(commit_path(3..=7));
            p.push(single_part_checkpoint_path(7));
            p
        },
        None,
        Expected::Version(3),
    )]
    #[case::listing_with_cleanup_checkpoints(
            {
                let mut p = multi_part_checkpoint_paths(3, 3);
                p.remove(0);
                p.extend(commit_path(3..=7));
                p.push(single_part_checkpoint_path(7));
                p
            },
            None,
            Expected::Version(7),
        )]
    #[case::listing_with_v0_and_full_checkpoint(
        {
            let mut p = commit_path(0..=5);
            p.push(single_part_checkpoint_path(5));
            p
        },
        None,
        Expected::Version(0),
    )]
    #[case::checkpoint_before_smallest_commit_anchors(
        {
            let mut p = vec![single_part_checkpoint_path(5)];
            p.extend(commit_path(6..=8));
            p
        },
        None,
        Expected::Version(5),
    )]
    #[case::empty_log(vec![], None, Expected::NoCommitsFound)]
    #[case::crc_only(vec![format!("_delta_log/{:020}.crc", 5)], None, Expected::NoCommitsFound)]
    #[case::compacted_only(vec![format!("_delta_log/{:020}.{:020}.compacted.json", 0, 5)], None, Expected::NoCommitsFound)]
    #[case::checkpoint_only(vec![single_part_checkpoint_path(5)], None, Expected::NoCommitsFound)]
    #[case::non_zero_ratified_on_empty_log(vec![], Some(5), Expected::NoCommitsFound)]
    #[case::ratified_zero_on_empty_log(vec![], Some(0), Expected::CCv2MissingV0FilesystemCommit)]
    #[case::commits_have_no_anchor(commit_path(2..=3), None, Expected::NoRecreatableCommit)]
    #[case::ratified_zero_commits_no_anchor(commit_path(2..=3), Some(0), Expected::NoRecreatableCommit)]
    fn earliest_recreatable_returns_expected(
        #[case] paths: Vec<String>,
        #[case] ratified: Option<Version>,
        #[case] expected: Expected,
    ) {
        let (engine, log_root) = engine_with_log_files(&paths);
        let res = get_earliest_recreatable_commit(&engine, &log_root, ratified);
        match expected {
            Expected::Version(v) => assert_eq!(res.unwrap(), v),
            Expected::NoCommitsFound => assert!(
                matches!(&res, Err(DeltaError::LogHistory(e)) if matches!(**e, LogHistoryError::NoCommitsFound { .. })),
                "expected NoCommitsFound error, got {res:?}"
            ),
            Expected::NoRecreatableCommit => assert!(
                matches!(&res, Err(DeltaError::LogHistory(e)) if matches!(**e, LogHistoryError::NoRecreatableCommit { .. })),
                "expected NoRecreatableCommit error, got {res:?}"
            ),
            Expected::CCv2MissingV0FilesystemCommit => {
                assert!(matches!(res, Err(DeltaError::Generic(_))), "{res:?}")
            }
        }
    }

    enum Expected {
        Version(Version),
        NoCommitsFound,
        NoRecreatableCommit,
        CCv2MissingV0FilesystemCommit,
    }

    #[tokio::test]
    #[rstest::rstest]
    #[case::no_ratified_commit(3, None, None, Expected::Version(0))]
    #[case::ratified_commit_version_0(3, Some(0), None, Expected::Version(0))]
    #[case::ratified_commit_version_greater_than_0(3, Some(2), None, Expected::Version(0))]
    #[case::log_listing_empty_no_ratified_commit(0, None, None, Expected::NoCommitsFound)]
    #[case::log_listing_empty_ratified_commit(0, Some(2), None, Expected::NoCommitsFound)]
    #[case::log_listing_empty_ratified_commit_v0(
        0,
        Some(0),
        None,
        Expected::CCv2MissingV0FilesystemCommit
    )]
    async fn test_get_earliest_published_commit_version(
        #[case] num_commits: usize,
        #[case] earliest_ratified_commit_version: Option<Version>,
        #[case] last_cleanup_version: Option<Version>,
        #[case] expected: Expected,
    ) {
        let timestamps = (0..num_commits)
            .map(|i| (i as i64, None))
            .collect::<Vec<_>>();
        let table = mock_table_with_timestamps(&timestamps, None).await;
        let engine = SyncEngine::new();
        let log_root = Url::from_directory_path(table.table_root().join("_delta_log")).unwrap();

        if let Some(last_cleanup_version) = last_cleanup_version {
            for version in 0..=last_cleanup_version {
                let filename = format!("{version:020}.json");
                remove_file(log_root.to_file_path().unwrap().join(filename)).unwrap();
            }
        }

        let res = get_earliest_published_commit_version(
            &engine,
            &log_root,
            earliest_ratified_commit_version,
        );
        match expected {
            Expected::Version(v) => assert_eq!(res.unwrap(), v),
            Expected::NoCommitsFound => assert!(
                matches!(res, Err(DeltaError::LogHistory(ref e)) if matches!(**e, LogHistoryError::NoCommitsFound { .. })),
                "{res:?}"
            ),
            Expected::CCv2MissingV0FilesystemCommit => {
                assert!(matches!(res, Err(DeltaError::Generic(_))), "{res:?}")
            }
            Expected::NoRecreatableCommit => {
                unreachable!(
                    "get_earliest_published_commit_version never returns NoRecreatableCommit"
                )
            }
        }
    }

    #[tokio::test]
    async fn test_get_earliest_published_commit_version_ignores_staged_commits() {
        let timestamps = vec![(0, None)];
        let table = mock_table_with_timestamps(&timestamps, None).await;
        let engine = SyncEngine::new();
        let log_dir = table.table_root().join("_delta_log");
        let log_root = Url::from_directory_path(&log_dir).unwrap();

        // Write a staged commit at v1 with a fixed UUID. Staged commits should NOT influence
        // the earliest-published-commit result.
        let staged_dir = log_dir.join("_staged_commits");
        std::fs::create_dir_all(&staged_dir).unwrap();
        let staged_file =
            staged_dir.join("00000000000000000001.01234567-89ab-cdef-0123-456789abcdef.json");
        std::fs::write(&staged_file, "{}").unwrap();

        let res = get_earliest_published_commit_version(&engine, &log_root, None);
        assert_eq!(res.unwrap(), 0);
    }
}
