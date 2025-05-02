use crate::internal_mod;

use error::LogHistoryError;
use error::TimestampOutOfRangeError;
use search::{binary_search_by_key_with_bounds, Bound, SearchError};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;

use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::snapshot::Snapshot;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error as DeltaError, RowVisitor, Version};
use timestamp_visitor::InCommitTimestampVisitor;

pub(crate) mod error;
internal_mod!(pub(crate) mod search);
mod timestamp_visitor;

type Timestamp = i64;

/// The [`LogHistoryManager`] performs timestamp queries over the Delta Log, translating
/// between timestamps and Delta versions.
///
/// # Usage
///
/// Use this manager to:
/// - Convert timestamps or timestamp ranges into Delta versions or version ranges
/// - Perform time travel queries using [`Table::snapshot`]
/// - Execute timestamp-based change data feed queries using [`Table::table_changes`]
///
/// The [`LogHistoryManager`] works with tables regardless of whether they have In-Commit
/// Timestamps enabled.
///
/// # Limitations
///
/// Once created, the [`LogHistoryManager`] does not automatically update with newer versions
/// of the table. All timestamp queries are limited to the state captured in the [`Snapshot`]
/// provided during construction.
#[allow(unused)]
#[derive(Debug)]
pub(crate) struct LogHistoryManager {
    log_segment: LogSegment,
    snapshot: Arc<Snapshot>,
}

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

/// When doing a search for ICT, we only consider the latest ICT range to be valid
impl LogHistoryManager {
    /// Creates a new [`LogHistoryManager`] instance that can query for timestamps up to the
    /// provided snapshot's version.
    ///
    /// # Parameters
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `snapshot`: A [`Snapshot`] of the table. The latest version in the snapshot will be the
    ///               greatest time that this [`LogHistoryManager`] will be able to resolve
    ///               timestamps for.
    /// - `limit`: Optional maximum number of versions to track in the history manager's state.
    ///            When specified, the earliest queryable version is `snapshot.version - limit`.
    ///            This parameter allows trading memory usage for historical reach.
    /// ```
    #[allow(unused)]
    pub(crate) fn try_new(
        engine: &dyn Engine,
        snapshot: Arc<Snapshot>,
        limit: Option<usize>,
    ) -> DeltaResult<Self> {
        #[allow(unused)]
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            limit,
        )?;
        debug_assert!(
            !log_segment.ascending_commit_files.is_empty(),
            "LogSegment should ensure that a segment is non-empty"
        );
        debug_assert!(log_segment.end_version == snapshot.log_segment().end_version);
        Ok(Self {
            log_segment,
            snapshot,
        })
    }

    /// Gets the timestamp for the `commit_file`. If `read_ict` is false ,this returns the file's
    /// modification timestamp. If `read_ict` is true, this reads the file's In-commit timestamp.
    #[allow(unused)]
    fn commit_file_to_timestamp(
        &self,
        engine: &dyn Engine,
        commit_file: &ParsedLogPath,
        read_ict: bool,
    ) -> Result<Timestamp, LogHistoryError> {
        let commit_timestamp = if read_ict {
            Self::read_in_commit_timestamp(engine, commit_file)?
        } else {
            // By default, the timestamp of a commit is its modification time
            commit_file.location.last_modified
        };

        Ok(commit_timestamp)
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

        // Take the first non-empty engine data batch
        let batch = action_iter
            .map(|res| res.map_err(wrap_err))
            .next()
            .transpose()?;

        // Visit the rows and get the in-commit timestamp if present
        let in_commit_timestamp_opt = batch
            .map(|batch| -> Result<Option<i64>, LogHistoryError> {
                let mut visitor = InCommitTimestampVisitor::default();
                visitor.visit_rows_of(batch.as_ref()).map_err(wrap_err)?;
                Ok(visitor.in_commit_timestamp)
            })
            .transpose()?
            .flatten();

        in_commit_timestamp_opt.ok_or_else(|| LogHistoryError::InCommitTimestampNotFoundError {
            version: commit_file.version,
        })
    }

    /// Given a timestamp, this function determines the commit range that timestamp conversion
    /// should search. A timestamp search may be conducted over one of two version ranges:
    ///     1) A range of commits whose timestamp is the file modification timestamp
    ///     2) A range of commits whose timestamp is an in-commit timestamp.
    #[allow(unused)]
    fn get_timestamp_search_bounds(
        &self,
        timestamp: Timestamp,
    ) -> Result<TimestampSearchBounds, LogHistoryError> {
        let table_config = self.snapshot.table_configuration();

        // Get the In-commit timestamp (ICT) enablement version and timestamp. If ICT is not
        // supported or enabled, we perform a regular file modification search.
        let Some((ict_version, ict_timestamp)) = table_config
            .in_commit_timestamp_enablement()
            .map_err(|err| LogHistoryError::InvalidTableConfiguration(Box::new(err)))?
        else {
            return Ok(TimestampSearchBounds::FileModificationSearch);
        };

        // Helper function to get the of index at which ict was enabled if it is present in this
        // LogSegment
        let version_idx = || {
            self.log_segment
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
        &self,
        engine: &dyn Engine,
        timestamp: Timestamp,
        bound: Bound,
    ) -> Result<Version, LogHistoryError> {
        require!(timestamp >= 0, LogHistoryError::InvalidTimestamp(timestamp));
        let len = self.log_segment.ascending_commit_files.len();

        // Determine the type of timestamp search. The search may either be over file modification
        // timestamps or over In-Commit Timestamps.
        let search_bounds = self.get_timestamp_search_bounds(timestamp)?;
        let (lo, hi, read_ict) = match search_bounds {
            TimestampSearchBounds::ExactMatch(version) => return Ok(version),
            TimestampSearchBounds::ICTSearchStartingFrom(lo) => (lo, len, true),
            TimestampSearchBounds::FileModificationSearch => (0, len, false),
            TimestampSearchBounds::FileModificationSearchUntil { index: hi, .. } => (0, hi, false),
        };
        debug_assert!(lo < hi, "Index range should be non-empty");

        // Declare the key function of the search that finds the timestamp given a commit.
        let commit_to_ts = |commit| self.commit_file_to_timestamp(engine, commit, read_ict);

        // We only search in the range [lo..hi).
        let commit_range = &self.log_segment.ascending_commit_files[lo..hi];
        let search_result =
            binary_search_by_key_with_bounds(commit_range, timestamp, commit_to_ts, bound);

        // Get the index from the commit range if successful
        let relative_idx = match search_result {
            Ok(idx) => idx,
            Err(SearchError::OutOfRange) => {
                // Special case: If searching for Least Upper Bound over file modification
                // timestamp range, and no version was found, then use the in-commit timestamp
                // enablement version. This is because the enablement version bounds all file
                // modification timestamps from above.
                if let (
                    TimestampSearchBounds::FileModificationSearchUntil {
                        ict_enablement_version,
                        ..
                    },
                    Bound::LeastUpper,
                ) = (search_bounds, bound)
                {
                    return Ok(ict_enablement_version);
                }

                return Err(LogHistoryError::TimestampOutOfRange(
                    TimestampOutOfRangeError::try_new(
                        commit_range,
                        timestamp,
                        commit_to_ts,
                        bound,
                        read_ict,
                    )?,
                ));
            }
            Err(SearchError::KeyFunctionError(error)) => {
                // Commit to timestamp conversion failed. Return the error
                return Err(error);
            }
        };

        // `relative_idx` holds for the range [lo..hi]. Add back `lo`
        let idx = lo + relative_idx;
        debug_assert!(idx < len, "Relative index should become a valid index");
        Ok(self.log_segment.ascending_commit_files[idx].version)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::time::{Duration, SystemTime};

    use crate::actions::{CommitInfo, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::snapshot::Snapshot;
    use crate::table_features::WriterFeature;
    use crate::utils::test_utils::{Action, LocalMockTable};
    use crate::Version;
    use test_utils::delta_path_for_version;
    use url::Url;

    use super::{
        Bound, LogHistoryError, LogHistoryManager, Timestamp, TimestampOutOfRangeError,
        TimestampSearchBounds,
    };

    // Helper to set the file modification timestamp of a file
    fn set_mod_time(mock_table: &LocalMockTable, commit_version: Version, timestamp: Timestamp) {
        let file_name = delta_path_for_version(commit_version, "json")
            .filename()
            .unwrap()
            .to_string();
        let path = mock_table.table_root().join("_delta_log/").join(file_name);
        let file = File::open(path).unwrap();
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
    async fn timestamp_search_bounds_no_ict() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();

        // Set the end version to a time before In-commit timestamps were enabled
        let snapshot = Snapshot::try_new(path, &engine, Some(2)).unwrap();
        let manager = LogHistoryManager::try_new(&engine, snapshot.into()).unwrap();

        // Before all commits
        let mut res = manager.get_timestamp_search_bounds(0);
        assert!(matches!(
            res,
            Ok(TimestampSearchBounds::FileModificationSearch)
        ));

        // Within the timestamp range
        res = manager.get_timestamp_search_bounds(100);
        assert!(matches!(
            res,
            Ok(TimestampSearchBounds::FileModificationSearch)
        ));

        // After all commits
        res = manager.get_timestamp_search_bounds(1000);
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
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let manager = LogHistoryManager::try_new(&engine, snapshot.into()).unwrap();

        // Exact match only applies to in-commit timestamp enablement version
        let mut res = manager.get_timestamp_search_bounds(50);
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
        res = manager.get_timestamp_search_bounds(60);
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
        res = manager.get_timestamp_search_bounds(299);
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
        res = manager.get_timestamp_search_bounds(300);
        assert!(
            matches!(res, Ok(TimestampSearchBounds::ExactMatch(3))),
            "{res:?}"
        );

        // Edge case: The timestamp is 1 + enablement_timestamp. This returns the beginning of the
        // in-commit timestamp index range.
        res = manager.get_timestamp_search_bounds(301);
        assert!(
            matches!(res, Ok(TimestampSearchBounds::ICTSearchStartingFrom(3))),
            "{res:?}"
        );

        // Timestamp is much larger than the last in-commit timestamp
        res = manager.get_timestamp_search_bounds(1000);
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
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let manager = LogHistoryManager::try_new(&engine, snapshot.into(), None).unwrap();
        let commits = manager.log_segment.ascending_commit_files;

        // File that has no In-commit timestamps
        let mut res = LogHistoryManager::read_in_commit_timestamp(&engine, &commits[0]);
        assert!(
            matches!(
                res,
                Err(LogHistoryError::InCommitTimestampNotFoundError { version: 0 })
            ),
            "{res:?} failed"
        );

        // File that doesn't exist
        let mut fake_path = commits[0].clone();
        fake_path.location.location = Url::from_file_path("/phony/path").unwrap();
        res = LogHistoryManager::read_in_commit_timestamp(&engine, &fake_path);
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
        res = LogHistoryManager::read_in_commit_timestamp(&engine, &commits[3]);
        assert!(matches!(res, Ok(300)), "{res:?}");
        res = LogHistoryManager::read_in_commit_timestamp(&engine, &commits[4]);
        assert!(matches!(res, Ok(400)), "{res:?}");
    }

    #[tokio::test]
    async fn file_modification_conversion() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let manager = LogHistoryManager::try_new(&engine, snapshot.into(), None).unwrap();
        let commits = &manager.log_segment.ascending_commit_files;

        // Read the file modification timestamps
        let ts = manager.commit_file_to_timestamp(&engine, &commits[0], false);
        assert!(matches!(ts, Ok(50)));

        let ts = manager.commit_file_to_timestamp(&engine, &commits[1], false);
        assert!(matches!(ts, Ok(150)));

        let ts = manager.commit_file_to_timestamp(&engine, &commits[2], false);
        assert!(matches!(ts, Ok(250)));

        // Read the in-commit timestamps
        let ts = manager.commit_file_to_timestamp(&engine, &commits[3], true);
        assert!(matches!(ts, Ok(300)));
        let ts = manager.commit_file_to_timestamp(&engine, &commits[4], true);
        assert!(matches!(ts, Ok(400)));
    }

    #[tokio::test]
    async fn convert_timestamp_to_version() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let manager = LogHistoryManager::try_new(&engine, snapshot.into()).unwrap();

        // Less than the lowest ICT
        let mut res = manager.timestamp_to_version(&engine, 0, Bound::LeastUpper);
        assert!(matches!(res, Ok(0)), "{res:?}");

        // Negative timestamps are not allowed:
        res = manager.timestamp_to_version(&engine, -1, Bound::LeastUpper);
        assert!(
            matches!(res, Err(LogHistoryError::InvalidTimestamp(-1))),
            "{res:?}"
        );

        // GreatestLower Bound on timestamp that is less that all commits fails
        res = manager.timestamp_to_version(&engine, 0, Bound::GreatestLower);
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
        res = manager.timestamp_to_version(&engine, 1000, Bound::GreatestLower);
        assert!(matches!(res, Ok(4)), "{res:?}");

        // LeastUpper Bound on timestamp that is greater than all commits fails
        res = manager.timestamp_to_version(&engine, 1000, Bound::LeastUpper);
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
        res = manager.timestamp_to_version(&engine, 251, Bound::LeastUpper);
        assert!(matches!(res, Ok(3)), "{res:?}");
        res = manager.timestamp_to_version(&engine, 251, Bound::GreatestLower);
        assert!(matches!(res, Ok(2)), "{res:?}");

        // Right before first in-commit timestamp
        res = manager.timestamp_to_version(&engine, 299, Bound::LeastUpper);
        assert!(matches!(res, Ok(3)), "{res:?}");
        res = manager.timestamp_to_version(&engine, 299, Bound::GreatestLower);
        assert!(matches!(res, Ok(2)), "{res:?}");

        // Right after first in-commit timestamp
        res = manager.timestamp_to_version(&engine, 301, Bound::GreatestLower);
        assert!(matches!(res, Ok(3)), "{res:?}");
        res = manager.timestamp_to_version(&engine, 301, Bound::LeastUpper);
        assert!(matches!(res, Ok(4)), "{res:?}");
    }
}
