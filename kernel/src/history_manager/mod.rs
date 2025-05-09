//! This module defines the [`LogHistoryManager`], which can be used to perform timestamp queries
//! over the Delta Log, translating from timestamps to Delta versions.

use error::{LogHistoryError, TimestampOutOfRangeError};
use search::{binary_search_by_key_with_bounds, Bound, SearchError};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use url::Url;

use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::snapshot::Snapshot;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error as DeltaError, RowVisitor, Version};
use timestamp_visitor::InCommitTimestampVisitor;

#[cfg(feature = "internal-api")]
pub mod search;
#[cfg(not(feature = "internal-api"))]
mod search;

pub mod error;
mod timestamp_visitor;

type Timestamp = i64;

/// The [`LogHistoryManager`] performs timestamp queries over the Delta Log, translating
/// between timestamps and Delta versions.
///
/// # Usage
///
/// Use this manager to:
/// - Convert timestamps or timestamp ranges into Delta versions or version ranges
/// - Perform time travel queries using `Table::snapshot`
/// - Execute timestamp-based change data feed queries using `Table::table_changes`
///
/// The [`LogHistoryManager`] works with tables regardless of whether they have In-Commit
/// Timestamps enabled.
///
/// # Limitations
///
/// Once created, the [`LogHistoryManager`] does not automatically update with newer versions
/// of the table. All timestamp queries are limited to the state captured in the [`Snapshot`]
/// provided during construction.
#[derive(Debug)]
pub struct LogHistoryManager {
    log_segment: LogSegment,
    snapshot: Arc<Snapshot>,
    commit_to_timestamp_cache: RefCell<HashMap<Url, Timestamp>>,
}

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
    ///   greatest time that this [`LogHistoryManager`] will be able to resolve
    ///   timestamps for.
    /// - `limit`: Optional maximum number of versions to track in the history manager's state.
    ///   When specified, the earliest queryable version is `snapshot.version - limit`.
    ///   This parameter allows trading memory usage for historical reach.
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
            commit_to_timestamp_cache: Default::default(),
        })
    }
    fn update_cache_with_timestamp(&self, commit_file: &ParsedLogPath, value: Timestamp) {
        self.commit_to_timestamp_cache
            .borrow_mut()
            .insert(commit_file.location.location.clone(), value);
    }
    fn get_cached_timestamp(&self, commit_file: &ParsedLogPath) -> Option<Timestamp> {
        self.commit_to_timestamp_cache
            .borrow()
            .get(&commit_file.location.location)
            .copied()
    }

    /// Gets the timestamp for the `commit_file`. If `read_ict` is false ,this returns the file's
    /// modification timestamp. If `read_ict` is true, this reads the file's In-commit timestamp.
    fn commit_file_to_timestamp(
        &self,
        engine: &dyn Engine,
        commit_file: &ParsedLogPath,
        read_ict: bool,
    ) -> Result<Timestamp, LogHistoryError> {
        if let Some(cached) = self.get_cached_timestamp(commit_file) {
            return Ok(cached);
        }
        let commit_timestamp = if read_ict {
            Self::read_in_commit_timestamp(engine, commit_file)?
        } else {
            // By default, the timestamp of a commit is its modification time
            commit_file.location.last_modified
        };

        self.update_cache_with_timestamp(commit_file, commit_timestamp);

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
        let action_iter = engine
            .json_handler()
            .read_json_files(
                &[commit_file.location.clone()],
                InCommitTimestampVisitor::schema(),
                None,
            )
            .map_err(wrap_err)?;

        // Take the first engine data batch
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

    /// Gets the latest version that occurs before or at the given `timestamp`.
    ///
    /// This finds the version whose timestamp is less than or equal to `timestamp`.
    /// If no such version exists, returns [`LogHistoryError::TimestampOutOfRange`].
    ///
    ////// # Examples
    /// ```rust
    /// # use delta_kernel::history_manager::error::LogHistoryError;
    /// # use delta_kernel::engine::sync::SyncEngine;
    /// # use delta_kernel::Table;
    /// # use std::sync::Arc;
    /// # let path = "./tests/data/with_checkpoint_no_last_checkpoint";
    /// # let engine = Arc::new(SyncEngine::new());
    /// let table = Table::try_from_uri(path)?;
    /// let manager = table.history_manager(engine.as_ref(), None)?;
    ///
    /// // Get the latest version as of January 1, 2023
    /// let timestamp = 1672531200000; // Milliseconds since epoch for 2023-01-01
    /// let version_res = manager.latest_version_as_of(engine.as_ref(), timestamp);
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn latest_version_as_of(
        &self,
        engine: &dyn Engine,
        timestamp: Timestamp,
    ) -> DeltaResult<Version> {
        Ok(self.timestamp_to_version(engine, timestamp, Bound::GreatestLower)?)
    }

    /// Gets the first version that occurs after the given `timestamp` (inclusive).
    ///
    /// This finds the version whose timestamp is greater than or equal to `timestamp`.
    /// If no such version exists, returns [`LogHistoryError::TimestampOutOfRange`].
    /// # Examples
    /// ```rust
    /// # use delta_kernel::engine::sync::SyncEngine;
    /// # use delta_kernel::Table;
    /// # use std::sync::Arc;
    /// # let path = "./tests/data/with_checkpoint_no_last_checkpoint";
    /// # let engine = Arc::new(SyncEngine::new());
    /// let table = Table::try_from_uri(path)?;
    /// let manager = table.history_manager(engine.as_ref(), None)?;
    ///
    /// // Find the first version that occurred after January 1, 2023
    /// let timestamp = 1672531200000; // Milliseconds since epoch for 2023-01-01
    /// let version_res = manager.first_version_after(engine.as_ref(), timestamp);
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn first_version_after(
        &self,
        engine: &dyn Engine,
        timestamp: Timestamp,
    ) -> DeltaResult<Version> {
        Ok(self.timestamp_to_version(engine, timestamp, Bound::LeastUpper)?)
    }

    /// Converts a timestamp range to a corresponding version range.
    ///
    /// This function finds the version range that corresponds to the given timestamp range.
    /// The returned tuple contains:
    /// - The first (earliest) version with a timestamp greater than or equal to `start_timestamp`
    /// - If `end_timestamp` is provided, the version with a timestamp less than or equal to `end_timestamp`.
    ///
    /// # Arguments
    /// * `engine` - The engine used to access version history
    /// * `start_timestamp` - The lower bound timestamp (inclusive)
    /// * `end_timestamp` - The optional upper bound timestamp (inclusive), or `None` to indicate no upper bound
    ///
    /// # Returns
    /// A tuple containing the start version and optional end version (inclusive)
    ///
    /// # Errors
    /// Returns [`LogHistoryError::TimestampOutOfRange`] if:
    /// - No version exists at or after `start_timestamp`
    /// - `end_timestamp` is provided and no version exists at or before it
    ///
    /// Returns [`LogHistoryError::InvalidTimestampRange`] if the entire range [start_timestamp,
    /// end_timestamp]
    ///
    /// # Examples
    /// ```rust
    /// # use delta_kernel::engine::sync::SyncEngine;
    /// # use delta_kernel::Table;
    /// # use std::sync::Arc;
    /// # let path = "./tests/data/with_checkpoint_no_last_checkpoint";
    /// # let engine = Arc::new(SyncEngine::new());
    ///
    /// let table = Table::try_from_uri(path)?;
    /// let manager = table.history_manager(engine.as_ref(), None)?;
    ///
    /// // Find versions between January 1, 2023 and March 1, 2023
    /// let start_timestamp = 1672531200000; // Jan 1, 2023 (milliseconds since epoch)
    /// let end_timestamp = 1677628800000;   // Mar 1, 2023 (milliseconds since epoch)
    ///
    /// let version_range_res =
    ///     manager.timestamp_range_to_versions(engine.as_ref(), start_timestamp, end_timestamp);
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn timestamp_range_to_versions(
        &self,
        engine: &dyn Engine,
        start_timestamp: Timestamp,
        end_timestamp: impl Into<Option<Timestamp>>,
    ) -> DeltaResult<(Version, Option<Version>)> {
        // Check that the start and end timestamps are valid. Timestamps must be positive
        let end_timestamp = end_timestamp.into();
        require!(
            0 <= start_timestamp,
            LogHistoryError::InvalidTimestamp(start_timestamp).into()
        );
        if let Some(end_timestamp) = end_timestamp {
            require!(
                0 <= end_timestamp,
                LogHistoryError::InvalidTimestamp(end_timestamp).into()
            );
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
        let start_version = self.first_version_after(engine, start_timestamp)?;

        // If the end timestamp is present, convert it to an end version
        let end_version = end_timestamp
            .map(|end| {
                let end_version = self.latest_version_as_of(engine, end)?;

                // Verify that the start version is no greater than the end version. This can
                // happen in the case that the entire timestamp range falls between two commits.
                // Consider the following history:
                // |-------------|--------------------|---------------|
                // v4       start_timestamp      end_timestamp       v5
                //
                // The latest version as of the end_timestamp is 4. The first version after the
                // start_timestamp is 5. Thus in the case where end_version < start_version, we
                // return and [`LogHistoryError::EmptyTimestampRange`].
                require!(
                    start_version <= end_version,
                    DeltaError::from(LogHistoryError::EmptyTimestampRange {
                        end_timestamp: end,
                        start_timestamp,
                        between_left: end_version,
                        between_right: start_version
                    })
                );

                Ok(end_version)
            })
            .transpose()?;

        Ok((start_version, end_version))
    }

    /// Given a timestamp, this function determines the commit range that timestamp conversion
    /// should search. A timestamp search may be conducted over one of two version ranges:
    ///     1) A range of commits whose timestamp is the file modification timestamp
    ///     2) A range of commits whose timestamp is an in-commit timestamp.
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
        let manager = LogHistoryManager::try_new(&engine, snapshot.into(), None).unwrap();

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
        let manager = LogHistoryManager::try_new(&engine, snapshot.into(), None).unwrap();

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
        let manager = LogHistoryManager::try_new(&engine, snapshot.into(), None).unwrap();

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
