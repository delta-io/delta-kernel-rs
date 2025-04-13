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
use crate::Error as DeltaError;
use crate::RowVisitor;
use crate::{DeltaResult, Engine, Version};
use timestamp_visitor::TimestampVisitor;

pub mod error;
mod search;
mod timestamp_visitor;

type Timestamp = i64;

#[derive(Debug)]
pub struct LogHistoryManager {
    log_segment: LogSegment,
    snapshot: Arc<Snapshot>,
    commit_to_timestamp_cache: RefCell<HashMap<Url, Timestamp>>,
}

static IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION: &str = "delta.inCommitTimestampEnablementVersion";
static IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP: &str =
    "delta.inCommitTimestampEnablementTimestamp";

enum TimestampSearchBounds {
    ExactMatch(Version),
    ICTSearchStartingFrom(usize),
    FileModificationSearch,
    FileModificationSearchUntil(usize),
}

/// When doing a search for ICT, we only consider the latest ICT range to be valid
impl LogHistoryManager {
    pub fn try_new(engine: &dyn Engine, table_root: Url) -> DeltaResult<Self> {
        let log_root = table_root.join("_delta_log/").unwrap();
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            log_root.clone(),
        )?;
        let snapshot = Arc::new(Snapshot::try_new(
            table_root,
            engine,
            Some(log_segment.end_version),
        )?);
        debug_assert!(
            !log_segment.ascending_commit_files.is_empty(),
            "LogSegment should ensure that a segment is non-empty"
        );
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
    fn commit_file_to_timestamp(
        &self,
        engine: &dyn Engine,
        commit_file: &ParsedLogPath,
        read_ict: bool,
    ) -> Result<Timestamp, LogHistoryError> {
        if let Some(cached) = self.get_cached_timestamp(commit_file) {
            return Ok(cached);
        }
        // By default, the timestamp of a commit is its modification time
        let mut commit_timestamp = commit_file.location.last_modified;

        if read_ict {
            let wrap_err = |error: DeltaError| LogHistoryError::FailedToReadTimestampForCommit {
                version: commit_file.version,
                error: Box::new(error),
            };

            // Read the commit file to get the In-Commit Timestamp if present.
            let mut action_iter = engine
                .json_handler()
                .read_json_files(
                    &[commit_file.location.clone()],
                    TimestampVisitor::schema(),
                    None,
                )
                .map_err(wrap_err)?
                // Take the first non-empty engine data batch
                .take_while(|res| res.as_ref().is_ok_and(|batch| batch.len() > 0));

            if let Some(batch) = action_iter.next() {
                let batch = batch.map_err(wrap_err)?;
                let mut visitor = TimestampVisitor {
                    commit_timestamp: &mut commit_timestamp,
                };
                // This updates the `commit_timestamp` through the mutable reference
                visitor.visit_rows_of(batch.as_ref()).map_err(wrap_err)?;
            }
        }

        self.update_cache_with_timestamp(commit_file, commit_timestamp);
        Ok(commit_timestamp)
    }

    pub fn latest_version_as_of(
        &self,
        engine: &dyn Engine,
        timestamp: Timestamp,
    ) -> DeltaResult<Version> {
        Ok(self.timestamp_to_version(engine, timestamp, Bound::GreatestLower)?)
    }

    pub fn first_version_after(
        &self,
        engine: &dyn Engine,
        timestamp: Timestamp,
    ) -> DeltaResult<Version> {
        Ok(self.timestamp_to_version(engine, timestamp, Bound::LeastUpper)?)
    }

    pub fn timestamp_range_to_versions(
        &self,
        engine: &dyn Engine,
        start_timestamp: Timestamp,
        end_timestamp: impl Into<Option<Timestamp>>,
    ) -> DeltaResult<(Version, Option<Version>)> {
        // Check that the start and end timestamps are valid
        let end_timestamp = end_timestamp.into();
        require!(
            start_timestamp > 0,
            LogHistoryError::InvalidTimestamp(start_timestamp).into()
        );
        if let Some(end_timestamp) = end_timestamp {
            require!(
                end_timestamp > 0,
                LogHistoryError::InvalidTimestamp(end_timestamp).into()
            );
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

                require!(
                    start_version <= end_version,
                    // This only fails if the entire timestamp range is between versions
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

    fn get_timestamp_search_bounds(
        &self,
        timestamp: Timestamp,
    ) -> Result<TimestampSearchBounds, LogHistoryError> {
        // FIXME: ensure that in-commit timestamp is enabled
        let config = self
            .snapshot
            .table_configuration()
            .metadata()
            .configuration();
        let ict_begin_timestamp = config.get(IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP);
        let ict_begin_version = config.get(IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION);

        let (ict_timestamp, ict_version) = match (ict_begin_timestamp, ict_begin_version) {
            (None, None) => return Ok(TimestampSearchBounds::FileModificationSearch),
            (Some(timestamp), Some(version)) => (
                timestamp.parse().map_err(|parse_error| {
                    LogHistoryError::FailedToParseConfiguration {
                        parse_error,
                        version: self.snapshot.version(),
                    }
                })?,
                version.parse().map_err(|parse_error| {
                    LogHistoryError::FailedToParseConfiguration {
                        parse_error,
                        version: self.snapshot.version(),
                    }
                })?,
            ),
            (_, _) => panic!("this shouldn't happen"),
        };

        let version_idx = || {
            self.log_segment
                .ascending_commit_files
                .binary_search_by(|x| x.version.cmp(&ict_version))
                .unwrap_or_else(|idx| idx)
        };

        let result = match timestamp.cmp(&ict_timestamp) {
            Ordering::Equal => TimestampSearchBounds::ExactMatch(ict_version),
            Ordering::Less => TimestampSearchBounds::FileModificationSearchUntil(version_idx()),
            Ordering::Greater => TimestampSearchBounds::ICTSearchStartingFrom(version_idx()),
        };
        Ok(result)
    }

    fn timestamp_to_version(
        &self,
        engine: &dyn Engine,
        timestamp: Timestamp,
        bound: Bound,
    ) -> Result<Version, LogHistoryError> {
        require!(timestamp > 0, LogHistoryError::InvalidTimestamp(timestamp));
        let len = self.log_segment.ascending_commit_files.len();

        // Determine the type of timestamp search. The search may either be over file modification
        // timestamps or over In-Commit Timestamps.
        let (lo, hi, read_ict) = match self.get_timestamp_search_bounds(timestamp)? {
            TimestampSearchBounds::ExactMatch(version) => return Ok(version),
            TimestampSearchBounds::ICTSearchStartingFrom(lo) => (lo, len, true),
            TimestampSearchBounds::FileModificationSearch => (0, len, false),
            TimestampSearchBounds::FileModificationSearchUntil(hi) => (0, hi, false),
        };
        debug_assert!(lo < hi, "Index range should be non-empty");

        let commit_to_ts = |commit| self.commit_file_to_timestamp(engine, commit, read_ict);

        // We only search in the range [lo..hi).
        let commit_range = &self.log_segment.ascending_commit_files[lo..hi];
        let search_result =
            binary_search_by_key_with_bounds(commit_range, timestamp, commit_to_ts, bound);

        // Get the index from the commit range if successful
        let relative_idx = match search_result {
            Ok(idx) => idx,
            Err(SearchError::OutOfRange) => {
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
