use crate::listed_log_files::*;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::{DeltaResult, Engine, Error, FileMeta, Version};

use url::Url;

use super::COMPACTION_ACTIONS_SCHEMA;
use crate::checkpoint::log_replay::{CheckpointBatch, CheckpointLogReplayProcessor};
use crate::engine_data::FilteredEngineData;
use crate::log_replay::LogReplayProcessor;

/// Utility function to determine if log compaction should be performed
/// based on the commit version and compaction interval.
#[allow(dead_code)]
pub(crate) fn should_compact(commit_version: Version, compaction_interval: Version) -> bool {
    // Commits start at 0, so we add one to the commit version to check if we've hit the interval
    compaction_interval > 0
        && commit_version > 0
        && ((commit_version + 1) % compaction_interval) == 0
}

/// Writer for log compaction files
///
/// This writer provides an API similar to [`CheckpointWriter`] for creating
/// log compaction files that aggregate actions from multiple commit files.
///
/// [`CheckpointWriter`]: crate::checkpoint::CheckpointWriter
#[derive(Debug)]
pub struct LogCompactionWriter {
    table_root: Url,
    start_version: Version,
    end_version: Version,
    compaction_path: ParsedLogPath<Url>,
    /// Minimum file retention timestamp for tombstone cleanup
    min_file_retention_timestamp_millis: i64,
}

impl LogCompactionWriter {
    pub fn try_new(
        table_root: Url,
        start_version: Version,
        end_version: Version,
        min_file_retention_timestamp_millis: i64,
    ) -> DeltaResult<Self> {
        if start_version > end_version {
            return Err(Error::generic(format!(
                "Invalid version range: start_version {} > end_version {}",
                start_version, end_version
            )));
        }

        let compaction_path =
            ParsedLogPath::new_log_compaction(&table_root, start_version, end_version)?;

        Ok(Self {
            table_root,
            start_version,
            end_version,
            compaction_path,
            min_file_retention_timestamp_millis,
        })
    }

    /// Get the path where the compaction file will be written
    pub fn compaction_path(&self) -> DeltaResult<Url> {
        Ok(self.compaction_path.location.clone())
    }

    /// Get an iterator over the compaction data to be written
    ///
    /// This method performs action reconciliation similar to checkpoint creation,
    /// but specifically for the version range specified in the constructor.
    /// It reuses the CheckpointLogReplayProcessor to ensure consistent reconciliation
    /// logic with checkpoint creation.
    pub fn compaction_data(
        &mut self,
        engine: &dyn Engine,
    ) -> DeltaResult<LogCompactionDataIterator> {
        // List commit files in the specified range
        let commit_files = self.list_commit_files(engine)?;

        // Validate that we have the expected number of commit files
        let expected_count: usize = self
            .end_version
            .checked_sub(self.start_version)
            .and_then(|diff| diff.checked_add(1))
            .and_then(|count| count.try_into().ok())
            .ok_or_else(|| {
                Error::generic(format!(
                    "Invalid version range: cannot compute expected file count for range [{}, {}]",
                    self.start_version, self.end_version
                ))
            })?;
        if commit_files.len() != expected_count {
            // Provide detailed information about missing versions
            let found_versions: Vec<Version> = commit_files.iter().map(|f| f.version).collect();
            let expected_versions: Vec<Version> = (self.start_version..=self.end_version).collect();
            let missing: Vec<Version> = expected_versions
                .iter()
                .filter(|v| !found_versions.contains(v))
                .copied()
                .collect();

            return Err(Error::generic(format!(
                "Expected {} commit files for range [{}, {}], but found {}. Missing versions: {:?}",
                expected_count,
                self.start_version,
                self.end_version,
                commit_files.len(),
                missing
            )));
        }

        // Create a log segment from these commit files
        let log_segment = self.create_log_segment(commit_files)?;

        // Read actions from the log segment - the segment handles reading commits in the right order
        let actions_iter = log_segment.read_actions(
            engine,
            COMPACTION_ACTIONS_SCHEMA.clone(),
            COMPACTION_ACTIONS_SCHEMA.clone(),
            None, // No predicate - we want all actions
        )?;

        // Create checkpoint log replay processor for compaction
        // This reuses the same reconciliation logic as checkpoints
        // Use None for txn_expiration_timestamp as we typically want to preserve all transactions in compaction
        let processor = CheckpointLogReplayProcessor::new(
            self.min_file_retention_timestamp_millis,
            None, // txn_expiration_timestamp
        );

        // Process actions using the same iterator pattern as checkpoints
        // The processor handles reverse chronological processing internally
        let result_iter = processor.process_actions_iter(actions_iter);

        // Wrap the iterator in a LogCompactionDataIterator to track action counts lazily
        Ok(LogCompactionDataIterator {
            compaction_batch_iterator: Box::new(result_iter),
            actions_count: 0,
            add_actions_count: 0,
        })
    }

    /// List commit files in the specified version range
    fn list_commit_files(&self, engine: &dyn Engine) -> DeltaResult<Vec<ParsedLogPath<FileMeta>>> {
        let log_root = self.table_root.join("_delta_log/")?;

        // Get all files in the _delta_log directory
        let log_files = engine.storage_handler().list_from(&log_root)?;

        // Filter and parse log files, keeping only commits in our version range
        let mut commit_files = Vec::new();

        for file in log_files {
            let file = file?; // Handle the Result<FileMeta, Error>
            if let Some(parsed_path) = ParsedLogPath::try_from(file)? {
                if parsed_path.is_commit()
                    && parsed_path.version >= self.start_version
                    && parsed_path.version <= self.end_version
                {
                    commit_files.push(parsed_path);
                }
            }
        }

        // Sort by version in ascending order (LogSegment expects this)
        // Note: Reverse chronological processing should happen in the log replay processor
        // to ensure latest actions win during reconciliation
        commit_files.sort_by_key(|f| f.version);

        Ok(commit_files)
    }

    /// Create a log segment from the commit files
    pub(crate) fn create_log_segment(
        &self,
        mut commit_files: Vec<ParsedLogPath<FileMeta>>,
    ) -> DeltaResult<LogSegment> {
        // Sort by version in ascending order (required by LogSegment)
        commit_files.sort_by_key(|f| f.version);

        // Create listed log files structure
        let listed_files = ListedLogFiles {
            ascending_commit_files: commit_files,
            ascending_compaction_files: vec![], // No compaction files for this segment
            checkpoint_parts: vec![],           // No checkpoint files for this segment
            latest_crc_file: None,              // No CRC files for this segment
        };

        // Create the log segment
        LogSegment::try_new(
            listed_files,
            self.table_root.clone(),
            Some(self.end_version),
        )
    }
}

/// Iterator over log compaction data
///
/// This iterator provides the reconciled actions that should be written
/// to the compaction file. It follows a similar pattern to CheckpointDataIterator.
pub struct LogCompactionDataIterator {
    /// The nested iterator that yields compaction batches with action counts
    pub(crate) compaction_batch_iterator:
        Box<dyn Iterator<Item = DeltaResult<CheckpointBatch>> + Send>,
    /// Running total of actions included in the compaction
    pub(crate) actions_count: i64,
    /// Running total of add actions included in the compaction
    pub(crate) add_actions_count: i64,
}

impl LogCompactionDataIterator {
    /// Get the total number of actions in the compaction
    pub fn total_actions(&self) -> i64 {
        self.actions_count
    }

    /// Get the total number of add actions in the compaction
    pub fn total_add_actions(&self) -> i64 {
        self.add_actions_count
    }
}

impl std::fmt::Debug for LogCompactionDataIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogCompactionDataIterator")
            .field("actions_count", &self.actions_count)
            .field("add_actions_count", &self.add_actions_count)
            .finish()
    }
}

impl Iterator for LogCompactionDataIterator {
    type Item = DeltaResult<FilteredEngineData>;

    /// Advances the iterator and returns the next value.
    ///
    /// This implementation transforms the `CheckpointBatch` items from the nested iterator into
    /// [`FilteredEngineData`] items for the engine to write, while accumulating action counts from
    /// each batch.
    fn next(&mut self) -> Option<Self::Item> {
        Some(self.compaction_batch_iterator.next()?.map(|batch| {
            self.actions_count += batch.actions_count;
            self.add_actions_count += batch.add_actions_count;
            batch.filtered_data
        }))
    }
}
