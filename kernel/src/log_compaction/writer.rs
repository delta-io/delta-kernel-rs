use url::Url;

use super::COMPACTION_ACTIONS_SCHEMA;
use crate::action_reconciliation::log_replay::ActionReconciliationProcessor;
use crate::action_reconciliation::{ActionReconciliationIterator, RetentionCalculator};
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Engine, Error, SnapshotRef, Version};

/// Determine if log compaction should be performed based on the commit version and
/// compaction interval.
pub fn should_compact(commit_version: Version, compaction_interval: Version) -> bool {
    // Commits start at 0, so we add one to the commit version to check if we've hit the interval
    compaction_interval > 0
        && commit_version > 0
        && ((commit_version + 1) % compaction_interval) == 0
}

/// Writer for log compaction files
///
/// This writer provides an API for creating log compaction files that aggregate actions
/// from multiple commit files.
#[derive(Debug)]
pub struct LogCompactionWriter {
    /// Reference to the snapshot of the table being compacted
    snapshot: SnapshotRef,
    start_version: Version,
    end_version: Version,
    /// Cached compaction file path
    compaction_path: Url,
}

impl RetentionCalculator for LogCompactionWriter {
    fn table_properties(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }
}

impl LogCompactionWriter {
    pub(crate) fn try_new(
        snapshot: SnapshotRef,
        start_version: Version,
        end_version: Version,
    ) -> DeltaResult<Self> {
        if start_version >= end_version {
            return Err(Error::generic(format!(
                "Invalid version range: end_version {end_version} must be greater than start_version {start_version}"
            )));
        }

        // We disallow compaction if the LogSegment contains any unpublished commits. (could create
        // gaps in the version history, thereby breaking old readers)
        snapshot.log_segment().validate_no_staged_commits()?;

        // Compute the compaction path once during construction
        let compaction_path =
            ParsedLogPath::new_log_compaction(snapshot.table_root(), start_version, end_version)?;

        Ok(Self {
            snapshot,
            start_version,
            end_version,
            compaction_path: compaction_path.location,
        })
    }

    /// Get the path where the compaction file will be written
    pub fn compaction_path(&self) -> &Url {
        &self.compaction_path
    }

    /// Get an iterator over the compaction data to be written
    ///
    /// Performs action reconciliation for the version range specified in the constructor
    pub fn compaction_data(
        &mut self,
        engine: &dyn Engine,
    ) -> DeltaResult<ActionReconciliationIterator> {
        // Validate that the requested version range is within the snapshot's range
        let snapshot_end_version = self.snapshot.version();
        if self.end_version > snapshot_end_version {
            return Err(Error::generic(format!(
                "End version {} exceeds snapshot version {}",
                self.end_version, snapshot_end_version
            )));
        }

        // Create a log segment specifically for the compaction range
        // This ensures we only process commits in [start_version, end_version]
        let compaction_log_segment = LogSegment::for_table_changes(
            engine.storage_handler().as_ref(),
            self.snapshot.log_segment().log_root.clone(),
            self.start_version,
            Some(self.end_version),
        )?;

        // Read actions from the version-filtered log segment
        let actions_iter = compaction_log_segment.read_actions(
            engine,
            COMPACTION_ACTIONS_SCHEMA.clone(),
            None, // No predicate - we want all actions in the version range
        )?;

        let min_file_retention_timestamp_millis = self.deleted_file_retention_timestamp()?;

        // Create action reconciliation processor for compaction
        // This reuses the same reconciliation logic as checkpoints
        let processor = ActionReconciliationProcessor::new(
            min_file_retention_timestamp_millis,
            self.get_transaction_expiration_timestamp()?,
        );

        // Process actions using the same iterator pattern as checkpoints
        // The processor handles reverse chronological processing internally
        let result_iter = processor.process_actions_iter(actions_iter);

        // Wrap the iterator to track action counts lazily
        Ok(ActionReconciliationIterator::new(Box::new(result_iter)))
    }
}
