// TODO(#2337): remove dead_code allows when log compaction is re-enabled
#![allow(dead_code, unused_imports)]

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
/// 
/// Always returns `false` because log compaction is currently disabled.
pub fn should_compact(_commit_version: Version, _compaction_interval: Version) -> bool {
    // TODO(#2337): re-enable log compaction once testing is sufficient
    //
    // // Commits start at 0, so we add one to the commit version to check if we've hit the interval
    // compaction_interval > 0
    //     && commit_version > 0
    //     && (commit_version + 1).is_multiple_of(compaction_interval)
    false
}

/// Writer for log compaction files
///
/// This writer provides an API for creating log compaction files that aggregate actions
/// from multiple commit files.
///
/// Log compaction is currently disabled (#2337)
#[derive(Debug)]
pub struct LogCompactionWriter {
    /// Reference to the snapshot of the table being compacted
    snapshot: SnapshotRef,
    // TODO(#2337): remove allow(dead_code) when log compaction is re-enabled
    #[allow(dead_code)]
    start_version: Version,
    #[allow(dead_code)]
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
    // TODO(#2337): re-enable log compaction once testing is sufficient
    pub(crate) fn try_new(
        _snapshot: SnapshotRef,
        _start_version: Version,
        _end_version: Version,
    ) -> DeltaResult<Self> {
        Err(Error::unsupported(
            "Log compaction is not currently supported",
        ))
        // if start_version >= end_version {
        //     return Err(Error::generic(format!(
        //         "Invalid version range: end_version {end_version} must be greater than \
        //          start_version {start_version}"
        //     )));
        // }
        //
        // // We disallow log compaction if the Snapshot is not published. If we didn't, this
        // // could create gaps in the version history, thereby breaking old readers.
        // snapshot.log_segment().validate_published()?;
        //
        // // Compute the compaction path once during construction
        // let compaction_path = ParsedLogPath::new_log_compaction(
        //     snapshot.table_root(),
        //     start_version,
        //     end_version,
        // )?;
        //
        // Ok(Self {
        //     snapshot,
        //     start_version,
        //     end_version,
        //     compaction_path: compaction_path.location,
        // })
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
        let actions_iter =
            compaction_log_segment.read_actions(engine, COMPACTION_ACTIONS_SCHEMA.clone())?;

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
