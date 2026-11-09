use itertools::Itertools;
use url::Url;

use super::COMPACTION_ACTIONS_SCHEMA;
use crate::action_reconciliation::ActionReconciliationIterator;
use crate::action_reconciliation::log_replay::ActionReconciliationProcessor;
use crate::log_replay::HasSelectionVector;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::table_features::TableFeature;
use crate::{Engine, KernelError, KernelResult, SnapshotRef, Version};

/// Determine if log compaction should be performed based on the commit version and
/// compaction interval.
///
/// Returns `false` for a zero interval or the initial commit. Otherwise returns whether
/// `commit_version` completes an interval of `compaction_interval` commits.
pub fn should_compact(commit_version: Version, compaction_interval: Version) -> bool {
    compaction_interval > 0
        && commit_version > 0
        && commit_version % compaction_interval == compaction_interval - 1
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

impl LogCompactionWriter {
    pub(crate) fn try_new(
        snapshot: SnapshotRef,
        start_version: Version,
        end_version: Version,
    ) -> KernelResult<Self> {
        if start_version >= end_version {
            return Err(KernelError::generic(format!(
                "Invalid version range: end_version {end_version} must be greater than \
                 start_version {start_version}"
            )));
        }
        snapshot
            .table_configuration()
            .ensure_read_write_supported()?;
        if snapshot
            .table_configuration()
            .is_feature_enabled(&TableFeature::AdaptiveMetadataPreview)
        {
            return Err(KernelError::unsupported(
                "Log compaction is not supported for tables with adaptiveMetadata enabled",
            ));
        }
        snapshot.log_segment().validate_published()?;
        let compaction_path =
            ParsedLogPath::new_log_compaction(snapshot.table_root(), start_version, end_version)?;
        Ok(Self {
            snapshot,
            start_version,
            end_version,
            compaction_path: compaction_path.location,
        })
    }

    /// Return the output URL for the inclusive version range supplied at construction.
    pub fn compaction_path(&self) -> &Url {
        &self.compaction_path
    }

    /// Get an iterator over the compaction data to be written
    ///
    /// Uses `engine` to read and reconcile the inclusive version range. Removals and transaction
    /// markers are retained so they can suppress state from before the range. The caller must
    /// consume every batch and write the selected rows atomically to [`Self::compaction_path`].
    ///
    /// # Errors
    /// Returns an error if the range exceeds the snapshot or required commits are unavailable.
    /// Read and reconciliation failures may also be returned by the iterator.
    pub fn compaction_data(
        &mut self,
        engine: &dyn Engine,
    ) -> KernelResult<ActionReconciliationIterator> {
        // Validate that the requested version range is within the snapshot's range
        let snapshot_end_version = self.snapshot.version();
        if self.end_version > snapshot_end_version {
            return Err(KernelError::generic(format!(
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

        // A partial range must preserve removals and transaction winners that mask older state.
        let mut processor = ActionReconciliationProcessor::for_compaction(
            self.snapshot.table_configuration().dv_normalization_root(),
        );

        // Process actions using the same iterator pattern as checkpoints
        // The processor handles reverse chronological processing internally
        let result_iter = actions_iter
            .map(move |batch| processor.process_actions_batch(batch?))
            .filter_ok(HasSelectionVector::has_selected_rows);

        // Wrap the iterator to track action counts lazily
        Ok(ActionReconciliationIterator::new(Box::new(result_iter)))
    }
}
