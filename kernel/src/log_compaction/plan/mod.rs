//! Declarative reconciliation of a bounded commit interval.

use crate::action_reconciliation::plan::action_set::ActionSet;
use crate::action_reconciliation::plan::{MetadataPlanner, VERSION};
use crate::actions::{ADD_NAME, REMOVE_NAME};
use crate::committer::file_meta_from_batches;
use crate::engine::plan_storage::PlanBasedStorageHandler;
use crate::expressions::lit;
use crate::path::ParsedLogPath;
use crate::plans::{Operation, PlanBuilder};
use crate::scan::StatsOptions;
use crate::table_features::TableFeature;
use crate::{Engine, FileMeta, KernelError, Result, Snapshot, StorageHandler, Version};

impl Snapshot {
    /// Atomically writes a minor log compaction and returns its file metadata.
    ///
    /// Requires a plan executor and a complete published interval with
    /// `start_version < end_version <= self.version()`, after this snapshot's checkpoint.
    /// Retains winning removes, transactions, and domain deletions regardless of age; includes
    /// protocol and metadata only when present.
    /// File actions have `dataChange=false`.
    ///
    /// Errors on existing destinations or empty results; missing commits report
    /// `DELTA_VERSIONS_NOT_CONTIGUOUS`. Errors after publication may leave a complete file.
    /// AMT, automatic compaction, and compacted-file discovery are unsupported.
    /// SyncEngine buffers input/output in memory; bound the interval to available memory.
    pub fn compact_log(
        &self,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Version,
    ) -> Result<FileMeta> {
        let executor = engine.require_plan_executor()?;
        self.table_configuration().ensure_read_write_supported()?;
        let storage = PlanBasedStorageHandler::new(executor.clone());
        let actions = self.log_compaction_actions(&storage, start_version, end_version)?;
        let path =
            ParsedLogPath::new_log_compaction(self.table_root(), start_version, end_version)?;
        let plan = PlanBuilder::write_json([actions], path.location, false, false)?.build()?;
        let result = executor.execute_op(Operation::QueryPlan(plan))?;
        Ok(file_meta_from_batches(result.into_data()?)?)
    }

    fn log_compaction_actions(
        &self,
        storage: &dyn StorageHandler,
        start_version: Version,
        end_version: Version,
    ) -> Result<PlanBuilder> {
        if self
            .table_configuration()
            .is_feature_enabled(&TableFeature::AdaptiveMetadataPreview)
        {
            return Err(KernelError::unsupported(
                "Declarative log compaction does not support adaptiveMetadata",
            )
            .into());
        }
        let segment = self
            .log_segment()
            .for_compaction(storage, start_version, end_version)?;
        Ok(MetadataPlanner::new(&segment, ActionSet::ALL)
            .with_stats(StatsOptions::json_only())
            .replay()?
            .project_patch(|patch| {
                patch
                    .replace_expr_at([ADD_NAME], "dataChange", lit(false))
                    .replace_expr_at([REMOVE_NAME], "dataChange", lit(false))
                    .drop(VERSION)
            })?)
    }
}

#[cfg(test)]
mod tests;
