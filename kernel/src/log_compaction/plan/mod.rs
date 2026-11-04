//! Declarative reconciliation of a bounded commit interval.

use crate::action_reconciliation::plan::{ActionReplay, ReplayGroup, VERSION};
use crate::actions::{ADD_NAME, REMOVE_NAME};
use crate::expressions::lit;
use crate::log_segment::LogSegment;
use crate::plans::Plan;
use crate::table_features::TableFeature;
use crate::{Engine, KernelError, KernelResult, PlanBuilder, Snapshot, Version};

impl Snapshot {
    /// Builds the rows for a JSON log compaction of `[start_version, end_version]` (inclusive).
    ///
    /// Requires `start_version < end_version <= self.version()` and a complete, published commit
    /// interval. Reads only that interval, without loading a checkpoint or executing the plan.
    /// Winning removes, transactions, and domain deletions are retained regardless of age; protocol
    /// and metadata appear only when present in the interval. File actions have `dataChange=false`.
    /// AMT tables are unsupported. Writing and publication are left to the caller; automatic log
    /// compaction remains disabled.
    pub fn declarative_log_compaction_data_plan(
        &self,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Version,
    ) -> KernelResult<Plan> {
        if start_version >= end_version || end_version > self.version() {
            return Err(KernelError::generic(
                "Log compaction requires start_version < end_version <= snapshot version",
            ));
        }
        if self
            .table_configuration()
            .is_feature_enabled(&TableFeature::AdaptiveMetadataPreview)
        {
            return Err(KernelError::unsupported(
                "Declarative log compaction does not support adaptiveMetadata",
            ));
        }
        let segment = LogSegment::for_table_changes(
            engine.storage_handler().as_ref(),
            self.log_segment().log_root.clone(),
            start_version,
            Some(end_version),
        )?;
        segment.validate_published()?;
        let replay = ActionReplay::new(&[
            ReplayGroup::Files,
            ReplayGroup::Protocol,
            ReplayGroup::Metadata,
            ReplayGroup::Transactions,
            ReplayGroup::Domains,
        ]);
        let commits = PlanBuilder::scan_json(
            segment.commit_cover_version_tagged_scan_files()?,
            &[VERSION],
            replay.read_schema()?,
        )?;
        replay
            .replay_commits(commits)?
            .project_patch(|patch| {
                patch
                    .replace_expr_at([ADD_NAME], "dataChange", lit(false))
                    .replace_expr_at([REMOVE_NAME], "dataChange", lit(false))
            })?
            .build()
    }
}

#[cfg(test)]
mod tests;
