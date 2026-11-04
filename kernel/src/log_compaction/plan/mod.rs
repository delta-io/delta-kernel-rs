//! Declarative reconciliation of a bounded commit interval.

use crate::action_reconciliation::plan::sources::commit_actions;
use crate::action_reconciliation::plan::{ActionOptions, VERSION, replay_commits};
use crate::actions::{ADD_NAME, ActionKind, REMOVE_NAME};
use crate::expressions::lit;
use crate::log_segment::LogSegment;
use crate::plans::Plan;
use crate::table_features::TableFeature;
use crate::{Engine, KernelError, KernelResult, Snapshot, Version};

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
        let options = ActionOptions {
            actions: vec![
                ActionKind::FileActions {
                    includes_removes: true,
                },
                ActionKind::Protocol,
                ActionKind::Metadata,
                ActionKind::SetTransaction,
                ActionKind::DomainMetadata,
            ],
            json_stats: true,
            parsed_stats: None,
            parsed_partition_values: false,
        };
        let commits = commit_actions(&segment, None, &options)?;
        replay_commits(commits)?
            .0
            .project_patch(|patch| {
                patch
                    .replace_expr_at([ADD_NAME], "dataChange", lit(false))
                    .replace_expr_at([REMOVE_NAME], "dataChange", lit(false))
                    .drop(VERSION)
            })?
            .build()
    }
}

#[cfg(test)]
mod tests;
