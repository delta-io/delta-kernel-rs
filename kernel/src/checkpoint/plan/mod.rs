//! Prototype checkpoint reconstruction using the same replay groups as metadata scans.

use super::{CheckpointShape, CheckpointWriter};
use crate::action_reconciliation::RetentionCalculator as _;
use crate::action_reconciliation::file_metadata::project_checkpoint_fields;
use crate::action_reconciliation::plan::sources::{checkpoint_actions, commit_actions};
use crate::action_reconciliation::plan::{
    ActionOptions, ReplayStrategy, parse_file_metadata, project_action_schema, replay,
    synthesize_json_stats,
};
use crate::actions::{ActionKind, CHECKPOINT_METADATA_NAME, CheckpointMetadata};
use crate::expressions::{Expression as Expr, Predicate, col, lit};
use crate::plans::Plan;
use crate::{Engine, KernelError, KernelResult, PlanBuilder};

impl CheckpointWriter {
    /// Builds a checkpoint-content relation using declarative action replay.
    ///
    /// Shares source planning and typed metadata conversion with scans. File writing and
    /// publication remain with the caller. Collation-stat sampling is unsupported.
    pub fn declarative_checkpoint_data_plan(&self, engine: &dyn Engine) -> KernelResult<Plan> {
        self.checkpoint_plan_with_cutoffs(
            engine,
            self.deleted_file_retention_timestamp()?,
            self.get_transaction_expiration_timestamp()?,
            if self.snapshot.log_segment().checkpoint_version.is_none() {
                ReplayStrategy::AggregateAll
            } else {
                ReplayStrategy::AggregateTailAndAntiJoin
            },
        )
    }

    fn checkpoint_plan_with_cutoffs(
        &self,
        engine: &dyn Engine,
        file_cutoff: i64,
        txn_cutoff: Option<i64>,
        replay_strategy: ReplayStrategy,
    ) -> KernelResult<Plan> {
        if self.schema_context.stats_config.write_stats_as_struct
            && self.snapshot.table_configuration().has_collations_feature()
        {
            return Err(KernelError::unsupported(
                "Declarative checkpoint prototype does not implement collation-stat sampling",
            ));
        }
        let segment = self.snapshot.log_segment();
        let shape = CheckpointShape::try_new_with_leaf_schema(
            engine.require_plan_executor()?.as_ref(),
            &self.snapshot,
        )?;
        let context = &self.schema_context;
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
            json_stats: context.stats_config.write_stats_as_json,
            parsed_stats: context
                .stats_config
                .write_stats_as_struct
                .then(|| context.stats_schema.clone()),
            parsed_partition_values: context.partition_schema.is_some()
                && context.stats_config.write_stats_as_struct,
        };
        // CRC allFiles omits retained removes and cannot serve as a checkpoint base.
        let base = checkpoint_actions(
            segment,
            Some(&shape),
            Some(self.snapshot.table_configuration()),
            &options,
        )?;
        let commits = commit_actions(segment, segment.checkpoint_version, &options)?;
        let mut state = replay(commits, base, replay_strategy)?;

        // Retention must follow both latest-action selection and cancellation of base values.
        // In particular, an expired remove or a deleted domain still suppresses older state.
        let txn_retained = match txn_cutoff {
            // Match Kernel's existing checkpoint implementation: missing lastUpdated is retained
            // for backward compatibility. This differs from the DBR rule discussed in the doc.
            Some(cutoff) => Predicate::or(
                col!("txn.lastUpdated").is_null(),
                col!("txn.lastUpdated").gt(lit(cutoff)),
            ),
            None => Predicate::TRUE,
        };
        state.plan = state.plan.filter(Predicate::or_from([
            col!("add.path").is_not_null(),
            Predicate::and(
                col!("remove.path").is_not_null(),
                Expr::coalesce([col!("remove.deletionTimestamp"), lit(0i64)]).gt(lit(file_cutoff)),
            ),
            col!("protocol.minReaderVersion").is_not_null(),
            col!("metaData.id").is_not_null(),
            Predicate::and(col!("txn.appId").is_not_null(), txn_retained),
            Predicate::and(
                col!("domainMetadata.domain").is_not_null(),
                Expr::coalesce([col!("domainMetadata.removed"), lit(false)]).eq(lit(false)),
            ),
        ]))?;

        // Parse only surviving actions. The same materializer runs before pruning for scans.
        let state = if context.stats_config.write_stats_as_struct {
            parse_file_metadata(&self.snapshot, state, &options, false)?
        } else {
            state
        };
        let state = if context.stats_config.write_stats_as_json {
            synthesize_json_stats(state)?
        } else {
            state
        };
        // Output padding is terminal: references are NULL columns, not available replay actions.
        let state = project_action_schema(state.plan, self.read_schema.clone())?
            .project_patch(|patch| project_checkpoint_fields(&context.stats_config, patch))?;
        let output_schema = state.schema().clone();
        if !context.is_v2 {
            return state.build();
        }
        let metadata: crate::expressions::Scalar = CheckpointMetadata {
            version: self.version,
            tags: None,
        }
        .into();
        let row = output_schema
            .fields()
            .map(|field| {
                if field.name() == CHECKPOINT_METADATA_NAME {
                    metadata.clone()
                } else {
                    crate::expressions::Scalar::null(field.data_type().clone())
                }
            })
            .collect();
        PlanBuilder::union_all([state, PlanBuilder::values(output_schema, vec![row])?])?.build()
    }
}

#[cfg(test)]
mod tests;
