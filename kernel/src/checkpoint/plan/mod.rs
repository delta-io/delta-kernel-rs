//! Prototype checkpoint reconstruction using the same replay groups as metadata scans.

use std::sync::Arc;

use super::checkpoint_transform::build_checkpoint_transform;
use super::{CheckpointShape, CheckpointWriter};
use crate::action_reconciliation::RetentionCalculator as _;
use crate::action_reconciliation::plan::{ActionReplay, ReplayGroup, VERSION, checkpoint_actions};
use crate::actions::{CHECKPOINT_METADATA_NAME, CheckpointMetadata};
use crate::expressions::{Expression as Expr, Predicate, col, lit, null_lit};
use crate::plans::Plan;
use crate::schema::StructType;
use crate::{Engine, KernelError, KernelResult, PlanBuilder};

impl CheckpointWriter {
    /// Builds a checkpoint-content relation using declarative action replay.
    ///
    /// This prototype reuses the imperative writer's schema and statistics transform, but leaves
    /// file writing and publication to the caller. It does not implement collation-stat sampling.
    pub fn declarative_checkpoint_data_plan(&self, engine: &dyn Engine) -> KernelResult<Plan> {
        self.checkpoint_plan_with_cutoffs(
            engine,
            self.deleted_file_retention_timestamp()?,
            self.get_transaction_expiration_timestamp()?,
        )
    }

    fn checkpoint_plan_with_cutoffs(
        &self,
        engine: &dyn Engine,
        file_cutoff: i64,
        txn_cutoff: Option<i64>,
    ) -> KernelResult<Plan> {
        if self.schema_context.stats_config.write_stats_as_struct
            && self.snapshot.table_configuration().has_collations_feature()
        {
            return Err(KernelError::unsupported(
                "Declarative checkpoint prototype does not implement collation-stat sampling",
            ));
        }
        let segment = self.snapshot.log_segment();
        let shape =
            CheckpointShape::try_new(engine.require_plan_executor()?.as_ref(), &self.snapshot)?;
        let replay = ActionReplay::new(&[
            ReplayGroup::Files,
            ReplayGroup::Protocol,
            ReplayGroup::Metadata,
            ReplayGroup::Transactions,
            ReplayGroup::Domains,
        ]);
        let read_schema = Arc::new(StructType::try_new(replay.read_schema()?.fields().map(
            |field| {
                self.read_schema
                    .field(field.name())
                    .unwrap_or(field)
                    .clone()
            },
        ))?);
        // Checkpoint roots supply non-file state; V2 sidecars supply file state. Never select a
        // CRC as the base here: its allFiles omits the checkpoint's retained removes.
        let base = checkpoint_actions(segment, &shape, read_schema.clone())?;
        let tail = PlanBuilder::scan_json(
            segment.commit_cover_version_tagged_scan_files()?,
            &[VERSION],
            read_schema,
        )?;
        let state = replay.replay(base, tail)?;

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
        let state = state.filter(Predicate::or_from([
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

        // Restore the writer's full input schema, including null checkpoint references. Reuse its
        // existing JSON/structured stats and partition projection instead of duplicating policy.
        let state_schema = state.schema().clone();
        let state = state.project(
            Expr::struct_from(self.read_schema.fields().map(|field| {
                if state_schema.contains(field.name()) {
                    Expr::column([field.name()])
                } else {
                    null_lit(field.data_type().clone())
                }
            })),
            self.read_schema.clone(),
        )?;
        let context = &self.schema_context;
        let (output_schema, transform) = build_checkpoint_transform(
            &context.stats_config,
            &self.read_schema,
            &context.stats_schema,
            context.partition_schema.as_ref(),
        )?;
        let state = state.project(transform, output_schema.clone())?;
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
