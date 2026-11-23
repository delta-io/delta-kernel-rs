//! Single-part checkpoint reconstruction using shared metadata planning and action replay.

mod write_metadata;

use super::CheckpointWriter;
use crate::action_reconciliation::RetentionCalculator as _;
use crate::action_reconciliation::plan::action_set::ActionSet;
use crate::action_reconciliation::plan::{MetadataPlanner, VERSION};
use crate::expressions::{Expression as Expr, Predicate, col, lit};
use crate::plans::{Plan, PlanBuilder};
use crate::scan::{PartitionValuesOptions, StatsOptions};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::{Engine, KernelError, KernelResult};

impl CheckpointWriter {
    /// Builds the rows for a single-part V1 checkpoint.
    ///
    /// Shares source planning and typed metadata conversion with scans. File writing and
    /// publication remain with the caller. Collation-stat sampling is unsupported.
    pub fn declarative_checkpoint_data_plan(&self, engine: &dyn Engine) -> KernelResult<Plan> {
        self.checkpoint_actions(engine)?.build()
    }

    fn checkpoint_actions(&self, engine: &dyn Engine) -> KernelResult<PlanBuilder> {
        if self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "This plan requires a V1 checkpoint writer",
            ));
        }
        self.metadata_planner(ActionSet::ALL)?
            .with_checkpoint_shape(self.snapshot.checkpoint_info(engine)?)
            .replay()?
            .filter(Predicate::and(
                file_retention_predicate(self.deleted_file_retention_timestamp()?),
                non_content_retention_predicate(self.get_transaction_expiration_timestamp()?),
            ))?
            .project_patch(checkpoint_file_projection)
    }

    /// Configures metadata replay using this writer's output representations and schemas.
    fn metadata_planner(&self, actions: ActionSet) -> KernelResult<MetadataPlanner<'_>> {
        let context = &self.schema_context;
        let config = self.snapshot.table_configuration();
        let segment = self.snapshot.log_segment();
        let write_struct = context.stats_config.write_stats_as_struct;
        if write_struct && config.has_collations_feature() {
            return Err(KernelError::unsupported(
                "Declarative checkpoint prototype does not implement collation-stat sampling",
            ));
        }
        let stats = StatsOptions {
            synthesize_json: context.stats_config.write_stats_as_json,
            // JSON synthesis preserves native bounds; structured output uses the writer's schema.
            variant_min_max: true,
            #[cfg(feature = "geo-type-in-dev")]
            geometry_min_max: true,
            ..if write_struct {
                StatsOptions::all()
            } else {
                StatsOptions::json_only()
            }
        };
        Ok(MetadataPlanner::new(segment, actions, false)
            .with_stats_schemas(
                stats
                    .json_synthesis_schema(config)?
                    .or_else(|| write_struct.then(|| context.stats_schema.clone())),
                write_struct.then(|| context.stats_schema.clone()),
            )
            .with_read_collations(stats.synthesize_json && config.has_collations_feature())
            .with_stats(stats)
            .with_partition_values(
                PartitionValuesOptions {
                    parsed_struct: write_struct,
                    ..Default::default()
                },
                context.partition_schema.clone().filter(|_| write_struct),
            ))
    }
}

// Checkpoint removes carry only tombstone metadata; sparse patches preserve null actions.
fn checkpoint_file_projection(
    patch: ProjectionStructPatchBuilder<'_>,
) -> ProjectionStructPatchBuilder<'_> {
    patch
        .drop_at(["remove"], "stats")
        .drop_at(["remove"], "tags")
        .replace_expr_at(["add"], "dataChange", lit(false))
        .replace_expr_at(["remove"], "dataChange", lit(false))
        .drop(VERSION)
}

// Retention follows replay so expired removes and deleted domains still suppress older state.
fn file_retention_predicate(file_cutoff: i64) -> Predicate {
    Predicate::or(
        col!("remove.path").is_null(),
        Expr::coalesce([col!("remove.deletionTimestamp"), lit(0i64)]).gt(lit(file_cutoff)),
    )
}

fn non_content_retention_predicate(txn_cutoff: Option<i64>) -> Predicate {
    let txn_retained = match txn_cutoff {
        // Replay has already suppressed older versions; missing lastUpdated expires with retention.
        Some(cutoff) => Predicate::or(
            col!("txn.appId").is_null(),
            col!("txn.lastUpdated").gt(lit(cutoff)),
        ),
        None => Predicate::TRUE,
    };
    Predicate::and(
        txn_retained,
        Expr::coalesce([col!("domainMetadata.removed"), lit(false)]).eq(lit(false)),
    )
}

#[cfg(test)]
mod tests;
