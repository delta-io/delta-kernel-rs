//! Single-part checkpoint reconstruction using shared metadata planning and action replay.

use super::{CheckpointShape, CheckpointWriter};
use crate::action_reconciliation::RetentionCalculator as _;
use crate::action_reconciliation::plan::{MetadataPlanner, VERSION};
use crate::actions::ActionSet;
use crate::expressions::{Expression as Expr, Predicate, col, lit};
use crate::plans::Plan;
use crate::scan::{PartitionValuesOptions, StatsOptions};
use crate::{Engine, KernelError, KernelResult};

impl CheckpointWriter {
    /// Builds the rows for a single-part V1 checkpoint.
    ///
    /// Shares source planning and typed metadata conversion with scans. File writing and
    /// publication remain with the caller. Collation-stat sampling is unsupported.
    pub fn declarative_checkpoint_data_plan(&self, engine: &dyn Engine) -> KernelResult<Plan> {
        if self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "This plan requires a V1 checkpoint writer",
            ));
        }
        self.metadata_planner(ActionSet::ALL)?
            .with_checkpoint_shape(CheckpointShape::try_new_with_leaf_schema(
                engine.require_plan_executor()?.as_ref(),
                &self.snapshot,
            )?)
            .replay()?
            .filter(Predicate::and(
                file_retention_predicate(self.deleted_file_retention_timestamp()?),
                non_content_retention_predicate(self.get_transaction_expiration_timestamp()?),
            ))?
            .project_patch(|patch| patch.drop(VERSION))?
            .build()
    }

    /// Configures metadata replay using this writer's output representations and schemas.
    fn metadata_planner(&self, actions: ActionSet) -> KernelResult<MetadataPlanner<'_>> {
        let context = &self.schema_context;
        let config = self.snapshot.table_configuration();
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
        Ok(MetadataPlanner::new(self.snapshot.log_segment(), actions)
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

// Retention follows replay so expired removes and deleted domains still suppress older state.
fn file_retention_predicate(file_cutoff: i64) -> Predicate {
    Predicate::or(
        col!("remove.path").is_null(),
        Expr::coalesce([col!("remove.deletionTimestamp"), lit(0i64)]).gt(lit(file_cutoff)),
    )
}

fn non_content_retention_predicate(txn_cutoff: Option<i64>) -> Predicate {
    let txn_retained = match txn_cutoff {
        // Kernel retains a missing lastUpdated for compatibility with existing checkpoints.
        Some(cutoff) => Predicate::or(
            col!("txn.lastUpdated").is_null(),
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
