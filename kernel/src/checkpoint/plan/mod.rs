//! Checkpoint reconstruction using shared metadata planning and action replay.

mod write_metadata;

use super::{CheckpointWriter, collation_stats};
use crate::action_reconciliation::RetentionCalculator as _;
use crate::action_reconciliation::log_replay::ActionReconciliationBatch;
use crate::action_reconciliation::plan::action_set::ActionSet;
use crate::action_reconciliation::plan::{MetadataPlanner, VERSION};
use crate::expressions::{Expression as Expr, Predicate, col, column_name, lit};
use crate::plans::{Operation, Plan};
use crate::scan::{PartitionValuesOptions, StatsOptions};
use crate::{Engine, FilteredEngineData, KernelError, KernelResult};

impl CheckpointWriter {
    /// Builds the rows for a single-part V1 checkpoint.
    ///
    /// Shares source planning and typed metadata conversion with scans. File writing and
    /// publication remain with the caller. Structured collation stats require an extra engine
    /// replay during planning to sample one live Add using Kernel's existing sampling policy.
    pub fn declarative_checkpoint_data_plan(&self, engine: &dyn Engine) -> KernelResult<Plan> {
        if self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "This plan requires a V1 checkpoint writer",
            ));
        }
        self.metadata_planner(engine, ActionSet::ALL)?
            .replay()?
            .filter(Predicate::and(
                file_retention_predicate(self.deleted_file_retention_timestamp()?),
                non_content_retention_predicate(self.get_transaction_expiration_timestamp()?),
            ))?
            .project_patch(|patch| patch.drop(VERSION))?
            .build()
    }

    /// Builds separate content and non-content reconstruction plans for a V2 checkpoint.
    /// Returns `(content, non_content)` for sidecar and manifest writes, respectively.
    ///
    /// The non-content plan reads the checkpoint root without resolving sidecars. Sidecar
    /// references, checkpointMetadata, and publication are supplied by the write orchestration.
    /// Structured collation stats require the same planning-time sampling replay as V1.
    pub fn declarative_v2_checkpoint_data_plans(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<(Plan, Plan)> {
        if !self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "V2 checkpoint plans require the v2Checkpoint feature",
            ));
        }
        let content = self
            .metadata_planner(engine, ActionSet::live_adds_with_tombstones())?
            .replay()?
            .filter(file_retention_predicate(
                self.deleted_file_retention_timestamp()?,
            ))?
            .project_patch(|patch| patch.drop(VERSION))?
            .build()?;
        let non_content = MetadataPlanner::new(
            self.snapshot.log_segment(),
            ActionSet {
                live_adds: false,
                include_tombstones: false,
                ..ActionSet::ALL
            },
        )
        .replay()?
        .filter(non_content_retention_predicate(
            self.get_transaction_expiration_timestamp()?,
        ))?
        .project_patch(|patch| patch.drop(VERSION))?
        .build()?;
        Ok((content, non_content))
    }

    /// Configures metadata replay using this writer's output representations and schemas.
    fn metadata_planner(
        &self,
        engine: &dyn Engine,
        actions: ActionSet,
    ) -> KernelResult<MetadataPlanner<'_>> {
        let context = &self.schema_context;
        let config = self.snapshot.table_configuration();
        let write_struct = context.stats_config.write_stats_as_struct;
        let shape = self.snapshot.checkpoint_info(engine)?;
        let output_stats_schema = if write_struct && config.has_collations_feature() {
            // The engine reconciles files; only one live Add returns for versioned schema
            // discovery.
            let sample = MetadataPlanner::new(
                self.snapshot.log_segment(),
                ActionSet::live_adds_with_tombstones(),
            )
            .with_checkpoint_shape(shape.clone())
            .with_stats(StatsOptions::json_only())
            .with_stats_schemas(Some(context.stats_schema.clone()), None)
            .with_read_collations(true)
            .replay()?
            .aggregate_ungrouped(|aggs| {
                aggs.max_non_null_by(
                    column_name!("add"),
                    column_name!("add.path"),
                    column_name!(VERSION),
                )
            })?
            .filter(col!("add.path").is_not_null())?
            .build()?;
            let mut batches = engine
                .require_plan_executor()?
                .execute_op(Operation::QueryPlan(sample))?
                .into_data()?
                .map(|data| {
                    data.map(|data| ActionReconciliationBatch {
                        actions_count: data.len() as i64,
                        add_actions_count: data.len() as i64,
                        remove_actions_count: 0,
                        filtered_data: FilteredEngineData::with_all_rows_selected(data),
                    })
                });
            collation_stats::checkpoint_stats_schema(
                &self.snapshot,
                context.stats_schema.clone(),
                &mut batches,
            )?
            .0
        } else {
            context.stats_schema.clone()
        };
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
            .with_checkpoint_shape(shape)
            .with_stats_schemas(
                stats
                    .json_synthesis_schema(config)?
                    .or_else(|| write_struct.then(|| output_stats_schema.clone())),
                write_struct.then_some(output_stats_schema),
            )
            .with_read_collations(config.has_collations_feature())
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
        // DBR checkpoint replay expires transactions with missing lastUpdated when retention is
        // set.
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
