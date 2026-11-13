//! Checkpoint reconstruction using shared metadata planning and action replay.

mod write_metadata;

use std::sync::Arc;

use super::CheckpointWriter;
use crate::action_reconciliation::RetentionCalculator as _;
use crate::action_reconciliation::plan::action_set::ActionSet;
use crate::action_reconciliation::plan::{MetadataPlanner, VERSION};
use crate::expressions::{Expression as Expr, Predicate, col, lit};
#[cfg(test)]
use crate::plans::RelationRef;
use crate::plans::{Plan, PlanBuilder};
use crate::scan::{PartitionValuesOptions, StatsOptions};
use crate::schema::StructType;
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::{Engine, KernelError, KernelResult};

impl CheckpointWriter {
    /// Builds the rows for a single-part V1 checkpoint.
    ///
    /// Shares source planning and typed metadata conversion with scans. File writing and
    /// publication remain with the caller. Collation-specific bounds stay in JSON stats;
    /// ordinary statistics also support structured output.
    pub fn declarative_checkpoint_data_plan(&self, engine: &dyn Engine) -> KernelResult<Plan> {
        self.checkpoint_actions(engine)?.build()
    }

    fn checkpoint_actions(&self, engine: &dyn Engine) -> KernelResult<PlanBuilder> {
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
            .project_patch(checkpoint_file_projection)
    }

    // Unit-test adapter for writing an already retained V1 reconstruction.
    #[cfg(test)]
    fn declarative_checkpoint_write_plan(&self, actions: RelationRef) -> KernelResult<Plan> {
        if self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "This plan requires a V1 checkpoint writer",
            ));
        }
        PlanBuilder::write_parquet(
            [PlanBuilder::relation_source(actions.clone())],
            actions.schema().clone(),
            self.checkpoint_path()?,
            false,
            false,
        )?
        .build()
    }

    /// Builds separate content and non-content reconstruction plans for a V2 checkpoint.
    /// Returns `(content, non_content)` for sidecar and manifest writes, respectively.
    ///
    /// The non-content plan reads the checkpoint root without resolving sidecars. Sidecar
    /// references, checkpointMetadata, and publication are supplied by the write orchestration.
    /// Collation-specific bounds stay in JSON stats, without sampling live Add actions.
    pub fn declarative_v2_checkpoint_data_plans(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<(Plan, Plan)> {
        let (content, non_content) = self.v2_checkpoint_actions(engine)?;
        Ok((content.build()?, non_content.build()?))
    }

    fn v2_checkpoint_actions(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<(PlanBuilder, PlanBuilder)> {
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
            .project_patch(checkpoint_file_projection)?;
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
        .project_patch(|patch| patch.drop(VERSION))?;
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
        // Versioned collation keys need dynamic field names. Keep them in JSON, while ordinary
        // structured stats use the writer's fixed schema without a planning-time replay.
        let output_stats_schema = context.stats_schema.clone();
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
        let read_collations = stats.synthesize_json && config.has_collations_feature();
        // Obsolete values may parse to null before replay, even for NOT NULL table columns.
        let partition_schema = context
            .partition_schema
            .as_ref()
            .filter(|_| write_struct)
            .map(|schema| {
                Arc::new(StructType::new_unchecked(schema.fields().cloned().map(
                    |mut field| {
                        field.nullable = true;
                        field
                    },
                )))
            });
        Ok(MetadataPlanner::new(self.snapshot.log_segment(), actions)
            .with_checkpoint_shape(shape)
            .with_permissive_partition_parsing()
            .with_stats_schemas(
                stats
                    .json_synthesis_schema(config)?
                    .or_else(|| write_struct.then(|| output_stats_schema.clone())),
                write_struct.then_some(output_stats_schema),
            )
            .with_read_collations(read_collations)
            .with_stats(stats)
            .with_partition_values(
                PartitionValuesOptions {
                    parsed_struct: write_struct,
                    ..Default::default()
                },
                partition_schema,
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
