//! One execution writes a V1 checkpoint and returns complete hint inputs to Kernel.

use std::sync::LazyLock;

use super::{NUM_ACTIONS, NUM_ADDS, NUM_BYTES, STATISTICS_SCHEMA, checkpoint_schema_json};
use crate::checkpoint::{CheckpointWriter, LastCheckpointHintStats};
use crate::engine_data::TypedGetData as _;
use crate::expressions::{ColumnName, Expression as Expr, col, column_name, lit, null_lit};
use crate::plans::ir::nodes::{Agg, FILE_META_SCHEMA, FileType, StatsAgg, WriteParquet};
use crate::plans::{Plan, PlanBuilder, PlanResult};
use crate::schema::{DataType, SchemaRef, lazy_schema_ref};
use crate::{DeltaError, Engine, FileMeta, GetData, KernelError, KernelResult, Result, RowVisitor};

static RESULT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "statistics": (STATISTICS_SCHEMA.clone()),
    nullable "root": (FILE_META_SCHEMA.clone()),
    nullable "checkpoint_schema": STRING,
};

impl CheckpointWriter {
    /// Builds one plan that writes a classic Parquet V1 checkpoint and computes its hint inputs.
    ///
    /// Counts cover the rows written, and the completed file descriptor supplies the byte size.
    /// Execute once and pass the complete result to [`Self::last_checkpoint_bytes_from_result`],
    /// then atomically publish the returned `_last_checkpoint` bytes. V2 writers and non-Parquet
    /// formats are rejected before planning reads.
    pub fn declarative_checkpoint_plan(
        &self,
        engine: &dyn Engine,
        format: FileType,
    ) -> KernelResult<Plan> {
        if self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "This plan requires a V1 checkpoint writer",
            ));
        }
        if format != FileType::Parquet {
            return Err(KernelError::unsupported("V1 checkpoints require Parquet"));
        }
        let actions = self.checkpoint_actions(engine)?;
        let checkpoint_schema = checkpoint_schema_json(actions.schema())?;
        let write = WriteParquet::new(actions.schema().clone(), self.checkpoint_path()?)
            .with_stats_agg([
                StatsAgg::new(NUM_ACTIONS, Agg::count_star()),
                StatsAgg::new(NUM_ADDS, Agg::count(column_name!("add"))),
            ]);
        let written = PlanBuilder::write_parquet([actions], write)?;
        let statistics = written.clone().project(
            Expr::struct_from([col!(NUM_ACTIONS), col!(NUM_ADDS), col!("size")]),
            STATISTICS_SCHEMA.clone(),
        )?;
        let root = written.project(
            Expr::struct_from(
                FILE_META_SCHEMA
                    .fields()
                    .map(|field| Expr::column([field.name()])),
            ),
            FILE_META_SCHEMA.clone(),
        )?;
        PlanBuilder::union_all([
            result_rows(statistics, "statistics", None)?,
            result_rows(root, "root", checkpoint_schema)?,
        ])?
        .build()
    }

    /// Consumes a complete checkpoint-plan result and serializes the `_last_checkpoint` hint.
    ///
    /// Missing or duplicate statistics/root rows, invalid metadata, and late stream errors fail
    /// before bytes are returned. File writes have already completed; publication of these bytes
    /// is the caller's final step. A complete CRC at this snapshot's version validates the Add
    /// count; a mismatch returns `DELTA_TXN_LOG_FAILED_INTEGRITY` before hint bytes are generated.
    pub fn last_checkpoint_bytes_from_result(&self, result: PlanResult) -> Result<Vec<u8>> {
        let (_, _, bytes) = self.checkpoint_info_from_result(result)?;
        Ok(bytes)
    }

    pub(crate) fn checkpoint_info_from_result(
        &self,
        result: PlanResult,
    ) -> Result<(FileMeta, LastCheckpointHintStats, Vec<u8>)> {
        let mut rows = CheckpointResult::default();
        let batches = result.into_data()?.collect::<KernelResult<Vec<_>>>()?;
        for batch in batches {
            rows.visit_rows_of(batch.as_ref())?;
        }
        let statistics = rows
            .statistics
            .ok_or_else(|| KernelError::checkpoint_write("Missing checkpoint statistics"))?;
        if let Some(expected) = self.snapshot.get_file_stats_if_present()
            && expected.num_files() != statistics.num_of_add_files
        {
            return Err(DeltaError::txn_log_failed_integrity(
                self.snapshot.version(),
                &format!(
                    "Number of files - Expected: {} Computed: {}",
                    expected.num_files(),
                    statistics.num_of_add_files,
                ),
            )
            .into());
        }
        let root = rows
            .root
            .ok_or_else(|| KernelError::checkpoint_write("Missing checkpoint root"))?;
        let bytes = self.serialize_checkpoint_hint(&statistics, &root, rows.schema)?;
        Ok((root, statistics, bytes))
    }
}

fn result_rows(
    input: PlanBuilder,
    name: &str,
    schema: Option<String>,
) -> KernelResult<PlanBuilder> {
    let expression = Expr::struct_from(RESULT_SCHEMA.fields().map(|field| {
        if field.name() == name {
            Expr::struct_from(
                input
                    .schema()
                    .fields()
                    .map(|field| Expr::column([field.name()])),
            )
        } else if field.name() == "checkpoint_schema" {
            schema
                .clone()
                .map_or_else(|| null_lit(DataType::STRING), lit)
        } else {
            null_lit(field.data_type().clone())
        }
    }));
    input.project(expression, RESULT_SCHEMA.clone())
}

#[derive(Default)]
struct CheckpointResult {
    statistics: Option<LastCheckpointHintStats>,
    root: Option<FileMeta>,
    schema: Option<SchemaRef>,
}

impl RowVisitor for CheckpointResult {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 7]> = LazyLock::new(|| {
            [
                column_name!("statistics.num_actions"),
                column_name!("statistics.num_of_add_files"),
                column_name!("statistics.size_in_bytes"),
                column_name!("root.location"),
                column_name!("root.size"),
                column_name!("root.last_modified"),
                column_name!("checkpoint_schema"),
            ]
        });
        (
            &*NAMES,
            &[
                DataType::LONG,
                DataType::LONG,
                DataType::LONG,
                DataType::STRING,
                DataType::LONG,
                DataType::LONG,
                DataType::STRING,
            ],
        )
    }

    fn visit<'a>(&mut self, count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        for row in 0..count {
            let actions: Option<i64> = getters[0].get_opt(row, NUM_ACTIONS)?;
            let adds: Option<i64> = getters[1].get_opt(row, NUM_ADDS)?;
            let bytes: Option<i64> = getters[2].get_opt(row, NUM_BYTES)?;
            let location: Option<&str> = getters[3].get_opt(row, "location")?;
            let size: Option<i64> = getters[4].get_opt(row, "size")?;
            let modified: Option<i64> = getters[5].get_opt(row, "last_modified")?;
            let schema: Option<&str> = getters[6].get_opt(row, "checkpoint_schema")?;
            match (actions, adds, bytes, location, size, modified, schema) {
                (
                    Some(num_actions),
                    Some(num_of_add_files),
                    Some(size_in_bytes),
                    None,
                    None,
                    None,
                    None,
                ) => {
                    let stats = LastCheckpointHintStats {
                        num_actions,
                        num_of_add_files,
                        size_in_bytes,
                    };
                    if stats.num_actions < stats.num_of_add_files
                        || stats.num_of_add_files < 0
                        || stats.size_in_bytes < 0
                        || self.statistics.replace(stats).is_some()
                    {
                        return Err(KernelError::checkpoint_write(
                            "Invalid or duplicate checkpoint statistics",
                        ));
                    }
                }
                (None, None, None, Some(location), Some(size), Some(modified), schema) => {
                    let root = FileMeta::new(
                        url::Url::parse(location)?,
                        modified,
                        size.try_into()
                            .map_err(|_| KernelError::checkpoint_write("Negative root size"))?,
                    );
                    if self.root.replace(root).is_some() {
                        return Err(KernelError::checkpoint_write("Duplicate checkpoint root"));
                    }
                    self.schema = schema.map(serde_json::from_str).transpose()?;
                }
                _ => {
                    return Err(KernelError::checkpoint_write(
                        "Invalid checkpoint result row",
                    ));
                }
            }
        }
        Ok(())
    }
}
