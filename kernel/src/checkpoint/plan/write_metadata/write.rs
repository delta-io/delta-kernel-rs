//! One execution writes checkpoint files and returns only bounded hint inputs to Kernel.

use super::*;
use crate::plans::ir::nodes::{FILE_META_SCHEMA, FileType};
use crate::plans::{Plan, PlanResult};
use crate::{DeltaError, Engine, Result};

static RESULT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "statistics": (STATISTICS_SCHEMA.clone()),
    nullable "root": (FILE_META_SCHEMA.clone()),
    nullable "prefetch": (PREFETCH_SCHEMA.clone()),
    nullable "checkpoint_schema": STRING,
};

impl CheckpointWriter {
    /// Builds one plan that writes a checkpoint and computes all `_last_checkpoint` inputs.
    ///
    /// V1 requires Parquet. V2 writes Parquet sidecars and a JSON or Parquet UUID root. Counts
    /// share the rows used by the writes, and byte totals use completed file descriptors. All
    /// output depends on the successful root write. Execute once and pass the complete result to
    /// [`Self::last_checkpoint_bytes_from_result`], then atomically publish the returned bytes.
    /// Collation-specific bounds remain in JSON stats, without a separate sampling query.
    pub fn declarative_checkpoint_plan(
        &self,
        engine: &dyn Engine,
        format: FileType,
    ) -> KernelResult<Plan> {
        let (actions, counts, sidecars) = if self.schema_context.is_v2 {
            let (content, non_content) = self.v2_checkpoint_actions(engine)?;
            let counts = action_counts(content.clone(), true)?;
            let sidecars = PlanBuilder::write_parquet_files(
                [content.clone()],
                content.schema().clone(),
                self.snapshot.log_segment().log_root.join("_sidecars/")?,
                format!("{:020}.checkpoint.{{uuid}}.parquet", self.version),
            )?
            .project(
                Expr::struct_from([col!("file_name"), col!("size"), col!("last_modified")]),
                Arc::new(schema! {
                    not_null "path": STRING,
                    not_null "size": LONG,
                    not_null "file_modification": LONG,
                }),
            )?;
            let manifest = self.v2_manifest(
                non_content,
                sidecars.clone(),
                counts.clone(),
                content.schema(),
            )?;
            let counts = PlanBuilder::union_all([counts, action_counts(manifest.clone(), false)?])?;
            (manifest, counts, Some(sidecars))
        } else {
            if format != FileType::Parquet {
                return Err(KernelError::unsupported("V1 checkpoints require Parquet"));
            }
            let actions = self.checkpoint_actions(engine)?;
            let counts = action_counts(actions.clone(), true)?;
            (actions, counts, None)
        };
        let root_path = if self.schema_context.is_v2 {
            let extension = match format {
                FileType::Json => "json",
                FileType::Parquet => "parquet",
            };
            self.snapshot.log_segment().log_root.join(&format!(
                "{:020}.checkpoint.{}.{extension}",
                self.version,
                uuid::Uuid::new_v4(),
            ))?
        } else {
            self.checkpoint_path()?
        };
        let checkpoint_schema = if format == FileType::Parquet {
            checkpoint_schema_json(actions.schema())?
        } else {
            None
        };
        let root = match format {
            FileType::Json => PlanBuilder::write_json([actions.clone()], root_path, false, false),
            FileType::Parquet => PlanBuilder::write_parquet(
                [actions.clone()],
                actions.schema().clone(),
                root_path,
                false,
                false,
            ),
        }?;
        let totals = checkpoint_totals(counts, sidecars, Some(root.clone()))?;
        let mut results = vec![
            result_rows(totals, "statistics", None)?,
            result_rows(root.clone(), "root", checkpoint_schema)?,
        ];
        if self.schema_context.is_v2 {
            results.push(result_rows(
                Self::last_checkpoint_prefetch(actions)?,
                "prefetch",
                None,
            )?);
        }
        // EXISTS on the completed root prevents hint inputs escaping before every write succeeds.
        PlanBuilder::union_all(results)?
            .semi_join(root, [], [])?
            .build()
    }

    /// Consumes a complete checkpoint-plan result and serializes the `_last_checkpoint` hint.
    ///
    /// Missing or duplicate statistics/root rows, invalid metadata, and late stream errors fail
    /// before bytes are returned. File writes have already completed; publication of these bytes
    /// is the caller's final step. A complete CRC at this snapshot's version validates the Add
    /// count; a mismatch returns `DELTA_TXN_LOG_FAILED_INTEGRITY` before hint bytes are generated.
    pub fn last_checkpoint_bytes_from_result(&self, result: PlanResult) -> Result<Vec<u8>> {
        let mut rows = CheckpointResult {
            statistics: None,
            root: None,
            schema: None,
            prefetch: LastCheckpointV2 {
                path: String::new(),
                size_in_bytes: None,
                modification_time: None,
                sidecar_files: None,
                non_file_actions: None,
            },
        };
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
        Ok(self.serialize_checkpoint_hint(
            statistics,
            &rows
                .root
                .ok_or_else(|| KernelError::checkpoint_write("Missing checkpoint root"))?,
            rows.schema,
            rows.prefetch,
        )?)
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

struct CheckpointResult {
    statistics: Option<LastCheckpointHintStats>,
    root: Option<FileMeta>,
    schema: Option<SchemaRef>,
    prefetch: LastCheckpointV2,
}

impl RowVisitor for CheckpointResult {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 9]> = LazyLock::new(|| {
            [
                column_name!("statistics.num_actions"),
                column_name!("statistics.num_of_add_files"),
                column_name!("statistics.size_in_bytes"),
                column_name!("root.location"),
                column_name!("root.size"),
                column_name!("root.last_modified"),
                column_name!("prefetch.is_sidecar"),
                column_name!("prefetch.json"),
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
                DataType::BOOLEAN,
                DataType::STRING,
                DataType::STRING,
            ],
        )
    }

    fn visit<'a>(&mut self, count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        for row in 0..count {
            let actions: Option<i64> = getters[0].get_opt(row, NUM_ACTIONS)?;
            let location: Option<&str> = getters[3].get_opt(row, "location")?;
            let sidecar: Option<bool> = getters[6].get_opt(row, "is_sidecar")?;
            if usize::from(actions.is_some())
                + usize::from(location.is_some())
                + usize::from(sidecar.is_some())
                != 1
            {
                return Err(KernelError::checkpoint_write(
                    "Invalid checkpoint result row",
                ));
            }
            if let Some(num_actions) = actions {
                let stats = LastCheckpointHintStats {
                    num_actions,
                    num_of_add_files: getters[1].get(row, NUM_ADDS)?,
                    size_in_bytes: getters[2].get(row, NUM_BYTES)?,
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
            if let Some(location) = location {
                let size: i64 = getters[4].get(row, "size")?;
                let root = FileMeta::new(
                    url::Url::parse(location)?,
                    getters[5].get(row, "last_modified")?,
                    size.try_into()
                        .map_err(|_| KernelError::checkpoint_write("Negative root size"))?,
                );
                if self.root.replace(root).is_some() {
                    return Err(KernelError::checkpoint_write("Duplicate checkpoint root"));
                }
                let schema: Option<&str> = getters[8].get_opt(row, "checkpoint_schema")?;
                self.schema = schema.map(serde_json::from_str).transpose()?;
            }
            if let Some(sidecar) = sidecar {
                let json: Option<&str> = getters[7].get_opt(row, "json")?;
                if sidecar {
                    collect_prefetch(
                        &mut self.prefetch.sidecar_files,
                        json,
                        LAST_CHECKPOINT_SIDECARS_THRESHOLD,
                    )?;
                } else {
                    collect_prefetch(
                        &mut self.prefetch.non_file_actions,
                        json,
                        LAST_CHECKPOINT_NON_FILE_ACTIONS_THRESHOLD,
                    )?;
                }
            }
        }
        Ok(())
    }
}
