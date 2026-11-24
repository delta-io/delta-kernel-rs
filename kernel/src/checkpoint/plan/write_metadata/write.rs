//! One execution writes checkpoint files and returns only bounded hint inputs to Kernel.

use url::Url;

use super::*;
use crate::checkpoint::{CheckpointNaming, V2CheckpointConfig};
use crate::plans::ir::nodes::{Agg, FILE_META_SCHEMA, StatsAgg, WriteParquet};
use crate::plans::{Plan, PlanResult};
use crate::{DeltaError, Engine, Result};

static ROOT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    ..(FILE_META_SCHEMA.fields()),
    not_null "checkpoint_schema": STRING,
};

static RESULT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "statistics": (STATISTICS_SCHEMA.clone()),
    nullable "root": (ROOT_SCHEMA.clone()),
    nullable "prefetch": (PREFETCH_SCHEMA.clone()),
};

impl CheckpointWriter {
    /// Chooses root naming for declarative checkpoint plans.
    ///
    /// Classic Parquet is the default. UUID roots require V2.
    pub fn with_checkpoint_naming(mut self, naming: CheckpointNaming) -> Self {
        self.checkpoint_naming = naming;
        self
    }

    /// Selects inline file actions or sidecars for declarative V2 writes.
    ///
    /// Inline file actions are the default. Sidecars require a V2 table. The executor chooses
    /// sidecar grouping; a nonzero file-action hint does not constrain those groups.
    pub fn with_v2_checkpoint_config(mut self, config: V2CheckpointConfig) -> Self {
        self.v2_checkpoint_config = config;
        self
    }

    /// Builds one plan that writes a checkpoint and computes all `_last_checkpoint` inputs.
    ///
    /// V1 and V2 default to a single classic-named Parquet file. V2 supports sidecars through
    /// [`Self::with_v2_checkpoint_config`] and UUID-named Parquet roots through
    /// [`Self::with_checkpoint_naming`]. Counts share the rows used by the writes, and byte totals
    /// use completed file descriptors. All output depends on the
    /// successful root write. Execute once and pass the complete result to
    /// [`Self::last_checkpoint_bytes_from_result`], then atomically publish the returned bytes.
    /// Collation-specific bounds remain in JSON stats, without a separate sampling query.
    pub fn declarative_checkpoint_plan(&self, engine: &dyn Engine) -> KernelResult<Plan> {
        let uuid_root = self.checkpoint_naming == CheckpointNaming::Uuid;
        if uuid_root && !self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "UUID-named checkpoints require V2",
            ));
        }
        let with_sidecars = matches!(
            &self.v2_checkpoint_config,
            V2CheckpointConfig::WithSidecar { .. }
        );
        if with_sidecars && !self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "Sidecar checkpoint plans require the v2Checkpoint feature",
            ));
        }
        if matches!(
            &self.v2_checkpoint_config,
            V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: Some(0)
            }
        ) {
            return Err(KernelError::checkpoint_write(
                "file_actions_per_sidecar_hint must be greater than 0",
            ));
        }
        let root_path = if uuid_root {
            self.snapshot.log_segment().log_root.join(&format!(
                "{:020}.checkpoint.{}.parquet",
                self.version,
                uuid::Uuid::new_v4(),
            ))?
        } else {
            self.checkpoint_path()?
        };
        if !with_sidecars {
            return self.single_part_checkpoint_plan(engine, root_path);
        }
        let (actions, counts, sidecars) = {
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
        };
        let checkpoint_schema = serde_json::to_string(actions.schema())?;
        let root = PlanBuilder::write_parquet(
            [actions.clone()],
            WriteParquet::new(actions.schema().clone(), root_path),
        )?;
        let totals = checkpoint_totals(counts, sidecars, Some(root.clone()))?;
        let statistics = totals.project(
            Expr::struct_from([
                Expr::struct_from([col!(NUM_ACTIONS), col!(NUM_ADDS), col!(NUM_BYTES)]),
                null_lit(ROOT_SCHEMA.clone()),
                null_lit(PREFETCH_SCHEMA.clone()),
            ]),
            RESULT_SCHEMA.clone(),
        )?;
        let root_result = root.clone().project(
            Expr::struct_from([
                null_lit(STATISTICS_SCHEMA.clone()),
                Expr::struct_from([
                    col!("location"),
                    col!("size"),
                    col!("last_modified"),
                    lit(checkpoint_schema),
                ]),
                null_lit(PREFETCH_SCHEMA.clone()),
            ]),
            RESULT_SCHEMA.clone(),
        )?;
        let mut results = vec![statistics, root_result];
        if uuid_root {
            results.push(self.last_checkpoint_prefetch(actions)?.project(
                Expr::struct_from([
                    null_lit(STATISTICS_SCHEMA.clone()),
                    null_lit(ROOT_SCHEMA.clone()),
                    Expr::struct_from([col!("is_sidecar"), col!("json")]),
                ]),
                RESULT_SCHEMA.clone(),
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
        let (_, _, bytes) = self.checkpoint_info_from_result(result)?;
        Ok(bytes)
    }

    pub(crate) fn checkpoint_info_from_result(
        &self,
        result: PlanResult,
    ) -> Result<(FileMeta, LastCheckpointHintStats, Vec<u8>)> {
        let mut rows = CheckpointResult {
            statistics: None,
            root: None,
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
        let (root, schema) = rows
            .root
            .ok_or_else(|| KernelError::checkpoint_write("Missing checkpoint root"))?;
        let bytes = self.serialize_checkpoint_hint(&statistics, &root, schema, rows.prefetch)?;
        Ok((root, statistics, bytes))
    }

    fn single_part_checkpoint_plan(
        &self,
        engine: &dyn Engine,
        root_path: Url,
    ) -> KernelResult<Plan> {
        let actions = self.checkpoint_actions(engine)?;
        let checkpoint_schema = serde_json::to_string(actions.schema())?;
        let write = WriteParquet::new(actions.schema().clone(), root_path).with_stats_agg([
            StatsAgg::new(NUM_ACTIONS, Agg::count_star()),
            StatsAgg::new(NUM_ADDS, Agg::count(column_name!("add"))),
        ]);
        let written = PlanBuilder::write_parquet([actions.clone()], write)?;
        let statistics = written.clone().project(
            Expr::struct_from([
                Expr::struct_from([col!(NUM_ACTIONS), col!(NUM_ADDS), col!("size")]),
                null_lit(ROOT_SCHEMA.clone()),
                null_lit(PREFETCH_SCHEMA.clone()),
            ]),
            RESULT_SCHEMA.clone(),
        )?;
        let root = written.project(
            Expr::struct_from([
                null_lit(STATISTICS_SCHEMA.clone()),
                Expr::struct_from([
                    col!("location"),
                    col!("size"),
                    col!("last_modified"),
                    lit(checkpoint_schema),
                ]),
                null_lit(PREFETCH_SCHEMA.clone()),
            ]),
            RESULT_SCHEMA.clone(),
        )?;
        let mut results = vec![statistics, root.clone()];
        if self.checkpoint_naming == CheckpointNaming::Uuid {
            results.push(self.last_checkpoint_prefetch(actions)?.project(
                Expr::struct_from([
                    null_lit(STATISTICS_SCHEMA.clone()),
                    null_lit(ROOT_SCHEMA.clone()),
                    Expr::struct_from([col!("is_sidecar"), col!("json")]),
                ]),
                RESULT_SCHEMA.clone(),
            )?);
            return PlanBuilder::union_all(results)?
                .semi_join(root, [], [])?
                .build();
        }
        PlanBuilder::union_all(results)?.build()
    }
}

struct CheckpointResult {
    statistics: Option<LastCheckpointHintStats>,
    root: Option<(FileMeta, SchemaRef)>,
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
                column_name!("root.checkpoint_schema"),
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
            let adds: Option<i64> = getters[1].get_opt(row, NUM_ADDS)?;
            let bytes: Option<i64> = getters[2].get_opt(row, NUM_BYTES)?;
            let location: Option<&str> = getters[3].get_opt(row, "location")?;
            let size: Option<i64> = getters[4].get_opt(row, "size")?;
            let modified: Option<i64> = getters[5].get_opt(row, "last_modified")?;
            let sidecar: Option<bool> = getters[6].get_opt(row, "is_sidecar")?;
            let json: Option<&str> = getters[7].get_opt(row, "json")?;
            let schema: Option<&str> = getters[8].get_opt(row, "checkpoint_schema")?;
            match (
                actions, adds, bytes, location, size, modified, sidecar, json, schema,
            ) {
                (
                    Some(num_actions),
                    Some(num_of_add_files),
                    Some(size_in_bytes),
                    None,
                    None,
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
                (
                    None,
                    None,
                    None,
                    Some(location),
                    Some(size),
                    Some(modified),
                    None,
                    None,
                    Some(schema),
                ) => {
                    let root = FileMeta::new(
                        url::Url::parse(location)?,
                        modified,
                        size.try_into()
                            .map_err(|_| KernelError::checkpoint_write("Negative root size"))?,
                    );
                    let schema = serde_json::from_str(schema)?;
                    if self.root.replace((root, schema)).is_some() {
                        return Err(KernelError::checkpoint_write("Duplicate checkpoint root"));
                    }
                }
                (None, None, None, None, None, None, Some(sidecar), json, None) => {
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
