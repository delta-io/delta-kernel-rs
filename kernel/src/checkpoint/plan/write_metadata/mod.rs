//! Checkpoint manifests and complete `_last_checkpoint` metadata.
//!
//! The complete checkpoint plan shares reconstruction between counts and writes. Byte sizes come
//! from completed file descriptors; V2 prefetch is bounded in the engine. Kernel drains the result
//! and serializes the hint, including schema hints and DBR's JSON checksum, before publication.
//! AMT and legacy multipart generation are outside this planner's scope.

use std::sync::{Arc, LazyLock};

use serde::Serialize;
use serde::de::DeserializeOwned;

mod json_checksum;
mod write;

use super::super::{CheckpointWriter, LastCheckpointHintStats};
#[cfg(test)]
use crate::EngineData;
use crate::actions::{CHECKPOINT_METADATA_FIELD, SIDECAR_FIELD, SIDECAR_FILE_SCHEMA_TAG, Sidecar};
use crate::engine_data::TypedGetData as _;
use crate::expressions::{
    ColumnName, Expression as Expr, Scalar, UnaryExpressionOp, col, column_name, lit, null_lit,
};
use crate::last_checkpoint_hint::{
    LAST_CHECKPOINT_NON_FILE_ACTIONS_THRESHOLD, LAST_CHECKPOINT_SIDECARS_THRESHOLD,
    LastCheckpointHint, LastCheckpointV2,
};
use crate::path::{CheckpointInstance, ParsedLogPath};
use crate::plans::ir::nodes::Agg;
#[cfg(test)]
use crate::plans::{Plan, RelationRef};
use crate::schema::{
    DataType, SchemaRef, StructField, StructType, ToSchema as _, lazy_schema_ref, schema,
};
use crate::{FileMeta, GetData, KernelError, KernelResult, PlanBuilder, RowVisitor};

const NUM_ACTIONS: &str = "num_actions";
const NUM_ADDS: &str = "num_of_add_files";
const NUM_BYTES: &str = "size_in_bytes";
// Delta-Spark's checkpointSchema.writeThresholdLength default, measured in UTF-16 code units.
const CHECKPOINT_SCHEMA_THRESHOLD: usize = 20_000;

static PREFETCH_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null "is_sidecar": BOOLEAN,
    nullable "json": STRING,
};

static STATISTICS_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null NUM_ACTIONS: LONG,
    not_null NUM_ADDS: LONG,
    not_null NUM_BYTES: LONG,
};

impl CheckpointWriter {
    /// Counts actions and adds in retained V1 checkpoint or V2 sidecar input rows.
    /// Returns one row, including zero counts for empty input. Reuse the same retained input for
    /// writing so counting does not repeat reconstruction or depend on output file boundaries.
    #[cfg(test)]
    pub(super) fn declarative_checkpoint_counts_plan(actions: RelationRef) -> KernelResult<Plan> {
        action_counts(PlanBuilder::relation_source(actions), true)?.build()
    }

    // Combine completed sidecars, retained non-content actions, and fresh checkpoint metadata.
    fn v2_manifest(
        &self,
        non_content: PlanBuilder,
        sidecars: PlanBuilder,
        content_counts: PlanBuilder,
        content_schema: &SchemaRef,
    ) -> KernelResult<PlanBuilder> {
        if !self.schema_context.is_v2 {
            return Err(KernelError::unsupported(
                "A V2 manifest requires a V2 checkpoint writer",
            ));
        }
        let tags = [
            ("sidecarNumActions", NUM_ACTIONS),
            ("sidecarSizeInBytes", NUM_BYTES),
            ("numOfAddFiles", NUM_ADDS),
        ]
        .into_iter()
        .map(|(tag, column)| {
            (
                lit(tag),
                Expr::cast(Expr::column([column]), DataType::STRING),
            )
        })
        .chain(
            checkpoint_schema_json(content_schema)?
                .map(|schema| (lit(SIDECAR_FILE_SCHEMA_TAG), lit(schema))),
        );
        let metadata = checkpoint_totals(content_counts, Some(sidecars.clone()), None)?.project(
            Expr::struct_from([Expr::struct_from([lit(self.version), Expr::map(tags)])]),
            Arc::new(schema! { (&CHECKPOINT_METADATA_FIELD) }),
        )?;
        let schema = Arc::new(StructType::try_new(
            non_content
                .schema()
                .fields()
                .cloned()
                .chain([SIDECAR_FIELD.clone(), CHECKPOINT_METADATA_FIELD.clone()]),
        )?);
        let inputs = [non_content, sidecar_actions(sidecars)?, metadata]
            .into_iter()
            .map(|input| {
                // Disjoint action arms share a root schema; only the union needs typed null
                // columns.
                let expr = Expr::struct_from(schema.fields().map(|field| {
                    if input.schema().contains(field.name()) {
                        Expr::column([field.name()])
                    } else {
                        null_lit(field.data_type().clone())
                    }
                }));
                input.project(expr, schema.clone())
            })
            .collect::<KernelResult<Vec<_>>>()?;
        PlanBuilder::union_all(inputs)
    }

    // Bound each complete hint list independently before returning any action JSON to Kernel.
    fn last_checkpoint_prefetch(manifest: PlanBuilder) -> KernelResult<PlanBuilder> {
        let non_content = manifest.clone().filter(col!("sidecar.path").is_null())?;
        let schema = Arc::new(schema! { not_null "action": (non_content.schema().clone()) });
        let expr = Expr::struct_from([Expr::struct_from(
            non_content
                .schema()
                .fields()
                .map(|field| Expr::column([field.name()])),
        )]);
        let non_content = non_content.project(expr, schema)?;
        let sidecars = manifest.filter(col!("sidecar.path").is_not_null())?;
        PlanBuilder::union_all([
            bounded_prefetch(
                non_content,
                Expr::unary(UnaryExpressionOp::ToJson, col!("action")),
                false,
                LAST_CHECKPOINT_NON_FILE_ACTIONS_THRESHOLD,
            )?,
            bounded_prefetch(
                sidecars,
                Expr::unary(UnaryExpressionOp::ToJson, col!("sidecar")),
                true,
                LAST_CHECKPOINT_SIDECARS_THRESHOLD,
            )?,
        ])
    }

    /// Computes the three `_last_checkpoint` statistics from retained actions and write results.
    ///
    /// For V1, `checkpoint_actions` is the retained checkpoint input and both optional arguments
    /// are absent. For V2, it is the retained manifest from
    /// the V2 manifest builder; supply `content_counts` from
    /// [`Self::declarative_checkpoint_counts_plan`] and the completed sidecar descriptors,
    /// including an empty relation when there are no sidecars.
    ///
    /// `root_file` contains exactly one completed write result with `size` in bytes, including
    /// the output of [`Self::declarative_checkpoint_write_plan`]. Sidecars contain `path` relative
    /// to `_sidecars/`, `size`, and `file_modification`.
    /// Pass the single output row and root file metadata to [`Self::last_checkpoint_bytes`] only
    /// after all referenced files have been successfully written.
    #[cfg(test)]
    pub(super) fn declarative_last_checkpoint_statistics_plan(
        &self,
        checkpoint_actions: RelationRef,
        root_file: RelationRef,
        content_counts: Option<RelationRef>,
        sidecars: Option<RelationRef>,
    ) -> KernelResult<Plan> {
        if self.schema_context.is_v2 != content_counts.is_some()
            || content_counts.is_some() != sidecars.is_some()
        {
            return Err(KernelError::checkpoint_write(
                "V2 checkpoint statistics require both content counts and sidecar write results",
            ));
        }
        let mut counts = vec![action_counts(
            PlanBuilder::relation_source(checkpoint_actions),
            !self.schema_context.is_v2,
        )?];
        if let Some(content_counts) = content_counts {
            counts.push(PlanBuilder::relation_source(content_counts));
        }
        checkpoint_totals(
            PlanBuilder::union_all(counts)?,
            sidecars.map(PlanBuilder::relation_source),
            Some(PlanBuilder::relation_source(root_file)),
        )?
        .build()
    }

    /// Serializes `_last_checkpoint` JSON from one aggregate row and the completed root file.
    ///
    /// `statistics` must be the single row from
    /// [`Self::declarative_last_checkpoint_statistics_plan`]. `root_file` describes the
    /// successfully written single-part checkpoint or V2 manifest. `checkpoint_schema` is the
    /// actual root input schema. For a UUID V2 root, `prefetch` must contain every batch from
    /// the bounded prefetch plan. Pass an empty iterator for classic
    /// roots. Schema fields and prefetch lists use DBR's size/count limits. The JSON checksum
    /// covers all serialized fields. The returned bytes may be published only
    /// after all referenced checkpoint files have been successfully written.
    #[cfg(test)]
    pub(super) fn last_checkpoint_bytes(
        &self,
        statistics: &dyn EngineData,
        root_file: &FileMeta,
        checkpoint_schema: &SchemaRef,
        prefetch: impl IntoIterator<Item = KernelResult<Box<dyn EngineData>>>,
    ) -> KernelResult<Vec<u8>> {
        let stats = read_statistics(statistics)?;
        let mut v2 = LastCheckpointV2 {
            path: String::new(),
            size_in_bytes: None,
            modification_time: None,
            sidecar_files: None,
            non_file_actions: None,
        };
        for batch in prefetch {
            v2.visit_rows_of(batch?.as_ref())?;
        }
        self.serialize_checkpoint_hint(
            stats,
            root_file,
            checkpoint_schema_json(checkpoint_schema)?.map(|_| checkpoint_schema.clone()),
            v2,
        )
    }

    fn serialize_checkpoint_hint(
        &self,
        stats: LastCheckpointHintStats,
        root_file: &FileMeta,
        checkpoint_schema: Option<SchemaRef>,
        mut v2: LastCheckpointV2,
    ) -> KernelResult<Vec<u8>> {
        let root = ParsedLogPath::new(root_file.clone())
            .ok_or_else(|| KernelError::checkpoint_write("Invalid checkpoint root file"))?;
        let valid_format = match CheckpointInstance::of(&root) {
            Some(CheckpointInstance::Classic) => true,
            Some(CheckpointInstance::Uuid { .. }) => self.schema_context.is_v2,
            _ => false,
        };
        if !valid_format
            || root.version != self.snapshot.version()
            || root_file.location != self.snapshot.log_segment().log_root.join(&root.filename)?
        {
            return Err(KernelError::checkpoint_write(
                "Checkpoint root does not match this writer",
            ));
        }
        let root_size = i64::try_from(root_file.size)
            .map_err(|_| KernelError::checkpoint_write("Checkpoint root size exceeds i64"))?;
        if stats.size_in_bytes < root_size
            || (!self.schema_context.is_v2 && stats.size_in_bytes != root_size)
        {
            return Err(KernelError::checkpoint_write(
                "Checkpoint byte count does not match root file",
            ));
        }
        let uuid_root = matches!(
            CheckpointInstance::of(&root),
            Some(CheckpointInstance::Uuid { .. })
        );
        v2.path = root.filename.clone();
        v2.size_in_bytes = Some(root_size);
        v2.modification_time = Some(root_file.last_modified);
        let parquet = root.filename.ends_with(".parquet");
        let hint = LastCheckpointHint {
            version: self.snapshot.version(),
            size: stats.num_actions,
            parts: None,
            size_in_bytes: Some(stats.size_in_bytes),
            num_of_add_files: Some(stats.num_of_add_files),
            checkpoint_schema: parquet.then_some(checkpoint_schema).flatten(),
            checksum: None,
            tags: None,
            v2_checkpoint: uuid_root.then_some(v2),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_type: None,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            amt_checkpoint: None,
        };
        // DBR also prefetches the file status for a singular classic checkpoint.
        let mut file = CheckpointHintFile {
            hint,
            checkpoint_files: (!uuid_root).then(|| {
                [serde_json::json!({
                    "path": root.filename,
                    "length": root_size,
                    "isDir": false,
                    "modificationTime": root_file.last_modified,
                })]
            }),
        };
        file.hint.checksum = Some(json_checksum::json_checksum(&serde_json::to_value(&file)?));
        Ok(serde_json::to_vec(&file)?)
    }
}

// Keep the public, portable hint type unchanged while writing DBR's optional file-status hint.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointHintFile {
    #[serde(flatten)]
    hint: LastCheckpointHint,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_files: Option<[serde_json::Value; 1]>,
}

impl RowVisitor for LastCheckpointHintStats {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 3]> = LazyLock::new(|| {
            [
                column_name!(NUM_ACTIONS),
                column_name!(NUM_ADDS),
                column_name!(NUM_BYTES),
            ]
        });
        (&*NAMES, &[DataType::LONG, DataType::LONG, DataType::LONG])
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        if row_count != 1 {
            return Err(KernelError::checkpoint_write(
                "Checkpoint statistics must contain exactly one row",
            ));
        }
        self.num_actions = getters[0].get(0, NUM_ACTIONS)?;
        self.num_of_add_files = getters[1].get(0, NUM_ADDS)?;
        self.size_in_bytes = getters[2].get(0, NUM_BYTES)?;
        if self.num_actions < self.num_of_add_files
            || self.num_of_add_files < 0
            || self.size_in_bytes < 0
        {
            return Err(KernelError::checkpoint_write(
                "Invalid checkpoint statistics",
            ));
        }
        Ok(())
    }
}

fn action_counts(actions: PlanBuilder, includes_adds: bool) -> KernelResult<PlanBuilder> {
    let counts = actions.aggregate_ungrouped(|aggs| {
        let aggs = aggs.aggregate_as(Agg::count_star(), NUM_ACTIONS);
        if includes_adds {
            aggs.aggregate_as(Agg::count(column_name!("add")), NUM_ADDS)
        } else {
            aggs
        }
    })?;
    if includes_adds {
        Ok(counts)
    } else {
        counts.project_patch(|patch| {
            patch.append(StructField::not_null(NUM_ADDS, DataType::LONG), lit(0i64))
        })
    }
}

// Only count rows and file descriptors enter this union; full action state stays in the engine.
fn checkpoint_totals(
    counts: PlanBuilder,
    sidecars: Option<PlanBuilder>,
    root: Option<PlanBuilder>,
) -> KernelResult<PlanBuilder> {
    let mut inputs = vec![counts.project(
        Expr::struct_from([col!(NUM_ACTIONS), col!(NUM_ADDS), lit(0i64)]),
        STATISTICS_SCHEMA.clone(),
    )?];
    for files in [sidecars, root].into_iter().flatten() {
        inputs.push(files.project(
            Expr::struct_from([lit(0i64), lit(0i64), col!("size")]),
            STATISTICS_SCHEMA.clone(),
        )?);
    }
    PlanBuilder::union_all(inputs)?.aggregate_ungrouped(|aggs| {
        aggs.sum(column_name!(NUM_ACTIONS))
            .sum(column_name!(NUM_ADDS))
            .sum(column_name!(NUM_BYTES))
    })
}

#[cfg(test)]
fn read_statistics(data: &dyn EngineData) -> KernelResult<LastCheckpointHintStats> {
    let mut stats = LastCheckpointHintStats {
        num_actions: 0,
        num_of_add_files: 0,
        size_in_bytes: 0,
    };
    stats.visit_rows_of(data)?;
    Ok(stats)
}

fn sidecar_actions(sidecars: PlanBuilder) -> KernelResult<PlanBuilder> {
    sidecars.project(
        Expr::struct_from([Expr::struct_from([
            col!("path"),
            col!("size"),
            col!("file_modification"),
            null_lit(
                Sidecar::to_schema()
                    .field_at(&column_name!("tags"))?
                    .data_type()
                    .clone(),
            ),
        ])]),
        Arc::new(schema! { (&SIDECAR_FIELD) }),
    )
}

fn checkpoint_schema_json(schema: &SchemaRef) -> KernelResult<Option<String>> {
    let json = serde_json::to_string(schema)?;
    Ok((json.encode_utf16().count() <= CHECKPOINT_SCHEMA_THRESHOLD).then_some(json))
}

fn bounded_prefetch(
    input: PlanBuilder,
    json: Expr,
    is_sidecar: bool,
    threshold: usize,
) -> KernelResult<PlanBuilder> {
    let within_limit = input
        .clone()
        .aggregate_ungrouped(|aggs| aggs.aggregate_as(Agg::count_star(), "count"))?
        .filter(col!("count").le(lit(threshold as i64)))?;
    let marker = PlanBuilder::values(
        PREFETCH_SCHEMA.clone(),
        vec![vec![is_sidecar.into(), Scalar::null(DataType::STRING)]],
    )?;
    // Gate before ToJson so omitted lists do not serialize every action. The marker distinguishes
    // a complete empty list from an omitted list.
    let rows = input.semi_join(within_limit.clone(), [], [])?.project(
        Expr::struct_from([lit(is_sidecar), json]),
        PREFETCH_SCHEMA.clone(),
    )?;
    PlanBuilder::union_all([marker.semi_join(within_limit, [], [])?, rows])
}

impl RowVisitor for LastCheckpointV2 {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 2]> =
            LazyLock::new(|| [column_name!("is_sidecar"), column_name!("json")]);
        (&*NAMES, &[DataType::BOOLEAN, DataType::STRING])
    }

    fn visit<'a>(&mut self, rows: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        for row in 0..rows {
            let json: Option<&str> = getters[1].get_opt(row, "json")?;
            let is_sidecar: bool = getters[0].get(row, "is_sidecar")?;
            if is_sidecar {
                collect_prefetch(
                    &mut self.sidecar_files,
                    json,
                    LAST_CHECKPOINT_SIDECARS_THRESHOLD,
                )?;
            } else {
                collect_prefetch(
                    &mut self.non_file_actions,
                    json,
                    LAST_CHECKPOINT_NON_FILE_ACTIONS_THRESHOLD,
                )?;
            }
        }
        Ok(())
    }
}

fn collect_prefetch<T: DeserializeOwned>(
    list: &mut Option<Vec<T>>,
    json: Option<&str>,
    threshold: usize,
) -> KernelResult<()> {
    let list = list.get_or_insert_with(Vec::new);
    if let Some(json) = json {
        if list.len() == threshold {
            return Err(KernelError::checkpoint_write(
                "Hint prefetch exceeds its count limit",
            ));
        }
        let mut value: serde_json::Value = serde_json::from_str(json)?;
        // ToJson engines may preserve null action columns. A hint action has exactly one key.
        if let Some(object) = value.as_object_mut() {
            object.retain(|_, value| !value.is_null());
        }
        list.push(serde_json::from_value(value)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
