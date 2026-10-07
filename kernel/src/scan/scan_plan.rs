//! Declarative metadata scan plans.
//!
//! [`Scan::declarative_metadata_scan_plan`](super::Scan::declarative_metadata_scan_plan) reconciles
//! checkpoint and commit actions into live adds, applying metadata pruning before
//! newest-action-wins replay.

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

use url::Url;

use super::data_skipping::as_sql_data_skipping_predicate_with_stats_columns;
use super::state_info::StateInfo;
use super::{PhysicalPredicate, Scan};
use crate::actions::{
    get_all_actions_schema, ADD_NAME, ADD_SCHEMA, REMOVE_FIELD, REMOVE_NAME, SIDECAR_FIELD,
    SIDECAR_NAME, STATS_PARSED,
};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{
    col, column_name, joined_column_expr, lit, ColumnName, Expression as Expr, MapToStructOptions,
    Predicate, UnaryExpressionOp,
};
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::plans::ir::plan::Plan;
use crate::schema::{
    lazy_schema_ref, schema, schema_ref, DataType, SchemaRef, SchemaStructPatchBuilder,
    StructField, StructType,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::utils::FoldWithOption as _;
use crate::{KernelError, KernelResult, PlanBuilder};

// === Internal column names ===

// Both add and remove provide path + DV (storageType, pathOrInlineDv, offset) columns. We
// materialize them as one top-level `file_action_key` column that are used by the plan's
// aggregate and anti-join operators.
const FILE_ACTION_KEY: &str = "file_action_key";
const STATS: &str = "stats";
const PARTITION_VALUES: &str = "partitionValues";
const PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
const IS_ADD: &str = "is_add";
const VERSION: &str = "version";

/// This planner centralizes schema and projection decisions shared by commits and checkpoints.
/// For each source, it constructs a plan that:
///
/// 1. Determines the read schema. A file may contain structured stats, JSON stats, or both; the
///    planner selects the representation to read.
/// 2. Projects metadata before filtering, parsing JSON stats or deserializing partition values from
///    `map<string, string>` when needed for filtering or requested output.
/// 3. Applies the data skipping filter when enabled. JSON stats output disables metadata filtering,
///    not the data-row predicate.
/// 4. Projects metadata after filtering into the requested stats and partition shape.
pub(super) struct MetadataPlanner<'a> {
    scan: &'a Scan,
    pre_filter_stats_schema: Option<&'a SchemaRef>,
    parsed_partition_schema: Option<&'a SchemaRef>,
    stats_filter: Option<Predicate>,
}

/// The statistics representation present in a metadata plan.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StatsKind {
    None,
    Json,
    Struct,
}

impl<'a> MetadataPlanner<'a> {
    pub(super) fn try_new(scan: &'a Scan) -> KernelResult<Self> {
        let (pre_filter_stats_schema, stats_filter) = if scan.stats.synthesize_json {
            // JSON output disables metadata filtering, not the data-row predicate.
            (None, None)
        } else {
            (
                scan.state_info.physical_stats_read_schema(),
                stats_skipping_predicate(&scan.state_info),
            )
        };
        let parsed_partition_schema = (stats_filter.is_some()
            || scan.partition_values.parsed_struct)
            .then(|| scan.state_info.physical_partition_schema.as_ref())
            .flatten();

        Ok(Self {
            scan,
            pre_filter_stats_schema,
            parsed_partition_schema,
            stats_filter,
        })
    }

    /// Whether [`CheckpointShape`] should inspect the leaf-level checkpoint schema to choose the
    /// statistics representation and identify compatible structured partition values.
    pub(super) fn requires_checkpoint_add_schema(&self) -> bool {
        self.pre_filter_stats_schema.is_some()
            || self.parsed_partition_schema.is_some()
            || self.scan.stats.synthesize_json
    }

    fn statically_skips_all(&self) -> bool {
        !self.scan.stats.synthesize_json
            && self.scan.state_info.physical_predicate == PhysicalPredicate::StaticSkipAll
    }

    /// Plans a metadata source using its physical schema. `source` must honor the supplied read
    /// schema; `read_removes` keeps remove actions for replay.
    fn build_metadata_arm(
        &self,
        available_file_schema: &StructType,
        read_removes: bool,
        source: impl FnOnce(SchemaRef) -> KernelResult<PlanBuilder>,
    ) -> KernelResult<PlanBuilder> {
        let (read_schema, stats, has_struct_partitions) =
            self.read_schema(available_file_schema, read_removes)?;
        let read = source(read_schema)?;
        let (pre_stats_filter, stats) =
            self.project_pre_stats_filter(read, stats, has_struct_partitions)?;
        let post_stats_filter =
            pre_stats_filter.try_fold_with(self.stats_filter.as_ref(), |plan, predicate| {
                plan.filter(Predicate::or(col!(ADD_NAME).is_null(), predicate.clone()))
            })?;
        self.project_post_stats_filter(post_stats_filter, stats)
    }

    /// Selects one stats representation from the physical schema. Parsed fields follow their
    /// serialized counterparts in canonical Add field order.
    fn read_schema(
        &self,
        file_schema: &StructType,
        read_removes: bool,
    ) -> KernelResult<(SchemaRef, StatsKind, bool)> {
        let add = match file_schema.field_at(&column_name!(ADD_NAME))?.data_type() {
            DataType::Struct(add) => add,
            data_type => {
                return Err(KernelError::schema(format!(
                    "metadata source field '{ADD_NAME}' must be a struct, found {}",
                    data_type.kind_name(),
                )));
            }
        };
        let field_type = |name: &str| add.field(name).map(StructField::data_type);
        let patch = SchemaStructPatchBuilder::new();
        let (add_patch, stats) = match (self.pre_filter_stats_schema, field_type(STATS_PARSED)) {
            (Some(required), Some(DataType::Struct(native)))
                if LogSegment::structs_have_compatible_types(native, required, STATS_PARSED) =>
            {
                let patch = patch.drop(STATS).insert_after(
                    STATS,
                    StructField::nullable(STATS_PARSED, required.as_ref().clone()),
                );
                (patch, StatsKind::Struct)
            }
            // JSON synthesis needs the source's full stats, not the requested subset.
            (None, Some(DataType::Struct(native)))
                if self.scan.stats.synthesize_json && !add.contains(STATS) =>
            {
                let patch = patch.drop(STATS).insert_after(
                    STATS,
                    StructField::nullable(STATS_PARSED, native.as_ref().clone()),
                );

                (patch, StatsKind::Struct)
            }
            _ if self.pre_filter_stats_schema.is_some() || self.scan.stats.synthesize_json => {
                (patch, StatsKind::Json)
            }
            _ => (patch.drop(STATS), StatsKind::None),
        };
        let native_partitions = field_type(PARTITION_VALUES_PARSED);
        let read_partitions = match native_partitions {
            Some(DataType::Struct(native)) => self.parsed_partition_schema.filter(|required| {
                LogSegment::structs_have_compatible_types(native, required, PARTITION_VALUES_PARSED)
            }),
            _ => None,
        };
        let add_patch = add_patch.fold_with(read_partitions, |patch, schema| {
            patch.insert_after(
                PARTITION_VALUES,
                StructField::nullable(PARTITION_VALUES_PARSED, schema.as_ref().clone()),
            )
        });
        let read_schema = schema_ref! {
            nullable ADD_NAME: (add_patch.build(&ADD_SCHEMA)?),
            ..(read_removes.then_some(&REMOVE_FIELD)),
            nullable VERSION: LONG,
        };
        Ok((read_schema, stats, read_partitions.is_some()))
    }

    /// Adds structured metadata needed by the stats filter or requested output when the source
    /// could not provide compatible native fields.
    ///
    /// `add.stats` is replaced by `add.stats_parsed` with the pre-filter stats schema when parsing
    /// is needed. `add.partitionValues_parsed` is inserted when needed but unavailable natively;
    /// the string map remains. `remove?` and `version` pass through unchanged. Metadata filtering
    /// adds top-level `is_add`, including for partition-only filters.
    fn project_pre_stats_filter(
        &self,
        plan: PlanBuilder,
        mut stats: StatsKind,
        has_struct_partitions: bool,
    ) -> KernelResult<(PlanBuilder, StatsKind)> {
        let pre_filter_stats_schema = self
            .pre_filter_stats_schema
            .filter(|_| stats == StatsKind::Json);
        let parsed_partition_schema = self
            .parsed_partition_schema
            .filter(|_| !has_struct_partitions);
        let plan = plan.project_patch(|patch| {
            patch
                .fold_with(pre_filter_stats_schema, |patch, schema| {
                    stats = StatsKind::Struct;
                    patch.drop_at([ADD_NAME], STATS).insert_after_at(
                        [ADD_NAME],
                        STATS,
                        StructField::nullable(STATS_PARSED, schema.as_ref().clone()),
                        Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema)),
                    )
                })
                .fold_with(parsed_partition_schema, |patch, schema| {
                    patch.insert_after_at(
                        [ADD_NAME],
                        PARTITION_VALUES,
                        StructField::nullable(PARTITION_VALUES_PARSED, schema.as_ref().clone()),
                        Expr::map_to_struct(
                            col!(ADD_NAME, PARTITION_VALUES),
                            MapToStructOptions::default(),
                        ),
                    )
                })
                .fold_with(self.stats_filter.as_ref(), |patch, _| {
                    patch.append(
                        StructField::not_null(IS_ADD, DataType::BOOLEAN),
                        Expr::from(col!("add.path").is_not_null()),
                    )
                })
        })?;
        Ok((plan, stats))
    }

    /// Builds the output projection for requested stats and partition values. The base of this
    /// transformation is the source's working projection after parsed metadata has been added for
    /// pruning.
    ///
    /// The output schema is:
    /// ```text
    /// add: struct<
    ///   path: string,
    ///   partitionValues: map<string, string>,
    ///   partitionValues_parsed: struct<...>,   // when parsed partition values are requested
    ///   size: long,
    ///   modificationTime: long,
    ///   dataChange: boolean,
    ///   stats: string,                         // when JSON stats are requested
    ///   stats_parsed: struct<...>,             // when parsed stats are requested
    ///   tags: map<string, string>,
    ///   deletionVector: struct<...>,
    ///   baseRowId: long,
    ///   defaultRowCommitVersion: long,
    ///   clusteringProvider: string,
    ///   backReference: struct<...>,            // with adaptive metadata support
    /// >
    /// ```
    /// Stats output may contain neither representation, JSON only, parsed only, or both. Parsed
    /// partition values are selected independently and omitted for unpartitioned tables. Fields
    /// needed only for pruning and top-level `is_add` are omitted. `remove?` and `version` remain
    /// for replay.
    fn project_post_stats_filter(
        &self,
        plan: PlanBuilder,
        mut stats: StatsKind,
    ) -> KernelResult<PlanBuilder> {
        let output_stats_schema = self.scan.state_info.physical_stats_output_schema();

        let plan = if self.scan.stats.synthesize_json && stats == StatsKind::Struct {
            stats = StatsKind::Json;
            plan.project_patch(|patch| {
                patch
                    .insert_after_at(
                        [ADD_NAME],
                        "dataChange",
                        StructField::nullable(STATS, DataType::STRING),
                        Expr::unary(UnaryExpressionOp::ToJson, col!(ADD_NAME, STATS_PARSED)),
                    )
                    .drop_at([ADD_NAME], STATS_PARSED)
            })?
        } else {
            plan
        };

        plan.project_patch(|mut patch| {
            // TODO: Remove both-output compatibility once legacy connectors use structured stats.
            // Parse only at the output boundary, preserving full JSON even for a structured subset.
            if stats == StatsKind::Json {
                patch = patch.fold_with(output_stats_schema, |patch, schema| {
                    patch.insert_after_at(
                        [ADD_NAME],
                        STATS,
                        StructField::nullable(STATS_PARSED, schema.as_ref().clone()),
                        Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema)),
                    )
                });
            }
            if stats == StatsKind::Struct && output_stats_schema.is_none() {
                patch = patch.drop_at([ADD_NAME], STATS_PARSED);
            }
            if let (Some(read), Some(output)) = (self.pre_filter_stats_schema, output_stats_schema)
            {
                // Recursively drop no-longer-needed stats columns by comparing the output schema
                // with current stats parsed schema.
                let mut parent = vec![ADD_NAME.into(), STATS_PARSED.into()];
                patch = Self::project_filter_only_stats(patch, read, output, &mut parent);
            }

            if self.parsed_partition_schema.is_some() && !self.scan.partition_values.parsed_struct {
                patch = patch.drop_at([ADD_NAME], PARTITION_VALUES_PARSED);
            }

            patch.fold_with(self.stats_filter.as_ref(), |patch, _| patch.drop(IS_ADD))
        })
    }

    /// Projects away stats fields omitted from structured output beneath `parent`.
    /// Missing fields are dropped as whole subtrees; only retained structs are visited.
    ///
    /// Example, showing only `minValues` for nested table columns `a.{b,c}` and `d.{e,f}`:
    /// ```text
    /// read:    minValues { a: { b, c }, d: { e, f } }
    /// output:  minValues { a: { b } }
    /// drops:   add.stats_parsed.minValues.a.c, add.stats_parsed.minValues.d
    /// ```
    /// `a` is retained, so only `a.c` is dropped. `d` is absent, so its children are not visited.
    fn project_filter_only_stats<'s>(
        mut patch: ProjectionStructPatchBuilder<'s>,
        read: &StructType,
        output: &StructType,
        parent: &mut Vec<String>,
    ) -> ProjectionStructPatchBuilder<'s> {
        for field in read.fields() {
            let output = output.field(field.name()).map(StructField::data_type);
            match (field.data_type(), output) {
                (_, None) => patch = patch.drop_at(parent.as_slice(), field.name()),
                (DataType::Struct(read), Some(DataType::Struct(output))) => {
                    parent.push(field.name().to_owned());
                    patch = Self::project_filter_only_stats(patch, read, output, parent);
                    parent.pop();
                }
                _ => {}
            }
        }
        patch
    }
}

impl Scan {
    /// Build the live-add metadata plan from checkpoint and commit actions.
    ///
    /// Returns `None` for an empty result or a statically false predicate.
    #[tracing::instrument(
        name = "scan_plan.build_metadata_scan_plan",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(super) fn build_metadata_scan_plan_with(
        &self,
        shape: &CheckpointShape,
        metadata: &MetadataPlanner<'_>,
    ) -> KernelResult<Option<Plan>> {
        // A statically-unsatisfiable predicate (e.g. `x > 10 AND FALSE`) skips the whole table.
        if metadata.statically_skips_all() {
            return Ok(None);
        }

        let commit_actions = self.commit_arm(metadata)?;

        let deduped_commit = commit_actions.aggregate_by([column_name!(FILE_ACTION_KEY)], |a| {
            // Each group with a non-null FILE_ACTION_KEY contains the adds and removes for a given
            // file; winning adds pass through unchanged while winning removes produce NULL. Non-
            // file actions have NULL FILE_ACTION_KEY and map to their own NULL group.
            a.max_non_null_by(
                column_name!(ADD_NAME),
                column_name!(FILE_ACTION_KEY),
                column_name!(VERSION),
            )
        })?;

        let checkpoint_adds = self.checkpoint_arm(shape, metadata)?;

        let checkpoint_live_adds = checkpoint_adds
            .anti_join(
                deduped_commit.clone(),
                [column_name!(FILE_ACTION_KEY)],
                [column_name!(FILE_ACTION_KEY)],
            )?
            .project_patch(|patch| patch.drop(FILE_ACTION_KEY))?;

        let commit_live_adds = deduped_commit
            .filter(col!("add").is_not_null())?
            .project_patch(|patch| patch.drop(FILE_ACTION_KEY))?;

        PlanBuilder::union_all([commit_live_adds, checkpoint_live_adds])?.build_opt()
    }

    #[cfg(test)]
    fn build_metadata_scan_plan(&self, shape: &CheckpointShape) -> KernelResult<Option<Plan>> {
        self.build_metadata_scan_plan_with(shape, &MetadataPlanner::try_new(self)?)
    }

    /// Build checkpoint adds in the requested output shape. Returns an empty relation when no
    /// checkpoint exists.
    ///
    /// ## SQL equivalent:
    //
    /// SELECT PATCH_STRUCT(add, <needed parsed fields>, <unrequested field drops>) AS add,
    ///        file_key(add) AS key
    /// FROM checkpoint_actions
    /// WHERE add.path IS NOT NULL
    ///
    /// When the checkpoint lacks native parsed metadata, `FROM_JSON(add.stats, physical_stats)`
    /// and `MAP_TO_STRUCT(add.partitionValues, physical_partitions)` replace the corresponding
    /// fields above. A parsed field is omitted when its schema is absent.
    fn checkpoint_arm(
        &self,
        shape: &CheckpointShape,
        metadata: &MetadataPlanner<'_>,
    ) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let available_file_schema = shape
            .leaf_checkpoint_schema
            .as_deref()
            .unwrap_or(get_all_actions_schema());
        metadata
            .build_metadata_arm(available_file_schema, false, |schema| {
                let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;
                let actions = match (&shape.checkpoint_type, checkpoint) {
                    (CheckpointType::Leaf, Some((FileType::Parquet, parts))) => {
                        PlanBuilder::scan_parquet(parts, &[VERSION], schema)
                    }
                    (CheckpointType::Leaf, Some((FileType::Json, parts))) => {
                        PlanBuilder::scan_json(parts, &[VERSION], schema)
                    }
                    (CheckpointType::Manifest, Some((file_type, parts))) => {
                        match log_segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
                            Some(sidecars) => {
                                PlanBuilder::scan_parquet(sidecars, &[VERSION], schema)
                            }
                            // Without a complete hint, load the sidecars referenced by the
                            // manifest.
                            None => {
                                sidecar_actions(file_type, parts, schema, &log_segment.log_root)
                            }
                        }
                    }
                    (CheckpointType::None, _) | (_, None) => PlanBuilder::values(schema, vec![]),
                }?;
                actions.filter(col!("add.path").is_not_null())
            })?
            .project_patch(|patch| {
                patch
                    .append(
                        FILE_ACTION_KEY_FIELD.clone(),
                        file_action_key_expr(|col| joined_column_expr!("add", col)),
                    )
                    .drop(VERSION)
            })
    }

    /// Build commit JSON actions in the requested output shape.
    ///
    /// ## SQL equivalent:
    ///
    /// SELECT PATCH_STRUCT(add, <needed parsed fields>, <unrequested field drops>) AS add,
    ///        version, file_key(COALESCE(add, remove)) AS key
    /// FROM json_commits
    /// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
    ///
    /// A parsed field is omitted when its schema is absent.
    fn commit_arm(&self, metadata: &MetadataPlanner<'_>) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let commit_files = log_segment.commit_cover_version_tagged_scan_files()?;
        metadata
            .build_metadata_arm(get_all_actions_schema(), true, |schema| {
                PlanBuilder::scan_json(commit_files, &[VERSION], schema)?.filter(Predicate::or(
                    col!("add.path").is_not_null(),
                    col!("remove.path").is_not_null(),
                ))
            })?
            .project_patch(|patch| {
                patch
                    .append(
                        FILE_ACTION_KEY_FIELD.clone(),
                        file_action_key_expr(|col| {
                            Expr::coalesce([
                                joined_column_expr!("add", col),
                                joined_column_expr!("remove", col),
                            ])
                        }),
                    )
                    .drop(REMOVE_NAME)
            })
    }
}

/// Read actions from V2 checkpoint sidecars.
fn sidecar_actions(
    file_type: FileType,
    root_parts: Vec<ScanFile>,
    action_schema: SchemaRef,
    log_root: &Url,
) -> KernelResult<PlanBuilder> {
    const FILE_PATH: &str = "path";
    const FILE_SIZE: &str = "size";
    const FILE_MOD: &str = "filemod";
    const SIDECAR_SIZE: &str = "sizeInBytes";
    const SIDECAR_FILE_MOD: &str = "modificationTime";

    static SIDECAR_FILE_META_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
        not_null FILE_PATH: STRING,
        not_null FILE_SIZE: LONG,
        not_null FILE_MOD: LONG,
        nullable VERSION: LONG,
    };

    static SIDECAR_READ_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
        (&SIDECAR_FIELD),
        nullable VERSION: LONG,
    };

    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    let sidecar_files = scan(root_parts, &[VERSION], SIDECAR_READ_SCHEMA.clone())?
        .filter(col!(SIDECAR_NAME, FILE_PATH).is_not_null())?
        .project(
            Expr::struct_from([
                col!(SIDECAR_NAME, FILE_PATH),
                col!(SIDECAR_NAME, SIDECAR_SIZE),
                col!(SIDECAR_NAME, SIDECAR_FILE_MOD),
                col!(VERSION),
            ]),
            SIDECAR_FILE_META_SCHEMA.clone(),
        )?;

    let dynamic_scan = DynamicScan::try_new(
        &SIDECAR_FILE_META_SCHEMA,
        action_schema,
        FileType::Parquet,
        log_root.join("_sidecars/")?,
        [VERSION],
        column_name!(FILE_PATH),
        column_name!(FILE_SIZE),
        column_name!(FILE_MOD),
        None,
    )?;

    sidecar_files.dynamic_scan(dynamic_scan)
}

// === Helpers ===

/// Physical checkpoint schema used by plan-shape tests.
#[cfg(test)]
fn checkpoint_file_schema(
    physical_stats: Option<&SchemaRef>,
    physical_partitions: Option<&SchemaRef>,
) -> KernelResult<SchemaRef> {
    let add_patch = SchemaStructPatchBuilder::new()
        .fold_with(physical_stats, |patch, schema| {
            patch.append(StructField::nullable(STATS_PARSED, schema.as_ref().clone()))
        })
        .fold_with(physical_partitions, |patch, schema| {
            patch.append(StructField::nullable(
                PARTITION_VALUES_PARSED,
                schema.as_ref().clone(),
            ))
        });
    Ok(schema_ref! {
        nullable ADD_NAME: (add_patch.build(&ADD_SCHEMA)?),
        nullable VERSION: LONG,
    })
}

/// File identity used for replay.
static FILE_ACTION_KEY_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    let schema = schema! {
        nullable "path": STRING,
        nullable "deletionVector": {
            not_null "storageType": STRING,
            not_null "pathOrInlineDv": STRING,
            nullable "offset": INTEGER,
        },
    };
    StructField::nullable(FILE_ACTION_KEY, schema)
});

/// Build a file identity from path and deletion vector.
fn file_action_key_expr(key_col_expr: impl Fn(ColumnName) -> Expr) -> Expr {
    let storage_type = key_col_expr(column_name!("deletionVector.storageType"));
    Expr::struct_from([
        key_col_expr(column_name!("path")),
        Expr::struct_with_nullability_from(
            [
                storage_type.clone(),
                key_col_expr(column_name!("deletionVector.pathOrInlineDv")),
                key_col_expr(column_name!("deletionVector.offset")),
            ],
            Expr::from_pred(storage_type.is_not_null()),
        ),
    ])
}

/// Build the metadata pruning predicate, or `None` when no pruning is possible.
fn stats_skipping_predicate(state: &StateInfo) -> Option<Predicate> {
    /// Re-roots metadata columns under `add`.
    struct MetadataSkippingColumnPrefixer;

    impl<'a> ExpressionTransform<'a> for MetadataSkippingColumnPrefixer {
        transform_output_type!(|'a, T| Cow<'a, T>);

        fn transform_expr_column(&mut self, name: &'a ColumnName) -> Cow<'a, ColumnName> {
            match name.path().first().map(String::as_str) {
                Some(STATS_PARSED | PARTITION_VALUES_PARSED) => {
                    Cow::Owned(column_name!(ADD_NAME).join(name))
                }
                _ => Cow::Borrowed(name),
            }
        }
    }

    let PhysicalPredicate::Some(pred, _) = &state.physical_predicate else {
        return None;
    };
    let partition_column_names = state
        .physical_partition_schema
        .iter()
        .flat_map(|s| s.fields().map(|f| ColumnName::new([f.name()])))
        .collect();
    let skipping = as_sql_data_skipping_predicate_with_stats_columns(
        pred,
        &partition_column_names,
        &state.eligible_physical_stats_columns,
    )?;
    // A null skipping verdict means the available metadata cannot prove the file is skippable.
    let skipping = Predicate::distinct(skipping, lit(false));
    let mut prefixer = MetadataSkippingColumnPrefixer;
    Some(prefixer.transform_pred(&skipping).into_owned())
}

#[cfg(test)]
#[path = "scan_plan/tests.rs"]
mod execution_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
    use crate::arrow::array::{
        Array, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use crate::arrow::datatypes::{DataType as ADT, Field, Fields, Schema as ArrowSchema};
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::Operation as PlanOperation;
    use crate::scan::{build_stats_output_schemas, PartitionValuesOptions, StatsOptions};
    use crate::schema::StructType;
    use crate::snapshot::Snapshot;
    use crate::unit_test_utils::{
        create_log_path, MockProtocolBuilder, MockTableConfigurationBuilder,
    };
    use crate::{Engine as _, Result};

    fn mock_snapshot(log_segment: LogSegment) -> Result<Arc<Snapshot>> {
        let table_configuration = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema())
            .with_partition_columns(["p"])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .with_table_root("memory:///")
            .try_build()?;
        Ok(Arc::new(Snapshot::new(log_segment, table_configuration)?))
    }

    fn partitioned_schema() -> SchemaRef {
        schema_ref! {
            nullable "x": LONG,
            nullable "p": STRING,
        }
    }

    fn log_root() -> Url {
        Url::parse("file:///_delta_log/").unwrap()
    }

    fn log_segment(log_root: Url, commits: &[&str], checkpoint: Option<&str>) -> LogSegment {
        let ascending_commit_files: Vec<_> =
            commits.iter().map(|path| create_log_path(path)).collect();
        let checkpoint_parts: Vec<_> = checkpoint.into_iter().map(create_log_path).collect();
        let checkpoint_version = checkpoint_parts.first().map(|path| path.version);
        let latest_commit_file = ascending_commit_files.last().cloned();
        let end_version = latest_commit_file
            .as_ref()
            .map(|path| path.version)
            .or(checkpoint_version)
            .unwrap_or_default();
        LogSegment {
            end_version,
            checkpoint_version,
            log_root,
            listed: LogSegmentFiles {
                ascending_commit_files,
                checkpoint_parts,
                latest_commit_file,
                max_published_version: Some(end_version),
                ..Default::default()
            },
            last_checkpoint_metadata: None,
        }
    }

    fn checkpoint_path(file_type: FileType) -> &'static str {
        match file_type {
            FileType::Json => concat!(
                "file:///_delta_log/00000000000000000000.checkpoint.",
                "11111111-1111-1111-1111-111111111111.json"
            ),
            FileType::Parquet => "file:///_delta_log/00000000000000000000.checkpoint.parquet",
        }
    }

    fn shape(checkpoint_type: CheckpointType, parsed_stats: Option<SchemaRef>) -> CheckpointShape {
        let leaf_checkpoint_schema = parsed_stats
            .as_ref()
            .map(|stats| checkpoint_file_schema(Some(stats), None).unwrap());
        CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema,
        }
    }

    fn no_checkpoint() -> CheckpointShape {
        shape(CheckpointType::None, None)
    }

    fn tags(plan: &Plan) -> Vec<String> {
        plan.nodes.iter().map(|node| node.op.to_string()).collect()
    }

    fn add_struct(schema: &SchemaRef) -> &StructType {
        let DataType::Struct(add_struct) = schema
            .field(ADD_NAME)
            .expect("schema should contain add")
            .data_type()
        else {
            panic!("add should be a struct");
        };
        add_struct
    }

    // One add with JSON stats and no `stats_parsed`.
    fn write_parquet_checkpoint(store: &Arc<InMemory>, path: &str) -> Result<()> {
        // An empty (non-null) `partitionValues` map for the single row; the canonical add schema
        // requires the field, so the checkpoint file must physically carry it. The inner field
        // names must match kernel's map convention (`key_value` / `key` / `value`).
        let map_names = MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut map = MapBuilder::new(Some(map_names), StringBuilder::new(), StringBuilder::new());
        map.append(true).unwrap();
        let partition_values = map.finish();

        // The reader null-fills missing *nullable* add fields, but the canonical add schema's
        // non-null scalars (`path`, `size`, `modificationTime`, `dataChange`) and `partitionValues`
        // must be present in the file. `stats` carries the JSON string parsed in the
        // no-parsed-stats path; there is deliberately no `stats_parsed` column.
        let add_fields = Fields::from(vec![
            Field::new("path", ADT::Utf8, true),
            Field::new("stats", ADT::Utf8, true),
            Field::new(
                "partitionValues",
                partition_values.data_type().clone(),
                true,
            ),
            Field::new("size", ADT::Int64, true),
            Field::new("modificationTime", ADT::Int64, true),
            Field::new("dataChange", ADT::Boolean, true),
        ]);
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(ADD_NAME, ADT::Struct(add_fields.clone()), true),
            Field::new(VERSION, ADT::Int64, true),
        ]));
        let add = StructArray::new(
            add_fields,
            vec![
                Arc::new(StringArray::from(vec!["c.parquet"])),
                Arc::new(StringArray::from(vec![
                    r#"{"numRecords":1,"minValues":{"x":10},"maxValues":{"x":10}}"#,
                ])),
                Arc::new(partition_values),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(add), Arc::new(Int64Array::from(vec![0i64]))],
        )?;

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        futures::executor::block_on(store.put(&Path::from(path), buf.into()))?;
        Ok(())
    }

    // Commit-arm operator sequence without optional pruning or metadata transformations.
    const COMMIT_ARM_TAGS: &[&str] = &[
        "scan_json", // commits
        "filter",    // keep file actions
        "project",   // add replay columns
        "aggregate", // newest-action-per-key
        "filter",    // live commit adds
        "project",   // extract add
    ];

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "semi_join", "project"])]
    #[case::leaf_json(shape(CheckpointType::Leaf, None), FileType::Json,
        vec!["scan_json", "filter", "project", "semi_join", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project",
            "semi_join", "project"])]
    fn metadata_plan_checkpoint_arm_shape(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> Result<()> {
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            Some(checkpoint_path(file_type)),
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan.build_metadata_scan_plan(&shape)?.expect("non-empty");

        let mut expected: Vec<&str> = COMMIT_ARM_TAGS.to_vec();
        expected.extend(checkpoint_arm_tags);
        expected.push("union_all"); // terminal
        assert_eq!(tags(&plan), expected);
        Ok(())
    }

    #[rstest::rstest]
    #[case::without_parsed_partitions(None, false)]
    #[case::compatible_partitions(Some(schema_ref! { nullable "p": STRING }), true)]
    // The table's partition column p is STRING, so native LONG values are incompatible.
    #[case::incompatible_partitions(Some(schema_ref! { nullable "p": LONG }), false)]
    fn metadata_plan_checkpoint_metadata_columns(
        #[case] parsed_partitions: Option<SchemaRef>,
        #[case] expect_native_partitions: bool,
        #[values(false, true)] native_stats: bool,
        #[values(CheckpointType::Leaf, CheckpointType::Manifest)] checkpoint_type: CheckpointType,
        #[values(
            StatsOptions::all_struct(),
            StatsOptions::json_only(),
            StatsOptions::all()
        )]
        stats: StatsOptions,
        #[values(true, false)] source_json_stats: bool,
        #[values(1, 32)] width: usize,
    ) -> Result<()> {
        let expect_struct_stats = native_stats && (!stats.synthesize_json || !source_json_stats);
        let partition_values = PartitionValuesOptions::with_struct();
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let table_schema = StructType::new_unchecked(partitioned_schema().fields().cloned().chain(
            (1..width).map(|index| StructField::nullable(format!("c{index}"), DataType::LONG)),
        ));
        let config = MockTableConfigurationBuilder::new()
            .with_schema(table_schema)
            .with_partition_columns(["p"])
            .with_table_root("memory:///")
            .try_build()?;
        let parsed_stats = native_stats
            .then(|| build_stats_output_schemas(&config, &StatsOptions::all_struct()))
            .transpose()?
            .flatten()
            .map(|schemas| schemas.physical);
        let scan = Arc::new(Snapshot::new(segment, config)?)
            .scan_builder()
            .with_stats(stats)
            .with_partition_values(partition_values)
            .build()?;
        let file_schema =
            checkpoint_file_schema(parsed_stats.as_ref(), parsed_partitions.as_ref())?;
        let file_schema = SchemaStructPatchBuilder::new()
            .fold_with((!source_json_stats).then_some(()), |patch, ()| {
                patch.drop_at([ADD_NAME], STATS)
            })
            .build(&file_schema)?;
        let shape = CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema: Some(Arc::new(file_schema)),
        };
        let metadata = MetadataPlanner::try_new(&scan)?;
        let commit = scan.commit_arm(&metadata)?.build()?;
        let checkpoint = scan.checkpoint_arm(&shape, &metadata)?.build()?;
        let commit_add: ArrowSchema = add_struct(&commit.schema).try_into_arrow()?;
        let checkpoint_add: ArrowSchema = add_struct(&checkpoint.schema).try_into_arrow()?;
        assert_eq!(commit_add, checkpoint_add, "ordered add schemas must match");
        let plan = scan.build_metadata_scan_plan(&shape)?.expect("non-empty");

        let checkpoint_schema = plan
            .nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::ScanParquet(scan) if shape.checkpoint_type == CheckpointType::Leaf => {
                    Some(&scan.schema)
                }
                Operator::DynamicScan(scan) => {
                    assert!(scan.dv_column.is_none(), "sidecar scan sets no dv column");
                    Some(&scan.schema)
                }
                _ => None,
            })
            .expect("checkpoint leaf scan");
        assert_eq!(
            add_struct(checkpoint_schema).field(STATS_PARSED).is_some(),
            expect_struct_stats,
        );
        assert_eq!(
            add_struct(checkpoint_schema).field(STATS).is_some(),
            !expect_struct_stats,
        );
        assert_eq!(
            add_struct(checkpoint_schema)
                .field(PARTITION_VALUES_PARSED)
                .is_some(),
            expect_native_partitions,
        );

        let mut parses_partitions = false;
        for node in &plan.nodes {
            let Operator::Project(project) = &node.op else {
                continue;
            };
            let expression = project.expr.to_string();
            parses_partitions |= expression.contains("MAP_TO_STRUCT");
            assert!(!expression.contains("COALESCE"));
            if expect_struct_stats
                && !scan.stats.synthesize_json
                && project.schema.contains(ADD_NAME)
            {
                assert!(!expression.contains("PARSE_JSON"));
                assert!(!expression.contains(STATS_PARSED));
                let Expr::StructPatch(patch) = project.expr.as_ref() else {
                    panic!("metadata projections should use sparse struct patches");
                };
                if expect_native_partitions {
                    assert!(!patch.field_patches.contains_key(ADD_NAME));
                }
            }
        }
        assert_eq!(parses_partitions, !expect_native_partitions);
        Ok(())
    }

    #[test]
    fn metadata_plan_commits_only() -> Result<()> {
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            None,
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&no_checkpoint())?
            .expect("non-empty");
        assert_eq!(tags(&plan), COMMIT_ARM_TAGS.to_vec());
        Ok(())
    }

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project",
            "project"])]
    fn metadata_plan_checkpoint_only(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> Result<()> {
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(file_type)));
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan.build_metadata_scan_plan(&shape)?.expect("non-empty");
        assert_eq!(tags(&plan), checkpoint_arm_tags);
        Ok(())
    }

    #[test]
    fn metadata_plan_empty_is_none() -> Result<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        assert!(scan.build_metadata_scan_plan(&no_checkpoint())?.is_none());
        Ok(())
    }

    #[test]
    fn metadata_plan_static_skip_all_is_none() -> Result<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_predicate(Arc::new(Predicate::FALSE))
            .with_stats(StatsOptions::all_struct())
            .build()?;
        assert_eq!(
            scan.state_info.physical_predicate,
            PhysicalPredicate::StaticSkipAll
        );
        assert!(scan
            .build_metadata_scan_plan(&shape(CheckpointType::Leaf, None))?
            .is_none());
        Ok(())
    }

    #[test]
    fn metadata_plan_executes_commit_dedup_with_sync_executor() -> Result<()> {
        let store = Arc::new(InMemory::new());
        futures::executor::block_on(async {
            store
                .put(
                    &Path::from("_delta_log/00000000000000000000.json"),
                    r#"{"add":{"path":"a.parquet","size":1,"modificationTime":1,"dataChange":true,"partitionValues":{}}}
{"add":{"path":"b.parquet","size":1,"modificationTime":1,"dataChange":true,"partitionValues":{}}}
"#
                    .into(),
                )
                .await?;
            store
                .put(
                    &Path::from("_delta_log/00000000000000000001.json"),
                    r#"{"remove":{"path":"a.parquet","deletionTimestamp":2,"dataChange":true}}
"#
                    .into(),
                )
                .await?;
            Result::<()>::Ok(())
        })?;

        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[
                "memory:///_delta_log/00000000000000000000.json",
                "memory:///_delta_log/00000000000000000001.json",
            ],
            None,
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&no_checkpoint())?
            .expect("non-empty");

        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let batch = batches
            .next()
            .expect("one batch")?
            .try_into_record_batch()?;
        assert!(batches.next().is_none());
        assert_eq!(batch.num_rows(), 1);

        let add = batch
            .column_by_name(ADD_NAME)
            .expect("add column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add struct");
        let paths = add
            .column_by_name("path")
            .expect("add.path")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path string");
        assert_eq!(paths.value(0), "b.parquet");
        Ok(())
    }

    #[rstest::rstest]
    #[case::keeps_matching_file(StatsOptions::all_struct(), col!("x").gt(lit(5i64)), 1)]
    #[case::prunes_non_matching_file(StatsOptions::all_struct(), col!("x").gt(lit(20i64)), 0)]
    #[case::json_ignores_predicate(StatsOptions::json_only(), col!("x").gt(lit(20i64)), 1)]
    #[case::json_ignores_false(StatsOptions::json_only(), Predicate::FALSE, 1)]
    #[case::both_ignores_predicate(StatsOptions::all(), col!("x").gt(lit(20i64)), 1)]
    #[case::both_ignores_false(StatsOptions::all(), Predicate::FALSE, 1)]
    fn metadata_plan_executes_leaf_without_stats_parsed(
        #[case] stats: StatsOptions,
        #[case] predicate: Predicate,
        #[case] expected_rows: usize,
    ) -> Result<()> {
        let store = Arc::new(InMemory::new());
        // A single-row parquet checkpoint carrying an `add` with a JSON `stats` string but no
        // `stats_parsed` column.
        write_parquet_checkpoint(&store, "_delta_log/00000000000000000000.checkpoint.parquet")?;

        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[],
            Some("memory:///_delta_log/00000000000000000000.checkpoint.parquet"),
        );
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_predicate(Arc::new(predicate))
            .build()?;
        let plan = scan
            // Leaf with no compatible parsed stats -> parse add.stats instead.
            .build_metadata_scan_plan(&shape(CheckpointType::Leaf, None))?
            .expect("non-empty");

        let output_schema: ArrowSchema = plan.schema.as_ref().try_into_arrow()?;
        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let actual_rows = batches.try_fold(0, |rows, batch| {
            let batch = batch?.try_into_record_batch()?;
            assert_eq!(batch.schema().as_ref(), &output_schema);
            Ok::<_, KernelError>(rows + batch.num_rows())
        })?;
        assert_eq!(actual_rows, expected_rows);
        Ok(())
    }
}
