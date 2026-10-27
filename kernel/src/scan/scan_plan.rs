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
    get_all_actions_schema, ADD_FIELD, ADD_NAME, ADD_SCHEMA, REMOVE_FIELD, REMOVE_NAME,
    SIDECAR_FIELD, SIDECAR_NAME, STATS_PARSED,
};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{
    col, column_name, joined_column_expr, lit, ColumnName, Expression as Expr, Predicate,
    UnaryExpressionOp,
};
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::plans::ir::plan::Plan;
use crate::schema::{
    lazy_schema_ref, schema, schema_ref, CheckpointFieldKind, DataType, SchemaRef,
    SchemaStructPatchBuilder, StructField, StructType,
};
use crate::struct_patch::{project_struct_preserving_nulls, ProjectionStructPatchBuilder};
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
/// 3. Applies the data skipping filter when enabled. If the user requests JSON stats, this is
///    disabled.
/// 4. Projects metadata after filtering into the stats and partition shape requested by the user.
pub(super) struct MetadataPlanner<'a> {
    scan: &'a Scan,
    stats_schema: Option<SchemaRef>,
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
        let stats_schema = if scan.stats.synthesize_json {
            let config = scan.snapshot.table_configuration();
            let columns = config
                .physical_data_schema_without_partition_columns()
                .leaves(None);
            // JSON synthesis must retain stats outside the scan's requested and indexed columns.
            let builder = config
                .stats_schema_builder()
                .with_required_physical_columns(Some(columns.as_ref().0))
                .with_variant_min_max(scan.stats.variant_min_max);
            #[cfg(feature = "geo-type-in-dev")]
            let builder = builder.with_geometry_min_max(scan.stats.geometry_min_max);
            Some(builder.build()?)
        } else {
            scan.state_info.physical_stats_read_schema().cloned()
        };
        let enable_metadata_filter = !scan.stats.synthesize_json;
        let stats_filter = enable_metadata_filter
            .then(|| stats_skipping_predicate(&scan.state_info))
            .flatten();
        let need_partition_schema =
            stats_filter.is_some() || scan.partition_values_options.parsed_struct;
        let parsed_partition_schema = need_partition_schema
            .then(|| scan.state_info.physical_partition_schema.as_ref())
            .flatten();

        Ok(Self {
            scan,
            stats_schema,
            parsed_partition_schema,
            stats_filter,
        })
    }

    /// Whether [`CheckpointShape`] should inspect the leaf-level checkpoint schema to choose the
    /// statistics representation and identify compatible structured partition values.
    pub(super) fn requires_checkpoint_add_schema(&self) -> bool {
        self.stats_schema.is_some() || self.parsed_partition_schema.is_some()
    }

    /// Plans a metadata source using its physical schema. `source` must honor the supplied read
    /// schema; `read_removes` keeps remove actions for replay.
    fn build_metadata_scan(
        &self,
        available_file_schema: &StructType,
        read_removes: bool,
        source: impl FnOnce(SchemaRef) -> KernelResult<PlanBuilder>,
    ) -> KernelResult<PlanBuilder> {
        let (read_schema, current_stats_kind, has_struct_partitions) =
            self.read_schema(available_file_schema, read_removes)?;
        let plan = source(read_schema)?;
        let (plan, current_stats_kind) =
            self.prepare_metadata(plan, current_stats_kind, has_struct_partitions)?;
        let plan = plan.try_fold_with(self.stats_filter.as_ref(), |plan, predicate| {
            plan.filter(Predicate::or(col!(ADD_NAME).is_null(), predicate.clone()))
        })?;
        self.project_output(plan, current_stats_kind)
    }

    /// Selects one stats representation from the physical schema.
    ///
    /// # Returns
    ///
    /// `(read_schema, stats_kind, has_struct_partitions)`: the requested read schema, its stats
    /// representation, and whether the schema includes native `add.partitionValues_parsed`.
    fn read_schema(
        &self,
        file_schema: &StructType,
        read_removes: bool,
    ) -> KernelResult<(SchemaRef, StatsKind, bool)> {
        let add_field = file_schema.field(ADD_NAME).unwrap_or(&ADD_FIELD);
        let file_add_schema = match add_field.data_type() {
            DataType::Struct(add) => add,
            data_type => {
                return Err(KernelError::schema(format!(
                    "metadata source field '{ADD_NAME}' must be a struct, found {}",
                    data_type.kind_name(),
                )));
            }
        };
        let get_type_of = |name: &str| file_add_schema.field(name).map(StructField::data_type);

        // A schema patch to apply to `ADD_SCHEMA`
        let patch = SchemaStructPatchBuilder::new();

        let prefer_json_stats = self.scan.stats.synthesize_json && file_add_schema.contains(STATS);

        // Decide what kind of stats to read from the file by transforming `ADD_SCHEMA` based on the
        // `file_schema` and table-derived stats schema.
        let (patch, stats_kind) = match (self.stats_schema.as_ref(), get_type_of(STATS_PARSED)) {
            // Parsed statistics are requested and present in the file
            (Some(required_stats), Some(DataType::Struct(file_parsed_stats)))
                if !prefer_json_stats
                    && LogSegment::structs_have_compatible_types(
                        file_parsed_stats,
                        required_stats,
                        STATS_PARSED,
                        CheckpointFieldKind::Stats,
                    ) =>
            {
                let stats_parsed_field =
                    StructField::nullable(STATS_PARSED, required_stats.clone());
                let patch = patch.drop(STATS).insert_after(STATS, stats_parsed_field);
                (patch, StatsKind::Struct)
            }
            // Json stats are used either by the dataskipping filter or are requested by the user
            _ if self.stats_schema.is_some() => (patch, StatsKind::Json),
            // Neither the user nor the data skipping filter needs stats. Drop them.
            _ => (patch.drop(STATS), StatsKind::None),
        };

        // Decide what kind of partition values to read from the file by transforming `ADD_SCHEMA`
        // based on `file_schema` and `parsed_partition_schema`.
        let (add_patch, has_struct_partitions) = match (
            self.parsed_partition_schema,
            get_type_of(PARTITION_VALUES_PARSED),
        ) {
            (Some(required), Some(DataType::Struct(native)))
                if LogSegment::structs_have_compatible_types(
                    native,
                    required,
                    PARTITION_VALUES_PARSED,
                    CheckpointFieldKind::PartitionValues,
                ) =>
            {
                let field = StructField::nullable(PARTITION_VALUES_PARSED, required.clone());
                (patch.insert_after(PARTITION_VALUES, field), true)
            }
            _ => (patch, false),
        };

        let read_schema = schema_ref! {
            nullable ADD_NAME: (add_patch.build(&ADD_SCHEMA)?),
            ..(read_removes.then_some(&REMOVE_FIELD)),
            nullable VERSION: LONG,
        };
        Ok((read_schema, stats_kind, has_struct_partitions))
    }

    /// Adds structured metadata needed by the stats filter or requested output when the source
    /// could not provide compatible native fields.
    ///
    /// * `add.stats` is replaced by `add.stats_parsed` with `stats_schema` when parsing is needed.
    /// * `add.partitionValues_parsed` is inserted when parsed partition values is not already
    ///   present; the string map remains.
    /// * Dataskipping adds top-level `is_add`
    fn prepare_metadata(
        &self,
        plan: PlanBuilder,
        mut stats: StatsKind,
        has_struct_partitions: bool,
    ) -> KernelResult<(PlanBuilder, StatsKind)> {
        let should_parse_stats = stats == StatsKind::Json && !self.scan.stats.synthesize_json;
        let stats_schema_to_parse = self.stats_schema.as_ref().filter(|_| should_parse_stats);
        let partition_schema_to_parse = self
            .parsed_partition_schema
            .filter(|_| !has_struct_partitions);
        let plan = plan.project_patch(|patch| {
            patch
                // Parses structured stats from json
                .fold_with(stats_schema_to_parse, |patch, schema| {
                    stats = StatsKind::Struct;
                    let field = StructField::nullable(STATS_PARSED, schema.clone());
                    let expr = Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema));
                    patch
                        .drop_at([ADD_NAME], STATS)
                        .insert_after_at([ADD_NAME], STATS, field, expr)
                })
                // Parses partition values from map<string, string>
                .fold_with(partition_schema_to_parse, |patch, schema| {
                    let field = StructField::nullable(PARTITION_VALUES_PARSED, schema.clone());
                    let expr =
                        Expr::map_to_struct(col!(ADD_NAME, PARTITION_VALUES), Default::default());
                    patch.insert_after_at([ADD_NAME], PARTITION_VALUES, field, expr)
                })
                // Adds the `IS_ADD`` column if dataskipping will be applied
                .fold_with(self.stats_filter.as_ref(), |patch, _| {
                    let field = StructField::not_null(IS_ADD, DataType::BOOLEAN);
                    patch.append(field, Expr::from(col!("add.path").is_not_null()))
                })
        })?;
        Ok((plan, stats))
    }

    /// Projects the requested stats and partition representations after pruning. Stats may contain
    /// JSON only, parsed only, both, or neither; parsed partitions are independent and omitted for
    /// unpartitioned tables. Removes filter-only fields and `is_add`, retaining `remove` and
    /// `version` for replay.
    fn project_output(&self, plan: PlanBuilder, stats: StatsKind) -> KernelResult<PlanBuilder> {
        let output_stats_schema = self.scan.state_info.physical_stats_output_schema();

        plan.project_patch(|patch| {
            let mut patch = match stats {
                StatsKind::None => patch,
                StatsKind::Json => patch.fold_with(output_stats_schema, |patch, schema| {
                    let field = StructField::nullable(STATS_PARSED, schema.clone());
                    let expr = Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema));
                    patch.insert_after_at([ADD_NAME], STATS, field, expr)
                }),
                StatsKind::Struct if self.scan.stats.synthesize_json => {
                    // Since the output requires JSON stats, but the current stats are
                    // structured, we serialize the structured stats to JSON string.
                    let stats_field = StructField::nullable(STATS, DataType::STRING);
                    let stats_expr =
                        Expr::unary(UnaryExpressionOp::ToJson, col!(ADD_NAME, STATS_PARSED));
                    let patch = patch.drop_at([ADD_NAME], STATS_PARSED);
                    let patch =
                        patch.insert_after_at([ADD_NAME], STATS_PARSED, stats_field, stats_expr);

                    // If the output requires structured stats (in addition to json stats), then
                    // project structured stats down to just the requested stats columns
                    // TODO(#3535): Remove support for outputing both structured and json stats.
                    patch.fold_with(output_stats_schema, |patch, schema| {
                        let field = StructField::nullable(STATS_PARSED, schema.clone());
                        let expr = if self.stats_schema.as_ref() == Some(schema) {
                            col!(ADD_NAME, STATS_PARSED)
                        } else {
                            project_struct_preserving_nulls([ADD_NAME, STATS_PARSED], schema)
                        };
                        patch.insert_after_at([ADD_NAME], STATS_PARSED, field, expr)
                    })
                }
                StatsKind::Struct => match (self.stats_schema.as_ref(), output_stats_schema) {
                    (_, None) => patch.drop_at([ADD_NAME], STATS_PARSED),
                    (Some(read), Some(output)) => {
                        let mut parent = vec![ADD_NAME.into(), STATS_PARSED.into()];
                        Self::project_filter_only_stats(patch, read, output, &mut parent)
                    }
                    _ => patch,
                },
            };

            // If parsed partitions is present but was not requested, drop it.
            if self.parsed_partition_schema.is_some()
                && !self.scan.partition_values_options.parsed_struct
            {
                patch = patch.drop_at([ADD_NAME], PARTITION_VALUES_PARSED);
            }

            // drop the `IS_ADD` field that is only used in dataskipping filtering
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
    /// Returns `None` for an empty result.
    #[tracing::instrument(
        name = "scan_plan.build_metadata_scan_plan",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(super) fn build_metadata_scan_plan(
        &self,
        shape: &CheckpointShape,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<Option<Plan>> {
        let commit_actions = self.commit_arm(planner)?;

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

        let checkpoint_adds = self.checkpoint_arm(shape, planner)?;

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

    /// Build checkpoint adds in the requested output shape. Returns an empty relation when no
    /// checkpoint exists.
    ///
    /// ## SQL equivalent:
    ///
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
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let available_file_schema = shape
            .leaf_checkpoint_schema
            .as_deref()
            .unwrap_or(get_all_actions_schema());
        planner
            .build_metadata_scan(available_file_schema, false, |schema| {
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
    fn commit_arm(&self, planner: &MetadataPlanner<'_>) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let commit_files = log_segment.commit_cover_version_tagged_scan_files()?;
        planner
            .build_metadata_scan(get_all_actions_schema(), true, |schema| {
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
    use ::test_utils::{add_commit, record_batch_to_bytes};

    use super::execution_tests::{declarative_metadata, metadata_row_count};
    use super::*;
    use crate::arrow::array::{Array, AsArray as _, StringArray};
    use crate::arrow::datatypes::Schema as ArrowSchema;
    use crate::engine::arrow_conversion::scalar::extract_primitive_scalar;
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::arrow_utils::parse_json_impl;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::Scalar;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::Operation as PlanOperation;
    use crate::scan::{build_stats_output_schemas, PartitionValuesOptions, StatsOptions};
    use crate::schema::StructType;
    use crate::snapshot::Snapshot;
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{
        create_log_path, MockProtocolBuilder, MockTableConfigurationBuilder,
    };
    use crate::{Engine as _, KernelResult, Version};

    fn mock_snapshot(log_segment: LogSegment) -> KernelResult<Arc<Snapshot>> {
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

    fn log_segment(commits: &[Version], checkpoint: Option<FileType>) -> LogSegment {
        let log_root = Url::parse("memory:///_delta_log/").unwrap();
        let log_path = |name: &str| create_log_path(log_root.join(name).unwrap().as_str());
        let ascending_commit_files: Vec<_> = commits
            .iter()
            .map(|version| log_path(&format!("{version:020}.json")))
            .collect();
        let checkpoint_parts: Vec<_> = checkpoint
            .into_iter()
            .map(checkpoint_path)
            .map(log_path)
            .collect();
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
                "00000000000000000000.checkpoint.",
                "11111111-1111-1111-1111-111111111111.json"
            ),
            FileType::Parquet => "00000000000000000000.checkpoint.parquet",
        }
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

    fn write_parquet_checkpoint(
        store: &InMemory,
        schema: SchemaRef,
        json: &[&str],
    ) -> KernelResult<()> {
        let batch = parse_json_impl(&StringArray::from(json.to_vec()), schema)?;
        let path = Path::from(format!("_delta_log/{}", checkpoint_path(FileType::Parquet)));
        futures::executor::block_on(store.put(&path, record_batch_to_bytes(&batch).into()))?;
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
    #[case::no_checkpoint(CheckpointType::None, FileType::Parquet, &[])]
    #[case::leaf_parquet(CheckpointType::Leaf, FileType::Parquet,
        &["scan_parquet", "filter", "project"])]
    #[case::leaf_json(CheckpointType::Leaf, FileType::Json,
        &["scan_json", "filter", "project"])]
    #[case::manifest(CheckpointType::Manifest, FileType::Parquet,
        &["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project"])]
    fn metadata_plan_operator_shape(
        #[case] checkpoint_type: CheckpointType,
        #[case] file_type: FileType,
        #[case] checkpoint_tags: &[&str],
        #[values(false, true)] newer_commit: bool,
    ) -> KernelResult<()> {
        let has_checkpoint = checkpoint_type != CheckpointType::None;
        let commits = if newer_commit { &[1][..] } else { &[] };
        let segment = log_segment(commits, has_checkpoint.then_some(file_type));
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let shape = CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema: None,
        };
        let plan = scan.build_metadata_scan_plan(&shape, &MetadataPlanner::try_new(&scan)?)?;
        let mut expected = vec![];
        if newer_commit {
            expected.extend_from_slice(COMMIT_ARM_TAGS);
        }
        expected.extend_from_slice(checkpoint_tags);
        match (has_checkpoint, newer_commit) {
            (true, true) => expected.extend(["semi_join", "project", "union_all"]),
            (true, false) => expected.push("project"),
            _ => {}
        }
        assert_eq!(plan.is_none(), expected.is_empty());
        if let Some(plan) = plan {
            let actual: Vec<_> = plan.nodes.iter().map(|node| node.op.to_string()).collect();
            assert_eq!(actual, expected);
        }
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
    ) -> KernelResult<()> {
        let expect_struct_stats = native_stats && (!stats.synthesize_json || !source_json_stats);
        let expect_stats_passthrough = expect_struct_stats && !stats.synthesize_json;
        let segment = log_segment(&[], Some(FileType::Parquet));
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
            .with_partition_values(PartitionValuesOptions::with_struct())
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
        let planner = MetadataPlanner::try_new(&scan)?;
        let commit = scan.commit_arm(&planner)?.build()?;
        let checkpoint = scan.checkpoint_arm(&shape, &planner)?.build()?;
        let commit_add: ArrowSchema = add_struct(&commit.schema).try_into_arrow()?;
        let checkpoint_add: ArrowSchema = add_struct(&checkpoint.schema).try_into_arrow()?;
        assert_eq!(commit_add, checkpoint_add, "ordered add schemas must match");
        let plan = scan
            .build_metadata_scan_plan(&shape, &planner)?
            .expect("non-empty");

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
        for (column, expected) in [
            (STATS_PARSED, expect_struct_stats),
            (STATS, !expect_struct_stats),
            (PARTITION_VALUES_PARSED, expect_native_partitions),
        ] {
            assert_eq!(
                add_struct(checkpoint_schema).contains(column),
                expected,
                "{column}"
            );
        }

        let mut parses_partitions = false;
        for node in &plan.nodes {
            let Operator::Project(project) = &node.op else {
                continue;
            };
            let expression = project.expr.to_string();
            parses_partitions |= expression.contains("MAP_TO_STRUCT");
            assert!(!expression.contains("COALESCE"));
            if expression.contains("TO_JSON") {
                assert!(!expression.contains("PARSE_JSON"));
            }
            if expect_stats_passthrough && project.schema.contains(ADD_NAME) {
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
    fn metadata_plan_static_skip_all_is_none() -> KernelResult<()> {
        let segment = log_segment(&[], None);
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
            .declarative_metadata_scan_plan(&SyncEngine::new_with_store(Arc::new(InMemory::new())))?
            .is_none());
        Ok(())
    }

    #[test]
    fn metadata_plan_executes_commit_dedup_with_sync_executor() -> KernelResult<()> {
        let store = Arc::new(InMemory::new());
        futures::executor::block_on(async {
            add_commit(
                "memory:///",
                store.as_ref(),
                0,
                r#"{"add":{"path":"a.parquet","size":1,"modificationTime":1,"dataChange":true,"partitionValues":{}}}
{"add":{"path":"b.parquet","size":1,"modificationTime":1,"dataChange":true,"partitionValues":{}}}
"#
                .into(),
            )
            .await?;
            add_commit(
                "memory:///",
                store.as_ref(),
                1,
                r#"{"remove":{"path":"a.parquet","deletionTimestamp":2,"dataChange":true}}
"#
                .into(),
            )
            .await
        }).unwrap();

        let scan = mock_snapshot(log_segment(&[0, 1], None))?
            .scan_builder()
            .build()?;
        let engine = SyncEngine::new_with_store(store);
        let plan = scan
            .declarative_metadata_scan_plan(&engine)?
            .expect("non-empty");
        let mut batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let batch = batches.next().expect("one batch")?;
        assert!(batches.next().is_none());
        let batch = batch.try_into_record_batch()?;
        assert_eq!(batch.num_rows(), 1);
        let add = batch
            .column_by_name(ADD_NAME)
            .expect("add column")
            .as_struct();
        let paths = add
            .column_by_name("path")
            .expect("add.path")
            .as_string::<i32>();
        assert_eq!(paths.value(0), "b.parquet");
        Ok(())
    }

    #[rstest::rstest]
    #[case::checkpoint_only(&[], &[])]
    #[case::with_newer_commit(&[1], &["later.parquet"])]
    fn metadata_plan_checkpoint_without_add(
        #[case] commits: &[Version],
        #[case] expected: &[&str],
    ) -> KernelResult<()> {
        let store = Arc::new(InMemory::new());
        let schema = SchemaStructPatchBuilder::new()
            .drop(ADD_NAME)
            .build(get_all_actions_schema())?;
        write_parquet_checkpoint(&store, Arc::new(schema), &[])?;
        if !commits.is_empty() {
            futures::executor::block_on(add_commit(
                "memory:///",
                store.as_ref(),
                1,
                concat!(
                    r#"{"add":{"path":"later.parquet","size":1,"modificationTime":1,"#,
                    r#""dataChange":true,"partitionValues":{"p":"p"}}}"#,
                )
                .into(),
            ))
            .unwrap();
        }
        let segment = log_segment(commits, Some(FileType::Parquet));
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let engine = SyncEngine::new_with_store(store);
        assert!(scan.declarative_metadata_scan_plan(&engine)?.is_some());
        let batches = declarative_metadata(&scan, &engine)?;
        let mut paths = Vec::new();
        for batch in batches {
            let column = batch.column_by_name("path").unwrap().as_string::<i32>();
            paths.extend(column.iter().map(|path| path.unwrap().to_owned()));
        }
        assert_eq!(paths, expected);
        Ok(())
    }

    #[rstest::rstest]
    #[case::date(Scalar::Date(1), DataType::INTEGER, "1970-01-02")]
    #[case::timestamp(Scalar::Timestamp(1), DataType::LONG, "1970-01-01T00:00:00.000Z")]
    #[case::timestamp_ntz(Scalar::TimestampNtz(1), DataType::LONG, "1970-01-01T00:00:00.000")]
    fn metadata_plan_synthesizes_json_with_table_stats_types(
        #[case] expected_native: Scalar,
        #[case] native_type: DataType,
        #[case] expected: &str,
        #[values(StatsOptions::json_only(), StatsOptions::all(), StatsOptions {
            synthesize_json: true,
            ..StatsOptions::all_struct_with_extra_indexed(vec![column_name!("other")])
        })]
        stats: StatsOptions,
    ) -> KernelResult<()> {
        let logical_type = expected_native.data_type();
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema_ref! {
                nullable "nested": { nullable "value": (logical_type.clone()) },
                nullable "other": LONG,
            })
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::TimestampWithoutTimezone])
                    .build(),
            )
            .with_properties([("delta.dataSkippingNumIndexedCols", "1")])
            .with_table_root("memory:///")
            .try_build()?;
        let bounds = schema! {
            nullable "nested": { nullable "value": (native_type) },
            nullable "other": LONG,
        };
        let file_schema = schema_ref! {
            nullable ADD_NAME: {
                ..(ADD_SCHEMA.fields().filter(|field| field.name() != STATS)),
                nullable STATS_PARSED: {
                    nullable "numRecords": LONG,
                    nullable "minValues": (bounds.clone()),
                    nullable "maxValues": (bounds),
                    nullable "nullCount": {
                        nullable "nested": { nullable "value": LONG },
                        nullable "other": LONG,
                    },
                },
            },
        };
        let store = Arc::new(InMemory::new());
        write_parquet_checkpoint(
            &store,
            file_schema,
            &[concat!(
                r#"{"add":{"path":"a.parquet","partitionValues":{},"size":1,"modificationTime":1,"#,
                r#""dataChange":true,"stats_parsed":{"numRecords":1,"#,
                r#""minValues":{"nested":{"value":1},"other":2},"#,
                r#""maxValues":{"nested":{"value":1},"other":2},"#,
                r#""nullCount":{"nested":{"value":0},"other":0}}}}"#,
            )],
        )?;
        let engine = SyncEngine::new_with_store(store);
        let segment = log_segment(&[], Some(FileType::Parquet));
        let scan = Arc::new(Snapshot::new(segment, config)?)
            .scan_builder()
            .with_stats(stats)
            .build()?;
        let batches = declarative_metadata(&scan, &engine)?;
        assert_eq!(metadata_row_count(&batches), 1);
        for batch in batches {
            let json = batch.column_by_name(STATS).unwrap().as_string::<i32>();
            let json: serde_json::Value = serde_json::from_str(json.value(0))?;
            for bound in ["minValues", "maxValues"] {
                assert_eq!(json[bound]["nested"]["value"], expected);
                assert_eq!(json[bound]["other"], 2);
                if let Some(parsed) = batch.column_by_name(STATS_PARSED) {
                    let parsed = parsed
                        .as_struct()
                        .column_by_name(bound)
                        .unwrap()
                        .as_struct();
                    let value = parsed
                        .column_by_name("nested")
                        .unwrap()
                        .as_struct()
                        .column_by_name("value")
                        .unwrap();
                    assert_eq!(value.data_type(), &(&logical_type).try_into_arrow()?);
                    assert_eq!(value.null_count(), 0);
                    assert_eq!(
                        extract_primitive_scalar(value.as_ref(), 0)?,
                        expected_native
                    );
                }
            }
            assert_eq!(json["nullCount"]["nested"]["value"], 0);
            assert_eq!(
                batch.column_by_name(STATS_PARSED).is_some(),
                scan.state_info.physical_stats_output_schema().is_some(),
            );
        }
        Ok(())
    }

    #[rstest::rstest]
    fn metadata_planner_falls_back_to_json_for_incompatible_stats(
        #[values(StatsOptions::json_only(), StatsOptions::all_struct())] stats: StatsOptions,
    ) -> KernelResult<()> {
        let scan = mock_snapshot(log_segment(&[], None))?
            .scan_builder()
            .with_stats(stats)
            .build()?;
        let file_schema = schema! {
            nullable ADD_NAME: {
                nullable STATS_PARSED: { nullable "minValues": { nullable "x": STRING } },
            },
        };
        let (read_schema, kind, _) =
            MetadataPlanner::try_new(&scan)?.read_schema(&file_schema, false)?;
        assert!(kind == StatsKind::Json);
        assert!(add_struct(&read_schema).contains(STATS));
        assert!(!add_struct(&read_schema).contains(STATS_PARSED));
        Ok(())
    }

    #[rstest::rstest]
    #[case::keeps_matching_file(StatsOptions::all_struct(), col!("x").gt(lit(5i64)), 1)]
    #[case::prunes_non_matching_file(StatsOptions::all_struct(), col!("x").gt(lit(20i64)), 0)]
    #[case::json_ignores_predicate(StatsOptions::json_only(), col!("x").gt(lit(20i64)), 1)]
    #[case::json_skips_false(StatsOptions::json_only(), Predicate::FALSE, 0)]
    #[case::both_ignores_predicate(StatsOptions::all(), col!("x").gt(lit(20i64)), 1)]
    #[case::both_skips_false(StatsOptions::all(), Predicate::FALSE, 0)]
    fn metadata_plan_executes_leaf_without_stats_parsed(
        #[case] stats: StatsOptions,
        #[case] predicate: Predicate,
        #[case] expected_rows: usize,
    ) -> KernelResult<()> {
        let store = Arc::new(InMemory::new());
        // Omit nullable add fields to exercise null-filling, and omit native parsed stats.
        let schema = schema_ref! {
            nullable ADD_NAME: {
                nullable "path": STRING,
                nullable STATS: STRING,
                nullable PARTITION_VALUES: { STRING => nullable STRING },
                nullable "size": LONG,
                nullable "modificationTime": LONG,
                nullable "dataChange": BOOLEAN,
            },
            nullable VERSION: LONG,
        };
        write_parquet_checkpoint(
            &store,
            schema,
            &[concat!(
                r#"{"add":{"path":"c.parquet","stats":"{\"numRecords\":1,"#,
                r#"\"minValues\":{\"x\":10},\"maxValues\":{\"x\":10}}","partitionValues":{},"#,
                r#""size":1,"modificationTime":1,"dataChange":true},"version":0}"#,
            )],
        )?;

        let segment = log_segment(&[], Some(FileType::Parquet));
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_predicate(Arc::new(predicate))
            .build()?;
        let engine = SyncEngine::new_with_store(store);
        let batches = declarative_metadata(&scan, &engine)?;
        assert_eq!(metadata_row_count(&batches), expected_rows);
        Ok(())
    }
}
