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
    get_all_actions_schema, Add, ADD_FIELD, ADD_NAME, ADD_SCHEMA, LOG_ADD_SCHEMA, REMOVE_FIELD,
    REMOVE_NAME, SIDECAR_FIELD, SIDECAR_NAME, STATS_PARSED,
};
use crate::checkpoint::collation_stats::source_collation_stats;
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
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::utils::FoldWithOption as _;
use crate::{KernelError, KernelResult, PlanBuilder, Version};

mod metadata_replay_base;

pub(super) use metadata_replay_base::MetadataReplayBase;

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
/// 3. Applies data skipping unless the user requests JSON stats without structured output.
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
    Both,
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
        let enable_metadata_filter =
            !scan.stats.synthesize_json || scan.state_info.physical_stats_output_schema().is_some();
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

        // Decide what kind of stats to read from the file by transforming `ADD_SCHEMA` based on the
        // `file_schema` and table-derived stats schema.
        let prefer_json_stats = self.scan.stats.synthesize_json && file_add_schema.contains(STATS);
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
                let stats_schema = required_stats.clone().try_fold_with(
                    source_collation_stats(file_schema)
                        .filter(|_| self.scan.needs_collation_stats_schema()),
                    |stats, field| {
                        SchemaStructPatchBuilder::new()
                            .append(field.clone())
                            .build(&stats)
                            .map(Arc::new)
                    },
                )?;
                let stats_parsed_field = StructField::nullable(STATS_PARSED, stats_schema);
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
    /// * JSON stats are parsed with `stats_schema`, retaining JSON when requested alongside them.
    /// * `add.partitionValues_parsed` is inserted when parsed partition values is not already
    ///   present; the string map remains.
    /// * Dataskipping adds top-level `is_add`
    fn prepare_metadata(
        &self,
        plan: PlanBuilder,
        mut stats: StatsKind,
        has_struct_partitions: bool,
    ) -> KernelResult<(PlanBuilder, StatsKind)> {
        let should_parse_stats = stats == StatsKind::Json
            && (!self.scan.stats.synthesize_json || self.stats_filter.is_some());
        let stats_schema_to_parse = self.stats_schema.as_ref().filter(|_| should_parse_stats);
        let partition_schema_to_parse = self
            .parsed_partition_schema
            .filter(|_| !has_struct_partitions);
        let plan = plan.project_patch(|patch| {
            patch
                .fold_with(stats_schema_to_parse, |patch, schema| {
                    let field = StructField::nullable(STATS_PARSED, schema.clone());
                    let expr = Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema));
                    let patch = if self.scan.stats.synthesize_json {
                        stats = StatsKind::Both;
                        patch
                    } else {
                        stats = StatsKind::Struct;
                        patch.drop_at([ADD_NAME], STATS)
                    };
                    patch.insert_after_at([ADD_NAME], STATS, field, expr)
                })
                .fold_with(partition_schema_to_parse, |patch, schema| {
                    let field = StructField::nullable(PARTITION_VALUES_PARSED, schema.clone());
                    let expr =
                        Expr::map_to_struct(col!(ADD_NAME, PARTITION_VALUES), Default::default());
                    patch.insert_after_at([ADD_NAME], PARTITION_VALUES, field, expr)
                })
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

        plan.project_patch(|mut patch| {
            let read_stats = patch
                .input_schema()
                .field_at(&column_name!(ADD_NAME, STATS_PARSED))
                .ok()
                .map(StructField::data_type);
            let mut patch = match stats {
                StatsKind::None => patch,
                StatsKind::Json => patch.fold_with(output_stats_schema, |patch, schema| {
                    let field = StructField::nullable(STATS_PARSED, schema.clone());
                    let expr = Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema));
                    patch.insert_after_at([ADD_NAME], STATS, field, expr)
                }),
                StatsKind::Struct | StatsKind::Both => {
                    if stats == StatsKind::Struct && self.scan.stats.synthesize_json {
                        let field = StructField::nullable(STATS, DataType::STRING);
                        let expr =
                            Expr::unary(UnaryExpressionOp::ToJson, col!(ADD_NAME, STATS_PARSED));
                        // Match ADD_SCHEMA's field order without moving native structured stats.
                        patch = patch.insert_after_at([ADD_NAME], "dataChange", field, expr)
                    }
                    // TODO(#3535): Remove support for outputting both structured and JSON stats.
                    match (read_stats, output_stats_schema) {
                        (_, None) => patch.drop_at([ADD_NAME], STATS_PARSED),
                        (Some(DataType::Struct(read)), Some(output))
                            if read.as_ref() != output.as_ref() =>
                        {
                            let mut parent = vec![ADD_NAME.into(), STATS_PARSED.into()];
                            Self::project_filter_only_stats(patch, read, output, &mut parent)
                        }
                        _ => patch,
                    }
                }
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
    pub(super) fn build_metadata_scan_plan_with(
        &self,
        base: MetadataReplayBase,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<Option<Plan>> {
        let state = &self.state_info;
        if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
            return Ok(None);
        }

        let commit_actions = self.commit_arm(base.version(), planner)?;

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

        let base_adds = match base {
            MetadataReplayBase::Crc { files, .. } => self.crc_arm(files, planner),
            MetadataReplayBase::Checkpoint { shape, .. } => self.checkpoint_arm(&shape, planner),
        }?;

        let base_live_adds = base_adds
            .anti_join(
                deduped_commit.clone(),
                [column_name!(FILE_ACTION_KEY)],
                [column_name!(FILE_ACTION_KEY)],
            )?
            .project_patch(|patch| patch.drop(FILE_ACTION_KEY))?;

        let commit_live_adds = deduped_commit
            .filter(col!("add").is_not_null())?
            .project_patch(|patch| patch.drop(FILE_ACTION_KEY))?;

        PlanBuilder::union_all([commit_live_adds, base_live_adds])?.build_opt()
    }

    #[cfg(test)]
    fn build_metadata_scan_plan(
        &self,
        shape: &CheckpointShape,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<Option<Plan>> {
        let base = MetadataReplayBase::Checkpoint {
            version: self.snapshot.log_segment().checkpoint_version,
            shape: shape.clone(),
        };
        self.build_metadata_scan_plan_with(base, planner)
    }

    fn crc_arm(&self, files: Vec<Add>, planner: &MetadataPlanner<'_>) -> KernelResult<PlanBuilder> {
        planner
            .build_metadata_scan(&LOG_ADD_SCHEMA, false, |schema| {
                PlanBuilder::values(
                    LOG_ADD_SCHEMA.clone(),
                    files.into_iter().map(|add| vec![add.into()]).collect(),
                )?
                .project_patch(|patch| {
                    // Canonical CRC adds can only omit JSON stats from the read schema.
                    if schema.field_at(&column_name!(ADD_NAME, STATS)).is_ok() {
                        patch
                    } else {
                        patch.drop_at([ADD_NAME], STATS)
                    }
                })
            })?
            .project_patch(|patch| {
                patch.append(
                    FILE_ACTION_KEY_FIELD.clone(),
                    file_action_key_expr(|col| joined_column_expr!("add", col)),
                )
            })
    }

    /// Build normalized checkpoint adds. Returns an empty relation when no checkpoint exists.
    ///
    /// ## SQL equivalent:
    //
    /// SELECT STRUCT(
    ///          add.* EXCEPT (
    ///            stats_parsed, partitionValues_parsed
    ///          ),
    ///          add.stats_parsed AS stats_parsed,
    ///          add.partitionValues_parsed AS partitionValues_parsed
    ///        ) AS add,
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
        let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;
        let source = |schema| match (&shape.checkpoint_type, checkpoint) {
            (CheckpointType::Leaf, Some((FileType::Parquet, parts))) => {
                PlanBuilder::scan_parquet(parts, &[VERSION], schema)
            }
            (CheckpointType::Leaf, Some((FileType::Json, parts))) => {
                PlanBuilder::scan_json(parts, &[VERSION], schema)
            }
            (CheckpointType::Manifest, Some((file_type, parts))) => {
                match log_segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
                    Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema),
                    // Without a complete hint, load the sidecars referenced by the
                    // manifest.
                    None => sidecar_actions(file_type, parts, schema, &log_segment.log_root),
                }
            }
            (CheckpointType::None, _) | (_, None) => PlanBuilder::values(schema, vec![]),
        };
        planner
            .build_metadata_scan(available_file_schema, false, |schema| {
                source(schema)?.filter(col!("add.path").is_not_null())
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

    /// Build the normalized commit JSON arm.
    /// When `base_version` is set, only commits newer than that base are read.
    ///
    /// ## SQL equivalent:
    ///
    /// SELECT STRUCT(
    ///          add.* EXCEPT (stats_parsed, partitionValues_parsed),
    ///          FROM_JSON(add.stats, physical_stats) AS stats_parsed,
    ///          MAP_TO_STRUCT(add.partitionValues, physical_partitions) AS partitionValues_parsed
    ///        ) AS add,
    ///        version,
    ///        file_key(COALESCE(add, remove)) AS key
    /// FROM json_commits
    /// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
    ///
    /// A parsed field is omitted when its schema is absent.
    fn commit_arm(
        &self,
        base_version: Option<Version>,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let commit_files = match base_version {
            Some(version) => log_segment
                .segment_after_version(version)
                .commit_cover_version_tagged_scan_files()?,
            None => log_segment.commit_cover_version_tagged_scan_files()?,
        };
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
fn parquet_read_schema(
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
            let path = name.path();
            let replacement_root = match path.first().map(String::as_str) {
                Some(STATS_PARSED) => [ADD_NAME, STATS_PARSED],
                Some(PARTITION_VALUES_PARSED) => [ADD_NAME, PARTITION_VALUES_PARSED],
                _ => return Cow::Borrowed(name),
            };
            Cow::Owned(ColumnName::new(
                replacement_root
                    .into_iter()
                    .map(str::to_string)
                    .chain(path.iter().skip(1).cloned()),
            ))
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
    use ::test_utils::record_batch_to_bytes;
    use serde_json::json;

    use super::execution_tests::{declarative_metadata, metadata_row_count};
    use super::*;
    use crate::arrow::array::{AsArray as _, StringArray, StructArray};
    use crate::crc::Crc;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::arrow_utils::parse_json_impl;
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::Operation as PlanOperation;
    use crate::scan::data_skipping::stats_schema::expected_stats_schema;
    use crate::scan::{PartitionValuesOptions, StatsOptions};
    use crate::schema::StructType;
    use crate::snapshot::Snapshot;
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{
        create_log_path, MockProtocolBuilder, MockTableConfigurationBuilder,
    };
    use crate::{Engine as _, KernelResult};

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

    fn log_root() -> Url {
        Url::parse("file:///_delta_log/").unwrap()
    }

    pub(super) fn log_segment(
        log_root: Url,
        commits: &[&str],
        checkpoint: Option<&str>,
    ) -> LogSegment {
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
            .map(|stats| parquet_read_schema(Some(stats), None).unwrap());
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
    fn write_parquet_checkpoint(store: &Arc<InMemory>, path: &str) -> KernelResult<()> {
        use crate::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
        use crate::arrow::array::{
            Array, BooleanArray, Int64Array, RecordBatch, StringArray as SA,
        };
        use crate::arrow::datatypes::{DataType as ADT, Field, Fields, Schema};
        use crate::parquet::arrow::arrow_writer::ArrowWriter;

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
        let schema = Arc::new(Schema::new(vec![
            Field::new(ADD_NAME, ADT::Struct(add_fields.clone()), true),
            Field::new(VERSION, ADT::Int64, true),
        ]));
        let add = StructArray::new(
            add_fields,
            vec![
                Arc::new(SA::from(vec!["c.parquet"])),
                Arc::new(SA::from(vec![
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

    fn struct_stats_schema() -> SchemaRef {
        let segment = log_segment(log_root(), &[], None);
        mock_snapshot(segment)
            .unwrap()
            .scan_builder()
            .with_predicate(Arc::new(col!("x").gt(lit(5i64))))
            .with_stats(StatsOptions::all())
            .build()
            .unwrap()
            .state_info
            .physical_stats_read_schema()
            .cloned()
            .expect("stats schema")
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
    #[case::checkpoint_without_add(
        CheckpointShape {
            checkpoint_type: CheckpointType::Leaf,
            leaf_checkpoint_schema: Some(schema_ref! {}),
        },
        FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "semi_join", "project"]
    )]
    fn metadata_plan_checkpoint_arm_shape(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> KernelResult<()> {
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            Some(checkpoint_path(file_type)),
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&shape, &MetadataPlanner::try_new(&scan)?)?
            .expect("non-empty");

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
        #[values(None, Some(struct_stats_schema()))] parsed_stats: Option<SchemaRef>,
        #[values(CheckpointType::Leaf, CheckpointType::Manifest)] checkpoint_type: CheckpointType,
    ) -> KernelResult<()> {
        let stats = StatsOptions::all_struct();
        let partition_values = PartitionValuesOptions::with_struct();
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_predicate(Arc::new(col!("x").gt(lit(5i64))))
            .with_partition_values(partition_values)
            .build()?;
        let shape = CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema: Some(parquet_read_schema(
                parsed_stats.as_ref(),
                parsed_partitions.as_ref(),
            )?),
        };
        let plan = scan
            .build_metadata_scan_plan(&shape, &MetadataPlanner::try_new(&scan)?)?
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
        assert_eq!(
            add_struct(checkpoint_schema).field(STATS_PARSED).is_some(),
            parsed_stats.is_some(),
        );
        assert_eq!(
            add_struct(checkpoint_schema)
                .field(PARTITION_VALUES_PARSED)
                .is_some(),
            expect_native_partitions,
        );

        let normalization = plan
            .nodes
            .iter()
            .rev()
            .find_map(|node| match &node.op {
                Operator::Project(project) if project.schema.field(IS_ADD).is_some() => {
                    Some(project.expr.to_string())
                }
                _ => None,
            })
            .expect("checkpoint normalization project");
        assert_eq!(
            normalization.contains("MAP_TO_STRUCT"),
            !expect_native_partitions
        );
        assert!(!normalization.contains("COALESCE"));
        Ok(())
    }

    #[test]
    fn metadata_plan_commits_only() -> KernelResult<()> {
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            None,
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&no_checkpoint(), &MetadataPlanner::try_new(&scan)?)?
            .expect("non-empty");
        assert_eq!(tags(&plan), COMMIT_ARM_TAGS.to_vec());
        Ok(())
    }

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "project"])]
    #[case::leaf_json(shape(CheckpointType::Leaf, None), FileType::Json,
        vec!["scan_json", "filter", "project", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project",
            "project"])]
    #[case::checkpoint_without_add(
        CheckpointShape {
            checkpoint_type: CheckpointType::Leaf,
            leaf_checkpoint_schema: Some(schema_ref! {}),
        },
        FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "project"]
    )]
    fn metadata_plan_checkpoint_only(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(file_type)));
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&shape, &MetadataPlanner::try_new(&scan)?)?
            .expect("non-empty");
        assert_eq!(tags(&plan), checkpoint_arm_tags);
        Ok(())
    }

    #[test]
    fn metadata_plan_empty_is_none() -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        assert!(scan
            .build_metadata_scan_plan(&no_checkpoint(), &MetadataPlanner::try_new(&scan)?)?
            .is_none());
        Ok(())
    }

    #[test]
    fn metadata_plan_static_skip_all_is_none() -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_predicate(Arc::new(Predicate::FALSE))
            .build()?;
        assert_eq!(
            scan.state_info.physical_predicate,
            PhysicalPredicate::StaticSkipAll
        );
        assert!(scan
            .build_metadata_scan_plan(
                &shape(CheckpointType::Leaf, None),
                &MetadataPlanner::try_new(&scan)?,
            )?
            .is_none());
        Ok(())
    }

    #[rstest::rstest]
    #[case::no_dv(None, None, &["b.parquet"])]
    #[case::matching_dv(Some(0), Some(0), &["b.parquet"])]
    #[case::different_dv(Some(0), Some(1), &["a.parquet", "b.parquet"])]
    fn metadata_plan_executes_commit_dedup_with_sync_executor(
        #[case] add_offset: Option<i32>,
        #[case] remove_offset: Option<i32>,
        #[case] expected_paths: &[&str],
        #[values(false, true)] crc_base: bool,
    ) -> KernelResult<()> {
        let store = Arc::new(InMemory::new());
        let dv = |offset: Option<i32>| {
            offset.map(|offset| {
                json!({
                    "storageType": "p", "pathOrInlineDv": "memory:///dv.bin", "offset": offset,
                    "sizeInBytes": 1, "cardinality": 1
                })
            })
        };
        let add = |path, offset| {
            json!({
                "path": path, "size": 1, "modificationTime": 1, "dataChange": true,
                "partitionValues": {}, "deletionVector": dv(offset)
            })
        };
        let adds = [add("a.parquet", add_offset), add("b.parquet", None)];
        futures::executor::block_on(async {
            store
                .put(
                    &Path::from("_delta_log/00000000000000000000.json"),
                    adds.iter()
                        .map(|add| json!({"add": add}).to_string())
                        .collect::<Vec<_>>()
                        .join("\n")
                        .into(),
                )
                .await?;
            store
                .put(
                    &Path::from("_delta_log/00000000000000000001.json"),
                    json!({"remove": {
                        "path": "a.parquet", "deletionTimestamp": 2, "dataChange": true,
                        "deletionVector": dv(remove_offset)
                    }})
                    .to_string()
                    .into(),
                )
                .await?;
            KernelResult::<()>::Ok(())
        })?;

        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[
                "memory:///_delta_log/00000000000000000000.json",
                "memory:///_delta_log/00000000000000000001.json",
            ],
            None,
        );
        let engine = SyncEngine::new_with_store(store);
        let features = [TableFeature::DeletionVectors];
        let config = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema())
            .with_partition_columns(["p"])
            .with_protocol(MockProtocolBuilder::new().with_features(features).build())
            .with_table_root("memory:///")
            .with_version(1)
            .try_build()?;
        let all_files = serde_json::from_value(json!(adds))?;
        let crc = crc_base.then(|| {
            Arc::new(Crc {
                version: 0,
                all_files: Some(all_files),
                ..Default::default()
            })
        });
        let scan = Arc::new(Snapshot::new_with_crc(segment, config, crc, false, false)?)
            .scan_builder()
            .build()?;
        let plan = scan
            .declarative_metadata_scan_plan(&engine)?
            .expect("non-empty");
        assert_eq!(
            plan.nodes
                .iter()
                .any(|node| matches!(node.op, Operator::Values(_))),
            crc_base
        );
        let batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let mut actual_paths = vec![];
        for batch in batches {
            let batch = batch?.try_into_record_batch()?;
            let add = batch
                .column_by_name(ADD_NAME)
                .expect("add column")
                .as_struct();
            let paths = add
                .column_by_name("path")
                .expect("add.path")
                .as_string::<i32>();
            actual_paths.extend(paths.iter().map(|path| path.unwrap().to_owned()));
        }
        actual_paths.sort_unstable();
        assert_eq!(actual_paths, expected_paths);
        Ok(())
    }

    #[rstest::rstest]
    #[case::date(DataType::DATE, DataType::INTEGER, "1970-01-02", "1970-01-02")]
    #[case::timestamp(
        DataType::TIMESTAMP,
        DataType::LONG,
        "1970-01-01T00:00:00.000Z",
        "1970-01-01T00:00:00.000001Z"
    )]
    #[case::timestamp_ntz(
        DataType::TIMESTAMP_NTZ,
        DataType::LONG,
        "1970-01-01T00:00:00.000",
        "1970-01-01T00:00:00.000001"
    )]
    fn metadata_plan_synthesizes_json_with_table_stats_types(
        #[case] logical_type: DataType,
        #[case] native_type: DataType,
        #[case] expected: &str,
        #[case] expected_native: &str,
    ) -> KernelResult<()> {
        let features = [TableFeature::TimestampWithoutTimezone];
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema_ref! { nullable "x": (logical_type) })
            .with_protocol(MockProtocolBuilder::new().with_features(features).build())
            .try_build()?;
        let stats_schema = config.stats_schema_builder().build()?;
        let file_schema = schema_ref! {
            nullable ADD_NAME: {
                ..(ADD_SCHEMA.fields().filter(|field| field.name() != STATS)),
                nullable STATS_PARSED: (expected_stats_schema(
                    &schema! { nullable "x": (native_type) },
                    &Default::default(), None, None,
                )?),
            },
        };
        let stats = |value: serde_json::Value| {
            serde_json::json!({
                "numRecords": 1,
                "minValues": {"x": value},
                "maxValues": {"x": value},
                "nullCount": {"x": 0},
            })
        };
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let add = serde_json::json!({ "add": {
            "path": "a.parquet", "partitionValues": {}, "size": 1,
            "modificationTime": 1, "dataChange": true, "stats_parsed": stats(1.into()),
        }});
        let batch = parse_json_impl(&StringArray::from(vec![add.to_string()]), file_schema)?;
        futures::executor::block_on(store.put(
            &Path::from("_delta_log/00000000000000000000.checkpoint.parquet"),
            record_batch_to_bytes(&batch).into(),
        ))?;
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let scan = Arc::new(Snapshot::new(segment, config)?)
            .scan_builder()
            .with_stats(StatsOptions::all())
            .build()?;
        let batches = declarative_metadata(&scan, &engine)?;
        assert_eq!(metadata_row_count(&batches), 1);
        let native = StringArray::from(vec![stats(expected_native.into()).to_string()]);
        let expected_native = StructArray::from(parse_json_impl(&native, stats_schema)?);
        for batch in batches {
            let json = batch.column_by_name(STATS).unwrap().as_string::<i32>();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(json.value(0))?,
                stats(expected.into())
            );
            assert_eq!(
                batch.column_by_name(STATS_PARSED).unwrap().as_struct(),
                &expected_native,
            );
        }
        Ok(())
    }

    #[rstest::rstest]
    fn metadata_plan_native_stats_need_no_projection(
        #[values(1, 100)] width: usize,
    ) -> KernelResult<()> {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(StructType::new_unchecked((0..width).map(|index| {
                StructField::nullable(format!("c{index}"), DataType::LONG)
            })))
            .with_properties([("delta.dataSkippingNumIndexedCols", "-1")])
            .try_build()?;
        let scan = Arc::new(Snapshot::new(log_segment(log_root(), &[], None), config)?)
            .scan_builder()
            .with_stats(StatsOptions::all_struct())
            .build()?;
        let planner = MetadataPlanner::try_new(&scan)?;
        let file_schema = parquet_read_schema(planner.stats_schema.as_ref(), None)?;
        let plan = planner
            .build_metadata_scan(&file_schema, false, |schema| {
                PlanBuilder::values(schema, vec![])
            })?
            .build()?;
        assert_eq!(tags(&plan), ["values"]);
        Ok(())
    }

    #[rstest::rstest]
    #[case::keeps_matching_file(StatsOptions::all_struct(), 5, 1)]
    #[case::prunes_non_matching_file(StatsOptions::all_struct(), 20, 0)]
    #[case::json_ignores_predicate(StatsOptions::json_only(), 20, 1)]
    fn metadata_planner_falls_back_to_json_for_incompatible_stats(
        #[case] stats: StatsOptions,
        #[case] lower_bound: i64,
        #[case] expected_rows: usize,
    ) -> KernelResult<()> {
        let stats_json =
            r#"{"numRecords":1,"minValues":{"x":10},"maxValues":{"x":10},"nullCount":{"x":0}}"#;
        let file_schema = schema_ref! {
            nullable ADD_NAME: {
                ..(ADD_SCHEMA.fields()),
                nullable STATS_PARSED: { nullable "minValues": { nullable "x": STRING } },
            },
        };
        let add = serde_json::json!({ "add": {
            "path": "a.parquet", "partitionValues": {}, "size": 1,
            "modificationTime": 1, "dataChange": true, "stats": stats_json,
            "stats_parsed": { "minValues": { "x": "incompatible" } },
        }});
        let json = StringArray::from(vec![add.to_string()]);
        let batch = parse_json_impl(&json, file_schema.clone())?;
        let store = Arc::new(InMemory::new());
        futures::executor::block_on(store.put(
            &Path::from("_delta_log/00000000000000000000.checkpoint.parquet"),
            record_batch_to_bytes(&batch).into(),
        ))?;
        let segment = log_segment(
            Url::parse("memory:///_delta_log/")?,
            &[],
            Some("memory:///_delta_log/00000000000000000000.checkpoint.parquet"),
        );
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_predicate(Arc::new(col!("x").gt(lit(lower_bound))))
            .build()?;
        let (read_schema, kind, _) =
            MetadataPlanner::try_new(&scan)?.read_schema(&file_schema, false)?;
        assert!(kind == StatsKind::Json);
        assert_eq!(add_struct(&read_schema), &*ADD_SCHEMA);
        let batches = declarative_metadata(&scan, &SyncEngine::new_with_store(store))?;
        assert_eq!(metadata_row_count(&batches), expected_rows);
        for batch in batches {
            let paths = batch.column_by_name("path").unwrap().as_string::<i32>();
            assert_eq!(paths.value(0), "a.parquet");
            if let Some(schema) = scan.state_info.physical_stats_output_schema() {
                let json = StringArray::from(vec![stats_json]);
                let expected = StructArray::from(parse_json_impl(&json, schema.clone())?);
                assert_eq!(
                    batch.column_by_name(STATS_PARSED).unwrap().as_struct(),
                    &expected
                );
            } else {
                let json = batch.column_by_name(STATS).unwrap().as_string::<i32>();
                assert_eq!(json.value(0), stats_json);
            }
        }
        Ok(())
    }

    #[rstest::rstest]
    #[case::keeps_matching_file(StatsOptions::all(), 5, 1)]
    #[case::prunes_non_matching_file(StatsOptions::all(), 20, 0)]
    #[case::json_ignores_predicate(StatsOptions::json_only(), 20, 1)]
    fn metadata_plan_executes_leaf_without_stats_parsed(
        #[case] stats: StatsOptions,
        #[case] lower_bound: i64,
        #[case] expected_rows: usize,
    ) -> KernelResult<()> {
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
            .with_predicate(Arc::new(col!("x").gt(lit(lower_bound))))
            .build()?;
        let plan = scan
            .build_metadata_scan_plan(
                &shape(CheckpointType::Leaf, None),
                &MetadataPlanner::try_new(&scan)?,
            )?
            .expect("non-empty");

        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let actual_rows = batches.try_fold(0, |rows, batch| {
            Ok::<_, crate::KernelError>(rows + batch?.try_into_record_batch()?.num_rows())
        })?;
        assert_eq!(actual_rows, expected_rows);
        Ok(())
    }
}
