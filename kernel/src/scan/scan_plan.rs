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
use crate::scan::log_replay::{PARTITION_VALUES_PARSED_NAME, STATS_PARSED_NAME};
#[cfg(test)]
use crate::schema::schema_ref;
use crate::schema::{
    lazy_schema_ref, schema, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::utils::FoldWithOption as _;
use crate::{KernelResult, PlanBuilder};

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

/// Plans the source-independent metadata transformations for a declarative scan.
///
/// A source supplies rows with the requested `add` read schema. The planner derives any working
/// structured metadata, applies metadata pruning, and returns the requested output `add` shape.
/// Unrelated top-level replay columns pass through unchanged.
pub(super) struct MetadataPlanner<'a> {
    scan: &'a Scan,
    stats_predicate: Option<Predicate>,
}

impl<'a> MetadataPlanner<'a> {
    pub(super) fn new(scan: &'a Scan) -> Self {
        Self {
            scan,
            stats_predicate: stats_skipping_predicate(&scan.state_info),
        }
    }

    /// Whether source selection must retain the checkpoint file-action schema.
    ///
    /// Structured metadata needs the schema to determine whether compatible native fields can be
    /// read. JSON output also needs it to recognize a structured-only checkpoint and synthesize
    /// `stats` with `ToJson`.
    pub(super) fn requires_checkpoint_add_schema(&self) -> bool {
        self.scan.state_info.physical_stats_read_schema().is_some()
            || self.scan.state_info.physical_partition_schema.is_some()
            || self.scan.stats.synthesize_json
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
    ///   size: long,
    ///   modificationTime: long,
    ///   dataChange: boolean,
    ///   stats: string,                         // when JSON stats are requested
    ///   tags: map<string, string>,
    ///   deletionVector: struct<...>,
    ///   baseRowId: long,
    ///   defaultRowCommitVersion: long,
    ///   clusteringProvider: string,
    ///   stats_parsed: struct<...>,             // when parsed stats are requested
    ///   partitionValues_parsed: struct<...>,   // when parsed partition values are requested
    /// >
    /// ```
    /// Stats output may contain neither representation, JSON only, parsed only, or both. Parsed
    /// partition values are selected independently and omitted for unpartitioned tables. Fields
    /// needed only for pruning are omitted. Source-specific top-level columns remain available for
    /// replay.
    fn with_metadata_output<'b>(
        &self,
        mut patch: ProjectionStructPatchBuilder<'b>,
    ) -> ProjectionStructPatchBuilder<'b> {
        let input_schema = patch.input_schema();
        let has_json_stats = input_schema.contains_col([ADD_NAME, STATS]);
        let has_struct_stats = input_schema.contains_col([ADD_NAME, STATS_PARSED]);
        let has_struct_partitions = input_schema.contains_col([ADD_NAME, PARTITION_VALUES_PARSED]);
        let read_struct_stats = self.scan.state_info.physical_stats_read_schema();
        let output_struct_stats = self.scan.state_info.physical_stats_output_schema();

        debug_assert!(
            has_struct_stats || output_struct_stats.is_none(),
            "requested struct stats must exist in the working projection"
        );

        patch = match (
            has_json_stats,
            has_struct_stats,
            self.scan.stats.synthesize_json,
        ) {
            (true, _, false) => patch.drop_at([ADD_NAME], STATS),
            (false, true, true) => patch.insert_after_at(
                [ADD_NAME],
                "dataChange",
                StructField::nullable(STATS, DataType::STRING),
                Expr::unary(UnaryExpressionOp::ToJson, col!(ADD_NAME, STATS_PARSED)),
            ),
            (true, _, true) | (false, _, false) | (false, false, true) => patch,
        };

        patch = match (has_struct_stats, read_struct_stats, output_struct_stats) {
            (true, Some(input), Some(output)) => drop_predicate_only_stats(patch, input, output),
            (true, _, None) => patch.drop_at([ADD_NAME], STATS_PARSED),
            (false, _, _) | (true, None, Some(_)) => patch,
        };

        match (
            has_struct_partitions,
            self.scan.partition_values.parsed_struct,
        ) {
            (true, false) => patch.drop_at([ADD_NAME], PARTITION_VALUES_PARSED),
            (true, true) | (false, _) => patch,
        }
    }

    /// Selects the `add` fields to read from a metadata source.
    ///
    /// The returned schema contains only metadata needed by the predicate or requested output.
    /// Compatible structured fields are read directly; otherwise their raw encodings are read so
    /// they can be derived after the source is built.
    fn read_add_schema(&self, available_file_schema: &StructType) -> KernelResult<SchemaRef> {
        let available_add_schema = action_add_schema(available_file_schema)?;
        let required_stats = self.scan.state_info.physical_stats_read_schema();
        let required_partitions = self.scan.state_info.physical_partition_schema.as_ref();
        let has_json_stats = available_add_schema.field(STATS).is_some();

        let native_stats = required_stats.filter(|schema| {
            LogSegment::schema_has_compatible_stats_parsed(available_file_schema, schema)
        });
        let native_partitions = required_partitions.filter(|schema| {
            LogSegment::schema_has_compatible_partition_values_parsed(available_file_schema, schema)
        });

        // JSON-only output has no StateInfo stats schema. If the source has only structured stats,
        // read its physical schema so the output projection can serialize it after filtering.
        let json_synthesis_stats =
            (self.scan.stats.synthesize_json && !has_json_stats && required_stats.is_none())
                .then(|| available_struct_field(available_add_schema, STATS_PARSED))
                .flatten();
        let read_struct_stats = native_stats.map(Arc::clone).or(json_synthesis_stats);
        let read_struct_partitions = native_partitions.map(Arc::clone);

        let parse_stats = required_stats.is_some() && read_struct_stats.is_none();
        let read_json_stats = parse_stats || (self.scan.stats.synthesize_json && has_json_stats);

        let add_patch = SchemaStructPatchBuilder::new()
            .fold_with((!read_json_stats).then_some(()), |patch, ()| {
                patch.drop(STATS)
            })
            .fold_with(read_struct_stats.as_ref(), |patch, schema| {
                patch.append(StructField::nullable(STATS_PARSED, schema.as_ref().clone()))
            })
            .fold_with(read_struct_partitions.as_ref(), |patch, schema| {
                patch.append(StructField::nullable(
                    PARTITION_VALUES_PARSED,
                    schema.as_ref().clone(),
                ))
            });
        Ok(Arc::new(add_patch.build(&ADD_SCHEMA)?))
    }

    /// Derives structured metadata needed by the predicate or requested output when the source
    /// could not provide compatible native fields.
    fn with_derived_metadata(&self, plan: PlanBuilder) -> KernelResult<PlanBuilder> {
        let add_schema = action_add_schema(plan.schema())?;
        let required_stats = self.scan.state_info.physical_stats_read_schema();
        let required_partitions = self.scan.state_info.physical_partition_schema.as_ref();
        let parse_stats = required_stats.is_some() && add_schema.field(STATS_PARSED).is_none();
        let parse_partitions =
            required_partitions.is_some() && add_schema.field(PARTITION_VALUES_PARSED).is_none();

        if parse_stats || parse_partitions || self.stats_predicate.is_some() {
            plan.project_patch(|patch| {
                let patch = patch
                    .with_parsed_add_stats(parse_stats.then_some(required_stats).flatten())
                    .with_parsed_add_partition_values(
                        parse_partitions.then_some(required_partitions).flatten(),
                    );
                if self.stats_predicate.is_some() {
                    patch.append(
                        StructField::not_null(IS_ADD, DataType::BOOLEAN),
                        Expr::from(col!("add.path").is_not_null()),
                    )
                } else {
                    patch
                }
            })
        } else {
            Ok(plan)
        }
    }

    /// Applies metadata pruning while retaining commit removes for replay.
    fn with_metadata_filter(&self, plan: PlanBuilder) -> KernelResult<PlanBuilder> {
        let Some(predicate) = &self.stats_predicate else {
            return Ok(plan);
        };
        if plan.schema().contains(REMOVE_NAME) {
            plan.filter(Predicate::or(col!(ADD_NAME).is_null(), predicate.clone()))
        } else {
            plan.filter(predicate.clone())
        }
    }

    /// Projects the working metadata representation to the consumer-requested output shape.
    fn project_metadata_output(&self, plan: PlanBuilder) -> KernelResult<PlanBuilder> {
        let add_schema = action_add_schema(plan.schema())?;
        let has_json_stats = add_schema.field(STATS).is_some();
        let has_struct_stats = add_schema.field(STATS_PARSED).is_some();
        let has_struct_partitions = add_schema.field(PARTITION_VALUES_PARSED).is_some();
        let read_struct_stats = self.scan.state_info.physical_stats_read_schema();
        let output_struct_stats = self.scan.state_info.physical_stats_output_schema();
        let output_has_struct_stats = output_struct_stats.is_some();
        let output_has_struct_partitions = self.scan.partition_values.parsed_struct
            && self.scan.state_info.physical_partition_schema.is_some();
        let narrows_struct_stats = read_struct_stats
            .zip(output_struct_stats)
            .is_some_and(|(read, output)| read.as_ref() != output.as_ref());
        let needs_output_projection = has_json_stats != self.scan.stats.synthesize_json
            || has_struct_stats != output_has_struct_stats
            || has_struct_partitions != output_has_struct_partitions
            || narrows_struct_stats
            || self.stats_predicate.is_some();

        if needs_output_projection {
            plan.project_patch(|patch| {
                let patch = self.with_metadata_output(patch);
                if self.stats_predicate.is_some() {
                    patch.drop(IS_ADD)
                } else {
                    patch
                }
            })
        } else {
            Ok(plan)
        }
    }

    /// Read and transform one metadata source into the requested `add` shape.
    ///
    /// `available_file_schema` is the physical action schema available from the source. `source`
    /// must return a relation whose `add` field has exactly the supplied read schema. Its actual
    /// output schema determines whether removes must survive filtering; other replay columns pass
    /// through unchanged.
    fn build_source(
        &self,
        available_file_schema: &StructType,
        source: impl FnOnce(SchemaRef) -> KernelResult<PlanBuilder>,
    ) -> KernelResult<PlanBuilder> {
        let read_add_schema = self.read_add_schema(available_file_schema)?;
        let plan = source(read_add_schema)?;
        let plan = self.with_derived_metadata(plan)?;
        let plan = self.with_metadata_filter(plan)?;
        self.project_metadata_output(plan)
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
        let state = &self.state_info;
        // A statically-unsatisfiable predicate (e.g. `x > 10 AND FALSE`) skips the whole table.
        if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
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
        self.build_metadata_scan_plan_with(shape, &MetadataPlanner::new(self))
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
        let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;
        let available_file_schema = shape
            .leaf_checkpoint_schema
            .as_deref()
            .unwrap_or(get_all_actions_schema());

        metadata
            .build_source(available_file_schema, |read_add| {
                let schema = action_read_schema(read_add, /* include_remove */ false);
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
            .build_source(get_all_actions_schema(), |read_add| {
                PlanBuilder::scan_json(
                    commit_files,
                    &[VERSION],
                    action_read_schema(read_add, /* include_remove */ true),
                )?
                .filter(Predicate::or(
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

/// Wrap an `add` read schema with the source's replay columns.
fn action_read_schema(add_schema: SchemaRef, include_remove: bool) -> SchemaRef {
    let mut fields = vec![StructField::nullable(ADD_NAME, add_schema.as_ref().clone())];
    if include_remove {
        fields.push(REMOVE_FIELD.clone());
    }
    fields.push(StructField::nullable(VERSION, DataType::LONG));
    Arc::new(StructType::new_unchecked(fields))
}

/// Read schema for parquet add actions. Kept as a compact test helper.
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
    Ok(action_read_schema(
        Arc::new(add_patch.build(&ADD_SCHEMA)?),
        /* include_remove */ false,
    ))
}

/// Return the `add` struct from an action schema.
fn action_add_schema(action_schema: &StructType) -> KernelResult<&StructType> {
    let add = action_schema.field(ADD_NAME).ok_or_else(|| {
        crate::KernelError::schema("metadata source schema is missing its add field")
    })?;
    let DataType::Struct(add_schema) = add.data_type() else {
        return Err(crate::KernelError::schema(
            "metadata source add field must be a struct",
        ));
    };
    Ok(add_schema)
}

/// Return a nested struct field as a shared schema.
fn available_struct_field(add_schema: &StructType, name: &str) -> Option<SchemaRef> {
    let DataType::Struct(schema) = add_schema.field(name)?.data_type() else {
        return None;
    };
    Some(Arc::new(schema.as_ref().clone()))
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

trait ProjectionStructPatchBuilderExt<'a> {
    /// Parses add stats, preferring a compatible parsed field.
    ///
    /// When `physical_stats` is present, the input must contain either
    /// `add.stats_parsed` or the fallback `add.stats` JSON field.
    fn with_parsed_add_stats(self, physical_stats: Option<&SchemaRef>) -> Self;

    /// Parses add partition values when a compatible parsed field is not already present.
    fn with_parsed_add_partition_values(self, physical_partitions: Option<&SchemaRef>) -> Self;
}

impl<'a> ProjectionStructPatchBuilderExt<'a> for ProjectionStructPatchBuilder<'a> {
    fn with_parsed_add_stats(self, physical_stats: Option<&SchemaRef>) -> Self {
        let has_stats_parsed = self
            .input_schema()
            .contains_col([ADD_NAME, STATS_PARSED_NAME]);
        let add = [ADD_NAME];
        match physical_stats {
            Some(schema) => {
                let field = StructField::nullable(STATS_PARSED, schema.as_ref().clone());
                let expr = Expr::parse_json(col!("add.stats"), Arc::clone(schema));
                if has_stats_parsed {
                    self
                } else {
                    self.append_at(add, field, expr)
                }
            }
            None => self,
        }
    }

    fn with_parsed_add_partition_values(self, physical_partitions: Option<&SchemaRef>) -> Self {
        let has_partition_values_parsed = self
            .input_schema()
            .contains_col([ADD_NAME, PARTITION_VALUES_PARSED_NAME]);
        let add = [ADD_NAME];
        match (physical_partitions, has_partition_values_parsed) {
            (Some(schema), false) => self.append_at(
                add,
                StructField::nullable(PARTITION_VALUES_PARSED, schema.as_ref().clone()),
                Expr::map_to_struct(
                    col!(ADD_NAME, PARTITION_VALUES),
                    MapToStructOptions::default(),
                ),
            ),
            (Some(_), true) | (None, _) => self,
        }
    }
}

/// Drops stats needed only by the predicate, preserving sparse nested struct patches.
fn drop_predicate_only_stats<'a>(
    patch: ProjectionStructPatchBuilder<'a>,
    input: &StructType,
    output: &StructType,
) -> ProjectionStructPatchBuilder<'a> {
    fn drop_fields<'a>(
        mut patch: ProjectionStructPatchBuilder<'a>,
        input: &StructType,
        output: &StructType,
        path: ColumnName,
    ) -> ProjectionStructPatchBuilder<'a> {
        for input_field in input.fields() {
            match output.field(input_field.name()) {
                None => patch = patch.drop_at(path.clone(), input_field.name()),
                Some(output_field) => {
                    if let (DataType::Struct(input), DataType::Struct(output)) =
                        (input_field.data_type(), output_field.data_type())
                    {
                        patch = drop_fields(
                            patch,
                            input,
                            output,
                            path.join(&ColumnName::new([input_field.name()])),
                        );
                    }
                }
            }
        }
        patch
    }

    drop_fields(patch, input, output, column_name!("add.stats_parsed"))
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
    use super::*;
    use crate::arrow::array::{StringArray, StructArray};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::ExpressionStructPatch;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::Operation as PlanOperation;
    use crate::scan::{PartitionValuesOptions, StatsOptions};
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

    fn mock_snapshot_with_schema(
        log_segment: LogSegment,
        table_schema: SchemaRef,
    ) -> Result<Arc<Snapshot>> {
        let table_configuration = MockTableConfigurationBuilder::new()
            .with_schema(table_schema)
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

    fn sparse_patch_size(patch: &ExpressionStructPatch) -> usize {
        1 + patch.field_patches.len()
            + patch
                .prepended_fields
                .iter()
                .chain(&patch.appended_fields)
                .chain(
                    patch
                        .field_patches
                        .values()
                        .flat_map(|field| &field.insertions),
                )
                .map(|expr| match expr.as_ref() {
                    Expr::StructPatch(nested) => sparse_patch_size(nested),
                    _ => 1,
                })
                .sum::<usize>()
    }

    // One add with JSON stats and no `stats_parsed`.
    fn write_parquet_checkpoint(store: &Arc<InMemory>, path: &str) -> Result<()> {
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
        #[values(None, Some(struct_stats_schema()))] parsed_stats: Option<SchemaRef>,
        #[values(CheckpointType::Leaf, CheckpointType::Manifest)] checkpoint_type: CheckpointType,
    ) -> Result<()> {
        let stats = StatsOptions::all();
        let partition_values = PartitionValuesOptions::with_struct();
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_partition_values(partition_values)
            .build()?;
        let shape = CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema: Some(parquet_read_schema(
                parsed_stats.as_ref(),
                parsed_partitions.as_ref(),
            )?),
        };
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
            parsed_stats.is_some(),
        );
        assert_eq!(
            add_struct(checkpoint_schema)
                .field(PARTITION_VALUES_PARSED)
                .is_some(),
            expect_native_partitions,
        );

        let normalization_exprs: Vec<_> = plan
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                Operator::Project(project) => Some(project.expr.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(
            normalization_exprs
                .iter()
                .any(|expr| expr.contains("MAP_TO_STRUCT")),
            !expect_native_partitions
        );
        assert!(normalization_exprs
            .iter()
            .all(|expr| !expr.contains("COALESCE")));
        Ok(())
    }

    #[test]
    fn native_struct_stats_output_patch_is_sparse_and_width_independent() -> Result<()> {
        let mut sizes = Vec::new();
        for width in [1, 32] {
            let table_schema =
                Arc::new(StructType::new_unchecked((0..width).map(|index| {
                    StructField::nullable(format!("c{index}"), DataType::LONG)
                })));
            let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
            let scan = mock_snapshot_with_schema(segment, table_schema)?
                .scan_builder()
                .with_stats(StatsOptions::all_struct())
                .build()?;
            let native_stats = scan
                .state_info
                .physical_stats_read_schema()
                .cloned()
                .expect("all-struct output has a stats schema");
            let plan = scan
                .build_metadata_scan_plan(&shape(CheckpointType::Leaf, Some(native_stats)))?
                .expect("checkpoint plan");

            let checkpoint_schema = plan
                .nodes
                .iter()
                .find_map(|node| match &node.op {
                    Operator::ScanParquet(scan) => Some(&scan.schema),
                    _ => None,
                })
                .expect("checkpoint scan");
            let add = add_struct(checkpoint_schema);
            assert!(add.field(STATS_PARSED).is_some());
            assert!(add.field(STATS).is_none());

            let mut size = 0;
            for node in &plan.nodes {
                let Operator::Project(project) = &node.op else {
                    continue;
                };
                assert!(!project.expr.to_string().contains("PARSE_JSON"));
                let Expr::StructPatch(patch) = project.expr.as_ref() else {
                    panic!("metadata projections should use sparse struct patches");
                };
                if let Some(add_patch) = patch.field_patches.get(ADD_NAME) {
                    assert!(
                        add_patch
                            .insertions
                            .iter()
                            .all(|expr| { matches!(expr.as_ref(), Expr::StructPatch(_)) }),
                        "metadata projection must not reconstruct add densely"
                    );
                }
                size += sparse_patch_size(patch);
            }
            sizes.push(size);
        }

        assert_eq!(sizes[0], sizes[1]);
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
    #[case::keeps_matching_file(5, 1)]
    #[case::prunes_non_matching_file(20, 0)]
    fn metadata_plan_executes_leaf_without_stats_parsed(
        #[case] lower_bound: i64,
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
            .with_stats(StatsOptions::all())
            .with_predicate(Arc::new(col!("x").gt(lit(lower_bound))))
            .build()?;
        let plan = scan
            // Leaf with no compatible parsed stats -> parse add.stats instead.
            .build_metadata_scan_plan(&shape(CheckpointType::Leaf, None))?
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
