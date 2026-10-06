//! Declarative metadata scan plans.
//!
//! [`Scan::build_metadata_scan_plan`] reconciles checkpoint and commit actions into live adds,
//! applying metadata pruning before newest-action-wins replay.

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

use url::Url;

use super::data_skipping::as_sql_data_skipping_predicate_with_stats_columns;
use super::state_info::StateInfo;
use super::{PhysicalPredicate, Scan};
use crate::actions::{
    ADD_FIELD, ADD_NAME, ADD_SCHEMA, REMOVE_FIELD, SIDECAR_FIELD, SIDECAR_NAME, STATS_PARSED,
};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{
    col, column_name, joined_column_expr, lit, ColumnName, Expression as Expr, MapToStructOptions,
    Predicate,
};
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::plans::ir::plan::Plan;
use crate::scan::log_replay::{PARTITION_VALUES_PARSED_NAME, STATS_PARSED_NAME};
use crate::schema::{
    lazy_schema_ref, schema, schema_ref, DataType, SchemaRef, SchemaStructPatchBuilder,
    StructField, StructType,
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
// Generated partition pruning predicates reference this to retain removes.
const IS_ADD: &str = "is_add";
const VERSION: &str = "version";

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
    pub(super) fn build_metadata_scan_plan(
        &self,
        shape: &CheckpointShape,
    ) -> KernelResult<Option<Plan>> {
        let state = &self.state_info;
        // A statically-unsatisfiable predicate (e.g. `x > 10 AND FALSE`) skips the whole table.
        if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
            return Ok(None);
        }

        let prune = stats_skipping_predicate(state);
        let prune = prune.as_ref();

        let commit_actions = self.commit_arm(prune)?;

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

        let checkpoint_adds = self.checkpoint_arm(shape, prune)?;

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
    //
    /// SELECT PATCH_STRUCT(add, <needed parsed fields>, <unrequested field drops>) AS add,
    ///        version, add.path IS NOT NULL AS is_add, file_key(add) AS key
    /// FROM checkpoint_actions
    /// WHERE add.path IS NOT NULL
    ///
    /// When the checkpoint lacks native parsed metadata, `FROM_JSON(add.stats, physical_stats)`
    /// and `MAP_TO_STRUCT(add.partitionValues, physical_partitions)` replace the corresponding
    /// fields above. A parsed field is omitted when its schema is absent.
    fn checkpoint_arm(
        &self,
        shape: &CheckpointShape,
        prune: Option<&Predicate>,
    ) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let physical_stats = self.state_info.physical_stats_read_schema();
        let physical_partitions = self.state_info.physical_partition_schema.as_ref();
        let source_physical_stats =
            physical_stats.and_then(|schema| shape.compatible_stats_parsed_schema(schema));
        let source_physical_partitions = physical_partitions
            .and_then(|schema| shape.compatible_partition_values_parsed_schema(schema));
        let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;

        let actions = match (&shape.checkpoint_type, checkpoint) {
            (CheckpointType::Leaf, Some((FileType::Parquet, parts))) => {
                let schema =
                    parquet_read_schema(source_physical_stats, source_physical_partitions)?;
                PlanBuilder::scan_parquet(parts, &[VERSION], schema)
            }
            (CheckpointType::Leaf, Some((FileType::Json, parts))) => {
                PlanBuilder::scan_json(
                    parts,
                    &[VERSION],
                    json_read_schema(/* include_remove */ false),
                )
            }
            (CheckpointType::Manifest, Some((file_type, parts))) => {
                let schema =
                    parquet_read_schema(source_physical_stats, source_physical_partitions)?;
                match log_segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
                    Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema),
                    // Without a complete hint, load the sidecars referenced by the manifest.
                    None => sidecar_actions(file_type, parts, schema, &log_segment.log_root),
                }
            }
            (CheckpointType::None, _) | (_, None) => {
                PlanBuilder::values(json_read_schema(/* include_remove */ false), vec![])
            }
        }?;

        actions
            .filter(col!("add.path").is_not_null())?
            .project_patch(|patch| {
                patch
                    .with_parsed_add_stats(physical_stats)
                    .with_parsed_add_partition_values(physical_partitions)
                    .append(
                        StructField::not_null(IS_ADD, DataType::BOOLEAN),
                        Expr::from(col!("add.path").is_not_null()),
                    )
                    .append(
                        FILE_ACTION_KEY_FIELD.clone(),
                        file_action_key_expr(|col| joined_column_expr!("add", col)),
                    )
            })?
            .try_fold_with(prune, |p, prune| p.filter(prune.clone()))?
            .project_patch(|patch| patch.with_metadata_output(self).drop(VERSION).drop(IS_ADD))
    }

    /// Build commit JSON actions in the requested output shape.
    ///
    /// ## SQL equivalent:
    ///
    /// SELECT PATCH_STRUCT(add, <needed parsed fields>, <unrequested field drops>) AS add,
    ///        remove, version, add.path IS NOT NULL AS is_add,
    ///        file_key(COALESCE(add, remove)) AS key
    /// FROM json_commits
    /// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
    ///
    /// A parsed field is omitted when its schema is absent.
    fn commit_arm(&self, prune: Option<&Predicate>) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let commit_files = log_segment.commit_cover_version_tagged_scan_files()?;
        PlanBuilder::scan_json(commit_files, &[VERSION], json_read_schema(true))?
            .filter(Predicate::or(
                col!("add.path").is_not_null(),
                col!("remove.path").is_not_null(),
            ))?
            .project_patch(|patch| {
                // Commits never carry source-native parsed columns, so derive the needed fields
                // from the raw encodings.
                patch
                    .with_parsed_add_stats(self.state_info.physical_stats_read_schema())
                    .with_parsed_add_partition_values(
                        self.state_info.physical_partition_schema.as_ref(),
                    )
                    .append(
                        StructField::not_null(IS_ADD, DataType::BOOLEAN),
                        Expr::from(col!("add.path").is_not_null()),
                    )
                    .append(
                        FILE_ACTION_KEY_FIELD.clone(),
                        file_action_key_expr(|col| {
                            Expr::coalesce([
                                joined_column_expr!("add", col),
                                joined_column_expr!("remove", col),
                            ])
                        }),
                    )
            })?
            .try_fold_with(prune, |p, prune| {
                // Removes must survive replay; only adds are safe to prune.
                p.filter(Predicate::or(col!("add").is_null(), prune.clone()))
            })?
            .project_patch(|patch| {
                patch
                    .with_metadata_output(self)
                    .drop(crate::actions::REMOVE_NAME)
                    .drop(IS_ADD)
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

/// Read schema for JSON actions tagged with their log version.
/// Commits include removes; JSON checkpoint leaves do not.
fn json_read_schema(include_remove: bool) -> SchemaRef {
    schema_ref! {
        (&ADD_FIELD),
        ..(include_remove.then_some(&REMOVE_FIELD)),
        nullable VERSION: LONG,
    }
}

/// Read schema for parquet add actions.
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

trait ProjectionStructPatchBuilderExt<'a> {
    /// Parses add stats, preferring a compatible parsed field.
    ///
    /// When `physical_stats` is present, the input must contain either
    /// `add.stats_parsed` or the fallback `add.stats` JSON field.
    fn with_parsed_add_stats(self, physical_stats: Option<&SchemaRef>) -> Self;

    /// Parses add partition values when a compatible parsed field is not already present.
    fn with_parsed_add_partition_values(self, physical_partitions: Option<&SchemaRef>) -> Self;

    /// Applies the engine-facing metadata shape after pruning consumes its working columns.
    fn with_metadata_output(self, scan: &Scan) -> Self;
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

    fn with_metadata_output(mut self, scan: &Scan) -> Self {
        if !scan.stats.synthesize_json && self.input_schema().contains_col([ADD_NAME, STATS]) {
            self = self.drop_at([ADD_NAME], STATS);
        }

        if let Some(output) = scan.state_info.physical_stats_output_schema() {
            if let Some(read) = scan.state_info.physical_stats_read_schema() {
                self = drop_unrequested_struct_fields(
                    self,
                    read,
                    output,
                    column_name!("add.stats_parsed"),
                );
            }
        } else if self.input_schema().contains_col([ADD_NAME, STATS_PARSED]) {
            self = self.drop_at([ADD_NAME], STATS_PARSED);
        }

        if !scan.partition_values.parsed_struct
            && self
                .input_schema()
                .contains_col([ADD_NAME, PARTITION_VALUES_PARSED])
        {
            self = self.drop_at([ADD_NAME], PARTITION_VALUES_PARSED);
        }

        self
    }
}

/// Applies sparse nested drops that narrow an input struct to the requested output struct.
///
/// The output schema is the consumer-visible subset of the read schema: it contains requested
/// stats, while the read schema may additionally contain fields used only by data skipping.
fn drop_unrequested_struct_fields<'a>(
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
                    patch = drop_unrequested_struct_fields(
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

    fn metadata_output_patch(plan: &Plan) -> &ExpressionStructPatch {
        let project = plan
            .nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::Project(project)
                    if project.schema.field(FILE_ACTION_KEY).is_some()
                        && project.schema.field(VERSION).is_none()
                        && project.schema.field(IS_ADD).is_none() =>
                {
                    Some(project)
                }
                _ => None,
            })
            .expect("metadata output project");
        let Expr::StructPatch(patch) = project.expr.as_ref() else {
            panic!("metadata output should use a sparse struct patch");
        };
        patch
    }

    fn nested_patch<'a>(
        patch: &'a ExpressionStructPatch,
        field: &str,
    ) -> &'a ExpressionStructPatch {
        let field_patch = patch
            .field_patches
            .get(field)
            .unwrap_or_else(|| panic!("missing patch for {field}"));
        assert!(!field_patch.keep_input);
        assert_eq!(field_patch.insertions.len(), 1);
        let Expr::StructPatch(nested) = field_patch.insertions[0].as_ref() else {
            panic!("{field} should be changed by a nested struct patch");
        };
        nested
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
                    Expr::Struct(..) => panic!("sparse metadata output contains a dense struct"),
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

    // Commit-arm operator sequence without optional pruning.
    const COMMIT_ARM_TAGS: &[&str] = &[
        "scan_json", // commits
        "filter",    // keep file actions
        "project",   // normalize
        "project",   // shape metadata output
        "aggregate", // newest-action-per-key
        "filter",    // live commit adds
        "project",   // extract add
    ];

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "project", "semi_join", "project"])]
    #[case::leaf_json(shape(CheckpointType::Leaf, None), FileType::Json,
        vec!["scan_json", "filter", "project", "project", "semi_join", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project",
            "project", "semi_join", "project"])]
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

            let output_patch = metadata_output_patch(&plan);
            let add_patch = nested_patch(output_patch, ADD_NAME);
            assert!(add_patch.field_patches.contains_key(STATS));
            assert!(
                !add_patch.field_patches.contains_key(STATS_PARSED),
                "native stats should pass through rather than be replaced"
            );
            sizes.push(sparse_patch_size(output_patch));
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
        vec!["scan_parquet", "filter", "project", "project", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project",
            "project", "project"])]
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
