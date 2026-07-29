//! Declarative metadata scan plans.
//!
//! [`build_metadata_scan_plan`] reconciles checkpoint and commit actions into live adds, applying
//! metadata pruning before newest-action-wins replay.

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

use url::Url;

use super::data_skipping::as_sql_data_skipping_predicate_with_stats_columns;
use super::state_info::StateInfo;
use super::PhysicalPredicate;
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{
    ADD_FIELD, ADD_NAME, ADD_SCHEMA, REMOVE_FIELD, SIDECAR_FIELD, SIDECAR_NAME, STATS_PARSED,
};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{
    col, column_name, joined_column_expr, ColumnName, Expression as Expr, Predicate,
};
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::{FileType, Load, LoadColumnFileMeta, ScanFile};
use crate::plans::ir::plan::{Plan, COMMIT_ACTION_POSITION_V1};
use crate::schema::{
    lazy_schema_ref, schema, schema_ref, DataType, MapType, SchemaRef, SchemaStructPatchBuilder,
    StructField, ToSchema as _,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::utils::FoldWithOption as _;
use crate::{DeltaResult, PlanBuilder};

// === Internal column names ===

// Both add and remove provide path + DV (storageType, pathOrInlineDv, offset) columns. We
// materialize them as one top-level `file_action_key` column that are used by the plan's
// aggregate and anti-join operators.
const FILE_ACTION_KEY: &str = "file_action_key";
const STATS: &str = "stats";
const PARTITION_VALUES: &str = "partitionValues";
const PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
const IS_ADD: &str = "is_add";
const ORDERING_KEY: &str = "ordering_key";
const ACTION_POSITION: &str = "actionPosition";
const VERSION: &str = "version";

/// Build the live-add metadata plan from checkpoint and commit actions.
///
/// Returns `None` for an empty result or a statically false predicate.
pub(crate) fn build_metadata_scan_plan(
    state: &StateInfo,
    log_segment: &LogSegment,
    shape: &CheckpointShape,
) -> DeltaResult<Option<Plan>> {
    // A statically-unsatisfiable predicate (e.g. `x > 10 AND FALSE`) skips the whole table.
    if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
        return Ok(None);
    }

    let stats_schema = state.physical_stats_schema.as_ref();
    let partition_schema = state.physical_partition_schema.as_ref();
    let prune = stats_skipping_predicate(state);
    let prune = prune.as_ref();

    // The output `add` after reparsing `stats`/`partitionValues`: shared by the commit arm's dedup
    // carrier and both terminal `{ add }` projections, so every arm agrees on the union schema.
    let add_field = add_field_with_parsed_stats_and_partitions(stats_schema, partition_schema)?;
    let output_schema = schema_ref! { (&add_field) };

    let commit_actions = commit_arm(log_segment, stats_schema, partition_schema)?.try_fold_with(
        prune,
        |plan, predicate| {
            // Removes must survive pruning so they can suppress older checkpoint Adds. Adds may be
            // pruned before commit dedup because the complete predicate is applied again to data.
            plan.filter(Predicate::or(col!("add").is_null(), predicate.clone()))
        },
    )?;

    let deduped_commit = commit_actions
        // Wrap `add` so removes, whose inner `add` is null, survive MaxNonNullBy. Order actions in
        // the same commit by their absolute JSON line byte position.
        .project_patch(|patch| {
            patch
                .replace(
                    ADD_NAME,
                    StructField::not_null(ADD_NAME, schema! { (add_field.clone()) }),
                    Expr::struct_from([col!("add")]),
                )
                .append(
                    StructField::not_null(
                        ORDERING_KEY,
                        schema! {
                            not_null (VERSION): LONG,
                            not_null (ACTION_POSITION): LONG,
                        },
                    ),
                    Expr::struct_from([col!(VERSION), col!(ACTION_POSITION)]),
                )
        })?
        .aggregate_by([ColumnName::new([FILE_ACTION_KEY])], |aggregate| {
            aggregate.max_non_null_by(ColumnName::new([ADD_NAME]), ColumnName::new([ORDERING_KEY]))
        })?
        .project_patch(|patch| patch.replace(ADD_NAME, add_field.clone(), col!("add.add")))?;

    let checkpoint_adds = checkpoint_arm(log_segment, shape, stats_schema, partition_schema)?
        .try_fold_with(prune, |plan, predicate| plan.filter(predicate.clone()))?;

    let checkpoint_live_adds = checkpoint_adds
        .anti_join(
            deduped_commit.clone(),
            [ColumnName::new([FILE_ACTION_KEY])],
            [ColumnName::new([FILE_ACTION_KEY])],
        )?
        .project(Expr::struct_from([col!("add")]), output_schema.clone())?;

    let commit_live_adds = deduped_commit
        .filter(col!("add").is_not_null())?
        .project(Expr::struct_from([col!("add")]), output_schema)?;

    Ok(
        PlanBuilder::union_all([commit_live_adds, checkpoint_live_adds])?
            .build_opt()?
            .map(|plan| plan.require_capability(COMMIT_ACTION_POSITION_V1)),
    )
}

/// Build normalized checkpoint adds. Returns an empty relation when no checkpoint exists.
///
/// ## SQL equivalent:
//
/// SELECT STRUCT(
///          add.* EXCEPT (
///            stats, stats_parsed, partitionValues, partitionValues_parsed
///          ),
///          add.stats_parsed AS stats,
///          MAP_TO_STRUCT(add.partitionValues, partition_schema) AS partitionValues
///        ) AS add,
///        version, add.path IS NOT NULL AS is_add, file_key(add) AS key
/// FROM checkpoint_actions
/// WHERE add.path IS NOT NULL
fn checkpoint_arm(
    log_segment: &LogSegment,
    shape: &CheckpointShape,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<PlanBuilder> {
    let source_stats_schema = shape.parsed_stats_schema.as_ref();
    let source_partition_schema = shape.parsed_partition_schema.as_ref();
    let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;

    let actions = match (&shape.checkpoint_type, checkpoint) {
        (CheckpointType::Leaf, Some((FileType::Parquet, parts))) => {
            let schema = parquet_read_schema(source_stats_schema, source_partition_schema)?;
            PlanBuilder::scan_parquet(parts, &[VERSION], schema)
        }
        (CheckpointType::Leaf, Some((FileType::Json, parts))) => {
            PlanBuilder::scan_json(
                parts,
                &[VERSION],
                json_read_schema(
                    /* include_remove */ false, /* action_position */ false,
                ),
            )
        }
        (CheckpointType::Manifest, Some((file_type, parts))) => {
            let schema = parquet_read_schema(source_stats_schema, source_partition_schema)?;
            match log_segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
                Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema),
                // Without a complete hint, load the sidecars referenced by the manifest.
                None => sidecar_actions(file_type, parts, schema, &log_segment.log_root),
            }
        }
        (CheckpointType::None, _) | (_, None) => {
            PlanBuilder::values(
                json_read_schema(
                    /* include_remove */ false, /* action_position */ false,
                ),
                vec![],
            )
        }
    }?;

    actions
        .filter(col!("add.path").is_not_null())?
        .project_patch(|patch| {
            patch
                .with_parsed_add_stats_and_partitions(stats_schema, partition_schema)
                .append(
                    StructField::not_null(IS_ADD, DataType::BOOLEAN),
                    Expr::from(col!("add.path").is_not_null()),
                )
                .append(
                    FILE_ACTION_KEY_FIELD.clone(),
                    file_action_key_expr(|column| joined_column_expr!("add", column)),
                )
        })
}

/// Build the normalized commit JSON arm.
///
/// ## SQL equivalent:
///
/// SELECT STRUCT(
///          add.* EXCEPT (stats, partitionValues),
///          FROM_JSON(add.stats, stats_schema) AS stats,
///          MAP_TO_STRUCT(add.partitionValues, partition_schema) AS partitionValues
///        ) AS add,
///        remove, version, add.path IS NOT NULL AS is_add,
///        file_key(COALESCE(add, remove)) AS key
/// FROM json_commits
/// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
fn commit_arm(
    log_segment: &LogSegment,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<PlanBuilder> {
    let commit_files = log_segment.commit_cover_version_tagged_scan_files()?;
    PlanBuilder::scan_json_with_action_position(
        commit_files,
        &[VERSION],
        json_read_schema(
            /* include_remove */ true, /* action_position */ true,
        ),
        ACTION_POSITION,
    )?
    .filter(Predicate::or(
        col!("add.path").is_not_null(),
        col!("remove.path").is_not_null(),
    ))?
    .project_patch(|patch| {
        // Commits never carry source-native parsed columns, so normalize from the raw
        // encodings.
        patch
            .with_parsed_add_stats_and_partitions(stats_schema, partition_schema)
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
    })
}

/// Load actions from V2 checkpoint sidecars.
fn sidecar_actions(
    file_type: FileType,
    root_parts: Vec<ScanFile>,
    action_schema: SchemaRef,
    log_root: &Url,
) -> DeltaResult<PlanBuilder> {
    const FILE_PATH: &str = "path";
    const FILE_SIZE: &str = "size";
    const NUM_RECORDS: &str = "num_records";
    const DV: &str = "dv";
    const SIDECAR_SIZE: &str = "sizeInBytes";

    static SIDECAR_FILE_META_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
        nullable (FILE_PATH): STRING,
        nullable (FILE_SIZE): LONG,
        nullable (NUM_RECORDS): LONG,
        nullable (DV): (DeletionVectorDescriptor::to_schema()),
        nullable (VERSION): LONG,
    };

    static SIDECAR_READ_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
        (&SIDECAR_FIELD),
        nullable (VERSION): LONG,
    };

    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    let sidecar_files = scan(root_parts, &[VERSION], SIDECAR_READ_SCHEMA.clone())?
        .filter(col!(SIDECAR_NAME).is_not_null())?
        .project(
            Expr::struct_from([
                col!(SIDECAR_NAME, FILE_PATH),
                col!(SIDECAR_NAME, SIDECAR_SIZE),
                Expr::null_literal(DataType::LONG),
                Expr::null_literal(DeletionVectorDescriptor::to_schema().into()),
                col!(VERSION),
            ]),
            SIDECAR_FILE_META_SCHEMA.clone(),
        )?;

    let load = Load::new(
        action_schema,
        FileType::Parquet,
        LoadColumnFileMeta::new(
            ColumnName::new([FILE_PATH]),
            ColumnName::new([FILE_SIZE]),
            ColumnName::new([NUM_RECORDS]),
        ),
        ColumnName::new([DV]),
    )
    .with_base_url(log_root.join("_sidecars/")?)
    .with_file_constant_columns([VERSION]);

    sidecar_files.load(load)
}

// === Helpers ===

/// Read schema for JSON actions tagged with their log version.
/// Commits include removes; JSON checkpoint leaves do not.
fn json_read_schema(include_remove: bool, include_action_position: bool) -> SchemaRef {
    schema_ref! {
        (&ADD_FIELD),
        ..(include_remove.then_some(&REMOVE_FIELD)),
        not_null (VERSION): LONG,
        ..(include_action_position.then(|| StructField::not_null(ACTION_POSITION, DataType::LONG))),
    }
}

/// Read schema for parquet add actions.
fn parquet_read_schema(
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<SchemaRef> {
    let add_patch = SchemaStructPatchBuilder::new()
        .fold_with(stats_schema, |patch, ss| {
            patch.append(StructField::nullable(STATS_PARSED, ss.as_ref().clone()))
        })
        .fold_with(partition_schema, |patch, ps| {
            patch.append(StructField::nullable(
                PARTITION_VALUES_PARSED,
                ps.as_ref().clone(),
            ))
        });
    Ok(schema_ref! {
        (StructField::nullable(ADD_NAME, add_patch.build(&ADD_SCHEMA)?)),
        not_null (VERSION): LONG,
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
    StructField::not_null(FILE_ACTION_KEY, schema)
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
    /// Parse add stats and partition values, preferring compatible parsed input columns.
    fn with_parsed_add_stats_and_partitions(
        self,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<&SchemaRef>,
    ) -> Self;
}

impl<'a> ProjectionStructPatchBuilderExt<'a> for ProjectionStructPatchBuilder<'a> {
    fn with_parsed_add_stats_and_partitions(
        mut self,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<&SchemaRef>,
    ) -> Self {
        let has_stats_parsed = self
            .input_schema()
            .field_at(&column_name!("add.stats_parsed"))
            .is_ok();
        let has_partition_values_parsed = self
            .input_schema()
            .field_at(&column_name!("add.partitionValues_parsed"))
            .is_ok();
        let add = [ADD_NAME];
        self = match stats_schema {
            Some(ss) => {
                let field = StructField::nullable(STATS, ss.as_ref().clone());
                if has_stats_parsed {
                    self.replace_at(add, STATS, field, col!(ADD_NAME, STATS_PARSED))
                        .drop_at(add, STATS_PARSED)
                } else {
                    let expr = Expr::parse_json(col!("add.stats"), Arc::clone(ss));
                    self.replace_at(add, STATS, field, expr)
                }
            }
            None => self.replace_expr_at(add, STATS, Expr::null_literal(DataType::STRING)),
        };
        match partition_schema {
            Some(ps) => {
                let field = StructField::nullable(PARTITION_VALUES, ps.as_ref().clone());
                if has_partition_values_parsed {
                    self.replace_at(
                        add,
                        PARTITION_VALUES,
                        field,
                        col!(ADD_NAME, PARTITION_VALUES_PARSED),
                    )
                    .drop_at(add, PARTITION_VALUES_PARSED)
                } else {
                    let expr = Expr::map_to_struct(col!("add.partitionValues"));
                    self.replace_at(add, PARTITION_VALUES, field, expr)
                }
            }
            None => {
                // The canonical `partitionValues` is non-null, but with no partition schema we
                // null it out, so the field must become nullable to match.
                let field = StructField::nullable(PARTITION_VALUES, partition_values_map_type());
                let expr = Expr::null_literal(partition_values_map_type());
                self.replace_at(add, PARTITION_VALUES, field, expr)
            }
        }
    }
}

/// The canonical `add.partitionValues` type.
fn partition_values_map_type() -> DataType {
    MapType::new(DataType::STRING, DataType::STRING, true).into()
}

/// The `add` field produced by [`with_parsed_add_stats_and_partitions`].
fn add_field_with_parsed_stats_and_partitions(
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<StructField> {
    // `partitionValues` is non-null in the log schema, but the parsed field is nullable.
    let partition_field = match partition_schema {
        Some(ps) => StructField::nullable(PARTITION_VALUES, ps.as_ref().clone()),
        None => StructField::nullable(PARTITION_VALUES, partition_values_map_type()),
    };
    let patch = SchemaStructPatchBuilder::new()
        .fold_with(stats_schema, |patch, ss| {
            patch.replace(STATS, StructField::nullable(STATS, ss.as_ref().clone()))
        })
        .replace(PARTITION_VALUES, partition_field);
    Ok(StructField::nullable(ADD_NAME, patch.build(&ADD_SCHEMA)?))
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
                Some(STATS_PARSED) => [ADD_NAME, STATS],
                Some(PARTITION_VALUES_PARSED) => [ADD_NAME, PARTITION_VALUES],
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
        &state.physical_stats_columns,
    )?;
    // A null skipping verdict means the available metadata cannot prove the file is skippable.
    let skipping = Predicate::distinct(skipping, Expr::literal(false));
    let mut prefixer = MetadataSkippingColumnPrefixer;
    Some(prefixer.transform_pred(&skipping).into_owned())
}

#[cfg(test)]
#[path = "scan_plan/tests.rs"]
mod execution_tests;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::arrow::array::{StringArray, StructArray};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::lit;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::Operation as PlanOperation;
    use crate::scan::state_info::tests::get_state_info_with_options;
    use crate::scan::{PartitionValuesOptions, StatsOptions};
    use crate::schema::StructType;
    use crate::utils::test_utils::create_log_path;
    use crate::Engine as _;

    fn state(
        schema: SchemaRef,
        partition_columns: Vec<String>,
        predicate: Option<Predicate>,
        stats: StatsOptions,
        partition_values: PartitionValuesOptions,
    ) -> StateInfo {
        get_state_info_with_options(
            schema,
            partition_columns,
            predicate.map(Arc::new),
            &[],
            HashMap::new(),
            vec![],
            stats,
            partition_values,
        )
        .expect("state info")
    }

    fn data_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "x",
            DataType::LONG,
        )]))
    }

    fn partitioned_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::nullable("x", DataType::LONG),
            StructField::nullable("p", DataType::STRING),
        ]))
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
        CheckpointShape {
            checkpoint_type,
            parsed_stats_schema: parsed_stats,
            parsed_partition_schema: None,
        }
    }

    fn no_checkpoint() -> CheckpointShape {
        shape(CheckpointType::None, None)
    }

    fn op_tag(op: &Operator) -> &'static str {
        match op {
            Operator::ScanParquet(_) => "scan_parquet",
            Operator::ScanJson(_) => "scan_json",
            Operator::Values(_) => "values",
            Operator::Filter(_) => "filter",
            Operator::Project(_) => "project",
            Operator::Load(_) => "load",
            Operator::Aggregate(_) => "aggregate",
            Operator::SemiJoin(_) => "semi_join",
            Operator::UnionAll(_) => "union_all",
        }
    }

    fn tags(plan: &Plan) -> Vec<&'static str> {
        plan.nodes.iter().map(|n| op_tag(&n.op)).collect()
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
    fn write_parquet_checkpoint(store: &Arc<InMemory>, path: &str) -> DeltaResult<()> {
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
        state(
            data_schema(),
            vec![],
            Some(col!("x").gt(lit(5i64))),
            StatsOptions::all(),
            PartitionValuesOptions::default(),
        )
        .physical_stats_schema
        .expect("stats schema")
    }

    // Commit-arm operator sequence without optional pruning.
    const COMMIT_ARM_TAGS: &[&str] = &[
        "scan_json", // commits
        "filter",    // keep file actions
        "project",   // normalize
        "project",   // wrap add and build ordering key
        "aggregate", // newest action per file identity
        "project",   // unwrap newest add
        "filter",    // live commit adds
        "project",   // extract add
    ];

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "semi_join", "project"])]
    #[case::leaf_json(shape(CheckpointType::Leaf, None), FileType::Json,
        vec!["scan_json", "filter", "project", "semi_join", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "load", "filter", "project", "semi_join", "project"])]
    fn metadata_plan_checkpoint_arm_shape(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            Some(checkpoint_path(file_type)),
        );
        let plan = build_metadata_scan_plan(&state, &segment, &shape)?.expect("non-empty");

        let mut expected = COMMIT_ARM_TAGS.to_vec();
        expected.extend(checkpoint_arm_tags);
        expected.push("union_all");
        assert_eq!(tags(&plan), expected);
        Ok(())
    }

    #[rstest::rstest]
    #[case::with_parsed_stats(Some(struct_stats_schema()), true)]
    #[case::without_parsed_stats(None, false)]
    fn metadata_plan_manifest_sidecar_load_stats_columns(
        #[case] parsed_stats: Option<SchemaRef>,
        #[case] expect_parsed_columns: bool,
    ) -> DeltaResult<()> {
        let state = state(
            partitioned_schema(),
            vec!["p".to_string()],
            None,
            StatsOptions::all(),
            PartitionValuesOptions::with_struct(),
        );
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let plan = build_metadata_scan_plan(
            &state,
            &segment,
            &shape(CheckpointType::Manifest, parsed_stats),
        )?
        .expect("non-empty");

        let load = plan
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Operator::Load(load) => Some(load),
                _ => None,
            })
            .expect("sidecar load");
        assert_eq!(
            add_struct(&load.schema).field(STATS_PARSED).is_some(),
            expect_parsed_columns,
        );
        assert!(
            add_struct(&load.schema)
                .field(PARTITION_VALUES_PARSED)
                .is_none(),
            "native parsed partition values are not requested yet"
        );
        Ok(())
    }

    #[test]
    fn metadata_plan_manifest_sidecar_load_partition_columns() -> DeltaResult<()> {
        let state = state(
            partitioned_schema(),
            vec!["p".to_string()],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::with_struct(),
        );
        let parsed_partition_schema = state
            .physical_partition_schema
            .clone()
            .expect("partition schema");
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let plan = build_metadata_scan_plan(
            &state,
            &segment,
            &CheckpointShape {
                checkpoint_type: CheckpointType::Manifest,
                parsed_stats_schema: None,
                parsed_partition_schema: Some(parsed_partition_schema),
            },
        )?
        .expect("non-empty");

        let load = plan
            .nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::Load(load) => Some(load),
                _ => None,
            })
            .expect("sidecar load");
        assert!(
            add_struct(&load.schema)
                .field(PARTITION_VALUES_PARSED)
                .is_some(),
            "compatible structured partition values must be requested from the Load"
        );
        Ok(())
    }

    #[test]
    fn metadata_plan_commits_only() -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            None,
        );
        let plan =
            build_metadata_scan_plan(&state, &segment, &no_checkpoint())?.expect("non-empty");
        assert_eq!(tags(&plan), COMMIT_ARM_TAGS);
        assert_eq!(
            plan.header.required_capabilities,
            [COMMIT_ACTION_POSITION_V1.to_string()]
        );
        let commit_scan = plan
            .nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::ScanJson(scan) => Some(scan),
                _ => None,
            })
            .expect("commit scan");
        assert_eq!(
            commit_scan.commit_action_position_column.as_deref(),
            Some(ACTION_POSITION)
        );
        Ok(())
    }

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "load", "filter", "project", "project"])]
    fn metadata_plan_checkpoint_only(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(file_type)));
        let plan = build_metadata_scan_plan(&state, &segment, &shape)?.expect("non-empty");
        assert_eq!(tags(&plan), checkpoint_arm_tags);
        Ok(())
    }

    #[test]
    fn metadata_plan_empty_is_none() -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let segment = log_segment(log_root(), &[], None);
        assert!(build_metadata_scan_plan(&state, &segment, &no_checkpoint())?.is_none());
        Ok(())
    }

    #[test]
    fn metadata_plan_static_skip_all_is_none() -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            Some(Predicate::literal(false)),
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        assert_eq!(state.physical_predicate, PhysicalPredicate::StaticSkipAll);
        let segment = log_segment(log_root(), &[], None);
        assert!(
            build_metadata_scan_plan(&state, &segment, &shape(CheckpointType::Leaf, None),)?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn metadata_plan_executes_commit_dedup_with_sync_executor() -> DeltaResult<()> {
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
            DeltaResult::<()>::Ok(())
        })?;

        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[
                "memory:///_delta_log/00000000000000000000.json",
                "memory:///_delta_log/00000000000000000001.json",
            ],
            None,
        );
        let plan =
            build_metadata_scan_plan(&state, &segment, &no_checkpoint())?.expect("non-empty");

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

    #[test]
    fn metadata_plan_orders_actions_within_one_commit() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        futures::executor::block_on(store.put(
            &Path::from("_delta_log/00000000000000000000.json"),
            r#"{"add":{"path":"a.parquet","size":1,"modificationTime":1,"dataChange":true,"partitionValues":{}}}
{"remove":{"path":"a.parquet","deletionTimestamp":2,"dataChange":true}}
{"add":{"path":"b.parquet","size":1,"modificationTime":1,"dataChange":true,"partitionValues":{}}}
"#
            .into(),
        ))?;
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &["memory:///_delta_log/00000000000000000000.json"],
            None,
        );
        let plan =
            build_metadata_scan_plan(&state, &segment, &no_checkpoint())?.expect("non-empty");
        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let batch = batches
            .next()
            .expect("one batch")?
            .try_into_record_batch()?;
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
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(paths.value(0), "b.parquet");
        Ok(())
    }

    #[rstest::rstest]
    #[case::keeps_matching_file(5, 1)]
    #[case::prunes_non_matching_file(20, 0)]
    fn metadata_plan_executes_leaf_without_stats_parsed(
        #[case] lower_bound: i64,
        #[case] expected_rows: usize,
    ) -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        // A single-row parquet checkpoint carrying an `add` with a JSON `stats` string but no
        // `stats_parsed` column.
        write_parquet_checkpoint(&store, "_delta_log/00000000000000000000.checkpoint.parquet")?;

        let state = state(
            data_schema(),
            vec![],
            Some(col!("x").gt(lit(lower_bound))),
            StatsOptions::all(),
            PartitionValuesOptions::default(),
        );
        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[],
            Some("memory:///_delta_log/00000000000000000000.checkpoint.parquet"),
        );
        let plan = build_metadata_scan_plan(
            &state,
            &segment,
            // Leaf with no compatible parsed stats -> parse add.stats instead.
            &shape(CheckpointType::Leaf, None),
        )?
        .expect("non-empty");

        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let actual_rows = batches.try_fold(0, |rows, batch| {
            Ok::<_, crate::Error>(rows + batch?.try_into_record_batch()?.num_rows())
        })?;
        assert_eq!(actual_rows, expected_rows);
        Ok(())
    }
}
