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
    ADD_NAME, ADD_SCHEMA, ADD_SCHEMA_NO_JSON_STATS, REMOVE_FIELD, SIDECAR_FIELD, SIDECAR_NAME,
    STATS, STATS_PARSED,
};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{
    col, column_name, joined_column_expr, lit, ColumnName, Expression as Expr, ExpressionRef,
    Predicate, UnaryExpressionOp,
};
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::plans::ir::plan::Plan;
use crate::scan::log_replay::{PARTITION_VALUES_PARSED_NAME, STATS_PARSED_NAME};
use crate::schema::{
    lazy_schema_ref, schema, schema_ref, DataType, SchemaRef, SchemaStructPatchBuilder,
    StructField, StructType,
};
use crate::struct_patch::{project_struct_to_schema, ProjectionStructPatchBuilder};
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::utils::FoldWithOption as _;
use crate::{DeltaResult, Error, PlanBuilder};

// === Internal column names ===

// Both add and remove provide path + DV (storageType, pathOrInlineDv, offset) columns. We
// materialize them as one top-level `file_action_key` column that are used by the plan's
// aggregate and anti-join operators.
const FILE_ACTION_KEY: &str = "file_action_key";
const PARTITION_VALUES: &str = "partitionValues";
const PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
// Generated partition pruning predicates reference this to retain removes.
const IS_ADD: &str = "is_add";
const VERSION: &str = "version";

impl Scan {
    /// Build the live-add metadata plan from checkpoint and commit actions.
    ///
    /// Returns `None` for an empty result or a statically false predicate.
    pub(super) fn build_metadata_scan_plan(
        &self,
        shape: &CheckpointShape,
    ) -> DeltaResult<Option<Plan>> {
        let state = &self.state_info;
        // A statically-unsatisfiable predicate (e.g. `x > 10 AND FALSE`) skips the whole table.
        if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
            return Ok(None);
        }

        let prune = stats_skipping_predicate(state);
        let prune = prune.as_ref();

        // The output `add` after reparsing `stats`/`partitionValues`: shared by the commit arm's
        // dedup carrier and both terminal `{ add }` projections, so every arm agrees on the
        // union schema.
        let add_field = self.normalized_add_field()?;
        let (output_expr, output_schema) = self.metadata_output_projection(&add_field)?;

        let commit_actions = self.commit_arm()?.try_fold_with(prune, |p, prune| {
            // We filter so that:
            // * All remove actions are kept
            // * Add actions that do not match the partition pruning or stats predicate are removed.
            //
            // NOTE: It is important that add actions are filtered by the partition predicate
            // because partition filtering may not be applied on data rows. On the other
            // hand, failing to skip based on data columns is safe because the data
            // predicate will also be evaluated on data rows. Thus it is crucial that we partition
            // prune adds here.
            //
            // NOTE: It is not safe to prune remove actions using the partition filter. This is
            // because a NULL result for `remove.partitionValues.partCol` may be due to
            // `remove.partitionValues` being NULL, or it may be from `partCol` being
            // NULL. Thus, we simply do not prune removes.
            p.filter(Predicate::or(col!("add").is_null(), prune.clone()))
        })?;

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

        let checkpoint_adds = self
            .checkpoint_arm(shape)?
            .try_fold_with(prune, |p, prune| p.filter(prune.clone()))?;

        let checkpoint_live_adds = checkpoint_adds
            .anti_join(
                deduped_commit.clone(),
                [column_name!(FILE_ACTION_KEY)],
                [column_name!(FILE_ACTION_KEY)],
            )?
            .project(output_expr.clone(), output_schema.clone())?;

        let commit_live_adds = deduped_commit
            .filter(col!("add").is_not_null())?
            .project(output_expr, output_schema)?;

        PlanBuilder::union_all([commit_live_adds, checkpoint_live_adds])?.build_opt()
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
    ///          MAP_TO_STRUCT(add.partitionValues, physical_partitions) AS partitionValues_parsed
    ///        ) AS add,
    ///        version, add.path IS NOT NULL AS is_add, file_key(add) AS key
    /// FROM checkpoint_actions
    /// WHERE add.path IS NOT NULL
    ///
    /// When the checkpoint lacks native parsed stats, `FROM_JSON(add.stats, physical_stats)`
    /// replaces `add.stats_parsed` above. A parsed field is omitted when its schema is absent.
    fn checkpoint_arm(&self, shape: &CheckpointShape) -> DeltaResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let physical_stats = self.state_info.stats_schema_for_transform();
        let physical_partitions = self.state_info.physical_partition_schema.as_ref();
        let source_physical_stats = shape.parsed_stats_schema.as_ref();
        let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;

        let actions = match (&shape.checkpoint_type, checkpoint) {
            (CheckpointType::Leaf, Some((FileType::Parquet, parts))) => {
                let schema = parquet_read_schema(source_physical_stats, None, self.reads_stats())?;
                PlanBuilder::scan_parquet(parts, &[VERSION], schema)
            }
            (CheckpointType::Leaf, Some((FileType::Json, parts))) => {
                PlanBuilder::scan_json(
                    parts,
                    &[VERSION],
                    json_read_schema(/* include_remove */ false, self.reads_stats()),
                )
            }
            (CheckpointType::Manifest, Some((file_type, parts))) => {
                let schema = parquet_read_schema(source_physical_stats, None, self.reads_stats())?;
                match log_segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
                    Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema),
                    // Without a complete hint, load the sidecars referenced by the manifest.
                    None => sidecar_actions(file_type, parts, schema, &log_segment.log_root),
                }
            }
            (CheckpointType::None, _) | (_, None) => {
                PlanBuilder::values(
                    json_read_schema(/* include_remove */ false, self.reads_stats()),
                    vec![],
                )
            }
        }?;

        actions
            .filter(col!("add.path").is_not_null())?
            .project_patch(|patch| {
                patch
                    .with_synthesized_add_json_stats(self.stats.emit_json)
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
            })
    }

    /// Build the normalized commit JSON arm.
    ///
    /// ## SQL equivalent:
    ///
    /// SELECT STRUCT(
    ///          add.* EXCEPT (stats_parsed, partitionValues_parsed),
    ///          FROM_JSON(add.stats, physical_stats) AS stats_parsed,
    ///          MAP_TO_STRUCT(add.partitionValues, physical_partitions) AS partitionValues_parsed
    ///        ) AS add,
    ///        remove, version, add.path IS NOT NULL AS is_add,
    ///        file_key(COALESCE(add, remove)) AS key
    /// FROM json_commits
    /// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
    ///
    /// A parsed field is omitted when its schema is absent.
    fn commit_arm(&self) -> DeltaResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let commit_files = log_segment.commit_cover_version_tagged_scan_files()?;
        PlanBuilder::scan_json(
            commit_files,
            &[VERSION],
            json_read_schema(true, self.reads_stats()),
        )?
        .filter(Predicate::or(
            col!("add.path").is_not_null(),
            col!("remove.path").is_not_null(),
        ))?
        .project_patch(|patch| {
            // Commits never carry source-native parsed columns, so normalize from the raw
            // encodings.
            patch
                .with_parsed_add_stats(self.state_info.stats_schema_for_transform())
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
        })
    }

    fn normalized_add_field(&self) -> DeltaResult<StructField> {
        let physical_stats_schema = self.state_info.stats_schema_for_transform();
        let physical_partition_schema = self.state_info.physical_partition_schema.as_ref();
        let add_schema = add_read_schema(self.reads_stats());
        let patch = SchemaStructPatchBuilder::new()
            .fold_with(physical_stats_schema, |patch, schema| {
                patch.append(StructField::nullable(STATS_PARSED, schema.as_ref().clone()))
            })
            .fold_with(physical_partition_schema, |patch, schema| {
                patch.append(StructField::nullable(
                    PARTITION_VALUES_PARSED,
                    schema.as_ref().clone(),
                ))
            });
        Ok(StructField::nullable(ADD_NAME, patch.build(&add_schema)?))
    }

    /// Builds the output projection for requested stats and partition values. The base of this
    /// transformation is constructed by [`Self::normalized_add_field`].
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
    /// needed only for pruning are omitted.
    fn metadata_output_projection(
        &self,
        add_field: &StructField,
    ) -> DeltaResult<(ExpressionRef, SchemaRef)> {
        let input_schema = schema_ref! { (add_field.clone()) };
        let has_stats_parsed = input_schema.contains_col([ADD_NAME, STATS_PARSED_NAME]);
        let projection = ProjectionStructPatchBuilder::new_nested(&input_schema, [ADD_NAME]);

        // JSON stats output. `StatsOptions` allows JSON only, parsed only, both, or neither.
        let has_json_stats = input_schema.contains_col([ADD_NAME, STATS]);
        let projection = match (self.stats.emit_json, has_json_stats) {
            (true, true) | (false, false) => projection,
            (true, false) => {
                return Err(Error::internal_error(
                    "JSON stats were requested, but add.stats is missing from the metadata schema",
                ));
            }
            (false, true) => projection.drop(STATS),
        };

        // Parsed stats output.
        let projection = match (
            self.state_info.output_stats_schema.as_ref(),
            has_stats_parsed,
        ) {
            (Some(physical_stats), _) => projection.replace(
                STATS_PARSED,
                StructField::nullable(STATS_PARSED, physical_stats.as_ref().clone()),
                project_struct_to_schema([ADD_NAME, STATS_PARSED_NAME], physical_stats),
            ),
            (None, true) => projection.drop(STATS_PARSED),
            (None, false) => projection,
        };

        // Parsed partition-values output.
        let has_partition_values_parsed =
            input_schema.contains_col([ADD_NAME, PARTITION_VALUES_PARSED_NAME]);
        let physical_partitions = self
            .partition_values
            .parsed_struct
            .then_some(self.state_info.physical_partition_schema.as_ref())
            .flatten();
        let projection = match (physical_partitions, has_partition_values_parsed) {
            (Some(schema), true) => projection.replace(
                PARTITION_VALUES_PARSED,
                StructField::nullable(PARTITION_VALUES_PARSED, schema.as_ref().clone()),
                project_struct_to_schema([ADD_NAME, PARTITION_VALUES_PARSED_NAME], schema),
            ),
            (Some(_), false) => {
                return Err(Error::internal_error(
                    "parsed partition values were requested, but add.partitionValues_parsed is \
                     missing",
                ));
            }
            (None, true) => projection.drop(PARTITION_VALUES_PARSED),
            (None, false) => projection,
        };

        let (add_schema, add_expr) = projection.build()?;
        let schema = schema_ref! { nullable ADD_NAME: (add_schema.as_ref().clone()) };
        Ok((Arc::new(Expr::struct_from([add_expr])), schema))
    }
}

/// Read actions from V2 checkpoint sidecars.
fn sidecar_actions(
    file_type: FileType,
    root_parts: Vec<ScanFile>,
    action_schema: SchemaRef,
    log_root: &Url,
) -> DeltaResult<PlanBuilder> {
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

fn add_read_schema(include_json_stats: bool) -> StructType {
    if include_json_stats {
        ADD_SCHEMA.clone()
    } else {
        ADD_SCHEMA_NO_JSON_STATS.clone()
    }
}

/// Read schema for JSON actions tagged with their log version.
/// Commits include removes; JSON checkpoint leaves do not.
fn json_read_schema(include_remove: bool, include_json_stats: bool) -> SchemaRef {
    let add_schema = add_read_schema(include_json_stats);
    schema_ref! {
        nullable ADD_NAME: (add_schema),
        ..(include_remove.then_some(&REMOVE_FIELD)),
        nullable VERSION: LONG,
    }
}

/// Read schema for parquet add actions.
fn parquet_read_schema(
    physical_stats: Option<&SchemaRef>,
    physical_partitions: Option<&SchemaRef>,
    include_json_stats: bool,
) -> DeltaResult<SchemaRef> {
    let add_schema = add_read_schema(include_json_stats);
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
        nullable ADD_NAME: (add_patch.build(&add_schema)?),
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
    /// Synthesizes JSON stats from a checkpoint's native parsed stats when needed.
    fn with_synthesized_add_json_stats(self, emit_json: bool) -> Self;

    /// Parses add stats, preferring a compatible parsed field.
    ///
    /// When `physical_stats` is present, the input must contain either
    /// `add.stats_parsed` or the fallback `add.stats` JSON field. When absent, any native
    /// `add.stats_parsed` field is removed from the normalized row.
    fn with_parsed_add_stats(self, physical_stats: Option<&SchemaRef>) -> Self;

    /// Parses add partition values, preferring a compatible parsed field.
    fn with_parsed_add_partition_values(self, physical_partitions: Option<&SchemaRef>) -> Self;
}

impl<'a> ProjectionStructPatchBuilderExt<'a> for ProjectionStructPatchBuilder<'a> {
    fn with_synthesized_add_json_stats(self, emit_json: bool) -> Self {
        if emit_json
            && self
                .input_schema()
                .contains_col([ADD_NAME, STATS_PARSED_NAME])
        {
            let stats = Expr::coalesce([
                col!(ADD_NAME, STATS),
                Expr::unary(UnaryExpressionOp::ToJson, col!(ADD_NAME, STATS_PARSED)),
            ]);
            self.replace_expr_at([ADD_NAME], STATS, stats)
        } else {
            self
        }
    }

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
            None if has_stats_parsed => self.drop_at(add, STATS_PARSED),
            None => self,
        }
    }

    fn with_parsed_add_partition_values(self, physical_partitions: Option<&SchemaRef>) -> Self {
        let has_partition_values_parsed = self
            .input_schema()
            .contains_col([ADD_NAME, PARTITION_VALUES_PARSED_NAME]);
        let add = [ADD_NAME];
        match physical_partitions {
            Some(schema) => {
                let field = StructField::nullable(PARTITION_VALUES_PARSED, schema.as_ref().clone());
                let expr = Expr::map_to_struct(col!(ADD_NAME, PARTITION_VALUES));
                if has_partition_values_parsed {
                    let expr = Expr::coalesce([col!(ADD_NAME, PARTITION_VALUES_PARSED), expr]);
                    self.replace_at(add, PARTITION_VALUES_PARSED, field, expr)
                } else {
                    self.append_at(add, field, expr)
                }
            }
            None => self,
        }
    }
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
        &state.physical_stats_columns,
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
    use crate::Engine as _;

    fn mock_snapshot(log_segment: LogSegment) -> DeltaResult<Arc<Snapshot>> {
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
        CheckpointShape {
            checkpoint_type,
            parsed_stats_schema: parsed_stats,
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
        let segment = log_segment(log_root(), &[], None);
        mock_snapshot(segment)
            .unwrap()
            .scan_builder()
            .with_predicate(Arc::new(col!("x").gt(lit(5i64))))
            .with_stats(StatsOptions::all())
            .build()
            .unwrap()
            .state_info
            .physical_stats_schema
            .clone()
            .expect("stats schema")
    }

    // Commit-arm operator sequence without optional pruning.
    const COMMIT_ARM_TAGS: &[&str] = &[
        "scan_json", // commits
        "filter",    // keep file actions
        "project",   // normalize
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
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project", "semi_join", "project"])]
    fn metadata_plan_checkpoint_arm_shape(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> DeltaResult<()> {
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
    #[case::with_parsed_stats(Some(struct_stats_schema()), true)]
    #[case::without_parsed_stats(None, false)]
    fn metadata_plan_manifest_sidecar_dynamic_scan_stats_columns(
        #[case] parsed_stats: Option<SchemaRef>,
        #[case] expect_parsed_columns: bool,
    ) -> DeltaResult<()> {
        let stats = StatsOptions::all();
        let partition_values = PartitionValuesOptions::with_struct();
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_partition_values(partition_values)
            .build()?;
        let plan = scan
            .build_metadata_scan_plan(&shape(CheckpointType::Manifest, parsed_stats))?
            .expect("non-empty");

        let dynamic_scan = plan
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Operator::DynamicScan(dynamic_scan) => Some(dynamic_scan),
                _ => None,
            })
            .expect("sidecar dynamic scan");
        assert_eq!(
            add_struct(&dynamic_scan.schema)
                .field(STATS_PARSED)
                .is_some(),
            expect_parsed_columns,
        );
        assert!(
            add_struct(&dynamic_scan.schema)
                .field(PARTITION_VALUES_PARSED)
                .is_none(),
            "native parsed partition values are not requested yet"
        );
        assert!(
            dynamic_scan.dv_column.is_none(),
            "sidecar scan sets no dv column"
        );
        Ok(())
    }

    #[test]
    fn metadata_plan_commits_only() -> DeltaResult<()> {
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
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project", "project"])]
    fn metadata_plan_checkpoint_only(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> DeltaResult<()> {
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(file_type)));
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan.build_metadata_scan_plan(&shape)?.expect("non-empty");
        assert_eq!(tags(&plan), checkpoint_arm_tags);
        Ok(())
    }

    #[test]
    fn metadata_plan_empty_is_none() -> DeltaResult<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        assert!(scan.build_metadata_scan_plan(&no_checkpoint())?.is_none());
        Ok(())
    }

    #[test]
    fn metadata_plan_static_skip_all_is_none() -> DeltaResult<()> {
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
    ) -> DeltaResult<()> {
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
            Ok::<_, crate::Error>(rows + batch?.try_into_record_batch()?.num_rows())
        })?;
        assert_eq!(actual_rows, expected_rows);
        Ok(())
    }
}
