//! Declarative log-scan plans (prototype, behind `declarative-plans`).
//!
//! [`build_metadata_scan_plan`] replaces the row-oriented imperative scan replay
//! ([`ScanLogReplayProcessor`]/[`AddRemoveDedupVisitor`] in [`super::log_replay`]) with a
//! declarative [`Plan`]: it reconciles the log (checkpoint + commits) into the set of *live* add
//! files, applying data-skipping / partition pruning as a plan [`Filter`] rather than the
//! imperative `DataSkippingFilter`, and deduplicating via an [`Aggregate`] "newest action wins"
//! instead of the add/remove dedup visitor.
//!
//! The builder takes a [`StateInfo`] -- the single kernel-owned description of the scan (logical /
//! physical schemas, physical predicate, stats / partition schemas, transform spec, column mapping)
//! -- and derives everything it needs from it. The only other inputs are snapshot-derived: the log
//! files for the metadata scan.
//!
//! This first cut parses stats/partition values from the **add** side only, so removes are never
//! pruned (safe -- the `add IS NULL` guard keeps every tombstone for anti-join).
//!
//! [`ScanLogReplayProcessor`]: super::log_replay::ScanLogReplayProcessor
//! [`AddRemoveDedupVisitor`]: super::log_replay::AddRemoveDedupVisitor
//! [`Filter`]: crate::plans::ir::nodes::Filter

// The plan builders are exercised by unit tests but not yet called from `Scan` (that wiring is a
// later phase). Allow dead code until then so the unwired prototype builds under `-D warnings`.
#![allow(dead_code)]

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
use crate::plans::ir::nodes::{FileType, Load, LoadColumnFileMeta, ScanFile};
use crate::plans::ir::plan::Plan;
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
// materialize them as one top-level "file action key" column so the plan's aggregate and anti-join
// operators can correctly pair up adds with removes.
const FILE_ACTION_KEY: &str = "file_action_key";
/// The `add` action sub-fields reparsed in place from their raw log encodings: `stats` (JSON string
/// -> typed stats struct, which pruning points at via `add.stats.minValues.*` etc.) and
/// `partitionValues` (string map -> typed partition struct).
const STATS: &str = "stats";
const PARTITION_VALUES: &str = "partitionValues";
const PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
const IS_ADD: &str = "is_add";
/// Per-file version, carried as a file-constant column so the aggregate can pick the newest action.
const VERSION: &str = "version";

// === Load-friendly source columns ===
// Column names a [`Load`] reads from its `source` relation: the file locator (path), size and
// record count (for splitting / row counts), and the deletion-vector descriptor. Used here by the
// checkpoint arm's sidecar [`Load`].
const FILE_PATH: &str = "path";
const FILE_SIZE: &str = "size";
const NUM_RECORDS: &str = "num_records";
const DV: &str = "dv";

/// The `sidecar` action's byte-size sub-field. Distinct from the [`FILE_SIZE`] output column: the
/// sidecar action names it `sizeInBytes` (see [`Sidecar`](crate::actions::Sidecar)).
const SIDECAR_SIZE_IN_BYTES: &str = "sizeInBytes";

/// The version-tagged read schema for a log file set: `add` (+ `remove` for commits) plus the
/// file-constant `version` column.
fn json_read_schema(include_remove: bool) -> SchemaRef {
    schema_ref! {
        (&ADD_FIELD),
        ..(include_remove.then_some(&REMOVE_FIELD)),
        nullable (VERSION): LONG,
    }
}

/// The checkpoint root read schema, used only to discover V2 sidecar file references.
fn sidecar_read_schema() -> SchemaRef {
    schema_ref! {
        (&SIDECAR_FIELD),
        nullable (VERSION): LONG,
    }
}

static SIDECAR_FILE_META_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable (FILE_PATH): STRING,
    nullable (FILE_SIZE): LONG,
    nullable (NUM_RECORDS): LONG,
    nullable (DV): (DeletionVectorDescriptor::to_schema()),
    nullable (VERSION): LONG,
};

/// Like [`json_read_schema`], but includes struct stats and partition values columns.
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
        nullable (VERSION): LONG,
    })
}

/// The subset of file action fields that uniquely identifies a logical file in the log, used for
/// deduplication of adds and removes during log replay.
///
/// Mirrors the canonical `deletionVector` shape: the struct is nullable (a file may have no
/// deletion vector), but when present its `storageType` / `pathOrInlineDv` are non-null. A DV-less
/// file action yields a *null* `deletionVector` struct, not one with null leaves (see
/// [`file_action_key_expr`]).
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

/// Build a struct-valued file action key from leaf expressions which differ per arm: coalesced
/// across add/remove for commits, add-only for checkpoints.
///
/// The `deletionVector` sub-struct is null for a file with no deletion vector (guarded on
/// `storageType`, which is non-null exactly when a DV is present), so its non-null `storageType` /
/// `pathOrInlineDv` leaves are only materialized when the struct itself is present.
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
    /// The initial JSON parse of a file action is incomplete: `stats` is a string containing a JSON
    /// literal representing the actual stats, and `partitionValues` is a string-string map. Convert
    /// both in-place to fully parsed structs. When the source carries the already-parsed
    /// counterparts (`stats_parsed` / `partitionValues_parsed`, stored in parquet checkpoints),
    /// prefer them: `read_stats_parsed` / `read_partition_values_parsed` say which are present in
    /// the source schema and should be coalesced ahead of the reparsed fallback.
    ///
    /// `read_stats_parsed` is gated on the resolved checkpoint shape (a parquet source only has a
    /// usable `stats_parsed` when the checkpoint actually carries a compatible one). Native parsed
    /// partition values are deferred until the shape reports their compatibility.
    fn reparse_add(
        self,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<&SchemaRef>,
        read_stats_parsed: bool,
        read_partition_values_parsed: bool,
    ) -> Self;
}

impl<'a> ProjectionStructPatchBuilderExt<'a> for ProjectionStructPatchBuilder<'a> {
    fn reparse_add(
        mut self,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<&SchemaRef>,
        read_stats_parsed: bool,
        read_partition_values_parsed: bool,
    ) -> Self {
        let add = [ADD_NAME];
        self = match stats_schema {
            Some(ss) => {
                let field = StructField::nullable(STATS, ss.as_ref().clone());
                let expr = Expr::parse_json(col!("add.stats"), Arc::clone(ss));
                if read_stats_parsed {
                    let expr = Expr::coalesce([col!(ADD_NAME, STATS_PARSED), expr]);
                    self.replace_at(add, STATS, field, expr)
                        .drop_at(add, STATS_PARSED)
                } else {
                    self.replace_at(add, STATS, field, expr)
                }
            }
            None => self.replace_expr_at(add, STATS, Expr::null_literal(DataType::STRING)),
        };
        match partition_schema {
            Some(ps) => {
                let field = StructField::nullable(PARTITION_VALUES, ps.as_ref().clone());
                let expr = Expr::map_to_struct(col!("add.partitionValues"));
                if read_partition_values_parsed {
                    let expr = Expr::coalesce([col!(ADD_NAME, PARTITION_VALUES_PARSED), expr]);
                    self.replace_at(add, PARTITION_VALUES, field, expr)
                        .drop_at(add, PARTITION_VALUES_PARSED)
                } else {
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

/// The canonical `add.partitionValues` log type: a `map<string, string>` whose values may be null
/// (mirroring [`Add::partition_values`](crate::actions::Add) / `#[allow_null_container_values]`).
fn partition_values_map_type() -> DataType {
    MapType::new(DataType::STRING, DataType::STRING, true).into()
}

/// The output `add` field produced by [`reparse_add`]: the canonical add schema with `stats` /
/// `partitionValues` retyped to their parsed forms (or left canonical when the schema is absent).
/// Kept in lockstep with [`reparse_add`] so the downstream dedup carrier and terminal `{ add }`
/// projections declare exactly the schema the reparse projection emits.
fn reparsed_add_field(
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<StructField> {
    // `partitionValues` is non-null in the canonical add schema, but `reparse_add` always retypes
    // it to a nullable field (parsed struct when a partition schema exists, null literal
    // otherwise).
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

/// Read the checkpoint root parts for their `sidecar` references, then [`Load`] the referenced
/// sidecar files as parquet file actions. `root_parts` share one [`FileType`] (a checkpoint version
/// is homogeneous), so the root is scanned once with the matching operator. `action_schema` is the
/// sidecar read schema; it must match what the sidecar files physically carry (see
/// [`build_metadata_scan_plan`]), so callers derive it from the resolved checkpoint shape.
fn sidecar_actions(
    file_type: FileType,
    root_parts: Vec<ScanFile>,
    action_schema: SchemaRef,
    log_root: &Url,
) -> DeltaResult<PlanBuilder> {
    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    let sidecar_files = scan(root_parts, &[VERSION], sidecar_read_schema())?
        .filter(col!(SIDECAR_NAME).is_not_null())?
        .project(
            Expr::struct_from([
                col!(SIDECAR_NAME, FILE_PATH),
                col!(SIDECAR_NAME, SIDECAR_SIZE_IN_BYTES),
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

/// Build the raw checkpoint file-action arm, driven by the resolved [`CheckpointShape`]. Returns
/// `{ add, version }` rows, including compatible source-native parsed columns requested by the
/// caller. When there is no checkpoint, returns an absent relation with the raw action schema.
///
/// A checkpoint version is homogeneous, so `checkpoint` carries a single [`FileType`] and one part
/// list. `shape` selects which relation actually holds the file actions:
/// - [`CheckpointType::Leaf`]: actions are inline in the root parts (classic V1, multi-part, inline
///   V2), read directly.
/// - [`CheckpointType::Manifest`]: the root parts hold only sidecar references; the referenced
///   sidecar files (always parquet) hold the actions.
///
/// The stats schemas used by the caller play two different roles:
/// - The scan's [`StateInfo::physical_stats_schema`] is the downstream reparse target.
/// - `shape.parsed_stats_schema` is what the checkpoint *source* actually carries. When `Some`, the
///   read schema includes an `add.stats_parsed` column for downstream normalization. It also drives
///   the sidecar [`Load`]'s read schema, so the Load never references a column the sidecars lack.
fn checkpoint_arm(
    checkpoint: Option<(FileType, Vec<ScanFile>)>,
    shape: &CheckpointShape,
    log_root: &Url,
    source_partition_schema: Option<&SchemaRef>,
) -> DeltaResult<PlanBuilder> {
    let source_stats_schema = shape.parsed_stats_schema.as_ref();

    match (&shape.checkpoint_type, checkpoint) {
        (CheckpointType::Leaf, Some((FileType::Parquet, parts))) => {
            let schema = parquet_read_schema(source_stats_schema, source_partition_schema)?;
            PlanBuilder::scan_parquet(parts, &[VERSION], schema)
        }
        (CheckpointType::Leaf, Some((FileType::Json, parts))) => {
            PlanBuilder::scan_json(parts, &[VERSION], json_read_schema(false))
        }
        (CheckpointType::Manifest, Some((file_type, parts))) => {
            // The root parts only reference sidecars; the sidecar files (always parquet) hold the
            // actions. The Load read schema must match what the sidecars carry, hence the
            // source-side schema.
            let schema = parquet_read_schema(source_stats_schema, source_partition_schema)?;
            sidecar_actions(file_type, parts, schema, log_root)
        }
        (CheckpointType::None, _) | (_, None) => {
            PlanBuilder::values(json_read_schema(false), vec![])
        }
    }
}

/// The plan-level pruning filter derived from the scan's physical predicate, re-rooted under the
/// reparsed per-file stats and partition structs.
///
/// Returns `None` (keep every file) when there is no predicate or the predicate is useless for
/// skipping. [`PhysicalPredicate::StaticSkipAll`] is handled by the caller (the whole plan
/// collapses to empty).
fn stats_skipping_predicate(state: &StateInfo) -> Option<Predicate> {
    let PhysicalPredicate::Some(pred, _) = &state.physical_predicate else {
        return None;
    };
    let partition_column_names = state
        .physical_partition_schema
        .iter()
        .flat_map(|s| s.fields().map(|f| f.name().to_string()))
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

/// Maps normalized skipping references onto the reparsed Add action.
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

/// Build the declarative metadata scan plan: the set of live `{ add }` rows for the table state,
/// with stats-based pruning derived from `state`'s physical predicate applied as a plan [`Filter`].
///
/// `state` supplies the parsed-stats schema ([`StateInfo::physical_stats_schema`]), the typed
/// partition schema ([`StateInfo::physical_partition_schema`]), and the predicate the pruning
/// filter is rewritten from. `commit_files` are version-tagged [`ScanFile`]s -- typically straight
/// from the log segment (newest commit wins). `checkpoint` is the version-tagged checkpoint parts
/// paired with their shared [`FileType`] (from
/// [`LogSegment::checkpoint_version_tagged_scan_files`]), or `None` when there is no checkpoint.
/// `shape` is the resolved [`CheckpointShape`]: it selects the leaf-vs-manifest checkpoint arm and
/// says whether the checkpoint carries a compatible `add.stats_parsed`. `log_root` is the
/// `_delta_log/` URL used to resolve V2 sidecar paths.
///
/// Returns `None` when nothing can survive: an empty table (no files after absence collapse) or a
/// statically-unsatisfiable predicate ([`PhysicalPredicate::StaticSkipAll`]).
///
/// [`LogSegment::checkpoint_version_tagged_scan_files`]: crate::log_segment::LogSegment::checkpoint_version_tagged_scan_files
pub(crate) fn build_metadata_scan_plan(
    state: &StateInfo,
    commit_files: Vec<ScanFile>,
    checkpoint: Option<(FileType, Vec<ScanFile>)>,
    shape: &CheckpointShape,
    log_root: Url,
) -> DeltaResult<Option<Plan>> {
    // A statically-unsatisfiable predicate (e.g. `x > 10 AND FALSE`) skips the whole table.
    if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
        return Ok(None);
    }

    let stats_schema = state.physical_stats_schema.as_ref();
    let partition_schema = state.physical_partition_schema.as_ref();
    // Native checkpoint partition values are deferred until CheckpointShape reports their schema.
    // Normalize from the canonical string map in every arm for now.
    let source_partition_schema: Option<&SchemaRef> = None;
    let prune = stats_skipping_predicate(state);
    let prune = prune.as_ref();

    // The output `add` after reparsing `stats`/`partitionValues`: shared by the commit arm's dedup
    // carrier and both terminal `{ add }` projections, so every arm agrees on the union schema.
    let add_field = reparsed_add_field(stats_schema, partition_schema)?;
    let output_schema = schema_ref! { (&add_field) };

    // Dedup the commit rows to the newest action per file identity. Wrap `add` in a never-null
    // struct so `max_non_null_by` still observes remove rows, then unwrap it afterward.
    let deduped_commit = PlanBuilder::scan_json(commit_files, &[VERSION], json_read_schema(true))?
        .filter(Predicate::or(
            col!("add.path").is_not_null(),
            col!("remove.path").is_not_null(),
        ))?
        .project_patch(|patch| {
            // Commits never carry the source-native parsed columns, so normalize from the raw
            // encodings.
            patch
                .reparse_add(stats_schema, partition_schema, false, false)
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
            // Only prune adds: remove.partitionValues is optional and partition pruning predicates
            // cannot safely distinguish between "missing" (never prune) and "null" (maybe prune).
            p.filter(Predicate::or(col!("add").is_null(), prune.clone()))
        })?
        .project_patch(|patch| {
            patch.replace(
                ADD_NAME,
                StructField::not_null(ADD_NAME, schema! { (add_field.clone()) }),
                Expr::struct_from([col!("add")]),
            )
        })?
        .aggregate_by([ColumnName::new([FILE_ACTION_KEY])], |a| {
            a.max_non_null_by(ColumnName::new([ADD_NAME]), ColumnName::new([VERSION]))
        })?
        .project_patch(|patch| patch.replace(ADD_NAME, add_field.clone(), col!("add.add")))?;

    // Live checkpoint adds: Commit content supersedes checkpoint content, so anti-join away every
    // checkpoint identity a commit also touched, then project out the `{ add }`. With no
    // checkpoint, the raw arm is absent and these transformations remain absent.
    let checkpoint_live_adds =
        checkpoint_arm(checkpoint, shape, &log_root, source_partition_schema)?
            .filter(col!("add.path").is_not_null())?
            .project_patch(|patch| {
                patch
                    .reparse_add(
                        stats_schema,
                        partition_schema,
                        shape.parsed_stats_schema.is_some(),
                        source_partition_schema.is_some(),
                    )
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
            .anti_join(
                deduped_commit.clone(),
                [ColumnName::new([FILE_ACTION_KEY])],
                [ColumnName::new([FILE_ACTION_KEY])],
            )?
            .project(Expr::struct_from([col!("add")]), output_schema.clone())?;

    let commit_live_adds = deduped_commit
        .filter(col!("add").is_not_null())?
        .project(Expr::struct_from([col!("add")]), output_schema)?;

    // NOTE: Both present/absent combinations of commit and checkpoint are possible, depending on
    // e.g. whether a checkpoint exists and what got pruned away.
    PlanBuilder::union_all([commit_live_adds, checkpoint_live_adds])?.build_opt()
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
    use crate::expressions::{lit, Scalar};
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::Operation as PlanOperation;
    use crate::scan::state_info::tests::get_state_info_with_options;
    use crate::scan::{PartitionValuesOptions, StatsOptions};
    use crate::schema::StructType;
    use crate::{Engine as _, FileMeta};

    /// Build a [`StateInfo`] for the plan tests: a legacy-protocol table with the given schema,
    /// partition columns, optional predicate, and stats / partition-value options.
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

    /// A `{ x: LONG }` data schema (no partitions).
    fn data_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "x",
            DataType::LONG,
        )]))
    }

    /// A `{ x: LONG, p: STRING }` schema where `p` is a partition column.
    fn partitioned_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::nullable("x", DataType::LONG),
            StructField::nullable("p", DataType::STRING),
        ]))
    }

    fn file(path: &str) -> FileMeta {
        FileMeta {
            location: Url::parse(path).unwrap(),
            last_modified: 0,
            size: 0,
        }
    }

    /// A version-tagged scan file, mirroring what the log segment hands the plan builders.
    fn scan_file(path: &str, version: i64) -> ScanFile {
        ScanFile {
            meta: file(path),
            file_constants: vec![Scalar::from(version)],
        }
    }

    fn log_root() -> Url {
        Url::parse("file:///_delta_log/").unwrap()
    }

    /// A resolved [`CheckpointShape`] for a plan-shape test. `parsed_stats` is the checkpoint's
    /// source-side parsed-stats schema (`Some` => the checkpoint carries a compatible
    /// `stats_parsed` and the plan reads/coalesces it).
    fn shape(checkpoint_type: CheckpointType, parsed_stats: Option<SchemaRef>) -> CheckpointShape {
        CheckpointShape {
            checkpoint_type,
            parsed_stats_schema: parsed_stats,
        }
    }

    /// The `None` shape: no checkpoint.
    fn no_checkpoint() -> CheckpointShape {
        shape(CheckpointType::None, None)
    }

    /// A discriminant tag for asserting a node's operator (mirrors the builder's own test helper).
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

    /// A single checkpoint part of the given format.
    fn checkpoint_part(file_type: FileType) -> Option<(FileType, Vec<ScanFile>)> {
        let path = match file_type {
            FileType::Json => "file:///0.checkpoint.json",
            FileType::Parquet => "file:///0.parquet",
        };
        checkpoint_part_at(file_type, path)
    }

    /// A single checkpoint part of the given format at a specific URL.
    fn checkpoint_part_at(file_type: FileType, path: &str) -> Option<(FileType, Vec<ScanFile>)> {
        Some((file_type, vec![scan_file(path, 0)]))
    }

    /// Write a minimal single-row parquet checkpoint to `store` at `path`: one `add` with a `path`
    /// and a JSON `stats` string (`x` min/max = 10), plus a `version` column, and NO `stats_parsed`
    /// column. The reader null-fills the remaining canonical `add` fields. Used to exercise the
    /// no-parsed-stats leaf path (stats parsed from the `add.stats` string).
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

    /// A minimal parsed-stats schema (`numRecords` plus min/max for `x`), matching what a parquet
    /// checkpoint's `add.stats_parsed` would carry for the [`data_schema`] table.
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

    /// The commit-arm prefix shared by every plan that has commits: scan the commits, normalize,
    /// (optionally prune), wrap for dedup, aggregate newest-per-key, unwrap, then extract the live
    /// commit adds.
    const COMMIT_ARM_TAGS: &[&str] = &[
        "scan_json", // commits
        "filter",    // keep file actions
        "project",   // normalize
        "project",   // wrap add for dedup
        "aggregate", // newest-action-per-key
        "project",   // unwrap newest add
        "filter",    // live commit adds
        "project",   // extract add
    ];

    /// A leaf checkpoint arm reads its inline actions directly from the root parts: no sidecar
    /// `load`. A manifest arm reads only sidecar refs from the root then `load`s the sidecar files.
    /// Neither builds the old always-both-arms union.
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
        let plan = build_metadata_scan_plan(
            &state,
            vec![scan_file("file:///1.json", 1)],
            checkpoint_part(file_type),
            &shape,
            log_root(),
        )?
        .expect("non-empty");

        let mut expected: Vec<&str> = COMMIT_ARM_TAGS.to_vec();
        expected.extend(checkpoint_arm_tags);
        expected.push("union_all"); // terminal
        assert_eq!(tags(&plan), expected);
        Ok(())
    }

    /// A manifest sidecar `load` reads `stats_parsed` only when the checkpoint shape reports a
    /// compatible schema. Native `partitionValues_parsed` support is deferred.
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
        let plan = build_metadata_scan_plan(
            &state,
            vec![],
            checkpoint_part(FileType::Parquet),
            &shape(CheckpointType::Manifest, parsed_stats),
            log_root(),
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

    /// Commits only (no checkpoint): the plan is just the commit arm, no anti-join, no union.
    #[test]
    fn metadata_plan_commits_only() -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let plan = build_metadata_scan_plan(
            &state,
            vec![scan_file("file:///1.json", 1)],
            None,
            &no_checkpoint(),
            log_root(),
        )?
        .expect("non-empty");
        assert_eq!(tags(&plan), COMMIT_ARM_TAGS.to_vec());
        Ok(())
    }

    /// Checkpoint only (no commits): the commit aggregate is absent, so the anti-join build side is
    /// empty and forwards the checkpoint probe unchanged; the commit-live arm drops out of the
    /// union.
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
        let plan = build_metadata_scan_plan(
            &state,
            vec![],
            checkpoint_part(file_type),
            &shape,
            log_root(),
        )?
        .expect("non-empty");
        assert_eq!(tags(&plan), checkpoint_arm_tags);
        Ok(())
    }

    /// An empty table (no commits, no checkpoint) has no plan to run.
    #[test]
    fn metadata_plan_empty_is_none() -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        assert!(
            build_metadata_scan_plan(&state, vec![], None, &no_checkpoint(), log_root())?.is_none()
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
        let plan = build_metadata_scan_plan(
            &state,
            vec![
                scan_file("memory:///_delta_log/00000000000000000000.json", 0),
                scan_file("memory:///_delta_log/00000000000000000001.json", 1),
            ],
            None,
            &no_checkpoint(),
            Url::parse("memory:///_delta_log/").unwrap(),
        )?
        .expect("non-empty");

        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
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

    /// Correctness gate for the shape wiring: a parquet leaf checkpoint whose file has NO
    /// `stats_parsed` column. Shape reports `parsed_stats_schema = None`, so the plan must parse
    /// `add.stats` from the JSON string rather than reference a non-existent `stats_parsed` column.
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
        let plan = build_metadata_scan_plan(
            &state,
            vec![],
            checkpoint_part_at(
                FileType::Parquet,
                "memory:///_delta_log/00000000000000000000.checkpoint.parquet",
            ),
            // Leaf with no compatible parsed stats -> parse add.stats instead.
            &shape(CheckpointType::Leaf, None),
            Url::parse("memory:///_delta_log/").unwrap(),
        )?
        .expect("non-empty");

        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let actual_rows = batches.try_fold(0, |rows, batch| {
            Ok::<_, crate::Error>(rows + batch?.try_into_record_batch()?.num_rows())
        })?;
        assert_eq!(actual_rows, expected_rows);
        Ok(())
    }
}
