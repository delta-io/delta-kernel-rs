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

use std::sync::{Arc, LazyLock};

use url::Url;

use super::data_skipping::as_checkpoint_skipping_predicate;
use super::state_info::StateInfo;
use super::{PhysicalPredicate, PrefixColumns};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{
    ADD_FIELD, ADD_NAME, ADD_SCHEMA, REMOVE_FIELD, SIDECAR_FIELD, SIDECAR_NAME, STATS_PARSED,
};
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
use crate::transforms::ExpressionTransform;
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
    /// both in-place to fully parsed structs. If available, prefer the already-parsed counterparts
    /// which are stored in parquet checkpoints as `stats_parsed` and `partitionValues_parsed`.
    fn reparse_add(
        self,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<&SchemaRef>,
        is_parquet_source: bool,
    ) -> Self;
}

impl<'a> ProjectionStructPatchBuilderExt<'a> for ProjectionStructPatchBuilder<'a> {
    fn reparse_add(
        mut self,
        stats_schema: Option<&SchemaRef>,
        partition_schema: Option<&SchemaRef>,
        is_parquet_source: bool,
    ) -> Self {
        let add = [ADD_NAME];
        self = match stats_schema {
            Some(ss) => {
                let field = StructField::nullable(STATS, ss.as_ref().clone());
                let expr = Expr::parse_json(col!("add.stats"), Arc::clone(ss));
                if is_parquet_source {
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
                if is_parquet_source {
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

/// Normalize commit file action fields to `{ add, remove, version, file_action_key }`, with parsed
/// stats and partition values; the caller handles pruning and dedup.
fn normalize_commit_actions(
    source: PlanBuilder,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<PlanBuilder> {
    source
        .filter(Predicate::or(
            col!("add.path").is_not_null(),
            col!("remove.path").is_not_null(),
        ))?
        .project_patch(|patch| {
            patch
                .reparse_add(stats_schema, partition_schema, false)
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

/// Normalize checkpoint file actions to `{ add, version, file_action_key }`, with parsed stats and
/// partition values; the caller handles pruning and dedup.
fn normalize_checkpoint_actions(
    source: PlanBuilder,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
    is_parquet_source: bool,
) -> DeltaResult<PlanBuilder> {
    source
        .filter(col!("add.path").is_not_null())?
        .project_patch(|patch| {
            patch
                .reparse_add(stats_schema, partition_schema, is_parquet_source)
                .append(
                    FILE_ACTION_KEY_FIELD.clone(),
                    file_action_key_expr(|col| joined_column_expr!("add", col)),
                )
        })
}

fn sidecar_actions(
    json_parts: Vec<ScanFile>,
    parquet_parts: Vec<ScanFile>,
    action_schema: SchemaRef,
    log_root: &Url,
) -> DeltaResult<PlanBuilder> {
    // Degenerate union: At most one of parquet or json checkpoint root is present
    let sidecar_roots = PlanBuilder::union_all([
        PlanBuilder::scan_json(json_parts, &[VERSION], sidecar_read_schema())?,
        PlanBuilder::scan_parquet(parquet_parts, &[VERSION], sidecar_read_schema())?,
    ])?;
    let sidecar_files = sidecar_roots
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

/// Build the checkpoint arm: scan the checkpoint parts and normalize file action fields. Returns
/// normalized `{ add, file_action_key, ... }` rows -- ready for the caller's pruning, anti-join
/// against the commit keys, and `{ add }` projection (see
/// [`build_metadata_scan_plan`]).
///
/// A checkpoint version is homogeneous (all JSON or all Parquet), so at most one of `json_parts` /
/// `parquet_parts` is non-empty. The root checkpoint is read twice: once as file actions and once
/// as sidecar metadata. The sidecar relation is then loaded as parquet file actions and unioned
/// with the root file actions.
fn checkpoint_arm(
    json_parts: Vec<ScanFile>,
    parquet_parts: Vec<ScanFile>,
    log_root: &Url,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<PlanBuilder> {
    let schema = parquet_read_schema(stats_schema, partition_schema)?;
    // Degenerate union: At most one of parquet or json checkpoint root is present, and it may or
    // may not contain any sidecars.
    PlanBuilder::union_all([
        normalize_checkpoint_actions(
            PlanBuilder::scan_json(json_parts.clone(), &[VERSION], json_read_schema(false))?,
            stats_schema,
            partition_schema,
            false,
        )?,
        normalize_checkpoint_actions(
            PlanBuilder::scan_parquet(parquet_parts.clone(), &[VERSION], Arc::clone(&schema))?,
            stats_schema,
            partition_schema,
            true,
        )?,
        normalize_checkpoint_actions(
            sidecar_actions(json_parts, parquet_parts, schema, log_root)?,
            stats_schema,
            partition_schema,
            true,
        )?,
    ])
}

/// The plan-level pruning filter derived from the scan's physical predicate: the null-guarded
/// checkpoint skipping predicate (see [`as_checkpoint_skipping_predicate`]), re-rooted under
/// `add.stats` to match the reparsed per-file stats struct.
///
/// Returns `None` (keep every file) when there is no parsed-stats column to point at, no predicate,
/// or the predicate is useless for skipping. [`PhysicalPredicate::StaticSkipAll`] is handled by the
/// caller (the whole plan collapses to empty).
fn stats_skipping_predicate(state: &StateInfo) -> Option<Predicate> {
    // Only prune when the plan actually materializes a typed `add.stats` struct to reference.
    state.physical_stats_schema.as_ref()?;
    let PhysicalPredicate::Some(pred, _) = &state.physical_predicate else {
        return None;
    };
    let partition_column_names: Vec<_> = state
        .physical_partition_schema
        .iter()
        .flat_map(|s| s.fields().map(|f| f.name().to_string()))
        .collect();
    let skipping = as_checkpoint_skipping_predicate(
        pred,
        &partition_column_names,
        &state.physical_stats_columns,
    )?;
    let mut prefixer = PrefixColumns {
        prefix: ColumnName::new([ADD_NAME, STATS]),
    };
    Some(prefixer.transform_pred(&skipping).into_owned())
}

/// Build the declarative metadata scan plan: the set of live `{ add }` rows for the table state,
/// with stats-based pruning derived from `state`'s physical predicate applied as a plan [`Filter`].
///
/// `state` supplies the parsed-stats schema ([`StateInfo::physical_stats_schema`]), the typed
/// partition schema ([`StateInfo::physical_partition_schema`]), and the predicate the pruning
/// filter is rewritten from. `commit_files` and the checkpoint parts (`json_checkpoint_files` /
/// `parquet_checkpoint_files`, as partitioned by
/// `LogSegment::checkpoint_version_tagged_scan_files`) are version-tagged [`ScanFile`]s --
/// typically straight from the log segment (newest commit wins). `log_root` is the `_delta_log/`
/// URL used to resolve V2 sidecar paths.
///
/// Returns `None` when nothing can survive: an empty table (no files after absence collapse) or a
/// statically-unsatisfiable predicate ([`PhysicalPredicate::StaticSkipAll`]).
pub(crate) fn build_metadata_scan_plan(
    state: &StateInfo,
    commit_files: Vec<ScanFile>,
    json_checkpoint_files: Vec<ScanFile>,
    parquet_checkpoint_files: Vec<ScanFile>,
    log_root: Url,
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
    let add_field = reparsed_add_field(stats_schema, partition_schema)?;
    let output_schema = schema_ref! { (&add_field) };

    // Dedup the commit rows to the newest action per file identity. Wrap `add` in a never-null
    // struct so `max_non_null_by` still observes remove rows, then unwrap it afterward.
    let deduped_commit = normalize_commit_actions(
        PlanBuilder::scan_json(commit_files, &[VERSION], json_read_schema(true))?,
        stats_schema,
        partition_schema,
    )?
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
    // checkpoint identity a commit also touched, then project out the `{ add }`.
    let checkpoint_live = checkpoint_arm(
        json_checkpoint_files,
        parquet_checkpoint_files,
        &log_root,
        stats_schema,
        partition_schema,
    )?
    .try_fold_with(prune, |p, prune| p.filter(prune.clone()))?
    .anti_join(
        deduped_commit.clone(),
        [ColumnName::new([FILE_ACTION_KEY])],
        [ColumnName::new([FILE_ACTION_KEY])],
    )?
    .project(Expr::struct_from([col!("add")]), output_schema.clone())?;

    // Live commit adds: keep the identities whose newest action is an add
    let commit_live = deduped_commit
        .filter(col!("add").is_not_null())?
        .project(Expr::struct_from([col!("add")]), output_schema)?;

    // NOTE: All four combos of present/absent commit and checkpoint are possible, depending on
    // e.g. whether a checkpoint exists and what got pruned away.
    PlanBuilder::union_all([commit_live, checkpoint_live])?.build_opt()
}

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

    /// The full metadata plan linearizes to the commit arm, the live-commit extraction, and the
    /// checkpoint arm. The checkpoint arm reads root file actions and root sidecar references
    /// separately, loads sidecar parquet actions, unions both checkpoint action sources, and then
    /// anti-joins against the shared commit aggregate.
    #[test]
    fn metadata_plan_has_expected_shape() -> DeltaResult<()> {
        let predicate = col!("x").gt(lit(5i64));
        let state = state(
            partitioned_schema(),
            vec!["p".to_string()],
            Some(predicate),
            StatsOptions::all(),
            PartitionValuesOptions::with_struct(),
        );
        let plan = build_metadata_scan_plan(
            &state,
            vec![scan_file("file:///1.json", 1)],
            vec![],
            vec![scan_file("file:///0.parquet", 0)],
            log_root(),
        )?
        .expect("non-empty");

        // The commit arm (union input 0) linearizes first, then the checkpoint arm; both consume
        // the single shared dedup projection (node 6).
        assert_eq!(
            tags(&plan),
            vec![
                "scan_json",    // 0 commits
                "filter",       // 1 keep file actions
                "project",      // 2 normalize
                "filter",       // 3 prune (guarded)
                "project",      // 4 wrap add
                "aggregate",    // 5 newest-action-per-key (shared)
                "project",      // 6 unwrap newest add
                "filter",       // 7 live commit adds (probe node 6)
                "project",      // 8 extract add
                "scan_parquet", // 9 checkpoint root actions
                "filter",       // 10 keep file actions
                "project",      // 11 normalize
                "scan_parquet", // 12 checkpoint roots for sidecar refs
                "filter",       // 13 keep sidecar rows
                "project",      // 14 sidecar file metadata
                "load",         // 15 sidecar file actions
                "filter",       // 16 keep file actions
                "project",      // 17 normalize sidecar actions
                "union_all",    // 18 root + sidecar checkpoint actions
                "filter",       // 19 prune checkpoint actions
                "semi_join",    // 20 anti-join vs commit keys (build = node 6)
                "project",      // 21 surviving checkpoint adds
                "union_all",    // 22 terminal
            ],
        );

        // The dedup projection (node 6) is emitted once and referenced by both the live-add filter
        // and the anti-join build side.
        assert_eq!(plan.nodes[7].inputs, vec![6]);
        let Operator::Aggregate(aggregate) = &plan.nodes[5].op else {
            panic!("expected aggregate");
        };
        assert_eq!(aggregate.group_by, vec![ColumnName::new([FILE_ACTION_KEY])]);
        assert_eq!(plan.nodes[20].inputs, vec![19, 6]);
        let Operator::SemiJoin(join) = &plan.nodes[20].op else {
            panic!("expected anti-join");
        };
        assert!(join.inverted);
        assert_eq!(join.probe_keys, vec![ColumnName::new([FILE_ACTION_KEY])]);
        assert_eq!(join.build_keys, vec![ColumnName::new([FILE_ACTION_KEY])]);

        let Operator::ScanParquet(root_scan) = &plan.nodes[9].op else {
            panic!("expected parquet checkpoint root action scan");
        };
        assert!(add_struct(&root_scan.schema).field(STATS_PARSED).is_some());
        assert!(add_struct(&root_scan.schema)
            .field(PARTITION_VALUES_PARSED)
            .is_some());

        let Operator::Project(root_normalize) = &plan.nodes[11].op else {
            panic!("expected root normalize project");
        };
        assert!(add_struct(&root_normalize.schema)
            .field(STATS_PARSED)
            .is_none());
        assert!(add_struct(&root_normalize.schema)
            .field(PARTITION_VALUES_PARSED)
            .is_none());

        let Operator::Load(sidecar_load) = &plan.nodes[15].op else {
            panic!("expected sidecar load");
        };
        assert_eq!(
            sidecar_load.base_url.as_ref().map(Url::as_str),
            Some("file:///_delta_log/_sidecars/")
        );
        assert_eq!(sidecar_load.file_constant_columns, vec![VERSION]);
        assert!(add_struct(&sidecar_load.schema)
            .field(STATS_PARSED)
            .is_some());
        assert!(add_struct(&sidecar_load.schema)
            .field(PARTITION_VALUES_PARSED)
            .is_some());
        Ok(())
    }

    /// With no predicate (so no pruning) and no partition columns, both arms filter only on
    /// source-level file-action presence.
    #[test]
    fn metadata_plan_without_pruning_or_partitions() -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::all_struct(),
            PartitionValuesOptions::default(),
        );
        let plan = build_metadata_scan_plan(
            &state,
            vec![scan_file("file:///1.json", 1)],
            vec![],
            vec![scan_file("file:///0.parquet", 0)],
            log_root(),
        )?
        .expect("non-empty");
        assert_eq!(
            tags(&plan),
            vec![
                "scan_json",
                "filter",
                "project",
                "project",
                "aggregate",
                "project",
                "filter",
                "project",
                "scan_parquet",
                "filter",
                "project",
                "scan_parquet",
                "filter",
                "project",
                "load",
                "filter",
                "project",
                "union_all",
                "semi_join",
                "project",
                "union_all",
            ],
        );
        Ok(())
    }

    /// Commits only: the checkpoint arm is absent, so the anti-join forwards nothing and the plan
    /// collapses to the commit arm (union of one present arm forwards it).
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
            vec![],
            vec![],
            log_root(),
        )?
        .expect("non-empty");
        assert_eq!(
            tags(&plan),
            vec![
                "scan_json",
                "filter",
                "project",
                "project",
                "aggregate",
                "project",
                "filter",
                "project"
            ],
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
            vec![],
            vec![],
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

    /// Checkpoint only, in either format: the commit aggregate is absent, so the anti-join forwards
    /// the checkpoint probe unchanged and the commit-live arm drops out of the union. A checkpoint
    /// is homogeneous, so exactly one partition is populated; the lone present scan is forwarded
    /// without a `UnionAll` node.
    #[rstest::rstest]
    #[case::parquet(vec![], vec![scan_file("file:///0.parquet", 0)], "scan_parquet")]
    #[case::json(vec![scan_file("file:///0.checkpoint.json", 0)], vec![], "scan_json")]
    fn metadata_plan_checkpoint_only(
        #[case] json_parts: Vec<ScanFile>,
        #[case] parquet_parts: Vec<ScanFile>,
        #[case] scan_tag: &'static str,
    ) -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let plan = build_metadata_scan_plan(&state, vec![], json_parts, parquet_parts, log_root())?
            .expect("non-empty");
        // No anti-join node (absent build forwarded the probe) and no commit arm. The checkpoint
        // relation still unions root file actions with sidecar file actions.
        assert_eq!(
            tags(&plan),
            vec![
                scan_tag,
                "filter",
                "project",
                scan_tag,
                "filter",
                "project",
                "load",
                "filter",
                "project",
                "union_all",
                "project"
            ]
        );
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
        assert!(build_metadata_scan_plan(&state, vec![], vec![], vec![], log_root())?.is_none());
        Ok(())
    }
}
