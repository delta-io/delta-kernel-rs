//! Declarative log-scan plans (prototype, behind `declarative-plans`).
//!
//! Two builders that together replace the row-oriented imperative scan replay
//! ([`ScanLogReplayProcessor`]/[`AddRemoveDedupVisitor`] in [`super::log_replay`]) with declarative
//! [`Plan`]s:
//!
//! - [`build_metadata_scan_plan`] reconciles the log (checkpoint + commits) into the set of *live*
//!   add files, applying data-skipping / partition pruning as a plan [`Filter`] rather than the
//!   imperative `DataSkippingFilter`, and deduplicating via an [`Aggregate`] "newest action wins"
//!   instead of the add/remove dedup visitor.
//! - [`build_data_scan_plan`] pairs with it: a [`Load`] over the file rows (partition values
//!   broadcast as file-constant columns, deletion vector applied) plus an optional reshape
//!   [`Project`], so callers read data without the legacy per-row `scan_file_transforms`.
//!
//! Both builders take a [`StateInfo`] -- the single kernel-owned description of the scan (logical /
//! physical schemas, physical predicate, stats / partition schemas, transform spec, column mapping)
//! -- and derive everything they need from it. The only other inputs are snapshot-derived: the log
//! files for the metadata scan, and the source relation + table root for the data scan.
//!
//! See `DECLARATIVE_LOG_SCAN_DESIGN.md` at the repo root for the full design, correctness
//! argument (struct-valued max-by, pre-aggregate guarded pruning), and phasing. This first cut
//! parses stats/partition values from the **add** side only, so removes are never pruned (safe --
//! the `add IS NULL` guard keeps every tombstone for anti-join).
//!
//! [`ScanLogReplayProcessor`]: super::log_replay::ScanLogReplayProcessor
//! [`AddRemoveDedupVisitor`]: super::log_replay::AddRemoveDedupVisitor
//! [`Filter`]: crate::plans::ir::nodes::Filter
//! [`Project`]: crate::plans::ir::nodes::Project

// The plan builders are exercised by unit tests but not yet called from `Scan` (that wiring is a
// later phase). Allow dead code until then so the unwired prototype builds under `-D warnings`.
#![allow(dead_code)]

use std::sync::Arc;

use url::Url;

use super::data_skipping::as_checkpoint_skipping_predicate;
use super::state_info::StateInfo;
use super::transform_spec::FieldTransformSpec;
use super::{PhysicalPredicate, PrefixColumns};
use crate::actions::{ADD_FIELD, ADD_NAME, REMOVE_FIELD};
use crate::expressions::{col, ColumnName, Expression, ExpressionStructPatchBuilder, Predicate};
use crate::plans::ir::nodes::{FileType, Load, LoadColumnFileMeta, ScanFile};
use crate::plans::ir::plan::Plan;
use crate::schema::{
    schema_ref, DataType, MapType, SchemaRef, SchemaStructPatchBuilder, StructField,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::transforms::ExpressionTransform;
use crate::utils::FoldWithOption as _;
use crate::{DeltaResult, Error, PlanBuilder};

// === Internal column names ===
// File identity mirrors `FileActionKey`: path + deletion-vector (storageType, pathOrInlineDv,
// offset). Materialized as top-level scaffolding columns so the aggregate can group on them and
// the anti-join can match on them.
const KEY_PATH: &str = "key_path";
const KEY_DV_STORAGE: &str = "key_dv_storage";
const KEY_DV_PATH: &str = "key_dv_path";
const KEY_DV_OFFSET: &str = "key_dv_offset";
/// The `add` action sub-fields reparsed in place from their raw log encodings: `stats` (JSON string
/// -> typed stats struct, which pruning points at via `add.stats.minValues.*` etc.) and
/// `partitionValues` (string map -> typed partition struct).
const STATS: &str = "stats";
const PARTITION_VALUES: &str = "partitionValues";
/// Per-file version, carried as a file-constant column so the aggregate can pick the newest action.
const VERSION: &str = "version";
/// The `struct(add, remove)` carrier that the aggregate takes the newest of.
const PAIR: &str = "pair";

// === Load-friendly source columns ===
// Column names the data plan's [`Load`] reads from its `source` relation: the file locator (path),
// size and record count (for splitting / row counts), and the deletion-vector descriptor. This is
// the convention the metadata->data bridge projection is expected to expose; that bridge is future
// work (the metadata scan currently emits only `{ add }`).
const FILE_PATH: &str = "path";
const FILE_SIZE: &str = "size";
const NUM_RECORDS: &str = "num_records";
const DV: &str = "dv";
const BASE_ROW_ID: &str = "baseRowId";

/// The version-tagged read schema for a log file set: `add` (+ `remove` for commits) plus the
/// file-constant `version` column.
fn read_schema(include_remove: bool) -> SchemaRef {
    schema_ref! {
        (&ADD_FIELD),
        ..(include_remove.then_some(&REMOVE_FIELD)),
        nullable (VERSION): LONG,
    }
}

/// The file-identity keys, mirroring `FileActionKey`: `(output column name, leaf type, trailing
/// path shared by the `add`/`remove` actions)`. Single source of truth for the group-by/join key
/// columns ([`key_columns`]), their schema fields ([`key_fields`]), and the projection that
/// materializes them ([`append_identity_keys`]).
fn identity_keys() -> [(&'static str, DataType, &'static [&'static str]); 4] {
    [
        (KEY_PATH, DataType::STRING, &["path"]),
        (
            KEY_DV_STORAGE,
            DataType::STRING,
            &["deletionVector", "storageType"],
        ),
        (
            KEY_DV_PATH,
            DataType::STRING,
            &["deletionVector", "pathOrInlineDv"],
        ),
        (
            KEY_DV_OFFSET,
            DataType::INTEGER,
            &["deletionVector", "offset"],
        ),
    ]
}

/// The identity (`key_*`) scaffolding fields, all nullable.
fn key_fields() -> Vec<StructField> {
    identity_keys()
        .into_iter()
        .map(|(name, dtype, _)| StructField::nullable(name, dtype))
        .collect()
}

/// The `key_*` group-by / join keys as [`ColumnName`]s.
fn key_columns() -> Vec<ColumnName> {
    identity_keys()
        .iter()
        .map(|(name, ..)| ColumnName::new([*name]))
        .collect()
}

/// `coalesce(add.<a.b>, remove.<a.b>)` where `segments` is the shared trailing path (e.g.
/// `["deletionVector", "offset"]`) -- the identity component that survives whichever side (add or
/// remove) populated the row.
fn coalesced_key(segments: &[&str]) -> Expression {
    let with =
        |root: &str| Expression::column(std::iter::once(root).chain(segments.iter().copied()));
    Expression::coalesce([with("add"), with("remove")])
}

/// Append the identity keys to a projection patch, each produced by `key_expr(segments)` -- which
/// differs per arm: coalesced across add/remove for commits, add-only for checkpoints.
fn append_identity_keys<'a>(
    patch: ProjectionStructPatchBuilder<'a>,
    key_expr: impl Fn(&[&str]) -> Expression,
) -> ProjectionStructPatchBuilder<'a> {
    identity_keys()
        .into_iter()
        .fold(patch, |patch, (name, dtype, segments)| {
            patch.append(StructField::nullable(name, dtype), key_expr(segments))
        })
}

/// Reparse the `add` action's `stats` (JSON string -> typed struct) and `partitionValues` (string
/// map -> typed struct) *in place*, per the scan's schemas -- so pruning and callers see a single
/// typed `add.stats` / `add.partitionValues`, never a second parallel column. v1 reparses the add
/// side only (removes keep their raw encodings, which nothing downstream reads).
///
/// A `None` schema is the corner case where the caller wants neither pruning nor that parsed
/// column: the field is nulled out but keeps its canonical log type ([`DataType::STRING`] for
/// `stats`, a string map for `partitionValues`), matching [`reparsed_add_field`].
fn reparse_add<'a>(
    patch: ProjectionStructPatchBuilder<'a>,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> ProjectionStructPatchBuilder<'a> {
    let add = [ADD_NAME];
    let patch = match stats_schema {
        Some(ss) => patch.replace_at(
            add,
            STATS,
            StructField::nullable(STATS, ss.as_ref().clone()),
            Expression::parse_json(col!("add.stats"), Arc::clone(ss)),
        ),
        None => patch.replace_expr_at(add, STATS, Expression::null_literal(DataType::STRING)),
    };
    match partition_schema {
        Some(ps) => patch.replace_at(
            add,
            PARTITION_VALUES,
            StructField::nullable(PARTITION_VALUES, ps.as_ref().clone()),
            Expression::map_to_struct(col!("add.partitionValues")),
        ),
        None => patch.replace_expr_at(
            add,
            PARTITION_VALUES,
            Expression::null_literal(partition_values_map_type()),
        ),
    }
}

/// The canonical `add.partitionValues` log type: a `map<string, string>` whose values may be null
/// (mirroring [`Add::partition_values`](crate::actions::Add) / `#[allow_null_container_values]`).
fn partition_values_map_type() -> DataType {
    MapType::new(DataType::STRING, DataType::STRING, true).into()
}

/// The output `add` field produced by [`reparse_add`]: the canonical add schema with `stats` /
/// `partitionValues` retyped to their parsed forms (or left canonical when the schema is absent).
/// Kept in lockstep with [`reparse_add`] so the downstream `pair` carrier and terminal `{ add }`
/// projections declare exactly the schema the reparse projection emits.
fn reparsed_add_field(
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<StructField> {
    let DataType::Struct(add_type) = ADD_FIELD.data_type() else {
        return Err(Error::internal_error("ADD_FIELD must be a struct"));
    };
    let patch = SchemaStructPatchBuilder::new()
        .fold_with(stats_schema, |patch, ss| {
            patch.replace(STATS, StructField::nullable(STATS, ss.as_ref().clone()))
        })
        .fold_with(partition_schema, |patch, ps| {
            let field = StructField::nullable(PARTITION_VALUES, ps.as_ref().clone());
            patch.replace(PARTITION_VALUES, field)
        });
    Ok(StructField::nullable(ADD_NAME, patch.build(add_type)?))
}

/// Build the commit arm: scan JSON commits, normalize + prune (guarded), and wrap each surviving
/// row's add/remove into the `pair` carrier. Returns `{ version, key_*, pair }` rows -- ready for
/// the caller's newest-action-per-key aggregate (see [`build_metadata_scan_plan`]).
fn commit_arm(
    commit_files: Vec<ScanFile>,
    add_field: &StructField,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
    prune: Option<&Predicate>,
) -> DeltaResult<PlanBuilder> {
    // Prune (per-file, before the aggregate). Removes lack add-side stats/partitions, so guard with
    // `add IS NULL` to keep every tombstone -- they never prune in this cut, only anti-join.
    let key_present = col!(KEY_PATH).is_not_null();
    let filter = key_present.fold_with(prune, |key_present, prune| {
        let gated_prune = Predicate::or(prune.clone(), col!("add").is_null());
        Predicate::and(key_present, gated_prune)
    });

    // Wrap add+remove into a single always-present `pair` so `max_non_null_by(pair, version)` picks
    // the newest action outright; liveness is then `pair.add IS NOT NULL`. This step mostly drops
    // columns, so a plain struct projection reads better than a patch.
    let wrapped_schema = schema_ref! {
        nullable (VERSION): LONG,
        ..(key_fields()),
        nullable (PAIR): { (add_field), (&REMOVE_FIELD) },
    };
    let wrap_exprs = vec![
        col!(VERSION),
        col!(KEY_PATH),
        col!(KEY_DV_STORAGE),
        col!(KEY_DV_PATH),
        col!(KEY_DV_OFFSET),
        Expression::struct_from([col!("add"), col!("remove")]),
    ];

    // Normalize: reparse `add.stats`/`add.partitionValues` in place, pass remove/version through,
    // and append the identity keys (coalesced across add/remove) that pruning and dedup key on.
    PlanBuilder::scan_json(commit_files, &[VERSION], read_schema(true))?
        .project_patch(|patch| {
            reparse_add(
                append_identity_keys(patch, coalesced_key),
                stats_schema,
                partition_schema,
            )
        })?
        .filter(filter)?
        .project(Expression::struct_from(wrap_exprs), wrapped_schema)
}

/// Build the checkpoint arm: scan the checkpoint parts, normalize + prune (no guard -- adds always
/// carry partition values). Returns normalized `{ add, key_*, ... }` rows -- ready for the caller's
/// anti-join against the commit keys and `{ add }` projection (see [`build_metadata_scan_plan`]).
///
/// A checkpoint version is homogeneous (all JSON or all Parquet), so at most one of `json_parts` /
/// `parquet_parts` is non-empty -- the log segment already partitions the single checkpoint by
/// format. Both read the same `{ add, version }` schema, so a `union_all` routes whichever format
/// is present without a `UnionAll` node (absent arms collapse away).
fn checkpoint_arm(
    json_parts: Vec<ScanFile>,
    parquet_parts: Vec<ScanFile>,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
    prune: Option<&Predicate>,
) -> DeltaResult<PlanBuilder> {
    let add_present = col!("add").is_not_null();
    let filter = add_present.fold_with(prune, |add_present, prune| {
        Predicate::and(add_present, prune.clone())
    });

    // Normalize: reparse `add.stats`/`add.partitionValues` in place, pass `version` through, and
    // append the identity keys (from `add` only -- checkpoint rows are all adds).
    let add_key = |segments: &[&str]| {
        Expression::column(std::iter::once("add").chain(segments.iter().copied()))
    };

    PlanBuilder::union_all([
        PlanBuilder::scan_json(json_parts, &[VERSION], read_schema(false))?,
        PlanBuilder::scan_parquet(parquet_parts, &[VERSION], read_schema(false))?,
    ])?
    .project_patch(|patch| {
        reparse_add(
            append_identity_keys(patch, add_key),
            stats_schema,
            partition_schema,
        )
    })?
    .filter(filter)
}

/// The physical partition-column names carried in `state`'s typed partition schema (empty when the
/// table is unpartitioned or no typed partition schema was built). Used to exclude partition
/// columns from stats skipping -- their values live in `add.partitionValues`, not `add.stats`.
fn partition_column_names(state: &StateInfo) -> Vec<String> {
    state
        .physical_partition_schema
        .iter()
        .flat_map(|s| s.fields().map(|f| f.name().to_string()))
        .collect()
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
    let skipping = as_checkpoint_skipping_predicate(
        pred,
        &partition_column_names(state),
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
/// typically straight from the log segment (newest commit wins). A checkpoint version is
/// homogeneous, so at most one checkpoint partition is non-empty.
///
/// Returns `None` when nothing can survive: an empty table (no files after absence collapse) or a
/// statically-unsatisfiable predicate ([`PhysicalPredicate::StaticSkipAll`]).
pub(crate) fn build_metadata_scan_plan(
    state: &StateInfo,
    commit_files: Vec<ScanFile>,
    json_checkpoint_files: Vec<ScanFile>,
    parquet_checkpoint_files: Vec<ScanFile>,
) -> DeltaResult<Option<Plan>> {
    // A statically-unsatisfiable predicate (e.g. `x > 10 AND FALSE`) skips the whole table.
    if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
        return Ok(None);
    }

    let stats_schema = state.physical_stats_schema.as_ref();
    let partition_schema = state.physical_partition_schema.as_ref();
    let prune = stats_skipping_predicate(state);
    let prune = prune.as_ref();

    // The output `add` after reparsing `stats`/`partitionValues`: shared by the commit arm's `pair`
    // carrier and both terminal `{ add }` projections, so every arm agrees on the union schema.
    let add_field = reparsed_add_field(stats_schema, partition_schema)?;
    let output_schema = schema_ref! { (&add_field) };

    // Dedup the commit rows to the newest action per file identity: `max_non_null_by(pair,
    // version)` keeps whichever of add/remove came last. Shared by both consumers below.
    let deduped_commit = commit_arm(
        commit_files,
        &add_field,
        stats_schema,
        partition_schema,
        prune,
    )?
    .aggregate_by(key_columns(), |a| {
        a.max_non_null_by(ColumnName::new([PAIR]), ColumnName::new([VERSION]))
    })?;

    // Live checkpoint adds: commits are newer than the checkpoint, so anti-join away every
    // checkpoint file whose identity a commit re-touched, then project the surviving `{ add }`.
    let checkpoint_live = checkpoint_arm(
        json_checkpoint_files,
        parquet_checkpoint_files,
        stats_schema,
        partition_schema,
        prune,
    )?
    .anti_join(deduped_commit.clone(), key_columns(), key_columns())?
    .project(
        Expression::struct_from([col!("add")]),
        output_schema.clone(),
    )?;

    // Live commit adds: keep the identities whose newest action is an add, projected to `{ add }`.
    let commit_live = deduped_commit
        .filter(col!(PAIR, "add").is_not_null())?
        .project(Expression::struct_from([col!("pair.add")]), output_schema)?;

    PlanBuilder::union_all([commit_live, checkpoint_live])?.build_opt()
}

struct DataReadShapeBuilder<'a> {
    state: &'a StateInfo,
    load_schema_patch: SchemaStructPatchBuilder,
    file_constant_columns: Vec<String>,
}

impl<'a> DataReadShapeBuilder<'a> {
    fn new(state: &'a StateInfo) -> Self {
        Self {
            state,
            load_schema_patch: SchemaStructPatchBuilder::new(),
            file_constant_columns: Vec::new(),
        }
    }

    fn apply_field_transform(
        &mut self,
        field_transform: &FieldTransformSpec,
        patch: ExpressionStructPatchBuilder,
    ) -> DeltaResult<ExpressionStructPatchBuilder> {
        // We can't move out the builder, so swap with a placeholder, frob it, and restore after.
        let mut schema_patch =
            std::mem::replace(&mut self.load_schema_patch, SchemaStructPatchBuilder::new());
        let patch = match field_transform {
            FieldTransformSpec::StaticInsert { insert_after, expr } => match insert_after {
                Some(predecessor) => patch.insert_after(predecessor.clone(), expr.clone()),
                None => patch.prepend(expr.clone()),
            },
            FieldTransformSpec::StaticDrop { field_name } => patch.drop(field_name.clone()),
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                let Some(field) = self.state.logical_schema.field_at_index(*field_index) else {
                    return Err(Error::internal_error(format!(
                        "transform spec references out-of-bounds field index {field_index}"
                    )));
                };
                let name = field.physical_name(self.state.column_mapping_mode);
                let load_field = StructField::nullable(name, field.data_type().clone());
                schema_patch = match insert_after {
                    Some(predecessor) => schema_patch.insert_after(predecessor.clone(), load_field),
                    None => schema_patch.prepend(load_field),
                };
                self.file_constant_columns.push(name.to_string());
                patch
            }
            FieldTransformSpec::GenerateRowId {
                field_name,
                row_index_field_name,
            } => {
                // Add the base row id column to the load schema if it's not already present.
                if self.state.physical_schema.field(BASE_ROW_ID).is_none() {
                    schema_patch =
                        schema_patch.append(StructField::nullable(BASE_ROW_ID, DataType::LONG));
                }
                self.file_constant_columns.push(BASE_ROW_ID.to_string());
                let expr = Expression::coalesce([
                    col!(field_name),
                    col!(BASE_ROW_ID) + col!(row_index_field_name),
                ]);
                patch.replace(field_name, expr).drop(BASE_ROW_ID)
            }
            FieldTransformSpec::DynamicColumn { .. } => {
                return Err(Error::unsupported(
                    "declarative data scan does not support CDF dynamic columns",
                ));
            }
        };
        self.load_schema_patch = schema_patch;
        Ok(patch)
    }

    fn build(self, reshape: Option<(Expression, SchemaRef)>) -> DeltaResult<DataReadShape> {
        Ok(DataReadShape {
            load_schema: Arc::new(self.load_schema_patch.build(&self.state.physical_schema)?),
            file_constant_columns: self.file_constant_columns,
            reshape,
        })
    }
}

/// Everything the data-read path derives from [`StateInfo`] before constructing the [`Load`] and
/// optional reshape [`Project`].
struct DataReadShape {
    load_schema: SchemaRef,
    file_constant_columns: Vec<String>,
    reshape: Option<(Expression, SchemaRef)>,
}

impl DataReadShape {
    /// Derive the shape of the declarative data-read plan from `state`. A schema patch extends the
    /// physical read schema with requested file-constant partition columns and appended internal
    /// file-constant helpers such as `baseRowId`; an expression patch adapts the resulting
    /// physical data to the logical schema the caller needs (e.g. computing row ids, dropping
    /// internal helper columns, and applying column mapping renames).
    fn try_new(state: &StateInfo) -> DeltaResult<Self> {
        let mut builder = DataReadShapeBuilder::new(state);
        let Some(spec) = state.transform_spec.as_deref() else {
            return builder.build(None);
        };

        let mut reshape_patch = ExpressionStructPatchBuilder::new();
        for field_transform in spec {
            reshape_patch = builder.apply_field_transform(field_transform, reshape_patch)?;
        }
        builder.build(Some((
            Expression::struct_patch(reshape_patch)?,
            state.logical_schema.clone(),
        )))
    }
}

/// Build the declarative data scan plan: a [`Load`] over `source`'s file rows (reading `state`'s
/// physical schema, broadcasting the scan's partition columns as file-constants), followed by an
/// optional reshape [`Project`] into the logical schema.
///
/// Everything but the snapshot-derived inputs comes from `state`: the physical read schema, the
/// broadcast partition columns, and the reshape expression (all lowered from
/// [`StateInfo::transform_spec`]). `source` is a relation whose rows name the files to read and
/// carry the file-meta / partition columns (see [`FILE_PATH`] et al.) -- typically the metadata
/// scan output; `base_url` is the table root. Returns `None` when `source` is empty.
///
/// [`Project`]: crate::plans::ir::nodes::Project
pub(crate) fn build_data_scan_plan(
    state: &StateInfo,
    source: PlanBuilder,
    base_url: Url,
) -> DeltaResult<Option<Plan>> {
    let shape = DataReadShape::try_new(state)?;
    let load = Load::new(
        shape.load_schema,
        FileType::Parquet,
        LoadColumnFileMeta::new(
            ColumnName::new([FILE_PATH]),
            ColumnName::new([FILE_SIZE]),
            ColumnName::new([NUM_RECORDS]),
        ),
        ColumnName::new([DV]),
    )
    .with_base_url(base_url)
    .with_file_constant_columns(shape.file_constant_columns);

    source
        .load(load)?
        .try_fold_with(shape.reshape, |loaded, (expr, schema)| {
            loaded.project(expr, schema)
        })?
        .build_opt()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::expressions::{lit, Scalar};
    use crate::plans::ir::nodes::Operator;
    use crate::scan::state_info::tests::get_state_info_with_options;
    use crate::scan::{PartitionValuesOptions, StatsOptions};
    use crate::schema::{MetadataColumnSpec, StructType};
    use crate::table_features::TableFeature;
    use crate::FileMeta;

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

    /// The full metadata plan (commits + checkpoint, with pruning) linearizes to the expected
    /// commit arm (scan_json -> project -> filter -> project -> aggregate), a checkpoint arm
    /// (scan_parquet -> project -> filter -> anti-join against the shared aggregate -> project),
    /// the live-commit extraction (filter -> project reusing the shared aggregate), and a
    /// terminal union. A predicate on the stats column `x` yields a pruning [`Filter`]; the typed
    /// partition schema retypes `add.partitionValues` in place.
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
        )?
        .expect("non-empty");

        // The commit arm (union input 0) linearizes first, then the checkpoint arm; both consume
        // the single shared aggregate (node 4).
        assert_eq!(
            tags(&plan),
            vec![
                "scan_json",    // 0 commits
                "project",      // 1 normalize
                "filter",       // 2 prune (guarded)
                "project",      // 3 wrap pair
                "aggregate",    // 4 newest-action-per-key (shared)
                "filter",       // 5 live commit adds (probe node 4)
                "project",      // 6 extract add
                "scan_parquet", // 7 checkpoint
                "project",      // 8 normalize
                "filter",       // 9 prune
                "semi_join",    // 10 anti-join vs commit keys (build = node 4)
                "project",      // 11 surviving checkpoint adds
                "union_all",    // 12 terminal
            ],
        );

        // The aggregate (node 4) is emitted once and referenced by both the live-add filter and
        // the anti-join build side.
        assert_eq!(plan.nodes[5].inputs, vec![4]);
        assert_eq!(plan.nodes[10].inputs, vec![9, 4]);
        let Operator::SemiJoin(join) = &plan.nodes[10].op else {
            panic!("expected anti-join");
        };
        assert!(join.inverted);
        Ok(())
    }

    /// With no predicate (so no pruning) and no partition columns, both arms filter only on
    /// identity presence; the plan shape is unchanged (the guard [`Filter`] is always present).
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
        )?
        .expect("non-empty");
        assert_eq!(
            tags(&plan),
            vec![
                "scan_json",
                "project",
                "filter",
                "project",
                "aggregate",
                "filter",
                "project",
                "scan_parquet",
                "project",
                "filter",
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
        let plan =
            build_metadata_scan_plan(&state, vec![scan_file("file:///1.json", 1)], vec![], vec![])?
                .expect("non-empty");
        assert_eq!(
            tags(&plan),
            vec![
                "scan_json",
                "project",
                "filter",
                "project",
                "aggregate",
                "filter",
                "project"
            ],
        );
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
        let plan = build_metadata_scan_plan(&state, vec![], json_parts, parquet_parts)?
            .expect("non-empty");
        // No union node (lone present partition forwarded), no anti-join node (absent build
        // forwarded the probe), no commit arm.
        assert_eq!(tags(&plan), vec![scan_tag, "project", "filter", "project"]);
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
        assert!(build_metadata_scan_plan(&state, vec![], vec![], vec![])?.is_none());
        Ok(())
    }

    /// A source relation exposing the load-friendly file-meta columns plus any columns [`Load`]
    /// broadcasts as file constants.
    fn data_source(file_constant_columns: &[&str]) -> DeltaResult<PlanBuilder> {
        let mut fields = vec![
            StructField::not_null(FILE_PATH, DataType::STRING),
            StructField::nullable(FILE_SIZE, DataType::LONG),
            StructField::nullable(NUM_RECORDS, DataType::LONG),
            StructField::nullable(DV, DataType::STRING),
        ];
        fields.extend(file_constant_columns.iter().map(|c| {
            let data_type = if *c == BASE_ROW_ID {
                DataType::LONG
            } else {
                DataType::STRING
            };
            StructField::nullable(*c, data_type)
        }));
        let schema = Arc::new(StructType::new_unchecked(fields));
        let row = schema
            .fields()
            .map(|f| Scalar::null(f.data_type().clone()))
            .collect();
        PlanBuilder::values(schema, vec![row])
    }

    /// A non-partitioned scan needs no reshape: the plan is `source -> load`.
    #[test]
    fn data_plan_no_reshape_when_no_transform() -> DeltaResult<()> {
        let state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let plan =
            build_data_scan_plan(&state, data_source(&[])?, Url::parse("memory:///").unwrap())?
                .expect("non-empty");
        assert_eq!(tags(&plan), vec!["values", "load"]);
        Ok(())
    }

    fn field_names(schema: &SchemaRef) -> Vec<&str> {
        schema.fields().map(|f| f.name().as_str()).collect()
    }

    /// A partitioned scan projects to impose the logical schema, but the partition column itself is
    /// handled by `Load`: it is broadcast as a file constant directly in user-requested order.
    #[test]
    fn data_plan_loads_partition_column_in_requested_order() -> DeltaResult<()> {
        let read_schema = schema_ref! {
            nullable "x": LONG,
            nullable "p1": STRING,
            nullable "y": LONG,
            nullable "p2": STRING,
        };
        let state = state(
            read_schema.clone(),
            vec!["p1".to_string(), "p2".to_string()],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let plan = build_data_scan_plan(
            &state,
            data_source(&["p1", "p2"])?,
            Url::parse("memory:///").unwrap(),
        )?
        .expect("non-empty");
        assert_eq!(tags(&plan), vec!["values", "load", "project"]);

        let Operator::Load(load) = &plan.nodes[1].op else {
            panic!("expected load");
        };
        assert_eq!(load.file_constant_columns, vec!["p1", "p2"]);
        assert_eq!(field_names(&load.schema), vec!["x", "p1", "y", "p2"]);

        let Operator::Project(project) = &plan.nodes[2].op else {
            panic!("expected project");
        };
        assert_eq!(project.schema, read_schema);
        let Expression::StructPatch(patch) = project.expr.as_ref() else {
            panic!("expected struct patch");
        };
        assert!(patch.is_empty());
        Ok(())
    }

    #[test]
    fn data_plan_supports_static_insert() -> DeltaResult<()> {
        let mut state = state(
            data_schema(),
            vec![],
            None,
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        );
        let read_schema = schema_ref! {
            nullable "inserted": LONG,
            nullable "x": LONG,
        };
        state.logical_schema = read_schema.clone();
        state.transform_spec = Some(Arc::new(vec![FieldTransformSpec::StaticInsert {
            insert_after: None,
            expr: Arc::new(lit(7i64)),
        }]));

        let plan =
            build_data_scan_plan(&state, data_source(&[])?, Url::parse("memory:///").unwrap())?
                .expect("non-empty");
        assert_eq!(tags(&plan), vec!["values", "load", "project"]);

        let Operator::Load(load) = &plan.nodes[1].op else {
            panic!("expected load");
        };
        assert_eq!(field_names(&load.schema), vec!["x"]);

        let Operator::Project(project) = &plan.nodes[2].op else {
            panic!("expected project");
        };
        assert_eq!(project.schema, read_schema);
        let Expression::StructPatch(patch) = project.expr.as_ref() else {
            panic!("expected struct patch");
        };
        assert_eq!(patch.prepended_fields, vec![Arc::new(lit(7i64))]);
        Ok(())
    }

    #[test]
    fn data_plan_generates_row_ids() -> DeltaResult<()> {
        let read_schema = schema_ref! { nullable "x": LONG };
        let state = get_state_info_with_options(
            read_schema,
            vec![],
            None,
            &[TableFeature::RowTracking, TableFeature::DomainMetadata],
            [
                ("delta.enableRowTracking".to_string(), "true".to_string()),
                (
                    "delta.rowTracking.materializedRowIdColumnName".to_string(),
                    "row_id_col".to_string(),
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName".to_string(),
                    "row_commit_version_col".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            vec![("row_id", MetadataColumnSpec::RowId)],
            StatsOptions::default(),
            PartitionValuesOptions::default(),
        )
        .expect("state info");

        let plan = build_data_scan_plan(
            &state,
            data_source(&[BASE_ROW_ID])?,
            Url::parse("memory:///").unwrap(),
        )?
        .expect("non-empty");
        assert_eq!(tags(&plan), vec!["values", "load", "project"]);

        let Operator::Load(load) = &plan.nodes[1].op else {
            panic!("expected load");
        };
        assert_eq!(load.file_constant_columns, vec![BASE_ROW_ID]);
        assert!(load.schema.field("row_id_col").is_some());
        assert!(load.schema.field("row_indexes_for_row_id_0").is_some());
        assert!(load.schema.field(BASE_ROW_ID).is_some());

        let Operator::Project(project) = &plan.nodes[2].op else {
            panic!("expected project");
        };
        assert_eq!(project.schema, state.logical_schema);
        let Expression::StructPatch(patch) = project.expr.as_ref() else {
            panic!("expected struct patch");
        };
        let base_row_id_patch = patch
            .field_patches
            .get(BASE_ROW_ID)
            .expect("baseRowId should be dropped");
        assert!(!base_row_id_patch.keep_input);
        assert!(base_row_id_patch.insertions.is_empty());

        let row_index_patch = patch
            .field_patches
            .get("row_indexes_for_row_id_0")
            .expect("row index helper should be dropped");
        assert!(!row_index_patch.keep_input);
        assert!(row_index_patch.insertions.is_empty());

        let row_id_patch = patch
            .field_patches
            .get("row_id_col")
            .expect("row_id_col should be replaced");
        assert!(!row_id_patch.keep_input);
        assert_eq!(
            row_id_patch.insertions,
            vec![Arc::new(Expression::coalesce([
                col!("row_id_col"),
                col!(BASE_ROW_ID) + col!("row_indexes_for_row_id_0"),
            ]))]
        );
        Ok(())
    }
}
