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

use std::sync::{Arc, LazyLock};

use url::Url;

use super::data_skipping::as_checkpoint_skipping_predicate;
use super::state_info::StateInfo;
use super::transform_spec::FieldTransformSpec;
use super::{PhysicalPredicate, PrefixColumns};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{
    ADD_FIELD, ADD_NAME, ADD_SCHEMA, REMOVE_FIELD, SIDECAR_FIELD, SIDECAR_NAME, STATS_PARSED,
};
use crate::expressions::{
    col, column_name, joined_column_expr, ColumnName, Expression as Expr,
    ExpressionStructPatchBuilder, Predicate,
};
use crate::plans::ir::nodes::{FileType, Load, LoadColumnFileMeta, ScanFile};
use crate::plans::ir::plan::Plan;
use crate::schema::{
    lazy_schema_ref, schema_ref, DataType, MapType, SchemaRef, SchemaStructPatchBuilder,
    StructField, ToSchema as _,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::transforms::ExpressionTransform;
use crate::utils::FoldWithOption as _;
use crate::{DeltaResult, Error, PlanBuilder};

// === Internal column names ===

// Both add and remove provide path + DV (storageType, pathOrInlineDv, offset) columns. We coalesce
// and materialize them as top-level scaffolding columns so the plan's aggregate and anti-join
// operators can correctly pair up adds with removes.
const FILE_ACTION_KEY_PATH: &str = "key_path";
const FILE_ACTION_KEY_DV_STORAGE: &str = "key_dv_storage";
const FILE_ACTION_KEY_DV_PATH: &str = "key_dv_path";
const FILE_ACTION_KEY_DV_OFFSET: &str = "key_dv_offset";
/// The `add` action sub-fields reparsed in place from their raw log encodings: `stats` (JSON string
/// -> typed stats struct, which pruning points at via `add.stats.minValues.*` etc.) and
/// `partitionValues` (string map -> typed partition struct).
const STATS: &str = "stats";
const PARTITION_VALUES: &str = "partitionValues";
const PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
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

/// Like [`json_read_schema`], but includes struct stats and partition values columns.
fn parquet_read_schema(
    include_remove: bool,
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
        ..(include_remove.then_some(&REMOVE_FIELD)),
        nullable (VERSION): LONG,
    })
}

/// The subset of file action fields that uniquely identifies a logical file in the log, used for
/// deduplication of adds and removes during log replay.
static FILE_ACTION_KEYS: LazyLock<[(&str, DataType, ColumnName); 4]> = LazyLock::new(|| {
    [
        (FILE_ACTION_KEY_PATH, DataType::STRING, column_name!("path")),
        (
            FILE_ACTION_KEY_DV_STORAGE,
            DataType::STRING,
            column_name!("deletionVector.storageType"),
        ),
        (
            FILE_ACTION_KEY_DV_PATH,
            DataType::STRING,
            column_name!("deletionVector.pathOrInlineDv"),
        ),
        (
            FILE_ACTION_KEY_DV_OFFSET,
            DataType::INTEGER,
            column_name!("deletionVector.offset"),
        ),
    ]
});

/// The file action key as fields
fn file_action_key_fields() -> Vec<StructField> {
    FILE_ACTION_KEYS
        .iter()
        .map(|(name, dtype, _)| StructField::nullable(*name, dtype.clone()))
        .collect()
}

/// The file action key as column names
fn file_action_key_columns() -> Vec<ColumnName> {
    FILE_ACTION_KEYS
        .iter()
        .map(|(name, ..)| ColumnName::new([*name]))
        .collect()
}

/// Append the identity keys to a projection patch, each produced by `key_expr(trailing)` -- which
/// differs per arm: coalesced across add/remove for commits, add-only for checkpoints.
fn append_file_action_keys<'a>(
    patch: ProjectionStructPatchBuilder<'a>,
    key_expr: impl Fn(&ColumnName) -> Expr,
) -> ProjectionStructPatchBuilder<'a> {
    FILE_ACTION_KEYS
        .iter()
        .fold(patch, |patch, (name, dtype, trailing)| {
            patch.append(
                StructField::nullable(*name, dtype.clone()),
                key_expr(trailing),
            )
        })
}

#[derive(Clone, Copy)]
enum ParsedSource {
    Null,
    Columns,
}

/// Reparse the `add` action's `stats` (JSON string -> typed struct) and `partitionValues` (string
/// map -> typed struct) *in place*, preferring checkpoint-native parsed columns when present. The
/// normalized output has a single typed `add.stats` / `add.partitionValues`; internal `_parsed`
/// siblings are dropped after use.
///
/// A `None` schema is the corner case where the caller wants neither pruning nor that parsed
/// column: the field is nulled out but keeps its canonical log type ([`DataType::STRING`] for
/// `stats`, a string map for `partitionValues`), matching [`reparsed_add_field`].
fn reparse_add<'a>(
    patch: ProjectionStructPatchBuilder<'a>,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
    parsed_source: ParsedSource,
) -> ProjectionStructPatchBuilder<'a> {
    let add = [ADD_NAME];
    let patch = match stats_schema {
        Some(ss) => {
            let field = StructField::nullable(STATS, ss.as_ref().clone());
            let expr = Expr::parse_json(col!("add.stats"), Arc::clone(ss));
            match parsed_source {
                ParsedSource::Null => patch.replace_at(add, STATS, field, expr),
                ParsedSource::Columns => {
                    let expr = Expr::coalesce([col!(ADD_NAME, STATS_PARSED), expr]);
                    patch
                        .replace_at(add, STATS, field, expr)
                        .drop_at(add, STATS_PARSED)
                }
            }
        }
        None => patch.replace_expr_at(add, STATS, Expr::null_literal(DataType::STRING)),
    };
    match partition_schema {
        Some(ps) => {
            let field = StructField::nullable(PARTITION_VALUES, ps.as_ref().clone());
            let expr = Expr::map_to_struct(col!("add.partitionValues"));
            match parsed_source {
                ParsedSource::Null => patch.replace_at(add, PARTITION_VALUES, field, expr),
                ParsedSource::Columns => {
                    let expr = Expr::coalesce([col!(ADD_NAME, PARTITION_VALUES_PARSED), expr]);
                    patch
                        .replace_at(add, PARTITION_VALUES, field, expr)
                        .drop_at(add, PARTITION_VALUES_PARSED)
                }
            }
        }
        None => {
            let expr = Expr::null_literal(partition_values_map_type());
            patch.replace_expr_at(add, PARTITION_VALUES, expr)
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
/// Kept in lockstep with [`reparse_add`] so the downstream `pair` carrier and terminal `{ add }`
/// projections declare exactly the schema the reparse projection emits.
fn reparsed_add_field(
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
) -> DeltaResult<StructField> {
    let patch = SchemaStructPatchBuilder::new()
        .fold_with(stats_schema, |patch, ss| {
            patch.replace(STATS, StructField::nullable(STATS, ss.as_ref().clone()))
        })
        .fold_with(partition_schema, |patch, ps| {
            let field = StructField::nullable(PARTITION_VALUES, ps.as_ref().clone());
            patch.replace(PARTITION_VALUES, field)
        });
    Ok(StructField::nullable(ADD_NAME, patch.build(&ADD_SCHEMA)?))
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
    let key_present = col!(FILE_ACTION_KEY_PATH).is_not_null();
    let filter = key_present.fold_with(prune, |key_present, prune| {
        let gated_prune = Predicate::or(prune.clone(), col!("add").is_null());
        Predicate::and(key_present, gated_prune)
    });

    // Wrap add+remove into a single always-present `pair` so `max_non_null_by(pair, version)` picks
    // the newest action outright; liveness is then `pair.add IS NOT NULL`. This step mostly drops
    // columns, so a plain struct projection reads better than a patch.
    let wrapped_schema = schema_ref! {
        nullable (VERSION): LONG,
        ..(file_action_key_fields()),
        nullable (PAIR): { (add_field), (&REMOVE_FIELD) },
    };
    let mut wrap_exprs = vec![col!(VERSION)];
    wrap_exprs.extend(file_action_key_columns().into_iter().map(Into::into));
    wrap_exprs.push(Expr::struct_from([col!("add"), col!("remove")]));

    PlanBuilder::scan_json(commit_files, &[VERSION], json_read_schema(true))?
        .project_patch(|patch| {
            reparse_add(
                append_file_action_keys(patch, |col| {
                    Expr::coalesce([
                        joined_column_expr!("add", col),
                        joined_column_expr!("remove", col),
                    ])
                }),
                stats_schema,
                partition_schema,
                ParsedSource::Null,
            )
        })?
        .filter(filter)?
        .project(Expr::struct_from(wrap_exprs), wrapped_schema)
}

fn normalize_checkpoint_actions(
    source: PlanBuilder,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
    prune: Option<&Predicate>,
    parsed_source: ParsedSource,
) -> DeltaResult<PlanBuilder> {
    let add_present = col!("add").is_not_null();
    let filter = add_present.fold_with(prune, |add_present, prune| {
        Predicate::and(add_present, prune.clone())
    });
    source
        .project_patch(|patch| {
            reparse_add(
                append_file_action_keys(patch, |col| joined_column_expr!("add", col)),
                stats_schema,
                partition_schema,
                parsed_source,
            )
        })?
        .filter(filter)
}

static SIDECAR_FILE_META_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable (FILE_PATH): STRING,
    nullable (FILE_SIZE): LONG,
    nullable (NUM_RECORDS): LONG,
    nullable (DV): (DeletionVectorDescriptor::to_schema()),
    nullable (VERSION): LONG,
};

fn sidecar_actions(
    json_parts: Vec<ScanFile>,
    parquet_parts: Vec<ScanFile>,
    action_schema: SchemaRef,
    log_root: &Url,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<&SchemaRef>,
    prune: Option<&Predicate>,
) -> DeltaResult<PlanBuilder> {
    let sidecar_roots = PlanBuilder::union_all([
        PlanBuilder::scan_json(json_parts, &[VERSION], sidecar_read_schema())?,
        PlanBuilder::scan_parquet(parquet_parts, &[VERSION], sidecar_read_schema())?,
    ])?;
    let sidecar_files = sidecar_roots
        .filter(col!(SIDECAR_NAME).is_not_null())?
        .project(
            Expr::struct_from([
                col!(SIDECAR_NAME, "path"),
                col!(SIDECAR_NAME, "sizeInBytes"),
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

    normalize_checkpoint_actions(
        sidecar_files.load(load)?,
        stats_schema,
        partition_schema,
        prune,
        ParsedSource::Columns,
    )
}

/// Build the checkpoint arm: scan the checkpoint parts, normalize + prune (no guard -- adds always
/// carry partition values). Returns normalized `{ add, key_*, ... }` rows -- ready for the caller's
/// anti-join against the commit keys and `{ add }` projection (see [`build_metadata_scan_plan`]).
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
    prune: Option<&Predicate>,
) -> DeltaResult<PlanBuilder> {
    let schema = parquet_read_schema(false, stats_schema, partition_schema)?;
    PlanBuilder::union_all([
        normalize_checkpoint_actions(
            PlanBuilder::scan_json(json_parts.clone(), &[VERSION], json_read_schema(false))?,
            stats_schema,
            partition_schema,
            prune,
            ParsedSource::Null,
        )?,
        normalize_checkpoint_actions(
            PlanBuilder::scan_parquet(parquet_parts.clone(), &[VERSION], Arc::clone(&schema))?,
            stats_schema,
            partition_schema,
            prune,
            ParsedSource::Columns,
        )?,
        sidecar_actions(
            json_parts,
            parquet_parts,
            schema,
            log_root,
            stats_schema,
            partition_schema,
            prune,
        )?,
    ])
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
    .aggregate_by(file_action_key_columns(), |a| {
        a.max_non_null_by(ColumnName::new([PAIR]), ColumnName::new([VERSION]))
    })?;

    // Live checkpoint adds: Commit content supersedes checkpoint content, so anti-join away every
    // checkpoint identity a commit also touched, then project out the `{ add }`.
    let checkpoint_live = checkpoint_arm(
        json_checkpoint_files,
        parquet_checkpoint_files,
        &log_root,
        stats_schema,
        partition_schema,
        prune,
    )?
    .anti_join(
        deduped_commit.clone(),
        file_action_key_columns(),
        file_action_key_columns(),
    )?
    .project(Expr::struct_from([col!("add")]), output_schema.clone())?;

    // Live commit adds: keep the identities whose newest action is an add, projected to `{ add }`.
    let commit_live = deduped_commit
        .filter(col!(PAIR, "add").is_not_null())?
        .project(Expr::struct_from([col!("pair.add")]), output_schema)?;

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
                let load_field = StructField::new(name, field.data_type().clone(), field.nullable);
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
                let expr = Expr::coalesce([
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

    fn build(self, reshape: Option<(Expr, SchemaRef)>) -> DeltaResult<DataReadShape> {
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
    reshape: Option<(Expr, SchemaRef)>,
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
            Expr::struct_patch(reshape_patch)?,
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
                "scan_parquet", // 7 checkpoint root actions
                "project",      // 8 normalize
                "filter",       // 9 prune
                "scan_parquet", // 10 checkpoint roots for sidecar refs
                "filter",       // 11 keep sidecar rows
                "project",      // 12 sidecar file metadata
                "load",         // 13 sidecar file actions
                "project",      // 14 normalize sidecar actions
                "filter",       // 15 prune sidecar actions
                "union_all",    // 16 root + sidecar checkpoint actions
                "semi_join",    // 17 anti-join vs commit keys (build = node 4)
                "project",      // 18 surviving checkpoint adds
                "union_all",    // 19 terminal
            ],
        );

        // The aggregate (node 4) is emitted once and referenced by both the live-add filter and
        // the anti-join build side.
        assert_eq!(plan.nodes[5].inputs, vec![4]);
        assert_eq!(plan.nodes[17].inputs, vec![16, 4]);
        let Operator::SemiJoin(join) = &plan.nodes[17].op else {
            panic!("expected anti-join");
        };
        assert!(join.inverted);

        let Operator::ScanParquet(root_scan) = &plan.nodes[7].op else {
            panic!("expected parquet checkpoint root action scan");
        };
        assert!(add_struct(&root_scan.schema).field(STATS_PARSED).is_some());
        assert!(add_struct(&root_scan.schema)
            .field(PARTITION_VALUES_PARSED)
            .is_some());

        let Operator::Project(root_normalize) = &plan.nodes[8].op else {
            panic!("expected root normalize project");
        };
        assert!(add_struct(&root_normalize.schema)
            .field(STATS_PARSED)
            .is_none());
        assert!(add_struct(&root_normalize.schema)
            .field(PARTITION_VALUES_PARSED)
            .is_none());

        let Operator::Load(sidecar_load) = &plan.nodes[13].op else {
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
            log_root(),
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
                "scan_parquet",
                "filter",
                "project",
                "load",
                "project",
                "filter",
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
        let plan = build_metadata_scan_plan(&state, vec![], json_parts, parquet_parts, log_root())?
            .expect("non-empty");
        // No anti-join node (absent build forwarded the probe) and no commit arm. The checkpoint
        // relation still unions root file actions with sidecar file actions.
        assert_eq!(
            tags(&plan),
            vec![
                scan_tag,
                "project",
                "filter",
                scan_tag,
                "filter",
                "project",
                "load",
                "project",
                "filter",
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
        let Expr::StructPatch(patch) = project.expr.as_ref() else {
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
        let Expr::StructPatch(patch) = project.expr.as_ref() else {
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
        let Expr::StructPatch(patch) = project.expr.as_ref() else {
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
            vec![Arc::new(Expr::coalesce([
                col!("row_id_col"),
                col!(BASE_ROW_ID) + col!("row_indexes_for_row_id_0"),
            ]))]
        );
        Ok(())
    }
}
