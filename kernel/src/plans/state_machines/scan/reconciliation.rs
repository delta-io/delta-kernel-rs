//! Shared FSR / Scan reconciliation pipeline.
//!
//! Builds the canonical *window-on-commits + anti-join-on-checkpoint* pipeline against the
//! [`Context`] / [`PlanBuilder`] API and the kernel plan IR. Consumed by
//! [`super::full_state::FullState::state_machine`] and [`super::scan_plan::build_scan_plan`].
//!
//! Pipeline shape:
//!
//! ```text
//! commit_load (Values -> Load)
//!   -> commit_dedup (filter -> project -> max_by_version)
//!   -> [checkpoint view, per shape variant]
//!     -> keyed (filter + project + append join key)
//!     -> LEFTANTI(commit_keys) -> survivors
//!     -> UNION(commit_dedup, survivors) -> Filter(retention) -> Project(checkpoint_pair)
//!     -> reconciled
//! ```
//!
//! Returns a [`PlanBuilder`] terminating on the reconciled stream; the caller drains it via
//! [`Context::into_result_plan`].
//!
//! The pipeline-shared schemas ([`SCAN_BASE`] / [`FSR_BASE`]), dedup-key expressions
//! ([`fsr_dedup_key`] / [`scan_file_dedup_key`]), and tombstone/txn retention predicate
//! ([`retention_filter`]) all live here -- they are inputs to the same canonical pipeline and
//! share its lifetime.

use std::sync::{Arc, LazyLock};

use url::Url;

use super::shape::{CheckpointShape, ScanShape, StatsInfo};
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
    SIDECAR_NAME,
};
use crate::expressions::{
    col, column_expr, lit, ColumnName, Expression, ExpressionRef, Predicate, PredicateRef, Scalar,
};
use crate::path::ParsedLogPath;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{
    default_scan_file_columns, FileFormat, FileType, LoadNode, ScanFileColumns,
};
use crate::plans::state_machines::framework::coroutine::Engine;
use crate::plans::state_machines::framework::plan_context::{Context, PlanBuilder};
use crate::schema::{arc_schema, ArrayType, DataType, SchemaRef, StructField, ToSchema};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta};

// ============================================================================
// Pipeline base schemas
// ============================================================================

/// Scan pipeline base: `{add, remove}` only. The scan path never reconciles
/// protocol/metaData/txn/domainMetadata (the snapshot reads those separately).
pub(super) static SCAN_BASE: LazyLock<SchemaRef> = LazyLock::new(|| {
    arc_schema([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
    ])
});

/// FSR pipeline base: all six action slots.
pub(super) static FSR_BASE: LazyLock<SchemaRef> = LazyLock::new(|| {
    arc_schema([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
    ])
});

// ============================================================================
// Synthetic dedup-key column (name + typed field)
// ============================================================================

/// Synthetic column carrying the materialized dedup key array so hash joins only need top-level
/// column keys.
pub(super) const FSR_JOIN_KEY_COL: &str = "__fsr_join_k";

/// Typed `StructField` for [`FSR_JOIN_KEY_COL`], appended before windowing and re-projected
/// through the antijoin.
static JOIN_KEY_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::nullable(FSR_JOIN_KEY_COL, ArrayType::new(DataType::STRING, true))
});

// ============================================================================
// Dedup-key expressions
// ============================================================================
//
// [`fsr_dedup_key`] keys the full six-slot action set; [`scan_file_dedup_key`] keys
// `{add, remove}` only. Both evaluate to NULL on rows that match no known slot, so the
// pipeline's identity filter is simply `dedup_key IS NOT NULL`.

const ADD_PATH: &[&str] = &["add", "path"];
const REMOVE_PATH: &[&str] = &["remove", "path"];
const METADATA_ID: &[&str] = &["metaData", "id"];

/// For file rows, the identity components come from either `add.<suffix>` or
/// `remove.<suffix>` (one is non-null per row); coalesce picks whichever side carries the
/// value. Shared by both dedup-key builders.
fn file_field(suffix: &[&str]) -> Expression {
    let add_path = ["add"].into_iter().chain(suffix.iter().copied());
    let remove_path = ["remove"].into_iter().chain(suffix.iter().copied());
    Expression::column(add_path).or_else(Expression::column(remove_path))
}

/// `Array<String?>?` NULL -- the fallback returned by both dedup-key `CASE` expressions when
/// no arm matches.
fn null_string_array() -> Expression {
    Expression::null_literal(ArrayType::new(DataType::STRING, true).into())
}

/// Predicate that selects rows whose `add.path` OR `remove.path` is non-null.
fn is_file_row() -> Predicate {
    Predicate::or(col(ADD_PATH).is_not_null(), col(REMOVE_PATH).is_not_null())
}

/// `["file", path_coalesce, dv_storage_coalesce, dv_inline_coalesce]` -- the file-row arm
/// of both dedup keys.
///
/// # Path-based DV offset gap
///
/// [`DeletionVectorDescriptor::unique_id`] concatenates
/// `storageType + pathOrInlineDv + "@" + offset` so two DVs that share storage + path but
/// differ in byte offset (only possible under `storageType = "p"`, where a single backing
/// file holds multiple packed DVs) are still distinct. This array key omits
/// `deletionVector.offset` because the plan IR has no `Cast(i64 -> string)` operator and
/// `Expression::array` requires uniform element types. Two such DVs would collapse here.
/// The kernel's existing `LogReplay` dedup uses `unique_id()` and is not subject to this
/// limitation; the executor running this plan must perform a final dedup by
/// `(path, unique_id())` on the emitted action stream until the IR gains a string-cast op.
///
/// [`DeletionVectorDescriptor::unique_id`]:
///     crate::actions::deletion_vector::DeletionVectorDescriptor::unique_id
fn file_arm() -> Expression {
    Expression::array(vec![
        lit("file"),
        file_field(&["path"]),
        file_field(&["deletionVector", "storageType"]),
        file_field(&["deletionVector", "pathOrInlineDv"]),
    ])
}

/// FSR pipeline dedup key: identity over all six action slots.
///
/// Returns an `ARRAY<STRING?>?` expression keyed by `[kind, id1, id2, id3]`, evaluating to
/// NULL on rows that match no known slot so `dedup_key IS NOT NULL` doubles as the identity
/// filter.
pub(super) fn fsr_dedup_key() -> Expression {
    // Every arm produces a 4-element array `[kind, primary_id, dv_storage, dv_inline_dv]`,
    // matching `file_arm` -- the widest arm. The DV slots are only populated by file rows
    // (`add`/`remove`); non-file actions null them. `singleton` is for actions with no
    // per-row id (protocol, metaData); `single_id` is for actions keyed by one id field
    // (domainMetadata.domain, txn.appId).
    let null_str = || Expression::null_literal(DataType::STRING);
    let singleton = |kind: &str| {
        Expression::array(vec![
            lit(kind),
            null_str(), // primary_id (none for singleton actions)
            null_str(), // dv_storage (file-row only)
            null_str(), // dv_inline_dv (file-row only)
        ])
    };
    let single_id = |kind: &str, id: Expression| {
        Expression::array(vec![
            lit(kind),
            id,
            null_str(), // dv_storage (file-row only)
            null_str(), // dv_inline_dv (file-row only)
        ])
    };
    let arms = vec![
        (is_file_row(), file_arm()),
        (col(["protocol"]).is_not_null(), singleton(PROTOCOL_NAME)),
        // Use a stable string label for the metadata dedup arm. We do NOT use `METADATA_NAME`
        // here (which is `"metaData"`) because the dedup key only needs to be unique
        // among arms; the explicit lowercase `"metadata"` matches the other action-name
        // constants like `DOMAIN_METADATA_NAME` ("domainMetadata") in canonical lowercase
        // form used by the test fixtures.
        (col(METADATA_ID).is_not_null(), singleton("metadata")),
        (
            col(["domainMetadata"]).is_not_null(),
            single_id(DOMAIN_METADATA_NAME, col(["domainMetadata", "domain"])),
        ),
        (
            col(["txn"]).is_not_null(),
            single_id(SET_TRANSACTION_NAME, col(["txn", "appId"])),
        ),
    ];
    Expression::case_when(arms, null_string_array())
}

/// Scan-pipeline dedup key: file-path identity over `{add, remove}` only.
///
/// Returns an `ARRAY<STRING?>?` of the form `[kind, file_path, dv_storage, dv_inline_dv]`
/// when the row carries an `add` or `remove` action; otherwise NULL. `dedup_key IS NOT NULL`
/// serves as both the dedup partition key and the "is this a valid scan-side action row"
/// identity filter.
pub(super) fn scan_file_dedup_key() -> Expression {
    Expression::case_when(vec![(is_file_row(), file_arm())], null_string_array())
}

// ============================================================================
// Tombstone / txn-expiration retention predicate
// ============================================================================

const REMOVE_DELETION_TIMESTAMP: &[&str] = &["remove", "deletionTimestamp"];
const TXN_LAST_UPDATED: &[&str] = &["txn", "lastUpdated"];

/// Tombstone / txn expiration predicate aligned with
/// [`ActionReconciliationVisitor::is_expired_tombstone`] and txn retention checks in the same
/// visitor (`kernel/src/action_reconciliation/log_replay.rs`).
///
/// [`ActionReconciliationVisitor::is_expired_tombstone`]:
///     crate::action_reconciliation::log_replay::ActionReconciliationVisitor::is_expired_tombstone
///
/// `txn_expiry` is `None` when `delta.setTransactionRetentionDuration` is unset -- txn rows
/// are not filtered by age.
fn retention_filter(min_file_ts: i64, txn_expiry: Option<i64>) -> Predicate {
    let remove_ok = Predicate::or(
        col(["remove"]).is_null(),
        col(REMOVE_DELETION_TIMESTAMP)
            .or_lit(0i64)
            .gt(lit(min_file_ts)),
    );
    let txn_ok = match txn_expiry {
        None => Predicate::literal(true),
        Some(cutoff) => Predicate::or_from([
            col(["txn"]).is_null(),
            col(TXN_LAST_UPDATED).is_null(),
            col(TXN_LAST_UPDATED).gt(lit(cutoff)),
        ]),
    };
    // `ActionReconciliationVisitor::check_domain_metadata_action` excludes removed
    // domainMetadata entries from FSR output (they are tombstones, not live state);
    // mirror that here so the dedup window does not emit a winning row when the latest
    // entry is `removed=true`.
    let domain_metadata_ok = Predicate::or(
        col(["domainMetadata"]).is_null(),
        col(["domainMetadata", "removed"]).or_lit(false).eq(lit(false)),
    );
    Predicate::and_from([remove_ok, txn_ok, domain_metadata_ok])
}

// ============================================================================
// Reconciliation builder
// ============================================================================

/// Sync core of the reconciliation. Builds against the supplied `ctx` and returns a
/// [`PlanBuilder`] terminating on the reconciled action stream. Caller wraps the result in
/// [`Context::into_result_plan`].
pub(super) fn build_reconciliation(
    ctx: &Context,
    snapshot: &Snapshot,
    shape: &ScanShape,
    base: &SchemaRef,
    dedup_key: ExpressionRef,
) -> Result<PlanBuilder, DeltaError> {
    let stats = shape.stats.as_ref().map(|s| &s.schema);
    let parts = shape.partition_schema.as_ref();
    let identity_not_null: PredicateRef = Arc::new(dedup_key.as_ref().clone().is_not_null());

    let seg = snapshot.log_segment();
    let log_root = seg.log_root.clone();
    let (min_file_ts, txn_expiry) = retention_timestamps(snapshot)?;

    // === Stage 1: commit_load ===========================================================
    // VALUES(commits) -> Load(JSON) broadcasts the per-commit `version` column onto every
    // emitted action row.
    let commit_rows = log_files_to_rows(&log_root, seg.find_commit_cover())?;
    let commit_load = LoadNode {
        file_schema: Arc::clone(base),
        file_type: FileType::Json,
        base_url: Some(log_root.clone()),
        passthrough_columns: vec![ColumnName::new(["version"])],
        file_meta: default_scan_file_columns(),
        dv_ref: None,
    };
    let commit_raw = ctx
        .values(commit_load_schema(), commit_rows)?
        .load(commit_load)?;

    // === Stage 2: commit_dedup ==========================================================
    // Commits never carry native parsed stats; always parse JSON. partitionValues_parsed is
    // derived via `map_to_struct` when partitions are requested. JOIN_KEY is appended as the
    // dedup key; `version` already lives on every row (Load broadcast it via
    // `passthrough_columns`) and feeds `max_by_version` directly. `version` is dropped by
    // `max_by_version` (it is not in `value_columns`).
    let value_columns: Vec<String> = base
        .fields()
        .map(|f| f.name().clone())
        .chain(std::iter::once(FSR_JOIN_KEY_COL.to_string()))
        .collect();
    let commit_dedup = commit_raw
        .filter(identity_not_null.clone())?
        .with_json_stats_parsed(stats)?
        .with_partitions_parsed(parts)?
        .append_col_typed(JOIN_KEY_FIELD.clone(), Arc::clone(&dedup_key))?
        .max_by_version(
            [col(FSR_JOIN_KEY_COL)],
            column_expr!("version"),
            value_columns,
        )?;

    // === Stages 3-5a: build the checkpoint view per shape variant =======================
    // Returns `Some(PlanBuilder)` aligned to `commit_dedup`'s schema minus JOIN_KEY (i.e. ready
    // for the antijoin's union arm), or `None` when the snapshot has no checkpoint at all.
    let checkpoint_view: Option<PlanBuilder> = match &shape.checkpoint {
        CheckpointShape::None => None,
        CheckpointShape::Leaf { files, file_format } => Some(load_checkpoint_files(
            ctx,
            base,
            shape,
            *file_format,
            files.clone(),
        )?),
        CheckpointShape::Manifest { files, file_format } => {
            // Re-scan the manifest with `base + sidecar` so that `drop_col(SIDECAR_NAME)`
            // recovers the action stream regardless of pipeline base width (scan = 2,
            // FSR = 6). The builder branches: one chases sidecars; the other selects
            // manifest-resident action rows directly.
            let manifest = match file_format {
                FileFormat::Parquet => {
                    ctx.scan_parquet(files.clone(), manifest_action_schema(base))?
                }
                FileFormat::Json => ctx.scan_json(files.clone(), manifest_action_schema(base))?,
            };
            let sidecar_base = log_root.join("_sidecars/").map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaStateRecoverError,
                    "build_reconciliation: join _sidecars base URL: {e}",
                )
            })?;
            // Sidecar branch: filter -> project (path/sizeInBytes) -> Load. The sidecar
            // parquet always uses `base` shape; native parsed stats (when present) are
            // surfaced by replacing `add.stats` with `add.stats_parsed` in the file_schema.
            let sidecar_load = manifest
                .clone()
                .filter(col([SIDECAR_NAME]).is_not_null())?
                .project([
                    ("path", col([SIDECAR_NAME, "path"])),
                    ("sizeInBytes", col([SIDECAR_NAME, "sizeInBytes"])),
                ])?
                .load(LoadNode {
                    file_schema: sidecar_file_schema(base, shape.stats.as_ref())?,
                    file_type: FileType::Parquet,
                    base_url: Some(sidecar_base),
                    passthrough_columns: vec![],
                    file_meta: ScanFileColumns {
                        path: ColumnName::new(["path"]),
                        size: Some(ColumnName::new(["sizeInBytes"])),
                        record_count: None,
                    },
                    dv_ref: None,
                })?;
            let sidecar_aligned = match shape.stats.as_ref() {
                Some(s) if s.has_parsed_stats => sidecar_load.with_partitions_parsed(parts)?,
                _ => sidecar_load
                    .with_json_stats_parsed(stats)?
                    .with_partitions_parsed(parts)?,
            };
            // Manifest top branch: drop sidecar then JSON-parse stats (manifest action rows
            // never carry native stats).
            let top = manifest
                .drop_col(SIDECAR_NAME)?
                .with_json_stats_parsed(stats)?
                .with_partitions_parsed(parts)?;
            Some(top.union_all(&[sidecar_aligned])?)
        }
    };

    // === Stage 5b: antijoin + union + retention -> reconciled ===========================
    let terminal_input = if let Some(view) = checkpoint_view {
        let keyed = view
            .filter(identity_not_null)?
            .append_col_typed(JOIN_KEY_FIELD.clone(), Arc::clone(&dedup_key))?;
        // `NodeKind::EquiJoin(EquiJoinNode { kind: LeftAnti, .. })` produces left-schema output
        // regardless of the right side's shape. Pass `commit_dedup` as-is and let the
        // engine's projection pushdown prune unused right-side columns.
        let survivors = keyed.left_anti_join(
            commit_dedup.clone(),
            [(col(FSR_JOIN_KEY_COL), col(FSR_JOIN_KEY_COL))],
        )?;
        commit_dedup.union_all(&[survivors])?
    } else {
        commit_dedup
    };

    terminal_input
        .filter(retention_filter(min_file_ts, txn_expiry))?
        .drop_col(FSR_JOIN_KEY_COL)
}

/// Async wrapper: resolve the scan shape (yielding `SchemaQuery` / `Reduce` phases as
/// needed) and then delegate to [`build_reconciliation`].
pub(super) async fn execute_reconciliation(
    ctx: &Context,
    engine: &mut Engine,
    snapshot: &Snapshot,
    base: &SchemaRef,
    stats: Option<SchemaRef>,
    parts: Option<SchemaRef>,
    dedup_key: ExpressionRef,
) -> Result<PlanBuilder, DeltaError> {
    let shape = ScanShape::resolve(ctx, engine, snapshot, stats.as_ref(), parts).await?;
    build_reconciliation(ctx, snapshot, &shape, base, dedup_key)
}

/// Helper: scan a leaf checkpoint and align it to the expected post-checkpoint shape.
/// `Some(StatsInfo { has_parsed_stats: true, .. })` declares `add.stats_parsed` directly in
/// the scan schema (parquet surfaces the parsed struct natively); the JSON-string and
/// no-stats arms scan with `base` and JSON-parse stats post-Load.
fn load_checkpoint_files(
    ctx: &Context,
    base: &SchemaRef,
    shape: &ScanShape,
    file_format: FileFormat,
    files: Vec<FileMeta>,
) -> Result<PlanBuilder, DeltaError> {
    let parts = shape.partition_schema.as_ref();
    let stats = shape.stats.as_ref().map(|s| &s.schema);
    let scan = match shape.stats.as_ref() {
        Some(s) if s.has_parsed_stats => {
            let scan_schema = stats_parsed_file_schema(base, &s.schema)?;
            match file_format {
                FileFormat::Parquet => ctx.scan_parquet(files, scan_schema)?,
                FileFormat::Json => ctx.scan_json(files, scan_schema)?,
            }
            .with_partitions_parsed(parts)?
        }
        _ => match file_format {
            FileFormat::Parquet => ctx.scan_parquet(files, base.clone())?,
            FileFormat::Json => ctx.scan_json(files, base.clone())?,
        }
        .with_json_stats_parsed(stats)?
        .with_partitions_parsed(parts)?,
    };
    Ok(scan)
}

// ============================================================================
// Reconciliation builder extension
// ============================================================================

/// Module-scoped extension trait providing fluent point-edits for the reconciliation
/// pipeline. Both methods *replace* (not append) -- the JSON / Map source columns have no
/// downstream consumer once their parsed form exists, so keeping both would just bloat
/// the projection.
pub(super) trait ReconciliationPlanBuilder: Sized {
    /// Replace `add.stats: STRING` with `add.stats_parsed: STRUCT<stats>` via
    /// [`Expression::parse_json`]. No-op when `stats` is `None`.
    fn with_json_stats_parsed(self, stats: Option<&SchemaRef>) -> Result<Self, DeltaError>;
    /// Replace `add.partitionValues: MAP<STRING, STRING>` with
    /// `add.partitionValues_parsed: STRUCT<parts>` via [`Expression::map_to_struct`].
    /// No-op when `parts` is `None`.
    fn with_partitions_parsed(self, parts: Option<&SchemaRef>) -> Result<Self, DeltaError>;
}

impl ReconciliationPlanBuilder for PlanBuilder {
    fn with_json_stats_parsed(self, stats: Option<&SchemaRef>) -> Result<Self, DeltaError> {
        let Some(stats_schema) = stats else {
            return Ok(self);
        };
        let new_field = StructField::nullable("stats_parsed", stats_schema);
        let new_expr = Expression::parse_json(col([ADD_NAME, "stats"]), Arc::clone(stats_schema));
        self.replace_col([ADD_NAME, "stats"], new_field, new_expr)
    }

    fn with_partitions_parsed(self, parts: Option<&SchemaRef>) -> Result<Self, DeltaError> {
        let Some(parts_schema) = parts else {
            return Ok(self);
        };
        let new_field = StructField::nullable("partitionValues_parsed", parts_schema);
        let new_expr = Expression::map_to_struct(col([ADD_NAME, "partitionValues"]));
        self.replace_col([ADD_NAME, "partitionValues"], new_field, new_expr)
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Manifest scan schema used by [`build_reconciliation`]: `base + sidecar`.
///
/// Building the schema from `base` (rather than the full action schema) lets the caller
/// recover the action stream by `drop_col(SIDECAR_NAME)` regardless of pipeline width
/// (scan = 2, FSR = 6).
fn manifest_action_schema(base: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<StructField> = base.fields().cloned().collect();
    fields.push(StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()));
    arc_schema(fields)
}

/// File schema for the sidecar parquet Load: `base` with `add.stats` swapped for
/// `add.stats_parsed: stats_schema` when the checkpoint advertises native parsed stats.
/// Other arms scan with `base` and JSON-parse `add.stats` post-Load.
fn sidecar_file_schema(
    base: &SchemaRef,
    stats: Option<&StatsInfo>,
) -> Result<SchemaRef, DeltaError> {
    match stats {
        Some(s) if s.has_parsed_stats => stats_parsed_file_schema(base, &s.schema),
        _ => Ok(Arc::clone(base)),
    }
}

/// Build a checkpoint Load `file_schema` that swaps `add.stats: STRING` for
/// `add.stats_parsed: stats_schema`. Parquet checkpoints with native parsed stats don't
/// carry the JSON form, so asking the engine for both columns would be wasted I/O.
fn stats_parsed_file_schema(
    base: &SchemaRef,
    stats_schema: &SchemaRef,
) -> Result<SchemaRef, DeltaError> {
    let new_field = StructField::nullable("stats_parsed", stats_schema.as_ref().clone());
    let new_struct = base
        .with_struct_at(&[ADD_NAME], |add| {
            add.with_field_replaced("stats", new_field)
        })
        .map_err(|source| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                source = source,
                "base schema must place `add` as a struct slot at index 0",
            )
        })?;
    Ok(Arc::new(new_struct))
}

/// `{path, size, version}` Values upstream schema for commit_load.
fn commit_load_schema() -> SchemaRef {
    arc_schema([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("version", DataType::LONG),
    ])
}

// ============================================================================
// Shared scan-pipeline helpers
// ============================================================================

/// Resolve the (deleted-file retention, txn expiry) timestamps for `snapshot`.
fn retention_timestamps(snapshot: &Snapshot) -> Result<(i64, Option<i64>), DeltaError> {
    let now = current_time_duration().map_err(|e| e.into_delta_default())?;
    let min_file_ts = deleted_file_retention_timestamp_with_time(
        snapshot.table_properties().deleted_file_retention_duration,
        now,
    )
    .map_err(|e| e.into_delta_default())?;
    let txn_expiry = calculate_transaction_expiration_timestamp(snapshot.table_properties())
        .map_err(|e| e.into_delta_default())?;
    Ok((min_file_ts, txn_expiry))
}

/// Convert Delta-log files (commit/compaction JSON) under `log_root` into Values rows
/// aligned to [`commit_load_schema`]: `{path, size, version}`. `path` is resolved relative
/// to `log_root`; `version` is recovered via [`ParsedLogPath`].
fn log_files_to_rows(log_root: &Url, files: Vec<FileMeta>) -> Result<Vec<Vec<Scalar>>, DeltaError> {
    files
        .into_iter()
        .map(|file| {
            let version = ParsedLogPath::try_from(file.clone())
                .map_err(|e| e.into_delta_default())?
                .ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaStateRecoverError,
                        "log_files_to_rows: file is not a log path: {}",
                        file.location,
                    )
                })?
                .version;
            Ok(vec![
                Scalar::String(path_under_log_root(log_root, &file.location)?),
                Scalar::Long(file.size as i64),
                Scalar::Long(version as i64),
            ])
        })
        .collect()
}

fn path_under_log_root(log_root: &Url, file: &Url) -> Result<String, DeltaError> {
    let base = log_root.path().trim_end_matches('/');
    let full = file.path();
    let suffix = full.strip_prefix(base).unwrap_or(full);
    Ok(suffix.trim_start_matches('/').to_string())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{AsArray, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::UnaryExpressionOp;
    use crate::schema::StructType;
    use crate::utils::test_utils::{parse_json_to_record_batch, string_array_to_engine_data};
    use crate::Engine as KernelEngine;

    fn names(fields: impl Iterator<Item = &'static str>) -> Vec<String> {
        fields.map(String::from).collect()
    }

    /// Pin canonical top-level action lists -- load-bearing for every downstream stage.
    #[test]
    fn bases_pin_canonical_action_slots() {
        let scan: Vec<_> = SCAN_BASE.fields().map(|f| f.name().clone()).collect();
        let fsr: Vec<_> = FSR_BASE.fields().map(|f| f.name().clone()).collect();
        assert_eq!(scan, names([ADD_NAME, REMOVE_NAME].into_iter()));
        assert_eq!(
            fsr,
            names(
                [
                    ADD_NAME,
                    REMOVE_NAME,
                    PROTOCOL_NAME,
                    METADATA_NAME,
                    DOMAIN_METADATA_NAME,
                    SET_TRANSACTION_NAME,
                ]
                .into_iter()
            )
        );
    }

    // === Dedup-key tests ==================================================================

    /// Pin the documented DV-offset gap (see `file_arm` docs): two DVs that share storage +
    /// path but differ in byte offset (only possible under `storageType = "p"`) collapse to
    /// the same in-plan dedup key, because `Expression::array` requires uniform element
    /// types and the IR has no `Cast(i64 -> string)` op. The executor running this plan
    /// MUST perform a final `(path, unique_id())` dedup. This test exists so that a future
    /// change which inadvertently lifts that gap surfaces here -- and the corresponding
    /// engine-side dedup contract can then be relaxed.
    #[test]
    fn fsr_dedup_key_collapses_dvs_that_differ_only_by_offset() {
        let line_a = r#"{"add":{"path":"p1.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"p","pathOrInlineDv":"shared","offset":7,"sizeInBytes":1,"cardinality":2}}}"#;
        let line_b = r#"{"add":{"path":"p1.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"p","pathOrInlineDv":"shared","offset":42,"sizeInBytes":1,"cardinality":2}}}"#;
        let batch = parse_json_to_record_batch(
            StringArray::from(vec![line_a, line_b]),
            FSR_BASE.clone(),
        );
        let out = evaluate_expression(
            &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
            &batch,
            Some(&DataType::STRING),
        )
        .unwrap();
        let s = out.as_string::<i32>();
        assert_eq!(
            s.value(0),
            s.value(1),
            "DVs differing only in offset must collide today; engine MUST run a final \
             (path, unique_id()) dedup. If you intend to fix this gap, update file_arm's \
             docs and the engine contract accordingly."
        );
    }

    #[test]
    fn fsr_dedup_key_eval_add_row_carries_path_and_dv_components() {
        // dv_unique_id is now `ToJson(Array(storageType, pathOrInlineDv))` rather than a
        // Plus-concatenated `unique_id_from_parts` string. Assert the encoded JSON carries the
        // path and DV identity components verbatim (any consumer that reduces over equality of
        // full JSON strings still dedups identical DVs).
        let line = r#"{"add":{"path":"p1.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"u","pathOrInlineDv":"dvpath","offset":7,"sizeInBytes":1,"cardinality":2}}}"#;
        let batch = parse_json_to_record_batch(StringArray::from(vec![line]), FSR_BASE.clone());

        let out = evaluate_expression(
            &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
            &batch,
            Some(&DataType::STRING),
        )
        .unwrap();
        let s = out.as_string::<i32>().value(0);
        // Outer Array(["file", path_coalesce, dv_coalesce]) JSON-encodes to a JSON array; the dv
        // arm is itself a JSON-encoded array string `["u","dvpath"]` embedded as a JSON string.
        assert!(
            s.contains("p1.parquet"),
            "expected `p1.parquet` in json={s}"
        );
        assert!(s.contains("dvpath"), "expected `dvpath` in json={s}");
        // The Plus-as-concat byte form must not appear (regression guard).
        let stringy_concat = DeletionVectorDescriptor::unique_id_from_parts("u", "dvpath", Some(7));
        assert!(
            !s.contains(&stringy_concat),
            "json contains the Plus-as-concat form `{stringy_concat}` -- dv_unique_id must use \
             ToJson(Array(...)), not Plus-as-string-concat: json={s}"
        );
    }

    #[test]
    fn fsr_dedup_key_eval_various_action_types() {
        let schema = FSR_BASE.clone();
        let check = |line: &str, subs: &[&str]| {
            let batch = parse_json_to_record_batch(StringArray::from(vec![line]), schema.clone());
            let out = evaluate_expression(
                &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
                &batch,
                Some(&DataType::STRING),
            )
            .unwrap();
            let s = out.as_string::<i32>().value(0);
            for sub in subs {
                assert!(s.contains(sub), "line={line} json={s} missing {sub}");
            }
        };

        check(
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            &["protocol"],
        );
        check(
            r#"{"metaData":{"id":"mid-9","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            &["metadata"],
        );
        check(
            r#"{"domainMetadata":{"domain":"dom-z","configuration":"conf-z","removed":false}}"#,
            &["dom-z"],
        );
        check(
            r#"{"txn":{"appId":"app-z","version":1,"lastUpdated":100}}"#,
            &["app-z"],
        );
        check(
            r#"{"remove":{"path":"r1.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            &["r1.parquet"],
        );
    }

    fn dedup_key_fixture_rows() -> StringArray {
        StringArray::from(vec![
            r#"{"add":{"path":"a.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#,
            r#"{"remove":{"path":"r.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            r#"{"domainMetadata":{"domain":"d1","configuration":"cfg","removed":false}}"#,
            r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":100}}"#,
            r#"{}"#,
        ])
    }

    #[rstest::rstest]
    #[case::fsr(fsr_dedup_key(), [true, true, true, true, true, true, false])]
    #[case::scan_file(
        scan_file_dedup_key(),
        [true, true, false, false, false, false, false]
    )]
    fn dedup_key_null_mask(#[case] key_expr: Expression, #[case] expected: [bool; 7]) {
        // The pipeline's identity filter is `dedup_key IS NOT NULL`; rows outside the keyed
        // action set must evaluate to NULL.
        let batch = parse_json_to_record_batch(dedup_key_fixture_rows(), FSR_BASE.clone());
        let out = evaluate_expression(
            &key_expr.is_not_null().into(),
            &batch,
            Some(&DataType::BOOLEAN),
        )
        .unwrap();
        let b = out.as_boolean();
        for (i, &want) in expected.iter().enumerate() {
            assert_eq!(b.value(i), want, "row {i} dedup-key null mask mismatch");
        }
    }

    // === Retention tests ==================================================================

    #[test]
    fn retention_filter_keeps_and_drops_tombstones() {
        let p = retention_filter(100, None);
        let engine = SyncEngine::new();
        let fields = [
            StructField::nullable(REMOVE_NAME, Remove::to_schema()),
            StructField::nullable("txn", SetTransaction::to_schema()),
        ];
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let rows = StringArray::from(vec![
            r#"{"remove":{"path":"old","deletionTimestamp":101,"dataChange":true,"partitionValues":{}}}"#
                .to_string(),
            r#"{"remove":{"path":"gone","deletionTimestamp":99,"dataChange":true,"partitionValues":{}}}"#
                .to_string(),
        ]);
        let parsed = engine
            .json_handler()
            .parse_json(string_array_to_engine_data(rows), schema)
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let arr = evaluate_expression(&p.into(), batch, Some(&DataType::BOOLEAN)).unwrap();
        let b = arr.as_boolean();
        assert!(b.value(0));
        assert!(!b.value(1));
    }

    /// Pin that `retention_filter` drops `domainMetadata.removed = true` tombstones,
    /// matching `ActionReconciliationVisitor::check_domain_metadata_action`. Without this
    /// arm, FSR would emit a winning row whose latest state is a tombstone -- which
    /// downstream consumers (`Snapshot::get_domain_metadata`) would then return as live
    /// domain state.
    #[test]
    fn retention_filter_drops_removed_domain_metadata_tombstones() {
        let p = retention_filter(0, None);
        let engine = SyncEngine::new();
        let fields = [StructField::nullable(
            "domainMetadata",
            DomainMetadata::to_schema(),
        )];
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let rows = StringArray::from(vec![
            r#"{"domainMetadata":{"domain":"live","configuration":"x","removed":false}}"#.to_string(),
            r#"{"domainMetadata":{"domain":"tomb","configuration":"x","removed":true}}"#.to_string(),
        ]);
        let parsed = engine
            .json_handler()
            .parse_json(string_array_to_engine_data(rows), schema)
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let arr = evaluate_expression(&p.into(), batch, Some(&DataType::BOOLEAN)).unwrap();
        let b = arr.as_boolean();
        assert!(b.value(0), "live domainMetadata must be kept");
        assert!(!b.value(1), "removed=true domainMetadata must be filtered");
    }
}
