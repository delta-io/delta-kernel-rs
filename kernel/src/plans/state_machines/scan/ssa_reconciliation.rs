//! Shared FSR / Scan reconciliation pipeline (SSA flavor).
//!
//! Builds the canonical *window-on-commits + anti-join-on-checkpoint* pipeline against the
//! [`super::super::framework::plan_context`] (`Context` / `PlanBuilder`) API and SSA IR. Consumed
//! by [`super::full_state::FullState::state_machine`] and [`super::ssa_scan::build_scan_ssa`].
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

use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
    SIDECAR_NAME,
};
use crate::expressions::{
    col, column_expr, ColumnName, Expression, ExpressionRef, Predicate, PredicateRef, Scalar,
};
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{default_scan_file_columns, FileFormat, FileType, ScanFileColumns};
use crate::plans::kernel_consumers::SidecarCollector;
use crate::plans::state_machines::framework::coroutine::context::Engine;
use crate::plans::state_machines::framework::plan_context::{Context, LoadSpec, PlanBuilder};
use crate::schema::{
    arc_schema, ArrayType, DataType, SchemaRef, StructField, StructType, ToSchema,
};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta, Version};

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
    StructField::nullable(
        FSR_JOIN_KEY_COL,
        DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
    )
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
    Expression::literal(Scalar::Null(DataType::Array(Box::new(ArrayType::new(
        DataType::STRING,
        true,
    )))))
}

/// Predicate that selects rows whose `add.path` OR `remove.path` is non-null.
fn is_file_row() -> Predicate {
    Predicate::or(col(ADD_PATH).is_not_null(), col(REMOVE_PATH).is_not_null())
}

/// `["file", path_coalesce, dv_storage_coalesce, dv_inline_coalesce]` -- the file-row arm of
/// both dedup keys.
fn file_arm() -> Expression {
    Expression::array(vec![
        Expression::literal("file"),
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
    let null_str = || Expression::literal(Scalar::Null(DataType::STRING));
    let arm = |kind: &str, id1: Expression, id2: Expression, id3: Expression| {
        Expression::array(vec![Expression::literal(kind), id1, id2, id3])
    };
    Expression::case_when(
        vec![
            (is_file_row(), file_arm()),
            (
                col(["protocol"]).is_not_null(),
                arm(PROTOCOL_NAME, null_str(), null_str(), null_str()),
            ),
            // Metadata is a singleton table state action: latest row wins regardless of prior id.
            (
                col(METADATA_ID).is_not_null(),
                arm("metadata", null_str(), null_str(), null_str()),
            ),
            // Domain metadata is keyed by domain; newer rows replace older configs for that
            // domain.
            (
                col(["domainMetadata"]).is_not_null(),
                arm(
                    DOMAIN_METADATA_NAME,
                    col(["domainMetadata", "domain"]),
                    null_str(),
                    null_str(),
                ),
            ),
            (
                col(["txn"]).is_not_null(),
                arm(
                    SET_TRANSACTION_NAME,
                    col(["txn", "appId"]),
                    null_str(),
                    null_str(),
                ),
            ),
        ],
        null_string_array(),
    )
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
/// [`crate::action_reconciliation::log_replay::ActionReconciliationVisitor::is_expired_tombstone`]
/// and txn retention checks in the same visitor (`kernel/src/action_reconciliation/log_replay.rs`).
///
/// `txn_expiry` is `None` when `delta.setTransactionRetentionDuration` is unset -- txn rows
/// are not filtered by age.
fn retention_filter(min_file_ts: i64, txn_expiry: Option<i64>) -> Predicate {
    let remove_ok = Predicate::or(
        col(["remove"]).is_null(),
        col(REMOVE_DELETION_TIMESTAMP)
            .or_lit(0i64)
            .gt(Expression::literal(min_file_ts)),
    );
    let txn_ok = match txn_expiry {
        None => Predicate::literal(true),
        Some(cutoff) => Predicate::or_from([
            col(["txn"]).is_null(),
            col(TXN_LAST_UPDATED).is_null(),
            col(TXN_LAST_UPDATED).gt(Expression::literal(cutoff)),
        ]),
    };
    Predicate::and(remove_ok, txn_ok)
}

// ============================================================================
// Shape resolution (SSA flavor)
// ============================================================================

/// Topology of a snapshot's checkpoint(s).
///
/// All variants are `pub(super)` only; this is a local IR for the SSA reconciliation pipeline.
#[derive(Clone, Debug)]
pub(super) enum SsaCheckpointShape {
    /// No checkpoint files: reconciliation degenerates to commit-only replay.
    None,
    /// Classic checkpoint with `add`/`remove` rows inline. The reconciliation re-scans
    /// `files` directly.
    Inline {
        files: Vec<FileMeta>,
        file_format: FileFormat,
    },
    /// V2 multipart manifest: `files` are the manifest parts. The reconciliation
    /// re-scans them, filters down to sidecar pointer rows, and lazily loads the
    /// referenced sidecar parquet files via `NodeKind::Load`.
    Manifest {
        files: Vec<FileMeta>,
        file_format: FileFormat,
    },
}

/// Configured SSA reconciliation shape: checkpoint topology + stats + partitioning.
#[derive(Clone, Debug)]
pub(super) struct SsaScanShape {
    pub(super) checkpoint: SsaCheckpointShape,
    /// Stats wiring. `None` means the caller didn't request stats. `Some` carries the
    /// projected stats schema along with the on-disk layout the snapshot's checkpoint
    /// uses for column stats.
    pub(super) stats: Option<SsaStatsInfo>,
    pub(super) partition_schema: Option<SchemaRef>,
}

/// Stats wiring: the projected stats schema plus the on-disk layout the snapshot's
/// checkpoint files use for column stats.
#[derive(Clone, Debug)]
pub(super) struct SsaStatsInfo {
    pub(super) schema: SchemaRef,
    pub(super) checkpoint_layout: CheckpointStatsLayout,
}

/// How the snapshot's checkpoint files store column stats. The variant names mirror the
/// protocol's column names (`add.stats_parsed` for the parsed struct, `add.stats` for the
/// JSON-string form).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CheckpointStatsLayout {
    /// Parsed struct (parquet `add.stats_parsed`).
    StatsParsed,
    /// JSON string (parquet/JSON `add.stats`).
    StatsJson,
}

/// Resolve the scan shape via a sequence of `EngineRequest::Consume` (sidecar URL extraction)
/// and `EngineRequest::SchemaQuery` (layout / stats probes) yields against `engine`.
///
/// Yields through the SSA dispatch surface
/// ([`Context::consume`](crate::plans::state_machines::framework::plan_context::Context::consume) /
/// [`Context::schema_query`](crate::plans::state_machines::framework::plan_context::Context::schema_query)).
/// At most one top-level `SchemaQuery`, one `Consume` (V2 manifest sidecar URL extraction),
/// and one sidecar `SchemaQuery` are emitted.
pub(super) async fn resolve_shape_ssa(
    ctx: &Context,
    engine: &mut Engine,
    snapshot: &Snapshot,
    stats_schema: Option<&SchemaRef>,
    partition_schema: Option<SchemaRef>,
) -> Result<SsaScanShape, DeltaError> {
    let seg = snapshot.log_segment();
    let checkpoint_parts = &seg.listed.checkpoint_parts;

    let make_info = |checkpoint, has_parsed_stats| SsaScanShape {
        checkpoint,
        stats: stats_schema.cloned().map(|schema| SsaStatsInfo {
            schema,
            checkpoint_layout: if has_parsed_stats {
                CheckpointStatsLayout::StatsParsed
            } else {
                CheckpointStatsLayout::StatsJson
            },
        }),
        partition_schema: partition_schema.clone(),
    };

    if checkpoint_parts.is_empty() {
        return Ok(make_info(SsaCheckpointShape::None, false));
    }

    let file_format = checkpoint_format_from_path(&checkpoint_parts[0]);
    let files: Vec<FileMeta> = checkpoint_parts
        .iter()
        .map(|p| p.location.clone())
        .collect();

    // Hint probe is authoritative -- `_last_checkpoint` describes the leaf file.
    let mut parsed_stats: Option<bool> =
        stats_probe(seg.checkpoint_schema().as_ref(), stats_schema);

    let is_manifest = match file_format {
        FileFormat::Parquet => {
            let url = checkpoint_parts[0].location.location.as_str().to_string();
            let cp_schema = ctx
                .schema_query(engine, url, "ScanShapeInfoSsa::resolve::checkpoint_schema")
                .await?;
            let is_mfst = cp_schema.contains(SIDECAR_NAME);
            if !is_mfst && parsed_stats.is_none() {
                parsed_stats = stats_probe(Some(&cp_schema), stats_schema);
            }
            is_mfst
        }
        FileFormat::Json => true,
    };

    if !is_manifest {
        return Ok(make_info(
            SsaCheckpointShape::Inline { files, file_format },
            parsed_stats.unwrap_or(false),
        ));
    }

    // V2 manifest: drain a `SidecarCollector` over (manifest_scan -> filter SIDECAR not null)
    // to recover sidecar URLs. The manifest is re-scanned in the build phase; this scan is
    // for the sidecar SchemaQuery probe only.
    let manifest_chain = ctx.scan(file_format, files.clone(), manifest_probe_schema())?;
    let sidecar_chain = manifest_chain.filter(col([SIDECAR_NAME]).is_not_null())?;
    let sidecar_files = ctx
        .consume(
            engine,
            sidecar_chain,
            SidecarCollector::new(snapshot.log_segment().log_root.clone()),
            "ScanShapeInfoSsa::resolve::sidecar_extract",
        )
        .await?;

    // Sidecar SchemaQuery probe (only if stats are requested AND hint didn't already answer).
    if parsed_stats.is_none() {
        if let (Some(reqd), Some(first)) = (stats_schema, sidecar_files.first()) {
            let side_schema = ctx
                .schema_query(
                    engine,
                    first.location.as_str().to_string(),
                    "ScanShapeInfoSsa::resolve::sidecar_schema",
                )
                .await?;
            parsed_stats = Some(LogSegment::schema_has_compatible_stats_parsed(
                side_schema.as_ref(),
                reqd.as_ref(),
            ));
        }
    }

    Ok(make_info(
        SsaCheckpointShape::Manifest { files, file_format },
        parsed_stats.unwrap_or(false),
    ))
}

// ============================================================================
// Reconciliation builder (SSA flavor)
// ============================================================================

/// Sync core of the SSA reconciliation. Builds against the supplied `ctx` and returns a
/// [`PlanBuilder`] terminating on the reconciled action stream. Caller wraps the result in
/// [`Context::into_result_plan`].
pub(super) fn build_reconciliation_ssa(
    ctx: &Context,
    snapshot: &Snapshot,
    shape: &SsaScanShape,
    base: &SchemaRef,
    dedup_key: ExpressionRef,
) -> Result<PlanBuilder, DeltaError> {
    let stats = shape.stats.as_ref().map(|s| &s.schema);
    let parts = shape.partition_schema.as_ref();
    let identity_not_null: PredicateRef = Arc::new(dedup_key.as_ref().clone().is_not_null());

    let commits = commit_cover_rows(snapshot.log_segment())?;
    let log_root = snapshot.log_segment().log_root.clone();
    let (min_file_ts, txn_expiry) = retention_timestamps(snapshot)?;

    // === Stage 1: commit_load ===========================================================
    // VALUES(commits) -> Load(JSON) broadcasts the per-commit `version` column onto every
    // emitted action row.
    let commit_rows: Vec<Vec<Scalar>> = commits
        .iter()
        .map(|c| {
            vec![
                Scalar::String(c.path.clone()),
                Scalar::Long(c.size),
                Scalar::Long(c.version as i64),
            ]
        })
        .collect();
    let commit_raw = ctx
        .values(commit_load_schema(), commit_rows)?
        .load(LoadSpec {
            file_schema: Arc::clone(base),
            file_type: FileType::Json,
            base_url: Some(log_root.clone()),
            passthrough_columns: vec![ColumnName::new(["version"])],
            file_meta: default_scan_file_columns(),
            dv_ref: None,
        })?;

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
        SsaCheckpointShape::None => None,
        SsaCheckpointShape::Inline { files, file_format } => Some(load_checkpoint_files(
            ctx,
            base,
            shape,
            *file_format,
            files.clone(),
        )?),
        SsaCheckpointShape::Manifest { files, file_format } => {
            // Re-scan the manifest with `base + sidecar` so that `drop_col(SIDECAR_NAME)`
            // recovers the action stream regardless of pipeline base width (scan = 2,
            // FSR = 6). The builder branches: one chases sidecars; the other selects
            // manifest-resident action rows directly.
            let manifest = ctx.scan(*file_format, files.clone(), manifest_action_schema(base))?;
            let sidecar_base = log_root.join("_sidecars/").map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaStateRecoverError,
                    "build_reconciliation_ssa: join _sidecars base URL: {e}",
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
                .load(LoadSpec {
                    file_schema: sidecar_file_schema(base, shape.stats.as_ref()),
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
                Some(s) if s.checkpoint_layout == CheckpointStatsLayout::StatsParsed => {
                    sidecar_load.with_partitions_parsed(parts)?
                }
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
        // `NodeKind::EquiJoin { kind: LeftAnti }` produces left-schema output regardless of the
        // right side's shape. Pass `commit_dedup` as-is and let the engine's projection
        // pushdown prune unused right-side columns.
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

/// Async wrapper: resolve the scan shape (yielding `SchemaQuery` / `Consume` phases as
/// needed) and then delegate to [`build_reconciliation_ssa`].
pub(super) async fn execute_reconciliation_ssa(
    ctx: &Context,
    engine: &mut Engine,
    snapshot: &Snapshot,
    base: &SchemaRef,
    stats: Option<SchemaRef>,
    parts: Option<SchemaRef>,
    dedup_key: ExpressionRef,
) -> Result<PlanBuilder, DeltaError> {
    let shape = resolve_shape_ssa(ctx, engine, snapshot, stats.as_ref(), parts).await?;
    build_reconciliation_ssa(ctx, snapshot, &shape, base, dedup_key)
}

/// Helper: scan an inline checkpoint and align it to the expected post-checkpoint shape.
/// `Some(StatsParsed)` declares `add.stats_parsed` directly in the scan schema (parquet
/// surfaces the parsed struct natively); `Some(StatsJson)` and `None` scan with `base` and
/// JSON-parse stats post-Load.
fn load_checkpoint_files(
    ctx: &Context,
    base: &SchemaRef,
    shape: &SsaScanShape,
    file_format: FileFormat,
    files: Vec<FileMeta>,
) -> Result<PlanBuilder, DeltaError> {
    let parts = shape.partition_schema.as_ref();
    let stats = shape.stats.as_ref().map(|s| &s.schema);
    let scan = match shape.stats.as_ref() {
        Some(s) if s.checkpoint_layout == CheckpointStatsLayout::StatsParsed => {
            let scan_schema = stats_parsed_file_schema(base, &s.schema);
            ctx.scan(file_format, files, scan_schema)?
                .with_partitions_parsed(parts)?
        }
        _ => ctx
            .scan(file_format, files, Arc::clone(base))?
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
        let new_field = StructField::nullable("stats_parsed", stats_schema.as_ref().clone());
        let new_expr = Expression::parse_json(col([ADD_NAME, "stats"]), Arc::clone(stats_schema));
        self.replace_col([ADD_NAME, "stats"], new_field, new_expr)
    }

    fn with_partitions_parsed(self, parts: Option<&SchemaRef>) -> Result<Self, DeltaError> {
        let Some(parts_schema) = parts else {
            return Ok(self);
        };
        let new_field =
            StructField::nullable("partitionValues_parsed", parts_schema.as_ref().clone());
        let new_expr = Expression::map_to_struct(col([ADD_NAME, "partitionValues"]));
        self.replace_col([ADD_NAME, "partitionValues"], new_field, new_expr)
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn checkpoint_format_from_path(cp: &ParsedLogPath<FileMeta>) -> FileFormat {
    if cp.extension == "json" {
        FileFormat::Json
    } else {
        FileFormat::Parquet
    }
}

fn stats_probe(leaf: Option<&SchemaRef>, requested: Option<&SchemaRef>) -> Option<bool> {
    let (l, r) = (leaf?, requested?);
    Some(LogSegment::schema_has_compatible_stats_parsed(
        l.as_ref(),
        r.as_ref(),
    ))
}

/// Manifest scan schema for the V2-multipart `is_manifest` probe in
/// [`resolve_shape_ssa`]. Only the `sidecar` column is consumed downstream
/// (`SidecarCollector` reads `sidecar.path` / `sidecar.sizeInBytes`); narrowing the scan
/// to that one column avoids paying the read cost for the action columns.
fn manifest_probe_schema() -> SchemaRef {
    arc_schema([StructField::nullable(SIDECAR_NAME, Sidecar::to_schema())])
}

/// Manifest scan schema used by [`build_reconciliation_ssa`]: `base + sidecar`.
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
fn sidecar_file_schema(base: &SchemaRef, stats: Option<&SsaStatsInfo>) -> SchemaRef {
    match stats {
        Some(s) if s.checkpoint_layout == CheckpointStatsLayout::StatsParsed => {
            stats_parsed_file_schema(base, &s.schema)
        }
        _ => Arc::clone(base),
    }
}

/// Build a checkpoint Load `file_schema` that swaps `add.stats: STRING` for
/// `add.stats_parsed: stats_schema`. Parquet checkpoints with native parsed stats don't
/// carry the JSON form, so asking the engine for both columns would be wasted I/O.
fn stats_parsed_file_schema(base: &SchemaRef, stats_schema: &SchemaRef) -> SchemaRef {
    let new_fields: Vec<StructField> = base
        .fields()
        .map(|f| {
            if f.name() != ADD_NAME {
                return f.clone();
            }
            let DataType::Struct(add) = f.data_type() else {
                unreachable!("base places `add` as a struct slot at index 0");
            };
            let new_add = StructType::new_unchecked(add.fields().map(|inner| {
                if inner.name() == "stats" {
                    StructField::nullable("stats_parsed", stats_schema.as_ref().clone())
                } else {
                    inner.clone()
                }
            }));
            StructField::new(
                ADD_NAME,
                DataType::Struct(Box::new(new_add)),
                f.is_nullable(),
            )
        })
        .collect();
    arc_schema(new_fields)
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

/// One literal row describing a Delta JSON commit file. Public so external tools that
/// consume the FSR commit file row layout can refer to it by name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitFileMeta {
    pub path: String,
    pub size: i64,
    pub version: Version,
}

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

/// Materialize the minimal set of commit / compaction file rows that cover the log segment,
/// preferring compactions over the commits they subsume. Delegates the cover-selection logic
/// to [`crate::log_segment::LogSegment::find_commit_cover`] and re-parses each file's
/// [`ParsedLogPath`] to recover the version.
fn commit_cover_rows(seg: &LogSegment) -> Result<Vec<CommitFileMeta>, DeltaError> {
    seg.find_commit_cover()
        .into_iter()
        .map(|file| {
            let version = ParsedLogPath::try_from(file.clone())
                .map_err(|e| e.into_delta_default())?
                .ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaStateRecoverError,
                        "commit_cover_rows: cover yielded a non-log-path file: {}",
                        file.location,
                    )
                })?
                .version;
            Ok(CommitFileMeta {
                path: path_under_log_root(&seg.log_root, &file.location)?,
                size: file.size as i64,
                version,
            })
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
            "json contains the legacy concat form `{stringy_concat}` -- dv_unique_id must use \
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

    #[test]
    fn fsr_dedup_key_is_null_for_unknown_rows() {
        // The pipeline's identity filter is `dedup_key IS NOT NULL`, so the dedup-key MUST
        // evaluate to NULL on rows that match no known slot. This is the load-bearing
        // contract.
        let rows = StringArray::from(vec![
            r#"{"add":{"path":"a.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#,
            r#"{"remove":{"path":"r.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            r#"{"domainMetadata":{"domain":"d1","configuration":"cfg","removed":false}}"#,
            r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":100}}"#,
            r#"{}"#,
        ]);
        let batch = parse_json_to_record_batch(rows, FSR_BASE.clone());
        let out = evaluate_expression(
            &fsr_dedup_key().is_not_null().into(),
            &batch,
            Some(&DataType::BOOLEAN),
        )
        .unwrap();
        let b = out.as_boolean();
        assert!(b.value(0), "add row should produce non-NULL dedup key");
        assert!(b.value(1), "remove row should produce non-NULL dedup key");
        assert!(b.value(2), "protocol row should produce non-NULL dedup key");
        assert!(b.value(3), "metaData row should produce non-NULL dedup key");
        assert!(
            b.value(4),
            "domainMetadata row should produce non-NULL dedup key"
        );
        assert!(b.value(5), "txn row should produce non-NULL dedup key");
        assert!(!b.value(6), "empty row should produce NULL dedup key");
    }

    #[test]
    fn scan_file_dedup_key_is_null_for_non_file_rows() {
        // The scan pipeline only cares about add/remove rows; everything else should be
        // filtered out via `dedup_key IS NOT NULL`.
        let rows = StringArray::from(vec![
            r#"{"add":{"path":"a.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#,
            r#"{"remove":{"path":"r.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            r#"{"domainMetadata":{"domain":"d1","configuration":"cfg","removed":false}}"#,
            r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":100}}"#,
            r#"{}"#,
        ]);
        let batch = parse_json_to_record_batch(rows, FSR_BASE.clone());
        let out = evaluate_expression(
            &scan_file_dedup_key().is_not_null().into(),
            &batch,
            Some(&DataType::BOOLEAN),
        )
        .unwrap();
        let b = out.as_boolean();
        assert!(b.value(0), "add row should produce non-NULL scan dedup key");
        assert!(
            b.value(1),
            "remove row should produce non-NULL scan dedup key"
        );
        assert!(
            !b.value(2),
            "protocol row should produce NULL scan dedup key"
        );
        assert!(
            !b.value(3),
            "metaData row should produce NULL scan dedup key"
        );
        assert!(
            !b.value(4),
            "domainMetadata row should produce NULL scan dedup key"
        );
        assert!(!b.value(5), "txn row should produce NULL scan dedup key");
        assert!(!b.value(6), "empty row should produce NULL scan dedup key");
    }

    // === Retention tests ==================================================================

    #[test]
    fn retention_filter_keeps_and_drops_tombstones() {
        let p = retention_filter(100, None);
        let engine = SyncEngine::new();
        let schema = Arc::new(
            StructType::try_new([
                StructField::nullable(REMOVE_NAME, Remove::to_schema()),
                StructField::nullable("txn", SetTransaction::to_schema()),
            ])
            .unwrap(),
        );
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
}
