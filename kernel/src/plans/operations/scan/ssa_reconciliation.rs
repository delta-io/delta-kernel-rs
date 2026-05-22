//! Shared FSR / Scan reconciliation pipeline (SSA flavor).
//!
//! Builds the canonical *window-on-commits + anti-join-on-checkpoint* pipeline against the
//! [`super::super::framework::plan_context`] (`Context` / `Cursor`) API and SSA IR. Consumed
//! by [`super::full_state::FullState::state_machine_ssa`] and
//! [`super::ssa_scan::build_scan_ssa`].
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
//! Returns a [`Cursor`] terminating on the reconciled stream; the caller drains it via
//! [`Context::into_result_plan`].

use std::sync::Arc;

use url::Url;

use super::action_pair::JOIN_KEY_FIELD;
use super::dedup::FSR_JOIN_KEY_COL;
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::{Sidecar, ADD_NAME, SIDECAR_NAME};
use crate::expressions::{
    col, column_expr, ColumnName, Expression, ExpressionRef, Predicate, PredicateRef, Scalar,
};
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{FileFormat, FileType, ScanFileColumns};
use crate::plans::kernel_consumers::SidecarCollector;
use crate::plans::operations::framework::coroutine::context::StepCo;
use crate::plans::operations::framework::plan_context::{Context, Cursor, LoadSpec};
use crate::schema::{arc_schema, DataType, SchemaRef, StructField, StructType, ToSchema};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta, Version};

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
    /// referenced sidecar parquet files via `Node::Load`.
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

/// Resolve the scan shape via a sequence of `Step::Consume` (sidecar URL extraction)
/// and `Step::SchemaQuery` (layout / stats probes) yields against `co`.
///
/// Yields through the SSA dispatch surface
/// ([`Context::consume`](crate::plans::operations::framework::plan_context::Context::consume) /
/// [`Context::schema_query`](crate::plans::operations::framework::plan_context::Context::schema_query)).
/// At most one top-level `SchemaQuery`, one `Consume` (V2 manifest sidecar URL extraction),
/// and one sidecar `SchemaQuery` are emitted.
pub(super) async fn resolve_shape_ssa(
    ctx: &Context,
    co: &mut StepCo,
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
            let url = first_checkpoint_url(snapshot)?;
            let cp_schema = ctx
                .schema_query(co, url, "ScanShapeInfoSsa::resolve::checkpoint_schema")
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
            co,
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
                    co,
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
/// [`Cursor`] terminating on the reconciled action stream. Caller wraps the result in
/// [`Context::into_result_plan`].
pub(super) fn build_reconciliation_ssa(
    ctx: &Context,
    snapshot: &Snapshot,
    shape: &SsaScanShape,
    base: &SchemaRef,
    dedup_key: ExpressionRef,
) -> Result<Cursor, DeltaError> {
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
        .values(path_size_schema(/* with_version= */ true), commit_rows)?
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
    // Returns `Some(Cursor)` aligned to `commit_dedup`'s schema minus JOIN_KEY (i.e. ready
    // for the antijoin's union arm), or `None` when the snapshot has no checkpoint at all.
    let checkpoint_view: Option<Cursor> = match &shape.checkpoint {
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
            // FSR = 6). The cursor branches: one chases sidecars; the other selects
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
        // `Node::EquiJoin { kind: LeftAnti }` produces left-schema output regardless of the
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
        .filter(retention_filter_predicate(min_file_ts, txn_expiry))?
        .drop_col(FSR_JOIN_KEY_COL)
}

/// Async wrapper: resolve the scan shape (yielding `SchemaQuery` / `Consume` phases as
/// needed) and then delegate to [`build_reconciliation_ssa`].
pub(super) async fn execute_reconciliation_ssa(
    ctx: &Context,
    co: &mut StepCo,
    snapshot: &Snapshot,
    base: &SchemaRef,
    stats: Option<SchemaRef>,
    parts: Option<SchemaRef>,
    dedup_key: ExpressionRef,
) -> Result<Cursor, DeltaError> {
    let shape = resolve_shape_ssa(ctx, co, snapshot, stats.as_ref(), parts).await?;
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
) -> Result<Cursor, DeltaError> {
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
// Reconciliation cursor extension
// ============================================================================

/// Module-scoped extension trait providing fluent point-edits for the reconciliation
/// pipeline. Both methods *replace* (not append) -- the JSON / Map source columns have no
/// downstream consumer once their parsed form exists, so keeping both would just bloat
/// the projection.
pub(super) trait ReconciliationCursor: Sized {
    /// Replace `add.stats: STRING` with `add.stats_parsed: STRUCT<stats>` via
    /// [`Expression::parse_json`]. No-op when `stats` is `None`.
    fn with_json_stats_parsed(self, stats: Option<&SchemaRef>) -> Result<Self, DeltaError>;
    /// Replace `add.partitionValues: MAP<STRING, STRING>` with
    /// `add.partitionValues_parsed: STRUCT<parts>` via [`Expression::map_to_struct`].
    /// No-op when `parts` is `None`.
    fn with_partitions_parsed(self, parts: Option<&SchemaRef>) -> Result<Self, DeltaError>;
}

impl ReconciliationCursor for Cursor {
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

fn first_checkpoint_url(snapshot: &Snapshot) -> Result<String, DeltaError> {
    snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .first()
        .map(|p| p.location.location.as_str().to_string())
        .ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "first_checkpoint_url: no checkpoint parts",
            )
        })
}

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

/// `{path, size, version?}` Values upstream schema for commit_load.
fn path_size_schema(with_version: bool) -> SchemaRef {
    let mut fields = vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
    ];
    if with_version {
        fields.push(StructField::not_null("version", DataType::LONG));
    }
    arc_schema(fields)
}

/// Bridge to [`super::retention::retention_filter`] returning a [`Predicate`].
fn retention_filter_predicate(min_file_ts: i64, txn_expiry: Option<i64>) -> Predicate {
    super::retention::retention_filter(min_file_ts, txn_expiry)
}

// ============================================================================
// Shared scan-pipeline helpers (moved from the deleted legacy `plans` module)
// ============================================================================

/// One literal row describing a Delta JSON commit file. Public so external tools that
/// consume the FSR commit file row layout can refer to it by name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitFileMeta {
    pub path: String,
    pub size: i64,
    pub version: Version,
}

/// File-meta column hints shared by every Load: `path` column, optional `size` column,
/// no record-count column.
pub(super) fn default_scan_file_columns() -> ScanFileColumns {
    ScanFileColumns {
        path: ColumnName::new(["path"]),
        size: Some(ColumnName::new(["size"])),
        record_count: None,
    }
}

/// Resolve the (deleted-file retention, txn expiry) timestamps for `snapshot`.
pub(super) fn retention_timestamps(snapshot: &Snapshot) -> Result<(i64, Option<i64>), DeltaError> {
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
pub(super) fn commit_cover_rows(seg: &LogSegment) -> Result<Vec<CommitFileMeta>, DeltaError> {
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
