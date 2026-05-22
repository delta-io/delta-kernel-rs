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

use super::action_pair::{augment_add, Pair, JOIN_KEY_FIELD, VERSION_FIELD};
use super::dedup::FSR_JOIN_KEY_COL;
use super::schemas::action_schema;
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::{Sidecar, SIDECAR_NAME};
use crate::expressions::{col, ColumnName, Expression, ExpressionRef, Predicate, Scalar};
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{FileFormat, FileType, ScanFileColumns};
use crate::plans::kernel_consumers::SidecarCollector;
use crate::plans::operations::framework::coroutine::context::StepCo;
use crate::plans::operations::framework::plan_context::{Context, Cursor, LoadSpec};
use crate::schema::{arc_schema, ArrayType, DataType, SchemaRef, StructField, ToSchema};
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
    pub(super) stats: SsaStatsInfo,
    pub(super) partition_schema: Option<SchemaRef>,
}

/// Stats wiring: the optional projected stats schema and whether the checkpoint stores
/// parsed (struct-typed) stats vs. JSON-string stats that need parsing.
#[derive(Clone, Debug)]
pub(super) struct SsaStatsInfo {
    pub(super) stats_schema: Option<SchemaRef>,
    pub(super) has_parsed_stats: bool,
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
        stats: SsaStatsInfo {
            stats_schema: stats_schema.cloned(),
            has_parsed_stats,
        },
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
    let manifest_schema = checkpoint_manifest_scan_schema();
    let manifest_chain = match file_format {
        FileFormat::Parquet => ctx.scan_parquet(files.clone(), manifest_schema)?,
        FileFormat::Json => ctx.scan_json(files.clone(), manifest_schema)?,
    };
    let sidecar_chain = manifest_chain.filter(Arc::new(col([SIDECAR_NAME]).is_not_null()))?;
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
    base: &Pair,
    dedup_key: Arc<Expression>,
) -> Result<Cursor, DeltaError> {
    let stats_for = |has_parsed: bool| {
        shape
            .stats
            .stats_schema
            .as_ref()
            .map(|s| (s.clone(), has_parsed))
    };
    let commit_pair = augment_add(
        base.clone(),
        stats_for(false),
        shape.partition_schema.clone(),
    );
    let checkpoint_pair = augment_add(
        base.clone(),
        stats_for(shape.stats.has_parsed_stats),
        shape.partition_schema.clone(),
    );
    // The checkpoint-side projection is identity-equivalent to the commit-side projection
    // when no augmentation is needed. Skip the project node entirely in that case.
    let checkpoint_project_is_identity = shape.partition_schema.is_none()
        && (shape.stats.stats_schema.is_none() || shape.stats.has_parsed_stats);

    // Reused by commit dedup and the antijoin path.
    let identity_not_null: Arc<Predicate> = Arc::new(dedup_key.as_ref().clone().is_not_null());

    let commits = commit_cover_rows(snapshot.log_segment())?;
    let log_root = snapshot.log_segment().log_root.clone();
    let (min_file_ts, txn_expiry) = retention_timestamps(snapshot)?;

    // === Stage 1: commit_load ============================================================
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
            file_schema: base.0.clone(),
            file_type: FileType::Json,
            base_url: Some(log_root.clone()),
            passthrough_columns: vec![ColumnName::new(["version"])],
            file_meta: default_scan_file_columns(),
            dv_ref: None,
        })?;

    // === Stage 2: commit_dedup ===========================================================
    // filter(identity) -> single project that emits commit_pair + JOIN_KEY + VERSION
    // (all expressions reference `commit_raw`'s schema, which carries `version` as a
    // passthrough column) -> max_by_version. The narrowed output is `commit_pair +
    // JOIN_KEY` (VERSION dropped). We must fold the three column additions into one
    // Project so that `col("version")` is still in scope when it is referenced --
    // splitting them would lose `version` after the first Project.
    let commit_pair_with_keys_schema: SchemaRef = {
        let mut fields: Vec<StructField> = commit_pair.0.fields().cloned().collect();
        fields.push(JOIN_KEY_FIELD.clone());
        fields.push(VERSION_FIELD.clone());
        arc_schema(fields)
    };
    let mut commit_pair_with_keys_exprs: Vec<ExpressionRef> = commit_pair.1.clone();
    commit_pair_with_keys_exprs.push(Arc::clone(&dedup_key));
    commit_pair_with_keys_exprs.push(Arc::new(col("version")) as ExpressionRef);
    let mut value_columns: Vec<String> = commit_pair.0.fields().map(|f| f.name().clone()).collect();
    value_columns.push(FSR_JOIN_KEY_COL.to_string());
    let commit_dedup = commit_raw
        .filter(Arc::clone(&identity_not_null))?
        .project_with_schema(commit_pair_with_keys_exprs, commit_pair_with_keys_schema)?
        .max_by_version(
            vec![Arc::new(col(FSR_JOIN_KEY_COL)) as ExpressionRef],
            Arc::new(col("version")),
            value_columns,
        )?;

    // === Stages 3-5a: build the checkpoint view per shape variant ========================
    // Returns `Some(Cursor)` aligned to `checkpoint_pair.0 + JOIN_KEY` (i.e. ready for the
    // antijoin), or `None` when the snapshot has no checkpoint at all.
    let checkpoint_view: Option<Cursor> = match &shape.checkpoint {
        SsaCheckpointShape::None => None,
        SsaCheckpointShape::Inline { files, file_format } => {
            let scan_schema = checkpoint_pair.0.clone();
            let scan = match file_format {
                FileFormat::Parquet => ctx.scan_parquet(files.clone(), scan_schema)?,
                FileFormat::Json => ctx.scan_json(files.clone(), scan_schema)?,
            };
            let aligned = if checkpoint_project_is_identity {
                scan
            } else {
                scan.project_with_schema(checkpoint_pair.1.clone(), checkpoint_pair.0.clone())?
            };
            Some(aligned)
        }
        SsaCheckpointShape::Manifest { files, file_format } => {
            // Re-scan the manifest. The cursor branches: one branch chases sidecars; the
            // other selects manifest-resident action rows directly.
            let manifest_schema = checkpoint_manifest_scan_schema();
            let manifest = match file_format {
                FileFormat::Parquet => ctx.scan_parquet(files.clone(), manifest_schema)?,
                FileFormat::Json => ctx.scan_json(files.clone(), manifest_schema)?,
            };
            let sidecar_base = log_root.join("_sidecars/").map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaStateRecoverError,
                    "build_reconciliation_ssa: join _sidecars base URL: {e}",
                )
            })?;
            // Sidecar branch: filter -> project (path/sizeInBytes) -> Load. The Load's
            // `file_schema` must be `checkpoint_pair.0` (not `base.0`) so the engine
            // surfaces native `add.stats_parsed` / `add.partitionValues_parsed` from the
            // sidecar parquet directly. With identity checkpoint projection the loaded
            // schema is the union arm shape; otherwise it is reprojected below.
            let sidecar_load = manifest
                .clone()
                .filter(Arc::new(col([SIDECAR_NAME]).is_not_null()))?
                .project([
                    (
                        "path",
                        Arc::new(col([SIDECAR_NAME, "path"])) as ExpressionRef,
                    ),
                    (
                        "sizeInBytes",
                        Arc::new(col([SIDECAR_NAME, "sizeInBytes"])) as ExpressionRef,
                    ),
                ])?
                .load(LoadSpec {
                    file_schema: checkpoint_pair.0.clone(),
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
            // Manifest top branch: select the action slots out of the manifest schema (drop
            // the `sidecar` column) and then apply `commit_pair` (parse_json stats since
            // manifest action rows never carry native stats).
            let top = manifest
                .select_columns(base.0.fields().map(|f| f.name().clone()))?
                .project_with_schema(commit_pair.1.clone(), commit_pair.0.clone())?;
            let side = if checkpoint_project_is_identity {
                sidecar_load
            } else {
                sidecar_load
                    .project_with_schema(checkpoint_pair.1.clone(), checkpoint_pair.0.clone())?
            };
            Some(top.union_all(&[side])?)
        }
    };

    // === Stage 5b: antijoin + union + retention -> reconciled ============================
    let terminal_input = if let Some(view) = checkpoint_view {
        let keyed = view
            .filter(Arc::clone(&identity_not_null))?
            .project_with_schema(checkpoint_pair.1.clone(), checkpoint_pair.0.clone())?
            .append_col_typed(JOIN_KEY_FIELD.clone(), Arc::clone(&dedup_key))?;
        let join_key_only_schema = arc_schema([StructField::nullable(
            FSR_JOIN_KEY_COL,
            DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
        )]);
        let commit_keys = commit_dedup.clone().project_with_schema(
            vec![Arc::new(col(FSR_JOIN_KEY_COL)) as ExpressionRef],
            join_key_only_schema,
        )?;
        // SSA convention: `left_anti_join` returns rows of the left whose key matches no
        // row of the right. Left = keyed (checkpoint side), right = commit_keys. Output
        // schema mirrors `keyed`.
        let survivors = keyed.left_anti_join(
            commit_keys,
            vec![(
                Arc::new(col(FSR_JOIN_KEY_COL)) as ExpressionRef,
                Arc::new(col(FSR_JOIN_KEY_COL)) as ExpressionRef,
            )],
        )?;
        commit_dedup.union_all(&[survivors])?
    } else {
        commit_dedup
    };

    let reconciled = terminal_input
        .filter(Arc::new(retention_filter_predicate(
            min_file_ts,
            txn_expiry,
        )))?
        .project_with_schema(checkpoint_pair.1.clone(), checkpoint_pair.0.clone())?;

    Ok(reconciled)
}

/// Async wrapper: resolve the scan shape (yielding `SchemaQuery` / `Consume` phases as
/// needed) and then delegate to [`build_reconciliation_ssa`].
pub(super) async fn execute_reconciliation_ssa(
    ctx: &Context,
    co: &mut StepCo,
    snapshot: &Snapshot,
    base: &Pair,
    stats: Option<SchemaRef>,
    parts: Option<SchemaRef>,
    dedup_key: Arc<Expression>,
) -> Result<Cursor, DeltaError> {
    let shape = resolve_shape_ssa(ctx, co, snapshot, stats.as_ref(), parts).await?;
    build_reconciliation_ssa(ctx, snapshot, &shape, base, dedup_key)
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

fn checkpoint_manifest_scan_schema() -> SchemaRef {
    let mut fields: Vec<StructField> = action_schema().fields().cloned().collect();
    fields.push(StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()));
    arc_schema(fields)
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
