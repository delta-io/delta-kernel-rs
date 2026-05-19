//! Shared reconciliation pipeline.
//!
//! [`register_reconciliation`] is the single source of truth for the multi-stage Delta
//! reconciliation DAG that both scan ([`super::file_scan`]) and FSR ([`super::full_state`])
//! build on top of:
//!
//! ```text
//! commit_load(Values -> Load) -> commit_dedup(filter -> project -> window -> filter -> project)
//!     -> checkpoint_top(scan, optional)
//!     -> sidecar_load(filter -> project -> Load, optional)
//!     -> checkpoint_keyed(filter(identity) + project(+dedup_key))
//!     -> LEFTANTI(commit_keys) -> survivors
//!     -> UNION(commit_dedup, survivors) -> Filter(retention) -> Project(action_pair)
//!     -> RECONCILED
//! ```
//!
//! The pipeline terminates at [`RECONCILED`] with the `action_pair` shape (the FSR_JOIN_KEY
//! has been dropped). Callers (scan / FSR) then layer their own terminal projection (and
//! optional data phase) on top.
//!
//! ## Sync vs async surfaces
//!
//! - [`register_reconciliation`] is **sync**: it builds all the plans into the registry but
//!   does not yield phases. Callers must supply a pre-resolved [`CheckpointShape`].
//! - [`execute_reconciliation`] is **async**: it first calls
//!   [`resolve_checkpoint_shape`](super::checkpoint_shape::resolve_checkpoint_shape) (which
//!   may yield `SchemaQuery` / `Plans` phases via `ctx`) and then delegates to
//!   [`register_reconciliation`].
//!
//! Both scan and FSR entry points use the async surface in their state machines.

use std::sync::Arc;

use url::Url;

use super::action_pair::{
    with_partition_values, with_stats, JOIN_KEY_FIELD, VERSION_FIELD,
};
use super::checkpoint_shape::{resolve_checkpoint_shape, CheckpointShape};
use super::dedup::FSR_JOIN_KEY_COL;
use super::retention::retention_filter;
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::SIDECAR_NAME;
use crate::expressions::{col, ColumnName, Expression, Scalar};
use crate::path::ParsedLogPath;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{
    FileFormat, FileType, JoinType, OrderingSpec, ScanFileColumns, WindowFunction,
};
use crate::plans::ir::{PlanBuilder, RelationRegistry};
use crate::plans::state_machines::framework::coroutine::context::Context;
use crate::schema::{arc_schema, ArrayType, DataType, SchemaRef, StructField, ToSchema};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta, Version};

// === Stage relation names ===
//
// Bare names; the per-pipeline prefix ("scan" or "fsr") is injected by
// `RelationRegistry::new(sm_id, sm_name)` at SM entry. A registry built with `sm_name="scan"`
// stores `COMMIT_RAW` as "scan.commit_raw"; one built with `"fsr"` stores it as
// "fsr.commit_raw". SM bodies always pass the bare const.
pub(super) const COMMIT_RAW: &str = "commit_raw";
pub(super) const COMMIT_DEDUP: &str = "commit_dedup";
pub(super) const CHECKPOINT_TOP: &str = "checkpoint_top";
pub(super) const SIDECAR_ACTIONS: &str = "sidecar_actions";
/// Shared metadata-pipeline terminal: action_pair-shaped row stream, FSR_JOIN_KEY dropped.
/// Each caller (scan / FSR) projects from this into its own terminal.
pub(super) const RECONCILED: &str = "reconciled";

/// One literal row describing a Delta JSON commit file consumed by the commit-load Values
/// upstream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitFileMeta {
    pub path: String,
    pub size: i64,
    pub version: Version,
}

// === Sync reconciliation pipeline (registry-only, no yields) ==============================

/// Sync core of the reconciliation pipeline. Builds every stage into `registry` without
/// yielding phases; caller is responsible for resolving the [`CheckpointShape`] beforehand
/// (typically via [`resolve_checkpoint_shape`] inside an async wrapper — see
/// [`execute_reconciliation`]).
///
/// `base` selects the action-slot set (`SCAN_BASE` for `{add, remove}`, `FSR_BASE` for the
/// full six slots). `dedup_key` MUST evaluate to NULL on rows that match no known slot — the
/// pipeline's identity filter is `dedup_key IS NOT NULL`.
pub(super) fn register_reconciliation(
    registry: &mut RelationRegistry,
    snapshot: &Snapshot,
    shape: &CheckpointShape,
    base: &'static std::sync::LazyLock<(SchemaRef, Vec<Arc<Expression>>)>,
    stats: Option<SchemaRef>,
    parts: Option<SchemaRef>,
    dedup_key: Arc<Expression>,
) -> Result<(), DeltaError> {
    let action_pair = with_partition_values(
        with_stats((**base).clone(), stats.clone(), /*native=*/ false),
        parts.clone(),
    );
    // Reused twice (commit filter + checkpoint-side filter); hold as Arc<Expression> so
    // each `.filter(...)` call is a cheap Arc clone instead of a Predicate clone + Arc alloc.
    let identity_not_null: Arc<Expression> =
        Arc::new(dedup_key.as_ref().clone().is_not_null().into());

    let commits = commit_cover_rows(snapshot.log_segment())?;
    let checkpoint_files: Vec<FileMeta> = snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .iter()
        .map(|p| p.location.clone())
        .collect();
    let log_root = snapshot.log_segment().log_root.clone();
    let (min_file_ts, txn_expiry) = retention_timestamps(snapshot)?;
    let has_checkpoint = !checkpoint_files.is_empty();

    // === Stage 1: commit_load ============================================================
    // VALUES(commits) → Load(JSON) → COMMIT_RAW. The Load broadcasts the per-commit
    // `version` column onto every emitted action row.
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
    PlanBuilder::values(path_size_schema(/*with_version=*/ true), commit_rows)
        .map_err(|e| e.into_delta_default())?
        .load(
            COMMIT_RAW,
            (**base).0.clone(),
            FileType::Json,
            Some(log_root.clone()),
            vec![ColumnName::new(["version"])],
            default_scan_file_columns(),
            None,
            registry,
        )?;

    // === Stage 2: commit_dedup ============================================================
    // filter(identity) → project(action_pair + JOIN_KEY + VERSION) → window(row_number) →
    // filter(rn <= 1) → project(action_pair + JOIN_KEY) → COMMIT_DEDUP.
    const COMMIT_DEDUP_RN_COL: &str = "__kernel_commit_dedup_rn";
    registry
        .relation_ref(COMMIT_RAW)?
        .filter(identity_not_null.clone())
        .project_pair(action_pair.clone())
        .add_column(JOIN_KEY_FIELD.clone(), dedup_key.clone())
        .add_column(VERSION_FIELD.clone(), Arc::new(col("version")))
        .window(
            vec![WindowFunction {
                output_col: COMMIT_DEDUP_RN_COL.to_string(),
            }],
            vec![Arc::new(col(FSR_JOIN_KEY_COL))],
            vec![OrderingSpec::desc(ColumnName::new(["version"]))],
        )
        .map_err(|e| e.into_delta_default())?
        .filter(Arc::new(
            col(COMMIT_DEDUP_RN_COL).le(Expression::literal(1i64)).into(),
        ))
        .project_pair(action_pair.clone())
        .add_column(JOIN_KEY_FIELD.clone(), dedup_key.clone())
        .into_relation(COMMIT_DEDUP, registry)?;

    // === Stage 3: checkpoint_top scan (optional) ==========================================
    // When the resolver already published the V2 manifest under CHECKPOINT_TOP, skip the
    // scan and reuse the existing relation. Otherwise scan with the action_pair shape
    // (plus the sidecar slot if V2-multipart needs sidecar discovery).
    if has_checkpoint && shape.manifest_relation.is_none() {
        let mut scan_fields: Vec<StructField> = action_pair.0.fields().cloned().collect();
        if shape.has_sidecars {
            scan_fields.push(StructField::nullable(
                SIDECAR_NAME,
                crate::actions::Sidecar::to_schema(),
            ));
        }
        let scan_schema = arc_schema(scan_fields);
        match shape.file_format {
            FileFormat::Parquet => {
                PlanBuilder::scan_parquet(checkpoint_files.clone(), scan_schema)
            }
            FileFormat::Json => PlanBuilder::scan_json(checkpoint_files.clone(), scan_schema),
        }
        .into_relation(CHECKPOINT_TOP, registry)?;
    }

    // === Stage 4: sidecar_load (V2-multipart only) =======================================
    if shape.has_sidecars {
        if !has_checkpoint && shape.manifest_relation.is_none() {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "register_reconciliation: has_sidecars=true but no checkpoint relation present",
            ));
        }
        let sidecar_base = log_root.join("_sidecars/").map_err(|e| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "register_reconciliation::join_sidecar_base: join _sidecars base URL: {e}",
            )
        })?;
        registry
            .relation_ref(CHECKPOINT_TOP)?
            .filter(Arc::new(col(SIDECAR_NAME).is_not_null().into()))
            .project(
                [
                    col([SIDECAR_NAME, "path"]).into(),
                    col([SIDECAR_NAME, "sizeInBytes"]).into(),
                ],
                path_size_schema(/*with_version=*/ false),
            )
            .load(
                SIDECAR_ACTIONS,
                action_pair.0.clone(),
                FileType::Parquet,
                Some(sidecar_base),
                vec![],
                default_scan_file_columns(),
                None,
                registry,
            )?;
    }

    // === Stage 5: antijoin + union + retention -> RECONCILED ==============================
    // Compute the upstream feeding the terminal `Filter(retention) -> Project(action_pair) ->
    // RECONCILED` pipe. Without a checkpoint, commit_dedup IS the live snapshot. With one,
    // the checkpoint side is aligned to `action_pair`, anti-joined against commit keys, and
    // unioned with commit_dedup.
    let terminal_input = if !has_checkpoint {
        registry.relation_ref(COMMIT_DEDUP)?
    } else {
        // Align the checkpoint side (top + optional sidecar) to action_pair shape so the
        // union is well-formed.
        let (align_schema, align_proj) = action_pair.clone();
        let top = registry
            .relation_ref(CHECKPOINT_TOP)?
            .project(align_proj.clone(), align_schema.clone());
        let full = if shape.has_sidecars {
            let side = registry
                .relation_ref(SIDECAR_ACTIONS)?
                .project(align_proj, align_schema);
            PlanBuilder::union(vec![top, side], /*ordered=*/ false)
                .map_err(|e| e.into_delta_default())?
        } else {
            top
        };

        // Filter to valid action rows + add the dedup key column for the antijoin.
        let keyed = full
            .filter(identity_not_null.clone())
            .project_pair(action_pair.clone())
            .add_column(JOIN_KEY_FIELD.clone(), dedup_key.clone());

        // Build side of the antijoin: just the dedup keys from commit winners.
        let dedup_key_array = DataType::Array(Box::new(ArrayType::new(DataType::STRING, true)));
        let join_key_only_schema =
            arc_schema([StructField::nullable(FSR_JOIN_KEY_COL, dedup_key_array)]);
        let commit_keys = registry
            .relation_ref(COMMIT_DEDUP)?
            .project(vec![col(FSR_JOIN_KEY_COL).into()], join_key_only_schema);
        let survivors = commit_keys
            .join(keyed, JoinType::LeftAnti)
            .map_err(|e| e.into_delta_default())?;

        PlanBuilder::union(
            vec![registry.relation_ref(COMMIT_DEDUP)?, survivors],
            /*ordered=*/ false,
        )
        .map_err(|e| e.into_delta_default())?
    };

    terminal_input
        .filter(Arc::new(retention_filter(min_file_ts, txn_expiry).into()))
        .project_pair(action_pair)
        .into_relation(RECONCILED, registry)?;

    Ok(())
}

/// Async wrapper: resolve the checkpoint shape (yielding `SchemaQuery` / `Plans` phases as
/// needed) and then delegate to [`register_reconciliation`].
pub(super) async fn execute_reconciliation(
    ctx: &mut Context<'_>,
    snapshot: &Snapshot,
    base: &'static std::sync::LazyLock<(SchemaRef, Vec<Arc<Expression>>)>,
    stats: Option<SchemaRef>,
    parts: Option<SchemaRef>,
    dedup_key: Arc<Expression>,
) -> Result<(), DeltaError> {
    let shape = resolve_checkpoint_shape(ctx, snapshot, stats.as_ref()).await?;
    register_reconciliation(&mut *ctx, snapshot, &shape, base, stats, parts, dedup_key)
}

// === Helpers ==============================================================================

/// File-meta column hints shared by every Load: `path` column, optional `size` column, no
/// record-count column.
pub(super) fn default_scan_file_columns() -> ScanFileColumns {
    ScanFileColumns {
        path: ColumnName::new(["path"]),
        size: Some(ColumnName::new(["size"])),
        record_count: None,
    }
}

/// `{path, size, version?}` schema for the Values upstream of commit_load.
pub(super) fn path_size_schema(with_version: bool) -> SchemaRef {
    let mut fields = vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
    ];
    if with_version {
        fields.push(StructField::not_null("version", DataType::LONG));
    }
    arc_schema(fields)
}

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
pub(super) fn commit_cover_rows(
    seg: &crate::log_segment::LogSegment,
) -> Result<Vec<CommitFileMeta>, DeltaError> {
    seg.find_commit_cover()
        .into_iter()
        .map(|file| {
            let version = ParsedLogPath::try_from(file.clone())
                .map_err(|e| e.into_delta_default())?
                .ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
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

pub(super) fn path_under_log_root(log_root: &Url, file: &Url) -> Result<String, DeltaError> {
    let base = log_root.path().trim_end_matches('/');
    let full = file.path();
    let suffix = full.strip_prefix(base).unwrap_or(full);
    Ok(suffix.trim_start_matches('/').to_string())
}
