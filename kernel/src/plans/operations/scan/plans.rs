//! Shared reconciliation pipeline used by scan ([`super::file_scan`]) and FSR
//! ([`super::full_state`]):
//!
//! ```text
//! commit_load(Values -> Load) -> commit_dedup(filter -> project -> window -> filter -> project)
//!     -> [checkpoint view, per CheckpointShape variant]
//!     -> checkpoint_keyed(filter(identity) + project(+dedup_key))
//!     -> LEFTANTI(commit_keys) -> survivors
//!     -> UNION(commit_dedup, survivors) -> Filter(retention) -> Project(checkpoint_pair)
//!     -> RECONCILED
//! ```
//!
//! Terminates at [`RECONCILED`] in the (augmented) `add`/`remove`-shaped row stream — `JOIN_KEY`
//! dropped, optional `stats_parsed` / `partitionValues_parsed` sub-fields materialized. Callers
//! layer their own terminal projection (and optional data phase) on top.
//!
//! [`register_reconciliation`] is sync (caller supplies a resolved [`ScanShapeInfo`]);
//! [`execute_reconciliation`] is async and resolves the shape itself via
//! [`ScanShapeInfo::resolve`].

use std::sync::Arc;

use url::Url;

use super::action_pair::{augment_add, Pair, JOIN_KEY_FIELD, VERSION_FIELD};
use super::checkpoint_shape::{CheckpointShape, ScanShapeInfo};
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
use crate::plans::operations::framework::coroutine::context::Context;
use crate::schema::{arc_schema, ArrayType, DataType, SchemaRef, StructField};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, Version};

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

/// One literal row describing a Delta JSON commit file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitFileMeta {
    pub path: String,
    pub size: i64,
    pub version: Version,
}

// === Sync reconciliation pipeline (registry-only, no yields) ==============================

/// Sync core of the reconciliation pipeline; caller resolves the [`ScanShapeInfo`] beforehand
/// (see [`execute_reconciliation`]). `base` picks the action-slot set; `dedup_key` MUST
/// evaluate to NULL on rows matching no known slot (the pipeline filter is `IS NOT NULL`).
pub(super) fn register_reconciliation(
    registry: &mut RelationRegistry,
    snapshot: &Snapshot,
    shape: &ScanShapeInfo,
    base: &'static std::sync::LazyLock<Pair>,
    dedup_key: Arc<Expression>,
) -> Result<(), DeltaError> {
    let stats_for = |has_parsed: bool| {
        shape
            .stats
            .stats_schema
            .as_ref()
            .map(|s| (s.clone(), has_parsed))
    };
    // commit_pair: rows from commit logs / manifest tops never carry native `add.stats_parsed`,
    // so stats are derived via `parse_json` here. checkpoint_pair: leaf files (classic checkpoint
    // or V2 sidecar) may carry native `add.stats_parsed`, in which case it's a passthrough.
    let commit_pair = augment_add(
        (**base).clone(),
        stats_for(false),
        shape.partition_schema.clone(),
    );
    let checkpoint_pair = augment_add(
        (**base).clone(),
        stats_for(shape.stats.has_parsed_stats),
        shape.partition_schema.clone(),
    );
    // checkpoint_pair is identity-equivalent to commit_pair when stats are absent or already
    // native — in that case the checkpoint-side `project_pair(checkpoint_pair)` is a no-op and
    // we can skip the node entirely.
    let checkpoint_project_is_identity = shape.partition_schema.is_none()
        && (shape.stats.stats_schema.is_none() || shape.stats.has_parsed_stats);

    // Reused twice (commit filter + checkpoint-side filter); hold as Arc<Expression> so
    // each `.filter(...)` call is a cheap Arc clone.
    let identity_not_null: Arc<Expression> =
        Arc::new(dedup_key.as_ref().clone().is_not_null().into());

    let commits = commit_cover_rows(snapshot.log_segment())?;
    let log_root = snapshot.log_segment().log_root.clone();
    let (min_file_ts, txn_expiry) = retention_timestamps(snapshot)?;

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
    PlanBuilder::values(path_size_schema(/* with_version= */ true), commit_rows)
        .map_err(|e| e.into_delta_default())?
        .load(
            COMMIT_RAW,
            base.0.clone(),
            FileType::Json,
            Some(log_root.clone()),
            vec![ColumnName::new(["version"])],
            default_scan_file_columns(),
            None,
            registry,
        )?;

    // === Stage 2: commit_dedup ============================================================
    // filter(identity) → project(commit_pair + JOIN_KEY + VERSION) → window(row_number) →
    // filter(rn <= 1) → project(commit_pair + JOIN_KEY) → COMMIT_DEDUP.
    const COMMIT_DEDUP_RN_COL: &str = "__kernel_commit_dedup_rn";
    registry
        .relation_ref(COMMIT_RAW)?
        .filter(identity_not_null.clone())
        .project_pair(commit_pair.clone())
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
            col(COMMIT_DEDUP_RN_COL)
                .le(Expression::literal(1i64))
                .into(),
        ))
        .project_pair(commit_pair.clone())
        .add_column(JOIN_KEY_FIELD.clone(), dedup_key.clone())
        .into_relation(COMMIT_DEDUP, registry)?;

    // === Stages 3-5a: build the checkpoint view per CheckpointShape variant ==============
    // Returns `Some(PlanBuilder)` aligned to `checkpoint_pair.0` (= the post-union schema),
    // or `None` when the snapshot has no checkpoint at all. The view is the union of the
    // checkpoint-side actions (top + optional sidecars) projected to the augmented action shape,
    // ready to be keyed and antijoined in stage 5b.
    let checkpoint_view: Option<PlanBuilder> = match &shape.checkpoint {
        CheckpointShape::NoCheckpoint => None,
        CheckpointShape::Scan { files, file_format } => {
            // Scan checkpoint files directly into `checkpoint_pair.0` so the engine surfaces
            // native `add.stats_parsed` / `add.partitionValues_parsed` when present.
            let scan_schema = checkpoint_pair.0.clone();
            let scan = match file_format {
                FileFormat::Parquet => PlanBuilder::scan_parquet(files.clone(), scan_schema),
                FileFormat::Json => PlanBuilder::scan_json(files.clone(), scan_schema),
            };
            scan.into_relation(CHECKPOINT_TOP, registry)?;
            let top = registry.relation_ref(CHECKPOINT_TOP)?;
            Some(if checkpoint_project_is_identity {
                top
            } else {
                top.project_pair(checkpoint_pair.clone())
            })
        }
        CheckpointShape::Manifest { .. } => {
            // Resolver already published `CHECKPOINT_TOP` as the manifest (schema:
            // `action_schema + sidecar`, no `stats_parsed`). Manifest rows are mostly
            // sidecar-pointers with `add IS NULL` and get filtered out below; the
            // manifest's own action rows use commit-style `parse_json`.
            let sidecar_base = log_root.join("_sidecars/").map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaStateRecoverError,
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
                    path_size_schema(/* with_version= */ false),
                )
                .load(
                    SIDECAR_ACTIONS,
                    checkpoint_pair.0.clone(),
                    FileType::Parquet,
                    Some(sidecar_base),
                    vec![],
                    default_scan_file_columns(),
                    None,
                    registry,
                )?;

            // Manifest top: commit_pair derives stats via parse_json — identical to what
            // we'd want for the manifest, which never has native stats either.
            let top = registry
                .relation_ref(CHECKPOINT_TOP)?
                .project_pair(commit_pair.clone());
            let side = if checkpoint_project_is_identity {
                registry.relation_ref(SIDECAR_ACTIONS)?
            } else {
                registry
                    .relation_ref(SIDECAR_ACTIONS)?
                    .project_pair(checkpoint_pair.clone())
            };
            Some(
                PlanBuilder::union(vec![top, side], /* ordered= */ false)
                    .map_err(|e| e.into_delta_default())?,
            )
        }
    };

    // === Stage 5b: antijoin + union + retention -> RECONCILED ============================
    // Without a checkpoint, commit_dedup IS the live snapshot. With one, the unified checkpoint
    // view is anti-joined against commit keys, unioned with commit_dedup, and retention-filtered.
    let terminal_input = if let Some(view) = checkpoint_view {
        // Filter to valid action rows + add the dedup key column for the antijoin.
        let keyed = view
            .filter(identity_not_null)
            .project_pair(checkpoint_pair.clone())
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
            /* ordered= */ false,
        )
        .map_err(|e| e.into_delta_default())?
    } else {
        registry.relation_ref(COMMIT_DEDUP)?
    };

    terminal_input
        .filter(Arc::new(retention_filter(min_file_ts, txn_expiry).into()))
        .project_pair(checkpoint_pair)
        .into_relation(RECONCILED, registry)?;

    Ok(())
}

/// Async wrapper: resolve the scan shape (yielding `SchemaQuery` / `Plans` phases as needed)
/// and then delegate to [`register_reconciliation`].
pub(super) async fn execute_reconciliation(
    ctx: &mut Context<'_>,
    snapshot: &Snapshot,
    base: &'static std::sync::LazyLock<Pair>,
    stats: Option<SchemaRef>,
    parts: Option<SchemaRef>,
    dedup_key: Arc<Expression>,
) -> Result<(), DeltaError> {
    let shape = ScanShapeInfo::resolve(ctx, snapshot, stats.as_ref(), parts).await?;
    register_reconciliation(&mut *ctx, snapshot, &shape, base, dedup_key)
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

pub(super) fn path_under_log_root(log_root: &Url, file: &Url) -> Result<String, DeltaError> {
    let base = log_root.path().trim_end_matches('/');
    let full = file.path();
    let suffix = full.strip_prefix(base).unwrap_or(full);
    Ok(suffix.trim_start_matches('/').to_string())
}
