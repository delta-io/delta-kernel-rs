//! Builders for the FSR plan vector.
//!
//! Composes the four canonical FSR plans (`commit_load`, `commit_dedup`,
//! optional `sidecar_load`, `results`) used by
//! [`FullState`](super::full_state::FullState). Each `build_*_plan` returns one
//! [`Plan`] node; [`build_fsr_plans`] sequences them in the topological order
//! required by their relation-handle dependencies.

use std::sync::Arc;

use url::Url;

use super::checkpoint_shape::CheckpointShape;
use super::schemas::{
    action_identity_projection, action_output_schema, action_read_schema, augmented_action_schema,
    checkpoint_manifest_scan_schema, fsr_dedup_key, fsr_row_has_identity_predicate,
    path_size_schema, retention_filter, FSR_JOIN_KEY_COL,
};
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::SIDECAR_NAME;
use crate::expressions::{ColumnName, Expression, Scalar};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{
    FileFormat, LoadSink, OrderingSpec, RelationHandle, ScanFileColumns,
};
use crate::plans::ir::{plan, DeclarativePlanNode, Plan};
use crate::schema::{ArrayType, DataType, SchemaBuilder, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta, Version};

/// Raw per-commit action stream, materialized by [`build_commit_load_plan`] into a
/// [`LoadSink`] so the downstream [`build_commit_dedup_plan`] can window over it. Schema =
/// [`action_read_schema`] plus a passthrough `version` column.
pub const FSR_COMMIT_RAW: &str = "fsr.commit_raw";
/// Commit winners: the single newest action per `__fsr_join_k` partition produced by
/// [`build_commit_dedup_plan`]. Schema = [`action_read_schema`] plus the dedup-key column
/// `FSR_JOIN_KEY_COL`.
pub const FSR_COMMIT_DEDUP: &str = "fsr.commit_dedup";
/// Top-level checkpoint rows scanned once and reused by sidecar extraction + checkpoint survivor
/// replay via relation references.
pub const FSR_CHECKPOINT_TOP: &str = "fsr.checkpoint_top";
/// Sidecar action stream materialized by [`build_sidecar_load_plan`] for V2-multipart
/// checkpoints; absent (relation handle never created) for V1 / V2-inline. Schema =
/// [`action_read_schema`].
pub const FSR_SIDECAR_ACTIONS: &str = "fsr.sidecar_actions";

/// Upper bound used by commit-dedup filtering (`row_number() <= k`).
///
/// Canonical FSR behavior uses `k = 1` (latest action per key).
const FSR_COMMIT_DEDUP_TOP_K: i64 = 1;

/// One literal row describing a Delta JSON commit file for [`build_commit_load_plan`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitFileMeta {
    pub path: String,
    pub size: i64,
    pub version: Version,
}

/// Build the full FSR plan vector for `snapshot` in topological order:
///
/// `[commit_load, commit_dedup, (sidecar_load if has_sidecars), results]`.
///
/// - `commit_load` -> `commit_dedup` is sequential by relation handle dependency
///   ([`FSR_COMMIT_RAW`]).
/// - `sidecar_load` (when present) -> `results` is sequential via [`FSR_SIDECAR_ACTIONS`].
/// - `commit_dedup` -> `results` is sequential via [`FSR_COMMIT_DEDUP`].
///
/// The single `PhaseOperation::Plans` yield wraps all of them; the executor walks them
/// in submitted order and the relation registry keeps each step's batches available to its
/// successors.
pub fn build_fsr_plans(
    snapshot: &Snapshot,
    shape: CheckpointShape,
) -> Result<Vec<Plan>, DeltaError> {
    let log_root = snapshot.log_segment().log_root.clone();
    let segment = snapshot.log_segment();

    let commits = commit_cover_rows(segment)?;
    let checkpoint_files: Vec<FileMeta> = segment
        .listed
        .checkpoint_parts
        .iter()
        .map(|p| p.location.clone())
        .collect();

    let commit_raw_schema =
        load_materialized_schema(&action_read_schema(), &path_size_schema(true), &["version"])?;
    let commit_dedup_schema = augmented_action_schema(false)?;

    let commit_raw_handle = RelationHandle::fresh(FSR_COMMIT_RAW, commit_raw_schema);
    let commit_dedup_handle = RelationHandle::fresh(FSR_COMMIT_DEDUP, commit_dedup_schema);
    let checkpoint_top_handle = RelationHandle::fresh(
        FSR_CHECKPOINT_TOP,
        checkpoint_manifest_scan_schema(shape.has_sidecars),
    );

    let now = current_time_duration().map_err(|e| e.into_delta_default())?;
    let min_file_ts = deleted_file_retention_timestamp_with_time(
        snapshot.table_properties().deleted_file_retention_duration,
        now,
    )
    .map_err(|e| e.into_delta_default())?;
    let txn_expiry = calculate_transaction_expiration_timestamp(snapshot.table_properties())
        .map_err(|e| e.into_delta_default())?;

    let mut plans = Vec::with_capacity(4);
    plans.push(build_commit_load_plan(
        &commits,
        &commit_raw_handle,
        &log_root,
    )?);
    plans.push(build_commit_dedup_plan(
        &commit_raw_handle,
        &commit_dedup_handle,
    )?);

    if !checkpoint_files.is_empty() {
        plans.push(build_checkpoint_top_load_plan(
            checkpoint_files.clone(),
            &checkpoint_top_handle,
            shape.file_format,
            shape.has_sidecars,
        )?);
    }

    let sidecar_handle = if shape.has_sidecars {
        let handle = RelationHandle::fresh(FSR_SIDECAR_ACTIONS, action_read_schema());
        plans.push(build_sidecar_load_plan(
            &checkpoint_top_handle,
            &handle,
            &log_root,
        )?);
        Some(handle)
    } else {
        None
    };

    plans.push(build_results_plan(
        &commit_dedup_handle,
        sidecar_handle.as_ref(),
        &checkpoint_top_handle,
        !checkpoint_files.is_empty(),
        min_file_ts,
        txn_expiry,
    )?);

    Ok(plans)
}

pub(super) fn load_materialized_schema(
    file_schema: &SchemaRef,
    upstream: &SchemaRef,
    passthrough: &[&str],
) -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<StructField> = file_schema.fields().cloned().collect();
    let up = upstream.as_ref();
    for name in passthrough {
        let field = up.fields().find(|f| f.name() == *name).ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::load_materialized_schema: upstream schema {:?} missing passthrough `{name}`",
                upstream,
            )
        })?;
        fields.push(StructField::new(
            *name,
            field.data_type().clone(),
            field.is_nullable(),
        ));
    }
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())
}

fn commit_cover_rows(
    seg: &crate::log_segment::LogSegment,
) -> Result<Vec<CommitFileMeta>, DeltaError> {
    let log_root = &seg.log_root;
    let mut rows = Vec::new();
    let merge = itertools::Itertools::merge_by(
        seg.listed.ascending_commit_files.iter(),
        seg.listed.ascending_compaction_files.iter(),
        |a, b| a.version <= b.version,
    );

    let mut last_pushed: Option<&ParsedLogPath<FileMeta>> = None;
    for next in merge {
        match last_pushed {
            Some(prev) if prev.version == next.version => {
                rows.pop();
            }
            Some(&ParsedLogPath {
                file_type: LogPathFileType::CompactedCommit { hi },
                ..
            }) if next.version <= hi => {
                continue;
            }
            _ => {}
        }
        last_pushed = Some(next);
        rows.push(CommitFileMeta {
            path: path_under_log_root(log_root, &next.location.location)?,
            size: next.location.size as i64,
            version: next.version,
        });
    }
    rows.reverse();
    Ok(rows)
}

fn path_under_log_root(log_root: &Url, file: &Url) -> Result<String, DeltaError> {
    let base = log_root.path().trim_end_matches('/');
    let full = file.path();
    let suffix = full.strip_prefix(base).unwrap_or(full);
    Ok(suffix.trim_start_matches('/').to_string())
}

/// Plan 1: materialize raw per-commit action rows into [`FSR_COMMIT_RAW`].
///
/// Each [`CommitFileMeta`] becomes one literal row; the [`LoadSink`] opens each commit JSON
/// at `<base_url>/<path>` with the action read schema and broadcasts the per-commit
/// `version` value onto every action row via `passthrough_columns`. Downstream
/// [`build_commit_dedup_plan`] uses that `version` column to `ORDER BY version DESC` inside
/// each `__fsr_join_k` partition.
fn build_commit_load_plan(
    commits: &[CommitFileMeta],
    commit_raw_handle: &RelationHandle,
    log_root: &Url,
) -> Result<Plan, DeltaError> {
    let rows: Vec<Vec<Scalar>> = commits
        .iter()
        .map(|c| {
            vec![
                Scalar::String(c.path.clone()),
                Scalar::Long(c.size),
                Scalar::Long(c.version as i64),
            ]
        })
        .collect();

    let literal_plan = DeclarativePlanNode::values(path_size_schema(true), rows)
        .map_err(|e| e.into_delta_default())?;
    let sink = LoadSink {
        output_relation: commit_raw_handle.clone(),
        file_schema: action_read_schema(),
        base_url: Some(log_root.clone()),
        file_meta: ScanFileColumns {
            path: ColumnName::new(["path"]),
            size: Some(ColumnName::new(["size"])),
            record_count: None,
        },
        dv_ref: None,
        passthrough_columns: vec![ColumnName::new(["version"])],
        file_type: FileFormat::Json,
    };
    Ok(literal_plan.into_load(sink))
}

/// Plan 2: window-dedup raw commit actions into per-key winners ([`FSR_COMMIT_DEDUP`]).
///
/// Steps:
/// 1. `Filter(fsr_row_has_identity_predicate)` — drop rows that aren't a recognized action.
/// 2. `Project(action_cols + __fsr_join_k + version)` — materialize the dedup key as a top-level
///    column so `Window.partition_by` and the downstream LeftAnti can use a column reference
///    (kernel + DF window/join compilers reject non-column partition keys).
/// 3. `Window(row_number PARTITION BY __fsr_join_k ORDER BY version DESC)` — assign a 1-based row
///    number per `(action_kind, identity)`, newest commit first.
/// 4. `Filter(__rn <= k)` — keep newest `k` actions per key (`k=1` for canonical dedup).
/// 5. `Project(action_cols + __fsr_join_k)` — drop `version` and `__rn`; the persisted relation
///    matches `augmented_action_schema()` so [`build_results_plan`]'s union schema-checks line up.
fn build_commit_dedup_plan(
    commit_raw_handle: &RelationHandle,
    commit_dedup_handle: &RelationHandle,
) -> Result<Plan, DeltaError> {
    for field in [
        "add",
        "remove",
        "protocol",
        "metaData",
        "domainMetadata",
        "txn",
        "version",
    ] {
        if !commit_raw_handle.schema.contains(field) {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::build_commit_dedup_plan::schema_check: commit raw relation schema is \
                 missing required field `{field}`",
            ));
        }
    }
    let dedup_expr = Arc::new(fsr_dedup_key());
    let with_key_and_version_schema = augmented_action_schema(true)?;
    let project_with_key_and_version: Vec<Arc<Expression>> = action_identity_projection()
        .into_iter()
        .chain(std::iter::once(Arc::clone(&dedup_expr)))
        .chain(std::iter::once(Arc::new(Expression::column(["version"]))))
        .collect();
    let projected = plan::relation_ref(commit_raw_handle)
        .project(project_with_key_and_version, with_key_and_version_schema)
        .filter(Arc::new(
            Expression::column([FSR_JOIN_KEY_COL]).is_not_null().into(),
        ));
    let windowed = projected
        .window_row_number(
            "__kernel_rn",
            vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
            vec![OrderingSpec::desc(ColumnName::new(["version"]))],
        )
        .map_err(|e| e.into_delta_default())?;

    let rn_top_k = Arc::new(
        Expression::column(["__kernel_rn"])
            .le(Expression::literal(FSR_COMMIT_DEDUP_TOP_K))
            .into(),
    );

    // Final project: drop `version` and `__rn`; keep action_cols + __fsr_join_k.
    let final_proj: Vec<Arc<Expression>> = action_identity_projection()
        .into_iter()
        .chain(std::iter::once(Arc::clone(&dedup_expr)))
        .collect();

    Ok(windowed
        .filter(rn_top_k)
        .project(final_proj, augmented_action_schema(false)?)
        .into_relation(commit_dedup_handle.clone()))
}

/// Plan 3 (only when `has_sidecars`): scan the top-level checkpoint for sidecar pointers and
/// materialize each referenced sidecar parquet's action rows into [`FSR_SIDECAR_ACTIONS`].
///
/// Top-level scan uses `sidecar_only_schema()` to read just the `sidecar` column; rows
/// without a sidecar pointer are dropped. The LoadSink reads each referenced parquet under
/// `<log_root>/_sidecars/<sidecar.path>` with the full action read schema. Sidecar payloads
/// are parquet-encoded.
fn build_checkpoint_top_load_plan(
    checkpoint_top_files: Vec<FileMeta>,
    checkpoint_top_handle: &RelationHandle,
    format: FileFormat,
    include_sidecar: bool,
) -> Result<Plan, DeltaError> {
    Ok(DeclarativePlanNode::scan(
        format,
        checkpoint_top_files,
        checkpoint_manifest_scan_schema(include_sidecar),
    )
    .into_relation(checkpoint_top_handle.clone()))
}

fn build_sidecar_load_plan(
    checkpoint_top_handle: &RelationHandle,
    sidecar_handle: &RelationHandle,
    log_root: &Url,
) -> Result<Plan, DeltaError> {
    let scan = plan::relation_ref(checkpoint_top_handle)
        .filter(Arc::new(
            Expression::column([SIDECAR_NAME]).is_not_null().into(),
        ))
        .project(
            vec![
                Arc::new(Expression::column([SIDECAR_NAME, "path"])),
                Arc::new(Expression::column([SIDECAR_NAME, "sizeInBytes"])),
            ],
            path_size_schema(false),
        );

    let sidecar_base = log_root.join("_sidecars/").map_err(|e| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "fsr::build_sidecar_load_plan::join_sidecar_base: join _sidecars base URL: {e}",
        )
    })?;
    let sink = LoadSink {
        output_relation: sidecar_handle.clone(),
        file_schema: action_read_schema(),
        base_url: Some(sidecar_base),
        file_meta: ScanFileColumns {
            path: ColumnName::new(["path"]),
            size: Some(ColumnName::new(["size"])),
            record_count: None,
        },
        dv_ref: None,
        passthrough_columns: vec![],
        file_type: FileFormat::Parquet,
    };

    Ok(scan.into_load(sink))
}

/// Plan 4 (terminal): assemble the live snapshot rows from commit winners + checkpoint
/// survivors and stream them to the
/// [`SinkType::Results`](crate::plans::ir::nodes::SinkType::Results) consumer.
///
/// Inline shape:
///
/// ```text
/// checkpoint_full = top_scan [∪ relation_ref(sidecar_actions)]
/// checkpoint_keyed = checkpoint_full
///     | Filter(fsr_row_has_identity_predicate)
///     | Project(action_cols + __fsr_join_k)
///
/// commit_keys = relation_ref(commit_dedup) | Project([__fsr_join_k])
///
/// survivors = LeftAntiJoin(probe = checkpoint_keyed, build = commit_keys) on __fsr_join_k
///
/// Union(relation_ref(commit_dedup), survivors)
///     | Filter(retention)
///     | Project(action_read_schema)
///     | into_results()
/// ```
///
/// The top-level checkpoint is read with the *full* action schema (missing fields
/// resolve to NULL); this keeps the union with the sidecar relation and the commit-dedup
/// relation schema-compatible without an explicit alignment project.
fn build_results_plan(
    commit_dedup_handle: &RelationHandle,
    sidecar_handle: Option<&RelationHandle>,
    checkpoint_top_handle: &RelationHandle,
    has_checkpoint_files: bool,
    min_file_retention_timestamp: i64,
    txn_expiration_cutoff: Option<i64>,
) -> Result<Plan, DeltaError> {
    // No checkpoint parts: there is no checkpoint side to anti-join. The full snapshot state is
    // entirely determined by commit winners.
    if !has_checkpoint_files {
        return Ok(plan::relation_ref(commit_dedup_handle)
            .filter(Arc::new(
                retention_filter(min_file_retention_timestamp, txn_expiration_cutoff).into(),
            ))
            .project(action_identity_projection(), action_read_schema())
            .into_results());
    }

    let dedup_expr = Arc::new(fsr_dedup_key());
    let augmented_schema = augmented_action_schema(false)?;

    // Top-level checkpoint rows are preloaded once into `FSR_CHECKPOINT_TOP`; project action
    // columns here so schema aligns with sidecar/action relations.
    let top_scan = plan::relation_ref(checkpoint_top_handle)
        .project(action_identity_projection(), action_read_schema());

    let checkpoint_full = match sidecar_handle {
        Some(handle) => {
            DeclarativePlanNode::union_unordered(vec![top_scan, plan::relation_ref(handle)])
                .map_err(|e| e.into_delta_default())?
        }
        None => top_scan,
    };

    let checkpoint_keyed = checkpoint_full
        .filter(Arc::new(fsr_row_has_identity_predicate().into()))
        .project(
            action_identity_projection()
                .into_iter()
                .chain(std::iter::once(Arc::clone(&dedup_expr)))
                .collect(),
            augmented_schema.clone(),
        );

    // Build side: just the dedup keys from commit winners (one column wide). LeftAnti emits
    // probe rows whose key is NOT in the build set, mirroring the probe child's schema.
    let join_key_only_schema = SchemaBuilder::new()
        .with_nullable(
            FSR_JOIN_KEY_COL,
            DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
        )
        .build()
        .map_err(|e| e.into_delta_default())?;
    let commit_keys = plan::relation_ref(commit_dedup_handle).project(
        vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
        join_key_only_schema,
    );

    let survivors = checkpoint_keyed.left_anti_join_on(
        commit_keys,
        vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
        vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
    );

    // Union directly on augmented rows (action + join key); drop join key once at the end.
    let everything = DeclarativePlanNode::union_unordered(vec![
        plan::relation_ref(commit_dedup_handle),
        survivors,
    ])
    .map_err(|e| e.into_delta_default())?;

    Ok(everything
        .filter(Arc::new(
            retention_filter(min_file_retention_timestamp, txn_expiration_cutoff).into(),
        ))
        .project(action_identity_projection(), action_output_schema())
        .into_results())
}
