//! Builders for the FSR plan vector.
//!
//! [`build_fsr_plans`] composes the four canonical FSR plans (`commit_load`,
//! `commit_dedup`, optional `sidecar_load`, `results`) used by
//! [`FullState`](super::full_state::FullState). Plans are produced through a
//! [`PlanCollector`], which mints fresh relation handles from each chain's
//! output schema and binds chains to their sinks. The terminal plan ends in a
//! [`SinkType::Relation`] keyed by [`FSR_RESULTS`]; the caller reads the
//! relation after executing every plan in [`ResultPlan::plans`].

use std::sync::Arc;

use url::Url;

use super::checkpoint_shape::{checkpoint_manifest_scan_schema, CheckpointShape};
use super::dedup::{fsr_dedup_key, fsr_row_has_identity_predicate, FSR_JOIN_KEY_COL};
use super::retention::retention_filter;
use super::schemas::{action_read_schema, augmented_action_schema, path_size_schema};
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::SIDECAR_NAME;
use crate::expressions::{col, ColumnName, Scalar};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{
    FileType, LoadSpec, OrderingSpec, RelationHandle, ScanFileColumns, SinkType,
};
use crate::plans::ir::{plan, DeclarativePlanNode, Plan, PlanCollector, ResultPlan};
use crate::schema::{ArrayType, DataType, SchemaBuilder, StructField};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta, Version};

/// Raw per-commit action stream materialized via a Load sink so the downstream
/// dedup chain can window over it. Schema = `action_read_schema` plus a
/// passthrough `version` column.
pub const FSR_COMMIT_RAW: &str = "fsr.commit_raw";
/// Commit winners: the single newest action per `__fsr_join_k` partition
/// produced by the dedup chain. Schema = `action_read_schema` plus
/// `FSR_JOIN_KEY_COL`.
pub const FSR_COMMIT_DEDUP: &str = "fsr.commit_dedup";
/// Top-level checkpoint rows scanned once and reused by sidecar extraction +
/// checkpoint survivor replay via relation references.
pub const FSR_CHECKPOINT_TOP: &str = "fsr.checkpoint_top";
/// Sidecar action stream materialized for V2-multipart checkpoints; absent for
/// V1 / V2-inline checkpoints. Schema = `action_read_schema`.
pub const FSR_SIDECAR_ACTIONS: &str = "fsr.sidecar_actions";
/// Terminal FSR relation: reconstructed live-action rows projected to
/// `action_output_schema`. The result-plan caller reads this relation after
/// executing every plan returned by [`build_fsr_plans`].
pub const FSR_RESULTS: &str = "fsr.results";

/// One literal row describing a Delta JSON commit file consumed by the
/// commit-load Values upstream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitFileMeta {
    pub path: String,
    pub size: i64,
    pub version: Version,
}

/// Build the canonical FSR [`ResultPlan`] for `snapshot` in topological order:
///
/// `[commit_load, commit_dedup, (checkpoint_top, sidecar_load if has_sidecars), results]`.
///
/// Each preceding plan publishes a [`RelationHandle`] consumed by its
/// successors. The terminal plan binds to [`FSR_RESULTS`]; the caller reads
/// that relation after executing every plan in [`ResultPlan::plans`].
pub fn build_fsr_plans(
    snapshot: &Snapshot,
    shape: CheckpointShape,
) -> Result<ResultPlan, DeltaError> {
    let log_root = snapshot.log_segment().log_root.clone();
    let segment = snapshot.log_segment();

    let commits = commit_cover_rows(segment)?;
    let checkpoint_files: Vec<FileMeta> = segment
        .listed
        .checkpoint_parts
        .iter()
        .map(|p| p.location.clone())
        .collect();

    let now = current_time_duration().map_err(|e| e.into_delta_default())?;
    let min_file_ts = deleted_file_retention_timestamp_with_time(
        snapshot.table_properties().deleted_file_retention_duration,
        now,
    )
    .map_err(|e| e.into_delta_default())?;
    let txn_expiry = calculate_transaction_expiration_timestamp(snapshot.table_properties())
        .map_err(|e| e.into_delta_default())?;

    let mut p = PlanCollector::new();

    // === commit_load: VALUES(commits) -> JSON load -> FSR_COMMIT_RAW ===
    // One literal row per commit file; the LoadSink opens each commit JSON and
    // broadcasts the per-commit `version` column onto every emitted action row.
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
    let commit_values = DeclarativePlanNode::values(path_size_schema(true), commit_rows)
        .map_err(|e| e.into_delta_default())?;
    let commit_raw = p.add_load(
        FSR_COMMIT_RAW,
        LoadSpec {
            scan_node: commit_values,
            file_schema: action_read_schema(),
            base_url: log_root.clone(),
            passthrough_columns: vec![ColumnName::new(["version"])],
            file_meta: default_scan_file_columns(),
            file_type: FileType::Json,
            dv_ref: None,
        },
    );

    // === commit_dedup: filter -> add_column -> window_dedup_by -> drop(version) ->
    // FSR_COMMIT_DEDUP === Steps:
    //   1. Filter rows that aren't a recognized action.
    //   2. Materialize the dedup key (`__fsr_join_k`) as a top-level column so the window's
    //      partition key (and the downstream LeftAnti) can reference it by name.
    //   3. Keep only the newest row per (`__fsr_join_k`) by `version DESC`.
    //   4. Drop the per-commit `version` column; the persisted relation matches
    //      `augmented_action_schema(false)` for the union in the results plan.
    let commit_dedup = p.add_relation(
        FSR_COMMIT_DEDUP,
        plan::relation_ref(&commit_raw)
            .filter(Arc::new(fsr_row_has_identity_predicate().into()))
            .add_column(
                StructField::nullable(FSR_JOIN_KEY_COL, dedup_key_type()),
                Arc::new(fsr_dedup_key()),
            )
            .window_dedup_by(
                [col(FSR_JOIN_KEY_COL)],
                [OrderingSpec::desc(ColumnName::new(["version"]))],
            )
            .map_err(|e| e.into_delta_default())?
            .drop_column("version"),
    );

    // === checkpoint_top (optional): bare scan of top-level checkpoint parts ===
    let checkpoint_top = if checkpoint_files.is_empty() {
        None
    } else {
        Some(p.add_relation(
            FSR_CHECKPOINT_TOP,
            DeclarativePlanNode::scan(
                shape.file_format,
                checkpoint_files.clone(),
                checkpoint_manifest_scan_schema(shape.has_sidecars),
            ),
        ))
    };

    // === sidecar_load (V2-multipart only): extract sidecar pointers and read each parquet ===
    // Filters top-level rows lacking a sidecar pointer, projects the sidecar
    // path/size, and reads the referenced parquet under `<log_root>/_sidecars/`.
    let sidecar = if shape.has_sidecars {
        let checkpoint_top_handle = checkpoint_top.as_ref().ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::build_fsr_plans: has_sidecars=true but no checkpoint files were listed",
            )
        })?;
        let sidecar_base = log_root.join("_sidecars/").map_err(|e| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::build_fsr_plans::join_sidecar_base: join _sidecars base URL: {e}",
            )
        })?;
        let sidecar_scan = plan::relation_ref(checkpoint_top_handle)
            .filter(Arc::new(col(SIDECAR_NAME).is_not_null().into()))
            .project(
                vec![
                    col([SIDECAR_NAME, "path"]).into(),
                    col([SIDECAR_NAME, "sizeInBytes"]).into(),
                ],
                path_size_schema(false),
            );
        Some(p.add_load(
            FSR_SIDECAR_ACTIONS,
            LoadSpec {
                scan_node: sidecar_scan,
                file_schema: action_read_schema(),
                base_url: sidecar_base,
                passthrough_columns: vec![],
                file_meta: default_scan_file_columns(),
                file_type: FileType::Parquet,
                dv_ref: None,
            },
        ))
    } else {
        None
    };

    let results_plan = build_results_plan(
        &commit_dedup,
        sidecar.as_ref(),
        checkpoint_top.as_ref(),
        min_file_ts,
        txn_expiry,
    )?;
    // The terminal plan is built by `build_results_plan` with an explicit
    // `FSR_RESULTS` Relation sink so that branches publishing different inner
    // schemas (no-checkpoint `action_read_schema` vs. with-checkpoint
    // `action_output_schema`) still expose the canonical strict shape to the
    // caller.
    let result_relation = match &results_plan.sink.sink_type {
        SinkType::Relation(h) => h.clone(),
        other => {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::build_fsr_plans: internal: terminal plan must end in a Relation sink, got {other:?}",
            ));
        }
    };
    p.push_plan(results_plan);
    Ok(ResultPlan::new(p.into_vec(), result_relation))
}

/// Type of the synthetic [`FSR_JOIN_KEY_COL`] column: `ARRAY<STRING?>?`.
fn dedup_key_type() -> DataType {
    DataType::Array(Box::new(ArrayType::new(DataType::STRING, true)))
}

/// File-meta column hints shared by every FSR load: `path` column, optional
/// `size` column, no record-count column.
fn default_scan_file_columns() -> ScanFileColumns {
    ScanFileColumns {
        path: ColumnName::new(["path"]),
        size: Some(ColumnName::new(["size"])),
        record_count: None,
    }
}

/// Materialize the minimal set of commit/compaction file rows that cover the
/// log segment, preferring compactions over the commits they subsume.
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

/// Plan 4 (terminal): assemble the live snapshot rows from commit winners +
/// checkpoint survivors and bind them to the [`FSR_RESULTS`] relation. The
/// caller reads that relation after executing every plan in the result-plan
/// vector.
///
/// Inline shape:
///
/// ```text
/// checkpoint_full = top_scan [U relation_ref(sidecar_actions)]
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
///     | into_relation(FSR_RESULTS)
/// ```
///
/// The top-level checkpoint is read with the *full* action schema (missing
/// fields resolve to NULL) so the union with the sidecar relation and the
/// commit-dedup relation is schema-compatible without an explicit alignment
/// project.
///
/// The terminal handle's published schema is [`action_read_schema`] (the
/// all-nullable shape the chain actually emits). Callers that need the
/// strict per-action shape -- notably `scan_metadata_plans_with_shape` --
/// rebind to [`action_output_schema`] separately at their own boundary.
fn build_results_plan(
    commit_dedup_handle: &RelationHandle,
    sidecar_handle: Option<&RelationHandle>,
    checkpoint_top_handle: Option<&RelationHandle>,
    min_file_retention_timestamp: i64,
    txn_expiration_cutoff: Option<i64>,
) -> Result<Plan, DeltaError> {
    let result_handle = RelationHandle::fresh(FSR_RESULTS, action_read_schema());
    // No checkpoint parts: there is no checkpoint side to anti-join. The full snapshot state is
    // entirely determined by commit winners.
    let Some(checkpoint_top_handle) = checkpoint_top_handle else {
        let chain = plan::relation_ref(commit_dedup_handle)
            .filter(Arc::new(
                retention_filter(min_file_retention_timestamp, txn_expiration_cutoff).into(),
            ))
            .drop_column(FSR_JOIN_KEY_COL);
        return Ok(chain.into_relation(result_handle));
    };

    let dedup_expr = Arc::new(fsr_dedup_key());
    let augmented_schema = augmented_action_schema(false)?;

    // Top-level checkpoint rows are preloaded once into `FSR_CHECKPOINT_TOP`; drop the optional
    // sidecar column so the schema aligns with sidecar/action relations.
    let top_scan = plan::relation_ref(checkpoint_top_handle).drop_column(SIDECAR_NAME);

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
            action_read_schema()
                .fields()
                .map(|f| col(f.name().as_str()).into())
                .chain(std::iter::once(Arc::clone(&dedup_expr)))
                .collect(),
            augmented_schema.clone(),
        );

    // Build side: just the dedup keys from commit winners (one column wide). LeftAnti emits
    // probe rows whose key is NOT in the build set, mirroring the probe child's schema.
    let join_key_only_schema = SchemaBuilder::new()
        .with_nullable(FSR_JOIN_KEY_COL, dedup_key_type())
        .build()
        .map_err(|e| e.into_delta_default())?;
    let commit_keys = plan::relation_ref(commit_dedup_handle)
        .project(vec![col(FSR_JOIN_KEY_COL).into()], join_key_only_schema);

    let survivors = checkpoint_keyed.left_anti_join_on(
        commit_keys,
        vec![col(FSR_JOIN_KEY_COL).into()],
        vec![col(FSR_JOIN_KEY_COL).into()],
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
        .project(
            action_read_schema()
                .fields()
                .map(|f| col(f.name().as_str()).into())
                .collect(),
            action_read_schema(),
        )
        .into_relation(result_handle))
}
