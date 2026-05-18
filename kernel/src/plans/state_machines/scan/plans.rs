//! Builders for the FSR plan vector.
//!
//! [`build_fsr_plans`] composes the four canonical FSR plans (`commit_load`,
//! `commit_dedup`, optional `sidecar_load`, `results`) used by
//! [`FullState`](super::full_state::FullState). Plans are produced through a
//! a local plan vector plus a [`RelationRegistry`] that mints fresh relation
//! handles from each chain's output schema and binds chains to their sinks.
//! The terminal plan ends in a [`SinkType::Relation`] keyed by
//! [`FSR_RESULTS`]; the caller reads the relation after executing every plan
//! in [`ResultPlan::plans`].

use std::sync::Arc;

use url::Url;

use super::checkpoint_shape::{checkpoint_manifest_scan_schema, CheckpointShape};
use super::dedup::{fsr_dedup_key, fsr_row_has_identity_predicate, FSR_JOIN_KEY_COL};
use super::retention::retention_filter;
use super::schemas::{action_schema, augmented_action_schema, path_size_schema};
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::SIDECAR_NAME;
use crate::expressions::{col, ColumnName, Expression, Scalar};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{
    FileFormat, FileType, JoinType, OrderingSpec, RelationHandle, ScanFileColumns, SinkType,
    WindowFunction,
};
use crate::plans::ir::{Plan, PlanBuilder, RelationRegistry, ResultPlan};
use crate::schema::{ArrayType, DataType, SchemaBuilder};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta, Version};

/// Raw per-commit action stream materialized via a Load sink so the downstream
/// dedup chain can window over it. Schema = `action_schema` plus a
/// passthrough `version` column.
pub const FSR_COMMIT_RAW: &str = "fsr.commit_raw";
/// Commit winners: the single newest action per `__fsr_join_k` partition
/// produced by the dedup chain. Schema = `action_schema` plus
/// `FSR_JOIN_KEY_COL`.
pub const FSR_COMMIT_DEDUP: &str = "fsr.commit_dedup";
/// Top-level checkpoint rows scanned once and reused by sidecar extraction +
/// checkpoint survivor replay via relation references.
pub const FSR_CHECKPOINT_TOP: &str = "fsr.checkpoint_top";
/// Sidecar action stream materialized for V2-multipart checkpoints; absent for
/// V1 / V2-inline checkpoints. Schema = `action_schema`.
pub const FSR_SIDECAR_ACTIONS: &str = "fsr.sidecar_actions";
/// Terminal FSR relation: reconstructed live-action rows projected to
/// `action_schema`. The result-plan caller reads this relation after
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
///
/// `registry` owns naming for every minted handle: callers thread their own
/// (typically SM-identity-scoped) registry through so all FSR-produced
/// relation IDs share the caller's `sm_id` prefix.
pub fn build_fsr_plans(
    snapshot: &Snapshot,
    shape: CheckpointShape,
    registry: &mut RelationRegistry,
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

    let mut plans: Vec<Plan> = Vec::new();

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
    let commit_values = PlanBuilder::values(path_size_schema(true), commit_rows)
        .map_err(|e| e.into_delta_default())?;
    let commit_raw_plan = commit_values.load(
        FSR_COMMIT_RAW,
        action_schema(),
        FileType::Json,
        Some(log_root.clone()),
        vec![ColumnName::new(["version"])],
        default_scan_file_columns(),
        None,
        registry,
    )?;
    plans.push(commit_raw_plan);

    // === commit_dedup: filter -> project(+join-key) -> window(row_number) -> filter rn<=1 ->
    // project(action + join-key) ->
    // FSR_COMMIT_DEDUP === Steps:
    //   1. Filter rows that aren't a recognized action.
    //   2. Materialize the dedup key (`__fsr_join_k`) as a top-level column so the window's
    //      partition key (and the downstream LeftAnti) can reference it by name.
    //   3. Keep only the newest row per (`__fsr_join_k`) by `version DESC`.
    //   4. Project away internal columns (`version`, row_number); persisted relation
    //      matches `augmented_action_schema(false)` for the union in the results plan.
    const COMMIT_DEDUP_RN_COL: &str = "__kernel_fsr_commit_dedup_rn";
    let commit_dedup_plan = registry
        .relation_ref(FSR_COMMIT_RAW)?
        .filter(Arc::new(fsr_row_has_identity_predicate().into()))
        .project(
            action_schema()
                .fields()
                .map(|f| col(f.name().as_str()).into())
                .chain(std::iter::once(Arc::new(fsr_dedup_key())))
                .chain(std::iter::once(col("version").into()))
                .collect(),
            augmented_action_schema(true)?,
        )
        .window(
            vec![WindowFunction {
                output_col: COMMIT_DEDUP_RN_COL.to_string(),
            }],
            vec![col(FSR_JOIN_KEY_COL).into()],
            vec![OrderingSpec::desc(ColumnName::new(["version"]))],
        )
        .map_err(|e| e.into_delta_default())?
        .filter(Arc::new(
            Expression::column([COMMIT_DEDUP_RN_COL])
                .le(Expression::literal(1i64))
                .into(),
        ))
        .project(
            action_schema()
                .fields()
                .map(|f| col(f.name().as_str()).into())
                .chain(std::iter::once(col(FSR_JOIN_KEY_COL).into()))
                .collect(),
            augmented_action_schema(false)?,
        )
        .into_relation(FSR_COMMIT_DEDUP, registry)?;
    plans.push(commit_dedup_plan);

    // === checkpoint_top (optional): bare scan of top-level checkpoint parts ===
    let checkpoint_top = if checkpoint_files.is_empty() {
        None
    } else {
        let checkpoint_scan = match shape.file_format {
            FileFormat::Parquet => PlanBuilder::scan_parquet(
                checkpoint_files.clone(),
                checkpoint_manifest_scan_schema(shape.has_sidecars),
            ),
            FileFormat::Json => PlanBuilder::scan_json(
                checkpoint_files.clone(),
                checkpoint_manifest_scan_schema(shape.has_sidecars),
            ),
        };
        let checkpoint_top_plan = checkpoint_scan.into_relation(FSR_CHECKPOINT_TOP, registry)?;
        let checkpoint_top = relation_output_handle(&checkpoint_top_plan)?;
        plans.push(checkpoint_top_plan);
        Some(checkpoint_top)
    };

    // === sidecar_load (V2-multipart only): extract sidecar pointers and read each parquet ===
    // Filters top-level rows lacking a sidecar pointer, projects the sidecar
    // path/size, and reads the referenced parquet under `<log_root>/_sidecars/`.
    let sidecar = if shape.has_sidecars {
        let _checkpoint_top_handle = checkpoint_top.as_ref().ok_or_else(|| {
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
        let sidecar_scan = registry
            .relation_ref(FSR_CHECKPOINT_TOP)?
            .filter(Arc::new(col(SIDECAR_NAME).is_not_null().into()))
            .project(
                vec![
                    col([SIDECAR_NAME, "path"]).into(),
                    col([SIDECAR_NAME, "sizeInBytes"]).into(),
                ],
                path_size_schema(false),
            );
        let sidecar_plan = sidecar_scan.load(
            FSR_SIDECAR_ACTIONS,
            action_schema(),
            FileType::Parquet,
            Some(sidecar_base),
            vec![],
            default_scan_file_columns(),
            None,
            registry,
        )?;
        let sidecar_handle = load_output_relation(&sidecar_plan)?;
        plans.push(sidecar_plan);
        Some(sidecar_handle)
    } else {
        None
    };

    let results_plan = build_results_plan(
        registry,
        sidecar.as_ref(),
        checkpoint_top.as_ref(),
        min_file_ts,
        txn_expiry,
    )?;
    // The terminal plan is built by `build_results_plan` with an explicit
    // `FSR_RESULTS` Relation sink exposing `action_schema`. Both the
    // no-checkpoint and with-checkpoint branches publish the same all-nullable
    // contract here -- the executor's per-batch validation enforces that the
    // declared relation schema matches the materialized rows.
    let result_relation = match &results_plan.sink {
        SinkType::Relation(h) => h.clone(),
        other => {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "fsr::build_fsr_plans: internal: terminal plan must end in a Relation sink, got {other:?}",
            ));
        }
    };
    plans.push(results_plan);
    Ok(ResultPlan::new(plans, result_relation))
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

fn relation_output_handle(plan: &Plan) -> Result<RelationHandle, DeltaError> {
    match &plan.sink {
        SinkType::Relation(h) => Ok(h.clone()),
        other => Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "internal: expected Relation sink, got {other:?}",
        )),
    }
}

fn load_output_relation(plan: &Plan) -> Result<RelationHandle, DeltaError> {
    match &plan.sink {
        SinkType::Load(load) => Ok(load.output_relation.clone()),
        other => Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "internal: expected Load sink, got {other:?}",
        )),
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
///     | Project(action_schema)
///     | into_relation(FSR_RESULTS)
/// ```
///
/// The top-level checkpoint is read with the *full* action schema (missing
/// fields resolve to NULL) so the union with the sidecar relation and the
/// commit-dedup relation is schema-compatible without an explicit alignment
/// project.
///
/// The terminal handle's published schema is [`action_schema`] (the
/// all-nullable shape the chain actually emits).
fn build_results_plan(
    registry: &mut RelationRegistry,
    sidecar_handle: Option<&RelationHandle>,
    checkpoint_top_handle: Option<&RelationHandle>,
    min_file_retention_timestamp: i64,
    txn_expiration_cutoff: Option<i64>,
) -> Result<Plan, DeltaError> {
    // No checkpoint parts: there is no checkpoint side to anti-join. The full snapshot state is
    // entirely determined by commit winners.
    let Some(_checkpoint_top_handle) = checkpoint_top_handle else {
        let chain = registry
            .relation_ref(FSR_COMMIT_DEDUP)?
            .filter(Arc::new(
                retention_filter(min_file_retention_timestamp, txn_expiration_cutoff).into(),
            ))
            .project(
                action_schema()
                    .fields()
                    .map(|f| col(f.name().as_str()).into())
                    .collect(),
                action_schema(),
            );
        return chain.into_relation(FSR_RESULTS, registry);
    };

    let dedup_expr = Arc::new(fsr_dedup_key());
    let augmented_schema = augmented_action_schema(false)?;

    // Top-level checkpoint rows are preloaded once into `FSR_CHECKPOINT_TOP`; project to canonical
    // action columns so the schema aligns with sidecar/action relations.
    let top_scan = registry.relation_ref(FSR_CHECKPOINT_TOP)?.project(
        action_schema()
            .fields()
            .map(|f| col(f.name().as_str()).into())
            .collect(),
        action_schema(),
    );

    let checkpoint_full = match sidecar_handle {
        Some(_handle) => PlanBuilder::union(
            vec![top_scan, registry.relation_ref(FSR_SIDECAR_ACTIONS)?],
            false,
        )
        .map_err(|e| e.into_delta_default())?,
        None => top_scan,
    };

    let checkpoint_keyed = checkpoint_full
        .filter(Arc::new(fsr_row_has_identity_predicate().into()))
        .project(
            action_schema()
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
    let commit_keys = registry
        .relation_ref(FSR_COMMIT_DEDUP)?
        .project(vec![col(FSR_JOIN_KEY_COL).into()], join_key_only_schema);

    let survivors = commit_keys
        .join(checkpoint_keyed, JoinType::LeftAnti)
        .map_err(|e| e.into_delta_default())?;

    // Union directly on augmented rows (action + join key); drop join key once at the end.
    let everything = PlanBuilder::union(
        vec![registry.relation_ref(FSR_COMMIT_DEDUP)?, survivors],
        false,
    )
    .map_err(|e| e.into_delta_default())?;

    everything
        .filter(Arc::new(
            retention_filter(min_file_retention_timestamp, txn_expiration_cutoff).into(),
        ))
        .project(
            action_schema()
                .fields()
                .map(|f| col(f.name().as_str()).into())
                .collect(),
            action_schema(),
        )
        .into_relation(FSR_RESULTS, registry)
}
