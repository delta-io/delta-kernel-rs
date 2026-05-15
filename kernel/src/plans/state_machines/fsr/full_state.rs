//! Canonical Full Snapshot Read (FSR) declarative plans — *window-on-commits +
//! anti-join-on-checkpoint*.
//!
//! Mirrors Delta log-replay semantics (`kernel/src/action_reconciliation/log_replay.rs`,
//! `kernel/src/log_replay/deduplicator.rs`) by composing three or four declarative plans
//! that flow as one [`PhaseOperation::Plans`] step:
//!
//! 1. **commit_load** — `Values(commit metadata) → LoadSink(JSON, action schema,
//!    passthrough=[version])` materializes the raw per-commit action stream into
//!    [`FSR_COMMIT_RAW`]. The kernel cover over `ascending_commit_files ∪
//!    ascending_compaction_files` is materialized verbatim so that downstream steps can `ORDER BY
//!    version DESC` to recover Delta's "newest action wins" semantics inside the commit tail.
//! 2. **commit_dedup** — `RelationRef(commit_raw) → Filter(has identity) → Project(action_cols +
//!    key) → Window(row_number PARTITION BY key ORDER BY version DESC) → Filter(__rn <= k) →
//!    Project(action_cols + key)` yields the *commit winners*, materialized into
//!    [`FSR_COMMIT_DEDUP`]. Commits supersede (and remove-tombstone) checkpoint state for any
//!    `(action_kind, identity)` pair they touch.
//! 3. **(only when `has_sidecars`) sidecar_load** — `Scan(top-level checkpoint, sidecar-only
//!    schema) → Filter(sidecar IS NOT NULL) → Project(sidecar.path, sidecar.sizeInBytes) →
//!    LoadSink(Parquet, action schema)` materializes each V2-multipart sidecar parquet's action
//!    rows into [`FSR_SIDECAR_ACTIONS`]. V1 / V2-inline checkpoints carry no sidecars; this plan is
//!    omitted entirely so the executor never opens them.
//! 4. **results** — `(Scan(top-level checkpoint) [∪ RelationRef(sidecar_actions)]) → Filter(has
//!    identity) → Project(action_cols + key) → LeftAntiJoin(probe=this,
//!    build=RelationRef(commit_dedup).project(key))` materializes the *checkpoint survivors* (rows
//!    the commit tail didn't touch). The plan completes with `Union(RelationRef(commit_dedup),
//!    survivors) -> Filter(retention) -> Project(action_read_schema) -> into_results()` so the
//!    engine's `Results` consumer sees the reconstructed action stream.
//!
//! The window applies only to the (typically-small) commit-tail stream; the (typically-large)
//! checkpoint stream goes through a single hash anti-join keyed on the dedup column. Compared
//! with windowing the union of commits and checkpoint, this avoids materializing per-key
//! orderings over the entire snapshot.
//!
//! ## Decision notes
//!
//! - **`dv_unique_id`**: Per-row deletion-vector identity, used only as a `partition_by` / join-key
//!   contribution to the dedup key. Implemented as `If(storageType IS NULL, NULL,
//!   ToJson(Array(storageType, pathOrInlineDv)))` — semantics-equivalent to
//!   [`crate::actions::deletion_vector::DeletionVectorDescriptor::unique_id_from_parts`] (matches
//!   [`crate::log_replay::deduplicator::Deduplicator::extract_dv_unique_id`]) for equality /
//!   non-equality but not byte-for-byte. The exact byte form is not protocol-stable, so the
//!   difference does not affect correctness; it only avoids overloading
//!   [`crate::expressions::BinaryExpressionOp::Plus`] with UTF-8 concat semantics. Note: `offset`
//!   is intentionally omitted (UUID DVs have no offset; inline DVs with the same `pathOrInlineDv`
//!   and distinct offsets would imply two distinct in-line DV byte payloads, which is not
//!   representable). A follow-up can include `offset` once kernel grows an int-to-string cast.
//! - **`action_read_schema`**: Full reconstructed action stream (add / remove / protocol / metaData
//!   / domainMetadata / txn).
//! - **Retention thresholds**: Derived like checkpoint reconciliation via
//!   [`crate::action_reconciliation::deleted_file_retention_timestamp_with_time`] and
//!   [`crate::action_reconciliation::calculate_transaction_expiration_timestamp`] against
//!   [`crate::snapshot::Snapshot::table_properties`] (`kernel/src/table_properties/mod.rs`).
//! - **`CheckpointShape.file_format`**: Taken from the first checkpoint part's filename extension
//!   in the snapshot listing (`crate::path::ParsedLogPath::extension`), with `_last_checkpoint`
//!   schema falling back through [`crate::log_segment::LogSegment::checkpoint_schema`].

use std::collections::HashSet;
use std::sync::Arc;

use url::Url;

use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
    SIDECAR_NAME,
};
use crate::expressions::{BinaryExpressionOp, ColumnName, Expression, Predicate, Scalar};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{
    DvRef, FileFormat, FileType, JoinHint, JoinNode, JoinType, LoadSink, OrderingSpec,
    RelationHandle, ScanFileColumns, WindowFunction,
};
use crate::plans::ir::{DeclarativePlanNode, Extractor, Plan};
use crate::plans::kdf::SidecarCollector;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::{PhaseOperation, SchemaQueryNode};
use crate::scan::data_skipping::stats_schema::schema_with_all_fields_nullable;
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::scan::Scan;
use crate::schema::{
    ArrayType, DataType, MetadataColumnSpec, SchemaRef, StructField, StructType, ToSchema,
};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta, Version};

/// Raw per-commit action stream, materialized by [`build_commit_load_plan`] into a
/// [`LoadSink`] so the downstream [`build_commit_dedup_plan`] can window over it. Schema =
/// [`action_read_schema`] plus a passthrough `version` column.
pub const FSR_COMMIT_RAW: &str = "fsr.commit_raw";
/// Commit winners: the single newest action per `__fsr_join_k` partition produced by
/// [`build_commit_dedup_plan`]. Schema = [`action_read_schema`] plus the dedup-key column
/// [`FSR_JOIN_KEY_COL`].
pub const FSR_COMMIT_DEDUP: &str = "fsr.commit_dedup";
/// Top-level checkpoint rows scanned once and reused by sidecar extraction + checkpoint survivor
/// replay via relation references.
pub const FSR_CHECKPOINT_TOP: &str = "fsr.checkpoint_top";
/// Sidecar action stream materialized by [`build_sidecar_load_plan`] for V2-multipart
/// checkpoints; absent (relation handle never created) for V1 / V2-inline. Schema =
/// [`action_read_schema`].
pub const FSR_SIDECAR_ACTIONS: &str = "fsr.sidecar_actions";

/// Synthetic column carrying the materialized dedup key array so hash joins only need top-level
/// column keys (`delta-kernel-datafusion-engine/src/compile/join.rs`).
pub const FSR_JOIN_KEY_COL: &str = "__fsr_join_k";
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

/// Resolved checkpoint encoding + schema hints. `actions_schema_subset` keys the top-level
/// checkpoint scan in [`build_results_plan`]; `has_sidecars` decides whether
/// [`build_sidecar_load_plan`] is appended to the plan vector.
#[derive(Clone, Debug)]
pub struct CheckpointShape {
    pub file_format: FileFormat,
    pub has_sidecars: bool,
    pub actions_schema_subset: SchemaRef,
}

/// Builder namespace for canonical Full State Reconstruction planning.
#[derive(Debug, Clone, Copy, Default)]
pub struct FullState;

/// Builder for canonical Full State Reconstruction plans.
#[derive(Debug, Clone)]
pub struct FullStateBuilder {
    snapshot: Arc<Snapshot>,
    checkpoint_shape: Option<CheckpointShape>,
}

impl FullState {
    /// Start building canonical FSR plans for `snapshot`.
    pub fn for_table(snapshot: Arc<Snapshot>) -> FullStateBuilder {
        FullStateBuilder {
            snapshot,
            checkpoint_shape: None,
        }
    }
}

impl FullStateBuilder {
    /// Override checkpoint shape discovery used by [`Self::build`].
    pub fn with_checkpoint_shape(mut self, shape: CheckpointShape) -> Self {
        self.checkpoint_shape = Some(shape);
        self
    }

    /// Build canonical FSR plans for this snapshot.
    pub fn build(self) -> Result<Vec<Plan>, DeltaError> {
        let shape = self
            .checkpoint_shape
            .unwrap_or(checkpoint_shape_from_last_checkpoint(
                self.snapshot.as_ref(),
            )?);
        build_fsr_plans(self.snapshot.as_ref(), shape)
    }

    /// Enable stats-aware planning on this builder.
    ///
    /// This is currently an API-level compatibility shim to align the FullState
    /// builder surface with scan replay builders. Canonical FSR output plans
    /// already preserve Add action stats columns, so there is no additional
    /// rewrite needed at this layer.
    pub fn with_stats(self) -> Self {
        self
    }
}

fn replay_partition_columns(scan: &Scan) -> HashSet<String> {
    scan.snapshot()
        .table_configuration()
        .partition_columns()
        .iter()
        .filter(|name| scan.logical_schema().contains(name.as_str()))
        .cloned()
        .collect()
}

impl Scan {
    /// Build replay plans for the metadata phase.
    ///
    /// Returns the metadata plans and the live-actions relation handle that must be fed
    /// into [`Self::replay_scan_data_plans`].
    pub fn scan_metadata_state_machine(&self) -> Result<(Vec<Plan>, RelationHandle), DeltaError> {
        let checkpoint_shape = checkpoint_shape_from_last_checkpoint(self.snapshot().as_ref())?;
        scan_metadata_plans_with_shape(self, checkpoint_shape)
    }

    /// Build replay plans for the data phase.
    pub fn replay_scan_data_plans(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<Vec<Plan>, DeltaError> {
        let snapshot = self.snapshot();
        let partition_columns = replay_partition_columns(self);
        let file_schema = scan_data_file_schema(self.physical_schema(), self.logical_schema())?;
        let data_rows_raw_relation = RelationHandle::fresh(
            "scan.data_rows_raw",
            load_materialized_schema(
                &file_schema,
                &scan_live_actions_schema(
                    scan_partition_values_physical_schema(
                        snapshot.as_ref(),
                        self.logical_schema(),
                    )?
                    .as_ref(),
                ),
                &[FILE_CONSTANT_VALUES_NAME, "path"],
            )?,
        );
        let load = LoadSink {
            output_relation: data_rows_raw_relation.clone(),
            file_schema: self.physical_schema().clone(),
            base_url: Some(snapshot.table_root().clone()),
            file_meta: ScanFileColumns {
                path: ColumnName::new(["path"]),
                size: Some(ColumnName::new(["size"])),
                record_count: None,
            },
            dv_ref: Some(DvRef::skip(ColumnName::new(["deletionVector"]))),
            passthrough_columns: vec![
                ColumnName::new([FILE_CONSTANT_VALUES_NAME]),
                ColumnName::new(["path"]),
            ],
            file_type: FileType::Parquet,
        };
        let f1 = DeclarativePlanNode::relation_ref(live_actions_relation).into_load(load);
        let logical_schema = self.logical_schema().clone();
        let logical_projection = scan_data_projection(
            &logical_schema,
            self.physical_schema(),
            &partition_columns,
            self.snapshot().table_configuration().column_mapping_mode(),
        )?;
        let f2 = DeclarativePlanNode::relation_ref(data_rows_raw_relation)
            .project(logical_projection, logical_schema.clone())
            .into_results_with_schema(logical_schema);
        Ok(vec![f1, f2])
    }

    /// Build replay plans for the full composed scan.
    pub fn replay_scan_plans(&self) -> Result<Vec<Plan>, DeltaError> {
        let (mut plans, live_actions_relation) = self.scan_metadata_state_machine()?;
        plans.extend(self.replay_scan_data_plans(live_actions_relation)?);
        Ok(plans)
    }

    /// Build a coroutine SM for metadata-only replay scan execution.
    ///
    /// The SM resolves checkpoint shape hints the same way as
    /// [`Self::replay_scan_state_machine`], executes metadata replay plans, and
    /// returns the materialized live-actions relation handle.
    pub fn replay_scan_metadata_state_machine(
        &self,
    ) -> Result<CoroutineSM<RelationHandle>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let shape = resolve_checkpoint_shape_for_scan(&mut phase, &scan).await?;
            let (metadata, live_actions_relation) = scan_metadata_plans_with_shape(&scan, shape)?;
            let _metadata_state = phase
                .execute(PhaseOperation::Plans(metadata), "scan.replay.metadata")
                .await
                .map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "scan::replay_scan_metadata_state_machine::metadata_phase",
                        detail = e.display_with_source_chain(),
                        source = e,
                    )
                })?;
            Ok(live_actions_relation)
        })
    }

    /// Build a coroutine SM for data-only replay scan execution.
    ///
    /// The caller provides the live-actions relation produced by metadata
    /// replay. The SM executes only the data-phase plans.
    pub fn replay_scan_data_state_machine(
        &self,
        live_actions_relation: RelationHandle,
    ) -> Result<CoroutineSM<()>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let data = scan.replay_scan_data_plans(live_actions_relation)?;
            let _data_state = phase
                .execute(PhaseOperation::Plans(data), "scan.replay.data")
                .await
                .map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "scan::replay_scan_data_state_machine::data_phase",
                        detail = e.display_with_source_chain(),
                        source = e,
                    )
                })?;
            Ok(())
        })
    }

    /// Build a coroutine SM for replay scan execution.
    ///
    /// This is the canonical replay path when checkpoint shape is ambiguous: if `_last_checkpoint`
    /// does not provide schema hints, the SM runs a checkpoint-schema query first, and for V2
    /// checkpoints with sidecars it runs sidecar discovery (`ConsumeByKdf`) followed by a sidecar
    /// schema query before building metadata/data plans.
    pub fn replay_scan_state_machine(&self) -> Result<CoroutineSM<()>, DeltaError> {
        let scan = self.clone();
        CoroutineSM::new(move |mut co| async move {
            let mut phase = Phase(&mut co);
            let shape = resolve_checkpoint_shape_for_scan(&mut phase, &scan).await?;

            let (metadata, live_actions_relation) =
                scan_metadata_plans_with_shape(&scan, shape.clone())?;
            let _metadata_state = phase
                .execute(PhaseOperation::Plans(metadata), "scan.replay.metadata")
                .await
                .map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "scan::replay_scan_state_machine::metadata_phase",
                        detail = e.display_with_source_chain(),
                        source = e,
                    )
                })?;
            let data = scan.replay_scan_data_plans(live_actions_relation)?;
            let _data_state = phase
                .execute(PhaseOperation::Plans(data), "scan.replay.data")
                .await
                .map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "scan::replay_scan_state_machine::data_phase",
                        detail = e.display_with_source_chain(),
                        source = e,
                    )
                })?;
            Ok(())
        })
    }
}

fn should_schema_query_checkpoint_for_scan(snapshot: &Snapshot) -> bool {
    snapshot_has_checkpoint_files(snapshot) && snapshot.log_segment().checkpoint_schema().is_none()
}

async fn resolve_checkpoint_shape_for_scan(
    phase: &mut Phase<'_>,
    scan: &Scan,
) -> Result<CheckpointShape, DeltaError> {
    let snapshot = scan.snapshot();
    let mut shape = checkpoint_shape_from_last_checkpoint(snapshot.as_ref())?;

    if should_schema_query_checkpoint_for_scan(snapshot.as_ref()) {
        if shape.file_format == FileFormat::Json {
            // JSON checkpoints are ambiguous without `_last_checkpoint` schema hints:
            // they may be manifest rows that carry sidecar pointers. SchemaQuery reads
            // parquet footers only, so for JSON we must conservatively treat this as
            // possibly-manifest and proceed through sidecar-aware replay.
            shape.has_sidecars = true;
        } else {
            let checkpoint_url = first_checkpoint_url(snapshot.as_ref())?;
            let checkpoint_state = phase
                .execute(
                    PhaseOperation::SchemaQuery(SchemaQueryNode::new(checkpoint_url)),
                    "scan.replay.checkpoint_schema_query",
                )
                .await
                .map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "scan::resolve_checkpoint_shape_for_scan::checkpoint_schema",
                        detail = e.display_with_source_chain(),
                        source = e,
                    )
                })?;
            let checkpoint_schema = checkpoint_state.take_schema().ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "scan::resolve_checkpoint_shape_for_scan::checkpoint_schema",
                    detail = "schema query phase returned no schema",
                )
            })?;
            shape = checkpoint_shape_from_schema(&checkpoint_schema)?;
        }

        if shape.has_sidecars {
            let (discover_sidecars, extract_sidecars) =
                build_sidecar_discovery_plan(snapshot.as_ref(), shape.file_format)?;
            let sidecar_state = phase
                .execute(
                    PhaseOperation::Plans(vec![discover_sidecars]),
                    "scan.replay.sidecar_discovery",
                )
                .await
                .map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "scan::resolve_checkpoint_shape_for_scan::sidecar_discovery",
                        detail = e.display_with_source_chain(),
                        source = e,
                    )
                })?;
            let sidecar_files = extract_sidecars.extract(&sidecar_state).map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "scan::resolve_checkpoint_shape_for_scan::extract_sidecars",
                    detail = e.display_with_source_chain(),
                    source = e,
                )
            })?;
            if let Some(first_sidecar) = sidecar_files.first() {
                let sidecar_state = phase
                    .execute(
                        PhaseOperation::SchemaQuery(SchemaQueryNode::new(
                            first_sidecar.location.as_str(),
                        )),
                        "scan.replay.sidecar_schema_query",
                    )
                    .await
                    .map_err(|e| {
                        delta_error!(
                            DeltaErrorCode::DeltaCommandInvariantViolation,
                            operation = "scan::resolve_checkpoint_shape_for_scan::sidecar_schema",
                            detail = e.display_with_source_chain(),
                            source = e,
                        )
                    })?;
                let sidecar_schema = sidecar_state.take_schema().ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "scan::resolve_checkpoint_shape_for_scan::sidecar_schema",
                        detail = "sidecar schema query phase returned no schema",
                    )
                })?;
                let mut sidecar_shape = checkpoint_shape_from_schema(&sidecar_schema)?;
                // We are in the v2 sidecar branch by construction.
                sidecar_shape.has_sidecars = true;
                shape = sidecar_shape;
            }
        }
    }

    Ok(shape)
}

fn build_sidecar_discovery_plan(
    snapshot: &Snapshot,
    checkpoint_format: FileFormat,
) -> Result<(Plan, Extractor<Vec<FileMeta>>), DeltaError> {
    let checkpoint_files: Vec<FileMeta> = snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .iter()
        .map(|p| p.location.clone())
        .collect();
    let sidecar_scan =
        DeclarativePlanNode::scan(checkpoint_format, checkpoint_files, sidecar_only_schema())
            .filter(Arc::new(
                Predicate::is_not_null(Expression::column(["sidecar"])).into(),
            ));
    Ok(sidecar_scan.consume(SidecarCollector::new(
        snapshot.log_segment().log_root.clone(),
    )))
}

fn scan_metadata_plans_with_shape(
    scan: &Scan,
    checkpoint_shape: CheckpointShape,
) -> Result<(Vec<Plan>, RelationHandle), DeltaError> {
    let snapshot = scan.snapshot();
    let partition_columns = replay_partition_columns(scan);
    let predicate_stats_schema = scan.physical_stats_schema();
    let predicate_partition_schema = scan.physical_partition_schema();
    let partition_values_schema =
        scan_partition_values_physical_schema(snapshot.as_ref(), scan.logical_schema())?;
    let live_actions_relation = RelationHandle::fresh(
        "scan.live_actions",
        scan_live_actions_schema(partition_values_schema.as_ref()),
    );
    let mut plans = build_fsr_plans(snapshot.as_ref(), checkpoint_shape)?;
    let last = plans.pop().ok_or_else(|| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            operation = "fsr::scan::scan_metadata",
            detail = "expected at least one plan from build_fsr_plans",
        )
    })?;
    let fsr_results_relation = RelationHandle::fresh("scan.fsr_results", action_output_schema());
    plans.push(last.root.into_relation(fsr_results_relation.clone()));
    let mut metadata_root = DeclarativePlanNode::relation_ref(fsr_results_relation);
    if let Some(predicate) = scan.build_actions_meta_predicate() {
        let needs_augmented_projection =
            predicate_stats_schema.is_some() || predicate_partition_schema.is_some();
        if needs_augmented_projection {
            metadata_root = metadata_root.project(
                scan_actions_with_parsed_projection(
                    predicate_stats_schema.as_ref(),
                    predicate_partition_schema.as_ref(),
                ),
                action_schema_with_augmented_add(
                    predicate_stats_schema.as_ref(),
                    predicate_partition_schema.as_ref(),
                ),
            );
        }
        let add_path_present = Predicate::is_not_null(Expression::column(["add", "path"]));
        // Data-skipping predicates are best-effort and must never introduce false negatives.
        // Keep rows when the skipping predicate evaluates to NULL (unknown) to avoid over-pruning
        // files with incomplete / partially-missing stats coverage.
        let skip_or_unknown = Predicate::or(
            predicate.as_ref().clone(),
            Predicate::is_null(predicate.as_ref().clone()),
        );
        let combined = Predicate::and(add_path_present, skip_or_unknown);
        metadata_root = metadata_root.filter(Arc::new(combined.into()));
    } else {
        metadata_root = metadata_root.filter(Arc::new(
            Predicate::is_not_null(Expression::column(["add", "path"])).into(),
        ));
    }
    let live_actions = metadata_root.project(
        scan_live_actions_projection(!partition_columns.is_empty()),
        scan_live_actions_schema(partition_values_schema.as_ref()),
    );
    plans.push(live_actions.into_relation(live_actions_relation.clone()));
    Ok((plans, live_actions_relation))
}

/// Public entry for full-state reconstruction.
pub fn full_state_sm(snapshot: Arc<Snapshot>) -> Result<CoroutineSM<()>, DeltaError> {
    CoroutineSM::new(move |mut co| async move {
        let mut phase = Phase(&mut co);

        // Derive shape from log-segment metadata (including checkpoint file format) so
        // JSON-v2 checkpoints are lowered correctly.
        let plans = FullState::for_table(Arc::clone(&snapshot)).build()?;
        let _state = phase
            .execute(PhaseOperation::Plans(plans), "fsr.full_state")
            .await
            .map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "fsr::full_state::execute",
                    detail = e.display_with_source_chain(),
                    source = e,
                )
            })?;
        Ok(())
    })
}

pub fn snapshot_has_checkpoint_files(snapshot: &Snapshot) -> bool {
    !snapshot.log_segment().listed.checkpoint_parts.is_empty()
}

pub fn first_checkpoint_url(snapshot: &Snapshot) -> Result<String, DeltaError> {
    snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .first()
        .map(|p| p.location.location.as_str().to_string())
        .ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "fsr::first_checkpoint_url",
                detail = "snapshot has no checkpoint parts but schema prelude was requested",
            )
        })
}

pub fn checkpoint_shape_from_schema(schema: &SchemaRef) -> Result<CheckpointShape, DeltaError> {
    // SchemaQuery is executed against an on-disk checkpoint part today — treat as Parquet-shaped
    // reads.
    let subset = checkpoint_actions_schema_projection(schema)?;
    Ok(CheckpointShape {
        file_format: FileFormat::Parquet,
        has_sidecars: schema.contains(SIDECAR_NAME),
        actions_schema_subset: subset,
    })
}

pub fn checkpoint_shape_from_last_checkpoint(
    snapshot: &Snapshot,
) -> Result<CheckpointShape, DeltaError> {
    let seg = snapshot.log_segment();
    let has_checkpoint_parts = !seg.listed.checkpoint_parts.is_empty();
    let fmt = seg
        .listed
        .checkpoint_parts
        .first()
        .map(checkpoint_format_from_path)
        .unwrap_or(FileFormat::Json);

    let full_schema = seg.checkpoint_schema().unwrap_or_else(action_read_schema);
    let subset = checkpoint_actions_schema_projection(&full_schema)?;
    Ok(CheckpointShape {
        file_format: fmt,
        has_sidecars: full_schema.contains(SIDECAR_NAME)
            || (has_checkpoint_parts && matches!(fmt, FileFormat::Json)),
        actions_schema_subset: subset,
    })
}

fn checkpoint_format_from_path(cp: &ParsedLogPath<FileMeta>) -> FileFormat {
    match cp.extension.as_str() {
        "json" => FileFormat::Json,
        _ => FileFormat::Parquet,
    }
}

fn checkpoint_actions_schema_projection(full: &SchemaRef) -> Result<SchemaRef, DeltaError> {
    const WANT: &[&str] = &[
        ADD_NAME,
        REMOVE_NAME,
        PROTOCOL_NAME,
        METADATA_NAME,
        DOMAIN_METADATA_NAME,
        SET_TRANSACTION_NAME,
    ];
    let names: Vec<_> = WANT.iter().copied().filter(|n| full.contains(*n)).collect();
    if names.is_empty() {
        Ok(action_read_schema())
    } else {
        full.project(names.as_slice())
            .map_err(|e| e.into_delta_default())
    }
}

/// Build the full FSR plan vector for `snapshot` in topological order:
///
/// `[commit_load, commit_dedup, (sidecar_load if has_sidecars), results]`.
///
/// - `commit_load` → `commit_dedup` is sequential by relation handle dependency
///   ([`FSR_COMMIT_RAW`]).
/// - `sidecar_load` (when present) → `results` is sequential via [`FSR_SIDECAR_ACTIONS`].
/// - `commit_dedup` → `results` is sequential via [`FSR_COMMIT_DEDUP`].
///
/// The single [`PhaseOperation::Plans`] yield wraps all of them; the executor walks them
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

    let commit_raw_schema = load_materialized_schema(
        &action_read_schema(),
        &path_size_version_schema(),
        &["version"],
    )?;
    let commit_dedup_schema = augmented_action_schema()?;

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

fn load_materialized_schema(
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
                operation = "fsr::load_materialized_schema",
                detail = format!(
                    "upstream schema {:?} missing passthrough `{name}`",
                    upstream
                ),
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

/// Action schema for the action-replay stream.
///
/// When `relaxed_nesting` is true (transport schema used while replaying / unioning action rows
/// from heterogeneous sources), nested fields are made all-nullable to avoid planner-time cast
/// failures during engine materialization. When false, the strict per-action `ToSchema` is used
/// (kernel-visible output schema).
fn action_schema(relaxed_nesting: bool) -> SchemaRef {
    let relax = |s: StructType| {
        if relaxed_nesting {
            schema_with_all_fields_nullable(&s).unwrap_or(s)
        } else {
            s
        }
    };
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, relax(Add::to_schema())),
        StructField::nullable(REMOVE_NAME, relax(Remove::to_schema())),
        StructField::nullable(PROTOCOL_NAME, relax(Protocol::to_schema())),
        StructField::nullable(METADATA_NAME, relax(Metadata::to_schema())),
        StructField::nullable(DOMAIN_METADATA_NAME, relax(DomainMetadata::to_schema())),
        StructField::nullable(SET_TRANSACTION_NAME, relax(SetTransaction::to_schema())),
    ]))
}

fn action_read_schema() -> SchemaRef {
    action_schema(true)
}

fn action_output_schema() -> SchemaRef {
    action_schema(false)
}

fn path_size_version_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("version", DataType::LONG),
    ]))
}

fn path_size_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
    ]))
}

fn scan_partition_values_schema(
    snapshot: &Snapshot,
    logical_schema: &SchemaRef,
) -> Option<SchemaRef> {
    let partition_columns = snapshot.table_configuration().partition_columns();
    if partition_columns.is_empty() {
        return None;
    }
    let fields = logical_schema
        .fields()
        .filter(|f| partition_columns.contains(f.name()))
        .cloned()
        .collect::<Vec<_>>();
    if fields.is_empty() {
        None
    } else {
        Some(Arc::new(StructType::new_unchecked(fields)))
    }
}

fn scan_partition_values_physical_schema(
    snapshot: &Snapshot,
    logical_schema: &SchemaRef,
) -> Result<Option<SchemaRef>, DeltaError> {
    let Some(logical_partition_schema) = scan_partition_values_schema(snapshot, logical_schema)
    else {
        return Ok(None);
    };
    let mode = snapshot.table_configuration().column_mapping_mode();
    let physical_partition_schema = logical_partition_schema
        .as_ref()
        .make_physical(mode)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())?;
    Ok(Some(physical_partition_schema))
}

fn scan_live_actions_schema(partition_values_parsed_schema: Option<&SchemaRef>) -> SchemaRef {
    let partition_values = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let tags = crate::schema::MapType::new(DataType::STRING, DataType::STRING, true);
    let mut file_constant_fields = vec![
        StructField::nullable("partitionValues", partition_values),
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        StructField::nullable("tags", tags),
        StructField::nullable("clusteringProvider", DataType::STRING),
    ];
    if let Some(schema) = partition_values_parsed_schema {
        file_constant_fields.push(StructField::nullable(
            "partitionValues_parsed",
            schema.as_ref().clone(),
        ));
    }
    Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        StructField::nullable(
            FILE_CONSTANT_VALUES_NAME,
            StructType::new_unchecked(file_constant_fields),
        ),
    ]))
}

fn scan_live_actions_projection(with_partition_values_parsed: bool) -> Vec<Arc<Expression>> {
    let mut file_constant_exprs = vec![
        Expression::column(["add", "partitionValues"]),
        Expression::column(["add", "baseRowId"]),
        Expression::column(["add", "defaultRowCommitVersion"]),
        Expression::column(["add", "tags"]),
        Expression::column(["add", "clusteringProvider"]),
    ];
    if with_partition_values_parsed {
        file_constant_exprs.push(Expression::map_to_struct(Expression::column([
            "add",
            "partitionValues",
        ])));
    }
    vec![
        Arc::new(Expression::column(["add", "path"])),
        Arc::new(Expression::column(["add", "size"])),
        Arc::new(Expression::column(["add", "deletionVector"])),
        Arc::new(Expression::struct_from(file_constant_exprs)),
    ]
}

fn action_schema_with_augmented_add(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> SchemaRef {
    let mut add_fields: Vec<StructField> = Add::to_schema().fields().cloned().collect();
    if let Some(schema) = stats_parsed_schema {
        add_fields.push(StructField::nullable(
            "stats_parsed",
            schema.as_ref().clone(),
        ));
    }
    if let Some(schema) = partition_values_parsed_schema {
        add_fields.push(StructField::nullable(
            "partitionValues_parsed",
            schema.as_ref().clone(),
        ));
    }
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, StructType::new_unchecked(add_fields)),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
    ]))
}

fn scan_actions_with_parsed_projection(
    stats_parsed_schema: Option<&SchemaRef>,
    partition_values_parsed_schema: Option<&SchemaRef>,
) -> Vec<Arc<Expression>> {
    let mut add_exprs = vec![
        Arc::new(Expression::column(["add", "path"])),
        Arc::new(Expression::column(["add", "partitionValues"])),
        Arc::new(Expression::column(["add", "size"])),
        Arc::new(Expression::column(["add", "modificationTime"])),
        Arc::new(Expression::column(["add", "dataChange"])),
        Arc::new(Expression::column(["add", "stats"])),
        Arc::new(Expression::column(["add", "tags"])),
        Arc::new(Expression::column(["add", "deletionVector"])),
        Arc::new(Expression::column(["add", "baseRowId"])),
        Arc::new(Expression::column(["add", "defaultRowCommitVersion"])),
        Arc::new(Expression::column(["add", "clusteringProvider"])),
    ];
    if let Some(schema) = stats_parsed_schema {
        add_exprs.push(Arc::new(Expression::parse_json(
            Expression::column(["add", "stats"]),
            schema.clone(),
        )));
    }
    if partition_values_parsed_schema.is_some() {
        add_exprs.push(Arc::new(Expression::map_to_struct(Expression::column([
            "add",
            "partitionValues",
        ]))));
    }
    vec![
        Arc::new(Expression::struct_from(add_exprs)),
        Arc::new(Expression::column([REMOVE_NAME])),
        Arc::new(Expression::column([PROTOCOL_NAME])),
        Arc::new(Expression::column([METADATA_NAME])),
        Arc::new(Expression::column([DOMAIN_METADATA_NAME])),
        Arc::new(Expression::column([SET_TRANSACTION_NAME])),
    ]
}

fn scan_data_file_schema(
    physical_schema: &SchemaRef,
    logical_schema: &SchemaRef,
) -> Result<SchemaRef, DeltaError> {
    if logical_schema.contains_metadata_column(&MetadataColumnSpec::RowId)
        && !physical_schema.contains_metadata_column(&MetadataColumnSpec::RowIndex)
    {
        let row_index_field = StructField::default_row_index_column().clone();
        if physical_schema.contains(row_index_field.name.as_str()) {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "fsr::scan::scan_data_file_schema",
                detail = format!(
                    "row-id projection requires row-index metadata field `{}` but schema already contains a field with that name",
                    row_index_field.name
                ),
            ));
        }
        return physical_schema
            .as_ref()
            .add([row_index_field])
            .map(Arc::new)
            .map_err(|e| e.into_delta_default());
    }
    Ok(physical_schema.clone())
}

fn scan_data_projection(
    logical_schema: &SchemaRef,
    physical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    column_mapping_mode: crate::table_features::ColumnMappingMode,
) -> Result<Vec<Arc<Expression>>, DeltaError> {
    let row_index_name = StructField::default_row_index_column().name.clone();
    let mut physical_fields = physical_schema.fields();
    logical_schema
        .fields()
        .map(|field| {
            let expr = match field.get_metadata_column_spec() {
                Some(MetadataColumnSpec::RowIndex) => Expression::column([row_index_name.as_str()]),
                Some(MetadataColumnSpec::RowCommitVersion) => {
                    Expression::column([FILE_CONSTANT_VALUES_NAME, "defaultRowCommitVersion"])
                }
                Some(MetadataColumnSpec::RowId) => Expression::binary(
                    BinaryExpressionOp::Plus,
                    Expression::column([FILE_CONSTANT_VALUES_NAME, "baseRowId"]),
                    Expression::column([row_index_name.as_str()]),
                ),
                Some(MetadataColumnSpec::FilePath) => Expression::column(["path"]),
                None if partition_columns.contains(field.name()) => Expression::column([
                    FILE_CONSTANT_VALUES_NAME,
                    "partitionValues_parsed",
                    field.physical_name(column_mapping_mode),
                ]),
                None => {
                    let physical_field = physical_fields.next().ok_or_else(|| {
                        delta_error!(
                            DeltaErrorCode::DeltaCommandInvariantViolation,
                            operation = "fsr::scan_data_projection",
                            detail = format!(
                                "missing physical field for logical field `{}`",
                                field.name()
                            ),
                        )
                    })?;
                    Expression::column([physical_field.name().as_str()])
                }
            };
            Ok(Arc::new(expr))
        })
        .collect()
}

fn sidecar_only_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        SIDECAR_NAME,
        Sidecar::to_schema(),
    )]))
}

fn checkpoint_manifest_scan_schema(include_sidecar: bool) -> SchemaRef {
    let mut fields: Vec<StructField> = action_read_schema().fields().cloned().collect();
    if include_sidecar {
        fields.push(StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()));
    }
    Arc::new(StructType::new_unchecked(fields))
}

/// Tombstone / txn expiration predicate aligned with
/// [`crate::action_reconciliation::log_replay::ActionReconciliationVisitor::is_expired_tombstone`]
/// and txn retention checks in the same visitor (`kernel/src/action_reconciliation/log_replay.rs`).
///
/// `txn_expiration_cutoff` is `None` when `delta.setTransactionRetentionDuration` is unset — txn
/// rows are not filtered by age.
fn retention_filter(
    min_file_retention_timestamp: i64,
    txn_expiration_cutoff: Option<i64>,
) -> Predicate {
    let removal_ts = Expression::coalesce([
        Expression::column(["remove", "deletionTimestamp"]),
        Expression::literal(Scalar::Long(0)),
    ]);
    let remove_ok = Predicate::or(
        Predicate::is_null(Expression::column(["remove"])),
        Predicate::gt(
            removal_ts,
            Expression::literal(Scalar::Long(min_file_retention_timestamp)),
        ),
    );

    let txn_ok = match txn_expiration_cutoff {
        None => Predicate::literal(true),
        Some(cutoff) => Predicate::or_from([
            Predicate::is_null(Expression::column(["txn"])),
            Predicate::is_null(Expression::column(["txn", "lastUpdated"])),
            Predicate::gt(
                Expression::column(["txn", "lastUpdated"]),
                Expression::literal(Scalar::Long(cutoff)),
            ),
        ]),
    };

    Predicate::and(remove_ok, txn_ok)
}

fn action_identity_projection() -> Vec<Arc<Expression>> {
    action_read_schema()
        .fields()
        .map(|f| Arc::new(Expression::column([f.name().as_str()])))
        .collect()
}

fn fsr_dedup_key() -> Expression {
    let null_str = || Expression::literal(Scalar::Null(DataType::STRING));
    let arm = |kind: &str, id1: Expression, id2: Expression, id3: Expression| {
        Expression::array(vec![
            Expression::literal(Scalar::String(kind.into())),
            id1,
            id2,
            id3,
        ])
    };
    let is_not_null = |path: &[&str]| Predicate::is_not_null(Expression::column(path.iter().copied()));
    let coalesce = |path: &[&str]| {
        Expression::coalesce([
            Expression::column(["add"].into_iter().chain(path.iter().copied())),
            Expression::column(["remove"].into_iter().chain(path.iter().copied())),
        ])
    };

    let file_arm = arm(
        "file",
        coalesce(&["path"]),
        coalesce(&["deletionVector", "storageType"]),
        coalesce(&["deletionVector", "pathOrInlineDv"]),
    );
    let proto_arm = arm(PROTOCOL_NAME, null_str(), null_str(), null_str());
    // Metadata is a singleton table state action: latest row wins regardless of prior id.
    let meta_arm = arm("metadata", null_str(), null_str(), null_str());
    // Domain metadata is keyed by domain; newer rows replace older configs for that domain.
    let domain_arm = arm(
        DOMAIN_METADATA_NAME,
        Expression::column(["domainMetadata", "domain"]),
        null_str(),
        null_str(),
    );
    let txn_arm = arm(
        SET_TRANSACTION_NAME,
        Expression::column(["txn", "appId"]),
        null_str(),
        null_str(),
    );

    let null_list = Expression::literal(Scalar::Null(DataType::Array(Box::new(ArrayType::new(
        DataType::STRING,
        true,
    )))));

    Expression::case_when(
        vec![
            (
                Predicate::or(is_not_null(&["add", "path"]), is_not_null(&["remove", "path"])),
                file_arm,
            ),
            (is_not_null(&["protocol"]), proto_arm),
            (is_not_null(&["metaData", "id"]), meta_arm),
            (is_not_null(&["domainMetadata"]), domain_arm),
            (is_not_null(&["txn"]), txn_arm),
        ],
        null_list,
    )
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

    let literal_plan = DeclarativePlanNode::values(path_size_version_schema(), rows)
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
    ensure_commit_raw_schema_for_dedup(&commit_raw_handle.schema)?;
    let dedup_expr = Arc::new(fsr_dedup_key());
    let with_key_and_version_schema = augmented_action_schema_with_version()?;
    let project_with_key_and_version: Vec<Arc<Expression>> = action_identity_projection()
        .into_iter()
        .chain(std::iter::once(Arc::clone(&dedup_expr)))
        .chain(std::iter::once(Arc::new(Expression::column(["version"]))))
        .collect();
    let projected = DeclarativePlanNode::relation_ref(commit_raw_handle.clone())
        .project(project_with_key_and_version, with_key_and_version_schema)
        .filter(Arc::new(
            Predicate::is_not_null(Expression::column([FSR_JOIN_KEY_COL])).into(),
        ));
    let windowed = projected
        .window(
            vec![WindowFunction {
                output_col: "__kernel_rn".into(),
            }],
            vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
            vec![OrderingSpec::desc(ColumnName::new(["version"]))],
        )
        .map_err(|e| e.into_delta_default())?;

    let rn_top_k = Arc::new(
        Predicate::le(
            Expression::column(["__kernel_rn"]),
            Expression::literal(Scalar::Long(FSR_COMMIT_DEDUP_TOP_K)),
        )
        .into(),
    );

    // Final project: drop `version` and `__rn`; keep action_cols + __fsr_join_k.
    let final_proj: Vec<Arc<Expression>> = action_identity_projection()
        .into_iter()
        .chain(std::iter::once(Arc::clone(&dedup_expr)))
        .collect();

    Ok(windowed
        .filter(rn_top_k)
        .project(final_proj, augmented_action_schema()?)
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
    let scan = DeclarativePlanNode::relation_ref(checkpoint_top_handle.clone())
        .filter(Arc::new(
            Predicate::is_not_null(Expression::column([SIDECAR_NAME])).into(),
        ))
        .project(
            vec![
                Arc::new(Expression::column([SIDECAR_NAME, "path"])),
                Arc::new(Expression::column([SIDECAR_NAME, "sizeInBytes"])),
            ],
            path_size_schema(),
        );

    let sidecar_base = log_root.join("_sidecars/").map_err(|e| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            operation = "fsr::build_sidecar_load_plan::join_sidecar_base",
            detail = format!("join _sidecars base URL: {e}"),
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
        return Ok(
            DeclarativePlanNode::relation_ref(commit_dedup_handle.clone())
                .filter(Arc::new(
                    retention_filter(min_file_retention_timestamp, txn_expiration_cutoff).into(),
                ))
                .project(action_identity_projection(), action_read_schema())
                .into_results(),
        );
    }

    let dedup_expr = Arc::new(fsr_dedup_key());
    let augmented_schema = augmented_action_schema()?;

    // Top-level checkpoint rows are preloaded once into `FSR_CHECKPOINT_TOP`; project action
    // columns here so schema aligns with sidecar/action relations.
    let top_scan = DeclarativePlanNode::relation_ref(checkpoint_top_handle.clone())
        .project(action_identity_projection(), action_read_schema());

    let checkpoint_full = match sidecar_handle {
        Some(handle) => DeclarativePlanNode::union_unordered(vec![
            top_scan,
            DeclarativePlanNode::relation_ref(handle.clone()),
        ])
        .map_err(|e| e.into_delta_default())?,
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
    let commit_keys = DeclarativePlanNode::relation_ref(commit_dedup_handle.clone()).project(
        vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
        join_key_only_schema()?,
    );

    let survivors = DeclarativePlanNode::join(
        JoinNode {
            build_keys: vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
            probe_keys: vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
            join_type: JoinType::LeftAnti,
            hint: JoinHint::Hash,
        },
        commit_keys,
        checkpoint_keyed,
    );

    // Union directly on augmented rows (action + join key); drop join key once at the end.
    let everything = DeclarativePlanNode::union_unordered(vec![
        DeclarativePlanNode::relation_ref(commit_dedup_handle.clone()),
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

/// Schema = [`action_read_schema`] plus the dedup-key column [`FSR_JOIN_KEY_COL`]. Used by
/// the [`FSR_COMMIT_DEDUP`] relation handle and by the projection that materializes
/// the dedup key on the checkpoint side of the LeftAnti.
fn augmented_action_schema() -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<_> = action_read_schema().fields().cloned().collect();
    fields.push(StructField::nullable(
        FSR_JOIN_KEY_COL,
        DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
    ));
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())
}

/// Schema = [`augmented_action_schema`] plus the per-commit `version` column. Carried only
/// inside [`build_commit_dedup_plan`] so the row_number window can `ORDER BY version DESC`;
/// the version column is dropped by the plan's final project.
fn augmented_action_schema_with_version() -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<_> = action_read_schema().fields().cloned().collect();
    fields.push(StructField::nullable(
        FSR_JOIN_KEY_COL,
        DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
    ));
    fields.push(StructField::not_null("version", DataType::LONG));
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())
}

fn ensure_commit_raw_schema_for_dedup(schema: &SchemaRef) -> Result<(), DeltaError> {
    for field in [
        "add",
        "remove",
        "protocol",
        "metaData",
        "domainMetadata",
        "txn",
        "version",
    ] {
        if !schema.contains(field) {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "fsr::build_commit_dedup_plan::schema_check",
                detail = format!("commit raw relation schema is missing required field `{field}`"),
            ));
        }
    }
    Ok(())
}

/// Schema = single column `__fsr_join_k: ARRAY<STRING>`. Build-side schema for the LeftAnti hash
/// join in [`build_results_plan`].
fn join_key_only_schema() -> Result<SchemaRef, DeltaError> {
    StructType::try_new([StructField::nullable(
        FSR_JOIN_KEY_COL,
        DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
    )])
    .map(Arc::new)
    .map_err(|e| e.into_delta_default())
}

fn fsr_row_has_identity_predicate() -> Predicate {
    Predicate::or_from([
        Predicate::is_not_null(Expression::column(["add", "path"])),
        Predicate::is_not_null(Expression::column(["remove", "path"])),
        Predicate::is_not_null(Expression::column(["protocol", "minReaderVersion"])),
        Predicate::is_not_null(Expression::column(["metaData", "id"])),
        Predicate::is_not_null(Expression::column(["domainMetadata", "domain"])),
        Predicate::is_not_null(Expression::column(["txn", "appId"])),
    ])
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{AsArray, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::UnaryExpressionOp;
    use crate::plans::ir::nodes::SinkType;
    use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
    use crate::plans::state_machines::framework::state_machine::AdvanceResult;
    use crate::plans::state_machines::framework::state_machine::StateMachine;
    use crate::schema::{ColumnMetadataKey, MetadataColumnSpec, MetadataValue};
    use crate::utils::test_utils::{load_test_table, string_array_to_engine_data};
    use crate::Engine;

    #[derive(Clone, Copy, Debug)]
    struct ReplayCoverageConfig {
        table: &'static str,
        with_predicate: bool,
    }

    fn replay_coverage_configs() -> [ReplayCoverageConfig; 4] {
        [
            ReplayCoverageConfig {
                table: "app-txn-no-checkpoint",
                with_predicate: false,
            },
            ReplayCoverageConfig {
                table: "app-txn-checkpoint",
                with_predicate: false,
            },
            ReplayCoverageConfig {
                table: "v2-checkpoints-parquet-without-sidecars",
                with_predicate: true,
            },
            ReplayCoverageConfig {
                table: "v2-checkpoints-parquet-with-sidecars",
                with_predicate: true,
            },
        ]
    }

    fn build_scan_for_config(
        snapshot: Arc<Snapshot>,
        cfg: ReplayCoverageConfig,
    ) -> Result<Scan, DeltaError> {
        let mut builder = ScanBuilder::new(snapshot).with_stats();
        if cfg.with_predicate {
            builder =
                builder.with_predicate(Arc::new(Predicate::is_not_null(Expression::column(["id"])).into()));
        }
        builder.build_replay()
    }

    #[test]
    fn full_state_builder_matches_direct_fsr_build() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let shape = checkpoint_shape_from_last_checkpoint(&snapshot).unwrap();
        let direct = build_fsr_plans(&snapshot, shape.clone()).unwrap();
        let built = FullState::for_table(Arc::clone(&snapshot))
            .with_stats()
            .with_checkpoint_shape(shape)
            .build()
            .unwrap();
        assert_eq!(direct.len(), built.len());
    }

    #[test]
    fn full_state_builder_with_stats_works_across_checkpoint_configs() {
        for cfg in replay_coverage_configs() {
            let (_engine, snapshot, _tmp) = load_test_table(cfg.table).unwrap();
            let shape = checkpoint_shape_from_last_checkpoint(snapshot.as_ref()).unwrap();
            let direct = build_fsr_plans(snapshot.as_ref(), shape.clone()).unwrap();
            let built = FullState::for_table(Arc::clone(&snapshot))
                .with_stats()
                .with_checkpoint_shape(shape)
                .build()
                .unwrap();
            assert_eq!(
                direct.len(),
                built.len(),
                "FullState builder should match direct build for table {}",
                cfg.table
            );
        }
    }

    #[test]
    fn scan_builder_scan_is_metadata_plus_data() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (metadata, live_actions) = scan.scan_metadata_state_machine().unwrap();
        let data = scan.replay_scan_data_plans(live_actions).unwrap();
        let composed = scan.replay_scan_plans().unwrap();
        assert_eq!(composed.len(), metadata.len() + data.len());
    }

    #[test]
    fn scan_builder_with_stats_supports_metadata_data_and_combined_modes_across_configs() {
        for cfg in replay_coverage_configs() {
            let (_engine, snapshot, _tmp) = load_test_table(cfg.table).unwrap();
            let scan = build_scan_for_config(Arc::clone(&snapshot), cfg).unwrap();
            let (metadata, live_actions) = scan.scan_metadata_state_machine().unwrap();
            assert!(
                !metadata.is_empty(),
                "metadata plans must be non-empty for {}",
                cfg.table
            );
            let data = scan.replay_scan_data_plans(live_actions).unwrap();
            assert_eq!(data.len(), 2, "data phase should remain two plans for {}", cfg.table);
            let combined = scan.replay_scan_plans().unwrap();
            assert_eq!(
                combined.len(),
                metadata.len() + data.len(),
                "combined replay should equal metadata + data for {}",
                cfg.table
            );
        }
    }

    #[test]
    fn scan_replay_metadata_only_sm_returns_live_actions_handle() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-no-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let mut sm = scan.replay_scan_metadata_state_machine().unwrap();
        assert_eq!(sm.phase_name(), "scan.replay.metadata");
        let op = sm.get_operation().unwrap();
        assert!(matches!(op, PhaseOperation::Plans(_)));
        let done = sm.advance(Ok(
            crate::plans::state_machines::framework::phase_state::PhaseState::empty(),
        ));
        match done.unwrap() {
            AdvanceResult::Done(handle) => assert_eq!(handle.name, "scan.live_actions"),
            AdvanceResult::Continue => panic!("metadata-only scan SM should finish in one phase"),
        }
    }

    #[test]
    fn scan_replay_data_only_sm_runs_single_data_phase() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (_, live_actions) = scan.scan_metadata_state_machine().unwrap();
        let mut sm = scan.replay_scan_data_state_machine(live_actions).unwrap();
        assert_eq!(sm.phase_name(), "scan.replay.data");
        let op = sm.get_operation().unwrap();
        match op {
            PhaseOperation::Plans(plans) => assert_eq!(plans.len(), 2),
            _ => panic!("expected data-only scan SM to yield plans"),
        }
        let done = sm.advance(Ok(
            crate::plans::state_machines::framework::phase_state::PhaseState::empty(),
        ));
        assert!(matches!(done.unwrap(), AdvanceResult::Done(())));
    }

    #[test]
    fn replay_scan_state_machine_queries_checkpoint_schema_when_hint_missing() {
        let (_engine, snapshot, _tmp) =
            load_test_table("with_checkpoint_no_last_checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let mut sm = scan.replay_scan_state_machine().unwrap();
        let op = sm.get_operation().unwrap();
        assert!(matches!(op, PhaseOperation::SchemaQuery(_)));
    }

    #[test]
    fn sidecar_discovery_plan_uses_consume_by_kdf_sink() {
        let (_engine, snapshot, _tmp) =
            load_test_table("v2-parquet-sidecars-struct-stats-only").unwrap();
        let shape = checkpoint_shape_from_last_checkpoint(snapshot.as_ref()).unwrap();
        assert!(shape.has_sidecars);
        let (plan, _extract) =
            build_sidecar_discovery_plan(snapshot.as_ref(), shape.file_format).unwrap();
        assert!(matches!(plan.sink.sink_type, SinkType::ConsumeByKdf(_)));
    }

    #[test]
    fn scan_metadata_terminal_materializes_live_actions_relation() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (metadata, _) = scan.scan_metadata_state_machine().unwrap();
        let last = metadata.last().expect("scan metadata plans");
        match &last.sink.sink_type {
            SinkType::Relation(h) => assert_eq!(h.name, "scan.live_actions"),
            other => panic!("expected terminal relation sink, got {other:?}"),
        }
    }

    #[test]
    fn scan_data_emits_load_then_results() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (_, live_actions) = scan.scan_metadata_state_machine().unwrap();
        let data = scan.replay_scan_data_plans(live_actions).unwrap();
        assert_eq!(data.len(), 2);
        assert!(matches!(data[0].sink.sink_type, SinkType::Load(_)));
        assert!(matches!(data[1].sink.sink_type, SinkType::Results(_)));
    }

    #[test]
    fn scan_metadata_with_predicate_materializes_parsed_stats() {
        let (_engine, snapshot, _tmp) = load_test_table("basic_partitioned").unwrap();
        let predicate = Arc::new(
            Predicate::gt(
                Expression::column(["number"]),
                Expression::literal(Scalar::Long(1)),
            )
            .into(),
        );
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_predicate(predicate)
            .with_stats()
            .build_replay()
            .unwrap();
        let (metadata, _) = scan.scan_metadata_state_machine().unwrap();
        let debug = format!("{metadata:#?}");
        assert!(
            debug.contains("ParseJson"),
            "metadata replay should materialize ParseJson before predicate filtering:\n{debug}"
        );
        assert!(
            debug.contains("stats_parsed"),
            "metadata replay should project stats_parsed for predicate filtering:\n{debug}"
        );
    }

    #[test]
    fn scan_metadata_with_stats_supports_classic_checkpoint() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (metadata, _) = scan.scan_metadata_state_machine().unwrap();
        assert!(!metadata.is_empty());
        let terminal = metadata.last().expect("metadata plans should be non-empty");
        assert!(matches!(terminal.sink.sink_type, SinkType::Relation(_)));
    }

    #[test]
    fn scan_metadata_with_stats_supports_v2_checkpoint() {
        let (_engine, snapshot, _tmp) =
            load_test_table("v2-checkpoints-parquet-without-sidecars").unwrap();
        let predicate = Arc::new(Predicate::is_not_null(Expression::column(["id"])).into());
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_predicate(predicate)
            .with_stats()
            .build_replay()
            .unwrap();
        let (metadata, _) = scan.scan_metadata_state_machine().unwrap();
        assert!(!metadata.is_empty());
        let terminal = metadata.last().expect("metadata plans should be non-empty");
        assert!(matches!(terminal.sink.sink_type, SinkType::Relation(_)));
    }

    #[test]
    fn scan_metadata_with_stats_supports_multipart_checkpoint() {
        let (_engine, snapshot, _tmp) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();
        let predicate = Arc::new(Predicate::is_not_null(Expression::column(["id"])).into());
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_predicate(predicate)
            .with_stats()
            .build_replay()
            .unwrap();
        let (metadata, _) = scan.scan_metadata_state_machine().unwrap();
        assert!(!metadata.is_empty());
        let terminal = metadata.last().expect("metadata plans should be non-empty");
        assert!(matches!(terminal.sink.sink_type, SinkType::Relation(_)));
    }

    #[test]
    fn scan_data_load_uses_flat_live_actions_columns() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let scan = ScanBuilder::new(Arc::clone(&snapshot))
            .with_stats()
            .build_replay()
            .unwrap();
        let (_, live_actions) = scan.scan_metadata_state_machine().unwrap();
        let data = scan.replay_scan_data_plans(live_actions).unwrap();
        let load = match &data[0].sink.sink_type {
            SinkType::Load(load) => load,
            other => panic!("expected load sink, got {other:?}"),
        };
        assert_eq!(load.file_meta.path, ColumnName::new(["path"]));
        assert_eq!(load.file_meta.size, Some(ColumnName::new(["size"])));
        assert_eq!(
            load.dv_ref,
            Some(DvRef::skip(ColumnName::new(["deletionVector"])))
        );
        assert_eq!(
            load.passthrough_columns,
            vec![
                ColumnName::new([FILE_CONSTANT_VALUES_NAME]),
                ColumnName::new(["path"])
            ]
        );
    }

    #[test]
    fn scan_data_projection_maps_metadata_columns() {
        let logical = Arc::new(StructType::new_unchecked([
            StructField::not_null("value", DataType::LONG),
            StructField::create_metadata_column(
                "_metadata.row_index",
                MetadataColumnSpec::RowIndex,
            ),
            StructField::create_metadata_column("_metadata.row_id", MetadataColumnSpec::RowId),
            StructField::create_metadata_column(
                "_metadata.row_commit_version",
                MetadataColumnSpec::RowCommitVersion,
            ),
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
        ]));
        let projection = scan_data_projection(
            &logical,
            &logical,
            &HashSet::new(),
            crate::table_features::ColumnMappingMode::None,
        )
        .unwrap();
        assert_eq!(projection.len(), 5);
        assert!(matches!(
            projection[0].as_ref(),
            Expression::Column(c) if c == &ColumnName::new(["value"])
        ));
        assert!(matches!(
            projection[1].as_ref(),
            Expression::Column(c) if c == &ColumnName::new(["_metadata.row_index"])
        ));
        assert!(matches!(
            projection[3].as_ref(),
            Expression::Column(c) if c == &ColumnName::new([FILE_CONSTANT_VALUES_NAME, "defaultRowCommitVersion"])
        ));
        assert!(matches!(
            projection[4].as_ref(),
            Expression::Column(c) if c == &ColumnName::new(["path"])
        ));
    }

    #[test]
    fn scan_data_file_schema_adds_row_index_for_row_id() {
        let logical = Arc::new(StructType::new_unchecked([
            StructField::not_null("value", DataType::LONG),
            StructField::create_metadata_column("_metadata.row_id", MetadataColumnSpec::RowId),
        ]));
        let file_schema = scan_data_file_schema(&logical, &logical).unwrap();
        assert!(file_schema.contains_metadata_column(&MetadataColumnSpec::RowIndex));
    }

    #[test]
    fn scan_data_file_schema_does_not_duplicate_existing_row_index() {
        let logical = Arc::new(StructType::new_unchecked([
            StructField::not_null("value", DataType::LONG),
            StructField::create_metadata_column("_metadata.row_id", MetadataColumnSpec::RowId),
            StructField::create_metadata_column(
                "_metadata.row_index",
                MetadataColumnSpec::RowIndex,
            ),
        ]));
        let file_schema = scan_data_file_schema(&logical, &logical).unwrap();
        assert_eq!(
            file_schema
                .fields()
                .filter(|f| { f.get_metadata_column_spec() == Some(MetadataColumnSpec::RowIndex) })
                .count(),
            1
        );
    }

    #[test]
    fn fsr_dedup_key_eval_add_row_carries_path_and_dv_components() {
        // dv_unique_id is now `ToJson(Array(storageType, pathOrInlineDv))` rather than a
        // Plus-concatenated `unique_id_from_parts` string. Assert the encoded JSON carries the
        // path and DV identity components verbatim (any consumer that reduces over equality of
        // full JSON strings still dedups identical DVs).
        let line = r#"{"add":{"path":"p1.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"u","pathOrInlineDv":"dvpath","offset":7,"sizeInBytes":1,"cardinality":2}}}"#;
        let engine = SyncEngine::new();
        let parsed = engine
            .json_handler()
            .parse_json(
                string_array_to_engine_data(StringArray::from(vec![line])),
                action_read_schema(),
            )
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let out = evaluate_expression(
            &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
            batch,
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
    fn fsr_dedup_key_debug_string_is_non_trivial() {
        let dbg = format!("{:?}", fsr_dedup_key());
        assert!(dbg.len() > 40, "{dbg}");
    }

    #[test]
    fn fsr_dedup_key_eval_various_action_types() {
        let engine = SyncEngine::new();
        let schema = action_read_schema();
        let check = |line: &str, subs: &[&str]| {
            let parsed = engine
                .json_handler()
                .parse_json(
                    string_array_to_engine_data(StringArray::from(vec![line])),
                    Arc::clone(&schema),
                )
                .unwrap();
            let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
            let batch = arrow.record_batch();
            let out = evaluate_expression(
                &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
                batch,
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
    fn fsr_row_has_identity_accepts_protocol_metadata_domain_and_file_actions() {
        let engine = SyncEngine::new();
        let schema = action_read_schema();
        let rows = StringArray::from(vec![
            // file actions
            r#"{"add":{"path":"a.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#,
            r#"{"remove":{"path":"r.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            // protocol / metadata / domain metadata / txn
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            r#"{"domainMetadata":{"domain":"d1","configuration":"cfg","removed":false}}"#,
            r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":100}}"#,
            // no action payload
            r#"{}"#,
        ]);
        let parsed = engine
            .json_handler()
            .parse_json(string_array_to_engine_data(rows), Arc::clone(&schema))
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let out = evaluate_expression(
            &fsr_row_has_identity_predicate().into(),
            batch,
            Some(&DataType::BOOLEAN),
        )
        .unwrap();
        let b = out.as_boolean();
        assert!(b.value(0), "add row should pass identity predicate");
        assert!(b.value(1), "remove row should pass identity predicate");
        assert!(b.value(2), "protocol row should pass identity predicate");
        assert!(b.value(3), "metaData row should pass identity predicate");
        assert!(
            b.value(4),
            "domainMetadata row should pass identity predicate"
        );
        assert!(b.value(5), "txn row should pass identity predicate");
        assert!(!b.value(6), "empty row should fail identity predicate");
    }

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
