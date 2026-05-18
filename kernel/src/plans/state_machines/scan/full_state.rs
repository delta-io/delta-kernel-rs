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
//!    survivors) -> Filter(retention) -> Project(action_schema) -> into_relation(FSR_RESULTS)`
//!    so the caller reads the reconstructed action stream from [`FSR_RESULTS`].
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
//! - **`action_schema`**: Full reconstructed action stream (add / remove / protocol / metaData
//!   / domainMetadata / txn).
//! - **Retention thresholds**: Derived like checkpoint reconciliation via
//!   [`crate::action_reconciliation::deleted_file_retention_timestamp_with_time`] and
//!   [`crate::action_reconciliation::calculate_transaction_expiration_timestamp`] against
//!   [`crate::snapshot::Snapshot::table_properties`] (`kernel/src/table_properties/mod.rs`).
//! - **`CheckpointShape.file_format`**: Taken from the first checkpoint part's filename extension
//!   in the snapshot listing (`crate::path::ParsedLogPath::extension`), with `_last_checkpoint`
//!   schema falling back through [`crate::log_segment::LogSegment::checkpoint_schema`].

use std::sync::Arc;

use super::checkpoint_shape::checkpoint_shape_from_last_checkpoint;
use super::plans::build_fsr_plans;
// Re-exports for the FSR relation handle name constants. External code references these via
// `fsr::full_state::FSR_COMMIT_DEDUP`; the actual definitions live in `super::plans`.
pub use super::plans::{
    CommitFileMeta, FSR_CHECKPOINT_TOP, FSR_COMMIT_DEDUP, FSR_COMMIT_RAW, FSR_RESULTS,
    FSR_SIDECAR_ACTIONS,
};
use crate::plans::errors::{DeltaError, KernelErrAsDelta};
use crate::plans::ir::{RelationRegistry, ResultPlan};
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::scan::state_info::StateInfo;
use crate::scan::StatsOutputMode;
use crate::snapshot::Snapshot;

/// Configured FSR plan source. Construct via [`FullState::for_table`] (or
/// [`Snapshot::full_state_builder`](crate::snapshot::Snapshot::full_state_builder)), then
/// [`FullStateBuilder::build`] to validate the configuration, then [`Self::state_machine`]
/// to drive plans through an engine.
///
/// The snapshot is the single source of truth for the table's log segment and
/// `_last_checkpoint` hint; there is no escape hatch for overriding the resolved
/// [`CheckpointShape`].
#[derive(Debug, Clone)]
pub struct FullState {
    snapshot: Arc<Snapshot>,
    /// `Some` iff the caller asked for parsed stats via
    /// [`FullStateBuilder::with_stats`]. Derived once at `build()` time so the SM body
    /// stays cheap.
    state_info: Option<Arc<StateInfo>>,
}

/// Builder for canonical Full State Reconstruction plans.
#[derive(Debug, Clone)]
pub struct FullStateBuilder {
    snapshot: Arc<Snapshot>,
    with_stats: bool,
}

impl FullState {
    /// Start building canonical FSR plans for `snapshot`.
    pub fn for_table(snapshot: Arc<Snapshot>) -> FullStateBuilder {
        FullStateBuilder {
            snapshot,
            with_stats: false,
        }
    }

    /// Wrap FSR plan composition in a [`CoroutineSM`].
    ///
    /// The returned SM resolves the [`CheckpointShape`] from the snapshot's
    /// `_last_checkpoint` hint and composes the FSR plans. Future commits add
    /// SchemaQuery yields when the hint is insufficient and a `Plans` yield to
    /// publish the V2 multipart manifest as a reusable relation.
    pub fn state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let snapshot = self.snapshot.clone();
        // TODO(parsed-stats-resolver): swap `checkpoint_shape_from_last_checkpoint` for the
        // SM-driven `resolve_checkpoint_shape(ctx, snapshot, state_info.physical_stats_schema)`
        // once that lands, threading `state_info` through here so V2 manifests are published
        // and `has_stats_parsed` is populated.
        let _state_info = self.state_info.clone();
        CoroutineSM::new("full_state", move |_co, sm_id| async move {
            let mut registry = RelationRegistry::new(sm_id);
            let shape = checkpoint_shape_from_last_checkpoint(snapshot.as_ref())?;
            build_fsr_plans(snapshot.as_ref(), shape, &mut registry)
        })
    }
}

impl FullStateBuilder {
    /// Request that FSR plans surface parsed `add.stats_parsed` rows.
    ///
    /// Sets `with_stats = true`; `build()` then constructs the [`StateInfo`]
    /// whose `physical_stats_schema` is used by the SM resolver to detect
    /// native `add.stats_parsed` columns and by plan builders to project them.
    pub fn with_stats(mut self) -> Self {
        self.with_stats = true;
        self
    }

    /// Finalize the builder into a [`FullState`] value.
    ///
    /// When `with_stats` is set, derives the `physical_stats_schema` for the
    /// snapshot's full data schema by constructing a
    /// [`StateInfo`](crate::scan::state_info::StateInfo) with no predicate and
    /// [`StatsOutputMode::AllColumns`]. Without `with_stats`, no `StateInfo`
    /// is constructed and plan composition has no stats wiring.
    pub fn build(self) -> Result<FullState, DeltaError> {
        let state_info = if self.with_stats {
            let logical_schema = self.snapshot.schema();
            let table_configuration = self.snapshot.table_configuration();
            let si = StateInfo::try_new(
                logical_schema,
                table_configuration,
                None,
                StatsOutputMode::AllColumns,
                (),
            )
            .map_err(|e| e.into_delta_default())?;
            Some(Arc::new(si))
        } else {
            None
        };
        Ok(FullState {
            snapshot: self.snapshot,
            state_info,
        })
    }
}
