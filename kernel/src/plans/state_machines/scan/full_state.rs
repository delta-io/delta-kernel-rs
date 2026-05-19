//! Canonical Full Snapshot Read (FSR) declarative plans — *window-on-commits +
//! anti-join-on-checkpoint*.
//!
//! Mirrors Delta log-replay semantics (`kernel/src/action_reconciliation/log_replay.rs`,
//! `kernel/src/log_replay/deduplicator.rs`) by composing three or four declarative plans
//! that flow as one [`PhaseOperation::Plans`] step:
//!
//! 1. **commit_load** — `Values(commit metadata) → LoadSink(JSON, action schema,
//!    passthrough=[version])` materializes the raw per-commit action stream into `fsr.commit_raw`.
//!    The kernel cover over `ascending_commit_files ∪ ascending_compaction_files` is materialized
//!    verbatim so that downstream steps can `ORDER BY version DESC` to recover Delta's "newest
//!    action wins" semantics inside the commit tail.
//! 2. **commit_dedup** — `RelationRef(commit_raw) → Filter(has identity) → Project(action_cols +
//!    key) → Window(row_number PARTITION BY key ORDER BY version DESC) → Filter(__rn <= k) →
//!    Project(action_cols + key)` yields the *commit winners*, materialized into
//!    `fsr.commit_dedup`. Commits supersede (and remove-tombstone) checkpoint state for any
//!    `(action_kind, identity)` pair they touch.
//! 3. **(only when `has_sidecars`) sidecar_load** — `Scan(top-level checkpoint, sidecar-only
//!    schema) → Filter(sidecar IS NOT NULL) → Project(sidecar.path, sidecar.sizeInBytes) →
//!    LoadSink(Parquet, action schema)` materializes each V2-multipart sidecar parquet's action
//!    rows into `fsr.sidecar_actions`. V1 / V2-inline checkpoints carry no sidecars; this plan is
//!    omitted entirely so the executor never opens them.
//! 4. **results** — `(Scan(top-level checkpoint) [∪ RelationRef(sidecar_actions)]) → Filter(has
//!    identity) → Project(action_cols + key) → LeftAntiJoin(probe=this,
//!    build=RelationRef(commit_dedup).project(key))` materializes the *checkpoint survivors* (rows
//!    the commit tail didn't touch). The plan completes with `Union(RelationRef(commit_dedup),
//!    survivors) -> Filter(retention) -> Project(action_schema) -> into_relation("results")` so the
//!    caller reads the reconstructed action stream from `fsr.results`.
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
//! - **`action_schema`**: Full reconstructed action stream (add / remove / protocol / metaData /
//!   domainMetadata / txn).
//! - **Retention thresholds**: Derived like checkpoint reconciliation via
//!   [`crate::action_reconciliation::deleted_file_retention_timestamp_with_time`] and
//!   [`crate::action_reconciliation::calculate_transaction_expiration_timestamp`] against
//!   [`crate::snapshot::Snapshot::table_properties`] (`kernel/src/table_properties/mod.rs`).
//! - **`CheckpointShape.file_format`**: Taken from the first checkpoint part's filename extension
//!   in the snapshot listing (`crate::path::ParsedLogPath::extension`), with `_last_checkpoint`
//!   schema falling back through [`crate::log_segment::LogSegment::checkpoint_schema`].

use std::sync::Arc;

use super::action_pair::FSR_BASE;
use super::dedup::fsr_dedup_key;
// Re-export for callers that need a stable type for the FSR commit-file row.
pub use super::plans::CommitFileMeta;
use super::plans::{execute_reconciliation, RECONCILED};
use crate::plans::errors::{DeltaError, KernelErrAsDelta};
use crate::plans::ir::{RelationRegistry, ResultPlan};
use crate::plans::state_machines::framework::coroutine::context::Context;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::scan::state_info::StateInfo;
use crate::scan::StatsOutputMode;
use crate::snapshot::Snapshot;

/// FSR terminal name. Combined with the `"fsr"` SM prefix this resolves to the full handle
/// name `fsr.results` — the relation external callers read after driving
/// [`FullState::state_machine`] to completion.
const FSR_RESULTS: &str = "results";

/// Configured FSR plan source. Construct via [`FullState::for_table`] (or
/// [`Snapshot::full_state_builder`](crate::snapshot::Snapshot::full_state_builder)), call
/// [`FullStateBuilder::build`], then drive [`Self::state_machine`] through an engine. The
/// snapshot is the sole source of truth for the table's log segment and `_last_checkpoint`
/// hint; the resolver derives shape from those, no override hook.
#[derive(Debug, Clone)]
pub struct FullState {
    snapshot: Arc<Snapshot>,
    /// `Some` iff [`FullStateBuilder::with_stats`] was called. Derived at `build()` time.
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

    /// Coroutine SM driving the FSR pipeline end-to-end: resolve shape + build the shared
    /// reconciliation pipeline over the full six-slot action set, then rebind `RECONCILED`
    /// under `fsr.results` as the SM's terminal.
    pub fn state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let snapshot = self.snapshot.clone();
        let state_info = self.state_info.clone();
        CoroutineSM::new("fsr", move |mut co, sm_id| async move {
            let mut ctx = Context::new(&mut co, RelationRegistry::new(sm_id, "fsr"));
            let stats = state_info
                .as_ref()
                .and_then(|si| si.physical_stats_schema.clone());
            execute_reconciliation(
                &mut ctx,
                snapshot.as_ref(),
                &FSR_BASE,
                stats,
                /* parts= */ None,
                Arc::new(fsr_dedup_key()),
            )
            .await?;
            // The reconciled relation IS the FSR result; project (identity-rebind by name)
            // to FSR_RESULTS so the terminal handle's logical full-name reads `fsr.results`.
            ctx.relation_ref(RECONCILED)?
                .into_result_plan(FSR_RESULTS, &mut ctx)
        })
    }
}

impl FullStateBuilder {
    /// Surface `add.stats_parsed` in FSR output. `build()` then derives the
    /// `physical_stats_schema` (used by the resolver to detect native
    /// `add.stats_parsed` and by plan builders to project it).
    pub fn with_stats(mut self) -> Self {
        self.with_stats = true;
        self
    }

    /// Finalize into a [`FullState`]. When [`Self::with_stats`] was called, constructs a
    /// [`StateInfo`] from the snapshot's logical schema with no predicate and
    /// [`StatsOutputMode::AllColumns`]; otherwise no `StateInfo` is built.
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
