//! Canonical Full Snapshot Read (FSR) declarative plans — *window-on-commits +
//! anti-join-on-checkpoint*.
//!
//! Mirrors Delta log-replay semantics by composing three or four declarative plans:
//!
//! 1. **commit_load** materializes the raw per-commit action stream into `fsr.commit_raw`, covering
//!    `ascending_commit_files ∪ ascending_compaction_files` so downstream steps can `ORDER BY
//!    version DESC` to recover "newest action wins" semantics.
//! 2. **commit_dedup** runs a `row_number PARTITION BY key ORDER BY version DESC` window over the
//!    commit tail to pick the winners, materialized into `fsr.commit_dedup`.
//! 3. **sidecar_load** (only when `has_sidecars`) materializes V2-multipart sidecar parquet actions
//!    into `fsr.sidecar_actions`.
//! 4. **results** anti-joins the (top-level checkpoint ∪ sidecar_actions) stream against
//!    `commit_dedup`, unions in the winners, applies retention, and binds the final reconciled
//!    action stream to `fsr.results`.
//!
//! The window applies only to the (small) commit tail; the (large) checkpoint stream
//! goes through a single hash anti-join, avoiding per-key orderings over the full snapshot.
//!
//! `dv_unique_id` is computed as `If(storageType IS NULL, NULL,
//! ToJson(Array(storageType, pathOrInlineDv)))` — equality-equivalent (not byte-equivalent)
//! to [`crate::actions::deletion_vector::DeletionVectorDescriptor::unique_id_from_parts`].
//! `offset` is omitted because kernel lacks an int-to-string cast; inline DVs sharing
//! `pathOrInlineDv` would already imply identical byte payloads.

use std::sync::Arc;

use super::action_pair::FSR_BASE;
use super::dedup::fsr_dedup_key;
use super::ssa_reconciliation::execute_reconciliation_ssa;
// Re-export for callers that need a stable type for the FSR commit-file row.
pub use super::ssa_reconciliation::CommitFileMeta;
use crate::plans::errors::{DeltaError, KernelErrAsDelta};
use crate::plans::ir::plan::ResultPlan;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::plan_context::Context as SsaContext;
use crate::scan::state_info::StateInfo;
use crate::scan::StatsOutputMode;
use crate::snapshot::Snapshot;

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

    /// CoroutineSM SM driving the FSR pipeline end-to-end.
    ///
    /// Builds against the [`crate::plans::state_machines::framework::plan_context::Context`]
    /// (SSA / PlanBuilder API) and yields a single
    /// [`ResultPlan`](crate::plans::ir::plan::ResultPlan) containing the entire
    /// reconciliation as one flat SSA program. Engines drive this through
    /// `drive_ssa_to_dataframe`.
    pub fn state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let snapshot = self.snapshot.clone();
        let state_info = self.state_info.clone();
        CoroutineSM::new("fsr_ssa", move |mut engine, _sm_id| async move {
            let ctx = SsaContext::new();
            let stats = state_info
                .as_ref()
                .and_then(|si| si.physical_stats_schema.clone());
            let reconciled = execute_reconciliation_ssa(
                &ctx,
                &mut engine,
                snapshot.as_ref(),
                &FSR_BASE,
                stats,
                /* parts= */ None,
                Arc::new(fsr_dedup_key()),
            )
            .await?;
            ctx.into_result_plan(reconciled)
        })
    }
}

impl FullStateBuilder {
    /// Surface `add.stats_parsed` in FSR output. `build()` then derives the
    /// `physical_stats_schema` driving native-stats detection and projection.
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
