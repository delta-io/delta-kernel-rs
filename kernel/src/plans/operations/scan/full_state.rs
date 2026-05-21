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
// Re-export for callers that need a stable type for the FSR commit-file row.
pub use super::plans::CommitFileMeta;
use super::plans::{execute_reconciliation, RECONCILED};
use crate::plans::errors::{DeltaError, KernelErrAsDelta};
use crate::plans::ir::{RelationRegistry, ResultPlan};
use crate::plans::operations::framework::coroutine::context::Context;
use crate::plans::operations::framework::coroutine::driver::Coroutine;
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
    pub fn state_machine(&self) -> Result<Coroutine<ResultPlan>, DeltaError> {
        let snapshot = self.snapshot.clone();
        let state_info = self.state_info.clone();
        Coroutine::new("fsr", move |mut co, sm_id| async move {
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
