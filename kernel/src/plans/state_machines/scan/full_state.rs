//! Canonical Full Snapshot Read (FSR) declarative plans.
//!
//! [`FullState::state_machine`] yields a single [`ResultPlan`] that runs the shared
//! reconciliation pipeline (window-on-commits + anti-join-on-checkpoint) against the
//! snapshot's log segment. Optional stats projection is configured via
//! [`FullStateBuilder::with_stats`] before [`FullStateBuilder::build`].

use std::sync::Arc;

use super::reconciliation::{execute_reconciliation, fsr_dedup_key, FSR_BASE};
use crate::plans::errors::{DeltaError, KernelErrAsDelta};
use crate::plans::ir::plan::ResultPlan;
use crate::plans::state_machines::framework::coroutine::CoroutineSM;
use crate::plans::state_machines::framework::plan_context::Context;
use crate::scan::state_info::StateInfo;
use crate::scan::StatsOutputMode;
use crate::snapshot::Snapshot;

/// Configured FSR plan source. Construct via [`FullState::for_table`] (or
/// [`Snapshot::full_state_builder`]), call [`FullStateBuilder::build`], then drive
/// [`Self::state_machine`] through an engine.
///
/// The snapshot is the sole source of truth for the table's log segment and
/// `_last_checkpoint` hint; the resolver derives shape from those with no override hook.
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

    /// CoroutineSM driving the FSR pipeline end-to-end.
    ///
    /// Builds against [`Context`] and yields a single [`ResultPlan`] containing the full
    /// reconciliation as one flat plan. Engines drive this through `drive_to_dataframe`.
    pub fn state_machine(&self) -> Result<CoroutineSM<ResultPlan>, DeltaError> {
        let snapshot = self.snapshot.clone();
        let state_info = self.state_info.clone();
        CoroutineSM::new("fsr", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let stats = state_info
                .as_ref()
                .and_then(|si| si.physical_stats_schema.clone());
            let reconciled = execute_reconciliation(
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
