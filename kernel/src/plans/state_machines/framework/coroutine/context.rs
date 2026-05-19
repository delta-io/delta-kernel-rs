//! [`Context`] — the body-facing surface of a coroutine state machine.
//!
//! Combines the yield channel ([`PhaseCo`]) with a [`RelationRegistry`] so SM bodies see one
//! object for both dispatching engine operations and managing named relations. Body code reaches
//! the full `RelationRegistry` API (relation_ref, register_relation_sink, drop_relation,
//! live_relations, ...) via `Deref` / `DerefMut`; the dispatch surface is [`Context::execute`].
//!
//! ## Protocol pieces
//!
//! - [`PhaseYield`] / [`PhaseResume`] / [`PhaseCo`] — the typed protocol flowing through the
//!   underlying generator. Each yield carries one [`PhaseOperation`], a static phase name, and a
//!   sorted snapshot of currently-live relation names. Each resume is a `Result<PhaseState,
//!   EngineError>`.
//! - [`Context`] — async surface SM authors call.
//!
//! ## Layering
//!
//! Engines emit [`EngineError`]; per the documented layering contract in
//! [`engine_error`](crate::plans::state_machines::framework::engine_error), the kernel matches on
//! [`EngineErrorKind`](crate::plans::state_machines::framework::engine_error::EngineErrorKind) at
//! SM level and translates to a typed kernel [`DeltaError`](crate::plans::errors::DeltaError) via
//! [`EngineError::into_delta`](crate::plans::state_machines::framework::engine_error::EngineError::into_delta).
//! [`Context::execute`] returns the raw `EngineError`; the body picks the appropriate code.
//!
//! ## 1:1 protocol
//!
//! Each `ctx.execute(op, name)` corresponds to exactly one operation handed to the engine and
//! exactly one resume. SM bodies that need to run multiple plans in a single engine call
//! construct a [`PhaseOperation::Plans`] with multiple plans and extract per-plan outputs from
//! the returned [`PhaseState`] using their respective
//! [`Extractor<O>`](crate::plans::ir::Extractor)s.

use std::ops::{Deref, DerefMut};

use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::{PlanBuilder, RelationRegistry};
use crate::plans::kdf::{ConsumerKdf, KdfOutput};
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
use crate::plans::state_machines::framework::phase_state::PhaseState;

/// Value yielded by a coroutine at each phase boundary. Carries the operation envelope, the phase
/// name, and a snapshot of currently-live relation names taken at yield time.
pub(crate) struct PhaseYield {
    pub operation: PhaseOperation,
    pub phase_name: &'static str,
    pub live_relations: Vec<String>,
}

/// Value the driver passes back to the coroutine on resume. Wraps the engine outcome for the most
/// recent [`PhaseYield::operation`].
pub(crate) struct PhaseResume(pub Result<PhaseState, EngineError>);

impl std::fmt::Debug for PhaseResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Ok(_) => f.write_str("PhaseResume(Ok(..))"),
            Err(e) => write!(f, "PhaseResume(Err({:?}))", e.kind),
        }
    }
}

/// Coroutine handle used inside phase bodies. Alias over [`genawaiter2::sync::Co`].
pub(crate) type PhaseCo = genawaiter2::sync::Co<PhaseYield, PhaseResume>;

/// Async surface SM authors call. Owns a [`RelationRegistry`] alongside the yield channel; lives
/// for the duration of one SM body.
///
/// Relation-management methods (`relation_ref`, `register_relation_sink`, `live_relations`, ...)
/// are reached transparently via `Deref` / `DerefMut`. The dispatch surface is
/// [`Context::execute`].
pub(crate) struct Context<'a> {
    co: &'a mut PhaseCo,
    registry: RelationRegistry,
}

impl<'a> Context<'a> {
    /// Construct a context wrapping the given yield channel and registry.
    pub(crate) fn new(co: &'a mut PhaseCo, registry: RelationRegistry) -> Self {
        Self { co, registry }
    }

    /// Hand `operation` to the engine and await its outcome.
    ///
    /// Snapshots the current set of live relations from the embedded [`RelationRegistry`] and
    /// bundles it with `operation` and `name` into the [`PhaseYield`] payload so that the driver
    /// can expose it via
    /// [`StateMachine::live_relations`](crate::plans::state_machines::framework::state_machine::StateMachine::live_relations).
    ///
    /// Returns the populated [`PhaseState`] on success or the raw [`EngineError`] on failure;
    /// callers match on
    /// [`EngineError::kind`](crate::plans::state_machines::framework::engine_error::EngineError::kind)
    /// and lift via
    /// [`EngineError::into_delta`](crate::plans::state_machines::framework::engine_error::EngineError::into_delta).
    pub(crate) async fn execute(
        &mut self,
        operation: PhaseOperation,
        name: &'static str,
    ) -> Result<PhaseState, EngineError> {
        let live_relations = self.registry.live_relations();
        let PhaseResume(result) = self
            .co
            .yield_(PhaseYield {
                operation,
                phase_name: name,
                live_relations,
            })
            .await;
        result
    }

    /// Yield a [`PhaseOperation::Plans`] consisting of every plan accumulated in the
    /// registry plus a `ConsumeSink` terminating `chain` with the given consumer `state`.
    /// Awaits the engine result and returns the extracted typed output.
    ///
    /// Side effect: the registry's plan accumulator is drained. Registered names persist
    /// (the engine has materialized the corresponding relations after this returns).
    ///
    /// This is the only sanctioned production path for emitting `PhaseOperation::Plans`
    /// from an SM body — it ensures no plan ever leaks to the caller and that the consume
    /// extractor is always paired with the executed plan.
    pub(crate) async fn consume_phase<S>(
        &mut self,
        chain: PlanBuilder,
        state: S,
        phase_name: &'static str,
    ) -> Result<S::Output, DeltaError>
    where
        S: ConsumerKdf + KdfOutput + 'static,
    {
        let (plan, extractor) = chain.consume(state);
        let mut plans = self.registry.take_plans();
        plans.push(plan);
        let result_state = self
            .execute(PhaseOperation::Plans(plans), phase_name)
            .await
            .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
        extractor
            .extract(&result_state)
            .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))
    }
}

impl<'a> Deref for Context<'a> {
    type Target = RelationRegistry;

    fn deref(&self) -> &RelationRegistry {
        &self.registry
    }
}

impl<'a> DerefMut for Context<'a> {
    fn deref_mut(&mut self) -> &mut RelationRegistry {
        &mut self.registry
    }
}
