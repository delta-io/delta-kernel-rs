//! [`Context`] — the body-facing surface of a coroutine state machine.
//!
//! Combines the yield channel ([`StepCo`]) with a [`RelationRegistry`] so SM bodies see one
//! object for both dispatching engine operations and managing named relations. The full
//! `RelationRegistry` API is reached via `Deref` / `DerefMut`; the dispatch surface is
//! [`Context::execute`], which is strictly 1:1 (one operation per call, one resume back).
//!
//! [`Context::execute`] returns the raw [`EngineError`]; per the layering contract in
//! [`engine_error`](crate::plans::operations::framework::engine_error), SM bodies match on
//! [`EngineErrorKind`](crate::plans::operations::framework::engine_error::EngineErrorKind)
//! and translate to a typed kernel [`DeltaError`](crate::plans::errors::DeltaError) via
//! [`EngineError::into_delta`](crate::plans::operations::framework::engine_error::EngineError::into_delta).

use std::ops::{Deref, DerefMut};

use crate::plans::errors::DeltaError;
use crate::plans::ir::{PlanBuilder, RelationRegistry};
use crate::plans::kernel_consumers::{KernelConsumer, KernelConsumerOutput};
use crate::plans::operations::framework::engine_error::EngineError;
use crate::plans::operations::framework::step::Step;
use crate::plans::operations::framework::step_result::StepResult;

/// Value yielded by a coroutine at each phase boundary. Carries the operation envelope, the phase
/// name, and a snapshot of currently-live relation names taken at yield time.
pub(crate) struct StepYield {
    pub operation: Step,
    pub step_name: &'static str,
    pub live_relations: Vec<String>,
}

/// Value the driver passes back to the coroutine on resume. Wraps the engine outcome for the most
/// recent [`StepYield::operation`].
pub(crate) struct StepResume(pub Result<StepResult, EngineError>);

impl std::fmt::Debug for StepResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Ok(_) => f.write_str("StepResume(Ok(..))"),
            Err(e) => write!(f, "StepResume(Err({:?}))", e.kind),
        }
    }
}

/// Coroutine handle used inside phase bodies. Alias over [`genawaiter2::rc::Co`].
///
/// `genawaiter2::rc` (not `sync`) is intentional: the [`Coroutine`](super::driver::Coroutine)
/// driver does not need a `Send` future bound (see the driver module docs for why), and
/// `rc::Co` is `!Send` -- the SM body's future inherits that, which is the correct
/// architectural shape for a CPU-only sequencer.
pub(crate) type StepCo = genawaiter2::rc::Co<StepYield, StepResume>;

/// Async surface SM authors call. Owns a [`RelationRegistry`] alongside the yield channel; lives
/// for the duration of one SM body.
///
/// Relation-management methods (`relation_ref`, `register_relation_sink`, `live_relations`, ...)
/// are reached transparently via `Deref` / `DerefMut`. The dispatch surface is
/// [`Context::execute`].
pub(crate) struct Context<'a> {
    co: &'a mut StepCo,
    registry: RelationRegistry,
}

impl<'a> Context<'a> {
    /// Construct a context wrapping the given yield channel and registry.
    pub(crate) fn new(co: &'a mut StepCo, registry: RelationRegistry) -> Self {
        Self { co, registry }
    }

    /// Hand `operation` to the engine and await its outcome.
    ///
    /// Snapshots the current set of live relations from the embedded [`RelationRegistry`] and
    /// bundles it with `operation` and `name` into the [`StepYield`] payload so that the driver
    /// can expose it via
    /// [`StateMachine::live_relations`](crate::plans::operations::framework::state_machine::StateMachine::live_relations).
    ///
    /// Returns the populated [`StepResult`] on success or the raw [`EngineError`] on failure;
    /// callers match on
    /// [`EngineError::kind`](crate::plans::operations::framework::engine_error::EngineError::kind)
    /// and lift via
    /// [`EngineError::into_delta`](crate::plans::operations::framework::engine_error::EngineError::into_delta).
    pub(crate) async fn execute(
        &mut self,
        operation: Step,
        name: &'static str,
    ) -> Result<StepResult, EngineError> {
        let live_relations = self.registry.live_relations();
        let StepResume(result) = self
            .co
            .yield_(StepYield {
                operation,
                step_name: name,
                live_relations,
            })
            .await;
        result
    }

    /// Yield a [`Step::Plans`] consisting of every plan accumulated in the
    /// registry plus a `ConsumeSink` terminating `chain` with the given consumer `state`.
    /// Awaits the engine result and returns the extracted typed output.
    ///
    /// Side effect: the registry's plan accumulator is drained. Registered names persist
    /// (the engine has materialized the corresponding relations after this returns).
    ///
    /// This is the only sanctioned production path for emitting `Step::Plans`
    /// from an SM body — it ensures no plan ever leaks to the caller and that the consume
    /// extractor is always paired with the executed plan.
    pub(crate) async fn consume_phase<S>(
        &mut self,
        chain: PlanBuilder,
        state: S,
        step_name: &'static str,
    ) -> Result<S::Output, DeltaError>
    where
        S: KernelConsumer + KernelConsumerOutput + 'static,
    {
        let (plan, extractor) = chain.consume(state);
        let mut plans = self.registry.take_plans();
        plans.push(plan);
        let result_state = self
            .execute(Step::Plans(plans), step_name)
            .await
            .map_err(|e| e.into_delta_typed())?;
        extractor
            .extract(&result_state)
            .map_err(|e| e.into_delta_typed())
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
