//! Engine-side success payload for one phase yield.
//!
//! Each [`Step`](super::step::Step) yielded by an SM produces at most one payload of one of
//! two shapes:
//!
//! - [`Step::Consume`](super::step::Step::Consume) -> drain produces one [`FinishedHandle`].
//! - [`Step::SchemaQuery`](super::step::Step::SchemaQuery) -> footer read produces one
//!   [`SchemaRef`].
//!
//! [`StepPayload`] carries the matching variant directly; the SM body destructures it on
//! resume. The previous keyed `StepResult` accumulator (HashMap + Mutex + first-wins error
//! slot) is gone -- nothing in the protocol can produce more than one payload per yield,
//! so the indirection bought nothing.

use crate::plans::kernel_consumers::FinishedHandle;
use crate::schema::SchemaRef;

/// Engine -> SM success payload for a single phase yield.
///
/// Travels through [`StateMachine::submit`](super::state_machine::StateMachine::submit) on
/// the success arm. The SM body destructures the variant matching its preceding yield;
/// any other variant is an executor bug and surfaces as an internal error in
/// [`Context`](super::plan_context::Context) dispatch helpers.
#[derive(Debug)]
pub enum StepPayload {
    /// A [`Step::Consume`](super::step::Step::Consume) finished and is handing back its
    /// drained consumer state.
    Consumer(FinishedHandle),
    /// A [`Step::SchemaQuery`](super::step::Step::SchemaQuery) finished and is handing
    /// back the resolved schema.
    Schema(SchemaRef),
    /// Driver-internal sentinel for the genawaiter trampoline's first `resume_with`
    /// (the one that runs the producer up to its first await). SM bodies never observe
    /// this variant: a yield always precedes any awaited resume, and zero-yield SMs
    /// terminate before inspecting the priming value.
    Empty,
}
