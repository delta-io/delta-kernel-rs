//! The [`StateMachine`] trait kernel SMs implement and engine-side executors drive, along
//! with the [`EngineRequest`] / [`EngineResponse`] protocol they exchange each tick.
//!
//! Engines drive every SM through the same three methods: ask for the next step's work,
//! execute it, hand back the outcome. The SM decides whether to continue to another step
//! or terminate with a typed result.
//!
//! ```text
//! loop {
//!     let op = sm.get_step()?;
//!     let result = executor.execute(op);
//!     match sm.submit(result)? {
//!         NextStep::Continue => continue,
//!         NextStep::Done(r) => return Ok(r),
//!     }
//! }
//! ```
//!
//! The [`EngineRequest`] / [`EngineResponse`] pair is the *protocol*: each request variant
//! maps to exactly one response variant on resume:
//!
//! - [`EngineRequest::Reduce`] -> [`EngineResponse::Reducer`]
//! - [`EngineRequest::SchemaQuery`] -> [`EngineResponse::Schema`]
//!
//! [`EngineResponse::Empty`] is a driver-internal priming sentinel SM bodies never observe.

use super::engine_error::EngineError;
use crate::plans::errors::DeltaError;
#[cfg(doc)]
use crate::plans::errors::DeltaErrorCode;
use crate::plans::ir::nodes::ReduceSink;
use crate::plans::ir::plan::{PlanNode, RefId};
use crate::plans::kernel_reducers::FinishedHandle;
use crate::schema::SchemaRef;

// ============================================================================
// Engine request: kernel -> engine, the "do this work" envelope
// ============================================================================

/// A metadata-only read: ask the engine to open a parquet file, read its schema from the
/// footer, and deliver it back as [`EngineResponse::Schema`].
///
/// Distinct from a data-carrying [`EngineRequest::Reduce`]: no row stream, no sink, no
/// KDF-producing pipeline -- the executor just does a footer read.
#[derive(Debug, Clone)]
pub struct SchemaQuery {
    /// Path to the parquet file whose schema the kernel wants.
    pub file_path: String,
}

impl SchemaQuery {
    /// Construct a schema query for `file_path`.
    pub fn new(file_path: impl Into<String>) -> Self {
        Self {
            file_path: file_path.into(),
        }
    }
}

/// What [`StateMachine::get_step`] hands to the executor.
///
/// Separates the concerns the executor understands:
///
/// - [`SchemaQuery`](Self::SchemaQuery) -- metadata-only footer read.
/// - [`Reduce`](Self::Reduce) -- plan dataflow drained into a [`ReduceSink`]. The engine compiles
///   `stmts` (a flat plan), runs the DAG, and feeds the rows produced at `terminal` into `sink`.
///   The reducer's typed output flows back as [`EngineResponse::Reducer`] carrying the
///   `FinishedHandle`, and the SM body recovers the typed value via the paired `Extractor`.
#[derive(Debug, Clone)]
pub enum EngineRequest {
    /// Read a file's schema without reading data.
    SchemaQuery(SchemaQuery),
    /// Plan dataflow + reducer drain. The engine evaluates `stmts` as a DAG and pipes the
    /// stream produced at `terminal` into `sink`.
    Reduce {
        stmts: Vec<PlanNode>,
        terminal: RefId,
        sink: ReduceSink,
    },
}

// ============================================================================
// Engine response: engine -> kernel, the success payload returned on resume
// ============================================================================

/// Engine -> SM success payload for a single phase yield.
///
/// Travels through [`StateMachine::submit`] on the success arm. Variant mismatches
/// (executor returned the wrong shape for the preceding yield) surface as internal errors
/// in the `Context` dispatch helpers (added under `plans::state_machines::framework`).
#[derive(Debug)]
pub enum EngineResponse {
    /// A [`EngineRequest::Reduce`] finished and is handing back its drained reducer state.
    Reducer(FinishedHandle),
    /// A [`EngineRequest::SchemaQuery`] finished and is handing back the resolved schema.
    Schema(SchemaRef),
    /// Driver-internal sentinel for the genawaiter trampoline's first `resume_with` (the
    /// one that runs the producer up to its first await). SM bodies never observe this
    /// variant: a yield always precedes any awaited resume, and zero-yield SMs terminate
    /// before inspecting the priming value.
    Empty,
}

// ============================================================================
// StateMachine trait: the contract the executor drives
// ============================================================================

/// Result of submitting a step result to a state machine.
#[derive(Debug)]
pub enum NextStep<R> {
    /// SM advanced to the next step; the driver should loop back to
    /// [`StateMachine::get_step`].
    Continue,
    /// SM completed with the given result. The driver must stop.
    Done(R),
}

/// The contract kernel state machines implement and engine-side executors drive.
///
/// Each SM-visible "step" is one tick of this loop: the executor asks
/// [`StateMachine::get_step`] for what to run, executes it, then calls
/// [`StateMachine::submit`] with the outcome ([`EngineResponse`] on success, an
/// [`EngineError`] on engine failure).
///
/// # Error layering
///
/// Both `get_step` and `submit` return [`DeltaError`] -- the typed,
/// template-parameterized kernel error surface. Engine failures arrive via the
/// `Err(EngineError)` arm of `submit`'s input; the SM chooses whether to lift them into
/// its own terminal result or propagate them as a [`DeltaError`].
pub trait StateMachine {
    /// What the SM returns when it finishes.
    type Result;

    /// Return the next step's work.
    ///
    /// Takes `&mut self` so the SM can lazily construct plans and move internal state.
    ///
    /// Returns `Err(DeltaError)` for unrecoverable kernel bugs hit while *building* the
    /// next step (plan construction can fail on e.g. a malformed schema projection). These
    /// are typically tagged [`DeltaErrorCode::DeltaCommandInvariantViolation`].
    fn get_step(&mut self) -> Result<EngineRequest, DeltaError>;

    /// Receive the step outcome from the driver.
    ///
    /// - `Ok(EngineResponse)` -- the executor ran the step and produced its single typed payload (a
    ///   finished reducer handle, a schema, or [`EngineResponse::Empty`] for the driver-internal
    ///   priming case). The SM body destructures the variant matching its preceding yield; any
    ///   other variant is an executor bug surfaced as an internal error.
    /// - `Err(EngineError)` -- a typed engine-side failure; the SM matches on [`EngineError::kind`]
    ///   and decides how to surface it.
    fn submit(
        &mut self,
        result: Result<EngineResponse, EngineError>,
    ) -> Result<NextStep<Self::Result>, DeltaError>;

    /// Static label for logging / diagnostics. Drivers use this in span names and error
    /// contexts; implementations return the currently-active step name, or `"done"`
    /// once the SM has finished.
    fn step_name(&self) -> &'static str;

    /// Sorted snapshot of logical relation names currently registered with the SM at the
    /// boundary of the most recent yield.
    ///
    /// Surfaces diagnostics / span context, parallel scheduling info, and cross-phase
    /// relation tracking. Returns the empty vector for SMs that do not surface relations,
    /// and for terminal states (after the SM completes).
    fn live_relations(&self) -> Vec<String> {
        Vec::new()
    }
}
