//! CoroutineSM-backed [`StateMachine`] implementation.
//!
//! Holds the internal yield/resume protocol shared between SM bodies and the
//! [`CoroutineSM`] driver:
//!
//! - `StepYield` / `StepResume` (both `pub(crate)`) -- the value pair shuttled through the
//!   genawaiter trampoline at each phase boundary.
//! - `Engine` (`pub(crate)`) -- the SM-internal yield-channel handle, aliased over
//!   [`genawaiter2::rc::Co`]. SM bodies emit yields through the `Context` `reduce` / `schema_query`
//!   plan-construction helpers (sibling submodule); this module never exposes raw yields.
//! - [`CoroutineSM<R>`] -- the driver that compiles `StepYield` sequences into the [`StateMachine`]
//!   contract.
//!
//! # `!Send` design
//!
//! The driver uses [`genawaiter2::rc`] (not `sync`). State machines are CPU-only
//! sequencers that never perform I/O or `await` real futures -- every
//! `engine.yield_(step).await` is just the genawaiter2 trampoline, polled synchronously by
//! the driver. There is no concurrent access to an SM, so the `Send` bound from
//! `sync::GenBoxed` is unnecessary load on the API. Callers that need to drive an SM from
//! a multi-threaded async runtime use `block_on` (which does not require `Send`) or
//! `LocalSet` / `spawn_local`.
//!
//! # No-panic policy
//!
//! Built on [`genawaiter2`], which panics on coroutine protocol misuse (awaiting a
//! non-yield future, retaining a `Co` after return). The no-panic rule is preserved
//! because the only path to `Co::yield_` is through the `Context` plan-construction
//! dispatch helpers; SM bodies are `pub(crate)`-only and FFI sees them through
//! `Box<dyn StateMachine>`, so the panic paths are unreachable externally.
//!
//! # Naming note
//!
//! The `Engine` alias below shares its name with the kernel's connector-facing `Engine`
//! trait. They are unrelated -- this `Engine` is the SM-internal yield channel, lives
//! behind `pub(crate)`, and is never exposed in the public API. If a single scope ever
//! needs both names, alias one at the use site (e.g. `use crate::Engine as EngineTrait;`).

use std::future::Future;
use std::pin::Pin;
use std::{fmt, mem};

use genawaiter2::rc::{Co, Gen};
use genawaiter2::GeneratorState;
use uuid::Uuid;

use super::engine_error::EngineError;
use super::state_machine::{EngineRequest, EngineResponse, NextStep, StateMachine};
use crate::plans::errors::DeltaError;

// ============================================================================
// Yield/resume protocol
// ============================================================================

/// Value yielded by a coroutine at each phase boundary. Carries the operation envelope and
/// the phase name.
pub(crate) struct StepYield {
    pub operation: EngineRequest,
    pub step_name: &'static str,
}

/// Value the driver passes back to the coroutine on resume. Wraps the engine outcome for
/// the most recent [`StepYield::operation`].
pub(crate) struct StepResume(pub Result<EngineResponse, EngineError>);

impl fmt::Debug for StepResume {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Ok(_) => f.write_str("StepResume(Ok(..))"),
            Err(e) => write!(f, "StepResume(Err({:?}))", e.kind),
        }
    }
}

/// CoroutineSM handle used inside phase bodies. Alias over [`genawaiter2::rc::Co`].
///
/// `genawaiter2::rc` (not `sync`) is intentional: the [`CoroutineSM`] driver does not need
/// a `Send` future bound (see the module docs for why), and `rc::Co` is `!Send` -- the SM
/// body's future inherits that, which is the correct architectural shape for a CPU-only
/// sequencer.
#[allow(dead_code)] // primary consumer is Context::dispatch (sibling state-machine framework module)
pub(crate) type Engine = Co<StepYield, StepResume>;

// ============================================================================
// CoroutineSM driver
// ============================================================================

type InnerGen<R> = Gen<StepYield, StepResume, Pin<Box<dyn Future<Output = Result<R, DeltaError>>>>>;

/// A state machine driven by a stackless coroutine (via [`genawaiter2`]).
///
/// At each phase the body yields a single `StepYield` (operation + static phase name)
/// and awaits the engine outcome. The driver shuttles operations to
/// [`StateMachine::get_step`] and resumes the body with the matching `StepResume` on
/// [`StateMachine::submit`]. Protocol is strictly 1:1: each yield carries one
/// [`EngineRequest`], the driver hands it to the engine, and the engine outcome flows back
/// as a `StepResume` on submit. Zero-yield coroutines are allowed; they return
/// [`NextStep::Done`] on the first submit.
///
/// # Identity
///
/// Every `CoroutineSM` carries a fresh `sm_id: Uuid` and a static `sm_kind` label (the
/// kernel SM's logical kind: `"scan_metadata"`, `"full_state"`, ...). Together with the
/// per-phase name available via [`StateMachine::step_name`] these form the
/// `(sm_id, sm_kind, step_name)` identity tuple used to correlate engine activity across
/// the SM boundary.
///
/// `sm_id` is generated by the driver and passed into the producer closure for any
/// SM-rooted identity stamping (e.g. reducer handles emitted by [`EngineRequest::Reduce`]).
pub struct CoroutineSM<R: 'static> {
    gen: InnerGen<R>,
    /// Where the SM is positioned. `Yielded` while engine work is pending; `ResultReady`
    /// when the body completed without ever yielding (the next `submit` hands the stored
    /// result back); `Done` once `submit` has returned the terminal result.
    state: SmPosition<R>,
    sm_id: Uuid,
    sm_kind: &'static str,
}

/// Internal positioning of a [`CoroutineSM`]. Drives the [`StateMachine`] contract.
#[allow(dead_code)] // Yielded/ResultReady are only constructed by ::new; consumer is Context::dispatch
enum SmPosition<R> {
    /// A phase is queued; engine work pending.
    Yielded(StepYield),
    /// Body has produced its terminal result; awaiting `submit` to hand it back. Reached
    /// only by zero-yield SMs (multi-yield SMs skip through this state inside `submit` and
    /// transition straight to `Done`).
    ResultReady(Result<R, DeltaError>),
    /// Result has been consumed; terminal sink.
    Done,
}

impl<R: 'static> CoroutineSM<R> {
    /// Construct a coroutine state machine and run the producer to its first yield (or to
    /// completion).
    ///
    /// `sm_kind` labels the SM's logical kind for diagnostics (e.g. `"scan_metadata"`); the
    /// driver mints a fresh `sm_id` and threads it into the producer closure for any
    /// SM-rooted identity stamping the body needs.
    ///
    /// If the producer yields, the first yield becomes the initial `StepYield` returned
    /// by [`StateMachine::get_step`]. If it completes without yielding, the terminal
    /// result is stored and handed back from the first [`StateMachine::submit`] call --
    /// callers don't need to special-case zero-yield SMs.
    #[allow(dead_code)] // Context::dispatch is the consumer (sibling module)
    pub(crate) fn new<F, Fut>(sm_kind: &'static str, producer: F) -> Self
    where
        F: FnOnce(Engine, Uuid) -> Fut + 'static,
        Fut: Future<Output = Result<R, DeltaError>> + 'static,
    {
        let sm_id = Uuid::new_v4();
        let mut gen: InnerGen<R> = Gen::new(move |engine| {
            Box::pin(producer(engine, sm_id))
                as Pin<Box<dyn Future<Output = Result<R, DeltaError>>>>
        });
        // genawaiter2's `Gen<Y, R, F>` exposes only `resume_with(R)`; the first resume arg
        // is discarded because nothing is awaiting it. We pass an inert
        // `Ok(EngineResponse::Empty)` sentinel that the body never observes (a yield
        // always precedes any awaited resume, and zero-yield bodies terminate before
        // inspection).
        let state = match gen.resume_with(StepResume(Ok(EngineResponse::Empty))) {
            GeneratorState::Yielded(phase) => SmPosition::Yielded(phase),
            GeneratorState::Complete(result) => SmPosition::ResultReady(result),
        };
        Self {
            gen,
            state,
            sm_id,
            sm_kind,
        }
    }

    /// `true` once the coroutine has terminated and [`submit`](Self::submit) has handed
    /// out its final result.
    pub fn is_done(&self) -> bool {
        matches!(self.state, SmPosition::Done)
    }

    /// Fresh per-instance identifier. Stable across the lifetime of this SM and embedded
    /// in SM-rooted artifacts (reducer handles, diagnostic logs).
    pub fn sm_id(&self) -> Uuid {
        self.sm_id
    }

    /// Static label for the SM's logical kind (e.g. `"scan_metadata"`, `"full_state"`).
    pub fn sm_kind(&self) -> &'static str {
        self.sm_kind
    }
}

impl<R: 'static> StateMachine for CoroutineSM<R> {
    type Result = R;

    fn get_step(&mut self) -> Result<EngineRequest, DeltaError> {
        match &self.state {
            SmPosition::Yielded(phase) => Ok(phase.operation.clone()),
            SmPosition::ResultReady(_) => Err(DeltaError::invariant(
                "CoroutineSM::get_step: zero-yield SM has no pending operation; \
                 call submit() to receive the terminal result",
            )),
            SmPosition::Done => Err(DeltaError::invariant(
                "CoroutineSM::get_step: state machine already completed",
            )),
        }
    }

    fn submit(
        &mut self,
        result: Result<EngineResponse, EngineError>,
    ) -> Result<NextStep<R>, DeltaError> {
        match mem::replace(&mut self.state, SmPosition::Done) {
            SmPosition::ResultReady(value) => {
                // Zero-yield SMs hand back their terminal result on the first `submit`.
                // The input is ignored because the body never awaited it, but a caller-
                // supplied `Err(EngineError)` would indicate the executor wrapped an
                // operation around a no-op SM and observed a real failure -- escalate
                // rather than silently dropping it.
                if let Err(err) = result {
                    return Err(DeltaError::invariant(err));
                }
                value.map(NextStep::Done)
            }
            SmPosition::Done => Err(DeltaError::invariant(
                "CoroutineSM::submit: cannot submit, already completed",
            )),
            SmPosition::Yielded(_) => match self.gen.resume_with(StepResume(result)) {
                GeneratorState::Yielded(next) => {
                    self.state = SmPosition::Yielded(next);
                    Ok(NextStep::Continue)
                }
                GeneratorState::Complete(final_result) => final_result.map(NextStep::Done),
            },
        }
    }

    fn step_name(&self) -> &'static str {
        match &self.state {
            SmPosition::Yielded(phase) => phase.step_name,
            SmPosition::ResultReady(_) | SmPosition::Done => "done",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::state_machines::framework::engine_error::EngineErrorKind;
    use crate::plans::state_machines::framework::state_machine::SchemaQuery;

    fn toy_step() -> EngineRequest {
        EngineRequest::SchemaQuery(SchemaQuery::new("/toy"))
    }

    fn toy_schema() -> crate::schema::SchemaRef {
        std::sync::Arc::new(crate::schema::StructType::new_unchecked(Vec::<
            crate::schema::StructField,
        >::new()))
    }

    /// Drive a two-phase SM: yield op A, observe a SchemaQuery response, yield op B, complete.
    ///
    /// The 1:1 pairing in `state_machine.rs` is SchemaQuery -> Schema; submitting
    /// `EngineResponse::Schema(_)` (and not `EngineResponse::Empty`, which is a driver
    /// sentinel only) exercises the documented protocol end-to-end.
    #[test]
    fn two_phase_sm_executes_in_sequence() {
        let mut sm = CoroutineSM::<i64>::new("test", |mut engine, _sm_id| async move {
            let phase_a = StepYield {
                operation: toy_step(),
                step_name: "phase_a",
            };
            let phase_b = StepYield {
                operation: toy_step(),
                step_name: "phase_b",
            };
            let _ = engine.yield_(phase_a).await;
            let _ = engine.yield_(phase_b).await;
            Ok(42)
        });

        assert_eq!(sm.sm_kind(), "test");

        assert_eq!(sm.step_name(), "phase_a");
        let op = sm.get_step().unwrap();
        assert!(matches!(op, EngineRequest::SchemaQuery(_)));

        let r = sm.submit(Ok(EngineResponse::Schema(toy_schema()))).unwrap();
        assert!(matches!(r, NextStep::Continue));
        assert_eq!(sm.step_name(), "phase_b");

        let op = sm.get_step().unwrap();
        assert!(matches!(op, EngineRequest::SchemaQuery(_)));

        let r = sm.submit(Ok(EngineResponse::Schema(toy_schema()))).unwrap();
        match r {
            NextStep::Done(v) => assert_eq!(v, 42),
            _ => panic!("expected Done"),
        }
        assert!(sm.is_done());
    }

    /// Engine errors flow through as `Err(EngineError)` on resume; the SM body receives
    /// them and decides what to do.
    #[test]
    fn engine_error_flows_to_body_as_resume_err() {
        let mut sm = CoroutineSM::<String>::new("test", |mut engine, _sm_id| async move {
            let step = StepYield {
                operation: toy_step(),
                step_name: "p",
            };
            let resume = engine.yield_(step).await;
            match resume.0 {
                Err(e) => Ok(format!("got: {}", e.kind)),
                Ok(_) => panic!("expected engine error"),
            }
        });

        let _ = sm.get_step().unwrap();
        let err = EngineError::new(EngineErrorKind::FileNotFound { path: "/x".into() });
        let r = sm.submit(Err(err)).unwrap();
        match r {
            NextStep::Done(s) => assert!(s.contains("file not found: /x")),
            _ => panic!("expected Done"),
        }
    }

    /// Zero-yield coroutines are allowed: the first `submit` call hands the stored
    /// terminal value back. Useful for SMs that compute their plans entirely up-front and
    /// have no intermediate engine work.
    #[test]
    fn zero_phase_coroutine_returns_value_on_first_submit() {
        let mut sm = CoroutineSM::<i32>::new("test", |_co, _sm_id| async move { Ok(7) });
        assert!(sm.get_step().is_err());
        let r = sm.submit(Ok(EngineResponse::Empty)).unwrap();
        match r {
            NextStep::Done(v) => assert_eq!(v, 7),
            _ => panic!("expected Done"),
        }
        assert!(sm.is_done());
    }
}
