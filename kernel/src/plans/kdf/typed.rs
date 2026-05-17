//! Typed-output companion trait for KDFs.
//!
//! [`KdfOutput`] is the one thing each KDF state impls alongside its
//! [`super::ConsumerKdf`] runtime impl. It declares the typed output callers
//! receive and how the finalized state reduces into that output.
//!
//! The bridge from state → plan-tree insertion → typed `Extractor<O>` lives
//! on [`crate::plans::ir::DeclarativePlanNode::consume`] — SM authors call
//! it directly on a state instance; no intermediate factory wrapper is exposed.

use std::any::Any;

use super::token::KdfStateToken;
use super::traits::ConsumerKdf;
use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::plans::state_machines::framework::phase_state::PhaseState;

/// Typed-output companion. Each KDF state impls this once, declaring the
/// typed output callers receive and how the finalized state reduces to it.
///
/// ```ignore
/// impl KdfOutput for SidecarCollector {
///     type Output = Vec<FileMeta>;
///     fn into_output(self) -> Result<Self::Output, DeltaError> {
///         /* project on self */
///     }
/// }
/// ```
pub trait KdfOutput: ConsumerKdf + Any + Sized + 'static {
    /// What downstream callers receive after `phase.execute(...)` completes.
    type Output: Send + 'static;

    /// Reduce the finalized state to [`Self::Output`].
    ///
    /// Each consume sink is single-partition by construction (the executor
    /// drains one root partition; the planner pins `target_partitions = 1`),
    /// so this consumes `Self` directly. Token-keyed identity validation
    /// happens upstream in [`Extractor::for_kdf`]'s closure.
    fn into_output(self) -> Result<Self::Output, DeltaError>;
}

/// Extract-closure shape used internally by plan-construction terminals.
pub(crate) type ExtractFn<O> =
    Box<dyn FnOnce(Box<dyn Any + Send>) -> Result<O, DeltaError> + Send + 'static>;

/// A typed adapter for pulling the output of a single consume sink out of a
/// [`PhaseState`].
pub struct Extractor<O> {
    token: KdfStateToken,
    extract: ExtractFn<O>,
}

impl<O: Send + 'static> Extractor<O> {
    /// Build an `Extractor` for KDF state `S` at `token`. The closure
    /// downcasts the erased payload back to `S` and runs `S::into_output`.
    pub(crate) fn for_kdf<S>(token: KdfStateToken) -> Self
    where
        S: KdfOutput<Output = O> + 'static,
    {
        let extract: ExtractFn<O> = {
            let token = token.clone();
            Box::new(move |erased| {
                let single = erased.downcast::<S>().map(|b| *b).map_err(|_| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "kdf::extract: expected `{}` for token `{token}`",
                        std::any::type_name::<S>(),
                    )
                })?;
                single.into_output()
            })
        };
        Self { token, extract }
    }

    /// Token identifying the entry this extractor will pull from a [`PhaseState`].
    ///
    /// Test-only: production code receives the typed payload through
    /// [`Extractor::extract`] and does not need direct access to the token.
    #[cfg(test)]
    pub(crate) fn token(&self) -> &KdfStateToken {
        &self.token
    }

    /// Pull this extractor's payload from `state` and decode it.
    ///
    /// Drains the entry under this extractor's [`KdfStateToken`] from
    /// `state` (so a second call would see it absent) and runs the typed
    /// reduction. Decoding failures are wrapped in [`EngineError::internal`]
    /// so SM bodies can uniformly handle them on the engine-error path.
    pub fn extract(self, state: &PhaseState) -> Result<O, EngineError> {
        let erased = state.take_by_token(&self.token).ok_or_else(|| {
            EngineError::internal(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "kdf::extract: no entry for token `{}`",
                self.token,
            ))
        })?;
        (self.extract)(erased).map_err(EngineError::internal)
    }
}

impl<O> std::fmt::Debug for Extractor<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extractor")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}
