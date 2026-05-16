//! Typed-output companion trait for KDFs.
//!
//! [`KdfOutput`] is the one thing each KDF state impls alongside its
//! [`super::ConsumerKdf`] runtime impl. It declares the typed output callers
//! receive and how per-partition finalized states reduce into that output.
//!
//! The bridge from state → plan-tree insertion → typed `Prepared<O>` lives
//! on [`crate::plans::ir::DeclarativePlanNode::consume`] — SM authors call
//! it directly on a state instance; no intermediate factory wrapper is exposed.

use std::any::Any;

use super::token::KdfStateToken;
use super::traits::Kdf;
use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode};

/// Typed-output companion. Each KDF state impls this once, declaring the
/// typed output callers receive and how per-partition finalized states
/// reduce to it.
///
/// ```ignore
/// impl KdfOutput for AddRemoveDedup {
///     type Output = Self;
///     fn into_output(parts: Vec<Self>) -> Result<Self, DeltaError> {
///         let mut out = Self::new();
///         for p in parts { out.merge(p); }
///         Ok(out)
///     }
/// }
/// ```
pub trait KdfOutput: Kdf + Any + Sized + 'static {
    /// What downstream callers receive after `phase.execute(...)` completes.
    type Output: Send + 'static;

    /// Reduce per-partition finalized states to [`Self::Output`].
    ///
    /// - Partitioned consumers: union/merge accumulators.
    /// - Global consumers: use [`take_single`] and project.
    /// - Global consumers passing state through unchanged: return the result of [`take_single`]
    ///   directly.
    fn into_output(parts: Vec<Self>) -> Result<Self::Output, DeltaError>;
}

/// Extract-closure shape used internally by plan-construction terminals.
pub(crate) type ExtractFn<O> =
    Box<dyn FnOnce(Vec<Box<dyn Any + Send>>) -> Result<O, DeltaError> + Send + 'static>;

/// Downcast each erased partition state to `S`. Error names the token so
/// logs tie the failure back to the right KDF.
pub fn downcast_all<S: Any>(
    erased: Vec<Box<dyn Any + Send>>,
    token: &KdfStateToken,
) -> Result<Vec<S>, DeltaError> {
    let mut out = Vec::with_capacity(erased.len());
    for (i, any_state) in erased.into_iter().enumerate() {
        match any_state.downcast::<S>() {
            Ok(s) => out.push(*s),
            Err(_) => {
                return Err(delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "kdf::downcast_all: expected `{}` for token `{}`, partition {} had wrong type",
                    std::any::type_name::<S>(),
                    token,
                    i,
                ));
            }
        }
    }
    Ok(out)
}

/// Assert exactly one partition state and return it. Used by global KDF
/// [`KdfOutput::into_output`] impls where single-state is part of the
/// contract.
pub fn take_single<S>(mut parts: Vec<S>, token: &KdfStateToken) -> Result<S, DeltaError> {
    if parts.len() == 1 {
        return parts.pop().ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "kdf::take_single: internal: len==1 but pop returned None",
            )
        });
    }
    Err(delta_error!(
        DeltaErrorCode::DeltaCommandInvariantViolation,
        "kdf::take_single: token `{}`: expected 1 partition, got {}",
        token,
        parts.len(),
    ))
}
