//! Error helpers for the DataFusion engine.
//!
//! Engine internals operate in [`DataFusionError`] space: every helper here produces a
//! [`DataFusionError`] variant, so engine code can use bare `?` to propagate errors. Conversion
//! into kernel-flavored errors ([`DeltaError`], [`EngineError`]) happens only at the engine ->
//! kernel boundary methods on [`crate::DataFusionExecutor`].

use datafusion_common::error::DataFusionError;
use delta_kernel::plans::errors::{DeltaError, DeltaErrorCode};

/// Wrap an arbitrary error chain into a [`DataFusionError::External`].
///
/// Used to bridge kernel-side errors (e.g. [`DeltaError`], [`delta_kernel::Error`]) into the
/// engine's native [`DataFusionError`] flow.
pub fn wrap_delta_err<E>(err: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(err))
}

/// Typed plan-compilation failure for the DataFusion engine path.
pub fn plan_compilation(detail: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(format!("PlanCompilation: {}", detail.into()))
}

/// Explicitly unsupported IR for this scaffold / engine slice.
pub fn unsupported(detail: impl Into<String>) -> DataFusionError {
    DataFusionError::NotImplemented(format!("Unsupported: {}", detail.into()))
}

/// Engine-internal invariant violation.
pub fn internal_error(detail: impl Into<String>) -> DataFusionError {
    DataFusionError::Internal(format!("Internal: {}", detail.into()))
}

/// Convert a [`DataFusionError`] produced by engine internals into a [`DeltaError`] at the
/// engine -> kernel boundary. [`DataFusionError::External`] values that already wrap a
/// [`DeltaError`] are unwrapped so callers receive the original typed error instead of a nested
/// wrapper.
pub fn df_to_delta(e: DataFusionError) -> DeltaError {
    match e {
        DataFusionError::External(inner) => match inner.downcast::<DeltaError>() {
            Ok(delta_err) => *delta_err,
            Err(orig) => {
                let wrapped = DataFusionError::External(orig);
                delta_kernel::delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    source = wrapped,
                )
            }
        },
        other => delta_kernel::delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            source = other,
        ),
    }
}
