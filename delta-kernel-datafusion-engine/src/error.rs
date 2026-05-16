//! Error helpers for the DataFusion engine.
//!
//! Engine internals operate in [`DataFusionError`] space: every helper here produces a
//! [`DataFusionError`] variant, so engine code can use bare `?` to propagate errors. Conversion
//! into kernel-flavored errors ([`DeltaError`], [`EngineError`]) happens only at the engine ->
//! kernel boundary methods on [`crate::DataFusionExecutor`], and is exposed as the
//! [`DfResultIntoDelta`] extension trait so boundary call sites can write `.into_delta()`
//! instead of `.map_err(df_to_delta)`.

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
///
/// Both the orphan rule (foreign-on-foreign forbids `impl From<DataFusionError> for DeltaError`)
/// and the lift-typed-`External` semantics keep this as a free function; the
/// [`DfResultIntoDelta`] trait is the call-site sugar.
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

/// Convert a `Result<T, DataFusionError>` into `Result<T, DeltaError>` via `.into_delta()?`.
/// Concentrates the engine -> kernel error transition at engine API boundaries into a single
/// fluent method, so call sites don't repeat `.map_err(df_to_delta)`.
pub(crate) trait DfResultIntoDelta<T> {
    fn into_delta(self) -> Result<T, DeltaError>;
}

impl<T> DfResultIntoDelta<T> for Result<T, DataFusionError> {
    fn into_delta(self) -> Result<T, DeltaError> {
        self.map_err(df_to_delta)
    }
}
