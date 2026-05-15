//! Error helpers mapping foreign failures into [`delta_kernel::plans::errors::DeltaError`].
//!
//! Orphan rules forbid `impl From<DataFusionError> for DeltaError` here; use
//! [`LiftDeltaErr::lift`] or [`datafusion_err_to_delta`] instead.

use datafusion_common::error::DataFusionError;
use delta_kernel::plans::errors::{DeltaError, DeltaErrorCode};

/// Wrap an arbitrary error chain into a [`DataFusionError::External`].
///
/// Reduces `DataFusionError::External(Box::new(e))` to `wrap_delta_err(e)` at every Stream-error
/// translation site across exec/* modules.
pub fn wrap_delta_err<E>(err: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(err))
}

/// Typed plan-compilation failure for the DataFusion engine path.
pub fn plan_compilation(detail: impl Into<String>) -> DeltaError {
    let detail = detail.into();
    delta_kernel::delta_error!(
        DeltaErrorCode::DeltaCommandInvariantViolation,
        operation = "PlanCompilation",
        detail = detail,
    )
}

/// Explicitly unsupported IR for this scaffold / engine slice.
pub fn unsupported(detail: impl Into<String>) -> DeltaError {
    let detail = detail.into();
    delta_kernel::delta_error!(
        DeltaErrorCode::DeltaCommandInvariantViolation,
        operation = "Unsupported",
        detail = detail,
    )
}

pub fn internal_error(detail: impl Into<String>) -> DeltaError {
    let detail = detail.into();
    delta_kernel::delta_error!(
        DeltaErrorCode::DeltaCommandInvariantViolation,
        operation = "Internal",
        detail = detail,
    )
}

/// Row-level [`AssertCheck`] violation surfaced during execution.
///
/// Uses [`DeltaErrorCode::DeltaCommandInvariantViolation`] with `<detail>` carrying the IR
/// `error_message` (verbatim), `error_code`, row index, and whether the predicate was NULL.
pub fn assert_violation(
    error_code: &str,
    error_message: &str,
    row_index: usize,
    predicate_was_null: bool,
) -> DeltaError {
    let pred_note = if predicate_was_null {
        "predicate was NULL"
    } else {
        "predicate was false"
    };
    let detail = format!(
        "{} [check `{}`, row {}, {}]",
        error_message, error_code, row_index, pred_note
    );
    delta_kernel::delta_error!(
        DeltaErrorCode::DeltaCommandInvariantViolation,
        operation = "DeclarativePlan.Assert",
        detail = detail,
    )
}

/// Best-effort mapping until DataFusion annotates richer categories.
///
/// [`DataFusionError::External`] values that wrap [`DeltaError`] (for example assert violations)
/// are unwrapped so callers receive the original typed error instead of a nested wrapper.
pub fn datafusion_err_to_delta(e: DataFusionError) -> DeltaError {
    match e {
        DataFusionError::External(inner) => match inner.downcast::<DeltaError>() {
            Ok(delta_err) => *delta_err,
            Err(orig) => delta_kernel::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                source = DataFusionError::External(orig),
            ),
        },
        other => delta_kernel::delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            source = other,
        ),
    }
}

/// Lift `Result<T, DataFusionError>` using [`datafusion_err_to_delta`].
pub trait LiftDeltaErr<T> {
    fn lift(self) -> Result<T, DeltaError>;
}

impl<T> LiftDeltaErr<T> for Result<T, DataFusionError> {
    fn lift(self) -> Result<T, DeltaError> {
        self.map_err(datafusion_err_to_delta)
    }
}
