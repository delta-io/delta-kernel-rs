//! Test-only buffered collectors for [`DataFusionExecutor`].
//!
//! Thin wrappers around the SSA result-plan APIs that drain a [`DataFrame`] into a
//! `Vec<RecordBatch>`. Returned batches carry whatever schema the SSA plan terminates at;
//! plans that need column-mapping renames / Delta field metadata bake those into the
//! terminal projection upstream of `Context::into_result_plan`.
//!
//! Named `testing` rather than `test_utils` to avoid colliding with the workspace
//! `test_utils` crate at import sites in tests that consume both. Gated by
//! `#[cfg(any(test, feature = "test-utils"))]`. The feature is activated for this crate's
//! own integration tests via a self dev-dependency in `Cargo.toml`, and for external test
//! crates (e.g. `acceptance`) by enabling `features = ["test-utils"]` on their
//! dev-dependency on this crate.

use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::ssa::ResultPlan as SsaResultPlan;

use crate::error::DfResultIntoDelta;
use crate::DataFusionExecutor;

/// Compile an [`SsaResultPlan`] to a [`DataFrame`](datafusion::dataframe::DataFrame) via
/// [`DataFusionExecutor::ssa_result_to_dataframe`] and drain it into a `Vec`. Suitable for
/// SSA plans constructed directly in tests (no coroutine required).
pub async fn collect_ssa_result(
    executor: &DataFusionExecutor,
    rp: SsaResultPlan,
) -> Result<Vec<RecordBatch>, DeltaError> {
    executor
        .ssa_result_to_dataframe(&rp)?
        .collect()
        .await
        .into_delta()
}
