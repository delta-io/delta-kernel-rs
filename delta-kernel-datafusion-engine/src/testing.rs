//! Test-only buffered collectors for [`DataFusionExecutor`].
//!
//! Thin wrappers over [`DataFusionExecutor::read_relation`] and
//! [`DataFusionExecutor::execute_plans`] that drain into a `Vec<RecordBatch>`. Since
//! [`DataFusionExecutor::read_relation`] now returns a
//! [`DataFrame`](datafusion::dataframe::DataFrame) whose logical schema matches `handle.schema`
//! byte-for-byte (logical names + Delta field metadata, recursively into nested struct / list / map
//! types), no post-collect stamping is required -- the returned batches already carry the
//! kernel-declared schema.
//!
//! The production API returns DataFrames; eager-materialization callers in non-test
//! code can just call `.collect()` on the returned DataFrame directly.
//!
//! Named `testing` rather than `test_utils` to avoid colliding with the workspace
//! `test_utils` crate at import sites in tests that consume both. Gated by
//! `#[cfg(any(test, feature = "test-utils"))]`. The feature is activated for this crate's
//! own integration tests via a self dev-dependency in `Cargo.toml`, and for external test
//! crates (e.g. `acceptance`) by enabling `features = ["test-utils"]` on their
//! dev-dependency on this crate.

use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::RelationHandle;
use delta_kernel::plans::ir::ssa::ResultPlan as SsaResultPlan;
use delta_kernel::plans::ir::ResultPlan;

use crate::error::DfResultIntoDelta;
use crate::DataFusionExecutor;

/// Drain every batch of a registered relation into a `Vec`. Thin wrapper over
/// [`DataFusionExecutor::read_relation`] +
/// [`DataFrame::collect`](datafusion::dataframe::DataFrame::collect). The resulting batches'
/// schemas already match `handle.schema` byte-for-byte (logical names + `delta.columnMapping.*` /
/// `parquet.field.id` metadata) because [`DataFusionExecutor::read_relation`] performs the rename +
/// stamp inside the logical plan; no post-collect cast pass is needed.
pub async fn collect_relation(
    executor: &DataFusionExecutor,
    handle: &RelationHandle,
) -> Result<Vec<RecordBatch>, DeltaError> {
    executor
        .read_relation(handle)
        .await?
        .collect()
        .await
        .into_delta()
}

/// Run [`ResultPlan::plans`] and then drain every batch in the result relation into a
/// `Vec`. Equivalent to [`DataFusionExecutor::drive_to_dataframe`] followed by
/// `.collect()`; convenient for callers that already hold a [`ResultPlan`] (e.g.
/// obtained by driving the SM by hand via [`DataFusionExecutor::drive_to_completion`]).
pub async fn collect_result(
    executor: &DataFusionExecutor,
    rp: ResultPlan,
) -> Result<Vec<RecordBatch>, DeltaError> {
    executor.execute_plans(&rp.plans).await?;
    collect_relation(executor, &rp.result_relation).await
}

/// SSA analog of [`collect_result`]: compile an [`SsaResultPlan`] to a [`DataFrame`] via
/// [`DataFusionExecutor::ssa_result_to_dataframe`] and drain it into a `Vec`. The terminal
/// `LogicalPlan` is wrapped without any relation-registry side effects, so this helper
/// is suitable for SSA plans constructed directly in tests (no coroutine required).
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
