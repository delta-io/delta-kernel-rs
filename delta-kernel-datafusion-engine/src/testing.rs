//! Test-only buffered collectors for [`DataFusionExecutor`].
//!
//! Thin wrappers over [`DataFusionExecutor::stream_relation`] and
//! [`DataFusionExecutor::execute_plans`] that drain into a `Vec<RecordBatch>` for
//! integration tests and acceptance harnesses. The production API stays streaming-first
//! ([`DataFusionExecutor::drive_to_stream`], [`DataFusionExecutor::stream_relation`]);
//! callers wanting eager materialization in non-test code should `try_collect()` the
//! stream themselves.
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
use delta_kernel::plans::ir::ResultPlan;
use futures::TryStreamExt;

use crate::error::DfResultIntoDelta;
use crate::DataFusionExecutor;

/// Drain every batch of a registered relation into a `Vec`. Thin wrapper over
/// [`DataFusionExecutor::stream_relation`] + [`futures::TryStreamExt::try_collect`].
pub async fn collect_relation(
    executor: &DataFusionExecutor,
    handle: &RelationHandle,
) -> Result<Vec<RecordBatch>, DeltaError> {
    executor
        .stream_relation(handle)
        .await?
        .try_collect()
        .await
        .into_delta()
}

/// Run [`ResultPlan::plans`] and then drain every batch in the result relation into a
/// `Vec`. Equivalent to [`DataFusionExecutor::drive_to_stream`] followed by `try_collect`
/// for callers that already hold a [`ResultPlan`] (e.g. obtained by driving the SM by
/// hand via [`DataFusionExecutor::drive_to_completion`]).
pub async fn collect_result(
    executor: &DataFusionExecutor,
    rp: ResultPlan,
) -> Result<Vec<RecordBatch>, DeltaError> {
    executor.execute_plans(&rp.plans).await?;
    collect_relation(executor, &rp.result_relation).await
}
