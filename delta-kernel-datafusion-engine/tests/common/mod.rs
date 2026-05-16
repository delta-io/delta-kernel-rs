//! Shared helpers for `delta-kernel-datafusion-engine` integration tests.
//!
//! Each `tests/*.rs` file compiles as a separate binary, so this module lives at
//! `tests/common/mod.rs` (rather than `tests/common.rs`) to keep it from being treated as a
//! standalone test binary. Test files include it via `mod common;`.
//!
//! Individual test binaries only use a subset of the helpers here, so `dead_code` is silenced
//! at the module level.

#![allow(dead_code)]

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::plans::ir::nodes::RelationHandle;
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel_datafusion_engine::DataFusionExecutor;

/// Wrap `node` in a Relation sink, run it on a freshly created [`DataFusionExecutor`], and
/// return the materialized batches. Helper name kept short because many test scenarios call it
/// repeatedly.
pub async fn run_to_batches(node: DeclarativePlanNode) -> Vec<RecordBatch> {
    let exec = DataFusionExecutor::try_new().expect("executor");
    run_to_batches_with(&exec, node).await
}

/// Same as [`run_to_batches`] but uses the supplied executor (allowing tests to inspect or reuse
/// its engine handle).
pub async fn run_to_batches_with(
    exec: &DataFusionExecutor,
    node: DeclarativePlanNode,
) -> Vec<RecordBatch> {
    let schema = node.output_schema();
    let handle = RelationHandle::fresh("test_out", schema);
    let plan = node.into_relation(handle.clone());
    exec.execute_plans(&[plan]).await.expect("execute");
    exec.collect_relation(&handle).await.expect("collect")
}

/// Synchronous wrapper around [`run_to_batches`] for `#[test]` (non-async) call sites.
pub fn run_to_batches_blocking(node: DeclarativePlanNode) -> Vec<RecordBatch> {
    futures::executor::block_on(run_to_batches(node))
}

/// Synchronous wrapper around [`run_to_batches_with`] for `#[test]` (non-async) call sites.
pub fn run_to_batches_with_blocking(
    exec: &DataFusionExecutor,
    node: DeclarativePlanNode,
) -> Vec<RecordBatch> {
    futures::executor::block_on(run_to_batches_with(exec, node))
}
