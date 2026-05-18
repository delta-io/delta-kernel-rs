//! Shared helpers for `delta-kernel-datafusion-engine` integration tests.
//!
//! Each `tests/*.rs` file compiles as a separate binary, so this module lives at
//! `tests/common/mod.rs` (rather than `tests/common.rs`) to keep it from being treated as a
//! standalone test binary. Test files include it via `mod common;`.
//!
//! Individual test binaries only use a subset of the helpers here, so `dead_code` is silenced
//! at the module level.

#![allow(dead_code)]

use std::any::Any;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::SinkType;
use delta_kernel::plans::ir::{PlanBuilder, RelationRegistry};
use delta_kernel::plans::kdf::{ConsumerKdf, ConsumerKdfId, KdfControl};
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, EngineData, EvaluationHandler};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use uuid::Uuid;

/// Wrap `node` in a Relation sink, run it on a freshly created [`DataFusionExecutor`], and
/// return the materialized batches. Helper name kept short because many test scenarios call it
/// repeatedly.
pub async fn run_to_batches(node: PlanBuilder) -> Result<Vec<RecordBatch>, DeltaError> {
    let exec = DataFusionExecutor::try_new()?;
    run_to_batches_with(&exec, node).await
}

/// Same as [`run_to_batches`] but uses the supplied executor (allowing tests to inspect or reuse
/// its engine handle).
pub async fn run_to_batches_with(
    exec: &DataFusionExecutor,
    node: PlanBuilder,
) -> Result<Vec<RecordBatch>, DeltaError> {
    let mut registry = RelationRegistry::new(Uuid::new_v4());
    let plan = node.into_relation("test_out", &mut registry)?;
    let SinkType::Relation(handle) = plan.sink.clone() else {
        unreachable!("PlanBuilder::into_relation always produces SinkType::Relation");
    };
    exec.execute_plans(&[plan]).await?;
    exec.collect_relation(&handle).await
}

/// Synchronous wrapper around [`run_to_batches`] for `#[test]` (non-async) call sites.
pub fn run_to_batches_blocking(node: PlanBuilder) -> Result<Vec<RecordBatch>, DeltaError> {
    futures::executor::block_on(run_to_batches(node))
}

/// Synchronous wrapper around [`run_to_batches_with`] for `#[test]` (non-async) call sites.
pub fn run_to_batches_with_blocking(
    exec: &DataFusionExecutor,
    node: PlanBuilder,
) -> Result<Vec<RecordBatch>, DeltaError> {
    futures::executor::block_on(run_to_batches_with(exec, node))
}

/// Merge a slice of [`RecordBatch`] values into a single batch via `concat_batches`, or return
/// the lone batch directly when only one is present. Panics on an empty slice (callers always
/// hold at least one batch).
pub fn concat_or_clone(batches: &[RecordBatch]) -> RecordBatch {
    match batches.len() {
        0 => panic!("empty batches"),
        1 => batches[0].clone(),
        _ => concat_batches(&batches[0].schema(), batches).expect("concat_batches"),
    }
}

/// Build a kernel-side reference [`RecordBatch`] by materializing literal `rows` against
/// `schema` via the kernel [`ArrowEvaluationHandler`]. Used by parity tests as a golden source.
pub fn kernel_literal_batch(schema: Arc<StructType>, rows: &[Vec<Scalar>]) -> RecordBatch {
    let handler = ArrowEvaluationHandler;
    let row_refs: Vec<&[Scalar]> = rows.iter().map(|r| r.as_slice()).collect();
    handler
        .create_many(schema, &row_refs)
        .expect("create_many")
        .try_into_record_batch()
        .expect("record batch")
}

/// Consumer KDF that accumulates the total number of rows seen across all batches and finishes
/// with the count as a `usize`. Used by tests that verify KDF wiring end-to-end.
///
/// Token identity is by-UUID, so the [`ConsumerKdfId`] tag is incidental for test wiring; we
/// reuse [`ConsumerKdfId::CheckpointHint`] as a stable placeholder.
#[derive(Debug, Clone, Default)]
pub struct SumRowsConsumer {
    pub total: usize,
}

impl SumRowsConsumer {
    pub fn new(_kdf_id_label: &'static str) -> Self {
        Self::default()
    }
}

impl ConsumerKdf for SumRowsConsumer {
    fn kdf_id(&self) -> ConsumerKdfId {
        ConsumerKdfId::CheckpointHint
    }

    fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
        Box::new(self.total)
    }

    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        let arrow = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| delta_kernel::Error::generic("expected ArrowEngineData"))?;
        self.total += arrow.record_batch().num_rows();
        Ok(KdfControl::Continue)
    }
}

/// Compare expected vs actual column data after re-stamping both with the canonical Arrow schema
/// derived from `kernel_schema`. Parquet/datafusion can produce schemas with extra metadata that
/// would make a naive `RecordBatch` equality compare fail even when column data matches.
pub fn assert_batch_column_data_equal(
    kernel_schema: &Arc<StructType>,
    expected: &RecordBatch,
    actual: &RecordBatch,
) {
    assert_eq!(expected.num_rows(), actual.num_rows(), "row count");
    assert_eq!(expected.num_columns(), actual.num_columns(), "column count");
    let canonical: Arc<delta_kernel::arrow::datatypes::Schema> = Arc::new(
        kernel_schema
            .as_ref()
            .try_into_arrow()
            .expect("kernel->arrow schema"),
    );
    let exp = RecordBatch::try_new(canonical.clone(), expected.columns().to_vec())
        .expect("canonical expected");
    let act = RecordBatch::try_new(canonical, actual.columns().to_vec()).expect("canonical actual");
    assert_eq!(exp, act);
}
