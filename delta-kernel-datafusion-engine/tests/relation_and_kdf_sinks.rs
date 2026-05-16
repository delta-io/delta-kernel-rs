//! Integration tests for [`SinkType::Relation`] materialization and [`SinkType::ConsumeByKdf`]
//! harvesting.

use std::any::Any;
use std::sync::Arc;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::{ConsumeByKdfSink, RelationHandle};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::plans::kdf::{ConsumerKdf, Kdf, KdfControl};
use delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{DeltaResult, EngineData};
use delta_kernel_datafusion_engine::DataFusionExecutor;

fn long_schema() -> delta_kernel::schema::SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "x",
        DataType::LONG,
    )]))
}

#[derive(Debug, Clone)]
struct SumRowsConsumer(usize);

impl Kdf for SumRowsConsumer {
    fn kdf_id(&self) -> &'static str {
        "consumer.sum_rows_test"
    }

    fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
        Box::new(self.0)
    }
}

impl ConsumerKdf for SumRowsConsumer {
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        let arrow = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| delta_kernel::Error::generic("expected ArrowEngineData"))?;
        self.0 += arrow.record_batch().num_rows();
        Ok(KdfControl::Continue)
    }
}

#[tokio::test]
async fn relation_sink_registers_batches_readable_via_relation_leaf() {
    let schema = long_schema();
    let rows = vec![vec![Scalar::Long(1)], vec![Scalar::Long(2)]];
    let handle = RelationHandle::fresh("pipe", schema.clone());

    let producer = DeclarativePlanNode::values(schema.clone(), rows)
        .expect("literal")
        .into_relation(handle.clone());

    let executor = DataFusionExecutor::try_new().unwrap();
    executor.execute_plans(&[producer]).await.unwrap();

    let batches = executor.collect_relation(&handle).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn consume_by_kdf_drains_literal_and_harvests_finished_handle() {
    let schema = long_schema();
    let rows = vec![vec![Scalar::Long(10)], vec![Scalar::Long(20)]];
    let sink = ConsumeByKdfSink::new_consumer(SumRowsConsumer(0));
    let token = sink.token.clone();
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .consume_by_kdf(sink);

    let executor = DataFusionExecutor::try_new().unwrap();
    let state = executor
        .execute_phase_operation(PhaseOperation::Plans(vec![plan]))
        .await
        .expect("phase execution");

    let payloads = state.take_by_token(&token);
    assert_eq!(payloads.len(), 1);
    let total = *payloads[0]
        .downcast_ref::<usize>()
        .expect("SumRowsConsumer finishes with usize");
    assert_eq!(total, 2);
}
