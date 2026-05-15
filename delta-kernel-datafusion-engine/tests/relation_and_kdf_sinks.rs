//! Integration tests for [`SinkType::Relation`] materialization and [`SinkType::ConsumeByKdf`]
//! harvesting.

use std::any::Any;
use std::sync::Arc;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::{ConsumeByKdfSink, RelationHandle};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::plans::kdf::{ConsumerKdf, Kdf, KdfControl};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{DeltaResult, EngineData};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use futures::TryStreamExt;

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
    // Drain the producer plan; relation registration happens inside execute_plan_collect for
    // SinkType::Relation regardless of whether batches surface to the caller.
    let _ = executor.execute_plan_collect(producer).await.unwrap();

    let consumer_plan = DeclarativePlanNode::relation_ref(handle).into_results();
    let batches = executor.execute_plan_collect(consumer_plan).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn consume_by_kdf_drains_literal_and_harvests_finished_handle() {
    let schema = long_schema();
    let rows = vec![vec![Scalar::Long(10)], vec![Scalar::Long(20)]];
    let sink = ConsumeByKdfSink::new_consumer(SumRowsConsumer(0));
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .consume_by_kdf(sink);

    let executor = DataFusionExecutor::try_new().unwrap();
    let stream = executor.execute_plan_to_stream(plan).await.unwrap();
    let collected: Vec<_> = stream.try_collect().await.unwrap();
    assert!(collected.is_empty());

    let fh = executor
        .take_last_kdf_finished()
        .expect("KDF harvest slot populated");
    let total = *fh
        .erased
        .downcast::<usize>()
        .expect("SumRowsConsumer finishes with usize");
    assert_eq!(total, 2);
}
