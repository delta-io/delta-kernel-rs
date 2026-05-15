//! [`delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation`]
//! wiring for [`delta_kernel_datafusion_engine::DataFusionExecutor`] (`execute_phase_operation`).

use std::any::Any;
use std::fs::File;
use std::sync::Arc;

use delta_kernel::arrow::array::Int64Array;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::{ConsumeByKdfSink, RelationHandle};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::plans::kdf::{ConsumerKdf, Kdf, KdfControl};
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{DeltaResult, EngineData};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;
use url::Url;

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
        "consumer.sum_rows_phase_op"
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
async fn phase_plans_runs_relation_producer_then_consumer_in_one_tick() {
    let schema = long_schema();
    let rows = vec![vec![Scalar::Long(1)], vec![Scalar::Long(2)]];
    let handle = RelationHandle::fresh("pipe", schema.clone());

    let producer = DeclarativePlanNode::values(schema.clone(), rows)
        .expect("literal")
        .into_relation(handle.clone());

    let consumer = DeclarativePlanNode::relation_ref(handle.clone()).into_results();

    let executor = DataFusionExecutor::try_new().unwrap();
    let accum = executor
        .execute_phase_operation(PhaseOperation::Plans(vec![producer, consumer]))
        .await
        .expect("phase execution");

    // Results sink does not submit KDF handles -- relation traffic stays in
    // the registry. The accumulator should observe neither a KDF token nor a
    // schema submission.
    assert!(accum.take_schema().is_none());

    let read_back = DeclarativePlanNode::relation_ref(handle).into_results();
    let batches = executor
        .execute_plan_collect(read_back)
        .await
        .expect("read relation");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn phase_plans_submits_consume_by_kdf_into_phase_kdf_state_not_harvest_slot() {
    let schema = long_schema();
    let rows = vec![vec![Scalar::Long(10)], vec![Scalar::Long(20)]];
    let sink = ConsumeByKdfSink::new_consumer(SumRowsConsumer(0));
    let token = sink.token.clone();
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .consume_by_kdf(sink);

    let executor = DataFusionExecutor::try_new().unwrap();
    let accum = executor
        .execute_phase_operation(PhaseOperation::Plans(vec![plan]))
        .await
        .expect("phase execution");

    assert!(
        executor.take_last_kdf_finished().is_none(),
        "phase execution must not populate the legacy harvest slot"
    );

    let payloads = accum.take_by_token(&token);
    assert_eq!(payloads.len(), 1);
    let total = *payloads[0]
        .downcast_ref::<usize>()
        .expect("SumRowsConsumer finishes with usize");
    assert_eq!(total, 2);
}

#[tokio::test]
async fn phase_schema_query_footer_round_trips_schema_via_take_schema() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("chunk.parquet");
    let arrow_schema = ArrowSchema::new(vec![Field::new("id", ArrowDataType::Int64, false)]);
    let batch = ArrowRecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as _],
    )
    .unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, Arc::new(arrow_schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let url = Url::from_file_path(&path).unwrap();
    let node = SchemaQueryNode::new(url.as_str());

    let executor = DataFusionExecutor::try_new().unwrap();
    let state = executor
        .execute_phase_operation(PhaseOperation::SchemaQuery(node))
        .await
        .expect("schema query phase");

    let schema = state.take_schema().expect("schema submitted");
    assert!(
        schema.fields().any(|f| f.name() == "id"),
        "expected id column in footer schema: {schema:?}"
    );
}

#[tokio::test]
async fn legacy_execute_plan_to_stream_still_harvests_via_slot_when_accumulator_disabled() {
    let schema = long_schema();
    let rows = vec![vec![Scalar::Long(7)]];
    let sink = ConsumeByKdfSink::new_consumer(SumRowsConsumer(0));
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .consume_by_kdf(sink);

    let executor = DataFusionExecutor::try_new().unwrap();
    let stream = executor.execute_plan_to_stream(plan).await.unwrap();
    assert!(stream.try_collect::<Vec<_>>().await.unwrap().is_empty());

    let fh = executor.take_last_kdf_finished().expect("harvest slot");
    let total = *fh.erased.downcast_ref::<usize>().unwrap();
    assert_eq!(total, 1);
}
