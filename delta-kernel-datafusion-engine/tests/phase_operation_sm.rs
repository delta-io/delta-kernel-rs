//! [`delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation`]
//! wiring for [`delta_kernel_datafusion_engine::DataFusionExecutor`] (`execute_phase_operation`).

mod common;

use std::fs::File;
use std::sync::Arc;

use common::SumRowsConsumer;
use delta_kernel::arrow::array::Int64Array;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::{ConsumeSink, SinkType};
use delta_kernel::plans::ir::{PlanBuilder, RelationRegistry};
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;
use test_utils::schemas::single_long_schema;
use url::Url;

#[tokio::test]
async fn phase_plans_runs_relation_producer_and_registers_relation() {
    let schema = single_long_schema();
    let rows = vec![vec![Scalar::Long(1)], vec![Scalar::Long(2)]];

    let mut registry = RelationRegistry::new();
    let producer = PlanBuilder::values(schema, rows)
        .expect("literal")
        .into_relation("pipe", &mut registry)
        .expect("relation sink");
    let SinkType::Relation(handle) = producer.sink.clone() else {
        unreachable!("into_relation always produces SinkType::Relation");
    };

    let executor = DataFusionExecutor::try_new().unwrap();
    let accum = executor
        .execute_phase_operation(PhaseOperation::Plans(vec![producer]))
        .await
        .expect("phase execution");

    // Relation sinks don't submit KDF or schema state; the accumulator stays empty.
    assert!(accum.take_schema().is_none());

    let batches = executor
        .collect_relation(&handle)
        .await
        .expect("read relation");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn phase_plans_submits_consume_sink_into_phase_kdf_state() {
    let schema = single_long_schema();
    let rows = vec![vec![Scalar::Long(10)], vec![Scalar::Long(20)]];
    let sink = ConsumeSink::new_consumer(SumRowsConsumer::new("consumer.sum_rows_phase_op"));
    let token = sink.token.clone();
    let plan = PlanBuilder::values(schema, rows)
        .expect("literal")
        .into_consume(sink);

    let executor = DataFusionExecutor::try_new().unwrap();
    let accum = executor
        .execute_phase_operation(PhaseOperation::Plans(vec![plan]))
        .await
        .expect("phase execution");

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
