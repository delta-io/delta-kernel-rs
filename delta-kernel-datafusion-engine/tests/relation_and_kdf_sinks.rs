//! Integration tests for [`SinkType::Relation`] materialization and [`SinkType::Consume`]
//! harvesting.

mod common;

use common::SumRowsConsumer;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::{ConsumeSink, SinkType};
use delta_kernel::plans::ir::{PlanBuilder, RelationRegistry};
use delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation;
use delta_kernel_datafusion_engine::DataFusionExecutor;
use test_utils::schemas::single_long_schema;

#[tokio::test]
async fn relation_sink_registers_batches_readable_via_relation_leaf() {
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
    executor.execute_plans(&[producer]).await.unwrap();

    let batches = executor.collect_relation(&handle).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn consume_sink_drains_literal_and_harvests_finished_handle() {
    let schema = single_long_schema();
    let rows = vec![vec![Scalar::Long(10)], vec![Scalar::Long(20)]];
    let sink = ConsumeSink::new_consumer(SumRowsConsumer::new("consumer.sum_rows_test"));
    let token = sink.token.clone();
    let plan = PlanBuilder::values(schema, rows)
        .expect("literal")
        .into_consume(sink);

    let executor = DataFusionExecutor::try_new().unwrap();
    let state = executor
        .execute_phase_operation(PhaseOperation::Plans(vec![plan]))
        .await
        .expect("phase execution");

    let payload = state
        .take_by_token(&token)
        .expect("SumRowsConsumer payload submitted");
    let total = *payload
        .downcast_ref::<usize>()
        .expect("SumRowsConsumer finishes with usize");
    assert_eq!(total, 2);
}
