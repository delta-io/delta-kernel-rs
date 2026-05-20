//! **Kernel/reference vs DataFusion executor** parity for declarative state machine framework
//! phase ops. Real FSR multi-phase execution is covered by dedicated FSR integration tests.

mod common;

use std::fs;
use std::sync::Arc;

use common::{assert_batch_column_data_equal, concat_or_clone, kernel_literal_batch};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::{PlanBuilder, RelationRegistry};
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel_datafusion_engine::{testing, DataFusionExecutor};
use tempfile::tempdir;
use test_utils::schemas::single_long_schema;
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn parity_phase_schema_query_matches_kernel_parquet_footer_read() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("chunk.parquet");
    use delta_kernel::arrow::array::Int64Array;
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    let arrow_schema = ArrowSchema::new(vec![Field::new("id", ArrowDataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let file = fs::File::create(&path).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, Arc::new(arrow_schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let url = Url::from_file_path(&path).unwrap();
    let executor = DataFusionExecutor::try_new().unwrap();

    let meta = executor.engine().storage_handler().head(&url).unwrap();
    let kernel_direct = executor
        .engine()
        .parquet_handler()
        .read_parquet_footer(&meta)
        .unwrap()
        .schema;

    let node = SchemaQueryNode::new(url.as_str());
    let state = executor
        .execute_phase_operation(PhaseOperation::SchemaQuery(node))
        .await
        .expect("schema query phase");

    let phase_schema = state.take_schema().expect("footer schema submitted");

    assert_eq!(
        phase_schema.as_ref(),
        kernel_direct.as_ref(),
        "PhaseOperation::SchemaQuery (DF executor driver) matches direct kernel footer schema read"
    );
}

#[tokio::test]
async fn parity_phase_plans_relation_pipe_matches_kernel_literal_materialization() {
    let schema = single_long_schema();
    let rows = vec![vec![Scalar::Long(100)], vec![Scalar::Long(200)]];
    let kernel_reference_batch = kernel_literal_batch(Arc::clone(&schema), &rows);

    let mut registry = RelationRegistry::new(Uuid::new_v4(), "");
    let handle = PlanBuilder::values(Arc::clone(&schema), rows.clone())
        .unwrap()
        .into_relation("parity_pipe", &mut registry)
        .expect("relation sink");

    let executor = DataFusionExecutor::try_new().unwrap();
    executor
        .execute_phase_operation(PhaseOperation::Plans(registry.take_plans()))
        .await
        .expect("phase Plans");

    let df_batches = testing::collect_relation(&executor, &handle)
        .await
        .expect("collect");
    let df_concat = concat_or_clone(&df_batches);

    assert_batch_column_data_equal(&schema, &kernel_reference_batch, &df_concat);
}
