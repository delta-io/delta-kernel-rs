//! **Kernel/reference vs DataFusion executor** parity for declarative state machine framework
//! phase ops. Real FSR multi-phase execution is covered by dedicated FSR integration tests.

use std::fs;
use std::sync::Arc;

use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::RelationHandle;
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::EvaluationHandler;
use delta_kernel_datafusion_engine::DataFusionExecutor;
use tempfile::tempdir;
use url::Url;

fn concat_or_clone(batches: &[RecordBatch]) -> RecordBatch {
    match batches.len() {
        0 => panic!("empty batches"),
        1 => batches[0].clone(),
        _ => concat_batches(&batches[0].schema(), batches).expect("concat_batches"),
    }
}

fn assert_batch_column_data_equal(
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
            .expect("kernel→arrow schema"),
    );
    let exp = RecordBatch::try_new(canonical.clone(), expected.columns().to_vec())
        .expect("canonical exp");
    let act = RecordBatch::try_new(canonical, actual.columns().to_vec()).expect("canonical act");
    assert_eq!(exp, act);
}

fn long_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "x",
        DataType::LONG,
    )]))
}

fn kernel_literal_record_batch(schema: SchemaRef, rows: &[Vec<Scalar>]) -> RecordBatch {
    let handler = ArrowEvaluationHandler;
    let row_refs: Vec<&[Scalar]> = rows.iter().map(|r| r.as_slice()).collect();
    handler
        .create_many(schema, &row_refs)
        .expect("create_many")
        .try_into_record_batch()
        .expect("record batch")
}

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
    let schema = long_schema();
    let rows = vec![vec![Scalar::Long(100)], vec![Scalar::Long(200)]];
    let kernel_reference_batch = kernel_literal_record_batch(Arc::clone(&schema), &rows);

    let handle = RelationHandle::fresh("parity_pipe", Arc::clone(&schema));
    let producer = DeclarativePlanNode::values(Arc::clone(&schema), rows.clone())
        .unwrap()
        .into_relation(handle.clone());

    let executor = DataFusionExecutor::try_new().unwrap();
    executor
        .execute_phase_operation(PhaseOperation::Plans(vec![producer]))
        .await
        .expect("phase Plans");

    let df_batches = executor.collect_relation(&handle).await.expect("collect");
    let df_concat = concat_or_clone(&df_batches);

    assert_batch_column_data_equal(&schema, &kernel_reference_batch, &df_concat);
}
