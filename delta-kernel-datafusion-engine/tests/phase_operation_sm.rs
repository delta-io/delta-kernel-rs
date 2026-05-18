//! [`delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation`]
//! wiring for [`delta_kernel_datafusion_engine::DataFusionExecutor`] (`execute_phase_operation`).
//!
//! Only the SchemaQuery surface is unique here. The Plans (Relation + Consume) surface is
//! covered by the user-facing `execute_plans` API in `tests/relation_and_kdf_sinks.rs`; the
//! `execute_phase_operation` variants were strict duplicates of those tests and were dropped.

use std::fs::File;
use std::sync::Arc;

use delta_kernel::arrow::array::Int64Array;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;
use url::Url;

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
