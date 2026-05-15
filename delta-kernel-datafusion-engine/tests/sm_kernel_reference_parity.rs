//! **Kernel/reference vs DataFusion executor** parity for declarative state machines
//! (`plans::state_machines::df` insert/checkpoint flows plus framework phase ops). Real FSR
//! multi-phase execution is covered by dedicated FSR integration tests.
//!
//! ## Deferred / out of scope (documented here only)
//!
//! - **Multipart V2 checkpoints / sidecars**: kernel checkpoint builder remains classic
//!   single-file; DF multipart emission is not modeled
//!   ([`delta_kernel::plans::state_machines::df::checkpoint_write`]).
//! - **Raw parquet byte identity**: writers may choose different row-group splits or compression;
//!   parity asserts decoded Arrow batches, not file hashes.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::ir::nodes::{RelationHandle, WriteSink};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::plans::state_machines::df::{
    checkpoint_classic_parquet_write_plan, prepare_classic_checkpoint_parquet_materialization,
};
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{Engine as KernelEngine, EngineData, EvaluationHandler, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
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

fn write_commit_json(log: &Path, version: u64, lines: &[serde_json::Value]) {
    let mut buf = String::new();
    for v in lines {
        buf.push_str(&serde_json::to_string(v).expect("json"));
        buf.push('\n');
    }
    let name = format!("{:020}.json", version);
    fs::write(log.join(name), buf).expect("commit write");
}

fn setup_v2_checkpoint_fixture(table_root: &Path) {
    let log = table_root.join("_delta_log");
    fs::create_dir_all(&log).expect("mkdir log");

    write_commit_json(
        &log,
        0,
        &[
            json!({
                "add": {
                    "path": "fake_path_2",
                    "partitionValues": {},
                    "size": 50_i64,
                    "modificationTime": 1_i64,
                    "dataChange": true,
                    "stats": r#"{"numRecords":50,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":5}}"#
                }
            }),
            json!({
                "remove": {
                    "path": "fake_path_1",
                    "deletionTimestamp": 9223372036854775807_i64,
                    "dataChange": true,
                }
            }),
        ],
    );

    write_commit_json(
        &log,
        1,
        &[
            json!({
                "metaData": {
                    "id": "388876aa-094f-49fb-aabd-be833707970b",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0_i64,
                }
            }),
            json!({
                "protocol": {
                    "minReaderVersion": 3_i32,
                    "minWriterVersion": 7_i32,
                    "readerFeatures": ["v2Checkpoint"],
                    "writerFeatures": ["v2Checkpoint"],
                }
            }),
        ],
    );
}

fn read_all_parquet_batches(path: &Path) -> Vec<RecordBatch> {
    let file = fs::File::open(path).expect("open parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("reader");
    reader.map(|b| b.expect("batch")).collect()
}

fn number_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "n",
        DataType::LONG,
    )]))
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
    let consumer = DeclarativePlanNode::relation_ref(handle.clone()).into_results();

    let executor = DataFusionExecutor::try_new().unwrap();
    executor
        .execute_phase_operation(PhaseOperation::Plans(vec![producer, consumer]))
        .await
        .expect("phase Plans");

    let read_plan = DeclarativePlanNode::relation_ref(handle).into_results();
    let df_batches = executor
        .execute_plan_collect(read_plan)
        .await
        .expect("collect");
    let df_concat = concat_or_clone(&df_batches);

    assert_batch_column_data_equal(&schema, &kernel_reference_batch, &df_concat);
}

#[tokio::test]
async fn parity_checkpoint_classic_kernel_parquet_write_matches_df_relation_write_sink() {
    let dir = tempdir().expect("table dir");
    setup_v2_checkpoint_fixture(dir.path());

    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let table_root = Url::from_directory_path(dir.path()).expect("table url");
    let snapshot = Arc::new(
        Snapshot::builder_for(table_root)
            .build(engine.as_ref())
            .expect("snapshot"),
    );
    let writer = Snapshot::create_checkpoint_writer(Arc::clone(&snapshot)).expect("writer");

    let (handle, batches, dest_checkpoint_url, _state) =
        prepare_classic_checkpoint_parquet_materialization(engine.as_ref(), &writer)
            .expect("materialize checkpoint rows");

    let kernel_reference_path = dir.path().join("kernel_reference.checkpoint.parquet");
    let kernel_url = Url::from_file_path(&kernel_reference_path).expect("kernel ref url");
    let batches_for_kernel = batches.clone();
    let kernel_iter = batches_for_kernel
        .into_iter()
        .map(|rb| Ok(Box::new(ArrowEngineData::new(rb)) as Box<dyn EngineData>));
    engine
        .parquet_handler()
        .write_parquet_file(kernel_url, Box::new(kernel_iter))
        .expect("kernel checkpoint parquet write");

    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    ex.relation_batch_registry()
        .register(handle.id, batches.clone());
    let plan = checkpoint_classic_parquet_write_plan(handle, dest_checkpoint_url.clone());
    ex.drive_checkpoint_classic_parquet_write_sm(plan)
        .await
        .expect("DF checkpoint parquet SM");

    let cp_df_path = dest_checkpoint_url.to_file_path().expect("checkpoint path");
    let batches_k = read_all_parquet_batches(&kernel_reference_path);
    let batches_d = read_all_parquet_batches(&cp_df_path);

    assert_eq!(
        concat_or_clone(&batches_k),
        concat_or_clone(&batches_d),
        "classic V2 checkpoint rows: kernel ParquetHandler stream write vs DF Relation→Write sink"
    );
}

#[tokio::test]
async fn parity_insert_kernel_parquet_handler_write_matches_df_insert_sm() {
    let dir = tempdir().unwrap();
    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());

    let schema = number_schema();
    let rows = vec![
        vec![Scalar::Long(11)],
        vec![Scalar::Long(22)],
        vec![Scalar::Long(33)],
    ];
    let rb = kernel_literal_record_batch(Arc::clone(&schema), &rows);

    let kernel_path = dir.path().join("kernel.parquet");
    let kernel_url = Url::from_file_path(&kernel_path).unwrap();

    let batch_iter = std::iter::once(Ok(
        Box::new(ArrowEngineData::new(rb.clone())) as Box<dyn EngineData>
    ));
    engine
        .parquet_handler()
        .write_parquet_file(kernel_url, Box::new(batch_iter))
        .expect("kernel parquet write");

    let df_path = dir.path().join("df.parquet");
    let df_url = Url::from_file_path(&df_path).unwrap();
    let plan = DeclarativePlanNode::values(Arc::clone(&schema), rows.clone())
        .unwrap()
        .into_write(WriteSink::parquet(df_url));

    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).unwrap();
    ex.drive_insert_write_sm(plan).await.unwrap();

    let kernel_batches = read_all_parquet_batches(&kernel_path);
    let df_batches = read_all_parquet_batches(&df_path);

    assert_batch_column_data_equal(
        &schema,
        &concat_or_clone(&kernel_batches),
        &concat_or_clone(&df_batches),
    );
}
