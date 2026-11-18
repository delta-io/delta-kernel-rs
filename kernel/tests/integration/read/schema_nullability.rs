use std::sync::Arc;

use delta_kernel::Snapshot;
use delta_kernel::arrow::array::{Array, ArrayRef, AsArray, Int32Array, RecordBatch, StructArray};
use delta_kernel::arrow::buffer::NullBuffer;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field, Int32Type, Schema as ArrowSchema,
};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::schema::schema_ref;
use delta_kernel::transaction::create_table::create_table;
use rstest::rstest;
use serde_json::json;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{add_commit, assert_result_error_with_message, read_scan, record_batch_to_bytes};

#[rstest]
#[case::present_struct_rejects_null_child(true)]
#[case::null_struct_allows_null_child(false)]
#[tokio::test]
async fn read_nullable_struct_with_required_child(
    #[case] struct_is_valid: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema_ref! {
        nullable "s": {
            not_null "c": INTEGER,
        },
    };
    let storage = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let table_root = "memory:///";
    create_table(table_root, schema.clone(), "test/1.0", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // The Parquet field permits nulls that the Delta schema forbids when its parent is present.
    let file_struct = StructArray::try_new(
        vec![Field::new("c", ArrowDataType::Int32, true)].into(),
        vec![Arc::new(Int32Array::from(vec![Some(42), None]))],
        Some(NullBuffer::from(vec![true, struct_is_valid])),
    )?;
    let batch =
        RecordBatch::try_from_iter_with_nullable([("s", Arc::new(file_struct) as ArrayRef, true)])?;
    let parquet_bytes = record_batch_to_bytes(&batch);
    let add = json!({
        "add": {
            "path": "data.parquet",
            "partitionValues": {},
            "size": parquet_bytes.len(),
            "modificationTime": 0,
            "dataChange": true,
        },
    });
    add_commit(table_root, storage.as_ref(), 1, add.to_string()).await?;
    storage
        .put(&Path::from("data.parquet"), parquet_bytes.into())
        .await?;

    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let result = read_scan(&scan, engine);
    if struct_is_valid {
        assert_result_error_with_message(
            result,
            "Found unmasked nulls for non-nullable StructArray",
        );
        return Ok(());
    }

    let batches = result?;
    let batch = concat_batches(&batches[0].schema(), &batches)?;
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch.schema().as_ref(),
        &ArrowSchema::try_from_kernel(schema.as_ref())?,
    );
    let values = batch.column(0).as_struct();
    assert!(values.is_valid(0));
    assert!(values.is_null(1));
    let child = values.column(0).as_primitive::<Int32Type>();
    assert_eq!(child.value(0), 42);
    assert!(child.is_null(1));
    Ok(())
}
