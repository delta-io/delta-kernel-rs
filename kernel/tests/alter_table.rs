//! Integration tests for ALTER TABLE schema evolution.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int32Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use test_utils::{test_table_setup, write_batch_to_table};

fn simple_schema() -> SchemaRef {
    Arc::new(
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap(),
    )
}

fn committer() -> Box<FileSystemCommitter> {
    Box::new(FileSystemCommitter::new())
}

fn create_test_table(
    table_path: &str,
    schema: SchemaRef,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<Arc<Snapshot>> {
    let committed = create_table(table_path, schema, "Test/1.0")
        .build(engine, committer())?
        .commit(engine)?
        .unwrap_committed();
    Ok(committed
        .post_commit_snapshot()
        .expect("should have post-commit snapshot")
        .clone())
}

#[tokio::test]
async fn add_column_reload_snapshot_verify_schema() -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref())?;

    // Write data before adding the column
    let batch = RecordBatch::try_new(
        Arc::new(simple_schema().as_ref().try_into_arrow().unwrap()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    // Add a column
    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Reload from storage to verify persistence
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), 2);
    let schema = reloaded.schema();
    assert_eq!(schema.fields().count(), 3);

    let email_field = schema.field("email").expect("email should exist");
    assert_eq!(email_field.data_type(), &DataType::STRING);
    assert!(email_field.is_nullable());

    // Scan back -- old rows should have NULL for the new column
    let evolved_arrow_schema: delta_kernel::arrow::datatypes::SchemaRef =
        Arc::new(reloaded.schema().as_ref().try_into_arrow().unwrap());
    let scan = reloaded.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
    assert_eq!(batches[0].num_columns(), 3);
    let email_col = batches[0].column_by_name("email").expect("email column");
    assert_eq!(email_col.null_count(), email_col.len());

    // Write new data WITH the new column populated
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let batch2 = RecordBatch::try_new(
        evolved_arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["c", "d"])),
            Arc::new(StringArray::from(vec!["c@test.com", "d@test.com"])),
        ],
    )
    .unwrap();
    let reloaded = Arc::new(reloaded);
    let _ = write_batch_to_table(&reloaded, engine.as_ref(), batch2, HashMap::new()).await?;

    // Scan back -- should have 4 rows total
    let final_snap = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(final_snap.version(), 3);
    let scan = final_snap.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);

    Ok(())
}

#[tokio::test]
async fn add_column_multiple_in_one_commit() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref())?;

    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .add_column(StructField::nullable("age", DataType::INTEGER))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.schema().fields().count(), 4);
    assert!(reloaded.schema().field("email").is_some());
    assert!(reloaded.schema().field("age").is_some());

    Ok(())
}

#[tokio::test]
async fn add_duplicate_column_fails() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref())?;

    let err = snapshot
        .alter_table()
        .add_column(StructField::nullable("name", DataType::STRING))
        .build(engine.as_ref(), committer());
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("already exists"));

    Ok(())
}

#[tokio::test]
async fn add_non_nullable_column_fails() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref())?;

    let err = snapshot
        .alter_table()
        .add_column(StructField::not_null("age", DataType::INTEGER))
        .build(engine.as_ref(), committer());
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("non-nullable"));

    Ok(())
}
