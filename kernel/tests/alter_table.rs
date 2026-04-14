//! Integration tests for ALTER TABLE schema evolution.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int32Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::column_name;
use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
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
    properties: &[(&str, &str)],
) -> DeltaResult<Arc<Snapshot>> {
    let committed = create_table(table_path, schema, "Test/1.0")
        .with_table_properties(properties.to_vec())
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
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref(), &[])?;

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
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref(), &[])?;

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
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref(), &[])?;

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
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref(), &[])?;

    let err = snapshot
        .alter_table()
        .add_column(StructField::not_null("age", DataType::INTEGER))
        .build(engine.as_ref(), committer());
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("non-nullable"));

    Ok(())
}

#[tokio::test]
async fn set_nullable_changes_required_to_nullable() -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref(), &[])?;

    // "id" is NOT NULL -- set it to nullable
    let committed = snapshot
        .alter_table()
        .set_nullable(column_name!("id"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap().clone();

    let schema = snapshot.schema();
    let id_field = schema.field("id").unwrap();
    assert!(id_field.is_nullable());

    // Write data with NULL id (now allowed since we set it nullable)
    let arrow_schema: delta_kernel::arrow::datatypes::SchemaRef =
        Arc::new(schema.as_ref().try_into_arrow().unwrap());
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![None, Some(1)])),
            Arc::new(StringArray::from(vec!["null_id", "has_id"])),
        ],
    )
    .unwrap();
    let _ = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    // Scan back -- should have 2 rows
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let scan = reloaded.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    Ok(())
}

#[tokio::test]
async fn set_nullable_already_nullable_is_noop() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref(), &[])?;

    // "name" is already nullable
    let _ = snapshot
        .alter_table()
        .set_nullable(column_name!("name"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    let name_field = schema.field("name").unwrap();
    assert!(name_field.is_nullable());

    Ok(())
}

#[tokio::test]
async fn set_nullable_nonexistent_column_fails() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref(), &[])?;

    let err = snapshot
        .alter_table()
        .set_nullable(column_name!("nonexistent"))
        .build(engine.as_ref(), committer());
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("does not exist"));

    Ok(())
}

#[tokio::test]
async fn chain_add_column_and_set_nullable() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(&table_path, simple_schema(), engine.as_ref(), &[])?;

    // Add a column then set "id" nullable in one commit
    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .set_nullable(column_name!("id"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.schema().fields().count(), 3);
    assert!(reloaded.schema().field("email").is_some());
    assert!(reloaded.schema().field("id").unwrap().is_nullable());

    Ok(())
}

#[tokio::test]
async fn add_column_with_column_mapping_assigns_metadata() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    let original_max_id: i64 = snapshot
        .table_configuration()
        .metadata()
        .configuration()
        .get("delta.columnMapping.maxColumnId")
        .and_then(|v| v.parse().ok())
        .expect("should have maxColumnId");

    // Add column
    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    let email_field = schema.field("email").expect("email should exist");

    // Verify column mapping ID was assigned
    let cm_id = email_field
        .get_config_value(&ColumnMetadataKey::ColumnMappingId)
        .expect("should have column mapping ID");
    match cm_id {
        MetadataValue::Number(id) => assert!(*id > original_max_id),
        other => panic!("Expected Number, got: {other:?}"),
    }

    // Verify physical name was assigned
    let cm_name = email_field
        .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
        .expect("should have physical name");
    match cm_name {
        MetadataValue::String(s) => assert!(s.starts_with("col-")),
        other => panic!("Expected String, got: {other:?}"),
    }

    // Verify maxColumnId was updated in table properties
    let new_max_id: i64 = reloaded
        .table_configuration()
        .metadata()
        .configuration()
        .get("delta.columnMapping.maxColumnId")
        .and_then(|v| v.parse().ok())
        .expect("should have maxColumnId");
    assert!(new_max_id > original_max_id);

    Ok(())
}

#[tokio::test]
async fn add_column_with_column_mapping_name_mode_data_survives(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    // Write data before adding the column
    let arrow_schema = Arc::new(snapshot.schema().as_ref().try_into_arrow().unwrap());
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    // Add column
    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Scan back -- old rows should have NULL for the new column
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let scan = reloaded.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    assert_eq!(batches[0].num_columns(), 3);

    let email_col = batches[0].column_by_name("email").expect("email column");
    assert_eq!(email_col.null_count(), email_col.len());

    Ok(())
}

#[tokio::test]
async fn add_column_with_column_mapping_id_mode_data_survives(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_test_table(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", "id")],
    )?;

    // Write data before adding the column
    let arrow_schema = Arc::new(snapshot.schema().as_ref().try_into_arrow().unwrap());
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    // Add column
    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("age", DataType::INTEGER))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Scan back -- old rows should have NULL for the new column
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let scan = reloaded.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
    assert_eq!(batches[0].num_columns(), 3);

    let age_col = batches[0].column_by_name("age").expect("age column");
    assert_eq!(age_col.null_count(), age_col.len());

    Ok(())
}
