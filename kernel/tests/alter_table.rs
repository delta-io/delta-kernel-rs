//! End-to-end integration tests for the AlterTable API.

use std::collections::HashMap;
use std::sync::Arc;

use url::Url;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{ColumnMetadataKey, DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::DeltaResult;
use test_utils::{generate_batch, read_scan, test_table_setup, write_batch_to_table, IntoArray};

fn create_test_table(
    table_path: &str,
    engine: &dyn delta_kernel::Engine,
    schema: Arc<StructType>,
    table_properties: &[(&str, &str)],
) -> DeltaResult<(Url, delta_kernel::snapshot::SnapshotRef)> {
    let mut builder = create_table(table_path, schema, "test/1.0");
    if !table_properties.is_empty() {
        builder = builder.with_table_properties(table_properties.to_vec());
    }
    let _ = builder
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?;

    let table_url = delta_kernel::try_parse_uri(table_path)?;
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    Ok((table_url, snapshot))
}

fn load_snapshot(
    table_url: &Url,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<delta_kernel::snapshot::SnapshotRef> {
    Snapshot::builder_for(table_url.clone()).build(engine)
}

fn committer() -> Box<FileSystemCommitter> {
    Box::new(FileSystemCommitter::new())
}

fn two_column_schema() -> DeltaResult<Arc<StructType>> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::nullable("name", DataType::STRING),
    ])?))
}

fn three_column_schema() -> DeltaResult<Arc<StructType>> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::nullable("name", DataType::STRING),
        StructField::nullable("age", DataType::INTEGER),
    ])?))
}

const COLUMN_MAPPING_NAME: [(&str, &str); 1] = [("delta.columnMapping.mode", "name")];

#[tokio::test]
async fn alter_table_add_column() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;
    assert_eq!(snapshot.version(), 0);
    assert_eq!(snapshot.schema().num_fields(), 2);

    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let evolved_schema = snapshot.schema();
    assert_eq!(evolved_schema.num_fields(), 3);

    let email_field = evolved_schema
        .field("email")
        .expect("email field should exist after ALTER TABLE");
    assert_eq!(email_field.data_type(), &DataType::STRING);
    assert!(email_field.is_nullable());

    Ok(())
}

#[tokio::test]
async fn alter_table_add_column_with_column_mapping() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) = create_test_table(
        &table_path,
        engine.as_ref(),
        two_column_schema()?,
        &COLUMN_MAPPING_NAME,
    )?;

    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let evolved_schema = snapshot.schema();
    assert_eq!(evolved_schema.num_fields(), 3);

    let email_field = evolved_schema.field("email").unwrap();
    assert!(
        email_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .is_some(),
        "email should have a column mapping ID"
    );
    assert!(
        email_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .is_some(),
        "email should have a physical name"
    );

    Ok(())
}

#[tokio::test]
async fn alter_table_add_non_nullable_column_rejected() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (_table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    let result = snapshot
        .alter_table()
        .add_column(StructField::new("required_col", DataType::STRING, false))
        .build(engine.as_ref(), committer());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("non-nullable"));

    Ok(())
}

#[tokio::test]
async fn alter_table_sequential_adds_stack_correctly() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot_v1 = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot_v1.version(), 1);
    assert_eq!(snapshot_v1.schema().num_fields(), 3);

    let _ = snapshot_v1
        .alter_table()
        .add_column(StructField::nullable("phone", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot_v2 = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot_v2.version(), 2);
    let schema = snapshot_v2.schema();
    assert_eq!(schema.num_fields(), 4);
    assert!(schema.field("id").is_some());
    assert!(schema.field("name").is_some());
    assert!(schema.field("email").is_some());
    assert!(schema.field("phone").is_some());

    Ok(())
}

#[tokio::test]
async fn alter_table_post_commit_snapshot_has_correct_version() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (_table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    let result = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let post_commit_snapshot = match result {
        CommitResult::CommittedTransaction(committed) => committed
            .post_commit_snapshot()
            .expect("post-commit snapshot should exist")
            .clone(),
        other => panic!("Expected CommittedTransaction, got: {other:?}"),
    };

    assert_eq!(post_commit_snapshot.version(), 1);

    Ok(())
}

#[tokio::test]
async fn alter_table_data_survives_add_column() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    let batch = generate_batch(vec![
        ("id", vec![1, 2, 3].into_array()),
        ("name", vec!["alice", "bob", "carol"].into_array()),
    ])
    .unwrap();

    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new())
        .await
        .unwrap();

    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot.version(), 2); // v0=create, v1=write, v2=alter

    let evolved_schema = snapshot.schema();
    assert_eq!(evolved_schema.num_fields(), 3);
    assert!(evolved_schema.field("email").is_some());

    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;

    assert!(!batches.is_empty(), "scan should return data");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "should have 3 rows");
    assert_eq!(batches[0].num_columns(), 3);

    let email_col = batches[0].column(2);
    assert_eq!(
        email_col.null_count(),
        3,
        "email column should be all nulls"
    );

    Ok(())
}

#[tokio::test]
async fn alter_table_write_data_after_schema_evolution() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    // Write initial data (v1)
    let batch = generate_batch(vec![
        ("id", vec![1, 2].into_array()),
        ("name", vec!["alice", "bob"].into_array()),
    ])
    .unwrap();
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new())
        .await
        .unwrap();

    // Evolve schema: add email column (v2)
    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    // Write new data conforming to evolved schema (v3)
    let snapshot = load_snapshot(&table_url, engine.as_ref())?;
    let batch = generate_batch(vec![
        ("id", vec![3, 4].into_array()),
        ("name", vec!["carol", "dave"].into_array()),
        ("email", vec!["carol@x.com", "dave@x.com"].into_array()),
    ])
    .unwrap();
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new())
        .await
        .unwrap();

    // Scan back all data -- old rows should have email=NULL, new rows should have email values
    assert_eq!(snapshot.version(), 3);
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "should have 4 rows total (2 old + 2 new)");

    // Verify all batches have 3 columns
    for batch in &batches {
        assert_eq!(batch.num_columns(), 3, "all batches should have 3 columns");
    }

    Ok(())
}

#[tokio::test]
async fn alter_table_commit_log_contains_metadata_action() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (_table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let log_path = format!("{}/_delta_log/00000000000000000001.json", table_path);
    let log_content = std::fs::read_to_string(&log_path)
        .unwrap_or_else(|_| panic!("Should be able to read commit log at {}", log_path));

    assert!(
        log_content.contains("\"metaData\""),
        "commit log should contain a metaData action"
    );
    assert!(
        log_content.contains("email"),
        "metaData action should include the new email column in schemaString"
    );
    assert!(
        log_content.contains("\"commitInfo\""),
        "commit log should contain commitInfo"
    );

    Ok(())
}

#[tokio::test]
async fn alter_table_set_nullable() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    let _ = snapshot
        .alter_table()
        .set_nullable(ColumnName::new(["id"]))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let evolved_schema = snapshot.schema();
    let id_field = evolved_schema.field("id").expect("id field should exist");
    assert!(id_field.is_nullable(), "id should now be nullable");

    Ok(())
}

#[tokio::test]
async fn alter_table_drop_column() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) = create_test_table(
        &table_path,
        engine.as_ref(),
        three_column_schema()?,
        &COLUMN_MAPPING_NAME,
    )?;

    let _ = snapshot
        .alter_table()
        .drop_column(ColumnName::new(["age"]))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let evolved_schema = snapshot.schema();
    assert_eq!(evolved_schema.num_fields(), 2);
    assert!(evolved_schema.field("id").is_some());
    assert!(evolved_schema.field("name").is_some());
    assert!(
        evolved_schema.field("age").is_none(),
        "age should be removed"
    );

    Ok(())
}

#[tokio::test]
async fn alter_table_rename_column() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) = create_test_table(
        &table_path,
        engine.as_ref(),
        two_column_schema()?,
        &COLUMN_MAPPING_NAME,
    )?;

    let _ = snapshot
        .alter_table()
        .rename_column(ColumnName::new(["name"]), "full_name")
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let evolved_schema = snapshot.schema();
    assert_eq!(evolved_schema.num_fields(), 2);
    assert!(evolved_schema.field("name").is_none());
    assert!(evolved_schema.field("full_name").is_some());

    let full_name_field = evolved_schema.field("full_name").unwrap();
    assert!(
        full_name_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .is_some(),
        "renamed column should preserve its physical name"
    );

    Ok(())
}

#[tokio::test]
async fn alter_table_drop_without_column_mapping_rejected() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (_table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    let result = snapshot
        .alter_table()
        .drop_column(ColumnName::new(["name"]))
        .build(engine.as_ref(), committer());
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("column mapping must be enabled"));

    Ok(())
}

#[tokio::test]
async fn alter_table_rename_without_column_mapping_rejected() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (_table_url, snapshot) =
        create_test_table(&table_path, engine.as_ref(), two_column_schema()?, &[])?;

    let result = snapshot
        .alter_table()
        .rename_column(ColumnName::new(["name"]), "full_name")
        .build(engine.as_ref(), committer());
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("column mapping must be enabled"));

    Ok(())
}

#[tokio::test]
async fn alter_table_chained_add_drop_set_nullable() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (table_url, snapshot) = create_test_table(
        &table_path,
        engine.as_ref(),
        three_column_schema()?,
        &COLUMN_MAPPING_NAME,
    )?;

    // Chain add + drop + set_nullable in one transaction
    let _ = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .drop_column(ColumnName::new(["age"]))
        .set_nullable(ColumnName::new(["id"]))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?;

    let snapshot = load_snapshot(&table_url, engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let evolved_schema = snapshot.schema();
    assert_eq!(evolved_schema.num_fields(), 3); // id, name, email

    let id_field = evolved_schema.field("id").expect("id should exist");
    assert!(id_field.is_nullable(), "id should be nullable");

    assert!(evolved_schema.field("name").is_some());
    assert!(evolved_schema.field("email").is_some());
    assert!(evolved_schema.field("age").is_none());

    Ok(())
}
