//! Integration tests for ALTER TABLE schema evolution.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int32Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::schema::{ArrayType, DataType, MapType, SchemaRef, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::DeltaResult;
use rstest::rstest;
use test_utils::{
    create_table_and_load_snapshot, test_table_setup, test_table_setup_mt, write_batch_to_table,
};

fn simple_schema() -> SchemaRef {
    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap(),
    )
}

fn committer() -> Box<FileSystemCommitter> {
    Box::new(FileSystemCommitter::new())
}

// ============================================================================
// Add column tests
// ============================================================================

#[rstest]
#[case::one_column(1)]
#[case::three_columns(3)]
#[tokio::test]
async fn add_columns_reload_snapshot_verify_schema(
    #[case] num_columns: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &[])?;

    // Write two rows with only the original columns populated.
    let batch = RecordBatch::try_new(
        Arc::new(simple_schema().as_ref().try_into_arrow().unwrap()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    let new_col_names: Vec<String> = (0..num_columns).map(|i| format!("col_{i}")).collect();

    // First add_column transitions Ready -> Modifying; subsequent calls stay in Modifying.
    let (first, rest) = new_col_names.split_first().unwrap();
    let committed = rest
        .iter()
        .fold(
            snapshot
                .alter_table()
                .add_column(StructField::nullable(first, DataType::STRING)),
            |b, name| b.add_column(StructField::nullable(name, DataType::STRING)),
        )
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Verify post-commit snapshot has evolved schema
    let post_snap = committed
        .post_commit_snapshot()
        .expect("post-commit snapshot");
    assert_eq!(post_snap.schema().fields().count(), 2 + num_columns);

    // Reload from storage to verify persistence
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), 2);
    let schema = reloaded.schema();
    assert_eq!(schema.fields().count(), 2 + num_columns);
    for name in &new_col_names {
        let field = schema.field(name).expect("added field should exist");
        assert_eq!(field.data_type(), &DataType::STRING);
        assert!(field.is_nullable());
    }

    // Scan back -- old rows should have NULL for every new column.
    let evolved_arrow_schema: delta_kernel::arrow::datatypes::SchemaRef =
        Arc::new(reloaded.schema().as_ref().try_into_arrow().unwrap());
    let scan = reloaded.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
    assert_eq!(batches[0].num_columns(), 2 + num_columns);
    for name in &new_col_names {
        let col = batches[0].column_by_name(name).expect("new column");
        assert_eq!(col.null_count(), col.len());
    }

    // Write two more rows with all columns populated.
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let mut new_arrays: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int32Array::from(vec![3, 4])),
        Arc::new(StringArray::from(vec!["c", "d"])),
    ];
    for _ in 0..num_columns {
        new_arrays.push(Arc::new(StringArray::from(vec!["new_c", "new_d"])));
    }
    let batch2 = RecordBatch::try_new(evolved_arrow_schema, new_arrays).unwrap();
    let reloaded = Arc::new(reloaded);
    let _ = write_batch_to_table(&reloaded, engine.as_ref(), batch2, HashMap::new()).await?;

    // Scan back -- 4 rows total, each new column has 2 NULLs (old rows) and 2 values (new rows).
    let final_snap = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(final_snap.version(), 3);
    let scan = final_snap.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
    for name in &new_col_names {
        let null_count: usize = batches
            .iter()
            .map(|b| b.column_by_name(name).expect("new column").null_count())
            .sum();
        assert_eq!(null_count, 2, "column {name} should have 2 NULLs");
    }

    Ok(())
}

/// Adding columns of complex types (struct, array, map) on a non-CM table. Verifies
#[rstest]
#[case::struct_column(
    StructField::nullable(
        "address",
        StructType::try_new(vec![
            StructField::nullable("city", DataType::STRING),
            StructField::nullable("zip", DataType::STRING),
        ]).unwrap(),
    )
)]
#[case::array_of_primitive(StructField::nullable("tags", ArrayType::new(DataType::STRING, true),))]
#[case::map_of_primitives(StructField::nullable(
    "labels",
    MapType::new(DataType::STRING, DataType::INTEGER, true),
))]
#[tokio::test]
async fn add_complex_type_column(#[case] field: StructField) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &[])?;
    let field_name = field.name().to_string();
    let expected_type = field.data_type().clone();

    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    let added = schema.field(&field_name).expect("added field should exist");
    assert_eq!(added.data_type(), &expected_type);
    Ok(())
}

#[rstest]
#[case::duplicate_column(&[], StructField::nullable("name", DataType::STRING), "already exists")]
#[case::duplicate_column_case_insensitive(
    &[],
    StructField::nullable("NAME", DataType::STRING),
    "already exists"
)]
#[case::timestamp_ntz_without_feature(
    &[],
    StructField::nullable("ts", DataType::TIMESTAMP_NTZ),
    "timestampNtz"
)]
#[case::non_nullable(&[], StructField::not_null("age", DataType::INTEGER), "non-nullable")]
#[case::column_mapping_enabled(
    &[("delta.columnMapping.mode", "name")],
    StructField::nullable("email", DataType::STRING),
    "column mapping"
)]
#[tokio::test]
async fn add_column_failures(
    #[case] properties: &[(&str, &str)],
    #[case] field: StructField,
    #[case] error_contains: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), properties)?;

    let err = snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer());
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains(error_contains));

    Ok(())
}

/// Back-to-back alters with a checkpoint in between, then a write against the evolved schema.
/// Exercises: create (v0) → alter add A (v1) → checkpoint at v1 → alter add B (v2) → write
/// row with values in both new columns (v3) → reload. The reload must rebuild the snapshot
/// from the checkpoint + alter commits + data commit and return the written values.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn back_to_back_alters_with_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    // Checkpoint writing requires the multi-threaded engine (like `maintenance_ops.rs`).
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;

    // v0: create.
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &[])?;

    // v1: add column "a".
    let v1 = snapshot
        .alter_table()
        .add_column(StructField::nullable("a", DataType::STRING))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let v1_snap = v1
        .post_commit_snapshot()
        .expect("post-commit snapshot at v1");

    // Checkpoint at v1.
    let (_, v1_ckpt) = v1_snap.clone().checkpoint(engine.as_ref())?;

    // v2: add column "b" on top of the checkpointed snapshot.
    let v2 = v1_ckpt
        .alter_table()
        .add_column(StructField::nullable("b", DataType::INTEGER))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let v2_snap = v2
        .post_commit_snapshot()
        .expect("post-commit snapshot at v2");

    // v3: write one row populating both new columns.
    let evolved_arrow_schema: delta_kernel::arrow::datatypes::SchemaRef =
        Arc::new(v2_snap.schema().as_ref().try_into_arrow().unwrap());
    let batch = RecordBatch::try_new(
        evolved_arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["alice"])),
            Arc::new(StringArray::from(vec!["val_a"])),
            Arc::new(delta_kernel::arrow::array::Int32Array::from(vec![100])),
        ],
    )
    .unwrap();
    write_batch_to_table(v2_snap, engine.as_ref(), batch, HashMap::new()).await?;

    // Reload from scratch: kernel must rebuild from checkpoint + alter commits + data commit.
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), 3);
    let schema = reloaded.schema();
    assert!(
        schema.field("a").is_some(),
        "column added at v1 must survive checkpoint"
    );
    assert!(
        schema.field("b").is_some(),
        "column added at v2 must be present"
    );

    // Scan the row back and verify values in both newly-added columns.
    let scan = reloaded.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
    let a_col = batches[0]
        .column_by_name("a")
        .expect("column a")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a is string");
    assert_eq!(a_col.value(0), "val_a");
    let b_col = batches[0]
        .column_by_name("b")
        .expect("column b")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("b is int");
    assert_eq!(b_col.value(0), 100);

    Ok(())
}
