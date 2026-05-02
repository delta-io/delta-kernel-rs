//! Integration tests for ALTER TABLE schema evolution.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int32Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::{column_name, ColumnName, Scalar};
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, SchemaRef, StructField,
    StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
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

fn max_column_id(snap: &Snapshot) -> i64 {
    snap.table_configuration()
        .metadata()
        .configuration()
        .get("delta.columnMapping.maxColumnId")
        .and_then(|v| v.parse().ok())
        .expect("maxColumnId should be set and parseable on a CM table")
}

// ============================================================================
// Add column tests
// ============================================================================

/// End-to-end lifecycle: write, ALTER to add columns, scan, write populated rows, scan again.
/// Each column is added in its own alter commit with a checkpoint after, exercising
/// "do some ops -> checkpoint -> do more ops -> checkpoint". Under CM, also verifies fresh
/// ids/physical names and that `maxColumnId` advanced.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_columns_lifecycle(
    #[values(None, Some("name"), Some("id"))] cm_mode: Option<&str>,
    #[values(1, 3)] num_columns: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let properties: Vec<(&str, &str)> = cm_mode
        .map(|m| vec![("delta.columnMapping.mode", m)])
        .unwrap_or_default();
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &properties)?;
    let original_max_id = cm_mode.map(|_| max_column_id(&snapshot));

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

    // One alter+checkpoint cycle per column.
    let mut current = snapshot;
    for name in &new_col_names {
        let committed = current
            .alter_table()
            .add_column(StructField::nullable(name, DataType::STRING))
            .build(engine.as_ref(), committer())?
            .commit(engine.as_ref())?
            .unwrap_committed();
        let post = committed
            .post_commit_snapshot()
            .expect("post-commit snapshot");
        let (_, ckpt) = post.clone().checkpoint(engine.as_ref(), None)?;
        current = ckpt;
    }

    // Reload from storage to verify persistence. v0 = create, v1 = write, then `num_columns`
    // alter commits.
    let alter_end_version = 1 + num_columns as u64;
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), alter_end_version);
    let schema = reloaded.schema();
    assert_eq!(schema.fields().count(), 2 + num_columns);
    for name in &new_col_names {
        let field = schema.field(name).expect("added field should exist");
        assert_eq!(field.data_type(), &DataType::STRING);
        assert!(field.is_nullable());
    }

    // When CM is enabled: each new column must have a fresh id/physical name, and the
    // table's maxColumnId must have advanced past the original value. When CM is disabled:
    // the property must remain absent.
    if let Some(orig) = original_max_id {
        for name in &new_col_names {
            let field = schema.field(name).unwrap();
            let cm_id = field.column_mapping_id().expect("CM id should be assigned");
            assert!(
                cm_id > orig,
                "new column '{name}' id {cm_id} must exceed original max {orig}"
            );
            match field
                .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
                .expect("physical name should be assigned")
            {
                MetadataValue::String(s) => assert!(s.starts_with("col-")),
                other => panic!("expected String, got {other:?}"),
            }
        }
        assert!(max_column_id(&reloaded) > orig);
    } else {
        assert!(reloaded
            .table_configuration()
            .metadata()
            .configuration()
            .get("delta.columnMapping.maxColumnId")
            .is_none());
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
    assert_eq!(final_snap.version(), alter_end_version + 1);
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

/// Adding columns of complex types (struct, array, map) -- with and without column mapping.
/// Verifies the data type round-trips and, under CM, that every reachable struct field
/// receives a distinct fresh ID and `maxColumnId` advances accordingly. `expected_id_count`
/// is the number of CM IDs the column should receive (1 for the parent + however many inner
/// struct fields the recursion reaches).
#[rstest]
#[case::struct_column(
    StructField::nullable(
        "address",
        StructType::try_new(vec![
            StructField::nullable("city", DataType::STRING),
            StructField::nullable("zip", DataType::STRING),
        ]).unwrap(),
    ),
    3,
)]
#[case::array_of_primitive(
    StructField::nullable("tags", ArrayType::new(DataType::STRING, true)),
    1
)]
#[case::map_of_primitives(
    StructField::nullable("labels", MapType::new(DataType::STRING, DataType::INTEGER, true)),
    1
)]
#[case::array_of_struct(
    StructField::nullable(
        "items",
        ArrayType::new(
            DataType::Struct(Box::new(
                StructType::try_new(vec![
                    StructField::nullable("a", DataType::STRING),
                    StructField::nullable("b", DataType::INTEGER),
                ]).unwrap(),
            )),
            true,
        ),
    ),
    3,
)]
#[case::map_value_is_struct(
    StructField::nullable(
        "by_id",
        MapType::new(
            DataType::STRING,
            DataType::Struct(Box::new(
                StructType::try_new(vec![
                    StructField::nullable("a", DataType::STRING),
                    StructField::nullable("b", DataType::INTEGER),
                ]).unwrap(),
            )),
            true,
        ),
    ),
    3,
)]
#[case::map_key_is_struct(
    StructField::nullable(
        "lookup",
        MapType::new(
            DataType::Struct(Box::new(
                StructType::try_new(vec![
                    StructField::nullable("a", DataType::STRING),
                    StructField::nullable("b", DataType::INTEGER),
                ]).unwrap(),
            )),
            DataType::INTEGER,
            true,
        ),
    ),
    3,
)]
#[tokio::test]
async fn add_complex_type_column(
    #[case] field: StructField,
    #[case] expected_id_count: usize,
    #[values(None, Some("name"), Some("id"))] cm_mode: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let properties: Vec<(&str, &str)> = cm_mode
        .map(|m| vec![("delta.columnMapping.mode", m)])
        .unwrap_or_default();
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &properties)?;
    let original_max_id = cm_mode.map(|_| max_column_id(&snapshot));

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

    if let Some(orig_max) = original_max_id {
        // Under CM, inner struct fields carry CM metadata that `expected_type` doesn't;
        // strict DataType equality won't hold. The ID-count check below implicitly verifies
        // that the type structure round-tripped correctly.
        let ids = added.collect_column_mapping_ids();
        let unique: HashSet<_> = ids.iter().copied().collect();
        assert_eq!(ids.len(), expected_id_count, "expected ID count mismatch");
        assert_eq!(unique.len(), ids.len(), "all assigned IDs must be distinct");
        assert!(
            ids.iter().all(|&id| id > orig_max),
            "all assigned IDs must exceed original max"
        );
        assert_eq!(
            max_column_id(&reloaded),
            ids.iter().copied().max().unwrap(),
            "table maxColumnId must equal the largest assigned ID",
        );
    } else {
        assert_eq!(added.data_type(), &expected_type);
    }
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
    let (_, v1_ckpt) = v1_snap.clone().checkpoint(engine.as_ref(), None)?;

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

// ============================================================================
// SET NULLABLE tests
// ============================================================================

/// Cross-product: 3 schema/column cases x 3 CM modes (off, name, id).
#[rstest]
#[case::already_nullable(simple_schema(), column_name!("name"))]
#[case::required_top_level(
    Arc::new(StructType::try_new(vec![
        StructField::not_null("id", DataType::INTEGER),
        StructField::nullable("name", DataType::STRING),
    ]).unwrap()),
    column_name!("id")
)]
#[case::required_nested(
    Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "address",
            StructType::try_new(vec![
                StructField::not_null("city", DataType::STRING),
                StructField::nullable("zip", DataType::STRING),
            ]).unwrap(),
        ),
    ]).unwrap()),
    column_name!("address.city")
)]
#[tokio::test]
async fn set_nullable_succeeds(
    #[case] schema: SchemaRef,
    #[case] column: ColumnName,
    #[values(None, Some("name"), Some("id"))] cm_mode: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let properties: Vec<(&str, &str)> = cm_mode
        .map(|m| vec![("delta.columnMapping.mode", m)])
        .unwrap_or_default();
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), &properties)?;
    // Snapshot the field before the alter so we can prove set_nullable changes only the
    // nullable bit -- preserving name, data type, and ALL metadata (including column-mapping
    // id and physical name when CM is enabled).
    let before = snapshot.schema().field_at_path(column.path()).clone();

    snapshot
        .alter_table()
        .set_nullable(column.clone())
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    let reloaded_schema = reloaded.schema();
    let after = reloaded_schema.field_at_path(column.path());
    assert!(after.is_nullable());
    assert_eq!(after.name(), before.name());
    assert_eq!(after.data_type(), before.data_type());
    assert_eq!(
        after.metadata(),
        before.metadata(),
        "field metadata (incl. column mapping id/physical name) must be preserved"
    );
    Ok(())
}

/// End-to-end: create a table with a non-null layout column (partition or clustering),
/// write a row, flip the layout column to nullable, checkpoint, reload from scratch, scan.
/// Cross-product: layout kind (partitioned, clustered) x column-mapping mode (off, name, id).
#[rstest]
#[case::partition("date", DataLayout::partitioned(["date"]), "2026-01-01")]
#[case::clustered("region", DataLayout::clustered(["region"]), "us")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_nullable_on_layout_column_with_checkpoint(
    #[case] col_name: &str,
    #[case] layout: DataLayout,
    #[case] col_value: &str,
    #[values(None, Some("name"), Some("id"))] cm_mode: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    // Partition values live in the directory path; clustering values live in the row batch.
    let is_partitioned = matches!(layout, DataLayout::Partitioned { .. });

    // v0: create the table with the layout column as non-null.
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::not_null(col_name, DataType::STRING),
    ])?);
    let properties: Vec<(&str, &str)> = cm_mode
        .map(|m| vec![("delta.columnMapping.mode", m)])
        .unwrap_or_default();
    create_table(&table_path, schema.clone(), "Test/1.0")
        .with_data_layout(layout)
        .with_table_properties(properties)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let v0 = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(!v0.schema().field(col_name).unwrap().is_nullable());

    // v1: write a single row.
    let v0_arc = Arc::new(v0);
    let v1 = if is_partitioned {
        // Partition cols are excluded from the row batch and passed via partition_values.
        let nonpartition_arrow_schema: delta_kernel::arrow::datatypes::SchemaRef =
            Arc::new(delta_kernel::arrow::datatypes::Schema::new(vec![
                delta_kernel::arrow::datatypes::Field::new(
                    "id",
                    delta_kernel::arrow::datatypes::DataType::Int32,
                    true,
                ),
            ]));
        let batch = RecordBatch::try_new(
            nonpartition_arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1]))],
        )?;
        let mut partition_values = HashMap::new();
        partition_values.insert(col_name.to_string(), Scalar::String(col_value.to_string()));
        write_batch_to_table(&v0_arc, engine.as_ref(), batch, partition_values).await?
    } else {
        // Clustering cols are regular columns; partition_values is empty.
        let arrow_schema: delta_kernel::arrow::datatypes::SchemaRef =
            Arc::new(schema.as_ref().try_into_arrow().unwrap());
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![col_value])),
            ],
        )?;
        write_batch_to_table(&v0_arc, engine.as_ref(), batch, HashMap::new()).await?
    };

    // v2: ALTER TABLE -- set the layout column nullable.
    let v2 = v1
        .alter_table()
        .set_nullable(ColumnName::new([col_name]))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let v2_snap = v2
        .post_commit_snapshot()
        .expect("post-commit snapshot at v2");
    assert!(v2_snap.schema().field(col_name).unwrap().is_nullable());

    // Checkpoint at v2 so reload exercises the checkpoint path.
    v2_snap.clone().checkpoint(engine.as_ref(), None)?;

    // Reload from scratch and verify the schema and row survive.
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), 2);
    assert!(reloaded.schema().field(col_name).unwrap().is_nullable());

    let scan = reloaded.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
    let col = batches[0]
        .column_by_name(col_name)
        .expect("layout column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("layout column is string");
    assert_eq!(col.value(0), col_value);
    Ok(())
}

#[tokio::test]
async fn set_nullable_nonexistent_column_fails() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &[])?;

    let err = snapshot
        .alter_table()
        .set_nullable(column_name!("nonexistent"))
        .build(engine.as_ref(), committer());
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("does not exist"));

    Ok(())
}

// ============================================================================
// CHAIN tests
// ============================================================================

/// Alternating chain: ADD COLUMN, SET NULLABLE, ADD COLUMN, SET NULLABLE. Verifies that
/// chaining mixed ops applies them in order and produces the expected final schema. Each
/// SET NULLABLE flips a still-NOT-NULL column from the original schema. Under CM, also
/// verifies existing fields' column mapping IDs are preserved by set_nullable while
/// add_column receives a new CM ID and bumps maxColumnId.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_add_column_and_set_nullable(
    #[values(None, Some("name"), Some("id"))] cm_mode: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::not_null("id", DataType::INTEGER),
        StructField::not_null("name", DataType::STRING),
    ])?);
    let properties: Vec<(&str, &str)> = cm_mode
        .map(|m| vec![("delta.columnMapping.mode", m)])
        .unwrap_or_default();
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), &properties)?;

    let original_id_cm_id = cm_mode.map(|_| {
        snapshot
            .schema()
            .field("id")
            .unwrap()
            .column_mapping_id()
            .expect("existing field should already have a column mapping ID")
    });
    let original_max_id = cm_mode.map(|_| max_column_id(&snapshot));

    // Two alter+checkpoint cycles: (add email + nullable id), (add age + nullable name).
    let v1 = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .set_nullable(column_name!("id"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let v1_snap = v1
        .post_commit_snapshot()
        .expect("post-commit snapshot at v1");
    let (_, v1_ckpt) = v1_snap.clone().checkpoint(engine.as_ref(), None)?;
    let v2 = v1_ckpt
        .alter_table()
        .add_column(StructField::nullable("age", DataType::INTEGER))
        .set_nullable(column_name!("name"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let v2_snap = v2
        .post_commit_snapshot()
        .expect("post-commit snapshot at v2");
    v2_snap.clone().checkpoint(engine.as_ref(), None)?;

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    assert_eq!(schema.fields().count(), 4);
    for name in ["id", "name", "email", "age"] {
        let field = schema.field(name).expect("field should be present");
        assert!(field.is_nullable(), "field '{name}' should be nullable");
    }

    if let (Some(orig_id), Some(orig_max)) = (original_id_cm_id, original_max_id) {
        for added in ["email", "age"] {
            assert!(
                schema.field(added).unwrap().column_mapping_id().is_some(),
                "added field '{added}' should have a column mapping ID"
            );
        }
        let id_after = schema
            .field("id")
            .unwrap()
            .column_mapping_id()
            .expect("existing id column mapping");
        assert_eq!(id_after, orig_id, "existing id CM id must not change");
        assert!(
            max_column_id(&reloaded) > orig_max,
            "chained add_column must bump maxColumnId"
        );
    }

    Ok(())
}

fn field_with_stray_cm_id(name: &str, ty: DataType) -> StructField {
    let mut f = StructField::nullable(name, ty);
    f.metadata.insert(
        ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
        MetadataValue::Number(99),
    );
    f
}

/// On a non-CM table, `apply_schema_operations` doesn't reject stray CM metadata up front --
/// `StructType::make_physical` (run from `TableConfiguration::try_new_with_schema`) is the
/// gate. This test locks in that downstream rejection, including for nested annotations.
#[rstest]
#[case::top_level(field_with_stray_cm_id("tainted", DataType::STRING))]
#[case::nested_in_struct(StructField::nullable(
    "outer",
    StructType::try_new(vec![field_with_stray_cm_id("inner", DataType::STRING)]).unwrap(),
))]
#[tokio::test]
async fn add_column_with_stray_cm_metadata_on_non_cm_table_fails(
    #[case] field: StructField,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &[])?;

    let err = snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("column mapping") || msg.contains("columnMapping"),
        "error should mention column mapping, got: {msg}"
    );
    Ok(())
}

/// Chain `add_column + drop_column + set_nullable` end-to-end on a column-mapped table,
/// split across two alter+checkpoint cycles. Verifies the chained ops compose across
/// checkpoint boundaries, the dropped column is removed, the added column gets a fresh CM id,
/// and the existing column's CM id is preserved through both checkpoints.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_add_drop_set_nullable_on_cm_table(
    #[values("name", "id")] cm_mode: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", cm_mode)],
    )?;
    let original_max_id = max_column_id(&snapshot);
    let original_id_cm_id = snapshot
        .schema()
        .field("id")
        .unwrap()
        .column_mapping_id()
        .expect("existing field should already have a column mapping ID");

    // Two alter+checkpoint cycles: (add email + drop name), then (set_nullable id).
    let v1 = snapshot
        .alter_table()
        .add_column(StructField::nullable("email", DataType::STRING))
        .drop_column(column_name!("name"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let v1_snap = v1
        .post_commit_snapshot()
        .expect("post-commit snapshot at v1");
    let (_, v1_ckpt) = v1_snap.clone().checkpoint(engine.as_ref(), None)?;
    let v2 = v1_ckpt
        .alter_table()
        .set_nullable(column_name!("id"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let v2_snap = v2
        .post_commit_snapshot()
        .expect("post-commit snapshot at v2");
    v2_snap.clone().checkpoint(engine.as_ref(), None)?;

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    assert_eq!(schema.fields().count(), 2, "name dropped, email added");
    assert!(schema.field("name").is_none());
    assert!(schema.field("email").is_some());
    assert!(schema.field("id").unwrap().is_nullable());
    assert!(
        max_column_id(&reloaded) > original_max_id,
        "add_column must bump maxColumnId even when chained with drop"
    );
    let id_after = schema
        .field("id")
        .unwrap()
        .column_mapping_id()
        .expect("existing id column mapping");
    assert_eq!(
        id_after, original_id_cm_id,
        "existing id CM id must not change"
    );

    Ok(())
}

/// Edge case: `add(foo) + drop(foo)` in one ALTER. The final schema matches the initial
/// schema, but `maxColumnId` still advances
#[tokio::test]
async fn chain_add_then_drop_same_column_burns_id_but_no_schema_change() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;
    let original_max_id = max_column_id(&snapshot);
    let original_field_count = snapshot.schema().fields().count();

    snapshot
        .alter_table()
        .add_column(StructField::nullable("foo", DataType::STRING))
        .drop_column(column_name!("foo"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    assert_eq!(
        schema.fields().count(),
        original_field_count,
        "net schema unchanged"
    );
    assert!(schema.field("foo").is_none(), "foo should not be present");
    assert!(
        max_column_id(&reloaded) > original_max_id,
        "maxColumnId must still advance (IDs never reused)"
    );

    Ok(())
}

// ============================================================================
// DROP COLUMN tests
// ============================================================================

/// End-to-end lifecycle: create a CM table, write rows, then drop columns one at a time with
/// a checkpoint after each drop ("do some ops -> checkpoint -> do more ops -> checkpoint").
/// Reload from scratch must rebuild from each checkpoint plus the subsequent commits and:
/// surface only the surviving columns in the schema and scan output (no value bleed-through
/// from physical Parquet storage), preserve remaining row data, and leave `maxColumnId`
/// unchanged (drops never bump it). Also verifies that time travel back to the pre-drop
/// version still surfaces the dropped columns with their original values (drops do not
/// retroactively rewrite earlier versions).
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_column_lifecycle_removes_column_and_preserves_remaining_data(
    #[values("name", "id")] cm_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("name", DataType::STRING),
        StructField::nullable("email", DataType::STRING),
        StructField::nullable("age", DataType::INTEGER),
    ])?);
    let properties = vec![("delta.columnMapping.mode", cm_mode)];
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &properties)?;
    let original_max_id = max_column_id(&snapshot);

    let batch = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow().unwrap()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["a@x", "b@x"])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    // One alter+checkpoint cycle per drop.
    let mut current = snapshot;
    for col in ["email", "age"] {
        let committed = current
            .alter_table()
            .drop_column(ColumnName::new([col]))
            .build(engine.as_ref(), committer())?
            .commit(engine.as_ref())?
            .unwrap_committed();
        let post = committed
            .post_commit_snapshot()
            .expect("post-commit snapshot");
        let (_, ckpt) = post.clone().checkpoint(engine.as_ref(), None)?;
        current = ckpt;
    }

    // Schema: dropped columns gone, `maxColumnId` unchanged across both drops.
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    assert_eq!(schema.fields().count(), 2);
    assert!(schema.field("email").is_none());
    assert!(schema.field("age").is_none());
    assert!(schema.field("id").is_some());
    assert!(schema.field("name").is_some());
    assert_eq!(max_column_id(&reloaded), original_max_id);

    // Scan output excludes the dropped columns; remaining values survive.
    let scan = reloaded.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
    assert_eq!(batches[0].num_columns(), 2);
    for dropped in ["email", "age"] {
        assert!(
            batches[0].column_by_name(dropped).is_none(),
            "dropped column '{dropped}' must not appear in scan output"
        );
    }
    let id_col = batches[0]
        .column_by_name("id")
        .expect("id column")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id is int");
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(1), 2);

    // Time travel: at v1 (post-write, pre-drop) all 4 columns are still in the schema with
    // their original values. Drops must not retroactively rewrite earlier versions.
    let pre_drop = Snapshot::builder_for(&table_path)
        .at_version(1)
        .build(engine.as_ref())?;
    assert_eq!(pre_drop.schema().fields().count(), 4);
    let scan = pre_drop.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
    let email_col = batches[0]
        .column_by_name("email")
        .expect("v1 must surface 'email'")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("email is string");
    assert_eq!(email_col.value(0), "a@x");
    assert_eq!(email_col.value(1), "b@x");
    let age_col = batches[0]
        .column_by_name("age")
        .expect("v1 must surface 'age'")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("age is int");
    assert_eq!(age_col.value(0), 10);
    assert_eq!(age_col.value(1), 20);

    Ok(())
}

/// Dropping a non-clustering column on a clustered CM table is allowed; the companion
/// "dropping the clustering column itself" case is covered by `drop_column_failures`.
#[tokio::test]
async fn drop_non_clustering_column_on_clustered_table_succeeds() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let committed = create_table(&table_path, simple_schema(), "Test/1.0")
        .with_data_layout(DataLayout::clustered(["name"]))
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot = committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone();

    snapshot
        .alter_table()
        .drop_column(column_name!("id"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(reloaded.schema().field("id").is_none());
    Ok(())
}

/// Dropping a top-level column whose `DataType` is non-primitive (struct, Array, Map, or a
/// complex type containing a Struct) succeeds and removes only that column. The matching
/// failure case for structs -- dropping a struct that is an ancestor of a clustering column
/// -- is covered by `drop_column_failures::clustering_ancestor`.
#[rstest]
#[case::top_level_struct(StructField::nullable(
    "address",
    StructType::try_new(vec![
        StructField::nullable("city", DataType::STRING),
        StructField::nullable("zip", DataType::STRING),
    ])
    .unwrap(),
))]
#[case::array_of_primitive(StructField::nullable("tags", ArrayType::new(DataType::STRING, true)))]
#[case::map_of_primitives(StructField::nullable(
    "labels",
    MapType::new(DataType::STRING, DataType::INTEGER, true),
))]
#[case::array_of_struct(StructField::nullable(
    "items",
    ArrayType::new(
        DataType::Struct(Box::new(
            StructType::try_new(vec![
                StructField::nullable("a", DataType::STRING),
                StructField::nullable("b", DataType::INTEGER),
            ])
            .unwrap(),
        )),
        true,
    ),
))]
#[case::map_value_is_struct(StructField::nullable(
    "by_id",
    MapType::new(
        DataType::STRING,
        DataType::Struct(Box::new(
            StructType::try_new(vec![
                StructField::nullable("a", DataType::STRING),
                StructField::nullable("b", DataType::INTEGER),
            ])
            .unwrap(),
        )),
        true,
    ),
))]
#[tokio::test]
async fn drop_complex_type_column(#[case] field: StructField) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let field_name = field.name().to_string();
    // Schema is simple_schema() + the complex column, so dropping it leaves a non-empty schema.
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("name", DataType::STRING),
        field,
    ])?);
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;
    let original_max_id = max_column_id(&snapshot);

    snapshot
        .alter_table()
        .drop_column(ColumnName::new([field_name.as_str()]))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    assert_eq!(schema.fields().count(), 2);
    assert!(schema.field(&field_name).is_none());
    assert!(schema.field("id").is_some());
    assert!(schema.field("name").is_some());
    assert_eq!(
        max_column_id(&reloaded),
        original_max_id,
        "drop must not bump maxColumnId, including for inner fields lost with a struct"
    );
    Ok(())
}

/// Setup-flavor selector for `drop_column_failures`. Each variant creates a distinct table
/// (CM mode, schema shape, data layout) so one rstest can exercise every drop-failure path.
enum DropFailureFlavor {
    /// Plain non-CM table; any drop fails because CM is required.
    NoCm,
    /// CM table with `simple_schema()`; used for drops of non-existent columns.
    Cm,
    /// CM table with a single-column schema; used for the "last field" rejection.
    CmSingleColumn,
    /// CM table partitioned by `name`; used for the partition-column rejection.
    CmPartitionedByName,
    /// CM table clustered by top-level `name`; used for the clustering-column rejection.
    CmClusteredByName,
    /// CM table clustered by nested `address.city`; used for the clustering-ancestor rejection.
    CmClusteredByNestedCity,
}

fn setup_for_drop_failure(
    table_path: &str,
    engine: &dyn delta_kernel::Engine,
    flavor: DropFailureFlavor,
) -> DeltaResult<Arc<Snapshot>> {
    let cm = [("delta.columnMapping.mode", "name")];
    Ok(match flavor {
        DropFailureFlavor::NoCm => {
            create_table_and_load_snapshot(table_path, simple_schema(), engine, &[])?
        }
        DropFailureFlavor::Cm => {
            create_table_and_load_snapshot(table_path, simple_schema(), engine, &cm)?
        }
        DropFailureFlavor::CmSingleColumn => {
            let schema = Arc::new(
                StructType::try_new(vec![StructField::nullable("only", DataType::STRING)]).unwrap(),
            );
            create_table_and_load_snapshot(table_path, schema, engine, &cm)?
        }
        DropFailureFlavor::CmPartitionedByName => {
            let committed = create_table(table_path, simple_schema(), "Test/1.0")
                .with_data_layout(DataLayout::partitioned(["name"]))
                .with_table_properties(cm.to_vec())
                .build(engine, committer())?
                .commit(engine)?
                .unwrap_committed();
            committed
                .post_commit_snapshot()
                .expect("post-commit snapshot")
                .clone()
        }
        DropFailureFlavor::CmClusteredByName => {
            let committed = create_table(table_path, simple_schema(), "Test/1.0")
                .with_data_layout(DataLayout::clustered(["name"]))
                .with_table_properties(cm.to_vec())
                .build(engine, committer())?
                .commit(engine)?
                .unwrap_committed();
            committed
                .post_commit_snapshot()
                .expect("post-commit snapshot")
                .clone()
        }
        DropFailureFlavor::CmClusteredByNestedCity => {
            let nested = Arc::new(
                StructType::try_new(vec![
                    StructField::nullable("id", DataType::INTEGER),
                    StructField::nullable(
                        "address",
                        StructType::try_new(vec![
                            StructField::nullable("city", DataType::STRING),
                            StructField::nullable("zip", DataType::STRING),
                        ])
                        .unwrap(),
                    ),
                ])
                .unwrap(),
            );
            let committed = create_table(table_path, nested, "Test/1.0")
                .with_data_layout(DataLayout::Clustered {
                    columns: vec![column_name!("address.city")],
                })
                .with_table_properties(cm.to_vec())
                .build(engine, committer())?
                .commit(engine)?
                .unwrap_committed();
            committed
                .post_commit_snapshot()
                .expect("post-commit snapshot")
                .clone()
        }
    })
}

#[rstest]
#[case::without_cm(DropFailureFlavor::NoCm, column_name!("name"), "column mapping")]
#[case::nonexistent(DropFailureFlavor::Cm, column_name!("nonexistent"), "does not exist")]
#[case::last_remaining(
    DropFailureFlavor::CmSingleColumn,
    column_name!("only"),
    "last field"
)]
#[case::partition_column(
    DropFailureFlavor::CmPartitionedByName,
    column_name!("name"),
    "partition column"
)]
#[case::clustering_column(
    DropFailureFlavor::CmClusteredByName,
    column_name!("name"),
    "clustering column"
)]
#[case::clustering_ancestor(
    DropFailureFlavor::CmClusteredByNestedCity,
    column_name!("address"),
    "clustering column"
)]
#[tokio::test]
async fn drop_column_failures(
    #[case] flavor: DropFailureFlavor,
    #[case] drop_column: delta_kernel::expressions::ColumnName,
    #[case] error_contains: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = setup_for_drop_failure(&table_path, engine.as_ref(), flavor)?;

    let err = snapshot
        .alter_table()
        .drop_column(drop_column)
        .build(engine.as_ref(), committer());
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains(error_contains));

    Ok(())
}
