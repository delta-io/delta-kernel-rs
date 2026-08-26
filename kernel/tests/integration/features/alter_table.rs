//! Integration tests for ALTER TABLE schema evolution.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int32Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::{column_name, ColumnName, Scalar};
use delta_kernel::schema::{
    schema, schema_ref, try_schema, ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue,
    SchemaRef, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use rstest::rstest;
use test_utils::{
    add_commit, column_mapping_fixtures as fixtures, create_table as create_test_table,
    create_table_and_load_snapshot, engine_store_setup, test_table_setup, test_table_setup_mt,
    write_batch_to_table,
};

fn simple_schema() -> SchemaRef {
    schema_ref! {
        nullable "id": INTEGER,
        nullable "name": STRING,
    }
}

fn committer() -> Box<FileSystemCommitter> {
    Box::new(FileSystemCommitter::new())
}

/// Reads `delta.columnMapping.maxColumnId` from the snapshot's metadata. Returns
/// `None` when the property is absent (e.g. non-CM tables) so callers can compare it
/// directly against `cm_mode.map(...)`-style expectations. CM-only callers should
/// `.expect(...)` at the use site to surface the protocol violation explicitly.
fn max_column_id(snap: &Snapshot) -> Option<i64> {
    snap.table_configuration()
        .metadata()
        .configuration()
        .get("delta.columnMapping.maxColumnId")
        .and_then(|v| v.parse().ok())
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
    let original_max_id =
        cm_mode.map(|_| max_column_id(&snapshot).expect("CM table must have maxColumnId"));

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
        assert!(max_column_id(&reloaded).expect("CM table must have maxColumnId") > orig);
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
        schema! {
            nullable "city": STRING,
            nullable "zip": STRING,
        },
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
            schema! {
                nullable "a": STRING,
                nullable "b": INTEGER,
            },
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
            schema! {
                nullable "a": STRING,
                nullable "b": INTEGER,
            },
            true,
        ),
    ),
    3,
)]
#[case::map_key_is_struct(
    StructField::nullable(
        "lookup",
        MapType::new(
            schema! {
                nullable "a": STRING,
                nullable "b": INTEGER,
            },
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
    let original_max_id =
        cm_mode.map(|_| max_column_id(&snapshot).expect("CM table must have maxColumnId"));

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
            max_column_id(&reloaded).expect("CM table must have maxColumnId"),
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
// Add column at tests
// ============================================================================

#[rstest]
#[tokio::test]
async fn add_column_at_round_trip(
    #[values(None, Some("name"), Some("id"))] cm_mode: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! {
        nullable "parent": {
            nullable "existing": INTEGER,
        },
    };
    let properties: Vec<(&str, &str)> = cm_mode
        .map(|mode| vec![("delta.columnMapping.mode", mode)])
        .unwrap_or_default();
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), &properties)?;

    snapshot
        .alter_table()
        .add_column_at(
            column_name!("parent"),
            StructField::nullable("added", DataType::STRING),
        )
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let reloaded_schema = reloaded.schema();
    let parent = reloaded_schema.field("parent").expect("parent must exist");
    let DataType::Struct(parent) = parent.data_type() else {
        panic!("parent must remain a struct")
    };
    assert!(parent.field("existing").is_some());

    let added = parent.field("added").expect("added field must exist");
    assert_eq!(added.data_type(), &DataType::STRING);
    assert!(added.is_nullable());
    assert_eq!(added.column_mapping_id().is_some(), cm_mode.is_some());
    assert_eq!(
        added
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .is_some(),
        cm_mode.is_some()
    );

    Ok(())
}

/// Nested `add_column_at` on a column-mapped table: commit + reload must persist the nested
/// field's CM id / physical name and advance `maxColumnId`.
#[rstest]
#[tokio::test]
async fn add_column_at_nested_struct_with_column_mapping(
    #[values("name", "id")] cm_mode: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "address",
            StructType::try_new(vec![StructField::nullable("city", DataType::STRING)])?,
        ),
    ])?);
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", cm_mode)],
    )?;
    let original_max = max_column_id(&snapshot).expect("CM table must have maxColumnId");

    snapshot
        .alter_table()
        .add_column_at(
            column_name!("address"),
            StructField::nullable("zip", DataType::STRING),
        )
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let reloaded_schema = reloaded.schema();
    let zip = reloaded_schema.field_at_path(&["address".to_string(), "zip".to_string()]);
    let cm_id = zip
        .column_mapping_id()
        .expect("nested field must receive a column mapping id");
    assert!(
        cm_id > original_max,
        "nested field id {cm_id} must exceed original max {original_max}"
    );
    match zip
        .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
        .expect("physical name should be assigned")
    {
        MetadataValue::String(s) => assert!(s.starts_with("col-")),
        other => panic!("expected String, got {other:?}"),
    }
    assert_eq!(
        max_column_id(&reloaded).expect("CM table must have maxColumnId"),
        cm_id,
        "table maxColumnId must equal the nested field's assigned id",
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn add_column_at_containers_round_trip(
    #[values(None, Some("name"), Some("id"))] cm_mode: Option<&str>,
) -> DeltaResult<()> {
    fn target_struct<'a>(schema: &'a StructType, path: &ColumnName) -> &'a StructType {
        let (first, rest) = path.path().split_first().expect("non-empty path");
        let mut data_type = schema
            .field(first)
            .unwrap_or_else(|| panic!("field '{first}' not found"))
            .data_type();
        for segment in rest {
            data_type = match (segment.as_str(), data_type) {
                ("element", DataType::Array(array)) => array.element_type(),
                ("key", DataType::Map(map)) => map.key_type(),
                ("value", DataType::Map(map)) => map.value_type(),
                (name, DataType::Struct(parent)) => parent
                    .field(name)
                    .unwrap_or_else(|| panic!("field '{name}' not found"))
                    .data_type(),
                (segment, data_type) => {
                    panic!("path segment '{segment}' does not match {data_type}")
                }
            };
        }
        let DataType::Struct(target) = data_type else {
            panic!("path '{path}' does not resolve to a struct")
        };
        target
    }

    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! {
        nullable "items": [ nullable {
            nullable "existing": STRING,
        } ],
        nullable "by_key": {
            { nullable "existing": STRING } => nullable INTEGER
        },
        nullable "by_value": {
            INTEGER => nullable {
                nullable "existing": STRING,
            }
        },
    };
    let properties: Vec<(&str, &str)> = cm_mode
        .map(|mode| vec![("delta.columnMapping.mode", mode)])
        .unwrap_or_default();
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), &properties)?;

    snapshot
        .alter_table()
        .add_column_at(
            column_name!("items.element"),
            StructField::nullable("added", DataType::STRING),
        )
        .add_column_at(
            column_name!("by_key.key"),
            StructField::nullable("added", DataType::STRING),
        )
        .add_column_at(
            column_name!("by_value.value"),
            StructField::nullable("added", DataType::STRING),
        )
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let reloaded_schema = reloaded.schema();
    for path in [
        column_name!("items.element"),
        column_name!("by_key.key"),
        column_name!("by_value.value"),
    ] {
        let target = target_struct(reloaded_schema.as_ref(), &path);
        assert!(target.field("existing").is_some());

        let added = target.field("added").expect("added field must exist");
        assert_eq!(added.data_type(), &DataType::STRING);
        assert!(added.is_nullable());
        assert_eq!(added.column_mapping_id().is_some(), cm_mode.is_some());
        assert_eq!(
            added
                .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
                .is_some(),
            cm_mode.is_some()
        );
    }

    Ok(())
}

/// Empty-schema tables are valid intermediate state, so they need to behave normally
/// once a column is added. This test runs the full lifecycle (create empty, ALTER ADD
/// COLUMN, write rows, scan them back, time-travel to v0) across all column-mapping
/// modes so the CM bookkeeping (maxColumnId, column-mapping id, physical name) is
/// exercised on a schema that started empty rather than being created with columns
/// up front.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_create_then_add_column(
    #[values(None, Some("name"), Some("id"))] cm_mode: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let properties: Vec<(&str, &str)> = cm_mode
        .map(|m| vec![("delta.columnMapping.mode", m)])
        .unwrap_or_default();

    let empty_schema = schema_ref! {};
    let v0 =
        create_table_and_load_snapshot(&table_path, empty_schema, engine.as_ref(), &properties)?;
    assert_eq!(v0.version(), 0);
    assert_eq!(v0.schema().num_fields(), 0);
    assert_eq!(max_column_id(&v0), cm_mode.map(|_| 0));

    // Scans and blind appends against the empty-schema snapshot are blocked with
    // friendly errors that point users at ALTER TABLE ADD COLUMN.
    let scan_err = v0
        .clone()
        .scan_builder()
        .build()
        .expect_err("scan_builder().build() must reject empty-schema snapshots");
    assert!(
        scan_err.to_string().contains("empty schema")
            && scan_err.to_string().contains("ALTER TABLE ADD COLUMN"),
        "scan error must point at ALTER TABLE ADD COLUMN, got: {scan_err}"
    );
    let write_state_err = v0
        .clone()
        .transaction(committer(), engine.as_ref())?
        .with_engine_info("EmptySchemaApp/0.1.0")
        .write_state()
        .expect_err("write_state() must reject empty-schema snapshots");
    assert!(
        write_state_err.to_string().contains("empty schema")
            && write_state_err.to_string().contains("alter_table"),
        "write_state error must point at alter_table, got: {write_state_err}"
    );

    v0.alter_table()
        .add_column(StructField::nullable("id", DataType::INTEGER))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let v1 = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(v1.version(), 1);
    let schema = v1.schema();
    assert_eq!(schema.num_fields(), 1);
    let id_field = schema.field("id").expect("added field should exist");
    assert_eq!(id_field.data_type(), &DataType::INTEGER);
    assert!(id_field.is_nullable());
    assert_eq!(max_column_id(&v1), cm_mode.map(|_| 1));
    assert_eq!(id_field.column_mapping_id(), cm_mode.map(|_| 1i64));
    assert_eq!(
        id_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .is_some(),
        cm_mode.is_some(),
        "physical name presence must track column-mapping enablement",
    );

    // Write rows through the freshly-added column and confirm the read path round-trips
    // them. Under CM this also exercises the physical-name plumbing for a column whose
    // schema started empty.
    let arrow_schema = v1.schema().as_ref().try_into_arrow().unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(99)]))],
    )
    .unwrap();
    let v2 = write_batch_to_table(&v1, engine.as_ref(), batch, HashMap::new())
        .await
        .map_err(|e| delta_kernel::Error::generic(format!("write_batch_to_table failed: {e}")))?;
    assert_eq!(v2.version(), 2);

    let scan = v2.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "post-ALTER write should yield 3 rows on read"
    );

    // The empty-schema snapshot must still be loadable via time-travel after later
    // commits introduce a column and data files; pins the contract that the v0 history
    // survives schema evolution.
    let v0_after = Snapshot::builder_for(&table_path)
        .at_version(0)
        .build(engine.as_ref())?;
    assert_eq!(v0_after.version(), 0);
    assert_eq!(v0_after.schema().num_fields(), 0);

    Ok(())
}

// ============================================================================
// SET NULLABLE tests
// ============================================================================

/// Cross-product: 3 schema/column cases x 3 CM modes (off, name, id).
#[rstest]
#[case::already_nullable(simple_schema(), column_name!("name"))]
#[case::required_top_level(
    schema_ref! {
        not_null "id": INTEGER,
        nullable "name": STRING,
    },
    column_name!("id")
)]
#[case::required_nested(
    schema_ref! {
        nullable "id": INTEGER,
        nullable "address": {
            not_null "city": STRING,
            nullable "zip": STRING,
        },
    },
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
    let schema = Arc::new(try_schema! {
        nullable "id": INTEGER,
        not_null (col_name): STRING,
    }?);
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
    let schema = schema_ref! {
        not_null "id": INTEGER,
        not_null "name": STRING,
    };
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
    let original_max_id =
        cm_mode.map(|_| max_column_id(&snapshot).expect("CM table must have maxColumnId"));

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
            max_column_id(&reloaded).expect("CM table must have maxColumnId") > orig_max,
            "chained add_column must bump maxColumnId"
        );
    }

    Ok(())
}

fn field_with_stray_key(name: &str, key: &ColumnMetadataKey, ty: DataType) -> StructField {
    let mut f = StructField::nullable(name, ty);
    f.metadata
        .insert(key.as_ref().to_string(), MetadataValue::Number(99));
    f
}

/// On a clean non-CM table, an ALTER that adds a column carrying stray CM metadata has that
/// metadata stripped (the commit introduces it into a previously-clean table), rather than
/// rejected -- matching delta-spark. Parametrized over each detected key and over placement
/// (top-level vs nested in a struct).
#[rstest]
#[case::top_level(false)]
#[case::nested_in_struct(true)]
#[tokio::test]
async fn add_column_with_stray_cm_metadata_on_non_cm_table_is_stripped(
    #[case] nested: bool,
    #[values(
        ColumnMetadataKey::ColumnMappingId,
        ColumnMetadataKey::ColumnMappingPhysicalName,
        ColumnMetadataKey::ColumnMappingNestedIds,
        ColumnMetadataKey::ParquetFieldId,
        ColumnMetadataKey::ParquetFieldNestedIds
    )]
    key: ColumnMetadataKey,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &[])?;

    let (field, stripped_path): (StructField, Vec<String>) = if nested {
        let outer = StructField::nullable(
            "outer",
            schema! {
                (field_with_stray_key("inner", &key, DataType::STRING)),
            },
        );
        (outer, vec!["outer".to_string(), "inner".to_string()])
    } else {
        (
            field_with_stray_key("tainted", &key, DataType::STRING),
            vec!["tainted".to_string()],
        )
    };

    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Reload from disk so we assert on the persisted schemaString, not the in-memory config.
    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let reloaded_schema = reloaded.schema();
    let leaf = reloaded_schema.field_at_path(&stripped_path);
    assert!(
        leaf.get_config_value(&key).is_none(),
        "stray {} at {stripped_path:?} should be stripped",
        key.as_ref()
    );
    Ok(())
}

/// Caller-supplied column-mapping metadata is removed before mode-specific handling. Mapping modes
/// assign fresh metadata, while `None` mode leaves the added field unannotated.
#[rstest]
#[case::none(ColumnMappingMode::None, &[])]
#[case::id(ColumnMappingMode::Id, &[("delta.columnMapping.mode", "id")])]
#[case::name(ColumnMappingMode::Name, &[("delta.columnMapping.mode", "name")])]
#[tokio::test]
async fn add_column_replaces_supplied_cm_metadata_in_all_modes(
    #[case] expected_mode: ColumnMappingMode,
    #[case] properties: &[(&str, &str)],
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), properties)?;
    assert_eq!(
        snapshot.table_configuration().column_mapping_mode(),
        expected_mode
    );

    let field = fixtures::cm_field("added", 99, "phys-added", DataType::STRING);
    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let added = reloaded.schema().field("added").unwrap().clone();
    match expected_mode {
        ColumnMappingMode::None => {
            assert!(added.column_mapping_id().is_none());
            assert!(added
                .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
                .is_none());
        }
        ColumnMappingMode::Id | ColumnMappingMode::Name => {
            assert_ne!(added.column_mapping_id(), Some(99));
            assert_ne!(physical_name_for_field(&added), "phys-added");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_blocked_when_iceberg_compat_v3_enabled() -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.enableIcebergCompatV3", "true")],
    )?;

    let msg = snapshot
        .alter_table()
        .add_column(StructField::nullable("new_col", DataType::STRING))
        .build(engine.as_ref(), committer())
        .unwrap_err()
        .to_string();
    assert!(
        msg.contains("ALTER TABLE is not yet supported on tables with icebergCompatV3 enabled"),
        "unexpected error: {msg}",
    );

    Ok(())
}

#[tokio::test]
async fn add_column_with_orphan_default_metadata_succeeds() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, simple_schema(), engine.as_ref(), &[])?;
    let field = StructField::nullable("new_col", DataType::INTEGER).add_metadata([(
        ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
        MetadataValue::String("42".to_string()),
    )]);

    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let reloaded_schema = reloaded.schema();
    let default = reloaded_schema
        .field("new_col")
        .expect("new_col must exist after ALTER")
        .column_default()?
        .expect("CURRENT_DEFAULT metadata must survive ALTER");
    assert_eq!(default.raw_sql(), "42");

    let txn = reloaded.transaction(committer(), engine.as_ref())?;
    assert!(
        txn.top_level_column_defaults()?.is_empty(),
        "default metadata must remain inert without allowColumnDefaults",
    );

    Ok(())
}

#[tokio::test]
async fn alter_blocked_when_allow_column_defaults_enabled() -> Result<(), Box<dyn std::error::Error>>
{
    let (store, engine, table_url) = engine_store_setup("alter_column_defaults", None);
    let table_url = create_test_table(
        store,
        table_url,
        simple_schema(),
        &[],
        true,
        vec![],
        vec!["allowColumnDefaults"],
    )
    .await?;
    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;

    let msg = snapshot
        .alter_table()
        .add_column(StructField::nullable("new_col", DataType::STRING))
        .build(&engine, committer())
        .unwrap_err()
        .to_string();
    assert!(
        msg.contains("ALTER TABLE is not yet supported on tables with allowColumnDefaults enabled"),
        "unexpected error: {msg}",
    );

    Ok(())
}

// ============================================================================
// ALTER TABLE ADD COLUMN replaces pre-populated column mapping metadata.
// ============================================================================

fn cm_id_for_field(field: &StructField) -> i64 {
    field
        .column_mapping_id()
        .expect("field must have a column mapping id")
}

fn physical_name_for_field(field: &StructField) -> &str {
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
        Some(MetadataValue::String(s)) => s.as_str(),
        other => panic!("expected physicalName string, got {other:?}"),
    }
}

/// ADD COLUMN replaces a supplied column-mapping ID and physical name with a fresh identity.
#[rstest]
#[tokio::test]
async fn add_column_replaces_complete_cm_metadata(
    #[values("name", "id")] cm_mode: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", cm_mode)],
    )?;
    let original_max = max_column_id(&snapshot).expect("CM table must have maxColumnId");

    let supplied_id = original_max + 100;
    let field = fixtures::cm_field(
        "replaced",
        supplied_id,
        "user-supplied-physical",
        DataType::STRING,
    );

    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    let added = schema.field("replaced").unwrap();
    assert_eq!(cm_id_for_field(added), original_max + 1);
    assert_ne!(physical_name_for_field(added), "user-supplied-physical");
    assert!(physical_name_for_field(added).starts_with("col-"));
    assert_eq!(
        max_column_id(&reloaded).expect("CM table must have maxColumnId"),
        original_max + 1
    );
    Ok(())
}

/// ADD COLUMN replaces a supplied physical name and allocates a fresh ID.
#[rstest]
#[tokio::test]
async fn add_column_replaces_supplied_physical_name(
    #[values("name", "id")] cm_mode: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", cm_mode)],
    )?;
    let original_max = max_column_id(&snapshot).expect("CM table must have maxColumnId");

    let field =
        fixtures::cm_field_physical_name_only("named_only", "phys-named-only", DataType::STRING);

    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    let added = schema.field("named_only").unwrap();
    assert_eq!(cm_id_for_field(added), original_max + 1);
    assert_ne!(physical_name_for_field(added), "phys-named-only");
    assert!(physical_name_for_field(added).starts_with("col-"));
    assert_eq!(
        max_column_id(&reloaded).expect("CM table must have maxColumnId"),
        original_max + 1
    );
    Ok(())
}

/// ADD COLUMN replaces a supplied ID and allocates a fresh physical name.
#[rstest]
#[tokio::test]
async fn add_column_replaces_supplied_id(#[values("name", "id")] cm_mode: &str) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", cm_mode)],
    )?;
    let original_max = max_column_id(&snapshot).expect("CM table must have maxColumnId");
    let supplied_id = original_max + 7;

    let field = fixtures::cm_field_id_only("id_only", supplied_id, DataType::STRING);

    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    let added = schema.field("id_only").unwrap();
    assert_eq!(cm_id_for_field(added), original_max + 1);
    assert_ne!(cm_id_for_field(added), supplied_id);
    assert!(
        physical_name_for_field(added).starts_with("col-"),
        "physical name should be allocated as col-<uuid>, got {}",
        physical_name_for_field(added)
    );
    assert_eq!(
        max_column_id(&reloaded).expect("CM table must have maxColumnId"),
        original_max + 1
    );
    Ok(())
}

/// ADD COLUMN replaces a supplied ID below `maxColumnId` with the next fresh identity.
#[tokio::test]
async fn add_column_replaces_supplied_id_below_max_column_id() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Pre-populate the table with sparse ids (1, 100) using the create-table preserve path.
    let schema = schema_ref! {
        (fixtures::cm_field("a", 1, "phys-a", DataType::INTEGER)),
        (fixtures::cm_field("b", 100, "phys-b", DataType::STRING)),
    };
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;
    assert_eq!(
        max_column_id(&snapshot).expect("CM table must have maxColumnId"),
        100
    );

    let field = fixtures::cm_field("inserted_below_max", 50, "phys-inserted", DataType::STRING);

    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    let added = schema.field("inserted_below_max").unwrap();
    assert_eq!(cm_id_for_field(added), 101);
    assert_ne!(physical_name_for_field(added), "phys-inserted");
    assert!(physical_name_for_field(added).starts_with("col-"));
    assert_eq!(
        max_column_id(&reloaded).expect("CM table must have maxColumnId"),
        101
    );
    Ok(())
}

/// ADD COLUMN replaces a supplied ID that collides with an existing field's identity.
#[tokio::test]
async fn add_column_replaces_supplied_id_colliding_with_existing_field() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        simple_schema(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    // Pick an id that already exists in the simple_schema (1, 2 typically).
    let existing_id = snapshot
        .schema()
        .field("id")
        .unwrap()
        .column_mapping_id()
        .expect("simple_schema 'id' must have a CM id under name mode");

    let field = fixtures::cm_field("colliding", existing_id, "phys-colliding", DataType::STRING);

    snapshot
        .alter_table()
        .add_column(field)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let added = reloaded.schema().field("colliding").unwrap().clone();
    assert_ne!(cm_id_for_field(&added), existing_id);
    assert_ne!(physical_name_for_field(&added), "phys-colliding");
    Ok(())
}

/// On a mapping-disabled table with residual column-mapping metadata, ALTER preserves metadata on
/// existing fields but strips it from newly added fields.
#[rstest]
#[case::clean_column(StructField::nullable("added", DataType::STRING))]
#[case::introduced_stray(
    field_with_stray_key("added", &ColumnMetadataKey::ColumnMappingId, DataType::STRING)
)]
#[tokio::test]
async fn add_column_on_stale_table_preserves_existing_metadata(
    #[case] added_field: StructField,
) -> DeltaResult<()> {
    let (store, engine, table_url) = engine_store_setup("alter_stale_cm", None);

    // `value` carries a stale id; protocol omits columnMapping and no mode is set (resolves to
    // None) -- residual annotations already on the table.
    let stale_schema = schema! {
        nullable "id": INTEGER,
        (StructField::nullable("value", DataType::INTEGER)
            .add_metadata([("delta.columnMapping.id", MetadataValue::Number(2))])),
    };
    let escaped = serde_json::to_string(&serde_json::to_string(&stale_schema)?).unwrap();
    // v0 written directly to bypass create_table validation (which strips stale annotations).
    let v0 = format!(
        r#"{{"protocol":{{"minReaderVersion":1,"minWriterVersion":2}}}}
{{"metaData":{{"id":"alter-stale-cm","format":{{"provider":"parquet","options":{{}}}},"schemaString":{escaped},"partitionColumns":[],"configuration":{{}},"createdTime":1700000000000}}}}
"#
    );
    add_commit(table_url.as_str(), store.as_ref(), 0, v0)
        .await
        .unwrap();

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    snapshot
        .alter_table()
        .add_column(added_field)
        .build(&engine, committer())?
        .commit(&engine)?
        .unwrap_committed();

    // Reload from disk so we assert on the persisted schemaString, not the in-memory config.
    let reloaded = Snapshot::builder_for(table_url).build(&engine)?;
    let schema = reloaded.schema();

    // The pre-existing annotation on the untouched `value` field survives verbatim.
    assert_eq!(schema.field("value").unwrap().column_mapping_id(), Some(2));
    assert!(schema.field("added").unwrap().column_mapping_id().is_none());
    Ok(())
}
