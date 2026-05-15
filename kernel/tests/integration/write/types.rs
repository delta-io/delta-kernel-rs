//! Integration tests for type-specific writes (timestampNtz, variant, shredded variant).

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    new_null_array, ArrayRef, BinaryArray, Int32Array, StructArray, TimestampMicrosecondArray,
};
use delta_kernel::arrow::buffer::NullBuffer;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::{TryFromKernel, TryIntoArrow as _};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::parquet::DefaultParquetHandler;
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{ArrayType, DataType, MapType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::{Engine, Error as KernelError, Snapshot};
use itertools::Itertools;
use rstest::rstest;
use serde_json::Deserializer;
use tempfile::tempdir;
use test_utils::{
    begin_transaction, create_add_files_metadata, create_table, engine_store_setup, test_read,
    test_table_setup,
};
use url::Url;

#[tokio::test]
async fn test_append_timestamp_ntz() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a table with TIMESTAMP_NTZ column
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "ts_ntz",
        DataType::TIMESTAMP_NTZ,
    )])?);

    let (store, engine, table_location) = engine_store_setup("test_table_timestamp_ntz", None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec!["timestampNtz"],
        vec!["timestampNtz"],
    )
    .await?;

    let mut txn = test_utils::load_and_begin_transaction(table_url.clone(), &engine)?
        .with_engine_info("default engine");

    // Create Arrow data with TIMESTAMP_NTZ values including edge cases
    // These are microseconds since Unix epoch
    let timestamp_values = vec![
        0i64,                  // Unix epoch (1970-01-01T00:00:00.000000)
        1634567890123456i64,   // 2021-10-18T12:31:30.123456
        1634567950654321i64,   // 2021-10-18T12:32:30.654321
        1672531200000000i64,   // 2023-01-01T00:00:00.000000
        253402300799999999i64, // 9999-12-31T23:59:59.999999 (near max valid timestamp)
        -62135596800000000i64, // 0001-01-01T00:00:00.000000 (near min valid timestamp)
    ];

    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(TimestampMicrosecondArray::from(timestamp_values))],
    )?;

    // Write data
    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());

    let add_files_metadata = engine
        .write_parquet(&ArrowEngineData::new(data.clone()), write_context.as_ref())
        .await?;

    txn.add_files(add_files_metadata);

    // Commit the transaction
    assert!(txn.commit(engine.as_ref())?.is_committed());

    // Verify the commit was written correctly
    let commit1 = store
        .get(&Path::from(
            "/test_table_timestamp_ntz/_delta_log/00000000000000000001.json",
        ))
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Check that we have the expected number of commits (commitInfo + add)
    assert_eq!(parsed_commits.len(), 2);

    // Check that the add action exists
    assert!(parsed_commits[1].get("add").is_some());
    // Ensure default of data change is true.
    assert!(parsed_commits[1]
        .get("add")
        .unwrap()
        .get("dataChange")
        .unwrap()
        .as_bool()
        .unwrap());

    // Verify the data can be read back correctly
    test_read(&ArrowEngineData::new(data), &table_url, engine)?;

    Ok(())
}

#[tokio::test]
async fn test_append_variant() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    fn unshredded_variant_schema_flipped() -> DataType {
        DataType::variant_type([
            StructField::not_null("value", DataType::BINARY),
            StructField::not_null("metadata", DataType::BINARY),
        ])
        .unwrap()
    }
    fn variant_arrow_type_flipped() -> ArrowDataType {
        let metadata_field = Field::new("metadata", ArrowDataType::Binary, false);
        let value_field = Field::new("value", ArrowDataType::Binary, false);
        let fields = vec![value_field, metadata_field];
        ArrowDataType::Struct(fields.into())
    }

    // create a table with VARIANT column
    let table_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("v", DataType::unshredded_variant()),
        StructField::nullable("i", DataType::INTEGER),
        StructField::nullable(
            "nested",
            // We flip the value and metadata fields in the actual parquet file for the test
            StructType::try_new(vec![StructField::nullable(
                "nested_v",
                unshredded_variant_schema_flipped(),
            )])?,
        ),
    ])?);

    let write_schema = table_schema.clone();

    let tmp_test_dir = tempdir()?;
    let tmp_test_dir_url = Url::from_directory_path(tmp_test_dir.path()).unwrap();

    let (store, engine, table_location) =
        engine_store_setup("test_table_variant", Some(&tmp_test_dir_url));

    // We can add shredding features as well as we are allowed to write unshredded variants
    // into shredded tables and shredded reads are explicitly blocked in the default
    // engine's parquet reader.
    let table_url = create_table(
        store.clone(),
        table_location,
        table_schema.clone(),
        &[],
        true,
        vec!["variantType", "variantShredding-preview"],
        vec!["variantType", "variantShredding-preview"],
    )
    .await?;

    let mut txn =
        test_utils::load_and_begin_transaction(table_url.clone(), &engine)?.with_data_change(true);

    // First value corresponds to the variant value "1". Third value corresponds to the variant
    // representing the JSON Object {"a":2}.
    let metadata_v = vec![
        Some(&[0x01, 0x00, 0x00][..]),
        None,
        Some(&[0x01, 0x01, 0x00, 0x01, 0x61][..]),
    ];
    let value_v = vec![
        Some(&[0x0C, 0x01][..]),
        None,
        Some(&[0x02, 0x01, 0x00, 0x00, 0x01, 0x02][..]),
    ];

    let metadata_v_array = Arc::new(BinaryArray::from(metadata_v)) as ArrayRef;
    let value_v_array = Arc::new(BinaryArray::from(value_v)) as ArrayRef;

    // First value corresponds to the variant value "2". Third value corresponds to the variant
    // representing the JSON Object {"b":3}.
    let metadata_nested_v = vec![
        Some(&[0x01, 0x00, 0x00][..]),
        None,
        Some(&[0x01, 0x01, 0x00, 0x01, 0x62][..]),
    ];
    let value_nested_v = vec![
        Some(&[0x0C, 0x02][..]),
        None,
        Some(&[0x02, 0x01, 0x00, 0x00, 0x01, 0x03][..]),
    ];

    let value_nested_v_array = Arc::new(BinaryArray::from(value_nested_v)) as ArrayRef;
    let metadata_nested_v_array = Arc::new(BinaryArray::from(metadata_nested_v)) as ArrayRef;

    let variant_arrow = ArrowDataType::try_from_kernel(&DataType::unshredded_variant()).unwrap();
    let variant_arrow_flipped = variant_arrow_type_flipped();

    let i_values = vec![31, 32, 33];

    let fields = match variant_arrow {
        ArrowDataType::Struct(fields) => Ok(fields),
        _ => Err(KernelError::Generic(
            "Variant arrow data type is not struct.".to_string(),
        )),
    }?;
    let fields_flipped = match variant_arrow_flipped {
        ArrowDataType::Struct(fields) => Ok(fields),
        _ => Err(KernelError::Generic(
            "Variant arrow data type is not struct.".to_string(),
        )),
    }?;

    let null_bitmap = NullBuffer::from_iter([true, false, true]);

    let variant_v_array = StructArray::try_new(
        fields.clone(),
        vec![metadata_v_array, value_v_array],
        Some(null_bitmap.clone()),
    )?;

    let variant_nested_v_array = Arc::new(StructArray::try_new(
        fields_flipped.clone(),
        vec![
            value_nested_v_array.clone(),
            metadata_nested_v_array.clone(),
        ],
        Some(null_bitmap.clone()),
    )?);

    let data = RecordBatch::try_new(
        Arc::new(write_schema.as_ref().try_into_arrow()?),
        vec![
            // v variant
            Arc::new(variant_v_array.clone()),
            // i int
            Arc::new(Int32Array::from(i_values.clone())),
            // nested struct<nested_v variant>
            Arc::new(StructArray::try_new(
                vec![Field::new("nested_v", variant_arrow_type_flipped(), true)].into(),
                vec![variant_nested_v_array.clone()],
                None,
            )?),
        ],
    )
    .unwrap();

    // Write data
    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());

    let add_files_metadata = (*engine)
        .parquet_handler()
        .as_any()
        .downcast_ref::<DefaultParquetHandler<TokioBackgroundExecutor>>()
        .unwrap()
        .write_parquet_file(Box::new(ArrowEngineData::new(data.clone())), &write_context)
        .await?;

    txn.add_files(add_files_metadata);

    // Commit the transaction
    assert!(txn.commit(engine.as_ref())?.is_committed());

    // Verify the commit was written correctly
    let commit1_url = tmp_test_dir_url
        .join("test_table_variant/_delta_log/00000000000000000001.json")
        .unwrap();
    let commit1 = store
        .get(&Path::from_url_path(commit1_url.path()).unwrap())
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Check that we have the expected number of commits (commitInfo + add)
    assert_eq!(parsed_commits.len(), 2);

    // Check that the add action exists
    assert!(parsed_commits[1].get("add").is_some());

    // The scanned data will match the logical schema, not the physical one
    let expected_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("v", DataType::unshredded_variant()),
        StructField::nullable("i", DataType::INTEGER),
        StructField::nullable(
            "nested",
            StructType::try_new(vec![StructField::nullable(
                "nested_v",
                DataType::unshredded_variant(),
            )])
            .unwrap(),
        ),
    ])?);

    // During the read, the flipped fields should be reordered into metadata, value.
    let variant_nested_v_array_expected = Arc::new(StructArray::try_new(
        fields,
        vec![metadata_nested_v_array, value_nested_v_array],
        Some(null_bitmap),
    )?);
    let variant_arrow_type: ArrowDataType =
        ArrowDataType::try_from_kernel(&DataType::unshredded_variant()).unwrap();
    let expected_data = RecordBatch::try_new(
        Arc::new(expected_schema.as_ref().try_into_arrow()?),
        vec![
            // v variant
            Arc::new(variant_v_array),
            // i int
            Arc::new(Int32Array::from(i_values)),
            // nested struct<nested_v variant>
            Arc::new(StructArray::try_new(
                vec![Field::new("nested_v", variant_arrow_type, true)].into(),
                vec![variant_nested_v_array_expected],
                None,
            )?),
        ],
    )
    .unwrap();

    test_read(&ArrowEngineData::new(expected_data), &table_url, engine)?;

    Ok(())
}

#[tokio::test]
async fn test_shredded_variant_read_rejection() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure that shredded variants are rejected by the default engine's parquet reader

    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    let table_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "v",
        DataType::unshredded_variant(),
    )])?);

    // The table will be attempted to be written in this form but be read into
    // STRUCT<metadata: BINARY, value: BINARY>. The read should fail because the default engine
    // currently does not support shredded reads.
    let shredded_write_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "v",
        DataType::try_struct_type([
            StructField::new("metadata", DataType::BINARY, true),
            StructField::new("value", DataType::BINARY, true),
            StructField::new("typed_value", DataType::INTEGER, true),
        ])?,
    )])?);

    let tmp_test_dir = tempdir()?;
    let tmp_test_dir_url = Url::from_directory_path(tmp_test_dir.path()).unwrap();

    let (store, engine, table_location) =
        engine_store_setup("test_table_variant_2", Some(&tmp_test_dir_url));
    let table_url = create_table(
        store.clone(),
        table_location,
        table_schema.clone(),
        &[],
        true,
        vec!["variantType", "variantShredding-preview"],
        vec!["variantType", "variantShredding-preview"],
    )
    .await?;

    let mut txn =
        test_utils::load_and_begin_transaction(table_url.clone(), &engine)?.with_data_change(true);

    // First value corresponds to the variant value "1". Third value corresponds to the variant
    // representing the JSON Object {"a":2}.
    let metadata_v = vec![
        Some(&[0x01, 0x00, 0x00][..]),
        Some(&[0x01, 0x01, 0x00, 0x01, 0x61][..]),
    ];
    let value_v = vec![
        Some(&[0x0C, 0x01][..]),
        Some(&[0x02, 0x01, 0x00, 0x00, 0x01, 0x02][..]),
    ];
    let typed_value_v = vec![Some(21), Some(3)];

    let metadata_v_array = Arc::new(BinaryArray::from(metadata_v)) as ArrayRef;
    let value_v_array = Arc::new(BinaryArray::from(value_v)) as ArrayRef;
    let typed_value_v_array = Arc::new(Int32Array::from(typed_value_v)) as ArrayRef;

    let variant_arrow = ArrowDataType::Struct(
        vec![
            Field::new("metadata", ArrowDataType::Binary, true),
            Field::new("value", ArrowDataType::Binary, true),
            Field::new("typed_value", ArrowDataType::Int32, true),
        ]
        .into(),
    );

    let fields = match variant_arrow {
        ArrowDataType::Struct(fields) => Ok(fields),
        _ => Err(KernelError::Generic(
            "Variant arrow data type is not struct.".to_string(),
        )),
    }?;

    let variant_v_array = StructArray::try_new(
        fields.clone(),
        vec![metadata_v_array, value_v_array, typed_value_v_array],
        None,
    )?;

    let data = RecordBatch::try_new(
        Arc::new(shredded_write_schema.as_ref().try_into_arrow()?),
        vec![
            // v variant
            Arc::new(variant_v_array.clone()),
        ],
    )
    .unwrap();

    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());

    let add_files_metadata = (*engine)
        .parquet_handler()
        .as_any()
        .downcast_ref::<DefaultParquetHandler<TokioBackgroundExecutor>>()
        .unwrap()
        .write_parquet_file(Box::new(ArrowEngineData::new(data.clone())), &write_context)
        .await?;

    txn.add_files(add_files_metadata);

    // Commit the transaction
    assert!(txn.commit(engine.as_ref())?.is_committed());

    // Verify the commit was written correctly
    let commit1_url = tmp_test_dir_url
        .join("test_table_variant_2/_delta_log/00000000000000000001.json")
        .unwrap();
    let commit1 = store
        .get(&Path::from_url_path(commit1_url.path()).unwrap())
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Check that we have the expected number of commits (commitInfo + add)
    assert_eq!(parsed_commits.len(), 2);

    // Check that the add action exists
    assert!(parsed_commits[1].get("add").is_some());

    let res = test_read(&ArrowEngineData::new(data), &table_url, engine);
    assert!(matches!(res,
        Err(e) if e.to_string().contains("The default engine does not support shredded reads")));

    Ok(())
}

// =============================================================================
// NOT NULL data column tests (default engine)
// =============================================================================
//
// These tests validate data-column nullability on the default engine. Kernel preserves
// `nullable: false` in the connector-facing schema and relies on the engine to reject
// nulls before writing.

/// Verifies the default-engine NOT NULL contract for non-partition columns
/// across a representative set of primitive types. The contract:
///
/// 1. `nullable: false` propagates from kernel's logical schema through the connector-facing
///    physical schema and into the Arrow schema engines build batches against.
/// 2. Constructing the Arrow batch an engine would hand to the kernel write API with a null in the
///    NOT NULL column fails at batch construction, before the engine is invoked.
///
/// See [#2465](https://github.com/delta-io/delta-kernel-rs/issues/2465).
#[rstest]
#[case::integer(DataType::INTEGER)]
#[case::long(DataType::LONG)]
#[case::string(DataType::STRING)]
#[case::binary(DataType::BINARY)]
#[case::boolean(DataType::BOOLEAN)]
#[case::timestamp(DataType::TIMESTAMP)]
#[case::decimal(DataType::decimal(10, 2).unwrap())]
#[tokio::test]
async fn test_not_null_data_column_rejects_null_in_batch(
    #[case] data_type: DataType,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Create a table with a NOT NULL column.
    let schema = Arc::new(StructType::try_new(vec![StructField::not_null(
        "c",
        data_type.clone(),
    )])?);
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let _ = kernel_create_table(&table_path, schema.clone(), "test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    // The non-null schema auto-enables the `invariants` writer feature.
    assert!(
        snapshot
            .table_configuration()
            .protocol()
            .writer_features()
            .is_some_and(|f| f.contains(&delta_kernel::table_features::TableFeature::Invariants)),
        "non-null schema must auto-enable the `invariants` writer feature",
    );

    let write_context = begin_transaction(snapshot, engine.as_ref())?
        .with_engine_info("default engine")
        .unpartitioned_write_context()?;

    // Use the connector-facing physical schema (not the logical one); the logical schema
    // would hide bugs in the logical->physical mapping that engines hit in production.
    let physical = write_context.physical_schema();
    let physical_field = physical
        .field("c")
        .expect("physical schema must contain column 'c'");
    assert!(
        !physical_field.nullable,
        "physical schema must preserve `nullable: false` for column 'c'",
    );
    let arrow_schema: delta_kernel::arrow::datatypes::Schema =
        physical.as_ref().try_into_arrow()?;
    assert!(
        !arrow_schema.field(0).is_nullable(),
        "Arrow conversion of physical schema must preserve `nullable: false`",
    );

    let arrow_type = arrow_schema.field(0).data_type().clone();
    let result = RecordBatch::try_new(Arc::new(arrow_schema), vec![new_null_array(&arrow_type, 1)]);
    assert!(
        result.is_err(),
        "batch construction should reject null in NOT NULL column ({data_type:?}); got: {result:?}",
    );

    Ok(())
}

// ---- Void type write-time validation tests ----
//
// The rstest cases below use `StructType::new_unchecked` for convenience; `StructType::try_new`
// would also accept them, since it validates structural properties (field-name uniqueness,
// metadata-column rules) and does not reject void placements. The validator
// (`validate_schema_for_write`) is the kernel-internal write-time rejection point for void in
// `Array`/`Map` and all-void structs, and it also protects schemas loaded from existing table
// metadata (JSON-deserialized from the log) and any `new_unchecked` paths.

/// Helper to create a table with a given schema and attempt a commit with dummy add_files.
/// Returns the commit error (panics if commit succeeds).
async fn try_write_with_void_schema(schema: SchemaRef) -> KernelError {
    let (store, engine, table_location) = engine_store_setup("void_write_test", None);
    let table_url = create_table(store, table_location, schema, &[], false, vec![], vec![])
        .await
        .expect("table creation should succeed");
    let engine = Arc::new(engine);
    let snapshot = Snapshot::builder_for(table_url)
        .build(engine.as_ref())
        .expect("snapshot should build");
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())
        .expect("transaction should create");

    // Add dummy file metadata to trigger write validation
    let add_schema = txn.add_files_schema().clone();
    let metadata =
        create_add_files_metadata(&add_schema, vec![("file.parquet", 100, 1000, Some(1))])
            .expect("metadata creation should succeed");
    txn.add_files(metadata);
    txn.commit(engine.as_ref())
        .expect_err("commit should fail for invalid void schema")
}

#[rstest]
#[case::void_array_element(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "arr",
            ArrayType::new(DataType::VOID, true),
        ),
    ])),
    "array element type"
)]
#[case::void_map_value(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "m",
            MapType::new(DataType::STRING, DataType::VOID, true),
        ),
    ])),
    "map value type"
)]
#[case::void_map_key(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "m",
            MapType::new(DataType::VOID, DataType::STRING, true),
        ),
    ])),
    "map key type"
)]
#[case::void_in_struct_in_array(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "arr",
            ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::VOID),
                ]))),
                true,
            ),
        ),
    ])),
    "Void type is not allowed inside"
)]
#[case::void_in_struct_in_map_value(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "m",
            MapType::new(
                DataType::STRING,
                StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::VOID),
                ]),
                true,
            ),
        ),
    ])),
    "Void type is not allowed inside"
)]
#[case::all_void_table(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("a", DataType::VOID),
        StructField::nullable("b", DataType::VOID),
    ])),
    "at least one non-void column"
)]
#[case::all_void_struct(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "s",
            StructType::new_unchecked([
                StructField::nullable("x", DataType::VOID),
                StructField::nullable("y", DataType::VOID),
            ]),
        ),
    ])),
    "contains no non-void fields"
)]
#[case::empty_struct_top_level(
    Arc::new(StructType::new_unchecked(Vec::<StructField>::new())),
    "at least one non-void column"
)]
#[case::nested_empty_struct(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "s",
            StructType::new_unchecked(Vec::<StructField>::new()),
        ),
    ])),
    "contains no non-void fields"
)]
#[case::empty_struct_in_array(
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "arr",
            ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked(Vec::<StructField>::new()))),
                true,
            ),
        ),
    ])),
    "struct nested in Array or Map must contain at least one non-void field"
)]
#[tokio::test]
async fn write_rejects_invalid_void_placement(
    #[case] schema: SchemaRef,
    #[case] expected_msg: &str,
) {
    let err = try_write_with_void_schema(schema).await;
    assert!(
        err.to_string().contains(expected_msg),
        "Expected error containing '{expected_msg}', got: {err}"
    );
}

// Validation must trigger when the connector requests a write context, before any Parquet is
// written. Both public entry points (partitioned and unpartitioned) are covered, and the
// `all_void_table` case is included because it is the shape where `strip_void_from_schema`
// would otherwise silently produce an empty physical schema -- so the fail-fast guard is most
// load-bearing there. The commit-time check (exercised by `write_rejects_invalid_void_placement`)
// remains as defense-in-depth.
#[rstest]
#[case::unpartitioned_void_array(
    "void_fail_fast_unpartitioned_array",
    vec![],
    HashMap::new(),
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("arr", ArrayType::new(DataType::VOID, true)),
    ])),
    "array element type",
)]
#[case::partitioned_void_array(
    "void_fail_fast_partitioned_array",
    vec!["id"],
    HashMap::from([("id".to_string(), Scalar::Integer(1))]),
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("arr", ArrayType::new(DataType::VOID, true)),
    ])),
    "array element type",
)]
#[case::unpartitioned_all_void(
    "void_fail_fast_unpartitioned_all_void",
    vec![],
    HashMap::new(),
    Arc::new(StructType::new_unchecked([
        StructField::nullable("a", DataType::VOID),
        StructField::nullable("b", DataType::VOID),
    ])),
    "at least one non-void column",
)]
#[tokio::test]
async fn write_context_creation_fails_fast_on_invalid_void_schema(
    #[case] test_name: &str,
    #[case] partition_columns: Vec<&str>,
    #[case] partition_values: HashMap<String, Scalar>,
    #[case] schema: SchemaRef,
    #[case] expected_msg: &str,
) {
    let (store, engine, table_location) = engine_store_setup(test_name, None);
    let table_url = create_table(
        store,
        table_location,
        schema,
        &partition_columns,
        false,
        vec![],
        vec![],
    )
    .await
    .expect("table creation should succeed");
    let engine = Arc::new(engine);
    let snapshot = Snapshot::builder_for(table_url)
        .build(engine.as_ref())
        .expect("snapshot should build");
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())
        .expect("transaction should create");

    let err = if partition_columns.is_empty() {
        txn.unpartitioned_write_context()
            .expect_err("unpartitioned_write_context should reject invalid void schema")
    } else {
        txn.partitioned_write_context(partition_values)
            .expect_err("partitioned_write_context should reject invalid void schema")
    };
    assert!(
        err.to_string().contains(expected_msg),
        "expected error containing '{expected_msg}', got: {err}"
    );
}

#[tokio::test]
async fn write_context_excludes_void_from_physical_schema() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("v", DataType::VOID),
        StructField::nullable("name", DataType::STRING),
    ]));
    let (store, engine, table_location) = engine_store_setup("void_physical_test", None);
    let table_url = create_table(store, table_location, schema, &[], false, vec![], vec![]).await?;
    let engine = Arc::new(engine);
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // Logical schema should contain void column
    {
        let logical = snapshot.schema();
        assert_eq!(logical.fields().count(), 3);
        assert!(logical.field("v").is_some());
    }

    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;

    let wc = txn.unpartitioned_write_context()?;
    let physical = wc.physical_schema();

    // Physical schema should NOT contain void column
    assert_eq!(physical.fields().count(), 2);
    assert!(physical.field("id").is_some());
    assert!(physical.field("name").is_some());
    assert!(physical.field("v").is_none());

    Ok(())
}

// Metadata-only operations should always succeed, even for schemas that are
// invalid for data writes (void-in-array, void-in-map, all-void structs).
#[tokio::test]
async fn metadata_only_commit_with_void_in_array_succeeds() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("arr", ArrayType::new(DataType::VOID, true)),
    ]));
    let (store, engine, table_location) = engine_store_setup("void_metadata_test", None);
    let table_url = create_table(store, table_location, schema, &[], false, vec![], vec![]).await?;
    let engine = Arc::new(engine);
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;

    // Commit with NO add_files — this is a metadata-only operation and should succeed
    let result = txn.commit(engine.as_ref());
    assert!(
        result.is_ok(),
        "Metadata-only commit on void-in-array schema should succeed, got: {:?}",
        result.err()
    );

    Ok(())
}

// Verify that nested void fields inside structs are excluded from the physical schema.
// Schema: {id: int, s: struct<a: int, b: void>}
// Physical: {id: int, s: struct<a: int>}
#[tokio::test]
async fn write_context_excludes_nested_void_from_physical_schema(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "s",
            StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]),
        ),
    ]));
    let (store, engine, table_location) = engine_store_setup("void_nested_physical_test", None);
    let table_url = create_table(store, table_location, schema, &[], false, vec![], vec![]).await?;
    let engine = Arc::new(engine);
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // Logical schema should contain the void field inside the struct
    {
        let logical = snapshot.schema();
        let s_field = logical.field("s").expect("s should exist");
        if let DataType::Struct(inner) = s_field.data_type() {
            assert!(
                inner.field("b").is_some(),
                "logical should have void field b"
            );
            assert_eq!(inner.fields().count(), 2);
        }
    }

    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let wc = txn.unpartitioned_write_context()?;
    let physical = wc.physical_schema();

    // Physical schema struct should NOT contain the void field
    let s_field = physical.field("s").expect("s should exist in physical");
    if let DataType::Struct(inner) = s_field.data_type() {
        assert_eq!(
            inner.fields().count(),
            1,
            "physical struct should have 1 field (void dropped)"
        );
        assert!(inner.field("a").is_some(), "non-void field should remain");
        assert!(inner.field("b").is_none(), "void field should be dropped");
    } else {
        panic!("s should be a struct type");
    }

    Ok(())
}

// Verify that the logical_to_physical transform drops nested void fields from structs.
// The transform expression should contain a nested transform that drops the void sub-field,
// matching the physical schema which has void stripped recursively.
#[tokio::test]
async fn write_transform_drops_nested_void_fields() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "s",
            StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]),
        ),
    ]));
    let (store, engine, table_location) = engine_store_setup("void_nested_transform_test", None);
    let table_url = create_table(store, table_location, schema, &[], false, vec![], vec![]).await?;
    let engine = Arc::new(engine);
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let wc = txn.unpartitioned_write_context()?;

    // The transform expression should mention dropping "b" inside the struct
    let l2p = wc.logical_to_physical();
    let expr_str = format!("{l2p}");
    assert!(
        expr_str.contains("drop b"),
        "Transform should drop nested void field 'b'. Expression: {expr_str}"
    );

    // Physical schema should also not contain void
    let physical = wc.physical_schema();
    let s_field = physical.field("s").expect("s should exist in physical");
    if let DataType::Struct(inner) = s_field.data_type() {
        assert_eq!(inner.fields().count(), 1);
        assert!(
            inner.field("b").is_none(),
            "void field b should be stripped"
        );
    } else {
        panic!("s should be struct");
    }

    Ok(())
}
