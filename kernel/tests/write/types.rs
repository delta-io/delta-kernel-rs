//! Integration tests for type-specific writes (timestampNtz, variant, shredded variant).

use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BinaryArray, Int32Array, StructArray, TimestampMicrosecondArray,
};
use delta_kernel::arrow::buffer::NullBuffer;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::{TryFromKernel, TryIntoArrow as _};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::parquet::DefaultParquetHandler;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{Engine, Error as KernelError, Snapshot};
use itertools::Itertools;
use serde_json::Deserializer;
use tempfile::tempdir;
use test_utils::{create_table, engine_store_setup, test_read};
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

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
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

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_data_change(true);

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

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_data_change(true);

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
