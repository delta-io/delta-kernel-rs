//! Integration tests for icebergCompat

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, MapArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::{TryFromKernel, TryIntoArrow as _};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::expressions::{ColumnName, Scalar};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::parquet::basic::Type as ParquetPhysicalType;
use delta_kernel::parquet::schema::types::Type as ParquetType;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transforms::{transform_output_type, SchemaTransform};
use test_utils::{
    add_commit, create_add_files_metadata, into_record_batch, read_add_infos, test_table_setup_mt,
    write_batch_to_table,
};
use url::Url;

use crate::common::write_utils::{collect_all_parquet_field_ids, read_parquet_root_schema};

const TABLE_ROOT: &str = "memory:///";

/// Hand-crafts a V0 create commit on an icebergCompatV3 table whose `data` field carries the
/// to-be-deprecated `parquet.field.nested.ids` metadata. Loading the snapshot must fail.
#[tokio::test]
async fn snapshot_blocked_when_v3_schema_has_legacy_nested_ids() {
    let (storage, engine) = make_default_engine_and_store();
    // Create table doesn't support the legacy nested ids metadata, so we hand-craft a V0 commit.
    let nested_ids_legacy = serde_json::json!({
        "data.key": 100,
        "data.value": 101,
        "data.value.element": 102,
    });
    let schema = StructType::try_new(vec![StructField::nullable(
        "data",
        DataType::Map(Box::new(MapType::new(
            DataType::INTEGER,
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            true,
        ))),
    )
    .with_metadata([
        (
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::from(1),
        ),
        (
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::from("col-1"),
        ),
        (
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
            MetadataValue::Other(nested_ids_legacy),
        ),
    ])])
    .unwrap();
    let schema_string = serde_json::to_string(&schema).unwrap();

    let commit = [
        serde_json::json!({
            "commitInfo": {
                "timestamp": 1587968586154_i64,
                "operation": "CREATE TABLE",
                "operationParameters": {},
                "isBlindAppend": true,
            }
        }),
        serde_json::json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping"],
                "writerFeatures": [
                    "icebergCompatV3",
                    "columnMapping",
                    "rowTracking",
                    "domainMetadata",
                ],
            }
        }),
        serde_json::json!({
            "metaData": {
                "id": "deadbeef-1234-5678-abcd-000000000001",
                "format": { "provider": "parquet", "options": {} },
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {
                    "delta.enableIcebergCompatV3": "true",
                    "delta.columnMapping.mode": "name",
                    "delta.enableRowTracking": "true",
                    "delta.rowTracking.materializedRowIdColumnName": "_row_id",
                    "delta.rowTracking.materializedRowCommitVersionColumnName":
                        "_row_commit_version",
                    "delta.columnMapping.maxColumnId": "1",
                },
                "createdTime": 1234567890000_i64,
            }
        }),
    ]
    .into_iter()
    .map(|action| serde_json::to_string(&action).unwrap())
    .collect::<Vec<_>>()
    .join("\n");
    add_commit(TABLE_ROOT, storage.as_ref(), 0, commit)
        .await
        .unwrap();

    let err = Snapshot::builder_for(TABLE_ROOT)
        .build(engine.as_ref())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("parquet.field.nested.ids")
            && err.contains("delta.columnMapping.nested.ids")
            && err.contains("data"),
        "expected error mentioning the legacy key, replacement key, and field path, got: {err}",
    );
}

#[rstest::rstest]
#[case::missing_num_records(None/* num_records */, Err("'stats.numRecords' is required"))]
#[case::with_num_records(Some(3), Ok(1))]
#[tokio::test]
async fn v3_commit_validates_num_records(
    #[case] num_records: Option<i64>,
    #[case] expected: Result<u64, &'static str>,
) {
    let (_, engine) = make_default_engine_and_store();

    let _ = create_table(TABLE_ROOT, simple_schema(), "Test/1.0")
        .with_table_properties([("delta.enableIcebergCompatV3", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(engine.as_ref())
        .unwrap();
    let snapshot = Snapshot::builder_for(TABLE_ROOT)
        .build(engine.as_ref())
        .unwrap();

    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())
        .unwrap()
        .with_engine_info("Test/1.0")
        .with_data_change(true);
    let add_files = create_add_files_metadata(
        txn.add_files_schema(),
        vec![("part-fake.parquet", 1024, 1_000_000, num_records)],
    )
    .unwrap();
    txn.add_files(add_files);

    match expected {
        Ok(expected_version) => {
            let committed = txn.commit(engine.as_ref()).unwrap().unwrap_committed();
            assert_eq!(committed.commit_version(), expected_version);
        }
        Err(needle) => {
            let err = txn.commit(engine.as_ref()).unwrap_err().to_string();
            assert!(
                err.contains(needle) && err.contains("part-fake.parquet"),
                "expected error containing {needle:?} and 'part-fake.parquet', got: {err}",
            );
        }
    }
}

/// V3 partitioned end-to-end: create + 4 commits with a checkpoint between, then validate
/// protocol features, parquet field IDs (top-level + nested), partition materialization,
/// timestamp physical type, and exact scan content.
///
/// IcebergCompatV3 supports all delta types, so we use a schema with all delta types here.
/// Two test cases:
/// - `min`: minimum feature set. Enables only `delta.enableIcebergCompatV3=true` and relies on V3's
///   auto-enablement of `columnMapping=name` + `rowTracking=true`.
/// - `max`: maximum feature set we are able to enable through create table, with exceptions for:
///   `materializePartitionColumns` (omitted so the partition-materialization check below proves V3
///   implies it), `typeWidening` (kernel rejects writes against tables declaring it), and
///   `catalogManaged` (requires a catalog committer).
#[rstest::rstest]
#[case::min(
    /* extra_props */ &[],
    /* enable_features */ &[],
    /* expected_features */ &[V3_BASELINE_FEATURES],
)]
#[case::max(
    // Intentionally set `delta.columnMapping.mode=id`, since the `min` case already covers the
    // V3-default `name` mode.
    /* extra_props */ &[("delta.columnMapping.mode", "id")],
    /* enable_features */ &[
        "deletionVectors", "inCommitTimestamp", "changeDataFeed", "appendOnly",
        "v2Checkpoint", "vacuumProtocolCheck", "invariants",
    ],
    /* expected_features */ &[READER_WRITER_FEATURES, WRITER_FEATURES],
)]
#[tokio::test(flavor = "multi_thread")]
async fn v3_e2e_partitioned_writes_with_field_ids(
    #[case] extra_props: &[(&str, &str)],
    #[case] enable_features: &[&str],
    #[case] expected_features: &[&[&str]],
) {
    // === Setup ===
    let (_tmp_dir, table_path, _) = test_table_setup_mt().unwrap();
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );

    // === Create partitioned V3 table ===
    let schema = nested_schema_with_all_delta_types();
    // Features without an enable property are added via `delta.feature.X=supported`. Build
    // the signal strings up front so the property tuples below can borrow them.
    let signal_props: Vec<(String, String)> = enable_features
        .iter()
        .filter(|f| enable_property_for(f).is_none())
        .map(|f| (format!("delta.feature.{f}"), "supported".to_string()))
        .collect();
    let mut props: Vec<(&str, &str)> = vec![("delta.enableIcebergCompatV3", "true")];
    props.extend(extra_props.iter().copied());
    props.extend(
        enable_features
            .iter()
            .filter_map(|f| enable_property_for(f).map(|p| (p, "true"))),
    );
    props.extend(signal_props.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    let _ = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties(props)
        .with_data_layout(DataLayout::partitioned(["region"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(engine.as_ref())
        .unwrap();

    // === 4 commits (2 outer iters x 2 partitions), checkpoint, 4 more commits ===
    for commit_idx in 0..2_i32 {
        write_partitioned_data(&table_url, engine.clone(), commit_idx).await;
    }
    Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())
        .unwrap()
        .checkpoint(engine.as_ref(), None)
        .unwrap();
    for commit_idx in 2..4_i32 {
        write_partitioned_data(&table_url, engine.clone(), commit_idx).await;
    }

    let final_snap = Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())
        .unwrap();

    // === Verify protocol features and column mapping mode ===
    let expected: Vec<&str> = expected_features
        .iter()
        .flat_map(|list| list.iter().copied())
        .collect();
    verify_protocol(&final_snap, &expected);
    let cm_mode = final_snap
        .table_properties()
        .column_mapping_mode
        .expect("V3 must enable column mapping");
    let expected_cm_mode = extra_props
        .iter()
        .find(|(k, _)| *k == "delta.columnMapping.mode")
        .map(|(_, v)| match *v {
            "id" => ColumnMappingMode::Id,
            "name" => ColumnMappingMode::Name,
            other => panic!("unexpected column mapping mode {other:?}"),
        })
        .unwrap_or(ColumnMappingMode::Name);
    assert_eq!(cm_mode, expected_cm_mode);

    // === Per-data-file validations ===
    let add_actions = read_add_infos(&final_snap, engine.as_ref()).unwrap();
    assert_eq!(
        add_actions.len(),
        8,
        "expected 4 outer iters x 2 partitions = 8 add files",
    );
    let logical_schema = final_snap.schema();
    for add in &add_actions {
        let parquet_url = table_url.join(&add.path).unwrap();
        let local_path = parquet_url.to_file_path().unwrap();
        let ids = collect_all_parquet_field_ids(&local_path);
        let parquet_root_schema = read_parquet_root_schema(&local_path);

        // Top-level + nested column-mapping IDs all appear as expected.
        // Expected count for `nested_schema_with_all_delta_types`:
        //   16 top-level fields
        //   + 3 nested.ids on `complex` (`complex.{key, key.element, value}`)
        //   + 2 struct fields inside `complex.value` (`inner_map`, `n`)
        //   + 3 nested.ids on `inner_map` (`inner_map.{key, key.element, value}`)
        //   = 24
        verify_column_mapping_ids_in_parquet(
            logical_schema.as_ref(),
            cm_mode,
            &ids,
            &parquet_url,
            24, /* expected_num_ids */
        );

        // Partition column is physically materialized (V3 invariant).
        let region_physical =
            get_top_level_physical_name(logical_schema.as_ref(), "region", cm_mode);
        assert!(
            find_top_level_field_in_parquet(&parquet_root_schema, &region_physical).is_some(),
            "partition column `region` (physical {region_physical}) not materialized in {parquet_url}",
        );

        // timestamp / timestamp_ntz are INT64 in parquet.
        for col in ["timestamp_col", "timestamp_ntz_col"] {
            let physical = get_top_level_physical_name(logical_schema.as_ref(), col, cm_mode);
            assert_eq!(
                find_top_level_physical_type_in_parquet(&parquet_root_schema, &physical),
                ParquetPhysicalType::INT64,
                "{col} must be INT64 in parquet",
            );
        }
    }

    // === Exact scan contents ===
    verify_scan_contents(final_snap.clone(), engine.clone());
}

// === Helpers ===

fn make_default_engine_and_store() -> (Arc<InMemory>, Arc<DefaultEngine<TokioBackgroundExecutor>>) {
    let storage = Arc::new(InMemory::new());
    let engine =
        Arc::new(DefaultEngineBuilder::<TokioBackgroundExecutor>::new(storage.clone()).build());
    (storage, engine)
}

fn simple_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap(),
    )
}

/// Schema covering every Delta types(primitive, struct, map, array, variant)
/// Including a deeply nested map<list<int>, struct<inner_map: map<list<int>, int>, n: int>> column.
/// `region` is the partition column.
fn nested_schema_with_all_delta_types() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("region", DataType::STRING),
            StructField::nullable("int_col", DataType::INTEGER),
            StructField::nullable("bool_col", DataType::BOOLEAN),
            StructField::nullable("byte_col", DataType::BYTE),
            StructField::nullable("short_col", DataType::SHORT),
            StructField::nullable("long_col", DataType::LONG),
            StructField::nullable("float_col", DataType::FLOAT),
            StructField::nullable("double_col", DataType::DOUBLE),
            StructField::nullable("decimal_col", DataType::decimal(10, 2).unwrap()),
            StructField::nullable("string_col", DataType::STRING),
            StructField::nullable("binary_col", DataType::BINARY),
            StructField::nullable("date_col", DataType::DATE),
            StructField::nullable("timestamp_col", DataType::TIMESTAMP),
            StructField::nullable("timestamp_ntz_col", DataType::TIMESTAMP_NTZ),
            StructField::nullable("variant_col", DataType::unshredded_variant()),
            StructField::nullable("complex", complex_nested_data_type()),
        ])
        .unwrap(),
    )
}

/// `map<list<int>, struct<inner_map: map<list<int>, int>, n: int>>`. A deeply nested
/// type used to validate field-id propagation through every level of map/list/struct.
fn complex_nested_data_type() -> DataType {
    let inner_struct = StructType::try_new(vec![
        StructField::nullable(
            "inner_map",
            DataType::Map(Box::new(MapType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
                DataType::INTEGER,
                true,
            ))),
        ),
        StructField::nullable("n", DataType::INTEGER),
    ])
    .unwrap();
    DataType::Map(Box::new(MapType::new(
        DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
        DataType::Struct(Box::new(inner_struct)),
        true,
    )))
}

const ROWS_PER_PARTITION: i32 = 3;
const PARTITION_REGIONS: &[&str] = &["a", "b"];

/// Build a [`RecordBatch`] with [`ROWS_PER_PARTITION`] rows for the given `region` following the
/// schema of [`nested_schema_with_all_delta_types`]. `random_seed` is used directly to produce
/// different values so different calls produce distinguishable rows;
fn build_partition_batch(random_seed: i32, region: &str) -> RecordBatch {
    let rows = ROWS_PER_PARTITION as usize;

    let schema = nested_schema_with_all_delta_types();
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow().unwrap();

    let region_partition_col: ArrayRef = Arc::new(StringArray::from(vec![region; rows]));
    let int_col: ArrayRef = Arc::new(Int32Array::from(
        (0..ROWS_PER_PARTITION)
            .map(|i| random_seed + i)
            .collect::<Vec<_>>(),
    ));
    let bool_col: ArrayRef = Arc::new(BooleanArray::from(
        (0..rows).map(|i| i % 2 == 0).collect::<Vec<_>>(),
    ));
    let byte_col: ArrayRef = Arc::new(Int8Array::from(
        (0..ROWS_PER_PARTITION)
            .map(|i| (random_seed + i) as i8)
            .collect::<Vec<_>>(),
    ));
    let short_col: ArrayRef = Arc::new(Int16Array::from(
        (0..ROWS_PER_PARTITION)
            .map(|i| (random_seed + i) as i16)
            .collect::<Vec<_>>(),
    ));
    let long_col: ArrayRef = Arc::new(Int64Array::from(
        (0..ROWS_PER_PARTITION)
            .map(|i| (random_seed + i) as i64)
            .collect::<Vec<_>>(),
    ));
    let float_col: ArrayRef = Arc::new(Float32Array::from(
        (0..ROWS_PER_PARTITION)
            .map(|i| (random_seed + i) as f32 + 0.5)
            .collect::<Vec<_>>(),
    ));
    let double_col: ArrayRef = Arc::new(Float64Array::from(
        (0..ROWS_PER_PARTITION)
            .map(|i| (random_seed + i) as f64 + 0.25)
            .collect::<Vec<_>>(),
    ));
    // Decimal(10, 2): values stored as scaled i128.
    let decimal_col: ArrayRef = Arc::new(
        Decimal128Array::from(
            (0..ROWS_PER_PARTITION)
                .map(|i| (random_seed + i) as i128 * 100)
                .collect::<Vec<_>>(),
        )
        .with_precision_and_scale(10, 2)
        .unwrap(),
    );
    let string_col: ArrayRef = Arc::new(StringArray::from(
        (0..rows)
            .map(|i| format!("s-{random_seed}-{i}"))
            .collect::<Vec<_>>(),
    ));
    let binary_col: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        (0..rows).map(|i| vec![(random_seed as u8).wrapping_add(i as u8)]),
    ));
    // Days since 1970-01-01.
    let date_col: ArrayRef = Arc::new(Date32Array::from(
        (0..ROWS_PER_PARTITION)
            .map(|i| 19000 + random_seed + i)
            .collect::<Vec<_>>(),
    ));
    // Microseconds since epoch: TIMESTAMP carries UTC ("+00:00"), TIMESTAMP_NTZ has no zone.
    let timestamp_col: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            (0..ROWS_PER_PARTITION)
                .map(|i| 1_700_000_000_000_000_i64 + (random_seed + i) as i64)
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    let timestamp_ntz_col: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
        (0..ROWS_PER_PARTITION)
            .map(|i| 1_700_000_000_000_000_i64 + (random_seed + i) as i64 + 1)
            .collect::<Vec<_>>(),
    ));
    let variant_col: ArrayRef = build_all_null_variant_array(rows);
    let complex: ArrayRef = build_all_null_complex_array(rows);

    RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            region_partition_col,
            int_col,
            bool_col,
            byte_col,
            short_col,
            long_col,
            float_col,
            double_col,
            decimal_col,
            string_col,
            binary_col,
            date_col,
            timestamp_col,
            timestamp_ntz_col,
            variant_col,
            complex,
        ],
    )
    .unwrap()
}

/// Builds an all-null unshredded variant column with [`rows`] rows.
fn build_all_null_variant_array(rows: usize) -> ArrayRef {
    let metadata_bytes: Vec<&[u8]> = (0..rows).map(|_| &[0x01, 0x00, 0x00][..]).collect();
    let value_bytes: Vec<&[u8]> = (0..rows).map(|_| &[0x0C, 0x01][..]).collect();
    let null_bitmap = NullBuffer::from(vec![false; rows]);
    let metadata_arr: ArrayRef = Arc::new(BinaryArray::from(metadata_bytes));
    let value_arr: ArrayRef = Arc::new(BinaryArray::from(value_bytes));
    let arrow_variant: ArrowDataType =
        ArrowDataType::try_from_kernel(&DataType::unshredded_variant()).unwrap();
    let fields = match arrow_variant {
        ArrowDataType::Struct(fields) => fields,
        _ => panic!("variant arrow type must be struct"),
    };
    Arc::new(
        StructArray::try_new(fields, vec![metadata_arr, value_arr], Some(null_bitmap)).unwrap(),
    )
}

/// Builds an all-null `complex` MapArray of length `rows` matching [`complex_nested_data_type`].
/// All map entries are null; the underlying entries struct is empty.
fn build_all_null_complex_array(rows: usize) -> ArrayRef {
    let complex_arrow_type: ArrowDataType = (&complex_nested_data_type()).try_into_arrow().unwrap();
    let (entries_field, _sorted) = match complex_arrow_type {
        ArrowDataType::Map(entries, sorted) => (entries, sorted),
        other => panic!("complex must be a Map type, got {other:?}"),
    };
    let entries_struct_fields = match entries_field.data_type() {
        ArrowDataType::Struct(fields) => fields.clone(),
        other => panic!("map entries must be struct, got {other:?}"),
    };
    // Empty entries struct.
    let empty_entries = StructArray::new(
        entries_struct_fields,
        entries_field_arrays_empty(entries_field.as_ref()),
        None,
    );
    let offsets = OffsetBuffer::from_lengths(vec![0_usize; rows]);
    let nulls = NullBuffer::from(vec![false; rows]);
    Arc::new(MapArray::new(
        entries_field,
        offsets,
        empty_entries,
        Some(nulls),
        false,
    ))
}

fn entries_field_arrays_empty(entries_field: &ArrowField) -> Vec<ArrayRef> {
    match entries_field.data_type() {
        ArrowDataType::Struct(fields) => fields
            .iter()
            .map(|f| delta_kernel::arrow::array::new_empty_array(f.data_type()))
            .collect(),
        other => panic!("expected struct entries, got {other:?}"),
    }
}

/// Write one commit per partition (regions "a" and "b"). Stats and
/// `numRecords` are auto-collected by the default engine, so the numRecords invariant is
/// satisfied. Assume the table schema is [`nested_schema_with_all_delta_types`].
async fn write_partitioned_data(
    table_url: &Url,
    engine: Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    random_seed: i32,
) {
    let mut snapshot = Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())
        .unwrap();
    // Defensive sanity check: confirm the table schema matches what `build_partition_batch`
    // produces. We compare top-level field names rather than full structs for simplicity.
    let actual_schema = snapshot.schema();
    let actual_fields: Vec<&str> = actual_schema.fields().map(|f| f.name().as_str()).collect();
    let expected_schema = nested_schema_with_all_delta_types();
    let expected_fields: Vec<&str> = expected_schema
        .fields()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        actual_fields, expected_fields,
        "table schema does not match `nested_schema_with_all_delta_types`",
    );

    for region in PARTITION_REGIONS {
        let batch = build_partition_batch(random_seed, region);
        let partition_values =
            HashMap::from([("region".to_string(), Scalar::String(region.to_string()))]);
        snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, partition_values)
            .await
            .unwrap();
    }
}

/// Features that V3 auto-enables (plus TS_NTZ and variant types which are auto-enabled by our test
/// schema).
const V3_BASELINE_FEATURES: &[&str] = &[
    "icebergCompatV3",
    "columnMapping",
    "rowTracking",
    "domainMetadata",
    "timestampNtz",
    "variantType",
];

/// ReaderWriter features used in this test file.
const READER_WRITER_FEATURES: &[&str] = &[
    "columnMapping",
    "deletionVectors",
    "timestampNtz",
    "v2Checkpoint",
    "vacuumProtocolCheck",
    "variantType",
];

/// Writer-only features used in this test file.
const WRITER_FEATURES: &[&str] = &[
    "appendOnly",
    "changeDataFeed",
    "domainMetadata",
    "icebergCompatV3",
    "inCommitTimestamp",
    "invariants",
    "rowTracking",
];

/// Maps each enablement-driven feature to its `delta.enable*` (or `delta.appendOnly`)
/// property name.
const FEATURE_ENABLE_PROPERTY: &[(&str, &str)] = &[
    ("appendOnly", "delta.appendOnly"),
    ("changeDataFeed", "delta.enableChangeDataFeed"),
    ("deletionVectors", "delta.enableDeletionVectors"),
    ("icebergCompatV3", "delta.enableIcebergCompatV3"),
    ("inCommitTimestamp", "delta.enableInCommitTimestamps"),
    ("rowTracking", "delta.enableRowTracking"),
];

/// Returns the `delta.enable*` property name for `feature` if one exists, or `None` if the
/// feature must be added via the `delta.feature.X=supported` signal instead.
fn enable_property_for(feature: &str) -> Option<&'static str> {
    FEATURE_ENABLE_PROPERTY
        .iter()
        .find(|(name, _)| *name == feature)
        .map(|(_, prop)| *prop)
}

/// Assert each name in `expected_features` appears in `writerFeatures`, plus that any
/// reader+writer feature also appears in `readerFeatures`.
fn verify_protocol(snapshot: &Snapshot, expected_features: &[&str]) {
    let protocol = snapshot.table_configuration().protocol();
    let writer_features: Vec<String> = protocol
        .writer_features()
        .expect("V3 table must publish writerFeatures")
        .iter()
        .map(|f| f.as_ref().to_string())
        .collect();
    let reader_features: Vec<String> = protocol
        .reader_features()
        .map(|fs| fs.iter().map(|f| f.as_ref().to_string()).collect())
        .unwrap_or_default();
    for f in expected_features {
        assert!(
            writer_features.iter().any(|w| w == f),
            "writerFeatures missing {f}; got {writer_features:?}",
        );
        let is_reader_writer = READER_WRITER_FEATURES.contains(f);
        let is_writer_only = WRITER_FEATURES.contains(f);
        assert!(
            is_reader_writer || is_writer_only,
            "feature '{f}' is not classified in READER_WRITER_FEATURES or WRITER_FEATURES; \
             update one of those lists",
        );
        if is_reader_writer {
            assert!(
                reader_features.iter().any(|r| r == f),
                "readerFeatures missing {f} (reader+writer feature); got {reader_features:?}",
            );
        } else {
            assert!(
                !reader_features.iter().any(|r| r == f),
                "readerFeatures unexpectedly contains writer-only feature {f}; \
                 got {reader_features:?}",
            );
        }
    }
}

/// Look up the physical name of a top-level logical column. Panics if not found.
fn get_top_level_physical_name(
    schema: &StructType,
    logical: &str,
    mode: ColumnMappingMode,
) -> String {
    let physical = get_any_level_column_physical_name(schema, &ColumnName::new([logical]), mode)
        .unwrap()
        .into_inner();
    physical
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no physical path for column {logical}"))
}

/// Assert that every column-mapping ID lands at the right `parquet_schema_path` in the parquet
/// schema. Walks the kernel schema, builds an expected `parquet_schema_path -> id` map (from
/// both `delta.columnMapping.id` and `delta.columnMapping.nested.ids`), then for each entry
/// asserts the parquet file has the same id at that path.
///
/// A `parquet_schema_path` is the dot-joined path of a field from the parquet schema root.
///
/// Example. A kernel schema with one column `m: map<list<int>, int>` carrying:
/// ```text
/// delta.columnMapping.id           = 1
/// delta.columnMapping.physicalName = "col-X"
/// delta.columnMapping.nested.ids   = {
///     "col-X.key":         2,
///     "col-X.key.element": 3,
///     "col-X.value":       4,
/// }
/// ```
/// produces the expected `parquet_schema_path -> id` map:
/// ```text
/// {
///   "col-X":                            1,
///   "col-X.key_value.key":              2,
///   "col-X.key_value.key.list.element": 3,
///   "col-X.key_value.value":            4,
/// }
/// ```
fn verify_column_mapping_ids_in_parquet(
    logical_schema: &StructType,
    mode: ColumnMappingMode,
    parquet_ids: &HashMap<String, i64>,
    parquet_url: &Url,
    expected_num_ids: usize,
) {
    let mut visitor = ExpectedFieldIdMap {
        mode,
        parquet_schema_path_stack: Vec::new(),
        expected: HashMap::new(),
    };
    visitor.transform_struct(logical_schema);
    assert_eq!(
        parquet_ids.len(),
        expected_num_ids,
        "expected {expected_num_ids} field IDs in {parquet_url}, got {}: {parquet_ids:?}",
        parquet_ids.len(),
    );
    for (parquet_schema_path, expected_id) in &visitor.expected {
        let actual_id = parquet_ids
            .get(parquet_schema_path)
            .copied()
            .unwrap_or_else(|| {
                panic!(
                    "expected field_id {expected_id} at parquet_schema_path \
                 '{parquet_schema_path}' in {parquet_url}; actual ids: {parquet_ids:?}",
                )
            });
        assert_eq!(
            actual_id, *expected_id,
            "parquet field_id mismatch at parquet_schema_path \
             '{parquet_schema_path}' in {parquet_url}: expected {expected_id}, got {actual_id}",
        );
    }
}

/// Schema visitor that builds `parquet_schema_path -> id` for
/// [`verify_column_mapping_ids_in_parquet`].
struct ExpectedFieldIdMap {
    mode: ColumnMappingMode,
    parquet_schema_path_stack: Vec<String>,
    expected: HashMap<String, i64>,
}

impl ExpectedFieldIdMap {
    fn current_parquet_schema_path(&self) -> String {
        self.parquet_schema_path_stack.join(".")
    }
}

impl<'a> SchemaTransform<'a> for ExpectedFieldIdMap {
    transform_output_type!(|'a, T| ());

    fn transform_struct_field(&mut self, field: &'a StructField) {
        self.parquet_schema_path_stack
            .push(field.physical_name(self.mode).to_string());
        let field_parquet_schema_path = self.current_parquet_schema_path();
        if let Some(id) = field.column_mapping_id() {
            self.expected.insert(field_parquet_schema_path.clone(), id);
        }
        if let Some(MetadataValue::Other(json)) =
            field.get_config_value(&ColumnMetadataKey::ColumnMappingNestedIds)
        {
            if let Some(obj) = json.as_object() {
                // Each key is a relative path rooted at the field's physical name, e.g.
                // `<phys>.key`, `<phys>.key.element`. We translate it to its corresponding
                // `parquet_schema_path`.
                for (physical_nested_path, value) in obj {
                    if let Some(id) = value.as_i64() {
                        self.expected.insert(
                            translate_nested_path(&field_parquet_schema_path, physical_nested_path),
                            id,
                        );
                    }
                }
            }
        }
        self.recurse_into_struct_field(field);
        self.parquet_schema_path_stack.pop();
    }

    fn transform_map(&mut self, mtype: &'a MapType) {
        self.parquet_schema_path_stack.push("key_value".to_string());
        self.parquet_schema_path_stack.push("key".to_string());
        self.transform_map_key(&mtype.key_type);
        self.parquet_schema_path_stack.pop();
        self.parquet_schema_path_stack.push("value".to_string());
        self.transform_map_value(&mtype.value_type);
        self.parquet_schema_path_stack.pop();
        self.parquet_schema_path_stack.pop();
    }

    fn transform_array(&mut self, atype: &'a ArrayType) {
        self.parquet_schema_path_stack.push("list".to_string());
        self.parquet_schema_path_stack.push("element".to_string());
        self.transform_array_element(&atype.element_type);
        self.parquet_schema_path_stack.pop();
        self.parquet_schema_path_stack.pop();
    }

    // Variant has its own internal struct (metadata/value) but the kernel parquet writer
    // does not carry column-mapping IDs into it; skip recursion to avoid recording paths
    // that don't correspond to parquet field IDs.
    fn transform_variant(&mut self, _stype: &'a StructType) {}
}

/// Translate a `delta.columnMapping.nested.ids` key (e.g. `<phys>.key.element`) to its
/// `parquet_schema_path` for map/array element/key/value fields.
fn translate_nested_path(field_parquet_schema_path: &str, physical_nested_path: &str) -> String {
    let segs: Vec<&str> = physical_nested_path.split('.').collect();
    let mut out = field_parquet_schema_path.to_string();
    for seg in &segs[1..] {
        match *seg {
            "key" => out.push_str(".key_value.key"),
            "value" => out.push_str(".key_value.value"),
            "element" => out.push_str(".list.element"),
            other => {
                panic!("unexpected nested-id path segment '{other}' in '{physical_nested_path}'")
            }
        }
    }
    out
}

/// Find a top-level field by physical name in a parquet root schema, or `None` if absent.
fn find_top_level_field_in_parquet<'a>(
    root: &'a ParquetType,
    name: &str,
) -> Option<&'a ParquetType> {
    root.get_fields()
        .iter()
        .find(|f| f.name() == name)
        .map(|f| f.as_ref())
}

/// Get the parquet physical type of a top-level field (looked up by physical name). Panics if
/// the field is missing or is a group (i.e. not a primitive leaf).
fn find_top_level_physical_type_in_parquet(root: &ParquetType, name: &str) -> ParquetPhysicalType {
    let field = find_top_level_field_in_parquet(root, name)
        .unwrap_or_else(|| panic!("top-level field {name} not found in parquet schema"));
    match field {
        ParquetType::PrimitiveType { physical_type, .. } => *physical_type,
        ParquetType::GroupType { .. } => {
            panic!("top-level field {name} is a group, expected primitive leaf")
        }
    }
}

/// Run a full scan and assert exact `(region, int_col, bool_col)` content for every row, plus
/// that `variant_col` and `complex` are entirely null. `int_col` derives from the writer's
/// `random_seed`, so the expected list is reproduced by replaying the same seeds (0..4) and
/// regions; sorting both sides by `(region, int_col)` makes the comparison order-independent.
fn verify_scan_contents(
    snapshot: Arc<Snapshot>,
    engine: Arc<DefaultEngine<TokioMultiThreadExecutor>>,
) {
    let scan = snapshot.scan_builder().build().unwrap();
    let mut rows: Vec<(String, i32, bool)> = Vec::new();
    let mut variant_null_count = 0_usize;
    let mut complex_null_count = 0_usize;
    let mut total_rows = 0_usize;

    for res in scan.execute(engine).unwrap() {
        let batch = into_record_batch(res.unwrap());

        let region_arr = batch
            .column_by_name("region")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("region column missing or wrong type");
        let int_arr = batch
            .column_by_name("int_col")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("int_col column missing or wrong type");
        let bool_arr = batch
            .column_by_name("bool_col")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
            .expect("bool_col column missing or wrong type");
        let variant_arr = batch
            .column_by_name("variant_col")
            .expect("variant_col column missing");
        let complex_arr = batch
            .column_by_name("complex")
            .expect("complex column missing");

        for i in 0..batch.num_rows() {
            rows.push((
                region_arr.value(i).to_string(),
                int_arr.value(i),
                bool_arr.value(i),
            ));
            if variant_arr.is_null(i) {
                variant_null_count += 1;
            }
            if complex_arr.is_null(i) {
                complex_null_count += 1;
            }
            total_rows += 1;
        }
    }

    // Sort by (region, int_col): same `random_seed` produces equal `int_col` across regions,
    // so `(region, int_col)` is the unique row key.
    rows.sort();

    let mut expected: Vec<(String, i32, bool)> = Vec::new();
    for random_seed in 0..4_i32 {
        for region in PARTITION_REGIONS {
            for i in 0..ROWS_PER_PARTITION {
                expected.push((region.to_string(), random_seed + i, i % 2 == 0));
            }
        }
    }
    expected.sort();

    let total_expected = expected.len();
    assert_eq!(rows, expected, "scan contents mismatch");
    assert_eq!(total_rows, total_expected);
    assert_eq!(
        variant_null_count, total_expected,
        "all variant_col values must be null",
    );
    assert_eq!(
        complex_null_count, total_expected,
        "all complex (map) values must be null",
    );
}
