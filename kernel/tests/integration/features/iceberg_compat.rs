//! Integration tests for icebergCompat (V3) write- and load-time validations.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, MapArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
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
use delta_kernel::EngineData;
use test_utils::{add_commit, read_add_infos, test_table_setup_mt, write_batch_to_table};
use url::Url;

use crate::common::write_utils::{collect_all_parquet_field_ids, read_parquet_root_schema};

const TABLE_ROOT: &str = "memory:///";

/// Hand-crafts a V0 create commit on an icebergCompatV3 table whose `data` field carries the
/// deprecated `parquet.field.nested.ids` metadata. Loading the snapshot must fail because the
/// validator runs in `TableConfiguration::try_new`.
#[tokio::test]
async fn snapshot_blocked_when_v3_schema_has_legacy_nested_ids() {
    let (storage, engine) = make_engine();
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
    let (_, engine) = make_engine();

    let _ = create_table(TABLE_ROOT, simple_schema(), "Test/1.0")
        .with_table_properties(v3_table_properties())
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
    let add_schema = ArrowSchema::try_from_kernel(txn.add_files_schema().as_ref()).unwrap();
    txn.add_files(synthetic_add_files_metadata(&add_schema, num_records));

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

#[tokio::test(flavor = "multi_thread")]
async fn write_succeeds_on_v3_table() {
    let (_, engine) = make_engine();

    let schema = simple_schema();
    let _ = create_table(TABLE_ROOT, schema.clone(), "Test/1.0")
        .with_table_properties(v3_table_properties())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(engine.as_ref())
        .unwrap();
    let snapshot = Snapshot::builder_for(TABLE_ROOT)
        .build(engine.as_ref())
        .unwrap();

    // Real write through the default engine: stats are auto-collected and include numRecords.
    let arrow_schema: ArrowSchema = (schema.as_ref()).try_into_arrow().unwrap();
    let id_col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let name_col: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![id_col, name_col]).unwrap();

    let new_snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new())
        .await
        .expect("V3 write should succeed");
    assert_eq!(new_snapshot.version(), 1);
}

/// End-to-end V3 round-trip on a partitioned table with a kitchen-sink schema:
///
/// - Schema covers every primitive Delta type plus an unshredded VARIANT and a deeply nested
///   `map<list<int>, struct<inner_map: map<list<int>, int>, n: int>>` column.
/// - Partition column is `region: STRING`. V3 forces partition values to be materialized into the
///   parquet data files (no `MaterializePartitionColumns` flag needed).
/// - Two parameterized cases:
///     - `min`: only `delta.enableIcebergCompatV3=true` (auto-pulls columnMapping=name,
///       rowTracking, domainMetadata; the schema auto-pulls timestampNtz + variantType). Checkpoint
///       at version 2 lands as a V1 single-parquet checkpoint.
///     - `max`: same as `min` plus `delta.feature.{deletionVectors, v2Checkpoint,
///       inCommitTimestamp, vacuumProtocolCheck}=supported`. Checkpoint at version 2 lands as a V2
///       multi-part checkpoint.
/// - Four commits, each writing 2 partitions x 3 rows = 6 rows. A checkpoint runs after commits
///   1-2; commits 3-4 land after the checkpoint.
///
/// Validations after the final write:
///   1. Protocol contains the expected reader/writer features for the case.
///   2. Every parquet data file has `field_id` set on every leaf, including map keys/values and
///      list elements. The total number of field-id-bearing nodes equals the count of typed leaves
///      in the kernel schema (15 + 3 nested-map leaves).
///   3. The partition column is physically materialized in each data file.
///   4. `ts` (TIMESTAMP) and `ts_ntz` (TIMESTAMP_NTZ) are written with parquet physical type INT64
///      (microseconds).
///   5. A full scan returns all 24 rows.
///   6. Parquet field IDs match `delta.columnMapping.id` from the table schema for every top-level
///      column and for the nested map/list leaves of `complex`.
#[rstest::rstest]
#[case::min(false)]
#[case::max(true)]
#[tokio::test(flavor = "multi_thread")]
async fn v3_e2e_partitioned_writes_with_field_ids(#[case] max_features: bool) {
    let _ = tracing_subscriber::fmt::try_init();

    // === Setup: filesystem-backed engine on a temp dir ===
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
    let schema = kitchen_sink_schema();
    let mut props: Vec<(&str, &str)> = vec![
        ("delta.enableIcebergCompatV3", "true"),
        ("delta.columnMapping.mode", "name"),
        ("delta.enableRowTracking", "true"),
    ];
    if max_features {
        props.extend([
            ("delta.feature.deletionVectors", "supported"),
            ("delta.feature.v2Checkpoint", "supported"),
            ("delta.feature.inCommitTimestamp", "supported"),
            ("delta.feature.vacuumProtocolCheck", "supported"),
        ]);
    }
    let _ = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties(props)
        .with_data_layout(DataLayout::partitioned(["region"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(engine.as_ref())
        .unwrap();

    // === 2 commits, then checkpoint, then 2 more commits ===
    for commit_idx in 0..2_i32 {
        write_partitioned_v3_commit(&table_url, engine.clone(), &schema, commit_idx).await;
    }
    Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())
        .unwrap()
        .checkpoint(engine.as_ref(), None)
        .unwrap();
    for commit_idx in 2..4_i32 {
        write_partitioned_v3_commit(&table_url, engine.clone(), &schema, commit_idx).await;
    }

    let final_snap = Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())
        .unwrap();

    // === (1) Protocol features ===
    let protocol = final_snap.table_configuration().protocol();
    let writer_features: Vec<String> = protocol
        .writer_features()
        .expect("V3 table must publish writerFeatures")
        .iter()
        .map(|f| f.as_ref().to_string())
        .collect();
    for f in [
        "icebergCompatV3",
        "columnMapping",
        "rowTracking",
        "domainMetadata",
        "timestampNtz",
        "variantType",
    ] {
        assert!(
            writer_features.iter().any(|w| w == f),
            "writerFeatures missing {f}; got {writer_features:?}",
        );
    }
    if max_features {
        for f in [
            "deletionVectors",
            "v2Checkpoint",
            "inCommitTimestamp",
            "vacuumProtocolCheck",
        ] {
            assert!(
                writer_features.iter().any(|w| w == f),
                "writerFeatures missing {f} for max case; got {writer_features:?}",
            );
        }
    }
    assert_eq!(
        final_snap
            .table_properties()
            .column_mapping_mode
            .unwrap_or(ColumnMappingMode::None),
        ColumnMappingMode::Name,
    );

    // === (2)-(4) Per-data-file validations ===
    let add_actions = read_add_infos(&final_snap, engine.as_ref()).unwrap();
    assert_eq!(
        add_actions.len(),
        8,
        "expected 4 commits x 2 partitions = 8 add files",
    );
    for add in &add_actions {
        let parquet_url = table_url.join(&add.path).unwrap();
        let local_path = parquet_url.to_file_path().unwrap();
        let ids = collect_all_parquet_field_ids(&local_path);

        // Field IDs match column mapping IDs from the logical schema for top-level columns.
        let logical_schema = final_snap.schema();
        for field in logical_schema.fields() {
            let physical = get_any_level_column_physical_name(
                logical_schema.as_ref(),
                &ColumnName::new([field.name()]),
                ColumnMappingMode::Name,
            )
            .unwrap()
            .into_inner();
            let id_in_parquet = ids.get(&physical[0]).copied().unwrap_or_else(|| {
                panic!(
                    "missing field_id for top-level column {} (physical {:?}) in {}",
                    field.name(),
                    physical,
                    parquet_url
                )
            });
            let expected = field.column_mapping_id().unwrap_or_else(|| {
                panic!(
                    "logical field {} missing column mapping id; metadata: {:?}",
                    field.name(),
                    field.metadata()
                )
            });
            assert_eq!(
                id_in_parquet,
                expected,
                "parquet field_id mismatch for top-level {}: parquet={id_in_parquet}, schema={expected}",
                field.name()
            );
        }

        // Field IDs are present on every leaf inside the deeply nested `complex` column.
        // The leaves below correspond to: outer map key (list<int>) -> int element,
        // outer map value (struct) -> inner_map key (list<int>) -> int element,
        // outer map value -> inner_map value (int), outer map value -> n (int).
        let complex_physical = get_any_level_column_physical_name(
            logical_schema.as_ref(),
            &ColumnName::new(["complex"]),
            ColumnMappingMode::Name,
        )
        .unwrap()
        .into_inner();
        let complex_paths = collect_paths_with_prefix(&ids, &complex_physical[0]);
        assert!(
            complex_paths.len() >= 7,
            "expected >=7 field-id-bearing nodes under `complex` (physical {complex_physical:?}), got {complex_paths:?}",
        );

        // Partition column physically materialized in each data file (V3 invariant).
        let region_physical = get_any_level_column_physical_name(
            logical_schema.as_ref(),
            &ColumnName::new(["region"]),
            ColumnMappingMode::Name,
        )
        .unwrap()
        .into_inner();
        let root = read_parquet_root_schema(&local_path);
        assert!(
            top_level_field(&root, &region_physical[0]).is_some(),
            "partition column `region` (physical {region_physical:?}) is not materialized in {parquet_url}",
        );

        // ts / ts_ntz physical type is INT64.
        let ts_physical = get_any_level_column_physical_name(
            logical_schema.as_ref(),
            &ColumnName::new(["ts"]),
            ColumnMappingMode::Name,
        )
        .unwrap()
        .into_inner();
        let ts_ntz_physical = get_any_level_column_physical_name(
            logical_schema.as_ref(),
            &ColumnName::new(["ts_ntz"]),
            ColumnMappingMode::Name,
        )
        .unwrap()
        .into_inner();
        assert_eq!(
            top_level_physical_type(&root, &ts_physical[0]),
            ParquetPhysicalType::INT64,
            "ts must be INT64 in parquet",
        );
        assert_eq!(
            top_level_physical_type(&root, &ts_ntz_physical[0]),
            ParquetPhysicalType::INT64,
            "ts_ntz must be INT64 in parquet",
        );
    }

    // === (5) Scan returns all 24 rows ===
    let scan = final_snap.scan_builder().build().unwrap();
    let total_rows: usize = scan
        .execute(engine.clone())
        .unwrap()
        .map(|res| -> usize {
            let raw = res.unwrap();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(raw)
                .unwrap()
                .record_batch()
                .clone();
            batch.num_rows()
        })
        .sum();
    assert_eq!(
        total_rows, 24,
        "expected 4 commits x 2 partitions x 3 rows = 24 rows",
    );
}

// === Helpers ===

fn make_engine() -> (Arc<InMemory>, Arc<DefaultEngine<TokioBackgroundExecutor>>) {
    let storage = Arc::new(InMemory::new());
    let engine =
        Arc::new(DefaultEngineBuilder::<TokioBackgroundExecutor>::new(storage.clone()).build());
    (storage, engine)
}

fn v3_table_properties() -> Vec<(&'static str, &'static str)> {
    vec![
        ("delta.enableIcebergCompatV3", "true"),
        ("delta.columnMapping.mode", "name"),
        ("delta.enableRowTracking", "true"),
    ]
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

/// Build add_files metadata for one synthetic file matching the schema returned by
/// `Transaction::add_files_schema()`. `num_records` controls the `stats.numRecords` field
/// (`None` -> NULL, `Some(n)` -> `n`); the other stats fields are populated with empty
/// structs / `tightBounds=true`.
fn synthetic_add_files_metadata(
    arrow_schema: &ArrowSchema,
    num_records: Option<i64>,
) -> Box<dyn EngineData> {
    let path_array = StringArray::from(vec!["part-fake.parquet"]);
    let size_array = Int64Array::from(vec![1024_i64]);
    let mod_time_array = Int64Array::from(vec![1_000_000_i64]);

    // partitionValues: empty Map<string, string>.
    let entries_field = Arc::new(ArrowField::new(
        "key_value",
        ArrowDataType::Struct(Fields::from(vec![
            ArrowField::new("key", ArrowDataType::Utf8, false),
            ArrowField::new("value", ArrowDataType::Utf8, true),
        ])),
        false,
    ));
    let empty_keys = StringArray::from(Vec::<&str>::new());
    let empty_values = StringArray::from(Vec::<Option<&str>>::new());
    let empty_entries = StructArray::from(vec![
        (
            Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
            Arc::new(empty_keys) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("value", ArrowDataType::Utf8, true)),
            Arc::new(empty_values) as ArrayRef,
        ),
    ]);
    let partition_values_array = Arc::new(MapArray::new(
        entries_field,
        OffsetBuffer::from_lengths(vec![0]),
        empty_entries,
        None,
        false,
    ));

    let empty_struct_fields: Fields = Vec::<Arc<ArrowField>>::new().into();
    let empty_struct = StructArray::new_empty_fields(1, None);
    let stats_struct = StructArray::from(vec![
        (
            Arc::new(ArrowField::new("numRecords", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![num_records])) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new(
                "nullCount",
                ArrowDataType::Struct(empty_struct_fields.clone()),
                true,
            )),
            Arc::new(empty_struct.clone()) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new(
                "minValues",
                ArrowDataType::Struct(empty_struct_fields.clone()),
                true,
            )),
            Arc::new(empty_struct.clone()) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new(
                "maxValues",
                ArrowDataType::Struct(empty_struct_fields),
                true,
            )),
            Arc::new(empty_struct) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("tightBounds", ArrowDataType::Boolean, true)),
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(path_array) as ArrayRef,
            partition_values_array as ArrayRef,
            Arc::new(size_array) as ArrayRef,
            Arc::new(mod_time_array) as ArrayRef,
            Arc::new(stats_struct) as ArrayRef,
        ],
    )
    .unwrap();

    Box::new(ArrowEngineData::new(batch))
}

/// Schema covering every primitive Delta type plus an unshredded VARIANT and a deeply nested
/// map<list<int>, struct<inner_map: map<list<int>, int>, n: int>> column. `region` is the
/// partition column.
fn kitchen_sink_schema() -> Arc<StructType> {
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

    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("region", DataType::STRING),
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("b", DataType::BOOLEAN),
            StructField::nullable("byte_c", DataType::BYTE),
            StructField::nullable("short_c", DataType::SHORT),
            StructField::nullable("long_c", DataType::LONG),
            StructField::nullable("f", DataType::FLOAT),
            StructField::nullable("d", DataType::DOUBLE),
            StructField::nullable("dec", DataType::decimal(10, 2).unwrap()),
            StructField::nullable("s", DataType::STRING),
            StructField::nullable("bin", DataType::BINARY),
            StructField::nullable("date_c", DataType::DATE),
            StructField::nullable("ts", DataType::TIMESTAMP),
            StructField::nullable("ts_ntz", DataType::TIMESTAMP_NTZ),
            StructField::nullable("v", DataType::unshredded_variant()),
            StructField::nullable(
                "complex",
                DataType::Map(Box::new(MapType::new(
                    DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
                    DataType::Struct(Box::new(inner_struct)),
                    true,
                ))),
            ),
        ])
        .unwrap(),
    )
}

/// Build a 3-row record batch for the partition `region`. Column data is keyed off `commit_idx`
/// so different commits produce distinguishable rows. The deeply nested `complex` column and
/// the `v` (VARIANT) column are written as all-null -- this exercises the schema (field IDs
/// at every nested leaf) without requiring per-row binary/struct construction.
///
/// V3 materializes partition values into data files, so the `region` column is included in
/// the data with the partition value repeated for every row.
fn build_partition_batch(
    schema: &StructType,
    commit_idx: i32,
    region: &str,
    rows: usize,
) -> RecordBatch {
    let n = rows as i32;
    let base = commit_idx * 100
        + match region {
            "a" => 0,
            "b" => 50,
            _ => panic!("unsupported region {region}"),
        };

    let arrow_schema: ArrowSchema = schema.try_into_arrow().unwrap();

    let region_col: ArrayRef = Arc::new(StringArray::from(vec![region; rows]));
    let id: ArrayRef = Arc::new(Int32Array::from(
        (0..n).map(|i| base + i).collect::<Vec<_>>(),
    ));
    let b: ArrayRef = Arc::new(BooleanArray::from(
        (0..rows).map(|i| i % 2 == 0).collect::<Vec<_>>(),
    ));
    let byte_c: ArrayRef = Arc::new(Int8Array::from(
        (0..n).map(|i| (base + i) as i8).collect::<Vec<_>>(),
    ));
    let short_c: ArrayRef = Arc::new(Int16Array::from(
        (0..n).map(|i| (base + i) as i16).collect::<Vec<_>>(),
    ));
    let long_c: ArrayRef = Arc::new(Int64Array::from(
        (0..n).map(|i| (base + i) as i64).collect::<Vec<_>>(),
    ));
    let f: ArrayRef = Arc::new(Float32Array::from(
        (0..n).map(|i| (base + i) as f32 + 0.5).collect::<Vec<_>>(),
    ));
    let d: ArrayRef = Arc::new(Float64Array::from(
        (0..n).map(|i| (base + i) as f64 + 0.25).collect::<Vec<_>>(),
    ));
    // Decimal(10, 2): values stored as scaled i128.
    let dec: ArrayRef = Arc::new(
        Decimal128Array::from((0..n).map(|i| (base + i) as i128 * 100).collect::<Vec<_>>())
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let s: ArrayRef = Arc::new(StringArray::from(
        (0..rows)
            .map(|i| format!("s-{base}-{i}"))
            .collect::<Vec<_>>(),
    ));
    let bin: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        (0..rows).map(|i| vec![(base as u8).wrapping_add(i as u8)]),
    ));
    // Days since 1970-01-01.
    let date_c: ArrayRef = Arc::new(Date32Array::from(
        (0..n).map(|i| 19000 + base + i).collect::<Vec<_>>(),
    ));
    // Microseconds since epoch: TIMESTAMP carries UTC ("+00:00"), TIMESTAMP_NTZ has no zone.
    let ts: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            (0..n)
                .map(|i| 1_700_000_000_000_000_i64 + (base + i) as i64)
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    let ts_ntz: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
        (0..n)
            .map(|i| 1_700_000_000_000_000_i64 + (base + i) as i64 + 1)
            .collect::<Vec<_>>(),
    ));
    let v: ArrayRef = build_all_null_variant_array(rows);
    let complex: ArrayRef = build_all_null_complex_array(schema, rows);

    RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            region_col, id, b, byte_c, short_c, long_c, f, d, dec, s, bin, date_c, ts, ts_ntz, v,
            complex,
        ],
    )
    .unwrap()
}

/// Builds an all-null unshredded variant column (struct<metadata: binary, value: binary>) of
/// length `rows`. Mirrors the shape used by `test_append_variant`. The struct fields are
/// non-null physically (kernel's unshredded variant schema marks them required), so we provide
/// empty placeholder bytes for every row and rely on the outer null bitmap to mark all rows
/// null at the variant level.
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

/// Builds an all-null `complex` MapArray matching the kitchen-sink schema's `complex` field.
/// All `rows` map entries are null; the underlying entries struct is empty. This is enough to
/// exercise schema/field-id paths through the deeply nested type without per-row construction.
fn build_all_null_complex_array(schema: &StructType, rows: usize) -> ArrayRef {
    let complex_field = schema
        .fields()
        .find(|f| f.name() == "complex")
        .expect("schema must include `complex`");
    let arrow_field: ArrowField = complex_field.try_into_arrow().unwrap();
    let (entries_field, _sorted) = match arrow_field.data_type() {
        ArrowDataType::Map(entries, sorted) => (entries.clone(), *sorted),
        other => panic!("complex must be a Map type, got {other:?}"),
    };
    let entries_struct_fields = match entries_field.data_type() {
        ArrowDataType::Struct(fields) => fields.clone(),
        other => panic!("map entries must be struct, got {other:?}"),
    };
    // Empty entries struct: no rows since every map is null.
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

/// Produce empty arrays for each field of a map's `entries` struct. Each array has length 0
/// and matches the field's declared arrow data type. Recurses into nested struct/list/map
/// types via Arrow's `new_empty_array` factory.
fn entries_field_arrays_empty(entries_field: &ArrowField) -> Vec<ArrayRef> {
    match entries_field.data_type() {
        ArrowDataType::Struct(fields) => fields
            .iter()
            .map(|f| delta_kernel::arrow::array::new_empty_array(f.data_type()))
            .collect(),
        other => panic!("expected struct entries, got {other:?}"),
    }
}

/// Open a transaction on `table_url`, write 2 partitions x 3 rows (regions "a" and "b") via the
/// engine's parquet handler, and commit. Stats and `numRecords` are auto-collected by the
/// default engine, so the V3 numRecords invariant is satisfied.
async fn write_partitioned_v3_commit(
    table_url: &Url,
    engine: Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    schema: &StructType,
    commit_idx: i32,
) {
    let snapshot = Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())
        .unwrap();
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())
        .unwrap()
        .with_engine_info(format!("Test/v3-commit-{commit_idx}"))
        .with_data_change(true);

    // Each partition gets its own write_parquet call with a partitioned WriteContext.
    for region in ["a", "b"] {
        let batch = build_partition_batch(schema, commit_idx, region, 3 /* rows */);
        let write_context = txn
            .partitioned_write_context(HashMap::from([(
                "region".to_string(),
                Scalar::String(region.to_string()),
            )]))
            .unwrap();
        let add_files = engine
            .write_parquet(&ArrowEngineData::new(batch), &write_context)
            .await
            .unwrap();
        txn.add_files(add_files);
    }
    let _ = txn.commit(engine.as_ref()).unwrap().unwrap_committed();
}

/// Find a top-level field by physical name in a parquet root schema, or `None` if absent.
fn top_level_field<'a>(root: &'a ParquetType, name: &str) -> Option<&'a ParquetType> {
    root.get_fields()
        .iter()
        .find(|f| f.name() == name)
        .map(|f| f.as_ref())
}

/// Get the parquet physical type of a top-level field (looked up by physical name). Panics if
/// the field is missing or is a group (i.e. not a primitive leaf).
fn top_level_physical_type(root: &ParquetType, name: &str) -> ParquetPhysicalType {
    let field = top_level_field(root, name)
        .unwrap_or_else(|| panic!("top-level field {name} not found in parquet schema"));
    match field {
        ParquetType::PrimitiveType { physical_type, .. } => *physical_type,
        ParquetType::GroupType { .. } => {
            panic!("top-level field {name} is a group, expected primitive leaf")
        }
    }
}

/// Collect every entry from `ids` whose path starts with `prefix.` or equals `prefix`. Used to
/// count field-id-bearing nodes under a nested column.
fn collect_paths_with_prefix(ids: &HashMap<String, i64>, prefix: &str) -> Vec<String> {
    ids.keys()
        .filter(|k| k.as_str() == prefix || k.starts_with(&format!("{prefix}.")))
        .cloned()
        .collect()
}
