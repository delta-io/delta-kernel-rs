//! Integration tests for icebergCompat (V3) write- and load-time validations.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, MapArray, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::buffer::OffsetBuffer;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::{TryFromKernel, TryIntoArrow as _};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::EngineData;
use test_utils::{add_commit, write_batch_to_table};

const TABLE_ROOT: &str = "memory:///";

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

// === Test 1: legacy `parquet.field.nested.ids` on a V3 table is rejected at snapshot load ===

/// Hand-crafts a V0 create commit on an icebergCompatV3 table whose `data` field carries the
/// deprecated `parquet.field.nested.ids` metadata. Loading the snapshot must fail because the
/// validator runs in `TableConfiguration::try_new`.
#[tokio::test]
async fn snapshot_blocked_when_v3_schema_has_legacy_nested_ids() {
    let (storage, engine) = make_engine();

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

// === Test 2: V3 add_files without `stats.numRecords` is rejected at commit ===

/// Build add_files metadata for one synthetic file whose `stats` carries every required field
/// EXCEPT `numRecords` (which is null), matching the schema returned by
/// `Transaction::add_files_schema()`.
fn add_files_metadata_without_num_records(arrow_schema: &ArrowSchema) -> Box<dyn EngineData> {
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

    // stats: numRecords=NULL, empty nullCount/minValues/maxValues structs, tightBounds=true.
    let empty_struct_fields: Fields = Vec::<Arc<ArrowField>>::new().into();
    let empty_struct = StructArray::new_empty_fields(1, None);
    let stats_struct = StructArray::from(vec![
        (
            Arc::new(ArrowField::new("numRecords", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![None as Option<i64>])) as ArrayRef,
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

#[tokio::test]
async fn write_blocked_when_v3_add_lacks_num_records() {
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
    txn.add_files(add_files_metadata_without_num_records(&add_schema));

    let err = txn.commit(engine.as_ref()).unwrap_err().to_string();
    assert!(
        err.contains("'stats.numRecords' is required") && err.contains("part-fake.parquet"),
        "expected numRecords-missing error, got: {err}",
    );
}

// === Test 3: V3 happy path: create + write commits successfully ===

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
