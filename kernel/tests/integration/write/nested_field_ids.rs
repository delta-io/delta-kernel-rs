//! End-to-end nested parquet field id propagation tests for `DefaultParquetHandler`.
use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
};
use delta_kernel::{EngineData, ParquetHandler};
use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use test_utils::delta_kernel_default_engine::parquet::DefaultParquetHandler;
use test_utils::nested_ids_json;
use url::Url;

use crate::common::write_utils::collect_all_parquet_field_ids;

#[tokio::test]
async fn test_nested_field_ids_round_trip() {
    let nested_ids_meta_key = ColumnMetadataKey::ColumnMappingNestedIds.as_ref();
    let kernel_schema = build_kernel_schema(nested_ids_meta_key);
    let arrow_schema: ArrowSchema = (&kernel_schema).try_into_arrow().unwrap();

    // Empty record batch is enough; the parquet writer still emits the schema (with field ids)
    // in the file footer regardless of row count.
    let record_batch = RecordBatch::new_empty(Arc::new(arrow_schema));
    let local_path = write_via_default_engine(record_batch);
    let id_by_path = collect_all_parquet_field_ids(&local_path);

    let expected: HashMap<String, i64> = [
        // array_in_map: map<int, array<int>>
        ("array_in_map", 1),
        ("array_in_map.key_value.key", 100),
        ("array_in_map.key_value.value", 101),
        ("array_in_map.key_value.value.list.element", 102),
        // map_in_list: list<map<int, int>>
        ("map_in_list", 2),
        ("map_in_list.list.element", 200),
        ("map_in_list.list.element.key_value.key", 201),
        ("map_in_list.list.element.key_value.value", 202),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    assert_eq!(
        id_by_path, expected,
        "parquet field ids by path don't match expected; got {id_by_path:?}",
    );
}

/// Build a kernel schema with two doubly nested top-level fields.
///
/// Layout:
/// - `array_in_map: map<int, array<int>>`
///   - top-level parquet.field.id = 1
///   - nested ids: `array_in_map.key` -> 100, `.value` -> 101, `.value.element` -> 102
/// - `map_in_list: list<map<int, int>>`
///   - top-level parquet.field.id = 2
///   - nested ids: `map_in_list.element` -> 200, `.element.key` -> 201, `.element.value` -> 202
fn build_kernel_schema(nested_ids_meta_key: &str) -> StructType {
    let with_field_ids = |name: &str, dtype, top_id: i64, nested: &[(&str, i64)]| {
        StructField::nullable(name, dtype).with_metadata([
            (
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::from(top_id),
            ),
            (
                nested_ids_meta_key,
                MetadataValue::Other(nested_ids_json(nested)),
            ),
        ])
    };

    let array_in_map = with_field_ids(
        "array_in_map",
        DataType::Map(Box::new(MapType::new(
            DataType::INTEGER,
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            true,
        ))),
        1,
        &[
            ("array_in_map.key", 100),
            ("array_in_map.value", 101),
            ("array_in_map.value.element", 102),
        ],
    );

    let map_in_list = with_field_ids(
        "map_in_list",
        DataType::Array(Box::new(ArrayType::new(
            DataType::Map(Box::new(MapType::new(
                DataType::INTEGER,
                DataType::INTEGER,
                true,
            ))),
            true,
        ))),
        2,
        &[
            ("map_in_list.element", 200),
            ("map_in_list.element.key", 201),
            ("map_in_list.element.value", 202),
        ],
    );

    StructType::try_new(vec![array_in_map, map_in_list]).unwrap()
}

/// Write `record_batch` via the `ParquetHandler::write_parquet_file` trait method to a
/// temp directory and return the resulting on-disk path.
fn write_via_default_engine(record_batch: RecordBatch) -> std::path::PathBuf {
    let temp_dir = tempfile::tempdir().unwrap();
    // Keep the tempdir so the file outlives this function; the OS cleans it up on test exit.
    let dir_path = temp_dir.keep();
    let file_path = dir_path.join("data.parquet");
    let location = Url::from_file_path(&file_path).unwrap();

    let store = Arc::new(LocalFileSystem::new());
    let handler = DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
    let data: Box<dyn Iterator<Item = delta_kernel::DeltaResult<Box<dyn EngineData>>> + Send> =
        Box::new(std::iter::once(Ok(
            Box::new(ArrowEngineData::new(record_batch)) as Box<dyn EngineData>,
        )));

    ParquetHandler::write_parquet_file(&handler, location, data).unwrap();
    file_path
}
