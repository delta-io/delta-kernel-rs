//! Integration test for `Scan::include_all_stats_columns` across column-mapping modes.
//!
//! Builds a table at runtime via the `test_utils` table-builder with
//! `delta.checkpoint.writeStatsAsStruct=true` so the checkpoint carries `stats_parsed`,
//! then verifies the parsed-stats struct column matches the JSON `stats` string row-by-row.
//! The table-builder's `default_schema` includes a nested struct column, so each case
//! exercises the recursive nested-stats path called out in issue #1766 (paths like
//! `minValues.nested_col.a`).
//!
//! Complementary to the unit test `test_scan_metadata_with_stats_columns` in
//! `kernel/src/scan/tests.rs`, which exercises a different code path: reading a
//! pre-existing Spark-written checkpoint with `stats_parsed` already on disk.

use delta_kernel::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS};
use delta_kernel::arrow::array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::Snapshot;
use rstest::rstest;
use test_utils::table_builder::{unpartitioned, version_latest, FeatureSet, LogState, TableConfig};
use test_utils::test_context;

/// Validate that JSON stats object values match the corresponding parsed struct array.
///
/// Recurses into nested struct sub-fields so paths like `minValues.info.age` are checked.
/// `field_path` is the dotted path used in assertion messages at this recursion
/// level (e.g. `"minValues"` at the top level; `"minValues.info.age"` when recursing).
/// Panics on missing fields to surface regressions where the parsed-stats schema drops a column.
fn assert_stats_struct_matches_json(
    struct_array: &StructArray,
    json_object: &serde_json::Map<String, serde_json::Value>,
    row_idx: usize,
    field_path: &str,
) {
    for (col_name, json_val) in json_object {
        let path = format!("{field_path}.{col_name}");
        let col = struct_array
            .column_by_name(col_name)
            .unwrap_or_else(|| panic!("{path}: present in JSON but missing from parsed struct"));
        if col.is_null(row_idx) {
            assert!(
                json_val.is_null(),
                "{path}: parsed is null but JSON is {json_val:?} at row {row_idx}"
            );
            continue;
        }
        match json_val {
            serde_json::Value::Number(_) => {
                let int_col = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap_or_else(|| {
                        panic!("{path}: expected Int64Array, got {:?}", col.data_type())
                    });
                assert_eq!(
                    json_val.as_i64().unwrap(),
                    int_col.value(row_idx),
                    "{path} mismatch at row {row_idx}"
                );
            }
            serde_json::Value::String(s) => {
                let str_col = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| {
                        panic!("{path}: expected StringArray, got {:?}", col.data_type())
                    });
                assert_eq!(
                    str_col.value(row_idx),
                    s.as_str(),
                    "{path} mismatch at row {row_idx}"
                );
            }
            serde_json::Value::Object(sub_obj) => {
                let sub_struct = col
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap_or_else(|| {
                        panic!("{path}: expected StructArray, got {:?}", col.data_type())
                    });
                assert_stats_struct_matches_json(sub_struct, sub_obj, row_idx, &path);
            }
            serde_json::Value::Null => {
                assert!(
                    col.is_null(row_idx),
                    "{path}: JSON is null but parsed is non-null at row {row_idx}"
                );
            }
            other => panic!("{path}: unsupported JSON variant {other:?} at row {row_idx}"),
        }
    }
}

/// Validate only nested-struct entries, not primitives.
fn validate_nested_columns(
    parsed: &StructArray,
    json_obj: &serde_json::Map<String, serde_json::Value>,
    row_idx: usize,
    field_prefix: &str,
) {
    for (key, val) in json_obj {
        let serde_json::Value::Object(sub_obj) = val else {
            continue;
        };
        let path = format!("{field_prefix}.{key}");
        let sub_struct = parsed
            .column_by_name(key)
            .unwrap_or_else(|| panic!("{path}: present in JSON but missing from parsed struct"))
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap_or_else(|| panic!("{path}: expected StructArray for nested column"));
        assert_stats_struct_matches_json(sub_struct, sub_obj, row_idx, &path);
    }
}

#[rstest]
fn scan_metadata_with_stats_columns_kernel_written(
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Id,
        ColumnMappingMode::Name
    )]
    cm_mode: ColumnMappingMode,
) {
    const STATS_PARSED_COL: &str = "stats_parsed";

    let cm_str = match cm_mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::Name => "name",
    };
    let (engine, snapshot, _table) = test_context!(
        LogState::with_latest_version(1).with_checkpoint_at([1]),
        FeatureSet::empty().column_mapping(cm_str),
        unpartitioned(),
        TableConfig::new().write_stats_as_struct(true),
        version_latest(),
    );

    let scan = snapshot
        .scan_builder()
        .include_all_stats_columns()
        .build()
        .unwrap();

    let scan_metadata_results: Vec<_> = scan
        .scan_metadata(&engine)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(
        !scan_metadata_results.is_empty(),
        "Should have scan metadata"
    );

    let mut total_num_records: i64 = 0;
    let mut file_count = 0;

    for scan_metadata in scan_metadata_results {
        let (underlying_data, selection_vector) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();
        let filtered_batch =
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();

        let schema = filtered_batch.schema();
        let field = schema
            .field_with_name(STATS_PARSED_COL)
            .expect("Schema should contain stats_parsed column");
        assert!(
            matches!(field.data_type(), ArrowDataType::Struct(_)),
            "stats_parsed should be a struct type, got: {:?}",
            field.data_type()
        );

        let stats_parsed = filtered_batch
            .column_by_name(STATS_PARSED_COL)
            .expect("stats_parsed column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("stats_parsed is a StructArray");
        let num_records = stats_parsed
            .column_by_name(NUM_RECORDS)
            .expect("numRecords sub-field")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("numRecords is Int64Array");
        let min_values = stats_parsed
            .column_by_name(MIN_VALUES)
            .expect("minValues sub-field")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("minValues is StructArray");
        let max_values = stats_parsed
            .column_by_name(MAX_VALUES)
            .expect("maxValues sub-field")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("maxValues is StructArray");
        let null_count = stats_parsed
            .column_by_name(NULL_COUNT)
            .expect("nullCount sub-field")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("nullCount is StructArray");
        let stats_json = filtered_batch
            .column_by_name("stats")
            .expect("stats column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("stats is StringArray");

        for i in 0..stats_json.len() {
            if stats_parsed.is_null(i) || stats_json.is_null(i) {
                continue;
            }

            let json_stats: serde_json::Value =
                serde_json::from_str(stats_json.value(i)).expect("stats JSON should be valid");

            let json_num = json_stats
                .get(NUM_RECORDS)
                .and_then(|v| v.as_i64())
                .expect("stats JSON must contain numRecords");
            assert_eq!(
                json_num,
                num_records.value(i),
                "numRecords mismatch at row {i}"
            );

            let min_obj = json_stats
                .get(MIN_VALUES)
                .and_then(|v| v.as_object())
                .expect("stats JSON must contain minValues object");
            validate_nested_columns(min_values, min_obj, i, MIN_VALUES);

            let max_obj = json_stats
                .get(MAX_VALUES)
                .and_then(|v| v.as_object())
                .expect("stats JSON must contain maxValues object");
            validate_nested_columns(max_values, max_obj, i, MAX_VALUES);

            let null_obj = json_stats
                .get(NULL_COUNT)
                .and_then(|v| v.as_object())
                .expect("stats JSON must contain nullCount object");
            validate_nested_columns(null_count, null_obj, i, NULL_COUNT);

            total_num_records += num_records.value(i);
            file_count += 1;
        }
    }

    assert!(file_count > 0, "Should have processed at least one file");
    assert!(total_num_records > 0, "Should have non-zero numRecords");
}
