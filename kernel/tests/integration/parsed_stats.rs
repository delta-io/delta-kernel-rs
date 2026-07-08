//! Integration tests for parsed-stats output.

use delta_kernel::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS, STATS_PARSED};
use delta_kernel::arrow::array::{
    Array, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::arrow::util::display::array_value_to_string;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::scan::StatsOptions;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::Snapshot;
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{unpartitioned, version_latest, FeatureSet, LogState, TableConfig};
use test_utils::{get_column, test_context};

/// Validate that JSON stats object values match the corresponding parsed struct array.
///
/// Panics on missing fields to surface regressions where the parsed-stats schema drops a column.
// TODO: cover interval columns once the default engine supports writing/stats for them.
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
            serde_json::Value::Number(n) => {
                if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
                    assert_eq!(
                        n.as_i64().unwrap(),
                        i64::from(arr.value(row_idx)),
                        "{path} mismatch at row {row_idx}"
                    );
                } else if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
                    assert_eq!(
                        n.as_i64().unwrap(),
                        i64::from(arr.value(row_idx)),
                        "{path} mismatch at row {row_idx}"
                    );
                } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    assert_eq!(
                        n.as_i64().unwrap(),
                        i64::from(arr.value(row_idx)),
                        "{path} mismatch at row {row_idx}"
                    );
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    assert_eq!(
                        n.as_i64().unwrap(),
                        arr.value(row_idx),
                        "{path} mismatch at row {row_idx}"
                    );
                } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                    assert_eq!(
                        n.as_f64().unwrap(),
                        f64::from(arr.value(row_idx)),
                        "{path} mismatch at row {row_idx}"
                    );
                } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    assert_eq!(
                        n.as_f64().unwrap(),
                        arr.value(row_idx),
                        "{path} mismatch at row {row_idx}"
                    );
                } else if let Some(arr) = col.as_any().downcast_ref::<Decimal128Array>() {
                    let ArrowDataType::Decimal128(_, scale) = arr.data_type() else {
                        unreachable!("Decimal128Array always has a Decimal128 data type")
                    };
                    assert_eq!(
                        n.as_f64().unwrap(),
                        arr.value(row_idx) as f64 / 10f64.powi(i32::from(*scale)),
                        "{path} mismatch at row {row_idx}"
                    );
                } else {
                    panic!("{path}: expected numeric array, got {:?}", col.data_type());
                }
            }
            serde_json::Value::String(s) => {
                let actual = array_value_to_string(col.as_ref(), row_idx)
                    .unwrap_or_else(|e| panic!("{path}: cannot format parsed value: {e}"));
                assert_eq!(&actual, s, "{path} mismatch at row {row_idx}");
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

/// Builds a table with `delta.checkpoint.writeStatsAsStruct=true` and a nested schema,
/// then verifies the parsed-stats struct column matches the JSON `stats` string.
#[rstest]
fn scan_metadata_with_stats_columns_kernel_written(
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Id,
        ColumnMappingMode::Name
    )]
    cm_mode: ColumnMappingMode,
) {
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
        .with_stats(StatsOptions::all())
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

        let stats_parsed = get_column!(filtered_batch, STATS_PARSED, StructArray);
        let num_records = get_column!(stats_parsed, NUM_RECORDS, Int64Array);
        let min_values = get_column!(stats_parsed, MIN_VALUES, StructArray);
        let max_values = get_column!(stats_parsed, MAX_VALUES, StructArray);
        let null_count = get_column!(stats_parsed, NULL_COUNT, StructArray);
        let stats_json = get_column!(filtered_batch, "stats", StringArray);

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
            assert_stats_struct_matches_json(min_values, min_obj, i, MIN_VALUES);

            let max_obj = json_stats
                .get(MAX_VALUES)
                .and_then(|v| v.as_object())
                .expect("stats JSON must contain maxValues object");
            assert_stats_struct_matches_json(max_values, max_obj, i, MAX_VALUES);

            let null_obj = json_stats
                .get(NULL_COUNT)
                .and_then(|v| v.as_object())
                .expect("stats JSON must contain nullCount object");
            assert_stats_struct_matches_json(null_count, null_obj, i, NULL_COUNT);

            total_num_records += num_records.value(i);
            file_count += 1;
        }
    }

    // The builder writes one data commit (v=1; v=0 is create-table with no data) of one
    // file with the default 10 rows.
    assert_eq!(file_count, 1, "Should have processed exactly one file");
    assert_eq!(total_num_records, 10, "Should have exactly 10 numRecords");
}
