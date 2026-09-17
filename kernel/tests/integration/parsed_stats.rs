//! Integration tests for parsed-stats output.

use std::sync::Arc;

use delta_kernel::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS, STATS_PARSED};
use delta_kernel::arrow::array::{
    Array, AsArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::arrow::util::display::{ArrayFormatter, FormatOptions};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::parquet::variant::VariantBuilder;
use delta_kernel::scan::StatsOptions;
use delta_kernel::schema::{schema_ref, DataType};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::Snapshot;
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{unpartitioned, version_latest, FeatureSet, LogState, TableConfig};
use test_utils::{
    add_commit, create_table_and_load_snapshot, get_column, test_context, test_table_setup_mt,
};
use url::Url;

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
                let format_options = FormatOptions::default()
                    .with_display_error(true)
                    .with_timestamp_format(Some("%Y-%m-%dT%H:%M:%S%.3f"))
                    .with_timestamp_tz_format(Some("%Y-%m-%dT%H:%M:%S%.3fZ"));
                let formatter = ArrayFormatter::try_new(col.as_ref(), &format_options)
                    .unwrap_or_else(|e| panic!("{path}: cannot build formatter: {e}"));
                let actual = formatter.value(row_idx).to_string();
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

#[test]
fn json_stats_truncate_timestamps_to_milliseconds() {
    let (engine, snapshot, _table) = test_context!(
        LogState::with_latest_version(1).with_checkpoint_at([1]),
        FeatureSet::empty(),
        unpartitioned(),
        TableConfig::new().write_stats_as_struct(true),
        version_latest(),
    );

    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .build()
        .unwrap();

    let mut checked = 0;
    for scan_metadata in scan.scan_metadata(&engine).unwrap() {
        let (underlying_data, selection_vector) = scan_metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();
        let filtered_batch =
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();
        let stats_json = get_column!(filtered_batch, "stats", StringArray);

        for i in 0..filtered_batch.num_rows() {
            let json: serde_json::Value = serde_json::from_str(stats_json.value(i)).unwrap();
            for bound in [MIN_VALUES, MAX_VALUES] {
                for (col, rendered) in json[bound].as_object().unwrap() {
                    let Some(ts) = rendered.as_str().filter(|s| s.contains('T')) else {
                        continue; // not a timestamp column
                    };
                    let fraction = ts
                        .trim_end_matches('Z')
                        .rsplit_once('.')
                        .unwrap_or_else(|| panic!("{bound}.{col} = {ts:?} has no fraction"))
                        .1;
                    assert_eq!(fraction.len(), 3, "{bound}.{col} = {ts:?}");
                    // The builder's values end in .298677, which floors to .298 rather than
                    // rounding to .299.
                    assert!(ts.ends_with(".298Z") || ts.ends_with(".298"), "{ts:?}");
                    checked += 1;
                }
            }
        }
    }
    assert!(checked > 0, "no timestamp stats were checked");
}

/// The VARIANT bound must survive the full log round trip: kernel writes it into `stats_parsed`
/// when it checkpoints, and must still recognize its own checkpoint as carrying usable parsed stats
/// when it reads it back. `writeStatsAsJson=false` removes the JSON fallback, so a checkpoint whose
/// `stats_parsed` kernel rejects leaves every statistic NULL and data skipping stops pruning.
#[tokio::test(flavor = "multi_thread")]
async fn variant_min_max_survives_a_struct_stats_only_checkpoint(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! {
        nullable "id": LONG,
        nullable "v": (DataType::unshredded_variant()),
    };
    let _ = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[
            ("delta.checkpoint.writeStatsAsStruct", "true"),
            ("delta.checkpoint.writeStatsAsJson", "false"),
        ],
    )?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());

    // The statistic is a variant object keyed by shredded path, carried in the stats JSON as the
    // Z85 encoding of `metadata || value` zero-filled to a four-byte group.
    let (metadata, value) = {
        let mut builder = VariantBuilder::new();
        let mut object = builder.new_object();
        object.insert("$.a", 1i32);
        object.finish();
        builder.finish()
    };
    let mut combined = [metadata.as_slice(), value.as_slice()].concat();
    combined.resize(combined.len().next_multiple_of(4), 0);
    let encoded = z85::encode(&combined);
    let stats = format!(
        r#"{{"numRecords":10,"minValues":{{"id":1,"v":"{encoded}"}},"maxValues":{{"id":9,"v":"{encoded}"}},"nullCount":{{"id":0,"v":0}},"tightBounds":true}}"#
    );
    let stats = serde_json::Value::String(stats).to_string();
    let commit = format!(
        r#"{{"commitInfo":{{"timestamp":1700000000000,"operation":"WRITE","version":1}}}}
{{"add":{{"path":"part-0.parquet","size":1024,"modificationTime":1700000000000,"dataChange":true,"partitionValues":{{}},"stats":{stats}}}}}"#
    );
    add_commit(&table_url.to_string(), store.as_ref(), 1, commit).await?;

    Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())?
        .checkpoint(engine.as_ref(), None)?;

    // Reload past the checkpoint and read the parsed stats back. `all_struct` requests no JSON
    // stats, and the checkpoint carries none either, so `stats_parsed` is the only source left.
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all_struct())
        .build()?;

    let mut files = 0;
    for scan_metadata in scan.scan_metadata(engine.as_ref())? {
        let (data, selection_vector) = scan_metadata?.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selection_vector))?;

        let stats_parsed = get_column!(batch, STATS_PARSED, StructArray);
        let num_records = get_column!(stats_parsed, NUM_RECORDS, Int64Array);
        for bound in [MIN_VALUES, MAX_VALUES] {
            let bounds = get_column!(stats_parsed, bound, StructArray);
            let id = get_column!(bounds, "id", Int64Array);
            let variant = get_column!(bounds, "v", StructArray);
            let variant_bytes = |field| {
                variant
                    .column_by_name(field)
                    .unwrap_or_else(|| panic!("{bound}.v should be the variant's physical struct"))
                    .as_binary::<i32>()
                    .value(0)
                    .to_vec()
            };
            for row in 0..batch.num_rows() {
                assert_eq!(num_records.value(row), 10, "{bound}: numRecords");
                assert!(!id.is_null(row), "{bound}: id bound should not be null");
                assert!(!variant.is_null(row), "{bound}: variant bound was dropped");
                assert_eq!(variant_bytes("metadata"), metadata, "{bound}.v.metadata");
                assert_eq!(variant_bytes("value"), value, "{bound}.v.value");
                files += 1;
            }
        }
    }
    assert_eq!(files, 2, "one file, checked for both bounds");

    Ok(())
}
