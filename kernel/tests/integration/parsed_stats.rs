//! Integration test for `Scan::include_all_stats_columns` covering nested-struct schemas.
//!
//! Builds a table at runtime via kernel APIs with
//! `delta.checkpoint.writeStatsAsStruct=true` so the checkpoint carries `stats_parsed`,
//! then verifies the parsed-stats struct column matches the JSON `stats` string row-by-row.
//! Parameterized over flat-primitive and nested-struct schemas, where the nested case is
//! the new coverage added for issue #1766 (paths like `minValues.info.age`).
//!
//! Complementary to the unit test `test_scan_metadata_with_stats_columns` in
//! `kernel/src/scan/tests.rs`, which exercises a different code path: reading a
//! pre-existing Spark-written checkpoint with `stats_parsed` already on disk.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS};
use delta_kernel::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use rstest::rstest;
use test_utils::{create_table_and_load_snapshot, test_table_setup_mt, write_batch_to_table};

/// Flat-primitive fixture: schema `id: long, val: long`, one batch of three rows.
fn flat_fixture() -> (SchemaRef, Vec<RecordBatch>) {
    let schema: SchemaRef = Arc::new(
        StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("val", DataType::LONG),
        ])
        .unwrap(),
    );
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow().unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10i64, 20, 30])) as ArrayRef,
        ],
    )
    .unwrap();
    (schema, vec![batch])
}

/// Nested-struct fixture: schema `id: long, info: struct<age: long, salary: long>`, one batch
/// of three rows. Exercises the recursive stats path (`minValues.info.age`, etc.).
fn nested_struct_fixture() -> (SchemaRef, Vec<RecordBatch>) {
    let info_type = DataType::try_struct_type([
        StructField::nullable("age", DataType::LONG),
        StructField::nullable("salary", DataType::LONG),
    ])
    .unwrap();
    let schema: SchemaRef = Arc::new(
        StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("info", info_type),
        ])
        .unwrap(),
    );
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow().unwrap();
    let info_fields = match arrow_schema.field_with_name("info").unwrap().data_type() {
        ArrowDataType::Struct(fields) => fields.clone(),
        other => panic!("info field should be a struct, got {other:?}"),
    };
    let info_array = StructArray::new(
        info_fields,
        vec![
            Arc::new(Int64Array::from(vec![21i64, 35, 48])) as ArrayRef,
            Arc::new(Int64Array::from(vec![50_000i64, 80_000, 120_000])) as ArrayRef,
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
            Arc::new(info_array) as ArrayRef,
        ],
    )
    .unwrap();
    (schema, vec![batch])
}

/// Validate that JSON stats object values match the corresponding parsed struct array.
///
/// Recurses into nested struct sub-fields so paths like `minValues.info.age` are checked.
/// `field_path` is the dotted path used in assertion messages (e.g. `"minValues"`).
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

#[rstest]
#[case::flat_schema(flat_fixture)]
#[case::nested_struct(nested_struct_fixture)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_metadata_with_nested_stats_struct_columns(
    #[case] fixture: fn() -> (SchemaRef, Vec<RecordBatch>),
) -> Result<(), Box<dyn std::error::Error>> {
    const STATS_PARSED_COL: &str = "stats_parsed";

    let (schema, batches) = fixture();
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let mut snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.checkpoint.writeStatsAsStruct", "true")],
    )?;
    for batch in batches {
        snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    }
    let (_checkpoint_result, snapshot) = snapshot.checkpoint(engine.as_ref(), None)?;

    let scan = snapshot
        .scan_builder()
        .include_all_stats_columns()
        .build()?;

    let scan_metadata_results: Vec<_> = scan
        .scan_metadata(engine.as_ref())?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        scan_metadata_results.len(),
        1,
        "Expected exactly one scan_metadata batch for a single-Add table"
    );

    let mut total_num_records: i64 = 0;
    let mut file_count = 0;

    for scan_metadata in scan_metadata_results {
        let (underlying_data, selection_vector) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)?.into();
        let filtered_batch = filter_record_batch(&batch, &BooleanArray::from(selection_vector))?;

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

    assert_eq!(file_count, 1, "Expected exactly one validated Add row");
    assert_eq!(
        total_num_records, 3,
        "Each fixture writes one batch of three rows"
    );
    Ok(())
}
