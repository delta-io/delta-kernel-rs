use std::path::PathBuf;
use std::sync::Arc;

use crate::arrow::array::{Array, BooleanArray, Int64Array, StringArray, StructArray};
use crate::arrow::compute::filter_record_batch;
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::arrow::record_batch::RecordBatch;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::expressions::{column_expr, column_pred, Expression as Expr, Predicate as Pred};
use crate::scan::state::ScanFile;
use crate::schema::{ColumnMetadataKey, DataType, StructField, StructType};
use crate::{EngineData, Snapshot};

use super::*;

/// Helper macro to extract a typed column from a RecordBatch or StructArray.
macro_rules! get_column {
    ($source:expr, $name:expr, $ty:ty) => {
        $source
            .column_by_name($name)
            .unwrap_or_else(|| panic!("should have column '{}'", $name))
            .as_any()
            .downcast_ref::<$ty>()
            .unwrap_or_else(|| panic!("column '{}' should be {}", $name, stringify!($ty)))
    };
}

#[test]
fn test_static_skipping() {
    const NULL: Pred = Pred::null_literal();
    let test_cases = [
        (false, column_pred!("a")),
        (true, Pred::literal(false)),
        (false, Pred::literal(true)),
        (true, NULL),
        (true, Pred::and(column_pred!("a"), Pred::literal(false))),
        (false, Pred::or(column_pred!("a"), Pred::literal(true))),
        (false, Pred::or(column_pred!("a"), Pred::literal(false))),
        (false, Pred::lt(column_expr!("a"), Expr::literal(10))),
        (false, Pred::lt(Expr::literal(10), Expr::literal(100))),
        (true, Pred::gt(Expr::literal(10), Expr::literal(100))),
        (true, Pred::and(NULL, column_pred!("a"))),
    ];
    for (should_skip, predicate) in test_cases {
        assert_eq!(
            can_statically_skip_all_files(&predicate),
            should_skip,
            "Failed for predicate: {predicate:#?}"
        );
    }
}

#[test]
fn test_physical_predicate() {
    let logical_schema = StructType::new_unchecked(vec![
        StructField::nullable("a", DataType::LONG),
        StructField::nullable("b", DataType::LONG).with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            "phys_b",
        )]),
        StructField::nullable("phys_b", DataType::LONG).with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            "phys_c",
        )]),
        StructField::nullable(
            "nested",
            StructType::new_unchecked(vec![
                StructField::nullable("x", DataType::LONG),
                StructField::nullable("y", DataType::LONG).with_metadata([(
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    "phys_y",
                )]),
            ]),
        ),
        StructField::nullable(
            "mapped",
            StructType::new_unchecked(vec![StructField::nullable("n", DataType::LONG)
                .with_metadata([(
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    "phys_n",
                )])]),
        )
        .with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            "phys_mapped",
        )]),
    ]);

    // NOTE: We break several column mapping rules here because they don't matter for this
    // test. For example, we do not provide field ids, and not all columns have physical names.
    let test_cases = [
        (Pred::literal(true), Some(PhysicalPredicate::None)),
        (Pred::literal(false), Some(PhysicalPredicate::StaticSkipAll)),
        (column_pred!("x"), None), // no such column
        (
            column_pred!("a"),
            Some(PhysicalPredicate::Some(
                column_pred!("a").into(),
                StructType::new_unchecked(vec![StructField::nullable("a", DataType::LONG)]).into(),
            )),
        ),
        (
            column_pred!("b"),
            Some(PhysicalPredicate::Some(
                column_pred!("phys_b").into(),
                StructType::new_unchecked(vec![StructField::nullable("phys_b", DataType::LONG)
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_b",
                    )])])
                .into(),
            )),
        ),
        (
            column_pred!("nested.x"),
            Some(PhysicalPredicate::Some(
                column_pred!("nested.x").into(),
                StructType::new_unchecked(vec![StructField::nullable(
                    "nested",
                    StructType::new_unchecked(vec![StructField::nullable("x", DataType::LONG)]),
                )])
                .into(),
            )),
        ),
        (
            column_pred!("nested.y"),
            Some(PhysicalPredicate::Some(
                column_pred!("nested.phys_y").into(),
                StructType::new_unchecked(vec![StructField::nullable(
                    "nested",
                    StructType::new_unchecked(vec![StructField::nullable(
                        "phys_y",
                        DataType::LONG,
                    )
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_y",
                    )])]),
                )])
                .into(),
            )),
        ),
        (
            column_pred!("mapped.n"),
            Some(PhysicalPredicate::Some(
                column_pred!("phys_mapped.phys_n").into(),
                StructType::new_unchecked(vec![StructField::nullable(
                    "phys_mapped",
                    StructType::new_unchecked(vec![StructField::nullable(
                        "phys_n",
                        DataType::LONG,
                    )
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_n",
                    )])]),
                )
                .with_metadata([(
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    "phys_mapped",
                )])])
                .into(),
            )),
        ),
        (
            Pred::and(column_pred!("mapped.n"), Pred::literal(true)),
            Some(PhysicalPredicate::Some(
                Pred::and(column_pred!("phys_mapped.phys_n"), Pred::literal(true)).into(),
                StructType::new_unchecked(vec![StructField::nullable(
                    "phys_mapped",
                    StructType::new_unchecked(vec![StructField::nullable(
                        "phys_n",
                        DataType::LONG,
                    )
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_n",
                    )])]),
                )
                .with_metadata([(
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    "phys_mapped",
                )])])
                .into(),
            )),
        ),
        (
            Pred::and(column_pred!("mapped.n"), Pred::literal(false)),
            Some(PhysicalPredicate::StaticSkipAll),
        ),
    ];

    for (predicate, expected) in test_cases {
        let result =
            PhysicalPredicate::try_new(&predicate, &logical_schema, ColumnMappingMode::Name).ok();
        assert_eq!(
            result, expected,
            "Failed for predicate: {predicate:#?}, expected {expected:#?}, got {result:#?}"
        );
    }
}

fn get_files_for_scan(scan: Scan, engine: &dyn Engine) -> DeltaResult<Vec<String>> {
    let scan_metadata_iter = scan.scan_metadata(engine)?;
    fn scan_metadata_callback(paths: &mut Vec<String>, scan_file: ScanFile) {
        paths.push(scan_file.path.to_string());
        assert!(scan_file.dv_info.deletion_vector.is_none());
    }
    let mut files = vec![];
    for res in scan_metadata_iter {
        let scan_metadata = res?;
        files = scan_metadata.visit_scan_files(files, scan_metadata_callback)?;
    }
    Ok(files)
}

#[test]
fn test_scan_metadata_paths() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let files = get_files_for_scan(scan, &engine).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0],
        "part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"
    );
}

#[test_log::test]
fn test_scan_metadata() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let files: Vec<Box<dyn EngineData>> = scan.execute(engine).unwrap().try_collect().unwrap();

    assert_eq!(files.len(), 1);
    let num_rows = files[0].as_ref().len();
    assert_eq!(num_rows, 10)
}

#[test_log::test]
fn test_scan_metadata_from_same_version() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let version = snapshot.version();
    let scan = snapshot.scan_builder().build().unwrap();
    let files: Vec<_> = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .map_ok(|ScanMetadata { scan_files, .. }| {
            let (underlying_data, selection_vector) = scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
                .unwrap()
                .into();
            let filtered_batch =
                filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();
            Box::new(ArrowEngineData::from(filtered_batch)) as Box<dyn EngineData>
        })
        .try_collect()
        .unwrap();
    let new_files: Vec<_> = scan
        .scan_metadata_from(engine.as_ref(), version, files, None)
        .unwrap()
        .try_collect()
        .unwrap();

    assert_eq!(new_files.len(), 1);
}

// reading v0 with 3 files.
// updating to v1 with 3 more files added.
#[test_log::test]
fn test_scan_metadata_from_with_update() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url.clone())
        .at_version(0)
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let files: Vec<_> = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .map_ok(|ScanMetadata { scan_files, .. }| {
            let (underlying_data, selection_vector) = scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
                .unwrap()
                .into();
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap()
        })
        .try_collect()
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].num_rows(), 3);

    let files: Vec<_> = files
        .into_iter()
        .map(|b| Box::new(ArrowEngineData::from(b)) as Box<dyn EngineData>)
        .collect();
    let snapshot = Snapshot::builder_for(url)
        .at_version(1)
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let new_files: Vec<_> = scan
        .scan_metadata_from(engine.as_ref(), 0, files, None)
        .unwrap()
        .map_ok(|ScanMetadata { scan_files, .. }| {
            let (underlying_data, selection_vector) = scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
                .unwrap()
                .into();
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap()
        })
        .try_collect()
        .unwrap();
    assert_eq!(new_files.len(), 2);
    assert_eq!(new_files[0].num_rows(), 3);
    assert_eq!(new_files[1].num_rows(), 3);
}

#[test]
fn test_get_partition_value() {
    let cases = [
        (
            "string",
            PrimitiveType::String,
            Scalar::String("string".to_string()),
        ),
        ("123", PrimitiveType::Integer, Scalar::Integer(123)),
        ("1234", PrimitiveType::Long, Scalar::Long(1234)),
        ("12", PrimitiveType::Short, Scalar::Short(12)),
        ("1", PrimitiveType::Byte, Scalar::Byte(1)),
        ("1.1", PrimitiveType::Float, Scalar::Float(1.1)),
        ("10.10", PrimitiveType::Double, Scalar::Double(10.1)),
        ("true", PrimitiveType::Boolean, Scalar::Boolean(true)),
        ("2024-01-01", PrimitiveType::Date, Scalar::Date(19723)),
        ("1970-01-01", PrimitiveType::Date, Scalar::Date(0)),
        (
            "1970-01-01 00:00:00",
            PrimitiveType::Timestamp,
            Scalar::Timestamp(0),
        ),
        (
            "1970-01-01 00:00:00.123456",
            PrimitiveType::Timestamp,
            Scalar::Timestamp(123456),
        ),
        (
            "1970-01-01 00:00:00.123456789",
            PrimitiveType::Timestamp,
            Scalar::Timestamp(123456),
        ),
    ];

    for (raw, data_type, expected) in &cases {
        let value = crate::transforms::parse_partition_value_raw(
            Some(&raw.to_string()),
            &DataType::Primitive(data_type.clone()),
        )
        .unwrap();
        assert_eq!(value, *expected);
    }
}

#[test]
fn test_replay_for_scan_metadata() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
    let url = url::Url::from_directory_path(path.unwrap()).unwrap();
    let engine = SyncEngine::new();

    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let result = scan.replay_for_scan_metadata(&engine).unwrap();
    let data: Vec<_> = result.actions.try_collect().unwrap();
    // No predicate pushdown attempted, because at most one part of a multi-part checkpoint
    // could be skipped when looking for adds/removes.
    //
    // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
    assert_eq!(data.len(), 5);
}

#[test]
fn test_data_row_group_skipping() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
    let url = url::Url::from_directory_path(path.unwrap()).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    // No predicate pushdown attempted, so the one data file should be returned.
    //
    // NOTE: The data file contains only five rows -- near guaranteed to produce one row group.
    let scan = snapshot.clone().scan_builder().build().unwrap();
    let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
    assert_eq!(data.len(), 1);

    // Ineffective predicate pushdown attempted, so the one data file should be returned.
    let int_col = column_expr!("numeric.ints.int32");
    let value = Expr::literal(1000i32);
    let predicate = Arc::new(int_col.clone().gt(value.clone()));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
    assert_eq!(data.len(), 1);

    // TODO(#860): we disable predicate pushdown until we support row indexes. Update this test
    // accordingly after support is reintroduced.
    //
    // Effective predicate pushdown, so no data files should be returned. BUT since we disabled
    // predicate pushdown, the one data file is still returned.
    let predicate = Arc::new(int_col.lt(value));
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let data: Vec<_> = scan.execute(engine).unwrap().try_collect().unwrap();
    assert_eq!(data.len(), 1);
}

#[test]
fn test_missing_column_row_group_skipping() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
    let url = url::Url::from_directory_path(path.unwrap()).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    // Predicate over a logically valid but physically missing column. No data files should be
    // returned because the column is inferred to be all-null.
    //
    // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 - This
    // optimization is currently disabled, so the one data file is still returned.
    let predicate = Arc::new(column_expr!("missing").lt(Expr::literal(1000i64)));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
    assert_eq!(data.len(), 1);

    // Predicate over a logically missing column fails the scan
    let predicate = Arc::new(column_expr!("numeric.ints.invalid").lt(Expr::literal(1000)));
    snapshot
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .expect_err("unknown column");
}

#[test_log::test]
fn test_scan_with_checkpoint() -> DeltaResult<()> {
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/data/with_checkpoint_no_last_checkpoint/",
    ))?;

    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let scan = snapshot.scan_builder().build()?;
    let files = get_files_for_scan(scan, &engine)?;
    // test case:
    //
    // commit0:     P and M, no add/remove
    // commit1:     add file-ad1
    // commit2:     remove file-ad1, add file-a19
    // checkpoint2: remove file-ad1, add file-a19
    // commit3:     remove file-a19, add file-70b
    //
    // thus replay should produce only file-70b
    assert_eq!(
        files,
        vec!["part-00000-70b1dcdf-0236-4f63-a072-124cdbafd8a0-c000.snappy.parquet"]
    );
    Ok(())
}

/// Helper to validate that JSON stats object values match the corresponding parsed struct array.
fn assert_stats_struct_matches_json(
    struct_array: &StructArray,
    json_object: &serde_json::Map<String, serde_json::Value>,
    row_idx: usize,
    field_name: &str,
) {
    for (col_name, json_val) in json_object {
        let Some(col) = struct_array.column_by_name(col_name) else {
            continue;
        };
        if col.is_null(row_idx) {
            continue;
        }
        // Currently only validates Int64 columns (the table has integer stats)
        if let Some(int_col) = col.as_any().downcast_ref::<Int64Array>() {
            assert_eq!(
                json_val.as_i64().unwrap(),
                int_col.value(row_idx),
                "{}.{} mismatch at row {}",
                field_name,
                col_name,
                row_idx
            );
        }
    }
}

/// Test that `with_stats_columns(vec![])` outputs parsed stats in scan_metadata batches.
/// Uses a table with a checkpoint that contains stats_parsed for e2e verification.
#[test]
fn test_scan_metadata_with_stats_columns() {
    const STATS_PARSED_COL: &str = "stats_parsed";

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    let scan = snapshot
        .scan_builder()
        .include_stats_columns()
        .build()
        .unwrap();

    let scan_metadata_results: Vec<_> = scan
        .scan_metadata(engine.as_ref())
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

        // Verify stats_parsed schema
        let schema = filtered_batch.schema();
        let field = schema
            .field_with_name(STATS_PARSED_COL)
            .expect("Schema should contain stats_parsed column");
        assert!(
            matches!(field.data_type(), ArrowDataType::Struct(_)),
            "stats_parsed should be a struct type, got: {:?}",
            field.data_type()
        );

        // Extract stats_parsed struct array
        let stats_parsed = get_column!(filtered_batch, STATS_PARSED_COL, StructArray);
        let num_records = get_column!(stats_parsed, "numRecords", Int64Array);
        let min_values = get_column!(stats_parsed, "minValues", StructArray);
        let max_values = get_column!(stats_parsed, "maxValues", StructArray);
        let null_count = get_column!(stats_parsed, "nullCount", StructArray);

        // Extract JSON stats column
        let stats_json = get_column!(filtered_batch, "stats", StringArray);

        // Validate each row: JSON stats should match structured stats
        for i in 0..stats_json.len() {
            if stats_parsed.is_null(i) || stats_json.is_null(i) {
                continue;
            }

            let json_stats: serde_json::Value =
                serde_json::from_str(stats_json.value(i)).expect("stats JSON should be valid");

            // Validate numRecords
            if let Some(json_num) = json_stats.get("numRecords").and_then(|v| v.as_i64()) {
                assert_eq!(
                    json_num,
                    num_records.value(i),
                    "numRecords mismatch at row {i}"
                );
            }

            // Validate minValues, maxValues, nullCount
            if let Some(obj) = json_stats.get("minValues").and_then(|v| v.as_object()) {
                assert_stats_struct_matches_json(min_values, obj, i, "minValues");
            }
            if let Some(obj) = json_stats.get("maxValues").and_then(|v| v.as_object()) {
                assert_stats_struct_matches_json(max_values, obj, i, "maxValues");
            }
            if let Some(obj) = json_stats.get("nullCount").and_then(|v| v.as_object()) {
                assert_stats_struct_matches_json(null_count, obj, i, "nullCount");
            }

            total_num_records += num_records.value(i);
            file_count += 1;
        }
    }

    assert!(file_count > 0, "Should have processed at least one file");
    assert!(total_num_records > 0, "Should have non-zero numRecords");
    println!(
        "Verified {file_count} files with total {total_num_records} records from stats_parsed"
    );
}

/// Test that data skipping works correctly with pre-parsed stats from a checkpoint.
///
/// The parsed-stats test table has a checkpoint at version 3 (containing stats_parsed) and
/// JSON commits at versions 4-5. This test exercises both code paths:
/// - Checkpoint batches: stats_parsed is read directly from the transformed batch
/// - JSON log batches: stats are parsed from JSON via the transform expression
///
/// Table layout (6 files, each 100 records):
///   File 1: id [1-100],   File 2: id [101-200], File 3: id [201-300]
///   File 4: id [301-400], File 5: id [401-500], File 6: id [501-600]
#[test]
fn test_data_skipping_with_parsed_stats() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    // Predicate: id > 400 should skip files 1-4 (max id: 100, 200, 300, 400) and keep files 5-6
    let predicate = Arc::new(Pred::gt(column_expr!("id"), Expr::literal(400i64)));
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();

    let scan_metadata_results: Vec<_> = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let mut selected_file_count = 0;
    for scan_metadata in &scan_metadata_results {
        let selection_vector = scan_metadata.scan_files.selection_vector();
        selected_file_count += selection_vector
            .iter()
            .filter(|&&selected| selected)
            .count();
    }

    assert_eq!(
        selected_file_count, 2,
        "Data skipping with parsed stats should keep only 2 files (id [401-500] and [501-600])"
    );
}

/// Test that `with_stats_columns` cannot be used with `with_predicate`.
/// See [#1751] for tracking.
/// [#1751]: https://github.com/delta-io/delta-kernel-rs/issues/1751
#[test]
fn test_scan_metadata_stats_columns_with_predicate_errors() {
    // Use the parsed-stats table
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    // Build scan with both predicate (that references a column) and stats_columns should error
    // Note: Pred::literal(true) has no column references, so it becomes PhysicalPredicate::None
    let predicate = Arc::new(column_pred!("id")); // References 'id' column
    let result = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .include_stats_columns()
        .build();

    assert!(
        result.is_err(),
        "Should error when using both predicate and stats_columns"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("predicate") || err.to_string().contains("stats_columns"),
        "Error message should mention predicate or stats_columns: {}",
        err
    );
}
