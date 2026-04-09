//! Integration tests for writing to partitioned Delta tables.
//!
//! Verifies that the full write pipeline works end-to-end for every supported partition
//! column type: create table with partition columns, write data with typed `Scalar` partition
//! values via `partitioned_write_context`, and read it back via scan.
//!
//! Tests are parameterized over column mapping modes (None, Name, Id) via rstest.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::Snapshot;
use rstest::rstest;
use test_utils::{read_scan, test_table_setup_mt};
use url::Url;

// ==============================================================================
// Tests
// ==============================================================================

/// Writes one row with a normal everyday value for every partition column type, reads it
/// back, checkpoints, reloads from checkpoint, and verifies the values survive both paths.
#[rstest]
#[case::cm_none(ColumnMappingMode::None)]
#[case::cm_name(ColumnMappingMode::Name)]
#[case::cm_id(ColumnMappingMode::Id)]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_partitioned_normal_values_roundtrip(
    #[case] cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    // ===== Step 1: Create table and write one row with normal partition values. =====
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = all_types_schema();
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    let snapshot = create_all_types_table(&table_path, engine.as_ref(), cm_mode)?;
    assert_eq!(snapshot.table_configuration().partition_columns().len(), 13);

    let batch = RecordBatch::try_new(arrow_schema, normal_arrow_columns())?;
    let snapshot = test_utils::write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        batch,
        normal_partition_values()?,
    )
    .await?;

    // ===== Step 2: Validate add.path structure in the commit log JSON. =====
    let adds = read_add_actions_json(&table_path, 1)?;
    assert_eq!(adds.len(), 1, "should have exactly one add action");
    let add = &adds[0];
    let path = add["path"].as_str().unwrap();
    let rel_path = strip_table_root(path, &table_path);

    match cm_mode {
        ColumnMappingMode::None => {
            // Hive-style path with Hive encoding: colons -> %3A, spaces -> %20.
            let expected_prefix = "\
                p_string=hello/p_int=42/p_long=9876543210/p_short=7/\
                p_byte=3/p_float=1.25/p_double=99.99/p_boolean=true/p_date=2025-03-31/\
                p_timestamp=2025-03-31T15%3A30%3A00.123456Z/p_decimal=123.45/\
                p_binary=Hello/p_timestamp_ntz=2025-03-31%2015%3A30%3A00.123456/";
            assert!(
                rel_path.starts_with(expected_prefix),
                "CM off: relative path mismatch.\n  \
                 expected: {expected_prefix}<uuid>.parquet\n  got: {rel_path}"
            );
            assert!(rel_path.ends_with(".parquet"));
        }
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            // Random 2-char prefix: <2char>/<uuid>.parquet
            let segments: Vec<&str> = rel_path.split('/').collect();
            assert_eq!(
                segments.len(),
                2,
                "CM on: path should be <prefix>/<file>, got: {rel_path}"
            );
            assert_eq!(segments[0].len(), 2, "prefix should be 2 chars");
            assert!(segments[0].chars().all(|c| c.is_ascii_alphanumeric()));
            assert!(segments[1].ends_with(".parquet"));
        }
    }

    // ===== Step 3: Validate add.partitionValues content. =====
    let pv = add["partitionValues"].as_object().unwrap();
    match cm_mode {
        ColumnMappingMode::None => {
            // Keys are logical column names, values are serialized strings.
            for (key, val) in EXPECTED_NORMAL_PVS {
                assert_eq!(
                    pv.get(*key).and_then(|v| v.as_str()),
                    Some(*val),
                    "partitionValues[{key}] mismatch"
                );
            }
        }
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            // Keys are physical names. Look up via schema and verify each value.
            let logical_schema = snapshot.schema();
            for (logical_key, expected_val) in EXPECTED_NORMAL_PVS {
                let field = logical_schema.field(logical_key).unwrap();
                let physical_key = field.physical_name(cm_mode);
                assert_eq!(
                    pv.get(physical_key).and_then(|v| v.as_str()),
                    Some(*expected_val),
                    "partitionValues[{physical_key}] (logical: {logical_key}) mismatch"
                );
            }
        }
    }

    // ===== Step 4: Read data back via scan and verify all column values. =====
    let sorted = read_sorted(&snapshot, engine.clone())?;
    assert_normal_values(&sorted);

    // ===== Step 5: Checkpoint, reload snapshot from checkpoint, read again. =====
    snapshot.checkpoint(engine.as_ref())?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot_after_cp = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let sorted = read_sorted(&snapshot_after_cp, engine)?;
    assert_normal_values(&sorted);

    Ok(())
}

/// Writes one row with all null partition values, reads it back, checkpoints, reloads
/// from checkpoint, and verifies every partition column is null in both reads.
#[rstest]
#[case::cm_none(ColumnMappingMode::None)]
#[case::cm_name(ColumnMappingMode::Name)]
#[case::cm_id(ColumnMappingMode::Id)]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_partitioned_null_values_roundtrip(
    #[case] cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    // ===== Step 1: Create table and write one row with all-null partition values. =====
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = all_types_schema();
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    let snapshot = create_all_types_table(&table_path, engine.as_ref(), cm_mode)?;

    let batch = RecordBatch::try_new(arrow_schema, null_arrow_columns())?;
    let snapshot = test_utils::write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        batch,
        null_partition_values()?,
    )
    .await?;

    // ===== Step 2: Validate add.path structure in the commit log JSON. =====
    let adds = read_add_actions_json(&table_path, 1)?;
    assert_eq!(adds.len(), 1, "should have exactly one add action");
    let add = &adds[0];
    let rel_path = strip_table_root(add["path"].as_str().unwrap(), &table_path);

    let hdp = "__HIVE_DEFAULT_PARTITION__";
    match cm_mode {
        ColumnMappingMode::None => {
            // Every partition column should use HIVE_DEFAULT_PARTITION in the path.
            let expected_prefix = hive_prefix(PARTITION_COLS, hdp);
            assert!(
                rel_path.starts_with(&expected_prefix),
                "CM off null: relative path mismatch.\n  \
                 expected: {expected_prefix}<uuid>.parquet\n  got: {rel_path}"
            );
        }
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            let segments: Vec<&str> = rel_path.split('/').collect();
            assert_eq!(
                segments.len(),
                2,
                "CM on: path should be <prefix>/<file>, got: {rel_path}"
            );
            assert_eq!(segments[0].len(), 2);
            assert!(segments[0].chars().all(|c| c.is_ascii_alphanumeric()));
        }
    }

    // ===== Step 3: Validate add.partitionValues content (all values should be JSON null). =====
    let pv = add["partitionValues"].as_object().unwrap();
    assert_eq!(pv.len(), PARTITION_COLS.len());
    for val in pv.values() {
        assert!(
            val.is_null(),
            "all partition values should be null, got: {val}"
        );
    }

    // ===== Step 4: Read data back via scan and verify all partition columns are null. =====
    let sorted = read_sorted(&snapshot, engine.clone())?;
    assert_all_partition_columns_null(&sorted);

    // ===== Step 5: Checkpoint, reload snapshot from checkpoint, read again. =====
    snapshot.checkpoint(engine.as_ref())?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot_after_cp = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let sorted = read_sorted(&snapshot_after_cp, engine)?;
    assert_all_partition_columns_null(&sorted);

    Ok(())
}

// ==============================================================================
// Schema and partition column definitions
// ==============================================================================

fn all_types_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("value", DataType::INTEGER),
            StructField::nullable("p_string", DataType::STRING),
            StructField::nullable("p_int", DataType::INTEGER),
            StructField::nullable("p_long", DataType::LONG),
            StructField::nullable("p_short", DataType::SHORT),
            StructField::nullable("p_byte", DataType::BYTE),
            StructField::nullable("p_float", DataType::FLOAT),
            StructField::nullable("p_double", DataType::DOUBLE),
            StructField::nullable("p_boolean", DataType::BOOLEAN),
            StructField::nullable("p_date", DataType::DATE),
            StructField::nullable("p_timestamp", DataType::TIMESTAMP),
            StructField::nullable("p_decimal", DataType::decimal(10, 2).unwrap()),
            StructField::nullable("p_binary", DataType::BINARY),
            StructField::nullable("p_timestamp_ntz", DataType::TIMESTAMP_NTZ),
        ])
        .unwrap(),
    )
}

const PARTITION_COLS: &[&str] = &[
    "p_string",
    "p_int",
    "p_long",
    "p_short",
    "p_byte",
    "p_float",
    "p_double",
    "p_boolean",
    "p_date",
    "p_timestamp",
    "p_decimal",
    "p_binary",
    "p_timestamp_ntz",
];

// ==============================================================================
// Normal-values test data
// ==============================================================================

/// Arrow columns for one row with normal everyday values for all 13 partition types.
fn normal_arrow_columns() -> Vec<ArrayRef> {
    let ts = ts_to_micros("2025-03-31 15:30:00.123456");
    vec![
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(StringArray::from(vec!["hello"])),
        Arc::new(Int32Array::from(vec![42])),
        Arc::new(Int64Array::from(vec![9_876_543_210i64])),
        Arc::new(Int16Array::from(vec![7i16])),
        Arc::new(Int8Array::from(vec![3i8])),
        Arc::new(Float32Array::from(vec![1.25f32])),
        Arc::new(Float64Array::from(vec![99.99f64])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(Date32Array::from(vec![date_to_days("2025-03-31")])),
        ts_array(ts),
        decimal_array(12345, 10, 2),
        Arc::new(BinaryArray::from_vec(vec![b"Hello"])),
        ts_ntz_array(ts),
    ]
}

/// Typed partition values matching `normal_arrow_columns`.
fn normal_partition_values() -> Result<HashMap<String, Scalar>, Box<dyn std::error::Error>> {
    let ts = ts_to_micros("2025-03-31 15:30:00.123456");
    Ok(HashMap::from([
        ("p_string".into(), Scalar::String("hello".into())),
        ("p_int".into(), Scalar::Integer(42)),
        ("p_long".into(), Scalar::Long(9_876_543_210)),
        ("p_short".into(), Scalar::Short(7)),
        ("p_byte".into(), Scalar::Byte(3)),
        ("p_float".into(), Scalar::Float(1.25)),
        ("p_double".into(), Scalar::Double(99.99)),
        ("p_boolean".into(), Scalar::Boolean(true)),
        ("p_date".into(), Scalar::Date(date_to_days("2025-03-31"))),
        ("p_timestamp".into(), Scalar::Timestamp(ts)),
        ("p_decimal".into(), Scalar::decimal(12345, 10, 2)?),
        ("p_binary".into(), Scalar::Binary(b"Hello".to_vec())),
        ("p_timestamp_ntz".into(), Scalar::TimestampNtz(ts)),
    ]))
}

/// Expected serialized partition values (logical key -> serialized string) for normal data.
const EXPECTED_NORMAL_PVS: &[(&str, &str)] = &[
    ("p_string", "hello"),
    ("p_int", "42"),
    ("p_long", "9876543210"),
    ("p_short", "7"),
    ("p_byte", "3"),
    ("p_float", "1.25"),
    ("p_double", "99.99"),
    ("p_boolean", "true"),
    ("p_date", "2025-03-31"),
    ("p_timestamp", "2025-03-31T15:30:00.123456Z"),
    ("p_decimal", "123.45"),
    ("p_binary", "Hello"),
    ("p_timestamp_ntz", "2025-03-31 15:30:00.123456"),
];

// ==============================================================================
// Null-values test data
// ==============================================================================

/// Arrow columns for one row with all null partition values.
fn null_arrow_columns() -> Vec<ArrayRef> {
    vec![
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(Int32Array::from(vec![None::<i32>])),
        Arc::new(Int64Array::from(vec![None::<i64>])),
        Arc::new(Int16Array::from(vec![None::<i16>])),
        Arc::new(Int8Array::from(vec![None::<i8>])),
        Arc::new(Float32Array::from(vec![None::<f32>])),
        Arc::new(Float64Array::from(vec![None::<f64>])),
        Arc::new(BooleanArray::from(vec![None::<bool>])),
        Arc::new(Date32Array::from(vec![None::<i32>])),
        Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
        Arc::new(
            Decimal128Array::from(vec![None::<i128>])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
        Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>])),
    ]
}

/// Typed null partition values for all 13 partition columns.
fn null_partition_values() -> Result<HashMap<String, Scalar>, Box<dyn std::error::Error>> {
    Ok(HashMap::from([
        ("p_string".into(), Scalar::Null(DataType::STRING)),
        ("p_int".into(), Scalar::Null(DataType::INTEGER)),
        ("p_long".into(), Scalar::Null(DataType::LONG)),
        ("p_short".into(), Scalar::Null(DataType::SHORT)),
        ("p_byte".into(), Scalar::Null(DataType::BYTE)),
        ("p_float".into(), Scalar::Null(DataType::FLOAT)),
        ("p_double".into(), Scalar::Null(DataType::DOUBLE)),
        ("p_boolean".into(), Scalar::Null(DataType::BOOLEAN)),
        ("p_date".into(), Scalar::Null(DataType::DATE)),
        ("p_timestamp".into(), Scalar::Null(DataType::TIMESTAMP)),
        ("p_decimal".into(), Scalar::Null(DataType::decimal(10, 2)?)),
        ("p_binary".into(), Scalar::Null(DataType::BINARY)),
        (
            "p_timestamp_ntz".into(),
            Scalar::Null(DataType::TIMESTAMP_NTZ),
        ),
    ]))
}

// ==============================================================================
// Assertions
// ==============================================================================

/// Downcast column `$idx` to `$arr_ty` and assert `value(0)` equals `$expected`.
macro_rules! assert_col {
    ($batch:expr, $idx:expr, $arr_ty:ty, $expected:expr) => {
        assert_eq!(
            $batch
                .column($idx)
                .as_any()
                .downcast_ref::<$arr_ty>()
                .unwrap()
                .value(0),
            $expected,
            "column {} ({}) value mismatch",
            $idx,
            $batch.schema().field($idx).name()
        );
    };
}

/// Asserts the normal-values row reads back correctly (1 row, all 13 partition columns).
fn assert_normal_values(sorted: &RecordBatch) {
    let ts = ts_to_micros("2025-03-31 15:30:00.123456");
    assert_eq!(sorted.num_rows(), 1);
    assert_col!(sorted, 0, Int32Array, 1); // value
    assert_col!(sorted, 1, StringArray, "hello"); // p_string
    assert_col!(sorted, 2, Int32Array, 42); // p_int
    assert_col!(sorted, 3, Int64Array, 9_876_543_210i64); // p_long
    assert_col!(sorted, 4, Int16Array, 7i16); // p_short
    assert_col!(sorted, 5, Int8Array, 3i8); // p_byte
    assert!(
        (sorted
            .column(6)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0)
            - 1.25f32)
            .abs()
            < f32::EPSILON,
        "p_float mismatch"
    );
    assert!(
        (sorted
            .column(7)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            - 99.99f64)
            .abs()
            < f64::EPSILON,
        "p_double mismatch"
    );
    assert!(
        sorted
            .column(8)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0),
        "p_boolean should be true"
    );
    assert_col!(sorted, 9, Date32Array, date_to_days("2025-03-31")); // p_date
    assert_col!(sorted, 10, TimestampMicrosecondArray, ts); // p_timestamp
    assert_col!(sorted, 11, Decimal128Array, 12345); // p_decimal
    assert_eq!(
        // p_binary (slice comparison)
        sorted
            .column(12)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"Hello"
    );
    assert_col!(sorted, 13, TimestampMicrosecondArray, ts); // p_timestamp_ntz
}

/// Asserts all partition columns (indices 1-13) are null for the single row.
fn assert_all_partition_columns_null(sorted: &RecordBatch) {
    assert_eq!(sorted.num_rows(), 1);
    for col_idx in 1..=13 {
        assert!(
            sorted.column(col_idx).is_null(0),
            "partition column at index {col_idx} ({}) should be null",
            sorted.schema().field(col_idx).name()
        );
    }
}

// ==============================================================================
// Table setup and utility helpers
// ==============================================================================

fn cm_mode_str(mode: ColumnMappingMode) -> &'static str {
    match mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::Name => "name",
    }
}

fn create_all_types_table(
    table_path: &str,
    engine: &dyn delta_kernel::Engine,
    cm_mode: ColumnMappingMode,
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let schema = all_types_schema();
    let mut builder = create_table(table_path, schema, "test/1.0")
        .with_data_layout(DataLayout::partitioned(PARTITION_COLS.to_vec()));
    if cm_mode != ColumnMappingMode::None {
        builder =
            builder.with_table_properties([("delta.columnMapping.mode", cm_mode_str(cm_mode))]);
    }
    let _ = builder
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?;
    let table_url = delta_kernel::try_parse_uri(table_path)?;
    Ok(Snapshot::builder_for(table_url).build(engine)?)
}

fn read_sorted(
    snapshot: &Arc<Snapshot>,
    engine: Arc<dyn delta_kernel::Engine>,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let scan = snapshot.clone().scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    assert!(!batches.is_empty(), "expected at least one batch");
    let merged = delta_kernel::arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
    let sort_indices = delta_kernel::arrow::compute::sort_to_indices(merged.column(0), None, None)?;
    let sorted_columns: Vec<ArrayRef> = merged
        .columns()
        .iter()
        .map(|col| delta_kernel::arrow::compute::take(col.as_ref(), &sort_indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(merged.schema(), sorted_columns)?)
}

fn ts_to_micros(s: &str) -> i64 {
    use chrono::{NaiveDateTime, TimeZone, Utc};
    let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap();
    Utc.from_utc_datetime(&ndt)
        .signed_duration_since(chrono::DateTime::UNIX_EPOCH)
        .num_microseconds()
        .unwrap()
}

fn date_to_days(s: &str) -> i32 {
    use chrono::{NaiveDate, TimeZone, Utc};
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
    let dt = Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap());
    dt.signed_duration_since(chrono::DateTime::UNIX_EPOCH)
        .num_days() as i32
}

fn ts_array(micros: i64) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"))
}

fn ts_ntz_array(micros: i64) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(vec![micros]))
}

fn decimal_array(value: i128, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(vec![value])
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

// ==============================================================================
// JSON commit log helpers
// ==============================================================================

/// Strips the table root URL prefix from an add.path to get the relative path.
fn strip_table_root(path: &str, table_path: &str) -> String {
    let prefix = Url::from_directory_path(table_path).unwrap().to_string();
    path.strip_prefix(&prefix)
        .unwrap_or_else(|| {
            panic!("add.path should start with table root.\n  root: {prefix}\n  path: {path}")
        })
        .to_string()
}

/// Builds an unescaped Hive-style path prefix like `col1=val/col2=val/`.
/// Only correct when `value` contains no Hive-special characters.
fn hive_prefix(cols: &[&str], value: &str) -> String {
    cols.iter()
        .map(|c| format!("{c}={value}"))
        .collect::<Vec<_>>()
        .join("/")
        + "/"
}

/// Reads the commit JSON at the given version and returns all add actions.
fn read_add_actions_json(
    table_path: &str,
    version: u64,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let commit_path = format!("{table_path}/_delta_log/{version:020}.json");
    let content = std::fs::read_to_string(commit_path)?;
    let parsed: Vec<serde_json::Value> = serde_json::Deserializer::from_str(&content)
        .into_iter::<serde_json::Value>()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(parsed
        .into_iter()
        .filter_map(|v| v.get("add").cloned())
        .collect())
}
