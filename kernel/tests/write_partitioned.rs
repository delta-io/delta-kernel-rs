//! Integration tests for writing to partitioned Delta tables.
//!
//! Writes a single table with every supported partition column type (13 columns) and 8 rows
//! of edge cases. Verifies both:
//!   1. The serialized `partitionValues` strings in the JSON log (interop correctness)
//!   2. The data round-trips correctly through write -> scan -> read
//!
//! The expected partition values are checked in as a golden Delta table under
//! `data/all-partition-types-and-extreme-values/`. The test writes to a temp dir and compares
//! the resulting partition values key-by-key against the golden log.
//!
//! # Known serialization differences
//!
//! Our `serialize_partition_value` output vs delta-spark and Arrow's `array_value_to_string`.
//!
//! ## Protocol-invalid differences (Arrow's `array_value_to_string` is wrong)
//!
//! These would produce partition values that violate the spec or break cross-engine interop:
//!
//! | Type / Case               | kernel (this impl)       | delta-spark              | Arrow (WRONG)               |
//! |---------------------------|--------------------------|--------------------------|-----------------------------|
//! | binary: "Hello"           | `Hello`                  | `Hello`                  | `48656c6c6f` (hex)          |
//! | binary: emoji bytes       | raw UTF-8 char           | raw UTF-8 char           | hex encoding                |
//! | float: +Infinity          | `Infinity`               | `Infinity`               | `inf` (Java cannot parse)   |
//! | float: -Infinity          | `-Infinity`              | `-Infinity`              | `-inf` (Java cannot parse)  |
//!
//! ## Protocol-valid differences (all are correct, just formatted differently)
//!
//! | Type / Case               | kernel (this impl)       | delta-spark              | Arrow                       |
//! |---------------------------|--------------------------|--------------------------|-----------------------------|
//! | timestamp (zero micros)   | `...T15:30:00.000000Z`   | `...T15:30:00.000000Z`   | `...T15:30:00Z` (omits .0)  |
//! | timestamp_ntz (zero us)   | `...00:00:00.000000`     | `...00:00:00` (no frac)  | `...T00:00:00` (T, no frac) |
//! | timestamp_ntz (non-zero)  | `... 12:00:00.000001`    | `... 12:00:00.000001`    | `...T12:00:00.000001` (T)   |
//! | float: 0.0                | `0`                      | `0.0`                    | `0.0`                       |
//! | double: -0.0              | `-0`                     | `-0.0`                   | `-0.0`                      |
//! | float: MAX                | `340282350000...` (full) | `3.4028235E38` (sci)     | `3.4028235e38` (sci)        |
//! | float: MIN_POSITIVE       | `0.0000...11754944`      | `1.17549435E-38` (sci)   | `1.1754944e-38` (sci)       |
//! | double: MIN               | `-17976931...` (full)    | `-1.79769...E308` (sci)  | `-1.79769...e308` (sci)     |
//! | double: MIN_POSITIVE      | `0.0000...2225...`       | `2.225...E-308` (sci)    | `2.225...e-308` (sci)       |

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use chrono::TimeZone as _;
use delta_kernel::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::Snapshot;
use tempfile::tempdir;
use url::Url;

/// Path to the checked-in golden Delta table with expected partition values.
const GOLDEN_TABLE: &str = "tests/data/all-partition-types-and-extreme-values";

/// Writes a partitioned table with all supported types and edge cases, then verifies:
///   1. Partition value serialization matches the golden delta log (exact string comparison)
///   2. Data round-trips correctly through write -> scan -> read
#[tokio::test]
async fn test_write_partitioned_all_types_and_edge_cases() -> Result<(), Box<dyn std::error::Error>>
{
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = tempdir()?;
    let table_path = tmp_dir.path();
    let table_url = Url::from_directory_path(table_path).unwrap();

    // Schema: value (data) + 13 partition columns covering every supported type
    let table_schema = Arc::new(
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
            StructField::nullable("p_decimal", DataType::decimal(10, 2)?),
            StructField::nullable("p_binary", DataType::BINARY),
            StructField::nullable("p_timestamp_ntz", DataType::TIMESTAMP_NTZ),
        ])
        .unwrap(),
    );
    let partition_cols = [
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

    let engine = DefaultEngineBuilder::new(store_from_url(&table_url)?).build();
    let _ = create_table(table_url.as_str(), table_schema.clone(), "test/1.0")
        .with_data_layout(DataLayout::partitioned(partition_cols))
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(snapshot.table_configuration().partition_columns().len(), 13);
    let arrow_schema = Arc::new(table_schema.as_ref().try_into_arrow()?);

    let rows: Vec<(Vec<ArrayRef>, HashMap<String, Scalar>)> = vec![
        normal_values(),
        zero_and_negative_values(),
        boundary_values(),
        null_values(),
        positive_infinity_values(),
        negative_infinity_values(),
        nan_values(),
        pre_epoch_unicode_values(),
    ];

    let mut snapshot = snapshot;
    for (i, (cols, pvs)) in rows.into_iter().enumerate() {
        snapshot = write_row(&snapshot, &engine, &arrow_schema, cols, pvs).await?;
        assert_eq!(snapshot.version(), (i + 1) as u64);
    }

    // === 1. Verify partition value serialization against golden delta log ===
    //
    // Compare the partitionValues from each commit in the written log against the
    // checked-in golden table. This catches serialization regressions.
    let golden_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN_TABLE);
    for version in 1..=8u64 {
        let actual_pv = read_partition_values_from_log(table_path, version);
        let golden_pv = read_partition_values_from_log(&golden_dir, version);
        assert_eq!(
            actual_pv, golden_pv,
            "partition values mismatch at version {version}"
        );
    }

    // === 2. Verify data round-trips through write -> scan -> read ===
    let actual = read_sorted(&snapshot, Arc::new(engine), &arrow_schema)?;
    assert_eq!(actual.num_rows(), 8, "expected 8 rows");

    // Verify value column (deterministic sort key)
    let value_col = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let actual_values: Vec<i32> = (0..8).map(|i| value_col.value(i)).collect();
    assert_eq!(actual_values, vec![1, 2, 3, 4, 5, 6, 7, 8]);

    // Spot-check key columns that exercise non-trivial deserialization

    // p_decimal: verify negative values round-trip (regression for -123.45 and -0.05)
    let col = actual
        .column(11)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(col.value(0), 12345); // 123.45
    assert_eq!(col.value(1), -12345); // -123.45
    assert_eq!(col.value(2), -5); // -0.05

    // p_binary: verify UTF-8 encoding (not hex)
    let col = actual
        .column(12)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), b"Hello");
    assert_eq!(col.value(1), "\u{1F608}".as_bytes()); // emoji
    assert_eq!(col.value(2), "Hi\u{1F608}".as_bytes()); // mixed

    // p_float: verify infinity and NaN round-trip
    let col = actual
        .column(6)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(col.value(4), f32::INFINITY);
    assert_eq!(col.value(5), f32::NEG_INFINITY);
    assert!(col.value(6).is_nan());

    // p_double: verify infinity and NaN round-trip
    let col = actual
        .column(7)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(col.value(4), f64::INFINITY);
    assert_eq!(col.value(5), f64::NEG_INFINITY);
    assert!(col.value(6).is_nan());

    // p_timestamp_ntz: verify sub-microsecond and epoch values
    let col = actual
        .column(13)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(col.value(0), ts_to_micros_sub("2025-03-31 15:30:00.123456"));
    assert_eq!(col.value(1), 0); // epoch

    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

/// Reads `_delta_log/{version}.json` and extracts the `partitionValues` map from the `add` action.
fn read_partition_values_from_log(
    table_path: &Path,
    version: u64,
) -> HashMap<String, Option<String>> {
    let log_path = table_path
        .join("_delta_log")
        .join(format!("{:020}.json", version));
    let contents = std::fs::read_to_string(&log_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", log_path.display()));
    for line in contents.lines() {
        let obj: serde_json::Value = serde_json::from_str(line).unwrap();
        if let Some(add) = obj.get("add") {
            if let Some(pv) = add.get("partitionValues") {
                let map = pv.as_object().unwrap();
                return map
                    .iter()
                    .map(|(k, v)| {
                        let val = if v.is_null() {
                            None
                        } else {
                            Some(v.as_str().unwrap().to_string())
                        };
                        (k.clone(), val)
                    })
                    .collect();
            }
        }
    }
    panic!("no add action found in {}", log_path.display());
}

fn date_to_days(s: &str) -> i32 {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    chrono::Utc
        .from_utc_datetime(&date)
        .signed_duration_since(chrono::DateTime::UNIX_EPOCH)
        .num_days() as i32
}

fn ts_to_micros(s: &str) -> i64 {
    let ts = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap();
    chrono::Utc
        .from_utc_datetime(&ts)
        .signed_duration_since(chrono::DateTime::UNIX_EPOCH)
        .num_microseconds()
        .unwrap()
}

fn ts_to_micros_sub(s: &str) -> i64 {
    let ts = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.6f").unwrap();
    chrono::Utc
        .from_utc_datetime(&ts)
        .signed_duration_since(chrono::DateTime::UNIX_EPOCH)
        .num_microseconds()
        .unwrap()
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

// ============================================================================
// (ArrayRef, Scalar) pair builders
// ============================================================================

fn s_str(v: &str) -> (ArrayRef, Scalar) {
    (
        Arc::new(StringArray::from(vec![v])),
        Scalar::String(v.into()),
    )
}
fn s_str_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(StringArray::from(vec![None::<&str>])),
        Scalar::Null(DataType::STRING),
    )
}
fn s_int(v: i32) -> (ArrayRef, Scalar) {
    (Arc::new(Int32Array::from(vec![v])), Scalar::Integer(v))
}
fn s_int_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(Int32Array::from(vec![None::<i32>])),
        Scalar::Null(DataType::INTEGER),
    )
}
fn s_long(v: i64) -> (ArrayRef, Scalar) {
    (Arc::new(Int64Array::from(vec![v])), Scalar::Long(v))
}
fn s_long_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(Int64Array::from(vec![None::<i64>])),
        Scalar::Null(DataType::LONG),
    )
}
fn s_short(v: i16) -> (ArrayRef, Scalar) {
    (Arc::new(Int16Array::from(vec![v])), Scalar::Short(v))
}
fn s_short_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(Int16Array::from(vec![None::<i16>])),
        Scalar::Null(DataType::SHORT),
    )
}
fn s_byte(v: i8) -> (ArrayRef, Scalar) {
    (Arc::new(Int8Array::from(vec![v])), Scalar::Byte(v))
}
fn s_byte_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(Int8Array::from(vec![None::<i8>])),
        Scalar::Null(DataType::BYTE),
    )
}
fn s_float(v: f32) -> (ArrayRef, Scalar) {
    (Arc::new(Float32Array::from(vec![v])), Scalar::Float(v))
}
fn s_float_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(Float32Array::from(vec![None::<f32>])),
        Scalar::Null(DataType::FLOAT),
    )
}
fn s_double(v: f64) -> (ArrayRef, Scalar) {
    (Arc::new(Float64Array::from(vec![v])), Scalar::Double(v))
}
fn s_double_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(Float64Array::from(vec![None::<f64>])),
        Scalar::Null(DataType::DOUBLE),
    )
}
fn s_bool(v: bool) -> (ArrayRef, Scalar) {
    (Arc::new(BooleanArray::from(vec![v])), Scalar::Boolean(v))
}
fn s_bool_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(BooleanArray::from(vec![None::<bool>])),
        Scalar::Null(DataType::BOOLEAN),
    )
}
fn s_date(days: i32) -> (ArrayRef, Scalar) {
    (Arc::new(Date32Array::from(vec![days])), Scalar::Date(days))
}
fn s_date_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(Date32Array::from(vec![None::<i32>])),
        Scalar::Null(DataType::DATE),
    )
}
fn s_ts(micros: i64) -> (ArrayRef, Scalar) {
    (ts_array(micros), Scalar::Timestamp(micros))
}
fn s_ts_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
        Scalar::Null(DataType::TIMESTAMP),
    )
}
fn s_decimal(bits: i128) -> (ArrayRef, Scalar) {
    (
        decimal_array(bits, 10, 2),
        Scalar::decimal(bits, 10, 2).unwrap(),
    )
}
fn s_decimal_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(
            Decimal128Array::from(vec![None::<i128>])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Scalar::Null(DataType::decimal(10, 2).unwrap()),
    )
}
fn s_binary(v: &[u8]) -> (ArrayRef, Scalar) {
    (
        Arc::new(BinaryArray::from_vec(vec![v])),
        Scalar::Binary(v.to_vec()),
    )
}
fn s_binary_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
        Scalar::Null(DataType::BINARY),
    )
}
fn s_ts_ntz(micros: i64) -> (ArrayRef, Scalar) {
    (ts_ntz_array(micros), Scalar::TimestampNtz(micros))
}
fn s_ts_ntz_null() -> (ArrayRef, Scalar) {
    (
        Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>])),
        Scalar::Null(DataType::TIMESTAMP_NTZ),
    )
}

// ============================================================================
// Row builder macro
// ============================================================================

/// Builds `(Vec<ArrayRef>, HashMap<String, Scalar>)` from a value column and 13 partition
/// column `(ArrayRef, Scalar)` pairs.
macro_rules! row {
    ($val:expr, $string:expr, $int:expr, $long:expr, $short:expr, $byte:expr,
     $float:expr, $double:expr, $bool:expr, $date:expr, $ts:expr,
     $decimal:expr, $binary:expr, $ts_ntz:expr) => {{
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![$val])),
            $string.0,
            $int.0,
            $long.0,
            $short.0,
            $byte.0,
            $float.0,
            $double.0,
            $bool.0,
            $date.0,
            $ts.0,
            $decimal.0,
            $binary.0,
            $ts_ntz.0,
        ];
        let pvs = HashMap::from([
            ("p_string".into(), $string.1),
            ("p_int".into(), $int.1),
            ("p_long".into(), $long.1),
            ("p_short".into(), $short.1),
            ("p_byte".into(), $byte.1),
            ("p_float".into(), $float.1),
            ("p_double".into(), $double.1),
            ("p_boolean".into(), $bool.1),
            ("p_date".into(), $date.1),
            ("p_timestamp".into(), $ts.1),
            ("p_decimal".into(), $decimal.1),
            ("p_binary".into(), $binary.1),
            ("p_timestamp_ntz".into(), $ts_ntz.1),
        ]);
        (columns, pvs)
    }};
}

// ============================================================================
// Row data functions
// ============================================================================

/// Row 1: normal everyday values.
fn normal_values() -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
    row!(
        1,
        s_str("hello"),
        s_int(42),
        s_long(9876543210),
        s_short(7),
        s_byte(3),
        s_float(1.25),
        s_double(99.99),
        s_bool(true),
        s_date(date_to_days("2025-03-31")),
        s_ts(ts_to_micros("2025-03-31 15:30:00")),
        s_decimal(12345), // 123.45
        s_binary(b"Hello"),
        s_ts_ntz(ts_to_micros_sub("2025-03-31 15:30:00.123456"))
    )
}

/// Row 2: zeros, negatives, epoch, empty string, false, negative decimal, emoji binary.
fn zero_and_negative_values() -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
    row!(
        2,
        s_str(""), // empty string -> null on read
        s_int(0),
        s_long(-1),
        s_short(-32768),
        s_byte(-128),
        s_float(0.0),
        s_double(-0.0),
        s_bool(false),
        s_date(0),                        // epoch
        s_ts(0),                          // epoch
        s_decimal(-12345),                // -123.45 (negative decimal regression)
        s_binary("\u{1F608}".as_bytes()), // emoji (4-byte UTF-8)
        s_ts_ntz(0)                       // epoch
    )
}

/// Row 3: boundary values, special chars, negative decimal between 0 and -1, mixed binary.
fn boundary_values() -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
    row!(
        3,
        s_str("special chars: /=:%\""),
        s_int(i32::MAX),
        s_long(i64::MAX),
        s_short(i16::MAX),
        s_byte(i8::MAX),
        s_float(f32::MAX),
        s_double(f64::MIN),
        s_bool(true),
        s_date(date_to_days("1970-01-01")),
        s_ts(ts_to_micros("2000-01-01 00:00:00")),
        s_decimal(-5),                      // -0.05 (sign-loss regression)
        s_binary("Hi\u{1F608}".as_bytes()), // mixed ASCII + emoji
        s_ts_ntz(ts_to_micros_sub("2000-06-15 12:00:00.000001"))
    )
}

/// Row 4: all nulls.
fn null_values() -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
    row!(
        4,
        s_str_null(),
        s_int_null(),
        s_long_null(),
        s_short_null(),
        s_byte_null(),
        s_float_null(),
        s_double_null(),
        s_bool_null(),
        s_date_null(),
        s_ts_null(),
        s_decimal_null(),
        s_binary_null(),
        s_ts_ntz_null()
    )
}

/// Row 5: float +Infinity, double +Infinity.
fn positive_infinity_values() -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
    row!(
        5,
        s_str("infinity_row"),
        s_int(100),
        s_long(100),
        s_short(100),
        s_byte(100),
        s_float(f32::INFINITY),
        s_double(f64::INFINITY),
        s_bool(true),
        s_date(date_to_days("2025-01-01")),
        s_ts(ts_to_micros("2025-01-01 00:00:00")),
        s_decimal(4200), // 42.00
        s_binary(b"ascii"),
        s_ts_ntz(ts_to_micros("2025-01-01 00:00:00"))
    )
}

/// Row 6: float -Infinity, double -Infinity, control-character binary.
fn negative_infinity_values() -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
    row!(
        6,
        s_str("neg_infinity_row"),
        s_int(-100),
        s_long(-100),
        s_short(-100),
        s_byte(-100),
        s_float(f32::NEG_INFINITY),
        s_double(f64::NEG_INFINITY),
        s_bool(false),
        s_date(date_to_days("1969-12-31")),
        s_ts(ts_to_micros("1970-01-01 00:00:00")),
        s_decimal(-9999), // -99.99
        s_binary(b"\x01\x02\x03"),
        s_ts_ntz(ts_to_micros("1970-01-01 00:00:00"))
    )
}

/// Row 7: float NaN, double NaN, leap day date.
fn nan_values() -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
    row!(
        7,
        s_str("nan_row"),
        s_int(999),
        s_long(999),
        s_short(999),
        s_byte(99),
        s_float(f32::NAN),
        s_double(f64::NAN),
        s_bool(true),
        s_date(date_to_days("2024-02-29")), // leap day
        s_ts(ts_to_micros("2024-02-29 00:00:00")),
        s_decimal(0), // 0.00
        s_binary(b"NaN_row"),
        s_ts_ntz(ts_to_micros("2024-02-29 00:00:00"))
    )
}

/// Row 8: pre-epoch date, unicode string, large decimal, MIN_POSITIVE floats, 2-byte UTF-8 binary.
fn pre_epoch_unicode_values() -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
    row!(
        8,
        s_str("\u{00FC}ber"), // u-umlaut
        s_int(i32::MIN),
        s_long(i64::MIN),
        s_short(i16::MIN),
        s_byte(i8::MIN),
        s_float(f32::MIN_POSITIVE),
        s_double(f64::MIN_POSITIVE),
        s_bool(false),
        s_date(-1), // 1969-12-31
        s_ts(ts_to_micros("1969-01-01 00:00:00")),
        s_decimal(99999999), // 999999.99 (near max for precision 10, scale 2)
        s_binary("\u{00FC}".as_bytes()), // 2-byte UTF-8
        s_ts_ntz(ts_to_micros_sub("1999-12-31 23:59:59.999999"))
    )
}

// ============================================================================
// Write/read helpers
// ============================================================================

/// Writes a single row and returns the post-commit snapshot.
async fn write_row(
    snapshot: &Arc<Snapshot>,
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    arrow_schema: &Arc<ArrowSchema>,
    columns: Vec<ArrayRef>,
    partition_values: HashMap<String, Scalar>,
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine)?
        .with_engine_info("test")
        .with_data_change(true);
    let batch = RecordBatch::try_new(arrow_schema.clone(), columns)?;
    let write_context = txn.get_write_context();
    let add_meta = engine
        .write_parquet(
            &ArrowEngineData::new(batch),
            &write_context,
            partition_values,
        )
        .await?;
    txn.add_files(add_meta);
    match txn.commit(engine)? {
        delta_kernel::transaction::CommitResult::CommittedTransaction(c) => Ok(c
            .post_commit_snapshot()
            .expect("post_commit_snapshot should exist")
            .clone()),
        _ => panic!("commit should succeed"),
    }
}

/// Reads all rows from a snapshot sorted by the first column (assumed to be an int "value" column).
fn read_sorted(
    snapshot: &Arc<Snapshot>,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    expected_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let scan = snapshot.clone().scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine)?;
    let combined = delta_kernel::arrow::compute::concat_batches(expected_schema, batches.iter())?;
    let sort_indices = delta_kernel::arrow::compute::sort_to_indices(
        combined.column(0), // sort by "value" column
        None,
        None,
    )?;
    let sorted_columns: Vec<ArrayRef> = combined
        .columns()
        .iter()
        .map(|col| delta_kernel::arrow::compute::take(col, &sort_indices, None))
        .collect::<Result<_, _>>()?;
    Ok(RecordBatch::try_new(
        expected_schema.clone(),
        sorted_columns,
    )?)
}
