//! Integration tests for writing to partitioned Delta tables.
//!
//! Verifies that the full write pipeline works end-to-end for every supported partition
//! column type: create table with partition columns, write data with typed `Scalar` partition
//! values via `partitioned_write_context`, and read it back via scan.

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
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::Snapshot;
use tempfile::tempdir;
use test_utils::read_scan;
use url::Url;

// === Timestamp helpers ===

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

/// Writes a partitioned table with one partition column per supported type (13 columns total),
/// inserts two rows (one with normal values, one with all nulls), and reads the data back to
/// verify it round-trips correctly.
#[tokio::test]
async fn test_write_partitioned_all_types_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let tmp_dir = tempdir()?;
    let table_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    // Schema: one data column + 13 partition columns (one per supported type)
    let table_schema = Arc::new(StructType::try_new(vec![
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
    ])?);

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

    let engine = Arc::new(DefaultEngineBuilder::new(store_from_url(&table_url)?).build());
    let _ = create_table(table_url.as_str(), table_schema.clone(), "test/1.0")
        .with_data_layout(DataLayout::partitioned(partition_cols))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let arrow_schema: Arc<ArrowSchema> = Arc::new(table_schema.as_ref().try_into_arrow()?);

    // Row 1: normal everyday values for every type
    let ts_micros = ts_to_micros("2025-03-31 15:30:00.123456");
    let date_days = date_to_days("2025-03-31");
    let row1_columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(StringArray::from(vec!["hello"])),
        Arc::new(Int32Array::from(vec![42])),
        Arc::new(Int64Array::from(vec![9_876_543_210i64])),
        Arc::new(Int16Array::from(vec![7i16])),
        Arc::new(Int8Array::from(vec![3i8])),
        Arc::new(Float32Array::from(vec![1.25f32])),
        Arc::new(Float64Array::from(vec![99.99f64])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(Date32Array::from(vec![date_days])),
        ts_array(ts_micros),
        decimal_array(12345, 10, 2), // 123.45
        Arc::new(BinaryArray::from_vec(vec![b"Hello"])),
        ts_ntz_array(ts_micros),
    ];
    let row1_pvs = HashMap::from([
        ("p_string".into(), Scalar::String("hello".into())),
        ("p_int".into(), Scalar::Integer(42)),
        ("p_long".into(), Scalar::Long(9_876_543_210)),
        ("p_short".into(), Scalar::Short(7)),
        ("p_byte".into(), Scalar::Byte(3)),
        ("p_float".into(), Scalar::Float(1.25)),
        ("p_double".into(), Scalar::Double(99.99)),
        ("p_boolean".into(), Scalar::Boolean(true)),
        ("p_date".into(), Scalar::Date(date_days)),
        ("p_timestamp".into(), Scalar::Timestamp(ts_micros)),
        ("p_decimal".into(), Scalar::decimal(12345, 10, 2)?),
        ("p_binary".into(), Scalar::Binary(b"Hello".to_vec())),
        ("p_timestamp_ntz".into(), Scalar::TimestampNtz(ts_micros)),
    ]);

    // Row 2: all null partition values
    let row2_columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![2])),
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
    ];
    let row2_pvs = HashMap::from([
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
        (
            "p_decimal".into(),
            Scalar::Null(DataType::decimal(10, 2)?),
        ),
        ("p_binary".into(), Scalar::Null(DataType::BINARY)),
        (
            "p_timestamp_ntz".into(),
            Scalar::Null(DataType::TIMESTAMP_NTZ),
        ),
    ]);

    // Write both rows
    let batch1 = RecordBatch::try_new(arrow_schema.clone(), row1_columns)?;
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let snapshot =
        test_utils::write_batch_to_table(&snapshot, engine.as_ref(), batch1, row1_pvs).await?;
    assert_eq!(snapshot.version(), 1);

    let batch2 = RecordBatch::try_new(arrow_schema.clone(), row2_columns)?;
    let snapshot =
        test_utils::write_batch_to_table(&snapshot, engine.as_ref(), batch2, row2_pvs).await?;
    assert_eq!(snapshot.version(), 2);

    // Read back and verify round-trip
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "expected 2 rows total");

    // Merge batches and sort by value column for deterministic assertions
    let merged = delta_kernel::arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
    let sort_indices =
        delta_kernel::arrow::compute::sort_to_indices(merged.column(0), None, None)?;
    let sorted_columns: Vec<ArrayRef> = merged
        .columns()
        .iter()
        .map(|col| delta_kernel::arrow::compute::take(col.as_ref(), &sort_indices, None).unwrap())
        .collect();
    let sorted = RecordBatch::try_new(merged.schema(), sorted_columns)?;

    // Verify value column
    let value_col = sorted
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(value_col.value(0), 1);
    assert_eq!(value_col.value(1), 2);

    // Verify p_string: row1 = "hello", row2 = null
    let col = sorted
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "hello");
    assert!(col.is_null(1));

    // Verify p_int: row1 = 42, row2 = null
    let col = sorted
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col.value(0), 42);
    assert!(col.is_null(1));

    // Verify p_date: row1 = 2025-03-31, row2 = null
    let col = sorted
        .column(9)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(col.value(0), date_days);
    assert!(col.is_null(1));

    // Verify p_decimal: row1 = 123.45 (stored as 12345 with scale 2), row2 = null
    let col = sorted
        .column(11)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(col.value(0), 12345);
    assert!(col.is_null(1));

    // Verify p_binary: row1 = b"Hello", row2 = null
    let col = sorted
        .column(12)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), b"Hello");
    assert!(col.is_null(1));

    // Verify p_boolean: row1 = true, row2 = null
    let col = sorted
        .column(8)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(col.value(0));
    assert!(col.is_null(1));

    Ok(())
}

// TODO: Add golden table comparison test.
// Check in a golden Delta table under tests/data/all-partition-types/ written by delta-spark,
// write the same data with kernel, and compare the partitionValues strings in the JSON log
// key-by-key to verify serialization interop.

// TODO: Add test for special float/double values (Infinity, -Infinity, NaN, -0.0).
// These exercise the format_f32/format_f64 code paths and verify round-trip through
// parse_scalar.

// TODO: Add test for boundary/extreme values (i32::MAX, i64::MIN, f32::MIN_POSITIVE,
// decimal negative between 0 and -1, pre-epoch dates, etc.).

// TODO: Add test for binary edge cases (emoji bytes, control characters, mixed ASCII + multi-byte).

// TODO: Add test for special string values (empty string -> null round-trip, strings with
// special characters like /=:%").

// TODO: Add test for negative timestamps and pre-epoch dates.

// TODO: Add test for multi-column partitioned writes verifying column order in hive paths.

// TODO: Add test for column-mapped partitioned table writes (ColumnMappingMode::Name).
