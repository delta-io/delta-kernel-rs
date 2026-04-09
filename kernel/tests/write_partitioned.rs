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

// =============================================================================
// Tests
// =============================================================================

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
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = all_types_schema();
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    let snapshot = create_all_types_table(&table_path, engine.as_ref(), cm_mode)?;
    assert_eq!(snapshot.table_configuration().partition_columns().len(), 13);

    let ts_micros = ts_to_micros("2025-03-31 15:30:00.123456");
    let date_days = date_to_days("2025-03-31");

    let columns: Vec<ArrayRef> = vec![
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
        decimal_array(12345, 10, 2),
        Arc::new(BinaryArray::from_vec(vec![b"Hello"])),
        ts_ntz_array(ts_micros),
    ];
    let pvs = HashMap::from([
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

    let batch = RecordBatch::try_new(arrow_schema, columns)?;
    let snapshot = test_utils::write_batch_to_table(&snapshot, engine.as_ref(), batch, pvs).await?;

    // Read before checkpoint
    let sorted = read_sorted(&snapshot, engine.clone())?;
    assert_normal_values(&sorted, date_days);

    // Checkpoint, reload from checkpoint, read again
    snapshot.checkpoint(engine.as_ref())?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot_after_cp = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let sorted = read_sorted(&snapshot_after_cp, engine)?;
    assert_normal_values(&sorted, date_days);

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
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = all_types_schema();
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    let snapshot = create_all_types_table(&table_path, engine.as_ref(), cm_mode)?;

    let columns: Vec<ArrayRef> = vec![
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
    ];
    let pvs = HashMap::from([
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
    ]);

    let batch = RecordBatch::try_new(arrow_schema, columns)?;
    let snapshot = test_utils::write_batch_to_table(&snapshot, engine.as_ref(), batch, pvs).await?;

    // Read before checkpoint
    let sorted = read_sorted(&snapshot, engine.clone())?;
    assert_all_partition_columns_null(&sorted);

    // Checkpoint, reload from checkpoint, read again
    snapshot.checkpoint(engine.as_ref())?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot_after_cp = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let sorted = read_sorted(&snapshot_after_cp, engine)?;
    assert_all_partition_columns_null(&sorted);

    Ok(())
}

// TODO: Add golden table comparison test.
// Check in a golden Delta table under tests/data/all-partition-types/ written by delta-spark,
// write the same data with kernel, and compare the partitionValues strings in the JSON log
// key-by-key to verify serialization interop.

// TODO: Add test for special float/double values (Infinity, -Infinity, NaN, -0.0).

// TODO: Add test for boundary/extreme values (i32::MAX, i64::MIN, f32::MIN_POSITIVE,
// decimal negative between 0 and -1, pre-epoch dates, etc.).

// TODO: Add test for binary edge cases (emoji bytes, control characters, mixed ASCII + multi-byte).

// TODO: Add test for special string values (empty string -> null round-trip, strings with
// special characters like /=:%").

// TODO: Add test for negative timestamps and pre-epoch dates.

// TODO: Add test for multi-column partitioned writes verifying column order in hive paths.

// =============================================================================
// Helpers
// =============================================================================

/// Asserts the normal-values row reads back correctly (1 row, spot-check key types).
fn assert_normal_values(sorted: &RecordBatch, date_days: i32) {
    assert_eq!(sorted.num_rows(), 1);
    assert_eq!(
        sorted
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "hello"
    );
    assert_eq!(
        sorted
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(
        sorted
            .column(9)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        date_days
    );
    assert_eq!(
        sorted
            .column(11)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        12345
    );
    assert_eq!(
        sorted
            .column(12)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"Hello"
    );
    assert!(sorted
        .column(8)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0));
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
