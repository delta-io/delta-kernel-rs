//! Integration tests for writing to partitioned Delta tables.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::TimeZone as _;
use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
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

// === Helpers ===

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

/// Holds a data column value and one Arrow array + Scalar per partition column for a single row.
struct TestRow {
    data_value: i32,
    p_string: (ArrayRef, Scalar),
    p_int: (ArrayRef, Scalar),
    p_long: (ArrayRef, Scalar),
    p_short: (ArrayRef, Scalar),
    p_byte: (ArrayRef, Scalar),
    p_float: (ArrayRef, Scalar),
    p_double: (ArrayRef, Scalar),
    p_boolean: (ArrayRef, Scalar),
    p_date: (ArrayRef, Scalar),
    p_timestamp: (ArrayRef, Scalar),
}

impl TestRow {
    fn into_columns_and_partition_values(self) -> (Vec<ArrayRef>, HashMap<String, Scalar>) {
        let columns = vec![
            Arc::new(Int32Array::from(vec![self.data_value])) as ArrayRef,
            self.p_string.0,
            self.p_int.0,
            self.p_long.0,
            self.p_short.0,
            self.p_byte.0,
            self.p_float.0,
            self.p_double.0,
            self.p_boolean.0,
            self.p_date.0,
            self.p_timestamp.0,
        ];
        let partition_values = HashMap::from([
            ("p_string".into(), self.p_string.1),
            ("p_int".into(), self.p_int.1),
            ("p_long".into(), self.p_long.1),
            ("p_short".into(), self.p_short.1),
            ("p_byte".into(), self.p_byte.1),
            ("p_float".into(), self.p_float.1),
            ("p_double".into(), self.p_double.1),
            ("p_boolean".into(), self.p_boolean.1),
            ("p_date".into(), self.p_date.1),
            ("p_timestamp".into(), self.p_timestamp.1),
        ]);
        (columns, partition_values)
    }
}

fn ts_array(micros: i64) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"))
}

fn ts_array_null() -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>]).with_timezone("UTC"))
}

/// Normal, everyday values.
fn normal_values() -> TestRow {
    TestRow {
        data_value: 1,
        p_string: (
            Arc::new(StringArray::from(vec!["hello"])),
            Scalar::String("hello".into()),
        ),
        p_int: (Arc::new(Int32Array::from(vec![42])), Scalar::Integer(42)),
        p_long: (
            Arc::new(Int64Array::from(vec![9876543210i64])),
            Scalar::Long(9876543210),
        ),
        p_short: (Arc::new(Int16Array::from(vec![7i16])), Scalar::Short(7)),
        p_byte: (Arc::new(Int8Array::from(vec![3i8])), Scalar::Byte(3)),
        p_float: (
            Arc::new(Float32Array::from(vec![1.25f32])),
            Scalar::Float(1.25),
        ),
        p_double: (
            Arc::new(Float64Array::from(vec![99.99f64])),
            Scalar::Double(99.99),
        ),
        p_boolean: (
            Arc::new(BooleanArray::from(vec![true])),
            Scalar::Boolean(true),
        ),
        p_date: (
            Arc::new(Date32Array::from(vec![date_to_days("2025-03-31")])),
            Scalar::Date(date_to_days("2025-03-31")),
        ),
        p_timestamp: (
            ts_array(ts_to_micros("2025-03-31 15:30:00")),
            Scalar::Timestamp(ts_to_micros("2025-03-31 15:30:00")),
        ),
    }
}

/// Zeros, negatives, epoch boundaries, empty string, false.
fn zero_and_negative_values() -> TestRow {
    TestRow {
        data_value: 2,
        p_string: (
            Arc::new(StringArray::from(vec![""])),
            Scalar::String("".into()),
        ),
        p_int: (Arc::new(Int32Array::from(vec![0])), Scalar::Integer(0)),
        p_long: (Arc::new(Int64Array::from(vec![-1i64])), Scalar::Long(-1)),
        p_short: (
            Arc::new(Int16Array::from(vec![-32768i16])),
            Scalar::Short(-32768),
        ),
        p_byte: (Arc::new(Int8Array::from(vec![-128i8])), Scalar::Byte(-128)),
        p_float: (
            Arc::new(Float32Array::from(vec![0.0f32])),
            Scalar::Float(0.0),
        ),
        p_double: (
            Arc::new(Float64Array::from(vec![-0.0f64])),
            Scalar::Double(-0.0),
        ),
        p_boolean: (
            Arc::new(BooleanArray::from(vec![false])),
            Scalar::Boolean(false),
        ),
        p_date: (Arc::new(Date32Array::from(vec![0])), Scalar::Date(0)),
        p_timestamp: (ts_array(0), Scalar::Timestamp(0)),
    }
}

/// Max/min boundaries and special characters in strings.
fn boundary_values() -> TestRow {
    TestRow {
        data_value: 3,
        p_string: (
            Arc::new(StringArray::from(vec!["special chars: /=:%\""])),
            Scalar::String("special chars: /=:%\"".into()),
        ),
        p_int: (
            Arc::new(Int32Array::from(vec![i32::MAX])),
            Scalar::Integer(i32::MAX),
        ),
        p_long: (
            Arc::new(Int64Array::from(vec![i64::MAX])),
            Scalar::Long(i64::MAX),
        ),
        p_short: (
            Arc::new(Int16Array::from(vec![i16::MAX])),
            Scalar::Short(i16::MAX),
        ),
        p_byte: (
            Arc::new(Int8Array::from(vec![i8::MAX])),
            Scalar::Byte(i8::MAX),
        ),
        p_float: (
            Arc::new(Float32Array::from(vec![f32::MAX])),
            Scalar::Float(f32::MAX),
        ),
        p_double: (
            Arc::new(Float64Array::from(vec![f64::MIN])),
            Scalar::Double(f64::MIN),
        ),
        p_boolean: (
            Arc::new(BooleanArray::from(vec![true])),
            Scalar::Boolean(true),
        ),
        p_date: (
            Arc::new(Date32Array::from(vec![date_to_days("1970-01-01")])),
            Scalar::Date(date_to_days("1970-01-01")),
        ),
        p_timestamp: (
            ts_array(ts_to_micros("2000-01-01 00:00:00")),
            Scalar::Timestamp(ts_to_micros("2000-01-01 00:00:00")),
        ),
    }
}

/// All partition columns are null.
fn null_values() -> TestRow {
    TestRow {
        data_value: 4,
        p_string: (
            Arc::new(StringArray::from(vec![None::<&str>])),
            Scalar::Null(DataType::STRING),
        ),
        p_int: (
            Arc::new(Int32Array::from(vec![None::<i32>])),
            Scalar::Null(DataType::INTEGER),
        ),
        p_long: (
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Scalar::Null(DataType::LONG),
        ),
        p_short: (
            Arc::new(Int16Array::from(vec![None::<i16>])),
            Scalar::Null(DataType::SHORT),
        ),
        p_byte: (
            Arc::new(Int8Array::from(vec![None::<i8>])),
            Scalar::Null(DataType::BYTE),
        ),
        p_float: (
            Arc::new(Float32Array::from(vec![None::<f32>])),
            Scalar::Null(DataType::FLOAT),
        ),
        p_double: (
            Arc::new(Float64Array::from(vec![None::<f64>])),
            Scalar::Null(DataType::DOUBLE),
        ),
        p_boolean: (
            Arc::new(BooleanArray::from(vec![None::<bool>])),
            Scalar::Null(DataType::BOOLEAN),
        ),
        p_date: (
            Arc::new(Date32Array::from(vec![None::<i32>])),
            Scalar::Null(DataType::DATE),
        ),
        p_timestamp: (ts_array_null(), Scalar::Null(DataType::TIMESTAMP)),
    }
}

/// Writes a single row to a partitioned table and returns the post-commit snapshot.
async fn write_row(
    snapshot: &Arc<Snapshot>,
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    arrow_schema: &Arc<ArrowSchema>,
    row: TestRow,
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let (columns, partition_values) = row.into_columns_and_partition_values();
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

fn partitioned_table_schema() -> Arc<StructType> {
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
        ])
        .unwrap(),
    )
}

const PARTITION_COLUMNS: [&str; 10] = [
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
];

// === Tests ===

/// Creates a partitioned table with every supported partition column type and writes multiple
/// commits exercising normal values, edge cases (zeros, negatives, empty strings, epoch
/// boundaries, max/min, special characters), and null partition values.
#[tokio::test]
async fn test_write_partitioned_with_all_supported_types() -> Result<(), Box<dyn std::error::Error>>
{
    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = tempdir()?;
    let table_url = Url::from_directory_path(tmp_dir.path()).unwrap();
    let table_schema = partitioned_table_schema();

    let engine = DefaultEngineBuilder::new(store_from_url(&table_url)?).build();
    let _ = create_table(table_url.as_str(), table_schema.clone(), "test/1.0")
        .with_data_layout(DataLayout::partitioned(PARTITION_COLUMNS))
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    // Verify partition columns are accessible via the public API
    assert_eq!(snapshot.table_configuration().partition_columns().len(), 10);

    let arrow_schema = Arc::new(table_schema.as_ref().try_into_arrow()?);

    let snapshot = write_row(&snapshot, &engine, &arrow_schema, normal_values()).await?;
    assert_eq!(snapshot.version(), 1);

    let snapshot = write_row(
        &snapshot,
        &engine,
        &arrow_schema,
        zero_and_negative_values(),
    )
    .await?;
    assert_eq!(snapshot.version(), 2);

    let snapshot = write_row(&snapshot, &engine, &arrow_schema, boundary_values()).await?;
    assert_eq!(snapshot.version(), 3);

    let snapshot = write_row(&snapshot, &engine, &arrow_schema, null_values()).await?;
    assert_eq!(snapshot.version(), 4);

    // Read back all 4 rows and verify the full round-trip. Build the expected RecordBatch
    // with all data + partition values as they should appear after read-side reconstruction.
    let expected = RecordBatch::try_new(
        arrow_schema,
        vec![
            // value (data column): 1, 2, 3, 4
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)])) as ArrayRef,
            // p_string: "hello", null (empty string becomes null per protocol), special chars, null
            Arc::new(StringArray::from(vec![
                Some("hello"),
                None, // empty string partition value is stored as null per Delta spec
                Some("special chars: /=:%\""),
                None,
            ])),
            // p_int: 42, 0, MAX, null
            Arc::new(Int32Array::from(vec![
                Some(42),
                Some(0),
                Some(i32::MAX),
                None,
            ])),
            // p_long: 9876543210, -1, MAX, null
            Arc::new(Int64Array::from(vec![
                Some(9876543210i64),
                Some(-1),
                Some(i64::MAX),
                None,
            ])),
            // p_short: 7, -32768, MAX, null
            Arc::new(Int16Array::from(vec![
                Some(7i16),
                Some(-32768),
                Some(i16::MAX),
                None,
            ])),
            // p_byte: 3, -128, MAX, null
            Arc::new(Int8Array::from(vec![
                Some(3i8),
                Some(-128),
                Some(i8::MAX),
                None,
            ])),
            // p_float: 1.25, 0.0, MAX, null
            Arc::new(Float32Array::from(vec![
                Some(1.25f32),
                Some(0.0),
                Some(f32::MAX),
                None,
            ])),
            // p_double: 99.99, -0.0, MIN, null
            Arc::new(Float64Array::from(vec![
                Some(99.99f64),
                Some(-0.0),
                Some(f64::MIN),
                None,
            ])),
            // p_boolean: true, false, true, null
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
            ])),
            // p_date: 2025-03-31, 1970-01-01, 1970-01-01, null
            Arc::new(Date32Array::from(vec![
                Some(date_to_days("2025-03-31")),
                Some(0),
                Some(date_to_days("1970-01-01")),
                None,
            ])),
            // p_timestamp: 2025-03-31 15:30:00, epoch, 2000-01-01, null
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(ts_to_micros("2025-03-31 15:30:00")),
                    Some(0),
                    Some(ts_to_micros("2000-01-01 00:00:00")),
                    None,
                ])
                .with_timezone("UTC"),
            ),
        ],
    )?;

    // Read the table and sort by the data column for deterministic comparison.
    // Each row was written in a separate commit (separate parquet file), so scan order
    // depends on file listing order which is not guaranteed.
    let engine = Arc::new(engine);
    let scan = snapshot.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine)?;
    let combined =
        delta_kernel::arrow::compute::concat_batches(&expected.schema(), batches.iter())?;
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
    let actual = RecordBatch::try_new(expected.schema(), sorted_columns)?;

    assert_eq!(actual, expected);

    Ok(())
}
