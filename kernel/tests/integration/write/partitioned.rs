//! Integration tests for writing to partitioned Delta tables.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
#[cfg(feature = "nanosecond-timestamps")]
use delta_kernel::arrow::array::TimestampNanosecondArray;
use delta_kernel::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::Snapshot;
use rstest::rstest;
use test_utils::{read_scan, test_table_setup_mt, write_batch_to_table};

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
    let (_tmp_dir, table_path, snapshot, engine) = setup_and_write(
        all_types_schema(),
        PARTITION_COLS,
        cm_mode,
        normal_arrow_columns(),
        normal_partition_values()?,
    )
    .await?;
    #[cfg(not(feature="nanosecond-timestamps"))]
    assert_eq!(snapshot.table_configuration().partition_columns().len(), 13);
    #[cfg(feature="nanosecond-timestamps")]
    assert_eq!(snapshot.table_configuration().partition_columns().len(), 14);

    // ===== Step 2: Validate add.path structure in the commit log JSON. =====
    let (add, rel_path) = read_single_add(&table_path, 1)?;
    match cm_mode {
        ColumnMappingMode::None => {
            // Every value except the timestamps is unreserved ASCII and passes through
            // both encoding layers unchanged. The two timestamps differ on `:`:
            //   `:` -> Hive `%3A` -> URI `%253A`  (all platforms)
            // TIMESTAMP_NTZ additionally contains a space, which diverges by platform:
            //   ` ` on non-Windows: Hive passes it through -> URI `%20`
            //   ` ` on Windows:     Hive escapes to `%20`  -> URI `%2520`
            // TIMESTAMP uses ISO-Z format (no space), so it's platform-identical.
            let ntz_segment = if cfg!(target_os = "windows") {
                "p_timestamp_ntz=2025-03-31%252015%253A30%253A00.123456/"
            } else {
                "p_timestamp_ntz=2025-03-31%2015%253A30%253A00.123456/"
            };
            let expected_prefix = format!(
                "p_string=hello/p_int=42/p_long=9876543210/p_short=7/\
                 p_byte=3/p_float=1.25/p_double=99.99/p_boolean=true/p_date=2025-03-31/\
                 p_timestamp=2025-03-31T15%253A30%253A00.123456Z/p_decimal=123.45/\
                 p_binary=Hello/{ntz_segment}"
            );
            assert!(
                rel_path.starts_with(&expected_prefix),
                "CM off: relative path mismatch.\n  \
                 expected: {expected_prefix}<uuid>.parquet\n  got: {rel_path}"
            );
            assert!(rel_path.ends_with(".parquet"));
        }
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            assert_cm_path(&rel_path);
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

    // ===== Step 4: Scan and verify values survive checkpoint + reload. =====
    verify_and_checkpoint(&snapshot, engine, assert_normal_values)?;

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
    let (_tmp_dir, table_path, snapshot, engine) = setup_and_write(
        all_types_schema(),
        PARTITION_COLS,
        cm_mode,
        null_arrow_columns(),
        null_partition_values()?,
    )
    .await?;

    // ===== Step 2: Validate add.path structure in the commit log JSON. =====
    let (add, rel_path) = read_single_add(&table_path, 1)?;
    match cm_mode {
        ColumnMappingMode::None => {
            // Every partition column should use HIVE_DEFAULT_PARTITION in the path.
            let expected_prefix = hive_prefix(PARTITION_COLS, "__HIVE_DEFAULT_PARTITION__");
            assert!(
                rel_path.starts_with(&expected_prefix),
                "CM off null: relative path mismatch.\n  \
                 expected: {expected_prefix}<uuid>.parquet\n  got: {rel_path}"
            );
        }
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            assert_cm_path(&rel_path);
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

    // ===== Step 4: Scan and verify all-null values survive checkpoint + reload. =====
    verify_and_checkpoint(&snapshot, engine, assert_all_partition_columns_null)?;

    Ok(())
}

// On Windows the Hive escape set additionally includes space, `<`, `>`, `|`. They get
// Hive-escaped to `%XX` first, and the URI layer double-encodes the leading `%`. Non-Windows
// passes them through Hive, so the URI layer applies only single encoding.
macro_rules! platform_path {
    ($win:literal, $non_win:literal) => {
        if cfg!(target_os = "windows") {
            $win
        } else {
            $non_win
        }
    };
}

/// Asserts `add.path` correctly encodes every non-control character in
/// `HADOOP_URI_PATH_ENCODE_SET`, a sample of ASCII controls, plus the Hive-only chars that
/// exercise the Hive -> URI double-encoding path. Partition value is always a three-char
/// string `a<char>b`, so `add.path` must start with `p=a<encoded>b/` (the encoded partition
/// directory plus its trailing separator) and end in `.parquet`. The `partitionValues` map
/// should carry the raw value unchanged.
#[rstest]
// === Chars in both Hive and URI sets: Hive `%XX` -> URI `%25XX` (double-encoded) ===
#[case::percent("a%b", "p=a%2525b/")]
#[case::quote("a\"b", "p=a%2522b/")]
#[case::hash("a#b", "p=a%2523b/")]
#[case::question("a?b", "p=a%253Fb/")]
#[case::backslash("a\\b", "p=a%255Cb/")]
#[case::caret("a^b", "p=a%255Eb/")]
#[case::left_brace("a{b", "p=a%257Bb/")]
#[case::left_bracket("a[b", "p=a%255Bb/")]
#[case::right_bracket("a]b", "p=a%255Db/")]
// === Hive set only (URI just double-encodes the `%` Hive produced) ===
#[case::colon("a:b", "p=a%253Ab/")]
#[case::slash("a/b", "p=a%252Fb/")]
#[case::equals("a=b", "p=a%253Db/")]
#[case::apostrophe("a'b", "p=a%2527b/")]
#[case::asterisk("a*b", "p=a%252Ab/")]
#[case::slash_percent("Serbia/srb%", "p=Serbia%252Fsrb%2525/")]
// === URI set only (Hive passthrough; URI single-encodes) ===
#[case::backtick("a`b", "p=a%60b/")]
#[case::right_brace("a}b", "p=a%7Db/")]
// === Platform-divergent: Hive set on Windows only ===
#[case::space("a b", platform_path!("p=a%2520b/", "p=a%20b/"))]
#[case::less_than("a<b", platform_path!("p=a%253Cb/", "p=a%3Cb/"))]
#[case::greater_than("a>b", platform_path!("p=a%253Eb/", "p=a%3Eb/"))]
#[case::pipe("a|b", platform_path!("p=a%257Cb/", "p=a%7Cb/"))]
#[case::multi_space("a   b", platform_path!("p=a%2520%2520%2520b/", "p=a%20%20%20b/"))]
// === A few ASCII control characters (sample: NUL, TAB, LF, DEL) ===
#[case::null_byte("a\0b", "p=a%2500b/")]
#[case::tab("a\tb", "p=a%2509b/")]
#[case::newline("a\nb", "p=a%250Ab/")]
#[case::del("a\x7Fb", "p=a%257Fb/")]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_partitioned_path_encodes_special_chars(
    #[case] value: &str,
    #[case] expected_path_prefix: &str,
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    // ===== Step 1: Create a single-STRING-partition table and write one row. =====
    let schema = Arc::new(
        StructType::try_new(vec![
            StructField::nullable("value", DataType::INTEGER),
            StructField::nullable("p", DataType::STRING),
        ])
        .unwrap(),
    );
    let (_tmp_dir, table_path, snapshot, engine) = setup_and_write(
        schema,
        &["p"],
        cm_mode,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![value])) as ArrayRef,
        ],
        HashMap::from([("p".to_string(), Scalar::String(value.into()))]),
    )
    .await?;

    // ===== Step 2: Read the single add action from the commit log JSON. =====
    let (add, rel_path) = read_single_add(&table_path, 1)?;

    // ===== Step 3: Validate add.partitionValues carries the raw value unchanged. =====
    let logical_schema = snapshot.schema();
    let physical_key = logical_schema.field("p").unwrap().physical_name(cm_mode);
    let pv = add["partitionValues"].as_object().unwrap();
    assert_eq!(
        pv.get(physical_key).and_then(|v| v.as_str()),
        Some(value),
        "partitionValues[{physical_key}] mismatch"
    );

    // ===== Step 4: Validate add.path has the expected URI-encoded layout. =====
    match cm_mode {
        ColumnMappingMode::None => {
            assert!(
                rel_path.starts_with(expected_path_prefix),
                "CM off: expected path to start with {expected_path_prefix:?}, got {rel_path:?}"
            );
            assert!(rel_path.ends_with(".parquet"));
        }
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            assert_cm_path(&rel_path);
        }
    }

    // ===== Step 5: Scan and verify the row round-trips across checkpoint + reload. =====
    verify_and_checkpoint(&snapshot, engine, |sorted| {
        assert_eq!(sorted.num_rows(), 1);
        assert_eq!(
            sorted
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1,
            "value column mismatch"
        );
        assert_eq!(
            sorted
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            value,
            "partition column `p` mismatch"
        );
    })?;

    Ok(())
}

// TODO: Test extreme low/high values
//       e.g. i32::MIN/MAX, f64::MAX, f64::MIN_POSITIVE, f64::INFINITY, f64::NEG_INFINITY,
//       f64::NAN, Date(-719_162) (year 0001), Date(2_932_896) (year 9999)

// TODO(#2423): Test non-ASCII UTF-8 strings once `add.path` encoding matches Delta-Spark.
//              Kernel currently percent-encodes non-ASCII UTF-8 bytes (e.g. München ->
//              M%C3%BCnchen) while Delta-Spark leaves them raw in `add.path`.
//              e.g. "M\u{00FC}nchen", "日本語", "🎵🎶"

// TODO: Test binary valid UTF-8
//       e.g. Scalar::Binary(b"HELLO".to_vec()), Scalar::Binary(vec![0x2F, 0x3D, 0x25])

// TODO: Test binary with non-UTF-8
//       e.g. Scalar::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]), Scalar::Binary(vec![0x00, 0xFF])

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
            #[cfg(feature = "nanosecond-timestamps")]
            StructField::nullable("p_timestamp_nanos", DataType::TIMESTAMP_NANOS),
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
    #[cfg(feature = "nanosecond-timestamps")]
    "p_timestamp_nanos",
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
        #[cfg(feature = "nanosecond-timestamps")]
        Arc::new(TimestampNanosecondArray::from(vec![ts * 1000 + 123]).with_timezone("UTC")),
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
        #[cfg(feature = "nanosecond-timestamps")]
        (
            "p_timestamp_nanos".into(),
            Scalar::TimestampNanos(ts * 1000 + 123),
        ),
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
    #[cfg(feature = "nanosecond-timestamps")]
    ("p_timestamp_nanos", "2025-03-31T15:30:00.123456123Z"),
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
        #[cfg(feature = "nanosecond-timestamps")]
        Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
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
        #[cfg(feature = "nanosecond-timestamps")]
        (
            "p_timestamp_nanos".into(),
            Scalar::Null(DataType::TIMESTAMP_NANOS),
        ),
    ]))
}

// ==============================================================================
// Assertions
// ==============================================================================

/// Downcast column `$idx` to `$arr_ty` and assert row 0's value equals `$expected`.
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
    assert_col!(sorted, 6, Float32Array, 1.25f32); // p_float
    assert_col!(sorted, 7, Float64Array, 99.99f64); // p_double
    assert_col!(sorted, 8, BooleanArray, true); // p_boolean
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
    #[cfg(feature = "nanosecond-timestamps")]
    assert_col!(sorted, 14, TimestampNanosecondArray, ts * 1000 + 123); // p_timestamp_nanos
}

/// Asserts all partition columns (indices 1-13) are null for the single row.
fn assert_all_partition_columns_null(sorted: &RecordBatch) {
    assert_eq!(sorted.num_rows(), 1);
    let mut num_columns = 13;
    if cfg!(feature = "nanosecond-timestamps") {
        num_columns += 1;
    }
    for col_idx in 1..=num_columns {
        assert!(
            sorted.column(col_idx).is_null(0),
            "partition column at index {col_idx} ({}) should be null",
            sorted.schema().field(col_idx).name()
        );
    }
}

/// Asserts a CM=name/id relative path has the shape `<2char>/<file>.parquet`.
fn assert_cm_path(rel_path: &str) {
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

fn create_partitioned_table(
    table_path: &str,
    engine: &dyn delta_kernel::Engine,
    schema: Arc<StructType>,
    partition_cols: &[&str],
    cm_mode: ColumnMappingMode,
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let mut builder = create_table(table_path, schema, "test/1.0")
        .with_data_layout(DataLayout::partitioned(partition_cols));
    if cm_mode != ColumnMappingMode::None {
        builder =
            builder.with_table_properties([("delta.columnMapping.mode", cm_mode_str(cm_mode))]);
    }
    let _ = builder
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?;
    Ok(Snapshot::builder_for(table_path).build(engine)?)
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
    let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap();
    Utc.from_utc_datetime(&ndt)
        .signed_duration_since(chrono::DateTime::UNIX_EPOCH)
        .num_microseconds()
        .unwrap()
}

fn date_to_days(s: &str) -> i32 {
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

/// Creates a partitioned table, writes one batch, and returns the post-commit snapshot.
/// Callers can subsequently use [`read_single_add`] with the returned `table_path` to
/// inspect the commit log.
async fn setup_and_write(
    schema: Arc<StructType>,
    partition_cols: &[&str],
    cm_mode: ColumnMappingMode,
    arrow_columns: Vec<ArrayRef>,
    partition_values: HashMap<String, Scalar>,
) -> Result<
    (
        tempfile::TempDir,
        String,
        Arc<Snapshot>,
        Arc<dyn delta_kernel::Engine>,
    ),
    Box<dyn std::error::Error>,
> {
    let (tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    let snapshot = create_partitioned_table(
        &table_path,
        engine.as_ref(),
        schema,
        partition_cols,
        cm_mode,
    )?;

    let batch = RecordBatch::try_new(arrow_schema, arrow_columns)?;
    let snapshot =
        write_batch_to_table(&snapshot, engine.as_ref(), batch, partition_values).await?;

    Ok((
        tmp_dir,
        table_path,
        snapshot,
        engine as Arc<dyn delta_kernel::Engine>,
    ))
}

/// Reads the snapshot, runs `assert_fn` on the sorted scan result, then checkpoints,
/// reloads a fresh snapshot from disk, and runs `assert_fn` again on the reloaded scan.
fn verify_and_checkpoint(
    snapshot: &Arc<Snapshot>,
    engine: Arc<dyn delta_kernel::Engine>,
    assert_fn: impl Fn(&RecordBatch),
) -> Result<(), Box<dyn std::error::Error>> {
    let sorted = read_sorted(snapshot, engine.clone())?;
    assert_fn(&sorted);

    snapshot.checkpoint(engine.as_ref())?;
    let reloaded = Snapshot::builder_for(snapshot.table_root()).build(engine.as_ref())?;
    let sorted = read_sorted(&reloaded, engine)?;
    assert_fn(&sorted);
    Ok(())
}

// ==============================================================================
// JSON commit log helpers
// ==============================================================================

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

/// Reads the single add action from the commit at `version` and returns the action
/// together with its path. Asserts the path is relative (no `scheme://` prefix).
fn read_single_add(
    table_path: &str,
    version: u64,
) -> Result<(serde_json::Value, String), Box<dyn std::error::Error>> {
    let adds = read_add_actions_json(table_path, version)?;
    assert_eq!(adds.len(), 1, "should have exactly one add action");
    let add = adds.into_iter().next().unwrap();
    let rel_path = add["path"].as_str().unwrap().to_string();
    assert!(
        !rel_path.contains("://"),
        "should produce relative paths, got: {rel_path}"
    );
    Ok((add, rel_path))
}

// ==============================================================================
// materializePartitionColumns tests
// ==============================================================================

/// Materialized partition columns are physically present in the parquet file, but their
/// stats must be omitted from the Add action because the value is already in
/// `partitionValues`. Verifies that contract end-to-end through the kernel `create_table`
/// builder.
#[tokio::test(flavor = "multi_thread")]
async fn test_materialized_partition_columns_excluded_from_stats(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let partition_col = "partition";
    let table_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("number", DataType::INTEGER),
        StructField::nullable(partition_col, DataType::STRING),
    ])?);

    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let _ = create_table(&table_path, table_schema.clone(), "test/1.0")
        .with_data_layout(DataLayout::partitioned([partition_col]))
        .with_table_properties([("delta.feature.materializePartitionColumns", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("default engine");

    // Build the input logical batch with all schema columns, including the partition column.
    // What ends up in the parquet file is determined by `materializePartitionColumns` later
    // in the pipeline (`Transaction::generate_logical_to_physical` skips the partition-column
    // drop when the feature is on); the input batch shape itself is unaffected.
    let arrow_schema = Arc::new(table_schema.as_ref().try_into_arrow()?);
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "a", "a"])),
        ],
    )?;
    let data = Box::new(ArrowEngineData::new(batch));

    let write_context = txn.partitioned_write_context(HashMap::from([(
        partition_col.to_string(),
        Scalar::String("a".into()),
    )]))?;
    let result = engine.write_parquet(&data, &write_context).await?;
    txn.add_files(result);
    assert!(txn.commit(engine.as_ref())?.is_committed());

    let (add, _) = read_single_add(&table_path, 1)?;
    let stats: serde_json::Value = serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();

    // Stats should contain the data column but NOT the partition column.
    assert!(
        stats["minValues"].get("number").is_some(),
        "data column 'number' should have minValues"
    );
    assert!(
        stats["maxValues"].get("number").is_some(),
        "data column 'number' should have maxValues"
    );
    assert!(
        stats["minValues"].get(partition_col).is_none(),
        "partition column should not have minValues even when materialized"
    );
    assert!(
        stats["maxValues"].get(partition_col).is_none(),
        "partition column should not have maxValues even when materialized"
    );
    assert!(
        stats["nullCount"].get(partition_col).is_none(),
        "partition column should not have nullCount even when materialized"
    );

    Ok(())
}
