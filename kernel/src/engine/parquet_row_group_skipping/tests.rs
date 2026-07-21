use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::File;
use std::sync::{Arc, LazyLock};

use rstest::rstest;

use super::*;
use crate::arrow::array::{Int64Array, RecordBatch, StringArray, StructArray};
use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema};
use crate::expressions::{
    column_expr, column_name, column_pred, Expression, OpaquePredicateOp, ScalarExpressionEvaluator,
};
use crate::kernel_predicates::{
    DataSkippingPredicateEvaluator as _, DirectDataSkippingPredicateEvaluator,
    DirectPredicateEvaluator, IndirectDataSkippingPredicateEvaluator,
    KernelPredicateEvaluator as _,
};
use crate::parquet::arrow::arrow_reader::ArrowReaderMetadata;
use crate::parquet::arrow::ArrowWriter;
use crate::parquet::data_type::{ByteArray, FixedLenByteArray};
use crate::parquet::file::properties::WriterProperties;
use crate::parquet::file::reader::FileReader;
use crate::parquet::file::serialized_reader::SerializedFileReader;
use crate::scan::data_skipping::as_prefixed_checkpoint_predicate;
use crate::{DeltaResult, Predicate};

/// Empty partition column set for tests that don't need partition columns.
static NO_PARTITIONS: LazyLock<HashSet<String>> = LazyLock::new(HashSet::new);

/// Performs an exhaustive set of reads against a specially crafted parquet file.
///
/// There is a column for each primitive type, and each has a distinct set of values so we can
/// reliably determine which physical column a given logical value was taken from (even in case of
/// type widening). We also "cheat" in a few places, interpreting the byte array of a 128-bit
/// decimal as STRING and BINARY column types (because Delta doesn't support fixed-len binary or
/// string types). The file also has nested columns to ensure we handle that case correctly. The
/// parquet footer of the file we use is:
///
/// ```text
/// Row group 0:  count: 5  total(compressed): 905 B total(uncompressed):940 B
/// --------------------------------------------------------------------------------
///                              type      nulls   min / max
/// bool                         BOOLEAN   3       "false" / "true"
/// chrono.date32                INT32     0       "1971-01-01" / "1971-01-05"
/// chrono.timestamp             INT96     0
/// chrono.timestamp_ntz         INT64     0       "1970-01-02T00:00:00.000000" / "1970-01-02T00:04:00.000000"
/// numeric.decimals.decimal128  FIXED[14] 0       "11.128" / "15.128"
/// numeric.decimals.decimal32   INT32     0       "11.032" / "15.032"
/// numeric.decimals.decimal64   INT64     0       "11.064" / "15.064"
/// numeric.floats.float32       FLOAT     0       "139.0" / "1048699.0"
/// numeric.floats.float64       DOUBLE    0       "1147.0" / "1.125899906842747E15"
/// numeric.ints.int16           INT32     0       "1000" / "1004"
/// numeric.ints.int32           INT32     0       "1000000" / "1000004"
/// numeric.ints.int64           INT64     0       "1000000000" / "1000000004"
/// numeric.ints.int8            INT32     0       "0" / "4"
/// varlen.binary                BINARY    0       "0x" / "0x00000000"
/// varlen.utf8                  BINARY    0       "a" / "e"
/// ```
#[test]
fn test_get_stat_values() {
    let file = File::open("./tests/data/parquet_row_group_skipping/part-00000-b92e017a-50ba-4676-8322-48fc371c2b59-c000.snappy.parquet").unwrap();
    let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();

    // The predicate doesn't matter -- it just needs to mention all the columns we care about.
    let columns = Predicate::and_from(vec![
        column_pred!("varlen.utf8"),
        column_pred!("numeric.ints.int64"),
        column_pred!("numeric.ints.int32"),
        column_pred!("numeric.ints.int16"),
        column_pred!("numeric.ints.int8"),
        column_pred!("numeric.floats.float32"),
        column_pred!("numeric.floats.float64"),
        column_pred!("bool"),
        column_pred!("varlen.binary"),
        column_pred!("numeric.decimals.decimal32"),
        column_pred!("numeric.decimals.decimal64"),
        column_pred!("numeric.decimals.decimal128"),
        column_pred!("chrono.date32"),
        column_pred!("chrono.timestamp"),
        column_pred!("chrono.timestamp_ntz"),
    ]);
    let filter = RowGroupFilter::new(metadata.metadata().row_group(0), &columns);

    assert_eq!(filter.get_rowcount_stat(), Some(5i64.into()));

    // Only the BOOL column has any nulls
    assert_eq!(
        filter.get_nullcount_stat(&column_name!("bool")),
        Some(3i64.into())
    );

    // No nulls -> exact zero count (parquet 58.1+, arrow-rs#9451).
    assert_eq!(
        filter.get_nullcount_stat(&column_name!("varlen.utf8")),
        Some(0i64.into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("varlen.utf8"), &DataType::STRING),
        Some("a".into())
    );

    // CHEAT: Interpret the decimal128 column's fixed-length binary as a string
    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal128"),
            &DataType::STRING
        ),
        Some("\0\0\0\0\0\0\0\0\0\0\0\0+x".into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("numeric.ints.int64"), &DataType::LONG),
        Some(1000000000i64.into())
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(&column_name!("numeric.ints.int32"), &DataType::LONG),
        Some(1000000i64.into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("numeric.ints.int32"), &DataType::INTEGER),
        Some(1000000i32.into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("numeric.ints.int16"), &DataType::SHORT),
        Some(1000i16.into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("numeric.ints.int8"), &DataType::BYTE),
        Some(0i8.into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("numeric.floats.float64"), &DataType::DOUBLE),
        Some(1147f64.into())
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(&column_name!("numeric.floats.float32"), &DataType::DOUBLE),
        Some(139f64.into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("numeric.floats.float32"), &DataType::FLOAT),
        Some(139f32.into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("bool"), &DataType::BOOLEAN),
        Some(false.into())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("varlen.binary"), &DataType::BINARY),
        Some([].as_slice().into())
    );

    // CHEAT: Interpret the decimal128 column's fixed-len array as binary
    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal128"),
            &DataType::BINARY
        ),
        Some(
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x2b, 0x78]
                .as_slice()
                .into()
        )
    );

    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(8, 3).unwrap()
        ),
        Some(Scalar::decimal(11032, 8, 3).unwrap())
    );

    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal64"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::decimal(11064, 16, 3).unwrap())
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::decimal(11032, 16, 3).unwrap())
    );

    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal128"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::decimal(11128, 32, 3).unwrap())
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal64"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::decimal(11064, 32, 3).unwrap())
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::decimal(11032, 32, 3).unwrap())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("chrono.date32"), &DataType::DATE),
        Some(PrimitiveType::Date.parse_scalar("1971-01-01").unwrap())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("chrono.timestamp"), &DataType::TIMESTAMP),
        None // Timestamp defaults to 96-bit, which doesn't get stats
    );

    // Read a random column as Variant. The actual read does not need to be performed, as stats on
    // Variant should always return None.
    assert_eq!(
        filter.get_min_stat(
            &column_name!("chrono.date32"),
            &DataType::unshredded_variant()
        ),
        None
    );

    // CHEAT: Interpret the timestamp_ntz column as a normal timestamp
    assert_eq!(
        filter.get_min_stat(&column_name!("chrono.timestamp_ntz"), &DataType::TIMESTAMP),
        Some(
            PrimitiveType::Timestamp
                .parse_scalar("1970-01-02 00:00:00.000000")
                .unwrap()
        )
    );

    assert_eq!(
        filter.get_min_stat(
            &column_name!("chrono.timestamp_ntz"),
            &DataType::TIMESTAMP_NTZ
        ),
        Some(
            PrimitiveType::TimestampNtz
                .parse_scalar("1970-01-02 00:00:00.000000")
                .unwrap()
        )
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(&column_name!("chrono.date32"), &DataType::TIMESTAMP_NTZ),
        Some(
            PrimitiveType::TimestampNtz
                .parse_scalar("1971-01-01 00:00:00.000000")
                .unwrap()
        )
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("varlen.utf8"), &DataType::STRING),
        Some("e".into())
    );

    // CHEAT: Interpret the decimal128 column's fixed-length binary as a string
    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal128"),
            &DataType::STRING
        ),
        Some("\0\0\0\0\0\0\0\0\0\0\0\0;\u{18}".into())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("numeric.ints.int64"), &DataType::LONG),
        Some(1000000004i64.into())
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(&column_name!("numeric.ints.int32"), &DataType::LONG),
        Some(1000004i64.into())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("numeric.ints.int32"), &DataType::INTEGER),
        Some(1000004.into())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("numeric.ints.int16"), &DataType::SHORT),
        Some(1004i16.into())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("numeric.ints.int8"), &DataType::BYTE),
        Some(4i8.into())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("numeric.floats.float64"), &DataType::DOUBLE),
        Some(1125899906842747f64.into())
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(&column_name!("numeric.floats.float32"), &DataType::DOUBLE),
        Some(1048699f64.into())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("numeric.floats.float32"), &DataType::FLOAT),
        Some(1048699f32.into())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("bool"), &DataType::BOOLEAN),
        Some(true.into())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("varlen.binary"), &DataType::BINARY),
        Some([0, 0, 0, 0].as_slice().into())
    );

    // CHEAT: Interpret the decimal128 columns' fixed-len array as binary
    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal128"),
            &DataType::BINARY
        ),
        Some(
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x3b, 0x18]
                .as_slice()
                .into()
        )
    );

    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(8, 3).unwrap()
        ),
        Some(Scalar::decimal(15032, 8, 3).unwrap())
    );

    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal64"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::decimal(15064, 16, 3).unwrap())
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::decimal(15032, 16, 3).unwrap())
    );

    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal128"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::decimal(15128, 32, 3).unwrap())
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal64"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::decimal(15064, 32, 3).unwrap())
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::decimal(15032, 32, 3).unwrap())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("chrono.date32"), &DataType::DATE),
        Some(PrimitiveType::Date.parse_scalar("1971-01-05").unwrap())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("chrono.timestamp"), &DataType::TIMESTAMP),
        None // Timestamp defaults to 96-bit, which doesn't get stats
    );

    // Read a random column as Variant. The actual read does not need to be performed, as stats on
    // Variant should always return None.
    assert_eq!(
        filter.get_max_stat(
            &column_name!("chrono.date32"),
            &DataType::unshredded_variant()
        ),
        None
    );

    // CHEAT: Interpret the timestamp_ntz column as a normal timestamp
    assert_eq!(
        filter.get_max_stat(&column_name!("chrono.timestamp_ntz"), &DataType::TIMESTAMP),
        Some(
            PrimitiveType::Timestamp
                .parse_scalar("1970-01-02 00:04:00.000000")
                .unwrap()
        )
    );

    assert_eq!(
        filter.get_max_stat(
            &column_name!("chrono.timestamp_ntz"),
            &DataType::TIMESTAMP_NTZ
        ),
        Some(
            PrimitiveType::TimestampNtz
                .parse_scalar("1970-01-02 00:04:00.000000")
                .unwrap()
        )
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(&column_name!("chrono.date32"), &DataType::TIMESTAMP_NTZ),
        Some(
            PrimitiveType::TimestampNtz
                .parse_scalar("1971-01-05 00:00:00.000000")
                .unwrap()
        )
    );
}

// A missing footer null count decodes to `None`; an exact zero is preserved as `Some(0)`. Parquet
// 58.1+ distinguishes the two (arrow-rs#9451).
#[test]
fn test_extract_nullcount_distinguishes_missing_from_zero() {
    let stats = |null_count| Statistics::int64(Some(1), Some(2), None, null_count, false);
    assert_eq!(extract_nullcount(None), None);
    assert_eq!(extract_nullcount(Some(&stats(None))), None);
    assert_eq!(extract_nullcount(Some(&stats(Some(0)))), Some(0));
    assert_eq!(extract_nullcount(Some(&stats(Some(3)))), Some(3));
}

// A zero null count lets IS NULL prune a column with no nulls; a positive count keeps it. The
// fixture's `varlen.utf8` has 0 nulls, `bool` has 3.
#[test]
fn test_row_group_filter_is_null_prunes_when_nullcount_is_zero() {
    let file = File::open("./tests/data/parquet_row_group_skipping/part-00000-b92e017a-50ba-4676-8322-48fc371c2b59-c000.snappy.parquet").unwrap();
    let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
    let row_group = metadata.metadata().row_group(0);

    assert!(!RowGroupFilter::apply(
        row_group,
        &Predicate::is_null(column_name!("varlen.utf8"))
    ));
    assert!(RowGroupFilter::apply(
        row_group,
        &Predicate::is_null(column_name!("bool"))
    ));
}

// Intervals are unsupported for skipping under any footer encoding, so extraction returns None.
#[test]
fn test_interval_skipping_unsupported_for_any_footer_stats() {
    let interval_flba = FixedLenByteArray::from(ByteArray::from(vec![0u8; 12]));
    let variants = [
        Statistics::int32(Some(0), Some(1), None, Some(0), false),
        Statistics::int64(Some(0), Some(1), None, Some(0), false),
        Statistics::fixed_len_byte_array(
            Some(interval_flba.clone()),
            Some(interval_flba),
            None,
            Some(0),
            false,
        ),
    ];
    for dt in [DataType::INTERVAL_YEAR_MONTH, DataType::INTERVAL_DAY_TIME] {
        for stats in &variants {
            // Call the extractor directly: the fixture has no interval column for get_min_stat
            assert_eq!(extract_min_scalar(&dt, stats), None);
            assert_eq!(extract_max_scalar(&dt, stats), None);
        }
    }
}

/// Wraps an Int64 leaf array in nested StructArrays matching the given column path.
///
/// For `col_path = &["a", "b"]` and a leaf array `[10, 20]`, produces the Arrow structure:
///   `a: Struct { b: Int64 }` with values `[{b: 10}, {b: 20}]`.
///
/// Returns the outermost `(field, array)` pair for embedding in a parent struct.
fn wrap_in_nested_struct(
    col_path: &[&str],
    values: Arc<Int64Array>,
) -> (Arc<Field>, Arc<dyn crate::arrow::array::Array>) {
    assert!(!col_path.is_empty());
    let mut field = Arc::new(Field::new(
        *col_path.last().unwrap(),
        ArrowDataType::Int64,
        true,
    ));
    let mut array: Arc<dyn crate::arrow::array::Array> = values;
    for &name in col_path[..col_path.len() - 1].iter().rev() {
        let struct_array = StructArray::from(vec![(field.clone(), array)]);
        field = Arc::new(Field::new(
            name,
            ArrowDataType::Struct(Fields::from(vec![field])),
            true,
        ));
        array = Arc::new(struct_array);
    }
    (field, array)
}

/// Builds a `(field, array)` pair for a single stat type (e.g. `minValues.<col_path>`).
fn build_stat_column(
    stat_name: &str,
    col_path: &[&str],
    values: Arc<Int64Array>,
) -> (Arc<Field>, Arc<dyn crate::arrow::array::Array>) {
    let (col_field, col_array) = wrap_in_nested_struct(col_path, values);
    let stat_struct = StructArray::from(vec![(col_field.clone(), col_array)]);
    let stat_field = Arc::new(Field::new(
        stat_name,
        ArrowDataType::Struct(Fields::from(vec![col_field])),
        true,
    ));
    (stat_field, Arc::new(stat_struct))
}

/// Writes a checkpoint-like parquet file with the given per-file statistics.
///
/// Schema: `add.stats_parsed.{minValues,maxValues,nullCount}.<col_path>` (INT64)
///         + optionally `add.partitionValues_parsed.part_col` (STRING)
///
/// `col_path` supports nested columns (e.g. `&["a", "b"]` produces `...minValues.a.b`).
/// Each array index represents one data file row. Use `None` to simulate missing statistics.
/// When `part_values` is `Some`, a `partitionValues_parsed.part_col` column is included.
fn write_checkpoint_parquet(
    min_values: &[Option<i64>],
    max_values: &[Option<i64>],
    null_counts: &[Option<i64>],
    col_path: &[&str],
    part_values: Option<&[Option<&str>]>,
) -> tempfile::NamedTempFile {
    let (min_f, min_a) = build_stat_column(
        MIN_VALUES,
        col_path,
        Arc::new(Int64Array::from(min_values.to_vec())),
    );
    let (max_f, max_a) = build_stat_column(
        MAX_VALUES,
        col_path,
        Arc::new(Int64Array::from(max_values.to_vec())),
    );
    let (nc_f, nc_a) = build_stat_column(
        NULL_COUNT,
        col_path,
        Arc::new(Int64Array::from(null_counts.to_vec())),
    );

    let stats_struct = StructArray::from(vec![
        (min_f.clone(), min_a),
        (max_f.clone(), max_a),
        (nc_f.clone(), nc_a),
    ]);
    let stats_parsed_field = Arc::new(Field::new(
        "stats_parsed",
        ArrowDataType::Struct(Fields::from(vec![min_f, max_f, nc_f])),
        true,
    ));

    let mut add_children: Vec<(Arc<Field>, Arc<dyn crate::arrow::array::Array>)> =
        vec![(stats_parsed_field, Arc::new(stats_struct))];

    if let Some(part_values) = part_values {
        let part_col_field = Arc::new(Field::new("part_col", ArrowDataType::Utf8, true));
        let pv_parsed_field = Arc::new(Field::new(
            "partitionValues_parsed",
            ArrowDataType::Struct(Fields::from(vec![part_col_field.clone()])),
            true,
        ));
        let part_col = Arc::new(StringArray::from(part_values.to_vec()));
        let pv_struct = StructArray::from(vec![(part_col_field.clone(), part_col as _)]);
        add_children.push((pv_parsed_field, Arc::new(pv_struct)));
    }

    let add_struct = StructArray::from(add_children.clone());
    let add_fields: Fields = add_children.iter().map(|(f, _)| f.clone()).collect();
    let add_field = Arc::new(Field::new("add", ArrowDataType::Struct(add_fields), true));
    let schema = Arc::new(ArrowSchema::new(vec![add_field]));

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(add_struct)]).unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    tmp
}

fn checkpoint_row_group_metadata(
    tmp: &tempfile::NamedTempFile,
) -> crate::parquet::file::metadata::ParquetMetaData {
    let file = File::open(tmp.path()).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    reader.metadata().clone()
}

#[test]
fn checkpoint_filter_returns_stats_when_no_nulls_in_stat_columns() {
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(2), Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let predicate = Predicate::gt(column_name!("x"), Scalar::from(50i64));
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &NO_PARTITIONS);

    // All stat columns are non-null, so stats should be available.
    // min(minValues.x) across rows = 10, max(maxValues.x) = 200
    assert_eq!(
        filter.get_min_stat(&column_name!("x"), &DataType::LONG),
        Some(10i64.into())
    );
    assert_eq!(
        filter.get_max_stat(&column_name!("x"), &DataType::LONG),
        Some(200i64.into())
    );
    // max(nullCount.x) = 2 (conservative: at least one file has nulls)
    assert_eq!(
        filter.get_nullcount_stat(&column_name!("x")),
        Some(2i64.into())
    );
    // Row count is None for checkpoint files (footer rowcount is not meaningful)
    assert_eq!(filter.get_rowcount_stat(), None);
}

#[test]
fn checkpoint_filter_returns_none_when_stat_column_has_nulls() {
    // Row 1 has null stats (file missing statistics)
    let tmp = write_checkpoint_parquet(
        &[Some(10), None, Some(20)],
        &[Some(100), None, Some(200)],
        &[Some(2), None, Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let predicate = Predicate::gt(column_name!("x"), Scalar::from(50i64));
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &NO_PARTITIONS);

    // Stat columns have nulls (row 1), so footer aggregates are unreliable.
    assert_eq!(
        filter.get_min_stat(&column_name!("x"), &DataType::LONG),
        None
    );
    assert_eq!(
        filter.get_max_stat(&column_name!("x"), &DataType::LONG),
        None
    );
    assert_eq!(filter.get_nullcount_stat(&column_name!("x")), None);
}

#[test]
fn checkpoint_filter_partition_columns_always_available() {
    // Row 1 has null stats, but partition values are always present.
    let tmp = write_checkpoint_parquet(
        &[Some(10), None],
        &[Some(100), None],
        &[Some(2), None],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let partition_columns: HashSet<String> = ["part_col".to_string()].into();
    let predicate = Predicate::and(
        Predicate::gt(column_name!("x"), Scalar::from(50i64)),
        Predicate::eq(column_name!("part_col"), Scalar::from("a")),
    );
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &partition_columns);

    // Partition column stats should always be available (not null-guarded).
    assert_eq!(
        filter.get_min_stat(&column_name!("part_col"), &DataType::STRING),
        Some("a".into())
    );
    assert_eq!(
        filter.get_max_stat(&column_name!("part_col"), &DataType::STRING),
        Some("b".into())
    );

    // Data column stats should still be None (stat columns have nulls).
    assert_eq!(
        filter.get_min_stat(&column_name!("x"), &DataType::LONG),
        None
    );
}

#[test]
fn checkpoint_filter_apply_keeps_row_group_with_missing_stats() {
    // Row 1 has null stats -- row group must NOT be pruned even though
    // the non-null footer max is 100 < 500.
    let tmp = write_checkpoint_parquet(
        &[Some(10), None],
        &[Some(100), None],
        &[Some(0), None],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let predicate = Predicate::gt(column_name!("x"), Scalar::from(500i64));
    // Without null guarding, footer max=100 < 500 would falsely prune this row group.
    assert!(CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &NO_PARTITIONS
    ));
}

#[test]
fn checkpoint_filter_apply_prunes_row_group_with_all_stats_present() {
    // All stats present, max=200 < 500, so row group can be pruned.
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(0), Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let predicate = Predicate::gt(column_name!("x"), Scalar::from(500i64));
    assert!(!CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &NO_PARTITIONS
    ));
}

#[test]
fn checkpoint_filter_is_null_with_all_stats_present() {
    // All nullCount values present. max(nullCount.x) = 5 > 0, so IS NULL should keep.
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(5), Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    // IS NULL(x): at least one file has nullCount > 0, so keep the row group.
    let predicate = Predicate::is_null(column_name!("x"));
    assert!(CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &NO_PARTITIONS
    ));
}

#[test]
fn checkpoint_filter_is_null_all_zero_nullcounts() {
    // All nullCount values are 0, so max(nullCount) = 0. The data skipping evaluator checks
    // nullcount != 0 for IS NULL. Since nullcount == 0, no files have nulls and IS NULL prunes.
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(0), Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let predicate = Predicate::is_null(column_name!("x"));
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &NO_PARTITIONS);

    // All nullCount entries are present (no nulls in the stat column), so the null guard
    // passes. max(nullCount.x) = 0, meaning no files have nulls for x.
    // The data skipping evaluator checks nullcount != 0 for IS NULL. Since nullcount == 0,
    // it means no files have nulls, so IS NULL can prune.
    let result = filter.eval_sql_where(&predicate);
    // eval_sql_where returns Some(false) -> can skip. Semantics: no file has null values for x.
    assert_eq!(result, Some(false));
}

#[test]
fn checkpoint_filter_is_not_null_never_prunes() {
    // IS NOT NULL can never prune checkpoint row groups because get_rowcount_stat() returns None.
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(5), Some(3)],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let predicate = Predicate::is_not_null(column_name!("x"));
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &NO_PARTITIONS);

    // IS NOT NULL checks nullcount != rowcount. Since rowcount is None, the evaluator
    // short-circuits and returns None (can't decide), which always keeps the row group.
    let result = filter.eval_sql_where(&predicate);
    assert_eq!(result, None);
}

#[test]
fn checkpoint_filter_timestamp_max_widened() {
    // Timestamp max stats are widened by 999us to account for millisecond truncation
    // in JSON-serialized stats (which stats_parsed inherits).
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(0), Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let predicate = column_pred!("x");
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &NO_PARTITIONS);

    // max(maxValues.x) = 200, widened to 200 + 999 = 1199
    assert_eq!(
        filter.get_max_stat(&column_name!("x"), &DataType::TIMESTAMP),
        Some(Scalar::Timestamp(1199))
    );
    assert_eq!(
        filter.get_max_stat(&column_name!("x"), &DataType::TIMESTAMP_NTZ),
        Some(Scalar::TimestampNtz(1199))
    );

    // Non-timestamp types are not widened.
    assert_eq!(
        filter.get_max_stat(&column_name!("x"), &DataType::LONG),
        Some(200i64.into())
    );
}

#[test]
fn checkpoint_filter_unknown_column_returns_none() {
    let tmp = write_checkpoint_parquet(
        &[Some(10)],
        &[Some(100)],
        &[Some(0)],
        &["x"],
        Some(&[Some("a")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    // Predicate references column "y" which doesn't exist in the checkpoint.
    let predicate = Predicate::gt(column_name!("y"), Scalar::from(50i64));
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &NO_PARTITIONS);

    assert_eq!(
        filter.get_min_stat(&column_name!("y"), &DataType::LONG),
        None
    );
    assert_eq!(
        filter.get_max_stat(&column_name!("y"), &DataType::LONG),
        None
    );
    assert_eq!(filter.get_nullcount_stat(&column_name!("y")), None);
}

#[test]
fn checkpoint_filter_mixed_partition_and_data_predicate() {
    // Row 1 has null data stats but valid partition values.
    // Predicate: part_col = "a" AND x > 500
    // The partition predicate should still work, but the data predicate can't prune.
    let tmp = write_checkpoint_parquet(
        &[Some(10), None],
        &[Some(100), None],
        &[Some(0), None],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let partition_columns: HashSet<String> = ["part_col".to_string()].into();

    // x > 500: data stats have nulls, can't prune. Overall AND can't prune.
    let predicate = Predicate::and(
        Predicate::eq(column_name!("part_col"), Scalar::from("a")),
        Predicate::gt(column_name!("x"), Scalar::from(500i64)),
    );
    assert!(CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &partition_columns
    ));

    // part_col = "c": partition stats show min="a", max="b", so "c" is out of range.
    // But x stats are unreliable. AND("c" not in [a,b], x unknown) -- the partition arm
    // is false, so the AND is false -> can prune!
    let predicate = Predicate::and(
        Predicate::eq(column_name!("part_col"), Scalar::from("c")),
        Predicate::gt(column_name!("x"), Scalar::from(5i64)),
    );
    assert!(!CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &partition_columns,
    ));
}

/// An opaque "less than" predicate op that supports data skipping. Used to verify that
/// `CheckpointRowGroupFilter` correctly delegates opaque predicates through the
/// `ParquetStatsProvider` trait, where null guarding is applied by the provider.
#[derive(Debug, PartialEq)]
struct OpaqueLessThanOp;

impl OpaquePredicateOp for OpaqueLessThanOp {
    fn name(&self) -> &str {
        "less_than"
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _evaluator: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        unimplemented!("not needed for data skipping tests")
    }

    fn eval_as_data_skipping_predicate(
        &self,
        evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        let (col, val, ord) = match exprs {
            [Expression::Column(col), Expression::Literal(val)] => (col, val, Ordering::Less),
            [Expression::Literal(val), Expression::Column(col)] => (col, val, Ordering::Greater),
            _ => return None,
        };
        evaluator.partial_cmp_min_stat(col, val, ord, inverted)
    }

    fn as_data_skipping_predicate(
        &self,
        _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<Predicate> {
        unimplemented!("not needed for data skipping tests")
    }
}

#[test]
fn checkpoint_filter_opaque_predicate_with_null_guarded_stats() {
    // All stats present: opaque predicate should participate in skipping.
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(0), Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    // OpaqueLessThanOp(x, 5) -> "x < 5". Data skipping checks min(x) < 5.
    // min(x) = 10, so 10 < 5 is false -> can skip the row group.
    let predicate = Predicate::opaque(
        OpaqueLessThanOp,
        vec![column_expr!("x"), Expression::literal(5i64)],
    );
    assert!(!CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &NO_PARTITIONS
    ));

    // OpaqueLessThanOp(x, 50) -> "x < 50". min(x) = 10, so 10 < 50 is true -> keep.
    let predicate = Predicate::opaque(
        OpaqueLessThanOp,
        vec![column_expr!("x"), Expression::literal(50i64)],
    );
    assert!(CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &NO_PARTITIONS
    ));
}

#[test]
fn checkpoint_filter_opaque_predicate_with_missing_stats() {
    // Row 1 has null stats: opaque predicate must not cause false pruning.
    let tmp = write_checkpoint_parquet(
        &[Some(10), None],
        &[Some(100), None],
        &[Some(0), None],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    // OpaqueLessThanOp(x, 5) -> "x < 5". The stat column has nulls, so the null-guarded
    // provider returns None for min(x). The opaque op gets None and returns None,
    // which means the row group cannot be pruned. This is the safe behavior.
    let predicate = Predicate::opaque(
        OpaqueLessThanOp,
        vec![column_expr!("x"), Expression::literal(5i64)],
    );
    assert!(CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &NO_PARTITIONS
    ));
}

#[test]
fn checkpoint_filter_partition_is_null_prunes_when_all_values_present() {
    // All partition values non-null -> exact zero null count, so IS NULL matches nothing and
    // prunes.
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(0), Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let partition_columns: HashSet<String> = ["part_col".to_string()].into();
    let predicate = Predicate::is_null(column_name!("part_col"));
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &partition_columns);

    assert_eq!(
        filter.get_nullcount_stat(&column_name!("part_col")),
        Some(0i64.into())
    );
    assert_eq!(filter.eval_sql_where(&predicate), Some(false));
}

// `CheckpointRowGroupFilter` reads partition footer min/max unguarded (see its doc): footer min/max
// ignore the null-partition sibling of a non-Add row, so a non-matching value prunes a group that
// still holds that row. This is the filter's documented contract, NOT a safe standalone verdict --
// the production path never uses it, instead applying the null-guarded meta-predicate from
// `as_prefixed_checkpoint_predicate`. `reader_keeps_mixed_row_group_with_null_partition_sibling`
// pins the safe production behavior for the same layout.
#[test]
fn checkpoint_filter_partition_unguarded_prunes_group_with_null_sibling() {
    // Row 0: add file, partition = "b"; Row 1: non-add, partition = null.
    let tmp = write_checkpoint_parquet(
        &[Some(10), None],
        &[Some(100), None],
        &[Some(0), None],
        &["x"],
        Some(&[Some("b"), None]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);
    let partition_columns: HashSet<String> = ["part_col".to_string()].into();

    // Matching value -> keep (footer range is ["b","b"], ignoring the null).
    assert!(CheckpointRowGroupFilter::apply(
        row_group,
        &Predicate::eq(column_name!("part_col"), Scalar::from("b")),
        &partition_columns
    ));
    // Non-matching value outside the footer range -> prune. The unguarded filter drops the group
    // despite its null-partition sibling; only sound because callers must pre-exclude such groups.
    assert!(!CheckpointRowGroupFilter::apply(
        row_group,
        &Predicate::eq(column_name!("part_col"), Scalar::from("z")),
        &partition_columns
    ));
}

// A row group spanning multiple partition values (footer min != max) must keep any predicate whose
// target falls within [min, max] and prune only when the target is provably outside the range.
#[test]
fn checkpoint_filter_partition_range_min_lt_max() {
    // Footer partition range ["a","c"] across three add rows.
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20), Some(30)],
        &[Some(100), Some(200), Some(300)],
        &[Some(0), Some(0), Some(0)],
        &["x"],
        Some(&[Some("a"), Some("b"), Some("c")]),
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);
    let partition_columns: HashSet<String> = ["part_col".to_string()].into();
    let col = column_name!("part_col");

    // (predicate, expected keep)
    let cases = [
        (Predicate::eq(col.clone(), Scalar::from("b")), true), // in range
        (Predicate::eq(col.clone(), Scalar::from("0")), false), // below min
        (Predicate::eq(col.clone(), Scalar::from("z")), false), // above max
        (Predicate::ne(col.clone(), Scalar::from("b")), true), // range spans other values
        (Predicate::gt(col.clone(), Scalar::from("a")), true), // max "c" > "a"
        (Predicate::gt(col.clone(), Scalar::from("d")), false), // max "c" not > "d"
    ];
    for (predicate, keep) in cases {
        assert_eq!(
            CheckpointRowGroupFilter::apply(row_group, &predicate, &partition_columns),
            keep,
            "{predicate:?}"
        );
    }
}

#[test]
fn checkpoint_filter_multi_row_group_skipping() {
    // Build schema: add.stats_parsed.{minValues,maxValues,nullCount}.x (INT64)
    let col_field = Arc::new(Field::new("x", ArrowDataType::Int64, true));
    let min_field = Arc::new(Field::new(
        MIN_VALUES,
        ArrowDataType::Struct(Fields::from(vec![col_field.clone()])),
        true,
    ));
    let max_field = Arc::new(Field::new(
        MAX_VALUES,
        ArrowDataType::Struct(Fields::from(vec![col_field.clone()])),
        true,
    ));
    let nc_field = Arc::new(Field::new(
        NULL_COUNT,
        ArrowDataType::Struct(Fields::from(vec![col_field.clone()])),
        true,
    ));
    let stats_field = Arc::new(Field::new(
        "stats_parsed",
        ArrowDataType::Struct(Fields::from(vec![
            min_field.clone(),
            max_field.clone(),
            nc_field.clone(),
        ])),
        true,
    ));
    let add_field = Arc::new(Field::new(
        "add",
        ArrowDataType::Struct(Fields::from(vec![stats_field.clone()])),
        true,
    ));
    let schema = Arc::new(ArrowSchema::new(vec![add_field]));

    let make_batch = |mins: &[i64], maxs: &[i64], ncs: &[i64]| {
        let min_arr = Arc::new(Int64Array::from(mins.to_vec()));
        let max_arr = Arc::new(Int64Array::from(maxs.to_vec()));
        let nc_arr = Arc::new(Int64Array::from(ncs.to_vec()));
        let min_s = StructArray::from(vec![(col_field.clone(), min_arr as _)]);
        let max_s = StructArray::from(vec![(col_field.clone(), max_arr as _)]);
        let nc_s = StructArray::from(vec![(col_field.clone(), nc_arr as _)]);
        let stats_s = StructArray::from(vec![
            (min_field.clone(), Arc::new(min_s) as _),
            (max_field.clone(), Arc::new(max_s) as _),
            (nc_field.clone(), Arc::new(nc_s) as _),
        ]);
        let add_s = StructArray::from(vec![(stats_field.clone(), Arc::new(stats_s) as _)]);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(add_s)]).unwrap()
    };

    // RG0: x in [10, 100] -> pruned by "x > 500"
    // RG1: x in [400, 600] -> kept by "x > 500"
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    #[allow(deprecated)] // renamed to set_max_row_group_row_count in newer parquet versions
    let props = WriterProperties::builder()
        .set_max_row_group_size(2)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    writer
        .write(&make_batch(&[10, 20], &[50, 100], &[0, 0]))
        .unwrap();
    writer
        .write(&make_batch(&[400, 450], &[500, 600], &[0, 0]))
        .unwrap();
    writer.close().unwrap();

    let file = File::open(tmp.path()).unwrap();
    let builder =
        crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap();
    assert_eq!(builder.metadata().num_row_groups(), 2);

    let predicate = Predicate::gt(column_name!("x"), Scalar::from(500i64));
    let builder = builder.with_checkpoint_row_group_filter(&predicate, &NO_PARTITIONS, None);

    // Only RG1 (x in [400, 600]) survives: max(x) = 600 > 500.
    let reader = builder.build().unwrap();
    let batches: Vec<_> = reader.into_iter().collect::<Result<_, _>>().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
}

#[test]
fn checkpoint_filter_nested_struct_column_stats() {
    // Per-file statistics mirror the data schema structure (Delta protocol spec, "Per-file
    // Statistics" section). For a nested column `a.b`, stats are stored as:
    //   add.stats_parsed.{minValues,maxValues,nullCount}.a.b (INT64)
    let tmp = write_checkpoint_parquet(
        &[Some(10), Some(20)],
        &[Some(100), Some(200)],
        &[Some(0), Some(0)],
        &["a", "b"],
        None,
    );
    let metadata = checkpoint_row_group_metadata(&tmp);
    let row_group = metadata.row_group(0);

    let col = ColumnName::new(["a", "b"]);
    let predicate = Predicate::gt(col.clone(), Scalar::from(500i64));
    let filter = CheckpointRowGroupFilter::new(row_group, &predicate, &NO_PARTITIONS);

    assert_eq!(
        filter.get_min_stat(&col, &DataType::LONG),
        Some(10i64.into())
    );
    assert_eq!(
        filter.get_max_stat(&col, &DataType::LONG),
        Some(200i64.into())
    );
    // max(x) = 200 < 500 -> can prune.
    assert!(!CheckpointRowGroupFilter::apply(
        row_group,
        &predicate,
        &NO_PARTITIONS
    ));
}

// ============================================================================
// End-to-end row-group I/O reduction tests.
//
// The tests above assert the skip/keep *decision* (`CheckpointRowGroupFilter::apply`). These tests
// exercise the whole PRODUCTION path: a user predicate is rewritten by
// `as_prefixed_checkpoint_predicate` -- exactly as `build_actions_meta_predicate` does -- and
// applied through the generic `with_row_group_filter` the default engine uses for checkpoint reads.
// The fixture carries one add file per row with a unique `id`, so the surviving `id`s reveal which
// row groups were actually read.
// ============================================================================

/// One add file's checkpoint stats: a unique `id`, a data column `x` (min/max), and a partition
/// value (`None` writes a real parquet null, modelling a non-Add row whose partition value is
/// null).
struct RgSpec {
    id: i64,
    x_min: i64,
    x_max: i64,
    part: Option<&'static str>,
}

/// Writes a checkpoint parquet carrying `add.stats_parsed.{minValues,maxValues,nullCount}.{id,x}`
/// and `add.partitionValues_parsed.part`, with one row group per `group` (a `flush` after each
/// forces the boundary). A single-row group models one add file; a multi-row group models an add
/// file sharing a row group with a null-partition sibling.
fn write_checkpoint(groups: &[&[RgSpec]]) -> tempfile::NamedTempFile {
    let id_field = Arc::new(Field::new("id", ArrowDataType::Int64, true));
    let x_field = Arc::new(Field::new("x", ArrowDataType::Int64, true));
    let stat_cols = Fields::from(vec![id_field.clone(), x_field.clone()]);
    let stat_struct_field = |name| {
        Arc::new(Field::new(
            name,
            ArrowDataType::Struct(stat_cols.clone()),
            true,
        ))
    };
    let min_field = stat_struct_field(MIN_VALUES);
    let max_field = stat_struct_field(MAX_VALUES);
    let nc_field = stat_struct_field(NULL_COUNT);
    let stats_field = Arc::new(Field::new(
        "stats_parsed",
        ArrowDataType::Struct(Fields::from(vec![
            min_field.clone(),
            max_field.clone(),
            nc_field.clone(),
        ])),
        true,
    ));
    let part_field = Arc::new(Field::new("part", ArrowDataType::Utf8, true));
    let pv_field = Arc::new(Field::new(
        "partitionValues_parsed",
        ArrowDataType::Struct(Fields::from(vec![part_field.clone()])),
        true,
    ));
    let add_field = Arc::new(Field::new(
        "add",
        ArrowDataType::Struct(Fields::from(vec![stats_field.clone(), pv_field.clone()])),
        true,
    ));
    let schema = Arc::new(ArrowSchema::new(vec![add_field]));

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut writer =
        ArrowWriter::try_new(tmp.as_file().try_clone().unwrap(), schema.clone(), None).unwrap();
    let stat_struct = |ids: &[i64], xs: &[i64]| {
        StructArray::from(vec![
            (
                id_field.clone(),
                Arc::new(Int64Array::from(ids.to_vec())) as _,
            ),
            (
                x_field.clone(),
                Arc::new(Int64Array::from(xs.to_vec())) as _,
            ),
        ])
    };
    for group in groups {
        let ids: Vec<i64> = group.iter().map(|s| s.id).collect();
        let x_mins: Vec<i64> = group.iter().map(|s| s.x_min).collect();
        let x_maxs: Vec<i64> = group.iter().map(|s| s.x_max).collect();
        let zeros: Vec<i64> = group.iter().map(|_| 0).collect();
        let parts: Vec<Option<&str>> = group.iter().map(|s| s.part).collect();
        let stats_struct = StructArray::from(vec![
            (min_field.clone(), Arc::new(stat_struct(&ids, &x_mins)) as _),
            (max_field.clone(), Arc::new(stat_struct(&ids, &x_maxs)) as _),
            (nc_field.clone(), Arc::new(stat_struct(&zeros, &zeros)) as _),
        ]);
        let pv_struct = StructArray::from(vec![(
            part_field.clone(),
            Arc::new(StringArray::from(parts)) as _,
        )]);
        let add_struct = StructArray::from(vec![
            (stats_field.clone(), Arc::new(stats_struct) as _),
            (pv_field.clone(), Arc::new(pv_struct) as _),
        ]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(add_struct)]).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    tmp
}

/// Rewrites `pred` (referencing physical partition/data column names) into the checkpoint
/// meta-predicate the production path applies, then reads `tmp` through `with_row_group_filter` and
/// returns the sorted `id`s of the add rows whose row group survived. An ineligible predicate
/// (`None` from the rewrite) reads every row group, matching production.
fn surviving_ids(
    tmp: &tempfile::NamedTempFile,
    pred: &Predicate,
    partition_columns: &[&str],
    stats_columns: &[&str],
) -> Vec<i64> {
    let partitions: HashSet<String> = partition_columns.iter().map(|s| s.to_string()).collect();
    let stats: HashSet<ColumnName> = stats_columns
        .iter()
        .map(|s| ColumnName::new([*s]))
        .collect();
    let meta = as_prefixed_checkpoint_predicate(pred, &partitions, &stats);

    let file = File::open(tmp.path()).unwrap();
    let mut builder =
        crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap();
    if let Some(meta) = &meta {
        builder = builder.with_row_group_filter(meta, None);
    }
    let mut ids: Vec<i64> = builder
        .build()
        .unwrap()
        .map(Result::unwrap)
        .flat_map(|batch| {
            let add = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let stats = struct_field(add, "stats_parsed");
            let min = struct_field(&stats, MIN_VALUES);
            let id = min
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            id.iter().flatten().collect::<Vec<_>>()
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn struct_field(parent: &StructArray, name: &str) -> StructArray {
    parent
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone()
}

/// Standard fixture: four single-row row groups.
///   id=1: x [0,10] part="a" | id=2: x [100,110] part="b"
///   id=3: x [200,210] part="a" | id=4: x [900,910] part="c"
fn standard_multi_rg() -> tempfile::NamedTempFile {
    write_checkpoint(&[
        &[RgSpec {
            id: 1,
            x_min: 0,
            x_max: 10,
            part: Some("a"),
        }],
        &[RgSpec {
            id: 2,
            x_min: 100,
            x_max: 110,
            part: Some("b"),
        }],
        &[RgSpec {
            id: 3,
            x_min: 200,
            x_max: 210,
            part: Some("a"),
        }],
        &[RgSpec {
            id: 4,
            x_min: 900,
            x_max: 910,
            part: Some("c"),
        }],
    ])
}

/// Row-group pruning across predicate shapes, driven through the production rewrite + reader.
/// `part` is the partition column, `x` a data-stat column. Each case lists the surviving `id`s.
#[rstest]
#[case::stats_gt(Predicate::gt(column_name!("x"), Scalar::from(150i64)), vec![3, 4])]
#[case::stats_le(Predicate::le(column_name!("x"), Scalar::from(110i64)), vec![1, 2])]
#[case::stats_all_kept(Predicate::ge(column_name!("x"), Scalar::from(0i64)), vec![1, 2, 3, 4])]
#[case::partition_eq(Predicate::eq(column_name!("part"), Scalar::from("a")), vec![1, 3])]
#[case::partition_lt(Predicate::lt(column_name!("part"), Scalar::from("b")), vec![1, 3])]
#[case::partition_all_pruned(Predicate::eq(column_name!("part"), Scalar::from("z")), vec![])]
#[case::and_stats_and_partition(
    Predicate::and(
        Predicate::eq(column_name!("part"), Scalar::from("a")),
        Predicate::gt(column_name!("x"), Scalar::from(150i64)),
    ),
    vec![3]
)]
#[case::or_stats_or_partition(
    Predicate::or(
        Predicate::eq(column_name!("part"), Scalar::from("c")),
        Predicate::gt(column_name!("x"), Scalar::from(150i64)),
    ),
    vec![3, 4]
)]
// `part IS NULL` prunes every group when all partition values are present.
#[case::partition_is_null(Predicate::is_null(column_name!("part")), vec![])]
// `part IS NOT NULL` can't prune (a non-Add row also has a null value), so every group is kept.
#[case::partition_is_not_null(Predicate::is_not_null(column_name!("part")), vec![1, 2, 3, 4])]
fn reader_skips_expected_row_groups(#[case] pred: Predicate, #[case] expected: Vec<i64>) {
    let tmp = standard_multi_rg();
    assert_eq!(
        surviving_ids(&tmp, &pred, &["part"], &["x"]),
        expected,
        "{pred:?}"
    );
}

// `part IS NULL` keeps only a row group that actually contains a null partition value (id=2).
#[test]
fn reader_keeps_only_null_partition_row_group_for_partition_is_null() {
    let tmp = write_checkpoint(&[
        &[RgSpec {
            id: 1,
            x_min: 0,
            x_max: 10,
            part: Some("a"),
        }],
        &[RgSpec {
            id: 2,
            x_min: 100,
            x_max: 110,
            part: None,
        }],
        &[RgSpec {
            id: 3,
            x_min: 200,
            x_max: 210,
            part: Some("c"),
        }],
    ]);
    let pred = Predicate::is_null(column_name!("part"));
    assert_eq!(surviving_ids(&tmp, &pred, &["part"], &["x"]), vec![2]);
}

// A row group holding an add file alongside a null-partition sibling (a non-Add row) must be kept
// even when the predicate excludes the add value: the IS NULL guard keeps any group whose partition
// column contains a null, so the sibling is never dropped from the checkpoint scan. Without the
// guard the group's footer range ["a","a"] would prune it under `part = "z"`.
#[test]
fn reader_keeps_mixed_row_group_with_null_partition_sibling() {
    let tmp = write_checkpoint(&[&[
        RgSpec {
            id: 1,
            x_min: 0,
            x_max: 10,
            part: Some("a"),
        },
        RgSpec {
            id: 2,
            x_min: 100,
            x_max: 110,
            part: None,
        },
    ]]);
    let pred = Predicate::eq(column_name!("part"), Scalar::from("z"));
    assert_eq!(surviving_ids(&tmp, &pred, &["part"], &["x"]), vec![1, 2]);
}

// A row group of only non-Add rows (all partition values null) must never be pruned by a
// value-comparison partition predicate: dropping it would remove tombstones from checkpoint replay.
// The footer carries no partition min/max for the all-null group, and the IS NULL guard keeps it.
#[test]
fn reader_keeps_all_null_partition_row_group_under_value_comparison() {
    let tmp = write_checkpoint(&[
        &[RgSpec {
            id: 1,
            x_min: 0,
            x_max: 10,
            part: Some("a"),
        }],
        &[
            RgSpec {
                id: 2,
                x_min: 100,
                x_max: 110,
                part: None,
            },
            RgSpec {
                id: 3,
                x_min: 200,
                x_max: 210,
                part: None,
            },
        ],
    ]);
    let eq = Predicate::eq(column_name!("part"), Scalar::from("z"));
    assert_eq!(surviving_ids(&tmp, &eq, &["part"], &["x"]), vec![2, 3]);
    let gt = Predicate::gt(column_name!("part"), Scalar::from("m"));
    assert_eq!(surviving_ids(&tmp, &gt, &["part"], &["x"]), vec![2, 3]);
}
