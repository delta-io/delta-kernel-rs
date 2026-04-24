use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::File;
use std::sync::{Arc, LazyLock};

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
use crate::parquet::file::properties::WriterProperties;
use crate::parquet::file::reader::FileReader;
use crate::parquet::file::serialized_reader::SerializedFileReader;
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

    // Should be Some(0), but https://github.com/apache/arrow-rs/issues/9451
    assert_eq!(
        filter.get_nullcount_stat(&column_name!("varlen.utf8")),
        None // Some(0i64.into())
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
        "minValues",
        col_path,
        Arc::new(Int64Array::from(min_values.to_vec())),
    );
    let (max_f, max_a) = build_stat_column(
        "maxValues",
        col_path,
        Arc::new(Int64Array::from(max_values.to_vec())),
    );
    let (nc_f, nc_a) = build_stat_column(
        "nullCount",
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
fn checkpoint_filter_partition_nullcount_is_null() {
    // All partition values are non-null, so footer nullcount for partitionValues_parsed.part_col
    // is 0. extract_nullcount suppresses Some(0) due to the arrow-rs#9451 workaround, so
    // get_nullcount_stat returns None. IS NULL on the partition column can't prune.
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

    // extract_nullcount never returns Some(0), so nullcount stat is None (can't decide).
    assert_eq!(filter.get_nullcount_stat(&column_name!("part_col")), None);
    // IS NULL evaluates to None (can't decide) -> row group is kept.
    assert_eq!(filter.eval_sql_where(&predicate), None);
}

#[test]
fn checkpoint_filter_multi_row_group_skipping() {
    // Build schema: add.stats_parsed.{minValues,maxValues,nullCount}.x (INT64)
    let col_field = Arc::new(Field::new("x", ArrowDataType::Int64, true));
    let min_field = Arc::new(Field::new(
        "minValues",
        ArrowDataType::Struct(Fields::from(vec![col_field.clone()])),
        true,
    ));
    let max_field = Arc::new(Field::new(
        "maxValues",
        ArrowDataType::Struct(Fields::from(vec![col_field.clone()])),
        true,
    ));
    let nc_field = Arc::new(Field::new(
        "nullCount",
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
