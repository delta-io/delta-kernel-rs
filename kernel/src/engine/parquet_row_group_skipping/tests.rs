use super::*;
use crate::expressions::{column_expr, column_name};
use crate::predicates::DataSkippingPredicateEvaluator as _;
use crate::Expression;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use std::fs::File;

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

    // The expression doesn't matter -- it just needs to mention all the columns we care about.
    let columns = Expression::and_from(vec![
        column_expr!("varlen.utf8"),
        column_expr!("numeric.ints.int64"),
        column_expr!("numeric.ints.int32"),
        column_expr!("numeric.ints.int16"),
        column_expr!("numeric.ints.int8"),
        column_expr!("numeric.floats.float32"),
        column_expr!("numeric.floats.float64"),
        column_expr!("bool"),
        column_expr!("varlen.binary"),
        column_expr!("numeric.decimals.decimal32"),
        column_expr!("numeric.decimals.decimal64"),
        column_expr!("numeric.decimals.decimal128"),
        column_expr!("chrono.date32"),
        column_expr!("chrono.timestamp"),
        column_expr!("chrono.timestamp_ntz"),
    ]);
    let filter = RowGroupFilter::new(metadata.metadata().row_group(0), &columns);

    assert_eq!(filter.get_rowcount_stat(), Some(5));

    // Only the BOOL column has any nulls
    assert_eq!(filter.get_nullcount_stat(&column_name!("bool")), Some(3));
    assert_eq!(
        filter.get_nullcount_stat(&column_name!("varlen.utf8")),
        Some(0)
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
        Some(Scalar::Decimal(11032, 8, 3))
    );

    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal64"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::Decimal(11064, 16, 3))
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::Decimal(11032, 16, 3))
    );

    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal128"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::Decimal(11128, 32, 3))
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal64"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::Decimal(11064, 32, 3))
    );

    // type widening!
    assert_eq!(
        filter.get_min_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::Decimal(11032, 32, 3))
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("chrono.date32"), &DataType::DATE),
        Some(PrimitiveType::Date.parse_scalar("1971-01-01").unwrap())
    );

    assert_eq!(
        filter.get_min_stat(&column_name!("chrono.timestamp"), &DataType::TIMESTAMP),
        None // Timestamp defaults to 96-bit, which doesn't get stats
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
        Some(Scalar::Decimal(15032, 8, 3))
    );

    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal64"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::Decimal(15064, 16, 3))
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(16, 3).unwrap()
        ),
        Some(Scalar::Decimal(15032, 16, 3))
    );

    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal128"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::Decimal(15128, 32, 3))
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal64"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::Decimal(15064, 32, 3))
    );

    // type widening!
    assert_eq!(
        filter.get_max_stat(
            &column_name!("numeric.decimals.decimal32"),
            &DataType::decimal(32, 3).unwrap()
        ),
        Some(Scalar::Decimal(15032, 32, 3))
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("chrono.date32"), &DataType::DATE),
        Some(PrimitiveType::Date.parse_scalar("1971-01-05").unwrap())
    );

    assert_eq!(
        filter.get_max_stat(&column_name!("chrono.timestamp"), &DataType::TIMESTAMP),
        None // Timestamp defaults to 96-bit, which doesn't get stats
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
