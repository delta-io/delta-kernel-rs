use super::*;
use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch, StructArray};
use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema};
use crate::expressions::{column_expr, column_name, column_pred, Expression};
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::parquet::arrow::arrow_reader::ArrowReaderMetadata;
use crate::parquet::file::properties::WriterProperties;
use crate::Predicate;
use std::fs::File;
use std::sync::Arc;

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

#[test]
fn test_checkpoint_layout_detection() {
    let value_field = Field::new("value", ArrowDataType::Int64, true);
    let min_values = StructArray::new(
        Fields::from(vec![value_field.clone()]),
        vec![Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef],
        None,
    );
    let max_values = StructArray::new(
        Fields::from(vec![value_field.clone()]),
        vec![Arc::new(Int64Array::from(vec![Some(10)])) as ArrayRef],
        None,
    );
    let stats_parsed = StructArray::new(
        Fields::from(vec![
            Field::new(
                "minValues",
                ArrowDataType::Struct(Fields::from(vec![value_field.clone()])),
                true,
            ),
            Field::new(
                "maxValues",
                ArrowDataType::Struct(Fields::from(vec![value_field])),
                true,
            ),
        ]),
        vec![Arc::new(min_values) as ArrayRef, Arc::new(max_values) as ArrayRef],
        None,
    );
    let add = StructArray::new(
        Fields::from(vec![Field::new(
            "stats_parsed",
            ArrowDataType::Struct(stats_parsed.fields().clone()),
            true,
        )]),
        vec![Arc::new(stats_parsed) as ArrayRef],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "add",
            ArrowDataType::Struct(add.fields().clone()),
            true,
        )])),
        vec![Arc::new(add) as ArrayRef],
    )
    .unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let mut writer = ArrowWriter::try_new(tmp.reopen().unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let checkpoint_md = ArrowReaderMetadata::load(&tmp.reopen().unwrap(), Default::default()).unwrap();
    assert!(has_checkpoint_stats_layout(checkpoint_md.metadata().row_group(0)));

    let regular = File::open(
        "./tests/data/parquet_row_group_skipping/part-00000-b92e017a-50ba-4676-8322-48fc371c2b59-c000.snappy.parquet",
    )
    .unwrap();
    let regular_md = ArrowReaderMetadata::load(&regular, Default::default()).unwrap();
    assert!(!has_checkpoint_stats_layout(regular_md.metadata().row_group(0)));
}

#[test]
fn test_checkpoint_stats_filter_handles_missing_min_max_independently() {
    fn checkpoint_batch(min: Option<i64>, max: Option<i64>) -> RecordBatch {
        let value_field = Field::new("value", ArrowDataType::Int64, true);
        let min_values_field = Field::new(
            "minValues",
            ArrowDataType::Struct(Fields::from(vec![value_field.clone()])),
            true,
        );
        let max_values_field = Field::new(
            "maxValues",
            ArrowDataType::Struct(Fields::from(vec![value_field.clone()])),
            true,
        );
        let stats_parsed_field = Field::new(
            "stats_parsed",
            ArrowDataType::Struct(Fields::from(vec![
                min_values_field.clone(),
                max_values_field.clone(),
            ])),
            true,
        );
        let add_field = Field::new(
            "add",
            ArrowDataType::Struct(Fields::from(vec![stats_parsed_field.clone()])),
            true,
        );

        let min_values = StructArray::new(
            Fields::from(vec![value_field.clone()]),
            vec![Arc::new(Int64Array::from(vec![min])) as ArrayRef],
            None,
        );
        let max_values = StructArray::new(
            Fields::from(vec![value_field]),
            vec![Arc::new(Int64Array::from(vec![max])) as ArrayRef],
            None,
        );
        let stats_parsed = StructArray::new(
            Fields::from(vec![min_values_field, max_values_field]),
            vec![Arc::new(min_values) as ArrayRef, Arc::new(max_values) as ArrayRef],
            None,
        );
        let add = StructArray::new(
            Fields::from(vec![stats_parsed_field]),
            vec![Arc::new(stats_parsed) as ArrayRef],
            None,
        );

        let schema = Arc::new(ArrowSchema::new(vec![add_field]));
        RecordBatch::try_new(schema, vec![Arc::new(add) as ArrayRef]).unwrap()
    }

    let tmp = tempfile::NamedTempFile::new().unwrap();
    // Row group 0: min is complete but max is missing.
    let batch1 = checkpoint_batch(Some(100), None);
    // Row group 1: max is complete but min is missing.
    let batch2 = checkpoint_batch(None, Some(10));
    {
        let props = WriterProperties::builder()
            .set_max_row_group_size(1)
            .build();
        let mut writer = ArrowWriter::try_new(
            tmp.reopen().unwrap(),
            batch1.schema(),
            Some(props.into()),
        )
        .unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.close().unwrap();
    }

    let file = tmp.reopen().unwrap();
    let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
    assert_eq!(metadata.metadata().num_row_groups(), 2);

    let pred_gt = Predicate::gt(column_expr!("value"), Expression::literal(50i64));
    // GT relies on maxValues. With max missing in row group 0, pruning fails (group is kept).
    assert!(CheckpointStatsRowGroupFilter::apply(
        metadata.metadata().row_group(0),
        &pred_gt
    ));
    // maxValues is complete in row group 1 and max=10 proves no row can satisfy value > 50.
    assert!(!CheckpointStatsRowGroupFilter::apply(
        metadata.metadata().row_group(1),
        &pred_gt
    ));

    let pred_lt = Predicate::lt(column_expr!("value"), Expression::literal(50i64));
    // LT relies on minValues. min=100 in row group 0 proves no row can satisfy value < 50.
    assert!(!CheckpointStatsRowGroupFilter::apply(
        metadata.metadata().row_group(0),
        &pred_lt
    ));
    // With min missing in row group 1, pruning fails (group is kept).
    assert!(CheckpointStatsRowGroupFilter::apply(
        metadata.metadata().row_group(1),
        &pred_lt
    ));
}
