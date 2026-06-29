//! Round-trip tests that verify `collect_stats` output passes the kernel
//! `StatsColumnVerifier` for every supported logical type, plus pure verifier
//! coverage for the validator's edge cases. These tests live here because
//! `collect_stats` is a default-engine concern; kernel only owns the verifier
//! contract.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::arrow::array::types::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, TimestampMicrosecondType,
    };
    use delta_kernel::arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Int16Array, Int64Array, Int8Array,
        LargeStringArray, PrimitiveArray, RecordBatch, StringArray, StringViewArray, StructArray,
    };
    #[cfg(feature = "nanosecond-timestamps")]
    use crate::arrow::array::TimestampNanosecondArray;
    use delta_kernel::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
    };
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::{column_name, ColumnName};
    use delta_kernel::schema::DataType;
    use delta_kernel::transaction::stats_verifier::{
        verify_num_records_present, StatsColumnVerifier,
    };
    use delta_kernel::{EngineData, Error};
    use rstest::rstest;

    use crate::stats::collect_stats;

    /// Creates test add file data with stats.numRecords, stats.nullCount.col,
    /// stats.minValues.col, and stats.maxValues.col — all of type LONG.
    fn create_add_file_batch(
        paths: Vec<&str>,
        num_records: Vec<Option<i64>>,
        null_counts: Vec<Option<i64>>,
        min_values: Vec<Option<i64>>,
        max_values: Vec<Option<i64>>,
    ) -> Box<dyn EngineData> {
        assert_eq!(paths.len(), num_records.len());
        assert_eq!(paths.len(), null_counts.len());
        assert_eq!(paths.len(), min_values.len());
        assert_eq!(paths.len(), max_values.len());

        let path_array = StringArray::from(paths.to_vec());
        let col_field = Arc::new(ArrowField::new("col", ArrowDataType::Int64, true));

        let num_records_array = Int64Array::from(num_records);
        let null_count_struct = StructArray::new(
            Fields::from(vec![col_field.clone()]),
            vec![Arc::new(Int64Array::from(null_counts)) as ArrayRef],
            None,
        );
        let min_values_struct = StructArray::new(
            Fields::from(vec![col_field.clone()]),
            vec![Arc::new(Int64Array::from(min_values)) as ArrayRef],
            None,
        );
        let max_values_struct = StructArray::new(
            Fields::from(vec![col_field]),
            vec![Arc::new(Int64Array::from(max_values)) as ArrayRef],
            None,
        );

        let inner_struct_type = |name: &str| {
            ArrowField::new(
                name,
                ArrowDataType::Struct(Fields::from(vec![ArrowField::new(
                    "col",
                    ArrowDataType::Int64,
                    true,
                )])),
                true,
            )
        };

        let stats_fields = Fields::from(vec![
            ArrowField::new("numRecords", ArrowDataType::Int64, true),
            inner_struct_type("nullCount"),
            inner_struct_type("minValues"),
            inner_struct_type("maxValues"),
        ]);
        let stats_struct = StructArray::new(
            stats_fields.clone(),
            vec![
                Arc::new(num_records_array) as ArrayRef,
                Arc::new(null_count_struct) as ArrayRef,
                Arc::new(min_values_struct) as ArrayRef,
                Arc::new(max_values_struct) as ArrayRef,
            ],
            None,
        );

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("stats", ArrowDataType::Struct(stats_fields), true),
        ]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(path_array), Arc::new(stats_struct)])
                .unwrap();

        Box::new(ArrowEngineData::new(batch))
    }

    #[test]
    fn test_verifier_with_empty_add_files() {
        let columns = vec![(ColumnName::new(["col"]), DataType::LONG)];
        let verifier = StatsColumnVerifier::new(columns);
        let result = verifier.verify(&[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_valid_stats() {
        let batch = create_add_file_batch(
            vec!["file1.parquet", "file2.parquet"],
            vec![Some(100), Some(100)],
            vec![Some(0), Some(5)],
            vec![Some(1), Some(10)],
            vec![Some(100), Some(50)],
        );

        let columns = vec![(ColumnName::new(["col"]), DataType::LONG)];
        let verifier = StatsColumnVerifier::new(columns);
        let result = verifier.verify(&[batch]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_missing_stat_category() {
        let cases = [
            ("nullCount", vec![None], vec![Some(1)], vec![Some(100)]),
            ("minValues", vec![Some(0)], vec![None], vec![Some(100)]),
            ("maxValues", vec![Some(0)], vec![Some(1)], vec![None]),
        ];
        for (category, null_counts, min_values, max_values) in cases {
            let batch = create_add_file_batch(
                vec!["file1.parquet"],
                vec![Some(100)],
                null_counts,
                min_values,
                max_values,
            );
            let verifier =
                StatsColumnVerifier::new(vec![(ColumnName::new(["col"]), DataType::LONG)]);
            let err_msg = verifier.verify(&[batch]).unwrap_err().to_string();
            assert!(err_msg.contains("file1.parquet"), "case: {category}");
            assert!(err_msg.contains(category), "case: {category}");
        }
    }

    #[test]
    fn test_verify_multiple_batches() {
        let batch1 = create_add_file_batch(
            vec!["good_file.parquet"],
            vec![Some(100)],
            vec![Some(0)],
            vec![Some(1)],
            vec![Some(100)],
        );
        let batch2 = create_add_file_batch(
            vec!["bad_file.parquet"],
            vec![Some(100)],
            vec![None],
            vec![None],
            vec![None],
        );

        let columns = vec![(ColumnName::new(["col"]), DataType::LONG)];
        let verifier = StatsColumnVerifier::new(columns);
        let result = verifier.verify(&[batch1, batch2]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("bad_file.parquet"));
        assert!(!err_msg.contains("good_file.parquet"));
    }

    #[test]
    fn test_verify_no_required_columns() {
        let batch = create_add_file_batch(
            vec!["file1.parquet"],
            vec![Some(100)],
            vec![None],
            vec![None],
            vec![None],
        );

        let verifier = StatsColumnVerifier::new(vec![]);
        let result = verifier.verify(&[batch]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_all_null_column_allows_null_min_max() {
        // nullCount == numRecords means all rows are null, so null min/max is valid
        let batch = create_add_file_batch(
            vec!["file1.parquet"],
            vec![Some(100)],
            vec![Some(100)],
            vec![None],
            vec![None],
        );

        let columns = vec![(ColumnName::new(["col"]), DataType::LONG)];
        let verifier = StatsColumnVerifier::new(columns);
        assert!(verifier.verify(&[batch]).is_ok());
    }

    #[test]
    fn test_verify_partial_null_column_requires_min_max() {
        // nullCount < numRecords means not all rows are null, so min/max must be present
        let batch = create_add_file_batch(
            vec!["file1.parquet"],
            vec![Some(100)],
            vec![Some(50)],
            vec![None],
            vec![None],
        );

        let columns = vec![(ColumnName::new(["col"]), DataType::LONG)];
        let verifier = StatsColumnVerifier::new(columns);
        let result = verifier.verify(&[batch]);
        assert!(matches!(result, Err(Error::StatsValidation(_))));
        let err = result.unwrap_err().to_string();
        assert!(err.contains("minValues"));
    }

    /// Creates test data with two columns (col_a, col_b) in stats, both LONG.
    #[allow(clippy::too_many_arguments)]
    fn create_two_column_batch(
        paths: Vec<&str>,
        num_records: Vec<Option<i64>>,
        col_a_nullcount: Vec<Option<i64>>,
        col_a_min: Vec<Option<i64>>,
        col_a_max: Vec<Option<i64>>,
        col_b_nullcount: Vec<Option<i64>>,
        col_b_min: Vec<Option<i64>>,
        col_b_max: Vec<Option<i64>>,
    ) -> Box<dyn EngineData> {
        let path_array = StringArray::from(paths.to_vec());
        let col_a_field = Arc::new(ArrowField::new("col_a", ArrowDataType::Int64, true));
        let col_b_field = Arc::new(ArrowField::new("col_b", ArrowDataType::Int64, true));
        let both_fields = Fields::from(vec![col_a_field, col_b_field]);

        let make_struct = |a: Vec<Option<i64>>, b: Vec<Option<i64>>| {
            StructArray::new(
                both_fields.clone(),
                vec![
                    Arc::new(Int64Array::from(a)) as ArrayRef,
                    Arc::new(Int64Array::from(b)) as ArrayRef,
                ],
                None,
            )
        };

        let num_records_array = Int64Array::from(num_records);
        let null_count_struct = make_struct(col_a_nullcount, col_b_nullcount);
        let min_values_struct = make_struct(col_a_min, col_b_min);
        let max_values_struct = make_struct(col_a_max, col_b_max);

        let inner_type = ArrowDataType::Struct(Fields::from(vec![
            ArrowField::new("col_a", ArrowDataType::Int64, true),
            ArrowField::new("col_b", ArrowDataType::Int64, true),
        ]));
        let stats_fields = Fields::from(vec![
            ArrowField::new("numRecords", ArrowDataType::Int64, true),
            ArrowField::new("nullCount", inner_type.clone(), true),
            ArrowField::new("minValues", inner_type.clone(), true),
            ArrowField::new("maxValues", inner_type, true),
        ]);
        let stats_struct = StructArray::new(
            stats_fields.clone(),
            vec![
                Arc::new(num_records_array) as ArrayRef,
                Arc::new(null_count_struct) as ArrayRef,
                Arc::new(min_values_struct) as ArrayRef,
                Arc::new(max_values_struct) as ArrayRef,
            ],
            None,
        );

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("stats", ArrowDataType::Struct(stats_fields), true),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(path_array), Arc::new(stats_struct)])
                .unwrap();
        Box::new(ArrowEngineData::new(batch))
    }

    #[test]
    fn test_verify_multiple_columns() {
        // Both columns have valid stats
        let batch = create_two_column_batch(
            vec!["file1.parquet"],
            vec![Some(100)],
            vec![Some(0)],
            vec![Some(1)],
            vec![Some(10)],
            vec![Some(0)],
            vec![Some(2)],
            vec![Some(20)],
        );
        let columns = vec![
            (ColumnName::new(["col_a"]), DataType::LONG),
            (ColumnName::new(["col_b"]), DataType::LONG),
        ];
        assert!(StatsColumnVerifier::new(columns).verify(&[batch]).is_ok());

        // col_a valid, col_b missing minValues
        let batch = create_two_column_batch(
            vec!["file1.parquet"],
            vec![Some(100)],
            vec![Some(0)],
            vec![Some(1)],
            vec![Some(10)],
            vec![Some(0)],
            vec![None],
            vec![Some(20)],
        );
        let columns = vec![
            (ColumnName::new(["col_a"]), DataType::LONG),
            (ColumnName::new(["col_b"]), DataType::LONG),
        ];
        let err_msg = StatsColumnVerifier::new(columns)
            .verify(&[batch])
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("col_b"));
        assert!(err_msg.contains("minValues"));
        assert!(!err_msg.contains("col_a"));
    }

    /// Verifies that stats collected from non-standard Arrow string representations
    /// (LargeUtf8/LargeStringArray, Utf8View/StringViewArray) can be validated by
    /// StatsColumnVerifier, which expects Delta's logical STRING type. Engines may use any of
    /// these representations, and the stats pipeline must handle them without type errors.
    #[rstest]
    #[case::large_utf8(Arc::new(LargeStringArray::from(vec!["Austin", "Boston", "Chicago"])) as ArrayRef)]
    #[case::utf8_view(Arc::new(StringViewArray::from(vec!["Austin", "Boston", "Chicago"])) as ArrayRef)]
    fn test_verify_string_stats(#[case] values: ArrayRef) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "city",
            values.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("city")]).unwrap();

        let path_array = StringArray::from(vec!["file1.parquet"]);
        let add_file_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("stats", stats.data_type().clone(), true),
        ]));
        let add_file_batch = RecordBatch::try_new(
            add_file_schema,
            vec![
                Arc::new(path_array) as ArrayRef,
                Arc::new(stats) as ArrayRef,
            ],
        )
        .unwrap();

        let engine_data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(add_file_batch));

        let verifier =
            StatsColumnVerifier::new(vec![(ColumnName::new(["city"]), DataType::STRING)]);
        verifier.verify(&[engine_data]).unwrap();
    }

    /// Verify collect_stats produces correct stats shape for all-null and empty batches.
    /// These cases keep the column in minValues/maxValues with null values (so that
    /// StatsColumnVerifier can find the field via visit_rows and check nullCount == numRecords).
    #[rstest]
    #[case::all_null_values(Arc::new(Int64Array::from(vec![None::<i64>, None, None])) as ArrayRef)]
    #[case::empty_batch(Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef)]
    fn test_collected_stats_shape_for_all_null_and_empty(#[case] values: ArrayRef) {
        let num_rows = values.len();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "col",
            values.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("col")]).unwrap();

        // numRecords should match row count
        let num_records = stats
            .column_by_name("numRecords")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(num_records.value(0), num_rows as i64);

        // All-null/empty columns are present in minValues/maxValues with null values
        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(min_values.column_by_name("col").unwrap().is_null(0));

        let max_values = stats
            .column_by_name("maxValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(max_values.column_by_name("col").unwrap().is_null(0));
    }

    /// Round-trip test: collect_stats produces stats that pass verification for every
    /// stats-eligible type. Covers non-null, all-null, and empty patterns per type.
    #[rstest]
    // Note: BOOLEAN and BINARY are omitted for non-null cases because collect_stats does not
    // produce min/max for those types (they fall through to the wildcard in compute_leaf_agg).
    // All-null cases are still tested since null min/max is valid when nullCount == numRecords.
    #[case::boolean_all_null(
        Arc::new(BooleanArray::from(vec![None::<bool>, None, None])) as ArrayRef,
        DataType::BOOLEAN,
    )]
    #[case::byte(
        Arc::new(Int8Array::from(vec![Some(1i8), Some(2), Some(3)])) as ArrayRef,
        DataType::BYTE,
    )]
    #[case::byte_all_null(
        Arc::new(Int8Array::from(vec![None::<i8>, None, None])) as ArrayRef,
        DataType::BYTE,
    )]
    #[case::short(
        Arc::new(Int16Array::from(vec![Some(100i16), Some(200), Some(300)])) as ArrayRef,
        DataType::SHORT,
    )]
    #[case::short_all_null(
        Arc::new(Int16Array::from(vec![None::<i16>, None, None])) as ArrayRef,
        DataType::SHORT,
    )]
    #[case::integer(
        Arc::new(PrimitiveArray::<Int32Type>::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
        DataType::INTEGER,
    )]
    #[case::integer_all_null(
        Arc::new(PrimitiveArray::<Int32Type>::from(vec![None::<i32>, None, None])) as ArrayRef,
        DataType::INTEGER,
    )]
    #[case::long(
        Arc::new(Int64Array::from(vec![Some(1i64), Some(2), Some(3)])) as ArrayRef,
        DataType::LONG,
    )]
    #[case::long_all_null(
        Arc::new(Int64Array::from(vec![None::<i64>, None, None])) as ArrayRef,
        DataType::LONG,
    )]
    #[case::long_empty(
        Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef,
        DataType::LONG,
    )]
    #[case::float(
        Arc::new(PrimitiveArray::<Float32Type>::from(vec![Some(1.0f32), Some(2.0), Some(3.0)])) as ArrayRef,
        DataType::FLOAT,
    )]
    #[case::float_all_null(
        Arc::new(PrimitiveArray::<Float32Type>::from(vec![None::<f32>, None, None])) as ArrayRef,
        DataType::FLOAT,
    )]
    #[case::double(
        Arc::new(PrimitiveArray::<Float64Type>::from(vec![Some(1.0f64), Some(2.0), Some(3.0)])) as ArrayRef,
        DataType::DOUBLE,
    )]
    #[case::double_all_null(
        Arc::new(PrimitiveArray::<Float64Type>::from(vec![None::<f64>, None, None])) as ArrayRef,
        DataType::DOUBLE,
    )]
    #[case::date(
        Arc::new(PrimitiveArray::<Date32Type>::from(vec![Some(18000), Some(19000), Some(20000)])) as ArrayRef,
        DataType::DATE,
    )]
    #[case::date_all_null(
        Arc::new(PrimitiveArray::<Date32Type>::from(vec![None::<i32>, None, None])) as ArrayRef,
        DataType::DATE,
    )]
    #[case::timestamp(
        Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(vec![Some(1_000_000i64), Some(2_000_000), Some(3_000_000)])) as ArrayRef,
        DataType::TIMESTAMP,
    )]
    #[case::timestamp_all_null(
        Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(vec![None::<i64>, None, None])) as ArrayRef,
        DataType::TIMESTAMP,
    )]
    #[case::timestamp_ntz(
        Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(vec![Some(1_000_000i64), Some(2_000_000), Some(3_000_000)])) as ArrayRef,
        DataType::TIMESTAMP_NTZ,
    )]
    #[case::timestamp_ntz_all_null(
        Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(vec![None::<i64>, None, None])) as ArrayRef,
        DataType::TIMESTAMP_NTZ,
    )]
    #[cfg_attr(feature = "nanosecond-timestamps", case::timestamp_nanos(
        Arc::new(TimestampNanosecondArray::from(vec![Some(1_000_000i64), Some(2_000_000), Some(3_000_123)])) as ArrayRef,
        DataType::TIMESTAMP_NANOS,
    ))]
    #[cfg_attr(feature = "nanosecond-timestamps", case::timestamp_nanos_all_null(
        Arc::new(TimestampNanosecondArray::from(vec![None::<i64>, None, None])) as ArrayRef,
        DataType::TIMESTAMP_NANOS,
    ))]
    #[cfg_attr(feature = "nanosecond-timestamps", case::timestamp_nanos_ntz(
        Arc::new(TimestampNanosecondArray::from(vec![Some(1_000_000i64), Some(2_000_000), Some(3_000_123)])) as ArrayRef,
        DataType::TIMESTAMP_NANOS_NTZ,
    ))]
    #[cfg_attr(feature = "nanosecond-timestamps", case::timestamp_nanos_ntz_all_null(
        Arc::new(TimestampNanosecondArray::from(vec![None::<i64>, None, None])) as ArrayRef,
        DataType::TIMESTAMP_NANOS_NTZ,
    ))]
    #[case::string(
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])) as ArrayRef,
        DataType::STRING,
    )]
    #[case::string_all_null(
        Arc::new(StringArray::from(vec![None::<&str>, None, None])) as ArrayRef,
        DataType::STRING,
    )]
    #[case::binary_all_null(
        Arc::new(BinaryArray::from(vec![None::<&[u8]>, None, None])) as ArrayRef,
        DataType::BINARY,
    )]
    #[case::decimal(
        Arc::new(PrimitiveArray::<Decimal128Type>::from(vec![Some(100i128), Some(200), Some(300)]).with_precision_and_scale(10, 2).unwrap()) as ArrayRef,
        DataType::decimal(10, 2).unwrap(),
    )]
    #[case::decimal_all_null(
        Arc::new(PrimitiveArray::<Decimal128Type>::from(vec![None::<i128>, None, None]).with_precision_and_scale(10, 2).unwrap()) as ArrayRef,
        DataType::decimal(10, 2).unwrap(),
    )]
    fn test_collected_stats_pass_verification_all_types(
        #[case] values: ArrayRef,
        #[case] dt: DataType,
    ) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "col",
            values.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();

        let stats = collect_stats(&batch, &[column_name!("col")]).unwrap();

        let path_array = StringArray::from(vec!["file1.parquet"]);
        let add_file_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("stats", stats.data_type().clone(), true),
        ]));
        let add_file_batch = RecordBatch::try_new(
            add_file_schema,
            vec![
                Arc::new(path_array) as ArrayRef,
                Arc::new(stats) as ArrayRef,
            ],
        )
        .unwrap();

        let engine_data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(add_file_batch));

        let verifier = StatsColumnVerifier::new(vec![(ColumnName::new(["col"]), dt)]);
        verifier.verify(&[engine_data]).unwrap();
    }

    // ============================================================================
    // verify_num_records_present tests
    // ============================================================================

    #[rstest]
    #[case::empty_input(vec![], None, vec![])]
    #[case::all_present(
        vec![vec![("a.parquet", Some(10)), ("b.parquet", Some(20))]],
        None/* expected_first_offender */,
        vec![]/* later_offenders */,
    )]
    #[case::first_offender_named_later_offenders_hidden(
        vec![vec![("a.parquet", Some(10)), ("b.parquet", None), ("c.parquet", None)]],
        Some("b.parquet"),
        vec!["c.parquet"],
    )]
    #[case::short_circuits_across_batches(
        vec![
            vec![("a.parquet", None), ("b.parquet", Some(20))],
            vec![("c.parquet", Some(30)), ("d.parquet", None)],
        ],
        Some("a.parquet"),
        vec!["d.parquet"],
    )]
    fn test_verify_num_records_present(
        #[case] batches: Vec<Vec<(&str, Option<i64>)>>,
        #[case] expected_first_offender: Option<&str>,
        #[case] later_offenders: Vec<&str>,
    ) {
        let batches: Vec<Box<dyn EngineData>> = batches
            .into_iter()
            .map(|rows| {
                let (paths, num_records): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
                create_add_file_batch_with_num_records(paths, num_records)
            })
            .collect();
        let result = verify_num_records_present(&batches);
        match expected_first_offender {
            None => result.unwrap(),
            Some(path) => {
                let err = result.unwrap_err().to_string();
                assert!(
                    err.contains("'stats.numRecords' is required") && err.contains(path),
                    "expected error containing '{path}', but got: {err}",
                );
                for later_offender in &later_offenders {
                    assert!(
                        !err.contains(later_offender),
                        "error should not mention '{later_offender}': {err}",
                    );
                }
            }
        }
    }

    #[test]
    fn test_verify_num_records_present_flags_add_without_stats() {
        let batch = create_add_file_batch_with_stats_mask(
            vec!["a.parquet", "b.parquet"],
            vec![true, false],
        );
        let err = verify_num_records_present(&[batch])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("'stats.numRecords' is required") && err.contains("b.parquet"),
            "expected error naming b.parquet, got: {err}",
        );
        assert!(
            !err.contains("a.parquet"),
            "row with stats present should not be flagged: {err}",
        );
    }

    fn create_add_file_batch_with_num_records(
        paths: Vec<&str>,
        num_records: Vec<Option<i64>>,
    ) -> Box<dyn EngineData> {
        let n = paths.len();
        create_add_file_batch(
            paths,
            num_records,
            vec![None; n],
            vec![None; n],
            vec![None; n],
        )
    }

    /// Build an add-file batch where rows with `stats_present[i] == false` carry no `stats`
    /// at all; other rows get `numRecords = 10`.
    fn create_add_file_batch_with_stats_mask(
        paths: Vec<&str>,
        stats_present: Vec<bool>,
    ) -> Box<dyn EngineData> {
        assert_eq!(paths.len(), stats_present.len());
        let n = paths.len();
        let num_records: Vec<Option<i64>> = stats_present.iter().map(|p| p.then_some(10)).collect();
        let path_array = StringArray::from(paths);

        let col_field = Arc::new(ArrowField::new("col", ArrowDataType::Int64, true));
        let inner_struct_type = |name: &str| {
            ArrowField::new(
                name,
                ArrowDataType::Struct(Fields::from(vec![ArrowField::new(
                    "col",
                    ArrowDataType::Int64,
                    true,
                )])),
                true,
            )
        };
        let stats_fields = Fields::from(vec![
            ArrowField::new("numRecords", ArrowDataType::Int64, true),
            inner_struct_type("nullCount"),
            inner_struct_type("minValues"),
            inner_struct_type("maxValues"),
        ]);
        let zero_inner_struct_array = StructArray::new(
            Fields::from(vec![col_field]),
            vec![Arc::new(Int64Array::from(vec![None as Option<i64>; n])) as ArrayRef],
            None, /* null buffer */
        );
        let stats_struct_array = StructArray::new(
            stats_fields.clone(),
            vec![
                Arc::new(Int64Array::from(num_records)) as ArrayRef,
                Arc::new(zero_inner_struct_array.clone()) as ArrayRef,
                Arc::new(zero_inner_struct_array.clone()) as ArrayRef,
                Arc::new(zero_inner_struct_array) as ArrayRef,
            ],
            // null buffer: rows with stats_present[i] == false are null
            Some(stats_present.into_iter().collect()),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("stats", ArrowDataType::Struct(stats_fields), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(path_array), Arc::new(stats_struct_array)],
        )
        .unwrap();
        Box::new(ArrowEngineData::new(batch))
    }
}
