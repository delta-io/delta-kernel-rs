//! Validates that add file statistics contain required columns.
//!
//! Per the Delta protocol, writers MUST write per-file statistics (nullCount, minValues,
//! maxValues) for certain required columns. For example, clustering columns require stats when
//! the `ClusteredTable` feature is enabled. This module validates that those stat entries
//! exist for each required column.

use std::sync::LazyLock;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::error::Error;
use crate::expressions::{column_name, ColumnName};
use crate::schema::{ColumnNamesAndTypes, DataType, DecimalType, PrimitiveType};
use crate::utils::require;
use crate::DeltaResult;

/// Verifies that add file statistics contain required columns.
///
/// For each required column, validates that `nullCount` is present (non-null) and that
/// `minValues` and `maxValues` are present unless the column is all-null
/// (`nullCount == numRecords`).
pub(crate) struct StatsVerifier {
    required_columns: Vec<(ColumnName, DataType)>,
}

impl StatsVerifier {
    /// Create a new verifier that checks statistics for the given required columns and types.
    pub(crate) fn new(required_columns: Vec<(ColumnName, DataType)>) -> Self {
        Self { required_columns }
    }

    /// Verify that all files in the provided batches have required statistics.
    ///
    /// For each required column, extracts all three stat columns (nullCount, minValues,
    /// maxValues) in a single `visit_rows` call per batch.
    pub(crate) fn verify(&self, add_files: &[Box<dyn crate::EngineData>]) -> DeltaResult<()> {
        if self.required_columns.is_empty() {
            return Ok(());
        }

        for (col, data_type) in &self.required_columns {
            self.verify_column(add_files, col, data_type)?;
        }

        Ok(())
    }

    /// Verify a single required column has nullCount, minValues, and maxValues stats in
    /// every file. Extracts all three stat columns in a single `visit_rows` call per batch.
    fn verify_column(
        &self,
        add_files: &[Box<dyn crate::EngineData>],
        column: &ColumnName,
        data_type: &DataType,
    ) -> DeltaResult<()> {
        let column_names = vec![
            ColumnName::new(["path"]),
            ColumnName::new(["stats", "numRecords"]),
            build_stat_path(column, "nullCount"),
            build_stat_path(column, "minValues"),
            build_stat_path(column, "maxValues"),
        ];
        let types = column_types_for(data_type)?;

        let mut missing_null_count: Vec<String> = Vec::new();
        let mut missing_min: Vec<String> = Vec::new();
        let mut missing_max: Vec<String> = Vec::new();

        for batch in add_files {
            let mut visitor = ColumnStatsVisitor {
                data_type,
                types,
                missing_null_count: &mut missing_null_count,
                missing_min: &mut missing_min,
                missing_max: &mut missing_max,
            };
            batch.visit_rows(&column_names, &mut visitor)?;
        }

        if !missing_null_count.is_empty() {
            return Err(Error::stats_validation(format!(
                "Required column '{column}' is missing 'nullCount' statistics for files: [{}]",
                missing_null_count.join(", ")
            )));
        }
        if !missing_min.is_empty() {
            return Err(Error::stats_validation(format!(
                "Required column '{column}' is missing 'minValues' statistics for files: [{}]",
                missing_min.join(", ")
            )));
        }
        if !missing_max.is_empty() {
            return Err(Error::stats_validation(format!(
                "Required column '{column}' is missing 'maxValues' statistics for files: [{}]",
                missing_max.join(", ")
            )));
        }
        Ok(())
    }
}

/// Build a stat column path: `stats.{category}.{column_path}`.
fn build_stat_path(column: &ColumnName, category: &str) -> ColumnName {
    let mut path = vec!["stats".to_string(), category.to_string()];
    path.extend(column.iter().map(|s| s.to_string()));
    ColumnName::new(path)
}

// Predefined static type arrays for per-column validation. Each array contains types for
// [path, numRecords, nullCount, minValues, maxValues], where numRecords and nullCount are
// always LONG and min/max use the column's original type. The column names are placeholders
// -- `visit_rows` receives actual column names as a separate parameter.
macro_rules! define_column_types {
    ($name:ident, $data_type:expr) => {
        static $name: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![
                column_name!("path"),
                column_name!("nr"),
                column_name!("nc"),
                column_name!("min"),
                column_name!("max"),
            ];
            let types = vec![
                DataType::STRING,
                DataType::LONG,
                DataType::LONG,
                $data_type,
                $data_type,
            ];
            (names, types).into()
        });
    };
}

define_column_types!(COL_TYPES_BOOL, DataType::BOOLEAN);
define_column_types!(COL_TYPES_BYTE, DataType::BYTE);
define_column_types!(COL_TYPES_SHORT, DataType::SHORT);
define_column_types!(COL_TYPES_INT, DataType::INTEGER);
define_column_types!(COL_TYPES_LONG, DataType::LONG);
define_column_types!(COL_TYPES_STRING, DataType::STRING);
define_column_types!(COL_TYPES_BINARY, DataType::BINARY);
define_column_types!(COL_TYPES_FLOAT, DataType::FLOAT);
define_column_types!(COL_TYPES_DOUBLE, DataType::DOUBLE);
define_column_types!(COL_TYPES_DATE, DataType::DATE);
define_column_types!(COL_TYPES_TIMESTAMP, DataType::TIMESTAMP);
define_column_types!(COL_TYPES_TIMESTAMP_NTZ, DataType::TIMESTAMP_NTZ);
#[allow(clippy::unwrap_used)]
static COL_TYPES_DECIMAL: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    let names = vec![
        column_name!("path"),
        column_name!("nr"),
        column_name!("nc"),
        column_name!("min"),
        column_name!("max"),
    ];
    let types = vec![
        DataType::STRING,
        DataType::LONG,
        DataType::LONG,
        DataType::Primitive(PrimitiveType::Decimal(DecimalType::try_new(38, 0).unwrap())),
        DataType::Primitive(PrimitiveType::Decimal(DecimalType::try_new(38, 0).unwrap())),
    ];
    (names, types).into()
});

/// Select the predefined static type array for a given column data type.
fn column_types_for(dt: &DataType) -> DeltaResult<&'static ColumnNamesAndTypes> {
    match dt {
        &DataType::BOOLEAN => Ok(&COL_TYPES_BOOL),
        &DataType::BYTE => Ok(&COL_TYPES_BYTE),
        &DataType::SHORT => Ok(&COL_TYPES_SHORT),
        &DataType::INTEGER => Ok(&COL_TYPES_INT),
        &DataType::LONG => Ok(&COL_TYPES_LONG),
        &DataType::STRING => Ok(&COL_TYPES_STRING),
        &DataType::BINARY => Ok(&COL_TYPES_BINARY),
        &DataType::FLOAT => Ok(&COL_TYPES_FLOAT),
        &DataType::DOUBLE => Ok(&COL_TYPES_DOUBLE),
        &DataType::DATE => Ok(&COL_TYPES_DATE),
        &DataType::TIMESTAMP => Ok(&COL_TYPES_TIMESTAMP),
        &DataType::TIMESTAMP_NTZ => Ok(&COL_TYPES_TIMESTAMP_NTZ),
        DataType::Primitive(PrimitiveType::Decimal(_)) => Ok(&COL_TYPES_DECIMAL),
        DataType::Primitive(PrimitiveType::Void)
        | DataType::Struct(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Variant(_) => Err(Error::internal_error(format!(
            "Unsupported data type for stats validation: {dt}"
        ))),
    }
}

/// Check if a stat value is present (non-null) using the appropriate typed getter.
fn is_stat_present<'b>(
    getter: &'b dyn GetData<'b>,
    row_idx: usize,
    data_type: &DataType,
) -> DeltaResult<bool> {
    let field_name = "stat";
    match data_type {
        &DataType::BOOLEAN => Ok(getter.get_bool(row_idx, field_name)?.is_some()),
        &DataType::BYTE => Ok(getter.get_byte(row_idx, field_name)?.is_some()),
        &DataType::SHORT => Ok(getter.get_short(row_idx, field_name)?.is_some()),
        &DataType::INTEGER => Ok(getter.get_int(row_idx, field_name)?.is_some()),
        &DataType::LONG => Ok(getter.get_long(row_idx, field_name)?.is_some()),
        &DataType::FLOAT => Ok(getter.get_float(row_idx, field_name)?.is_some()),
        &DataType::DOUBLE => Ok(getter.get_double(row_idx, field_name)?.is_some()),
        &DataType::DATE => Ok(getter.get_date(row_idx, field_name)?.is_some()),
        &DataType::TIMESTAMP | &DataType::TIMESTAMP_NTZ => {
            Ok(getter.get_timestamp(row_idx, field_name)?.is_some())
        }
        &DataType::STRING => Ok(getter.get_str(row_idx, field_name)?.is_some()),
        &DataType::BINARY => Ok(getter.get_binary(row_idx, field_name)?.is_some()),
        DataType::Primitive(PrimitiveType::Decimal(_)) => {
            Ok(getter.get_decimal(row_idx, field_name)?.is_some())
        }
        DataType::Primitive(PrimitiveType::Void)
        | DataType::Struct(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Variant(_) => Err(Error::internal_error(format!(
            "Unsupported data type for stats presence check: {data_type}"
        ))),
    }
}

/// Visitor that checks nullCount, minValues, and maxValues for a single column in one pass.
/// Expects 5 getters: [path, numRecords, nullCount, minValues, maxValues].
struct ColumnStatsVisitor<'a> {
    data_type: &'a DataType,
    types: &'static ColumnNamesAndTypes,
    missing_null_count: &'a mut Vec<String>,
    missing_min: &'a mut Vec<String>,
    missing_max: &'a mut Vec<String>,
}

impl RowVisitor for ColumnStatsVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.types.as_ref()
    }

    fn visit<'b>(&mut self, row_count: usize, getters: &[&'b dyn GetData<'b>]) -> DeltaResult<()> {
        require!(
            getters.len() == 5,
            Error::internal_error(format!(
                "Expected 5 getters for column stats validation, got {}",
                getters.len()
            ))
        );

        for row_idx in 0..row_count {
            let path: String = getters[0].get(row_idx, "path")?;
            let num_records = getters[1].get_long(row_idx, "numRecords")?;
            let null_count = getters[2].get_long(row_idx, "nullCount")?;

            // When all rows are null (or the file is empty), minValues/maxValues are
            // expected to be null since there are no non-null values to aggregate.
            let all_null = matches!((num_records, null_count), (Some(nr), Some(nc)) if nr == nc);

            if null_count.is_none() {
                self.missing_null_count.push(path.clone());
            }
            if !(all_null || is_stat_present(getters[3], row_idx, self.data_type)?) {
                self.missing_min.push(path.clone());
            }
            if !(all_null || is_stat_present(getters[4], row_idx, self.data_type)?) {
                self.missing_max.push(path);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::types::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, TimestampMicrosecondType,
    };
    use crate::arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Int16Array, Int64Array, Int8Array,
        LargeStringArray, PrimitiveArray, RecordBatch, StringArray, StringViewArray, StructArray,
    };
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
    };
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::stats::collect_stats;
    use crate::expressions::column_name;
    use crate::EngineData;

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
        let verifier = StatsVerifier::new(columns);
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
        let verifier = StatsVerifier::new(columns);
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
            let verifier = StatsVerifier::new(vec![(ColumnName::new(["col"]), DataType::LONG)]);
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
        let verifier = StatsVerifier::new(columns);
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

        let verifier = StatsVerifier::new(vec![]);
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
        let verifier = StatsVerifier::new(columns);
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
        let verifier = StatsVerifier::new(columns);
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
        assert!(StatsVerifier::new(columns).verify(&[batch]).is_ok());

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
        let err_msg = StatsVerifier::new(columns)
            .verify(&[batch])
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("col_b"));
        assert!(err_msg.contains("minValues"));
        assert!(!err_msg.contains("col_a"));
    }

    /// Verifies that stats collected from non-standard Arrow string representations
    /// (LargeUtf8/LargeStringArray, Utf8View/StringViewArray) can be validated by
    /// StatsVerifier, which expects Delta's logical STRING type. Engines may use any of
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

        let verifier = StatsVerifier::new(vec![(ColumnName::new(["city"]), DataType::STRING)]);
        verifier.verify(&[engine_data]).unwrap();
    }

    /// Verify collect_stats produces correct stats shape for all-null and empty batches.
    /// These cases keep the column in minValues/maxValues with null values (so that
    /// StatsVerifier can find the field via visit_rows and check nullCount == numRecords).
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

        let verifier = StatsVerifier::new(vec![(ColumnName::new(["col"]), dt)]);
        verifier.verify(&[engine_data]).unwrap();
    }
}
