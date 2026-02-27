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
/// For each required column, validates that `nullCount`, `minValues`, and `maxValues`
/// entries are present (non-null) in every add file's statistics.
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
// [path, nullCount, minValues, maxValues], where nullCount is always LONG and min/max use
// the column's original type. The column names are placeholders — `visit_rows` receives
// actual column names as a separate parameter.
macro_rules! define_column_types {
    ($name:ident, $data_type:expr) => {
        static $name: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![
                column_name!("path"),
                column_name!("nc"),
                column_name!("min"),
                column_name!("max"),
            ];
            let types = vec![DataType::STRING, DataType::LONG, $data_type, $data_type];
            (names, types).into()
        });
    };
}

define_column_types!(COL_TYPES_BOOL, DataType::BOOLEAN);
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
        column_name!("nc"),
        column_name!("min"),
        column_name!("max"),
    ];
    let types = vec![
        DataType::STRING,
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
        _ => Err(Error::internal_error(format!(
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
        _ => Err(Error::internal_error(format!(
            "Unsupported data type for stats presence check: {data_type}"
        ))),
    }
}

/// Visitor that checks nullCount, minValues, and maxValues for a single column in one pass.
/// Expects 4 getters: [path, nullCount, minValues, maxValues].
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
            getters.len() == 4,
            Error::internal_error(format!(
                "Expected 4 getters for column stats validation, got {}",
                getters.len()
            ))
        );

        for row_idx in 0..row_count {
            let path: String = getters[0].get(row_idx, "path")?;
            // nullCount is always LONG regardless of the column's original type
            if getters[1].get_long(row_idx, "nullCount")?.is_none() {
                self.missing_null_count.push(path.clone());
            }
            if !is_stat_present(getters[2], row_idx, self.data_type)? {
                self.missing_min.push(path.clone());
            }
            if !is_stat_present(getters[3], row_idx, self.data_type)? {
                self.missing_max.push(path);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
    };
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::EngineData;

    /// Creates test add file data with stats.nullCount.col, stats.minValues.col,
    /// and stats.maxValues.col — all of type LONG.
    fn create_add_file_batch(
        paths: Vec<&str>,
        null_counts: Vec<Option<i64>>,
        min_values: Vec<Option<i64>>,
        max_values: Vec<Option<i64>>,
    ) -> Box<dyn EngineData> {
        assert_eq!(paths.len(), null_counts.len());
        assert_eq!(paths.len(), min_values.len());
        assert_eq!(paths.len(), max_values.len());

        let path_array = StringArray::from(paths.to_vec());
        let col_field = Arc::new(ArrowField::new("col", ArrowDataType::Int64, true));

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
            inner_struct_type("nullCount"),
            inner_struct_type("minValues"),
            inner_struct_type("maxValues"),
        ]);
        let stats_struct = StructArray::new(
            stats_fields.clone(),
            vec![
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
            let batch =
                create_add_file_batch(vec!["file1.parquet"], null_counts, min_values, max_values);
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
            vec![Some(0)],
            vec![Some(1)],
            vec![Some(100)],
        );
        let batch2 =
            create_add_file_batch(vec!["bad_file.parquet"], vec![None], vec![None], vec![None]);

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
        let batch =
            create_add_file_batch(vec!["file1.parquet"], vec![None], vec![None], vec![None]);

        let verifier = StatsVerifier::new(vec![]);
        let result = verifier.verify(&[batch]);
        assert!(result.is_ok());
    }

    /// Creates test data with two columns (col_a, col_b) in stats, both LONG.
    fn create_two_column_batch(
        paths: Vec<&str>,
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

        let null_count_struct = make_struct(col_a_nullcount, col_b_nullcount);
        let min_values_struct = make_struct(col_a_min, col_b_min);
        let max_values_struct = make_struct(col_a_max, col_b_max);

        let inner_type = ArrowDataType::Struct(Fields::from(vec![
            ArrowField::new("col_a", ArrowDataType::Int64, true),
            ArrowField::new("col_b", ArrowDataType::Int64, true),
        ]));
        let stats_fields = Fields::from(vec![
            ArrowField::new("nullCount", inner_type.clone(), true),
            ArrowField::new("minValues", inner_type.clone(), true),
            ArrowField::new("maxValues", inner_type, true),
        ]);
        let stats_struct = StructArray::new(
            stats_fields.clone(),
            vec![
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
}
