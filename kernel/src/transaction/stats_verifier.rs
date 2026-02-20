//! Validates that add file statistics contain required clustering columns.
//!
//! Per the Delta protocol, writers MUST write per-file statistics for clustering columns
//! when the `ClusteredTable` feature is enabled. This module validates that `nullCount`,
//! `minValues`, and `maxValues` entries exist for each required column.

use std::sync::LazyLock;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::error::Error;
use crate::expressions::{column_name, ColumnName};
use crate::schema::{ColumnNamesAndTypes, DataType, DecimalType, PrimitiveType, StructType};
use crate::utils::require;
use crate::DeltaResult;

/// Verifies that add file statistics contain the required clustering columns.
///
/// For each required column, validates that `nullCount`, `minValues`, and `maxValues`
/// entries are present (non-null) in every add file's statistics.
pub(crate) struct StatsVerifier {
    required_columns: Vec<(ColumnName, DataType)>,
}

impl StatsVerifier {
    /// Create a new verifier that requires statistics for the given columns and their types.
    pub(crate) fn new(required_columns: Vec<(ColumnName, DataType)>) -> Self {
        Self { required_columns }
    }

    /// Verify that all files in the provided batches have required statistics.
    pub(crate) fn verify(&self, add_files: &[Box<dyn crate::EngineData>]) -> DeltaResult<()> {
        if self.required_columns.is_empty() {
            return Ok(());
        }

        for (col, data_type) in &self.required_columns {
            self.verify_column(add_files, col, data_type)?;
        }

        Ok(())
    }

    /// Verify a single clustering column has all required stats in every file.
    fn verify_column(
        &self,
        add_files: &[Box<dyn crate::EngineData>],
        column: &ColumnName,
        data_type: &DataType,
    ) -> DeltaResult<()> {
        // nullCount is always LONG regardless of the column's original type
        self.verify_stat(add_files, column, "nullCount", &DataType::LONG)?;
        // minValues and maxValues use the column's original type
        self.verify_stat(add_files, column, "minValues", data_type)?;
        self.verify_stat(add_files, column, "maxValues", data_type)?;
        Ok(())
    }

    /// Verify a single stat category for a column across all files.
    fn verify_stat(
        &self,
        add_files: &[Box<dyn crate::EngineData>],
        column: &ColumnName,
        stat_category: &str,
        stat_type: &DataType,
    ) -> DeltaResult<()> {
        // Build the stat column path: stats.{category}.{column_path}
        let mut stat_path = vec!["stats".to_string(), stat_category.to_string()];
        stat_path.extend(column.iter().map(|s| s.to_string()));
        let stat_col = ColumnName::new(stat_path);

        let column_names = vec![ColumnName::new(["path"]), stat_col];
        let types = types_for_data_type(stat_type)?;

        let mut invalid_files: Vec<String> = Vec::new();

        for batch in add_files {
            let mut visitor = StatsPresenceVisitor::new(&mut invalid_files, types, stat_type);
            batch.visit_rows(&column_names, &mut visitor)?;
        }

        if invalid_files.is_empty() {
            return Ok(());
        }

        Err(Error::stats_validation(format!(
            "Clustering column '{}' is missing '{}' statistics for files: [{}]",
            column,
            stat_category,
            invalid_files.join(", ")
        )))
    }
}

/// Resolve a column's data type from the table schema by walking nested struct fields.
pub(crate) fn resolve_column_type(
    schema: &StructType,
    column: &ColumnName,
) -> DeltaResult<DataType> {
    let path = column.path();
    require!(
        !path.is_empty(),
        Error::internal_error(format!("Empty clustering column name: {column}"))
    );

    let mut current_struct = schema;
    for (i, part) in path.iter().enumerate() {
        let field = current_struct.field(part).ok_or_else(|| {
            Error::internal_error(format!(
                "Clustering column '{column}' not found in table schema"
            ))
        })?;
        if i == path.len() - 1 {
            return Ok(field.data_type().clone());
        }
        match field.data_type() {
            DataType::Struct(s) => current_struct = s,
            _ => {
                return Err(Error::internal_error(format!(
                    "Expected struct type at '{part}' in column '{column}'"
                )));
            }
        }
    }

    unreachable!("Loop should return for the last path component")
}

// Predefined static type arrays for supported data types. `visit_rows` reads column types
// from `selected_column_names_and_types()` to create typed getters. The column names here
// are placeholders — `visit_rows` receives actual column names as a separate parameter.
macro_rules! define_stat_types {
    ($name:ident, $data_type:expr) => {
        static $name: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![column_name!("path"), column_name!("stat")];
            let types = vec![DataType::STRING, $data_type];
            (names, types).into()
        });
    };
}

define_stat_types!(TYPES_BOOL, DataType::BOOLEAN);
define_stat_types!(TYPES_INT, DataType::INTEGER);
define_stat_types!(TYPES_LONG, DataType::LONG);
define_stat_types!(TYPES_STRING, DataType::STRING);
define_stat_types!(TYPES_BINARY, DataType::BINARY);
define_stat_types!(TYPES_FLOAT, DataType::FLOAT);
define_stat_types!(TYPES_DOUBLE, DataType::DOUBLE);
define_stat_types!(TYPES_DATE, DataType::DATE);
define_stat_types!(TYPES_TIMESTAMP, DataType::TIMESTAMP);
define_stat_types!(TYPES_TIMESTAMP_NTZ, DataType::TIMESTAMP_NTZ);
// Precision and scale don't affect physical layout; any valid DecimalType works here.
#[allow(clippy::unwrap_used)]
static TYPES_DECIMAL: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    let names = vec![column_name!("path"), column_name!("stat")];
    let types = vec![
        DataType::STRING,
        DataType::Primitive(PrimitiveType::Decimal(DecimalType::try_new(38, 0).unwrap())),
    ];
    (names, types).into()
});

/// Select the predefined static type array matching the given data type.
fn types_for_data_type(dt: &DataType) -> DeltaResult<&'static ColumnNamesAndTypes> {
    match dt {
        &DataType::BOOLEAN => Ok(&TYPES_BOOL),
        &DataType::INTEGER => Ok(&TYPES_INT),
        &DataType::LONG => Ok(&TYPES_LONG),
        &DataType::STRING => Ok(&TYPES_STRING),
        &DataType::BINARY => Ok(&TYPES_BINARY),
        &DataType::FLOAT => Ok(&TYPES_FLOAT),
        &DataType::DOUBLE => Ok(&TYPES_DOUBLE),
        &DataType::DATE => Ok(&TYPES_DATE),
        &DataType::TIMESTAMP => Ok(&TYPES_TIMESTAMP),
        &DataType::TIMESTAMP_NTZ => Ok(&TYPES_TIMESTAMP_NTZ),
        DataType::Primitive(PrimitiveType::Decimal(_)) => Ok(&TYPES_DECIMAL),
        _ => Err(Error::internal_error(format!(
            "Unsupported data type for stats validation: {dt}"
        ))),
    }
}

/// Check if a stat value is present (non-null) using the appropriate typed getter.
fn is_stat_present<'b>(
    getter: &'b dyn GetData<'b>,
    row_idx: usize,
    stat_type: &DataType,
) -> DeltaResult<bool> {
    let field_name = "stat";
    match stat_type {
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
            "Unsupported data type for stats presence check: {stat_type}"
        ))),
    }
}

/// Visitor that checks whether a stat column value is present (non-null) in each row.
struct StatsPresenceVisitor<'a> {
    invalid_files: &'a mut Vec<String>,
    types: &'static ColumnNamesAndTypes,
    stat_type: &'a DataType,
}

impl<'a> StatsPresenceVisitor<'a> {
    fn new(
        invalid_files: &'a mut Vec<String>,
        types: &'static ColumnNamesAndTypes,
        stat_type: &'a DataType,
    ) -> Self {
        Self {
            invalid_files,
            types,
            stat_type,
        }
    }
}

impl RowVisitor for StatsPresenceVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.types.as_ref()
    }

    fn visit<'b>(&mut self, row_count: usize, getters: &[&'b dyn GetData<'b>]) -> DeltaResult<()> {
        require!(
            getters.len() == 2,
            Error::internal_error(format!(
                "Expected 2 getters for stats validation, got {}",
                getters.len()
            ))
        );

        for row_idx in 0..row_count {
            let path: String = getters[0].get(row_idx, "path")?;
            if !is_stat_present(getters[1], row_idx, self.stat_type)? {
                self.invalid_files.push(path);
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
    use crate::schema::StructField;
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
    fn test_verify_missing_null_count() {
        let batch = create_add_file_batch(
            vec!["file1.parquet"],
            vec![None],
            vec![Some(1)],
            vec![Some(100)],
        );

        let columns = vec![(ColumnName::new(["col"]), DataType::LONG)];
        let verifier = StatsVerifier::new(columns);
        let result = verifier.verify(&[batch]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("file1.parquet"));
        assert!(err_msg.contains("nullCount"));
    }

    #[test]
    fn test_verify_missing_min_values() {
        let batch = create_add_file_batch(
            vec!["file1.parquet"],
            vec![Some(0)],
            vec![None],
            vec![Some(100)],
        );

        let columns = vec![(ColumnName::new(["col"]), DataType::LONG)];
        let verifier = StatsVerifier::new(columns);
        let result = verifier.verify(&[batch]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("file1.parquet"));
        assert!(err_msg.contains("minValues"));
    }

    #[test]
    fn test_verify_missing_max_values() {
        let batch = create_add_file_batch(
            vec!["file1.parquet"],
            vec![Some(0)],
            vec![Some(1)],
            vec![None],
        );

        let columns = vec![(ColumnName::new(["col"]), DataType::LONG)];
        let verifier = StatsVerifier::new(columns);
        let result = verifier.verify(&[batch]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("file1.parquet"));
        assert!(err_msg.contains("maxValues"));
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

    #[test]
    fn test_resolve_column_type_simple() {
        let schema = StructType::try_new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap();
        assert_eq!(
            resolve_column_type(&schema, &ColumnName::new(["id"])).unwrap(),
            DataType::LONG
        );
        assert_eq!(
            resolve_column_type(&schema, &ColumnName::new(["name"])).unwrap(),
            DataType::STRING
        );
    }

    #[test]
    fn test_resolve_column_type_nested() {
        let inner =
            StructType::try_new([StructField::new("value", DataType::INTEGER, true)]).unwrap();
        let schema = StructType::try_new([StructField::new(
            "outer",
            DataType::Struct(Box::new(inner)),
            true,
        )])
        .unwrap();
        assert_eq!(
            resolve_column_type(&schema, &ColumnName::new(["outer", "value"])).unwrap(),
            DataType::INTEGER
        );
    }

    #[test]
    fn test_resolve_column_type_not_found() {
        let schema = StructType::try_new([StructField::new("id", DataType::LONG, false)]).unwrap();
        assert!(resolve_column_type(&schema, &ColumnName::new(["missing"])).is_err());
    }
}
