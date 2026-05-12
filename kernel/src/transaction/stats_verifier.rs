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
use crate::scan::data_skipping::stats_schema::STATS_NUM_RECORDS;
use crate::schema::{ColumnNamesAndTypes, DataType, DecimalType, PrimitiveType};
use crate::utils::require;
use crate::DeltaResult;

/// Verifies that add file statistics contain required columns.
///
/// For each required column, validates that `nullCount` is present (non-null) and that
/// `minValues` and `maxValues` are present unless the column is all-null
/// (`nullCount == numRecords`).
pub(crate) struct StatsColumnVerifier {
    required_columns: Vec<(ColumnName, DataType)>,
}

impl StatsColumnVerifier {
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
            let mut visitor = ColumnStatsValidator {
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

/// [`ColumnNamesAndTypes`] for [`NumRecordsValidator`].
static NUM_RECORDS_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    let names = vec![column_name!("path"), column_name!("stats.numRecords")];
    let types = vec![DataType::STRING, DataType::LONG];
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
        DataType::Struct(_) | DataType::Array(_) | DataType::Map(_) | DataType::Variant(_) => Err(
            Error::internal_error(format!("Unsupported data type for stats validation: {dt}")),
        ),
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
        DataType::Struct(_) | DataType::Array(_) | DataType::Map(_) | DataType::Variant(_) => {
            Err(Error::internal_error(format!(
                "Unsupported data type for stats presence check: {data_type}"
            )))
        }
    }
}

/// Visitor that checks nullCount, minValues, and maxValues for a single column in one pass.
/// Expects 5 getters: [path, numRecords, nullCount, minValues, maxValues].
struct ColumnStatsValidator<'a> {
    data_type: &'a DataType,
    types: &'static ColumnNamesAndTypes,
    missing_null_count: &'a mut Vec<String>,
    missing_min: &'a mut Vec<String>,
    missing_max: &'a mut Vec<String>,
}

impl RowVisitor for ColumnStatsValidator<'_> {
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

/// Verify that every `add` action has `stats.numRecords` populated. Short-circuits on the first
/// violation and returns an error containing the `add.path`.
pub(crate) fn verify_num_records_present(
    add_files: &[Box<dyn crate::EngineData>],
) -> DeltaResult<()> {
    let column_names = vec![
        ColumnName::new(["path"]),
        ColumnName::new(["stats", STATS_NUM_RECORDS]),
    ];
    let mut first_missing: Option<String> = None;
    for batch in add_files {
        let mut visitor = NumRecordsValidator {
            first_missing: &mut first_missing,
        };
        batch.visit_rows(&column_names, &mut visitor)?;
        if first_missing.is_some() {
            break;
        }
    }
    if let Some(path) = first_missing {
        return Err(Error::stats_validation(format!(
            "'stats.numRecords' is required for this table (see \
             `TableConfiguration::requires_stats_num_records`), but is missing for file '{path}'",
        )));
    }
    Ok(())
}

/// Visitor validates that every `add` action has `stats.numRecords` populated.
/// Stops at the first row whose `numRecords` is null and records that row's `add.path`.
struct NumRecordsValidator<'a> {
    first_missing: &'a mut Option<String>,
}

impl RowVisitor for NumRecordsValidator<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        NUM_RECORDS_TYPES.as_ref()
    }

    fn visit<'b>(&mut self, row_count: usize, getters: &[&'b dyn GetData<'b>]) -> DeltaResult<()> {
        require!(
            getters.len() == 2,
            Error::internal_error(format!(
                "Expected 2 getters for numRecords validation, got {}",
                getters.len()
            ))
        );
        for row_idx in 0..row_count {
            if getters[1].get_long(row_idx, STATS_NUM_RECORDS)?.is_none() {
                *self.first_missing = Some(getters[0].get(row_idx, "path")?);
                return Ok(());
            }
        }
        Ok(())
    }
}
