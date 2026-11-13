use super::{StagedDataCheck, invalid_summary};
use crate::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS};
use crate::expressions::{
    ColumnName, Expression, Predicate, Scalar, StructData, col, column_name, lit,
};
use crate::schema::{DataType, StructType, schema};
use crate::transaction::stats_verifier::{self, column_types_for};
use crate::{KernelError, KernelResult};

// TODO: Return structured DeltaError conditions for validation failures instead of generic
// KernelError::InvalidTransactionState messages.

/// Requires every staged add to have a nonempty path, a non-negative size, and a modification time.
/// The materialized error contains `path`, `size`, and `modification_time` from a failing row,
/// including nulls for missing fields.
pub(in crate::transaction::declarative) struct AddFileFieldsCheck;

impl StagedDataCheck for AddFileFieldsCheck {
    fn name(&self) -> &str {
        "add.fields"
    }

    fn input_cols(&self) -> Vec<(ColumnName, DataType)> {
        vec![
            (column_name!("add.path"), DataType::STRING),
            (column_name!("add.size"), DataType::LONG),
            (column_name!("add.modificationTime"), DataType::LONG),
        ]
    }

    fn output_schema(&self) -> StructType {
        schema! {
            nullable "path": STRING,
            nullable "size": LONG, nullable "modification_time": LONG,
        }
    }

    fn project_error(&self) -> Expression {
        let is_failed = Predicate::and(
            Predicate::is_not_null(col!("add")),
            Predicate::or_from([
                Predicate::is_null(col!("add.path")),
                Predicate::eq(col!("add.path"), lit("")),
                Predicate::is_null(col!("add.size")),
                Predicate::lt(col!("add.size"), lit(0i64)),
                Predicate::is_null(col!("add.modificationTime")),
            ]),
        );
        Expression::struct_with_nullability_from(
            [
                col!("add.path"),
                col!("add.size"),
                col!("add.modificationTime"),
            ],
            Expression::from(is_failed),
        )
    }

    fn build_error(&self, sample: &StructData) -> KernelResult<KernelError> {
        let context = file_context(sample)?;
        let Some(path) = extract_string(sample, "path")? else {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} is missing required field 'path'"
            )));
        };
        if path.is_empty() {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} has an empty path"
            )));
        }
        let Some(size) = extract_long(sample, "size")? else {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} is missing required field 'size'"
            )));
        };
        if size < 0 {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} has negative size {size}; size must be non-negative"
            )));
        }
        if extract_long(sample, "modification_time")?.is_none() {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} is missing required field 'modificationTime'"
            )));
        }
        Err(invalid_summary("Field sample satisfies its check"))
    }
}

/// Requires every staged remove to have a nonempty path and a non-negative size.
/// The materialized error contains `path` and `size` from a failing row, including nulls for
/// missing fields.
pub(in crate::transaction::declarative) struct RemoveFileFieldsCheck;

impl StagedDataCheck for RemoveFileFieldsCheck {
    fn name(&self) -> &str {
        "remove.fields"
    }

    fn input_cols(&self) -> Vec<(ColumnName, DataType)> {
        vec![
            (column_name!("remove.path"), DataType::STRING),
            (column_name!("remove.size"), DataType::LONG),
        ]
    }

    fn output_schema(&self) -> StructType {
        schema! { nullable "path": STRING, nullable "size": LONG }
    }

    fn project_error(&self) -> Expression {
        let is_failed = Predicate::and(
            Predicate::is_not_null(col!("remove")),
            Predicate::or_from([
                Predicate::is_null(col!("remove.path")),
                Predicate::eq(col!("remove.path"), lit("")),
                Predicate::is_null(col!("remove.size")),
                Predicate::lt(col!("remove.size"), lit(0i64)),
            ]),
        );
        Expression::struct_with_nullability_from(
            [col!("remove.path"), col!("remove.size")],
            Expression::from(is_failed),
        )
    }

    fn build_error(&self, sample: &StructData) -> KernelResult<KernelError> {
        let context = file_context(sample)?;
        let Some(path) = extract_string(sample, "path")? else {
            return Ok(KernelError::invalid_transaction_state(format!(
                "RemoveFile {context} is missing required field 'path'"
            )));
        };
        if path.is_empty() {
            return Ok(KernelError::invalid_transaction_state(format!(
                "RemoveFile {context} has an empty path"
            )));
        }
        let Some(size) = extract_long(sample, "size")? else {
            return Ok(KernelError::invalid_transaction_state(format!(
                "RemoveFile {context} is missing required field 'size'"
            )));
        };
        if size < 0 {
            return Ok(KernelError::invalid_transaction_state(format!(
                "RemoveFile {context} has negative size {size}; size must be non-negative"
            )));
        }
        Err(invalid_summary("Field sample satisfies its check"))
    }
}

/// Rejects every staged remove when selected for an append-only, data-changing transaction.
/// The materialized error contains the remove `path`, which may be null.
pub(in crate::transaction::declarative) struct AppendOnlyCheck;

impl StagedDataCheck for AppendOnlyCheck {
    fn name(&self) -> &str {
        "append_only.no_removes"
    }

    fn input_cols(&self) -> Vec<(ColumnName, DataType)> {
        vec![(column_name!("remove.path"), DataType::STRING)]
    }

    fn output_schema(&self) -> StructType {
        schema! { nullable "path": STRING }
    }

    fn project_error(&self) -> Expression {
        Expression::struct_with_nullability_from(
            [col!("remove.path")],
            Expression::from(Predicate::is_not_null(col!("remove"))),
        )
    }

    fn build_error(&self, sample: &StructData) -> KernelResult<KernelError> {
        Ok(KernelError::invalid_transaction_state(format!(
            "Append-only table cannot remove file {} when data_change is true",
            file_context(sample)?
        )))
    }
}

/// Requires every staged add to have a non-null `stats.numRecords` value.
/// The materialized error contains the add `path` and `num_records`, which is null for failures.
/// This check does not constrain the count's value.
pub(in crate::transaction::declarative) struct NumRecordsCheck;

impl StagedDataCheck for NumRecordsCheck {
    fn name(&self) -> &str {
        "add.num_records_present"
    }

    fn input_cols(&self) -> Vec<(ColumnName, DataType)> {
        vec![
            (column_name!("add.path"), DataType::STRING),
            (column_name!("add", "stats", NUM_RECORDS), DataType::LONG),
        ]
    }

    fn output_schema(&self) -> StructType {
        schema! { nullable "path": STRING, nullable "num_records": LONG }
    }

    fn project_error(&self) -> Expression {
        let is_failed = Predicate::and(
            Predicate::is_not_null(col!("add")),
            Predicate::is_null(col!("add.stats.numRecords")),
        );
        Expression::struct_with_nullability_from(
            [col!("add.path"), col!("add.stats.numRecords")],
            Expression::from(is_failed),
        )
    }

    fn build_error(&self, sample: &StructData) -> KernelResult<KernelError> {
        if extract_long(sample, "num_records")?.is_some() {
            return Err(invalid_summary("Record count is present"));
        }
        Ok(KernelError::invalid_transaction_state(format!(
            "AddFile {} is missing required statistics 'numRecords'",
            file_context(sample)?
        )))
    }
}

/// Requires `nullCount` for one physical clustering column on every staged add. When `numRecords`
/// is missing or differs from `nullCount`, it also requires both `minValues` and `maxValues`.
/// The materialized error contains `path`, `num_records`, `null_count`, and `has_min` and `has_max`
/// Booleans indicating whether each bound is present. It does not retain the bound values.
pub(in crate::transaction::declarative) struct ClusteringStatsCheck {
    name: String,
    column: ColumnName,
    data_type: DataType,
}

impl ClusteringStatsCheck {
    /// Creates a check for physical `column` with `data_type`, rejecting unsupported stats types.
    pub(in crate::transaction::declarative) fn new(
        column: ColumnName,
        data_type: DataType,
    ) -> KernelResult<Self> {
        column_types_for(&data_type)?;
        let segments: Vec<_> = column.iter().collect();
        Ok(Self {
            name: format!("add.clustering_stats:{}", serde_json::to_string(&segments)?),
            column,
            data_type,
        })
    }

    fn stat_path(&self, category: &str) -> ColumnName {
        let path = stats_verifier::build_stat_path(&self.column, category);
        ColumnName::new(std::iter::once("add").chain(path.iter().map(String::as_str)))
    }

    fn stat(&self, category: &str) -> Expression {
        Expression::column(self.stat_path(category))
    }
}

impl StagedDataCheck for ClusteringStatsCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_cols(&self) -> Vec<(ColumnName, DataType)> {
        vec![
            (column_name!("add.path"), DataType::STRING),
            (column_name!("add", "stats", NUM_RECORDS), DataType::LONG),
            (self.stat_path(NULL_COUNT), DataType::LONG),
            (self.stat_path(MIN_VALUES), self.data_type.clone()),
            (self.stat_path(MAX_VALUES), self.data_type.clone()),
        ]
    }

    fn output_schema(&self) -> StructType {
        schema! {
            nullable "path": STRING,
            nullable "num_records": LONG, nullable "null_count": LONG,
            not_null "has_min": BOOLEAN, not_null "has_max": BOOLEAN,
        }
    }

    fn project_error(&self) -> Expression {
        // A missing record count requires both bounds, just like unequal non-null counts.
        // Explicit null tests make the failure guard Boolean even when comparison yields NULL.
        let needs_bounds = Predicate::or(
            Predicate::is_null(col!("add.stats.numRecords")),
            Predicate::ne(col!("add.stats.numRecords"), self.stat(NULL_COUNT)),
        );
        let missing_bounds = Predicate::or(
            Predicate::is_null(self.stat(MIN_VALUES)),
            Predicate::is_null(self.stat(MAX_VALUES)),
        );
        let is_failed = Predicate::and(
            Predicate::is_not_null(col!("add")),
            Predicate::or(
                Predicate::is_null(self.stat(NULL_COUNT)),
                Predicate::and(needs_bounds, missing_bounds),
            ),
        );
        Expression::struct_with_nullability_from(
            [
                col!("add.path"),
                col!("add.stats.numRecords"),
                self.stat(NULL_COUNT),
                Predicate::is_not_null(self.stat(MIN_VALUES)).into(),
                Predicate::is_not_null(self.stat(MAX_VALUES)).into(),
            ],
            Expression::from(is_failed),
        )
    }

    fn build_error(&self, sample: &StructData) -> KernelResult<KernelError> {
        let nr = extract_long(sample, "num_records")?;
        let nc = extract_long(sample, "null_count")?;
        let mut missing = Vec::new();
        if nc.is_none() {
            missing.push("nullCount");
        }
        let needs_bounds = nr.is_none() || nc.is_none() || nr != nc;
        if needs_bounds && !extract_boolean(sample, "has_min")? {
            missing.push("minValues");
        }
        if needs_bounds && !extract_boolean(sample, "has_max")? {
            missing.push("maxValues");
        }
        if missing.is_empty() {
            return Err(invalid_summary(
                "Clustering sample has all required statistics",
            ));
        }
        Ok(KernelError::invalid_transaction_state(format!(
            "AddFile {} is missing statistics [{}] for physical column '{}'",
            file_context(sample)?,
            missing.join(", "),
            self.column,
        )))
    }
}

fn file_context(sample: &StructData) -> KernelResult<String> {
    Ok(extract_string(sample, "path")?
        .map(|p| format!("for '{p}'"))
        .unwrap_or_else(|| "with missing path".to_owned()))
}

fn extract_field<'a>(sample: &'a StructData, name: &str) -> KernelResult<&'a Scalar> {
    sample
        .fields()
        .iter()
        .zip(sample.values())
        .find(|(f, _)| f.name() == name)
        .map(|(_, value)| value)
        .ok_or_else(|| invalid_summary(format!("Missing sample field '{name}'")))
}

fn extract_string<'a>(sample: &'a StructData, name: &str) -> KernelResult<Option<&'a str>> {
    match extract_field(sample, name)? {
        Scalar::String(value) => Ok(Some(value)),
        Scalar::Null(DataType::STRING) => Ok(None),
        _ => Err(invalid_summary(format!(
            "Expected STRING sample field '{name}'"
        ))),
    }
}

fn extract_long(sample: &StructData, name: &str) -> KernelResult<Option<i64>> {
    match extract_field(sample, name)? {
        Scalar::Long(value) => Ok(Some(*value)),
        Scalar::Null(DataType::LONG) => Ok(None),
        _ => Err(invalid_summary(format!(
            "Expected LONG sample field '{name}'"
        ))),
    }
}

fn extract_boolean(sample: &StructData, name: &str) -> KernelResult<bool> {
    match extract_field(sample, name)? {
        Scalar::Boolean(value) => Ok(*value),
        _ => Err(invalid_summary(format!(
            "Expected BOOLEAN sample field '{name}'"
        ))),
    }
}
