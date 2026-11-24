use delta_kernel_derive::{ToSchema, TryFromStructData};

use super::{StagedDataCheck, invalid_summary};
use crate::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT};
use crate::expressions::{ColumnName, Expression, Predicate, col, lit};
use crate::schema::DataType;
use crate::transaction::stats_verifier::{self, column_types_for};
use crate::{KernelError, KernelResult};

// TODO: Return structured DeltaError conditions for validation failures instead of generic
// KernelError::InvalidTransactionState messages.

/// Requires every staged add to have a nonempty path, a non-negative size, and a modification time.
/// The materialized error contains `path`, `size`, and `modification_time` from a failing row,
/// including nulls for missing fields.
pub(in crate::transaction::declarative) struct AddFileFieldsCheck;

/// The file fields retained when an add fails validation.
#[derive(ToSchema, TryFromStructData)]
pub(in crate::transaction::declarative) struct AddFileFieldsError {
    path: Option<String>,
    size: Option<i64>,
    modification_time: Option<i64>,
}

impl StagedDataCheck for AddFileFieldsCheck {
    type ErrorInfo = AddFileFieldsError;

    const NAME: &'static str = "add_fields";

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

    fn build_error(&self, sample: AddFileFieldsError) -> KernelResult<KernelError> {
        let context = file_context(sample.path.as_deref());
        let Some(path) = sample.path.as_deref() else {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} is missing required field 'path'"
            )));
        };
        if path.is_empty() {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} has an empty path"
            )));
        }
        let Some(size) = sample.size else {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} is missing required field 'size'"
            )));
        };
        if size < 0 {
            return Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {context} has negative size {size}; size must be non-negative"
            )));
        }
        if sample.modification_time.is_none() {
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

/// The file fields retained when a remove fails validation.
#[derive(ToSchema, TryFromStructData)]
pub(in crate::transaction::declarative) struct RemoveFileFieldsError {
    path: Option<String>,
    size: Option<i64>,
}

impl StagedDataCheck for RemoveFileFieldsCheck {
    type ErrorInfo = RemoveFileFieldsError;

    const NAME: &'static str = "remove_fields";

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

    fn build_error(&self, sample: RemoveFileFieldsError) -> KernelResult<KernelError> {
        let context = file_context(sample.path.as_deref());
        let Some(path) = sample.path.as_deref() else {
            return Ok(KernelError::invalid_transaction_state(format!(
                "RemoveFile {context} is missing required field 'path'"
            )));
        };
        if path.is_empty() {
            return Ok(KernelError::invalid_transaction_state(format!(
                "RemoveFile {context} has an empty path"
            )));
        }
        let Some(size) = sample.size else {
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

/// The removed file rejected by an append-only check.
#[derive(ToSchema, TryFromStructData)]
pub(in crate::transaction::declarative) struct AppendOnlyError {
    path: Option<String>,
}

impl StagedDataCheck for AppendOnlyCheck {
    type ErrorInfo = AppendOnlyError;

    const NAME: &'static str = "append_only_no_removes";

    fn project_error(&self) -> Expression {
        Expression::struct_with_nullability_from(
            [col!("remove.path")],
            Expression::from(Predicate::is_not_null(col!("remove"))),
        )
    }

    fn build_error(&self, sample: AppendOnlyError) -> KernelResult<KernelError> {
        Ok(KernelError::invalid_transaction_state(format!(
            "Append-only table cannot remove file {} when data_change is true",
            file_context(sample.path.as_deref())
        )))
    }
}

/// Requires every staged add to have a non-null, non-negative `stats.numRecords` value.
/// The materialized error contains the add `path` and the invalid `num_records` value.
pub(in crate::transaction::declarative) struct NumRecordsCheck;

/// The file and record count retained when record-count validation fails.
#[derive(ToSchema, TryFromStructData)]
pub(in crate::transaction::declarative) struct NumRecordsError {
    path: Option<String>,
    num_records: Option<i64>,
}

impl StagedDataCheck for NumRecordsCheck {
    type ErrorInfo = NumRecordsError;

    const NAME: &'static str = "add_num_records_present";

    fn project_error(&self) -> Expression {
        let is_failed = Predicate::and(
            Predicate::is_not_null(col!("add")),
            Predicate::or(
                Predicate::is_null(col!("add.stats.numRecords")),
                Predicate::lt(col!("add.stats.numRecords"), lit(0i64)),
            ),
        );
        Expression::struct_with_nullability_from(
            [col!("add.path"), col!("add.stats.numRecords")],
            Expression::from(is_failed),
        )
    }

    fn build_error(&self, sample: NumRecordsError) -> KernelResult<KernelError> {
        match sample.num_records {
            None => Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {} is missing required statistics 'numRecords'",
                file_context(sample.path.as_deref())
            ))),
            Some(count) if count < 0 => Ok(KernelError::invalid_transaction_state(format!(
                "AddFile {} has negative numRecords {count}; numRecords must be non-negative",
                file_context(sample.path.as_deref())
            ))),
            Some(_) => Err(invalid_summary("Record count is non-negative")),
        }
    }
}

/// Requires `nullCount` for every physical clustering column on each staged add. When `numRecords`
/// is missing or differs from `nullCount`, it also requires both `minValues` and `maxValues`.
/// The error retains one failing file/column pair: `path`, `num_records`, the physical `column`
/// name, `null_count`, and `has_min` and `has_max` Booleans. It does not retain the bound values.
pub(in crate::transaction::declarative) struct ClusteringStatsCheck {
    columns: Vec<ColumnName>,
}

/// The failing file and physical column, with counts and bound-presence flags.
#[derive(ToSchema, TryFromStructData)]
pub(in crate::transaction::declarative) struct ClusteringStatsError {
    path: Option<String>,
    num_records: Option<i64>,
    column: String,
    null_count: Option<i64>,
    has_min: bool,
    has_max: bool,
}

impl ClusteringStatsCheck {
    /// Creates a check for physical columns and their types, rejecting unsupported stats types.
    pub(in crate::transaction::declarative) fn new(
        columns: impl IntoIterator<Item = (ColumnName, DataType)>,
    ) -> KernelResult<Self> {
        let columns = columns
            .into_iter()
            .map(|(column, data_type)| {
                column_types_for(&data_type)?;
                Ok(column)
            })
            .collect::<KernelResult<Vec<_>>>()?;
        if columns.is_empty() {
            return Err(KernelError::internal_error(
                "Clustering statistics validation requires columns",
            ));
        }
        Ok(Self { columns })
    }

    fn project_column_error(column: &ColumnName) -> Expression {
        let stat = |category| {
            let path = stats_verifier::build_stat_path(column, category);
            Expression::column(ColumnName::new(
                std::iter::once("add").chain(path.iter().map(String::as_str)),
            ))
        };
        // A missing record count requires both bounds, just like unequal non-null counts.
        // Explicit null tests make the failure guard Boolean even when comparison yields NULL.
        let needs_bounds = Predicate::or(
            Predicate::is_null(col!("add.stats.numRecords")),
            Predicate::ne(col!("add.stats.numRecords"), stat(NULL_COUNT)),
        );
        let missing_bounds = Predicate::or(
            Predicate::is_null(stat(MIN_VALUES)),
            Predicate::is_null(stat(MAX_VALUES)),
        );
        let is_failed = Predicate::and(
            Predicate::is_not_null(col!("add")),
            Predicate::or(
                Predicate::is_null(stat(NULL_COUNT)),
                Predicate::and(needs_bounds, missing_bounds),
            ),
        );
        Expression::struct_with_nullability_from(
            [
                col!("add.path"),
                col!("add.stats.numRecords"),
                lit(column.to_string()),
                stat(NULL_COUNT),
                Predicate::is_not_null(stat(MIN_VALUES)).into(),
                Predicate::is_not_null(stat(MAX_VALUES)).into(),
            ],
            Expression::from(is_failed),
        )
    }
}

impl StagedDataCheck for ClusteringStatsCheck {
    type ErrorInfo = ClusteringStatsError;

    const NAME: &'static str = "add_clustering_stats";

    fn project_error(&self) -> Expression {
        // All columns share one fixed error shape, with the failing column carried as data.
        Expression::coalesce(self.columns.iter().map(Self::project_column_error))
    }

    fn build_error(&self, sample: ClusteringStatsError) -> KernelResult<KernelError> {
        let nr = sample.num_records;
        let nc = sample.null_count;
        let column = sample.column;
        let mut missing = Vec::new();
        if nc.is_none() {
            missing.push("nullCount");
        }
        let needs_bounds = nr.is_none() || nc.is_none() || nr != nc;
        if needs_bounds && !sample.has_min {
            missing.push("minValues");
        }
        if needs_bounds && !sample.has_max {
            missing.push("maxValues");
        }
        if missing.is_empty() {
            return Err(invalid_summary(
                "Clustering sample has all required statistics",
            ));
        }
        Ok(KernelError::invalid_transaction_state(format!(
            "AddFile {} is missing statistics [{}] for physical column '{}'",
            file_context(sample.path.as_deref()),
            missing.join(", "),
            column,
        )))
    }
}

fn file_context(path: Option<&str>) -> String {
    path.map(|p| format!("for '{p}'"))
        .unwrap_or_else(|| "with missing path".to_owned())
}
