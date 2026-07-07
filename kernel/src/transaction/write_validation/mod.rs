//! Pre-commit validation of data staged on a [`Transaction`].
//!
//! [`Transaction`]: super::Transaction

mod addfile;

use crate::engine_data::{GetData, RowVisitor};
use crate::expressions::ColumnName;
use crate::schema::{ColumnNamesAndTypes, DataType};
use crate::{DeltaResult, EngineData};

/// A single row-level validation.
pub(crate) trait Validation {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()>;
}

/// Runs a set of [`Validation`]s over staged-data batches. One [`StagedDataValidator`] per
/// staged-data schema, i.e. one each for the `Transaction`'s `add_files_metadata`,
/// `remove_files_metadata`, and `dv_matched_files`.
///
/// `columns_and_types` is the shared set of column names and types for one staged-data schema;
/// every [`Validation`] sees the full getter list and reads the columns it needs.
// TODO(#2869): Add the remaining write-side validations:
// - No missing partition columns in `txn.add_files_metadata`
// - Required fields for `txn.remove_files_metadata`
// - Required fields for `txn.dv_matched_files`
// - No missing partition columns in `txn.dv_matched_files`
// - No duplicate (path, DvId) in `txn.add_files_metadata`, `txn.remove_files_metadata`,
//   `txn.dv_matched_files`
// - AppendOnly table can not have `removeFile` with dataChange = true
pub(crate) struct StagedDataValidator {
    columns_and_types: &'static ColumnNamesAndTypes,
    validations: Vec<Box<dyn Validation>>,
}

impl StagedDataValidator {
    pub(crate) fn new(
        columns_and_types: &'static ColumnNamesAndTypes,
        validations: Vec<Box<dyn Validation>>,
    ) -> Self {
        Self {
            columns_and_types,
            validations,
        }
    }

    /// Run every validation against each batch. Returns the first validation error encountered.
    pub(crate) fn validate(mut self, batches: &[Box<dyn EngineData>]) -> DeltaResult<()> {
        for batch in batches {
            self.visit_rows_of(batch.as_ref())?;
        }
        Ok(())
    }
}

impl RowVisitor for StagedDataValidator {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns_and_types.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for row in 0..row_count {
            for validation in &mut self.validations {
                validation.validate_row(row, getters)?;
            }
        }
        Ok(())
    }
}
