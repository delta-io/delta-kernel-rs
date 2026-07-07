//! Pre-commit validation of delta actions staged on a [`Transaction`].
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

/// Runs a set of [`Validation`]s over staged action batches in a single `visit_rows` pass.
///
/// `columns_types` is the shared column set for all validations; every [`Validation`] sees
/// the full getter list and reads the columns it needs.
// TODO(#2869): Add the remaining write-side validations:
// - No missing partition columns in `txn.add_files_metadata`
// - Required fields for `txn.remove_files_metadata`
// - Required fields for `txn.dv_matched_files`
// - No missing partition columns in `txn.dv_matched_files`
// - No duplicate (path, DvId) in `txn.add_files_metadata`, `txn.remove_files_metadata`,
//   `txn.dv_matched_files`
// - AppendOnly table can not have `removeFile` with dataChange = true
pub(crate) struct ActionValidator {
    columns_types: &'static ColumnNamesAndTypes,
    validations: Vec<Box<dyn Validation>>,
}

impl ActionValidator {
    pub(crate) fn new(
        columns_types: &'static ColumnNamesAndTypes,
        validations: Vec<Box<dyn Validation>>,
    ) -> Self {
        Self {
            columns_types,
            validations,
        }
    }

    pub(crate) fn validate(mut self, batches: &[Box<dyn EngineData>]) -> DeltaResult<()> {
        for batch in batches {
            self.visit_rows_of(batch.as_ref())?;
        }
        Ok(())
    }
}

impl RowVisitor for ActionValidator {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns_types.as_ref()
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
