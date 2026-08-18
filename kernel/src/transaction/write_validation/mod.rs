//! Pre-commit validation of data staged on a [`Transaction`].
//!
//! [`Transaction`]: super::Transaction

mod addfile;
mod dv;
mod removefile;
mod utils;

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use crate::engine_data::{
    FilteredEngineData, FilteredRowVisitor, GetData, RowIndexIterator, RowVisitor,
};
use crate::expressions::ColumnName;
use crate::schema::{ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error};

#[derive(Default)]
pub(super) struct FileActionTracker {
    add_paths: HashMap<String, Option<String>>,
    remove_paths: HashMap<String, Option<String>>,
}

impl FileActionTracker {
    fn record_add(&mut self, path: &str, dv_id: Option<String>) -> DeltaResult<()> {
        let Entry::Vacant(entry) = self.add_paths.entry(path.to_owned()) else {
            return Err(Error::generic(format!(
                "Transaction contains multiple AddFile actions for path '{path}'"
            )));
        };
        require!(
            self.remove_paths.get(path) != Some(&dv_id),
            Error::generic(format!(
                "Transaction contains AddFile and RemoveFile actions for path '{path}' with the \
                 same deletion vector ID"
            ))
        );
        entry.insert(dv_id);
        Ok(())
    }

    fn record_remove(&mut self, path: &str, dv_id: Option<String>) -> DeltaResult<()> {
        let Entry::Vacant(entry) = self.remove_paths.entry(path.to_owned()) else {
            return Err(Error::generic(format!(
                "Transaction contains multiple RemoveFile actions for path '{path}'"
            )));
        };
        require!(
            self.add_paths.get(path) != Some(&dv_id),
            Error::generic(format!(
                "Transaction contains AddFile and RemoveFile actions for path '{path}' with the \
                 same deletion vector ID"
            ))
        );
        entry.insert(dv_id);
        Ok(())
    }
}

/// A single row-level validation.
pub(crate) trait Validation {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()>;
}

/// Runs validations over batches that share one staged-data schema.
///
/// Each instance uses one column projection and applies its configured validations to every staged
/// row. Every [`Validation`] sees the full getter list and reads the columns it needs.
pub(crate) struct StagedDataValidator<'a> {
    columns_and_types: &'static ColumnNamesAndTypes,
    validations: Vec<Box<dyn Validation + 'a>>,
}

impl<'a> StagedDataValidator<'a> {
    pub(crate) fn new(
        columns_and_types: &'static ColumnNamesAndTypes,
        validations: Vec<Box<dyn Validation + 'a>>,
    ) -> Self {
        Self {
            columns_and_types,
            validations,
        }
    }

    /// Run every validation against each batch. Returns the first validation error encountered.
    pub(crate) fn validate(mut self, batches: &[Box<dyn EngineData>]) -> DeltaResult<()> {
        for batch in batches {
            RowVisitor::visit_rows_of(&mut self, batch.as_ref())?;
        }
        Ok(())
    }

    /// Runs every validation against each selected staged-data row.
    pub(crate) fn validate_filtered(mut self, batches: &[FilteredEngineData]) -> DeltaResult<()> {
        for batch in batches {
            FilteredRowVisitor::visit_rows_of(&mut self, batch)?;
        }
        Ok(())
    }

    fn validate_rows<'data>(
        &mut self,
        rows: impl IntoIterator<Item = usize>,
        getters: &[&'data dyn GetData<'data>],
    ) -> DeltaResult<()> {
        for row in rows {
            for validation in &mut self.validations {
                validation.validate_row(row, getters)?;
            }
        }
        Ok(())
    }
}

impl RowVisitor for StagedDataValidator<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns_and_types.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        self.validate_rows(0..row_count, getters)
    }
}

impl FilteredRowVisitor for StagedDataValidator<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns_and_types.as_ref()
    }

    fn visit_filtered<'a>(
        &mut self,
        getters: &[&'a dyn GetData<'a>],
        rows: RowIndexIterator<'_>,
    ) -> DeltaResult<()> {
        self.validate_rows(rows, getters)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::unit_test_utils::assert_result_error_with_message;

    #[rstest]
    #[case::duplicate_add_different_dv(FileActionCase {
        first: TestFileAction::Add,
        first_path: "same",
        first_dv_id: Some("dv-1"),
        second: TestFileAction::Add,
        second_path: "same",
        second_dv_id: Some("dv-2"),
        expected_error: Some("multiple AddFile actions"),
    })]
    #[case::duplicate_remove_different_dv(FileActionCase {
        first: TestFileAction::Remove,
        first_path: "same",
        first_dv_id: Some("dv-1"),
        second: TestFileAction::Remove,
        second_path: "same",
        second_dv_id: Some("dv-2"),
        expected_error: Some("multiple RemoveFile actions"),
    })]
    #[case::add_remove_same_dv(FileActionCase {
        first: TestFileAction::Add,
        first_path: "same",
        first_dv_id: Some("dv"),
        second: TestFileAction::Remove,
        second_path: "same",
        second_dv_id: Some("dv"),
        expected_error: Some("same deletion vector ID"),
    })]
    #[case::remove_add_same_dv(FileActionCase {
        first: TestFileAction::Remove,
        first_path: "same",
        first_dv_id: None,
        second: TestFileAction::Add,
        second_path: "same",
        second_dv_id: None,
        expected_error: Some("same deletion vector ID"),
    })]
    #[case::add_remove_different_dv(FileActionCase {
        first: TestFileAction::Add,
        first_path: "same",
        first_dv_id: Some("dv-1"),
        second: TestFileAction::Remove,
        second_path: "same",
        second_dv_id: Some("dv-2"),
        expected_error: None,
    })]
    #[case::same_dv_different_paths(FileActionCase {
        first: TestFileAction::Add,
        first_path: "first",
        first_dv_id: Some("dv"),
        second: TestFileAction::Add,
        second_path: "second",
        second_dv_id: Some("dv"),
        expected_error: None,
    })]
    fn file_action_combinations_accepted_or_rejected(#[case] case: FileActionCase) {
        let mut tracker = FileActionTracker::default();
        case.first
            .record(
                &mut tracker,
                case.first_path,
                case.first_dv_id.map(str::to_owned),
            )
            .expect("first file action should be accepted");
        let result = case.second.record(
            &mut tracker,
            case.second_path,
            case.second_dv_id.map(str::to_owned),
        );

        if let Some(expected_error) = case.expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.expect("valid file-action combination should be accepted");
        }
    }

    #[derive(Clone, Copy)]
    enum TestFileAction {
        Add,
        Remove,
    }

    impl TestFileAction {
        fn record(
            self,
            tracker: &mut FileActionTracker,
            path: &str,
            dv_id: Option<String>,
        ) -> DeltaResult<()> {
            match self {
                Self::Add => tracker.record_add(path, dv_id),
                Self::Remove => tracker.record_remove(path, dv_id),
            }
        }
    }

    struct FileActionCase {
        first: TestFileAction,
        first_path: &'static str,
        first_dv_id: Option<&'static str>,
        second: TestFileAction,
        second_path: &'static str,
        second_dv_id: Option<&'static str>,
        expected_error: Option<&'static str>,
    }
}
