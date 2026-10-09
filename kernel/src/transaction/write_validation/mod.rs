//! Pre-commit validation of data staged on a [`Transaction`].
//!
//! [`Transaction`]: super::Transaction

mod addfile;
mod dv;
mod removefile;
mod utils;

use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub(super) use addfile::validate_add_files;
use derive_more::Constructor;
pub(super) use dv::validate_dv_matched_files;
pub(super) use removefile::validate_remove_files;

use crate::engine_data::{
    FilteredEngineData, FilteredRowVisitor, GetData, RowIndexIterator, RowVisitor,
};
use crate::expressions::ColumnName;
use crate::schema::{ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{EngineData, KernelError, KernelResult, Result};

/// Tracks staged file actions across a transaction.
///
/// - AddFile paths must be unique, regardless of deletion vector ID.
/// - RemoveFile paths must be unique, regardless of deletion vector ID.
/// - An AddFile and RemoveFile cannot share the same `(path, dv_id)`, including when both IDs are
///   absent. Different DV IDs on the same path are allowed for deletion-vector updates.
///
/// Note that [`FileActionDeduplicator`](crate::log_replay::FileActionDeduplicator) uses different
/// rules: it deduplicates `(path, dv_id)` pairs during log replay.
#[derive(Default)]
pub(super) struct FileActionTracker {
    add_paths: HashMap<String, Option<String>>,
    remove_paths: HashMap<String, Option<String>>,
}

impl FileActionTracker {
    fn record_add(&mut self, path: &str, dv_id: Option<String>) -> KernelResult<()> {
        Self::record(
            path,
            dv_id,
            "AddFile",
            &mut self.add_paths,
            &self.remove_paths,
        )
    }

    fn record_remove(&mut self, path: &str, dv_id: Option<String>) -> KernelResult<()> {
        Self::record(
            path,
            dv_id,
            "RemoveFile",
            &mut self.remove_paths,
            &self.add_paths,
        )
    }

    fn record(
        path: &str,
        dv_id: Option<String>,
        action_name: &str,
        same_type_actions: &mut HashMap<String, Option<String>>,
        different_type_actions: &HashMap<String, Option<String>>,
    ) -> KernelResult<()> {
        let Entry::Vacant(entry) = same_type_actions.entry(path.to_owned()) else {
            return Err(KernelError::invalid_transaction_state(format!(
                "Transaction contains multiple {action_name} actions for path '{path}'"
            )));
        };
        require!(
            different_type_actions.get(path) != Some(&dv_id),
            KernelError::invalid_transaction_state(match dv_id.as_deref() {
                Some(dv_id) => format!(
                    "Transaction contains AddFile and RemoveFile actions for path '{path}' with the \
                     same deletion vector ID '{dv_id}'"
                ),
                None => format!(
                    "Transaction contains AddFile and RemoveFile actions for path '{path}' \
                     without a deletion vector"
                ),
            })
        );
        entry.insert(dv_id);
        Ok(())
    }
}

/// A single row-level validation.
pub(crate) trait Validation {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>])
        -> KernelResult<()>;
}

/// Runs validations over batches that share one staged-data schema.
///
/// Each instance uses one column projection and applies its configured validations to every staged
/// row. Every [`Validation`] sees the full getter list and reads the columns it needs.
/// The `'a` lifetime lets validations borrow shared state, such as [`FileActionTracker`], so all
/// the checks can be run in one pass.
#[derive(Constructor)]
struct StagedDataValidator<'a> {
    columns_and_types: &'static ColumnNamesAndTypes,
    validations: Vec<Box<dyn Validation + 'a>>,
}

impl<'a> StagedDataValidator<'a> {
    /// Run every validation against each batch. Returns the first validation error encountered.
    fn validate(mut self, batches: &[Box<dyn EngineData>]) -> KernelResult<()> {
        for batch in batches {
            RowVisitor::visit_rows_of(&mut self, batch.as_ref())?;
        }
        Ok(())
    }

    /// Runs every validation against each selected staged-data row.
    fn validate_filtered(mut self, batches: &[FilteredEngineData]) -> KernelResult<()> {
        for batch in batches {
            FilteredRowVisitor::visit_rows_of(&mut self, batch)?;
        }
        Ok(())
    }

    fn validate_rows<'data>(
        &mut self,
        rows: impl IntoIterator<Item = usize>,
        getters: &[&'data dyn GetData<'data>],
    ) -> KernelResult<()> {
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

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> Result<()> {
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
    ) -> Result<()> {
        self.validate_rows(rows, getters)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::unit_test_utils::assert_result_error_with_message;

    #[rstest]
    #[case::duplicate_add_different_dv(
        &[
            FileActionTrackerTestCase::new(TestFileActionType::Add, "same_path", Some("dv-1")),
            FileActionTrackerTestCase::new(TestFileActionType::Add, "same_path", Some("dv-2")),
        ],
        Some("multiple AddFile actions"),
    )]
    #[case::duplicate_remove_different_dv(
        &[
            FileActionTrackerTestCase::new(TestFileActionType::Remove, "same_path", Some("dv-1")),
            FileActionTrackerTestCase::new(TestFileActionType::Remove, "same_path", Some("dv-2")),
        ],
        Some("multiple RemoveFile actions"),
    )]
    #[case::add_remove_same_dv(
        &[
            FileActionTrackerTestCase::new(TestFileActionType::Add, "same_path", Some("dv")),
            FileActionTrackerTestCase::new(TestFileActionType::Remove, "same_path", Some("dv")),
        ],
        Some("same deletion vector ID"),
    )]
    #[case::remove_add_same_dv(
        &[
            FileActionTrackerTestCase::new(TestFileActionType::Remove, "same_path", None),
            FileActionTrackerTestCase::new(TestFileActionType::Add, "same_path", None),
        ],
        Some("without a deletion vector"),
    )]
    #[case::remove_add_same_non_null_dv(
        &[
            FileActionTrackerTestCase::new(TestFileActionType::Remove, "same_path", Some("dv")),
            FileActionTrackerTestCase::new(TestFileActionType::Add, "same_path", Some("dv")),
        ],
        Some("same deletion vector ID"),
    )]
    #[case::remove_add_different_dv(
        &[
            FileActionTrackerTestCase::new(TestFileActionType::Remove, "same_path", Some("dv-1")),
            FileActionTrackerTestCase::new(TestFileActionType::Add, "same_path", Some("dv-2")),
        ],
        None,
    )]
    #[case::add_remove_different_dv(
        &[
            FileActionTrackerTestCase::new(TestFileActionType::Add, "same_path", Some("dv-1")),
            FileActionTrackerTestCase::new(TestFileActionType::Remove, "same_path", Some("dv-2")),
        ],
        None,
    )]
    #[case::same_dv_different_paths(
        &[
            FileActionTrackerTestCase::new(TestFileActionType::Add, "path_0", Some("dv")),
            FileActionTrackerTestCase::new(TestFileActionType::Add, "path_1", Some("dv")),
        ],
        None,
    )]
    fn file_action_combinations_accepted_or_rejected(
        #[case] file_actions: &[FileActionTrackerTestCase],
        #[case] expected_error: Option<&str>,
    ) {
        let mut tracker = FileActionTracker::default();
        let result = file_actions
            .iter()
            .try_for_each(|file_action| file_action.record(&mut tracker));

        if let Some(expected_error) = expected_error {
            assert!(matches!(
                result,
                Err(KernelError::InvalidTransactionState(_))
            ));
            assert_result_error_with_message(result, expected_error);
        } else {
            result.expect("valid file-action combination should be accepted");
        }
    }

    #[derive(Clone, Copy)]
    enum TestFileActionType {
        Add,
        Remove,
    }

    impl TestFileActionType {
        fn record(
            self,
            tracker: &mut FileActionTracker,
            path: &str,
            dv_id: Option<String>,
        ) -> KernelResult<()> {
            match self {
                Self::Add => tracker.record_add(path, dv_id),
                Self::Remove => tracker.record_remove(path, dv_id),
            }
        }
    }

    #[derive(Clone, Copy)]
    struct FileActionTrackerTestCase {
        action_type: TestFileActionType,
        path: &'static str,
        dv_id: Option<&'static str>,
    }

    impl FileActionTrackerTestCase {
        const fn new(
            action_type: TestFileActionType,
            path: &'static str,
            dv_id: Option<&'static str>,
        ) -> Self {
            Self {
                action_type,
                path,
                dv_id,
            }
        }

        fn record(self, tracker: &mut FileActionTracker) -> KernelResult<()> {
            self.action_type
                .record(tracker, self.path, self.dv_id.map(str::to_owned))
        }
    }
}
