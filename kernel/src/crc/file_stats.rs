// No consumers yet -- will be integrated in a follow-up PR.
#![allow(dead_code)]
//! File statistics delta: the file count and size changes from a single commit.
//!
//! [`FileStatsDelta`] captures how many files were added/removed and their total sizes. It can be
//! produced from either:
//! 1. In-memory transaction data via [`FileStatsDelta::try_new_from_transaction_data`]
//! 2. A parsed .json commit file during forward log replay (future)

use std::sync::LazyLock;

use crate::engine_data::{FilteredEngineData, GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error, RowVisitor};

/// Net file count and size changes from a single commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileStatsDelta {
    /// Net change in file count (files added minus files removed).
    pub(crate) net_files: i64,
    /// Net change in total bytes (bytes added minus bytes removed).
    pub(crate) net_bytes: i64,
}

impl FileStatsDelta {
    /// Compute file stats from a transaction's staged add and remove file metadata.
    ///
    /// A commit writes three kinds of file actions:
    ///   (1) Add actions (from `add_files_metadata`)
    ///   (2) Remove actions (from `remove_files_metadata`)
    ///   (3) DV update actions (which contain both a Remove and an Add for the same file at the same size).
    ///
    /// Only the first two need visiting -- DV updates have a net-zero effect on file counts and sizes.
    pub(crate) fn try_compute_for_txn(
        add_files_metadata: &[Box<dyn EngineData>],
        remove_files_metadata: &[FilteredEngineData],
    ) -> DeltaResult<Self> {
        // Visit add files. Every row is a file being added (no selection vector).
        let mut add_visitor = FileStatsVisitor::new();
        for batch in add_files_metadata {
            add_visitor.visit_rows_of(batch.as_ref())?;
        }

        // Visit remove files. FilteredEngineData pairs rows with a selection vector -- only
        // rows marked `true` are actually being removed.
        let mut remove_visitor = FileStatsVisitor::new();
        for filtered_batch in remove_files_metadata {
            let sv = filtered_batch.selection_vector();
            if sv.is_empty() {
                // All rows selected -- use simple visitor.
                remove_visitor.visit_rows_of(filtered_batch.data())?;
            } else {
                // Some rows filtered -- use SV-aware visitor.
                let data: &dyn EngineData = filtered_batch.data();
                let (names, _types) = remove_visitor.selected_column_names_and_types();
                let mut sv_visitor = SelectionVectorFileStatsVisitor::new(sv);
                data.visit_rows(names, &mut sv_visitor)?;
                remove_visitor.count += sv_visitor.count;
                remove_visitor.total_size += sv_visitor.total_size;
            }
        }

        Ok(FileStatsDelta {
            net_files: add_visitor.count - remove_visitor.count,
            net_bytes: add_visitor.total_size - remove_visitor.total_size,
        })
    }
}

/// Visitor that extracts the `size` column from file metadata and accumulates counts and totals.
struct FileStatsVisitor {
    count: i64,
    total_size: i64,
}

impl FileStatsVisitor {
    fn new() -> Self {
        Self {
            count: 0,
            total_size: 0,
        }
    }
}

impl RowVisitor for FileStatsVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![ColumnName::new(["size"])], vec![DataType::LONG]).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "Wrong number of FileStatsVisitor getters: {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            let size: i64 = getters[0].get(i, "size")?;
            self.count += 1;
            self.total_size += size;
        }
        Ok(())
    }
}

/// Visitor that extracts file sizes while respecting a selection vector.
///
/// Remove file batches come as [`FilteredEngineData`], which pairs rows with a selection vector.
/// Only rows marked `true` are actually being removed; `false` rows are untouched files that
/// happen to be in the same batch.
///
/// Maintains an `offset` counter across multiple `visit()` calls to correctly index into the
/// selection vector when processing multi-batch data.
struct SelectionVectorFileStatsVisitor<'sv> {
    /// Guaranteed non-empty; the caller uses [`FileStatsVisitor`] directly when the SV is empty.
    selection_vector: &'sv [bool],
    /// Offset into the selection vector, tracking position across multiple visit calls.
    offset: usize,
    count: i64,
    total_size: i64,
}

impl<'sv> SelectionVectorFileStatsVisitor<'sv> {
    fn new(selection_vector: &'sv [bool]) -> Self {
        Self {
            selection_vector,
            offset: 0,
            count: 0,
            total_size: 0,
        }
    }
}

impl RowVisitor for SelectionVectorFileStatsVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![ColumnName::new(["size"])], vec![DataType::LONG]).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "Wrong number of SelectionVectorFileStatsVisitor getters: {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            let global_idx = self.offset + i;
            // Rows beyond the SV length are implicitly selected.
            let selected = self
                .selection_vector
                .get(global_idx)
                .copied()
                .unwrap_or(true);
            if selected {
                let size: i64 = getters[0].get(i, "size")?;
                self.count += 1;
                self.total_size += size;
            }
        }
        self.offset += row_count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::arrow_data::ArrowEngineData;
    use rstest::rstest;
    use test_utils::{generate_batch, IntoArray};

    fn size_batch(sizes: Vec<i64>) -> Box<dyn EngineData> {
        let batch = generate_batch(vec![("size", sizes.into_array())]).unwrap();
        Box::new(ArrowEngineData::new(batch))
    }

    struct TryComputeCase {
        add_batches: Vec<Vec<i64>>,
        remove_batches: Vec<Vec<i64>>,
        expected: FileStatsDelta,
    }

    #[rstest]
    #[case::empty(TryComputeCase {
        add_batches: vec![],
        remove_batches: vec![],
        expected: FileStatsDelta { net_files: 0, net_bytes: 0 },
    })]
    #[case::adds_only(TryComputeCase {
        add_batches: vec![vec![100, 200, 300]],
        remove_batches: vec![],
        expected: FileStatsDelta { net_files: 3, net_bytes: 600 }, // 600 = 100 + 200 + 300
    })]
    #[case::multiple_add_batches(TryComputeCase {
        add_batches: vec![vec![100, 200], vec![300, 400, 500]],
        remove_batches: vec![],
        expected: FileStatsDelta { net_files: 5, net_bytes: 1500 }, // 1500 = 100 + 200 + 300 + 400 + 500
    })]
    #[case::removes_only(TryComputeCase {
        add_batches: vec![],
        remove_batches: vec![vec![500, 700]],
        expected: FileStatsDelta { net_files: -2, net_bytes: -1200 }, // -1200 = -(500 + 700)
    })]
    #[case::adds_and_removes(TryComputeCase {
        add_batches: vec![vec![100, 200], vec![300, 400]],
        remove_batches: vec![vec![500], vec![600, 700]],
        expected: FileStatsDelta { net_files: 1, net_bytes: -800 }, // -800 = (100 + 200 + 300 + 400) -(500 + 600 + 700)
    })]
    fn test_try_compute(#[case] case: TryComputeCase) {
        let adds: Vec<_> = case.add_batches.into_iter().map(size_batch).collect();
        let removes: Vec<_> = case
            .remove_batches
            .into_iter()
            .map(|sizes| FilteredEngineData::with_all_rows_selected(size_batch(sizes)))
            .collect();
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes).unwrap();
        assert_eq!(stats, case.expected);
    }

    #[test]
    fn test_with_selection_vectors() {
        // Multiple add batches + multiple remove batches with mixed SV scenarios
        let adds = vec![size_batch(vec![100, 200]), size_batch(vec![300])];
        let removes = vec![
            // First remove batch: all rows selected (no SV)
            FilteredEngineData::with_all_rows_selected(size_batch(vec![400, 500])),
            // Second remove batch: partial selection (600 skipped)
            FilteredEngineData::try_new(size_batch(vec![600, 700, 800]), vec![false, true, true])
                .unwrap(),
        ];
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes).unwrap();
        // adds: 3 files, 600 bytes (100 + 200 + 300)
        // removes: 4 files, 2400 bytes (400 + 500 + 700 + 800)
        assert_eq!(stats.net_files, -1); // 3 - 4
        assert_eq!(stats.net_bytes, -1800); // 600 - 2400
    }
}
