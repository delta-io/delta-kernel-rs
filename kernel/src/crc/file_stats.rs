// No consumers yet -- will be integrated in a follow-up PR.
#![allow(dead_code)]
//! File statistics delta: the file count and size changes from a single commit.
//!
//! [`FileStatsDelta`] captures how many files were added/removed and their total sizes. It can be
//! produced from either:
//! 1. In-memory transaction data via [`FileStatsDelta::try_compute_for_txn`]
//! 2. A parsed .json commit file during forward log replay (future)

use std::sync::LazyLock;

use crate::engine_data::{FilteredEngineData, GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error, RowVisitor};

/// Net file count and size changes from a single commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FileStatsDelta {
    /// Net change in file count (files added minus files removed).
    pub(crate) net_files: i64,
    /// Net change in total bytes (bytes added minus bytes removed).
    pub(crate) net_bytes: i64,
}

impl FileStatsDelta {
    /// Returns `true` if the given operation can be safely tracked by incremental file stats.
    ///
    /// Incremental-safe operations produce add/remove actions whose net counts give correct
    /// file stats. Unknown or missing operations are treated as unsafe. For example, ANALYZE
    /// STATS re-adds existing files with updated statistics -- if we naively counted those
    /// adds, we'd double count file stats.
    const INCREMENTAL_SAFE_OPS: &[&str] = &[
        "WRITE",
        "MERGE",
        "UPDATE",
        "DELETE",
        "OPTIMIZE",
        "CREATE TABLE",
        "REPLACE TABLE",
        "CREATE TABLE AS SELECT",
        "REPLACE TABLE AS SELECT",
        "CREATE OR REPLACE TABLE AS SELECT",
    ];

    pub(crate) fn is_incremental_safe(operation: &str) -> bool {
        Self::INCREMENTAL_SAFE_OPS.contains(&operation)
    }

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
        let mut add_visitor = FileStatsVisitor::new(None);
        for batch in add_files_metadata {
            add_visitor.visit_rows_of(batch.as_ref())?;
        }

        // Visit remove files. Each FilteredEngineData has its own selection vector, so we
        // create a visitor per batch and accumulate counts.
        let mut remove_count = 0i64;
        let mut remove_size = 0i64;
        for filtered_batch in remove_files_metadata {
            let sv = filtered_batch.selection_vector();
            let sv_opt = if sv.is_empty() { None } else { Some(sv) };
            let mut visitor = FileStatsVisitor::new(sv_opt);
            visitor.visit_rows_of(filtered_batch.data())?;
            remove_count += visitor.count;
            remove_size += visitor.total_size;
        }

        Ok(FileStatsDelta {
            net_files: add_visitor.count - remove_count,
            net_bytes: add_visitor.total_size - remove_size,
        })
    }
}

/// Visitor that extracts the `size` column from file metadata and accumulates counts and totals.
///
/// Accepts an optional selection vector to filter which rows are visited. AddFiles pass `None`
/// (count every row); RemoveFiles may pass `Some(sv)` from [`FilteredEngineData`] to skip rows
/// that are not actually being removed.
///
/// Tracks an `offset` across multiple `visit()` calls to correctly index into the selection
/// vector when the engine delivers data in multiple batches.
struct FileStatsVisitor<'sv> {
    /// Optional selection vector. When `Some`, only rows marked `true` are counted. Rows beyond
    /// the SV length are implicitly selected.
    selection_vector: Option<&'sv [bool]>,
    /// Offset into the selection vector, tracking position across multiple visit calls.
    offset: usize,
    count: i64,
    total_size: i64,
}

impl<'sv> FileStatsVisitor<'sv> {
    fn new(selection_vector: Option<&'sv [bool]>) -> Self {
        Self {
            selection_vector,
            offset: 0,
            count: 0,
            total_size: 0,
        }
    }
}

impl RowVisitor for FileStatsVisitor<'_> {
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
            let selected = match self.selection_vector {
                Some(sv) => sv.get(self.offset + i).copied().unwrap_or(true),
                None => true,
            };
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
