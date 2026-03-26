//! File statistics and deltas for CRC tracking.
//!
//! [`FileStats`] represents absolute file-level statistics (count, size, histogram) for a table
//! version. [`FileStatsDelta`] captures the net changes from a single commit, including per-side
//! [`FileSizeHistogram`]s for incremental histogram updates.
//!
//! [`FileStatsDelta`] captures how many files were added/removed and their total sizes. It can be
//! produced from either:
//! 1. In-memory transaction data via [`FileStatsDelta::try_compute_for_txn`]
//! 2. A parsed .json commit file during forward log replay (future)

use std::sync::LazyLock;

use super::FileSizeHistogram;
use crate::engine_data::{FilteredEngineData, GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error, RowVisitor};

/// File-level statistics for a table version: total file count, size, and histogram.
///
/// Obtained via [`Crc::file_stats()`](super::Crc::file_stats), which returns `None` when
/// the stats are not known to be valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStats {
    /// Number of active [`Add`](crate::actions::Add) file actions in this table version.
    pub num_files: i64,
    /// Total size of the table in bytes (sum of all active
    /// [`Add`](crate::actions::Add) file sizes).
    pub table_size_bytes: i64,
    /// Size distribution of active files, if available.
    pub file_size_histogram: Option<FileSizeHistogram>,
}

/// Net file count and size changes from a single commit, with optional per-side histograms.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FileStatsDelta {
    /// Net change in file count (files added minus files removed).
    pub(crate) net_files: i64,
    /// Net change in total bytes (bytes added minus bytes removed).
    pub(crate) net_bytes: i64,
    /// Histogram of file sizes added in this delta. `None` when the delta source does not
    /// provide histogram data (e.g. forward log replay without histogram support).
    pub(crate) added_histogram: Option<FileSizeHistogram>,
    /// Histogram of file sizes removed in this delta. `None` when the delta source does not
    /// provide histogram data.
    pub(crate) removed_histogram: Option<FileSizeHistogram>,
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

    /// Compute file stats and histograms from a transaction's staged add and remove metadata.
    ///
    /// A commit writes three kinds of file actions:
    ///   (1) Add actions (from `add_files_metadata`)
    ///   (2) Remove actions (from `remove_files_metadata`)
    ///   (3) DV update actions (which contain both a Remove and an Add for the same file at
    ///       the same size).
    ///
    /// Only the first two need visiting -- DV updates have a net-zero effect on file counts,
    /// sizes, and histograms.
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
        // create a visitor per batch and accumulate counts and histograms.
        let mut remove_count = 0i64;
        let mut remove_size = 0i64;
        let mut remove_histogram = FileSizeHistogram::create_default();
        for filtered_batch in remove_files_metadata {
            let sv = filtered_batch.selection_vector();
            let sv_opt = if sv.is_empty() { None } else { Some(sv) };
            let mut visitor = FileStatsVisitor::new(sv_opt);
            visitor.visit_rows_of(filtered_batch.data())?;
            remove_count += visitor.count;
            remove_size += visitor.total_size;
            remove_histogram = remove_histogram.try_add(&visitor.histogram)?;
        }

        Ok(FileStatsDelta {
            net_files: add_visitor.count - remove_count,
            net_bytes: add_visitor.total_size - remove_size,
            added_histogram: Some(add_visitor.histogram),
            removed_histogram: Some(remove_histogram),
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
    /// Histogram tracking file size distribution for the visited files.
    histogram: FileSizeHistogram,
}

impl<'sv> FileStatsVisitor<'sv> {
    fn new(selection_vector: Option<&'sv [bool]>) -> Self {
        Self {
            selection_vector,
            offset: 0,
            count: 0,
            total_size: 0,
            histogram: FileSizeHistogram::create_default(),
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
                self.histogram.insert(size)?;
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
        expected_net_files: i64,
        expected_net_bytes: i64,
    }

    #[rstest]
    #[case::empty(TryComputeCase {
        add_batches: vec![],
        remove_batches: vec![],
        expected_net_files: 0,
        expected_net_bytes: 0,
    })]
    #[case::adds_only(TryComputeCase {
        add_batches: vec![vec![100, 200, 300]],
        remove_batches: vec![],
        expected_net_files: 3,
        expected_net_bytes: 600, // 600 = 100 + 200 + 300
    })]
    #[case::multiple_add_batches(TryComputeCase {
        add_batches: vec![vec![100, 200], vec![300, 400, 500]],
        remove_batches: vec![],
        expected_net_files: 5,
        expected_net_bytes: 1500, // 1500 = 100 + 200 + 300 + 400 + 500
    })]
    #[case::removes_only(TryComputeCase {
        add_batches: vec![],
        remove_batches: vec![vec![500, 700]],
        expected_net_files: -2,
        expected_net_bytes: -1200, // -1200 = -(500 + 700)
    })]
    #[case::adds_and_removes(TryComputeCase {
        add_batches: vec![vec![100, 200], vec![300, 400]],
        remove_batches: vec![vec![500], vec![600, 700]],
        expected_net_files: 1,
        expected_net_bytes: -800, // -800 = (100 + 200 + 300 + 400) -(500 + 600 + 700)
    })]
    fn test_try_compute(#[case] case: TryComputeCase) {
        let adds: Vec<_> = case.add_batches.into_iter().map(size_batch).collect();
        let removes: Vec<_> = case
            .remove_batches
            .into_iter()
            .map(|sizes| FilteredEngineData::with_all_rows_selected(size_batch(sizes)))
            .collect();
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes).unwrap();
        assert_eq!(stats.net_files, case.expected_net_files);
        assert_eq!(stats.net_bytes, case.expected_net_bytes);
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

    #[test]
    fn try_compute_builds_histograms_from_add_and_remove_sizes() {
        let adds = vec![size_batch(vec![100, 200, 300])];
        let removes = vec![FilteredEngineData::with_all_rows_selected(size_batch(
            vec![500, 700],
        ))];
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes).unwrap();

        // All sizes < 8KB so they all land in bin 0
        let added = stats.added_histogram.unwrap();
        assert_eq!(added.file_counts[0], 3);
        assert_eq!(added.total_bytes[0], 600);
        let removed = stats.removed_histogram.unwrap();
        assert_eq!(removed.file_counts[0], 2);
        assert_eq!(removed.total_bytes[0], 1200);
    }

    #[test]
    fn try_compute_empty_batches_produce_zero_histograms() {
        let stats = FileStatsDelta::try_compute_for_txn(&[], &[]).unwrap();
        let added = stats.added_histogram.unwrap();
        assert!(added.file_counts.iter().all(|&c| c == 0));
        let removed = stats.removed_histogram.unwrap();
        assert!(removed.file_counts.iter().all(|&c| c == 0));
    }

    #[test]
    fn try_compute_histograms_with_selection_vectors() {
        let adds = vec![size_batch(vec![100, 200])];
        let removes = vec![FilteredEngineData::try_new(
            size_batch(vec![300, 400, 500]),
            vec![true, false, true], // 300 selected, 400 skipped, 500 selected
        )
        .unwrap()];
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes).unwrap();

        let added = stats.added_histogram.unwrap();
        assert_eq!(added.file_counts[0], 2);
        assert_eq!(added.total_bytes[0], 300);
        // Only 300 and 500 are selected (400 skipped)
        let removed = stats.removed_histogram.unwrap();
        assert_eq!(removed.file_counts[0], 2);
        assert_eq!(removed.total_bytes[0], 800);
    }
}
