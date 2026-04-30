//! File statistics and deltas for CRC tracking.
//!
//! [`FileStats`] represents absolute file-level statistics (count, size, histogram) for a table
//! version. [`FileStatsDelta`] captures the net changes from a single commit as a single delta
//! [`FileSizeHistogram`] (adds minus removes).
//!
//! [`FileStatsDelta`] captures how many files were added/removed and their total sizes. It can be
//! produced from either:
//! 1. In-memory transaction data via [`FileStatsDelta::try_compute_for_txn`]
//! 2. A parsed .json commit file

use std::sync::LazyLock;

use super::FileSizeHistogram;
use crate::engine_data::{FilteredEngineData, GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error, RowVisitor};

/// File-level statistics for a table version: total file count, size, and histogram.
///
/// Obtained via [`Snapshot::get_or_load_file_stats`], [`Snapshot::get_file_stats_if_loaded`],
/// or [`Crc::file_stats()`](super::Crc::file_stats). Returns `None` when the stats are not
/// known to be valid.
///
/// [`Snapshot::get_or_load_file_stats`]: crate::snapshot::Snapshot::get_or_load_file_stats
/// [`Snapshot::get_file_stats_if_loaded`]: crate::snapshot::Snapshot::get_file_stats_if_loaded
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStats {
    /// Number of active [`Add`](crate::actions::Add) file actions in this table version.
    pub(crate) num_files: i64,
    /// Total size of the table in bytes (sum of all active
    /// [`Add`](crate::actions::Add) file sizes).
    pub(crate) table_size_bytes: i64,
    /// Size distribution of active files, if available.
    pub(crate) file_size_histogram: Option<FileSizeHistogram>,
}

impl FileStats {
    /// Returns the number of active [`Add`](crate::actions::Add) file actions in this table
    /// version.
    pub fn num_files(&self) -> i64 {
        self.num_files
    }

    /// Returns the total size of the table in bytes (sum of all active
    /// [`Add`](crate::actions::Add) file sizes).
    pub fn table_size_bytes(&self) -> i64 {
        self.table_size_bytes
    }

    /// Returns the size distribution of active files, if available.
    pub fn file_size_histogram(&self) -> Option<&FileSizeHistogram> {
        self.file_size_histogram.as_ref()
    }
}

/// Net file count and size changes from a single commit, with an optional net histogram.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FileStatsDelta {
    /// Net change in file count (files added minus files removed).
    pub(crate) net_files: i64,
    /// Net change in total bytes (bytes added minus bytes removed).
    pub(crate) net_bytes: i64,
    /// Net change in file size histogram (adds minus removes per bin). May contain negative
    /// values in bins where more files were removed than added. `None` when the delta source
    /// does not provide histogram data.
    pub(crate) net_histogram: Option<FileSizeHistogram>,
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

    /// Compute file stats and a delta histogram from a transaction's staged add and remove
    /// metadata.
    ///
    /// A commit writes three kinds of file actions:
    ///   (1) Add actions (from `add_files_metadata`)
    ///   (2) Remove actions (from `remove_files_metadata`)
    ///   (3) DV update actions (which contain both a Remove and an Add for the same file at
    ///       the same size).
    ///
    /// Only the first two need visiting -- DV updates have a net-zero effect on file counts,
    /// sizes, and histograms.
    ///
    /// `bin_boundaries` specifies the histogram bin boundaries to use. When `Some`, the
    /// delta histogram is built with those boundaries (matching the previous CRC's histogram).
    /// When `None`, the standard default boundaries are used. Callers should pass the previous
    /// CRC's boundaries when available so that `try_apply_delta` in [`Crc::apply`] succeeds.
    pub(crate) fn try_compute_for_txn(
        add_files_metadata: &[Box<dyn EngineData>],
        remove_files_metadata: &[FilteredEngineData],
        bin_boundaries: Option<&[i64]>,
    ) -> DeltaResult<Self> {
        let mut histogram = match bin_boundaries {
            Some(b) => FileSizeHistogram::create_empty_with_boundaries(b.to_vec())?,
            None => FileSizeHistogram::create_default(),
        };
        let mut net_files = 0i64;
        let mut net_bytes = 0i64;

        // Visit add files (insert into histogram). Every row is a file being added.
        for batch in add_files_metadata {
            let mut visitor = FileStatsVisitor::new(None, false, &mut histogram);
            visitor.visit_rows_of(batch.as_ref())?;
            net_files += visitor.count;
            net_bytes += visitor.total_size;
        }

        // Visit remove files (remove from histogram). Each FilteredEngineData has its own
        // selection vector, so we create a visitor per batch.
        for filtered_batch in remove_files_metadata {
            let sv = filtered_batch.selection_vector();
            let sv_opt = if sv.is_empty() { None } else { Some(sv) };
            let mut visitor = FileStatsVisitor::new(sv_opt, true, &mut histogram);
            visitor.visit_rows_of(filtered_batch.data())?;
            net_files += visitor.count;
            net_bytes += visitor.total_size;
        }

        Ok(FileStatsDelta {
            net_files,
            net_bytes,
            net_histogram: Some(histogram),
        })
    }
}

/// Visitor that extracts the `size` column from file metadata and updates a shared histogram.
///
/// When `is_remove` is false (add files), each visited row increments the histogram bin's count
/// and bytes. When true (remove files), each row decrements them. This builds a single delta
/// histogram directly without needing separate add/remove histograms.
///
/// Accepts an optional selection vector to filter which rows are visited. AddFiles pass `None`
/// (count every row); RemoveFiles may pass `Some(sv)` from [`FilteredEngineData`] to skip rows
/// that are not actually being removed.
struct FileStatsVisitor<'sv, 'h> {
    /// Optional selection vector. When `Some`, only rows marked `true` are counted. Rows beyond
    /// the SV length are implicitly selected.
    selection_vector: Option<&'sv [bool]>,
    /// Offset into the selection vector, tracking position across multiple visit calls.
    offset: usize,
    /// Whether this visitor is processing remove files (decrements) vs add files (increments).
    is_remove: bool,
    /// Net file count contribution from this visitor. Negative for remove visitors.
    count: i64,
    /// Net byte size contribution from this visitor. Negative for remove visitors.
    total_size: i64,
    /// Shared histogram that all visitors (add and remove) write to.
    histogram: &'h mut FileSizeHistogram,
}

impl<'sv, 'h> FileStatsVisitor<'sv, 'h> {
    fn new(
        selection_vector: Option<&'sv [bool]>,
        is_remove: bool,
        histogram: &'h mut FileSizeHistogram,
    ) -> Self {
        Self {
            selection_vector,
            offset: 0,
            is_remove,
            count: 0,
            total_size: 0,
            histogram,
        }
    }
}

impl RowVisitor for FileStatsVisitor<'_, '_> {
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
                if self.is_remove {
                    self.count -= 1;
                    self.total_size -= size;
                    self.histogram.remove(size)?;
                } else {
                    self.count += 1;
                    self.total_size += size;
                    self.histogram.insert(size)?;
                }
            }
        }
        self.offset += row_count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use test_utils::{generate_batch, IntoArray};

    use super::*;
    use crate::engine::arrow_data::ArrowEngineData;

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
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes, None).unwrap();
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
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes, None).unwrap();
        // adds: 3 files, 600 bytes (100 + 200 + 300)
        // removes: 4 files, 2400 bytes (400 + 500 + 700 + 800)
        assert_eq!(stats.net_files, -1); // 3 - 4
        assert_eq!(stats.net_bytes, -1800); // 600 - 2400
    }

    #[test]
    fn try_compute_builds_delta_histogram_from_add_and_remove_sizes() {
        let adds = vec![size_batch(vec![100, 200, 300])];
        let removes = vec![FilteredEngineData::with_all_rows_selected(size_batch(
            vec![500, 700],
        ))];
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes, None).unwrap();

        // All sizes < 8KB so they all land in bin 0. Net: 3 adds - 2 removes = 1 file,
        // 600 - 1200 = -600 bytes.
        let delta = stats.net_histogram.unwrap();
        assert_eq!(delta.file_counts[0], 1);
        assert_eq!(delta.total_bytes[0], -600);
    }

    #[test]
    fn try_compute_empty_batches_produce_zero_histogram() {
        let stats = FileStatsDelta::try_compute_for_txn(&[], &[], None).unwrap();
        let delta = stats.net_histogram.unwrap();
        assert!(delta.file_counts.iter().all(|&c| c == 0));
        assert!(delta.total_bytes.iter().all(|&b| b == 0));
    }

    #[test]
    fn try_compute_histogram_with_selection_vectors() {
        let adds = vec![size_batch(vec![100, 200])];
        let removes = vec![FilteredEngineData::try_new(
            size_batch(vec![300, 400, 500]),
            vec![true, false, true], // 300 selected, 400 skipped, 500 selected
        )
        .unwrap()];
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes, None).unwrap();

        // Net bin 0: 2 adds - 2 removes = 0 files, 300 - 800 = -500 bytes
        let delta = stats.net_histogram.unwrap();
        assert_eq!(delta.file_counts[0], 0);
        assert_eq!(delta.total_bytes[0], -500);
    }

    #[test]
    fn try_compute_with_custom_boundaries_uses_them() {
        // Custom 3-bin histogram: [0, 200) [200, 1000) [1000, inf)
        let boundaries: &[i64] = &[0, 200, 1000];
        let adds = vec![size_batch(vec![50, 300, 1500])];
        let removes = vec![FilteredEngineData::with_all_rows_selected(size_batch(
            vec![100, 500],
        ))];
        let stats = FileStatsDelta::try_compute_for_txn(&adds, &removes, Some(boundaries)).unwrap();

        let delta = stats.net_histogram.unwrap();
        assert_eq!(delta.sorted_bin_boundaries, vec![0, 200, 1000]);
        // Net per bin: (1-1, 1-1, 1-0) = (0, 0, 1)
        assert_eq!(delta.file_counts, vec![0, 0, 1]);
        // Net per bin: (50-100, 300-500, 1500-0) = (-50, -200, 1500)
        assert_eq!(delta.total_bytes, vec![-50, -200, 1500]);
    }

    #[test]
    fn try_compute_with_custom_boundaries_produces_mergeable_histogram() {
        // Build a base histogram with custom boundaries, then verify delta merges correctly.
        let boundaries = vec![0, 200, 1000];
        let mut base = FileSizeHistogram::create_empty_with_boundaries(boundaries.clone()).unwrap();
        base.insert(150).unwrap(); // bin 0
        base.insert(500).unwrap(); // bin 1

        let adds = vec![size_batch(vec![100, 300])];
        let removes = vec![FilteredEngineData::with_all_rows_selected(size_batch(
            vec![150],
        ))];
        let stats =
            FileStatsDelta::try_compute_for_txn(&adds, &removes, Some(&boundaries)).unwrap();

        let delta = stats.net_histogram.unwrap();
        let merged = base.try_apply_delta(&delta).unwrap();
        assert_eq!(merged.file_counts, vec![1, 2, 0]); // (1+1-1), (1+1-0), (0+0-0)
        assert_eq!(merged.total_bytes, vec![100, 800, 0]); // (150+100-150), (500+300-0)
    }
}
