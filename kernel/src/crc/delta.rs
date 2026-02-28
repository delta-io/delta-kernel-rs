//! CRC delta: the change a single commit introduces to CRC state.
//!
//! Note: "delta" here means "difference/change", not a Delta commit (.json) file.
//!
//! A [`CrcDelta`] captures the CRC-relevant changes introduced by a single commit. It can be
//! produced from either:
//! 1. In-memory transaction data (the connector just committed N+1)
//! 2. A parsed .json commit file during forward log replay (future)
//!
//! The same struct is used in both cases. [`Crc::apply_delta`] folds a `CrcDelta` into a
//! [`Crc`] to produce the next version's CRC state.
//!
//! [`Crc::apply_delta`]: super::Crc::apply_delta
//! [`Crc`]: super::Crc

use std::sync::LazyLock;

use crate::engine_data::{FilteredEngineData, GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error, RowVisitor};

/// The change a single commit introduces to [`Crc`] state.
///
/// Produced from in-memory transaction data or from a parsed .json commit file during forward
/// log replay (future). Folded into a [`Crc`] via [`Crc::apply_delta`].
///
/// Always faithfully reports what it observed. Whether accumulated file stats are trustworthy
/// across multiple deltas will be tracked by a future `CrcAccumulator` (not yet implemented).
///
/// [`Crc`]: super::Crc
/// [`Crc::apply_delta`]: super::Crc::apply_delta
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CrcDelta {
    /// File-level statistics changes observed in this commit (add/remove counts and sizes).
    /// Always present, even for metadata-only commits (zero delta).
    pub(crate) file_stats: FileStatsDelta,
    /// The in-commit timestamp of this commit, if ICT is enabled.
    pub(crate) in_commit_timestamp: Option<i64>,
    // Future fields:
    // pub(crate) domain_metadata: Vec<DomainMetadata>,
    // pub(crate) set_transactions: Vec<SetTransaction>,
    // pub(crate) protocol: Option<Protocol>,
    // pub(crate) metadata: Option<Metadata>,
}

/// The file count and size changes from a single commit (adds and removes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileStatsDelta {
    /// Number of files added by this commit.
    pub(crate) num_adds: i64,
    /// Number of files removed by this commit.
    pub(crate) num_removes: i64,
    /// Total size in bytes of files added by this commit.
    pub(crate) size_bytes_adds: i64,
    /// Total size in bytes of files removed by this commit.
    pub(crate) size_bytes_removes: i64,
}

/// Visitor that extracts the `size` column from add/remove file metadata and accumulates
/// file counts and total sizes.
struct FileStatsVisitor {
    /// Running count of files visited.
    count: i64,
    /// Running sum of file sizes visited.
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

impl FileStatsDelta {
    /// Compute file stats delta from a transaction's staged add and remove file metadata.
    ///
    /// A commit writes three kinds of actions: add actions (from `add_files_metadata`), remove
    /// actions (from `remove_files_metadata`), and DV update actions (from `dv_matched_files`,
    /// which contain both a Remove and an Add for the same file at the same size). We only need
    /// to visit the first two -- DV updates have a net-zero effect on file counts and sizes.
    pub(crate) fn try_compute(
        add_files_metadata: &[Box<dyn EngineData>],
        remove_files_metadata: &[FilteredEngineData],
    ) -> DeltaResult<Self> {
        // Visit add files. add_files_metadata is Vec<Box<dyn EngineData>> (no selection
        // vector), so every row is a file being added.
        let mut add_visitor = FileStatsVisitor::new();
        for batch in add_files_metadata {
            add_visitor.visit_rows_of(batch.as_ref())?;
        }

        // Visit remove files. remove_files_metadata is Vec<FilteredEngineData>, which pairs
        // rows with a selection vector -- only rows marked `true` are actually being removed.
        // See SelectionVectorFileStatsVisitor for details.
        let mut remove_visitor = FileStatsVisitor::new();
        for filtered_batch in remove_files_metadata {
            let sv = filtered_batch.selection_vector();
            if sv.is_empty() {
                // All rows selected
                remove_visitor.visit_rows_of(filtered_batch.data())?;
            } else {
                let data: &dyn EngineData = filtered_batch.data();
                let (names, _types) = remove_visitor.selected_column_names_and_types();
                let mut sv_visitor = SelectionVectorFileStatsVisitor::new(sv);
                data.visit_rows(names, &mut sv_visitor)?;
                remove_visitor.count += sv_visitor.count;
                remove_visitor.total_size += sv_visitor.total_size;
            }
        }

        Ok(FileStatsDelta {
            num_adds: add_visitor.count,
            num_removes: remove_visitor.count,
            size_bytes_adds: add_visitor.total_size,
            size_bytes_removes: remove_visitor.total_size,
        })
    }
}

/// Visitor that extracts file sizes while respecting a selection vector.
///
/// Remove file batches come as `FilteredEngineData`, which pairs rows with a selection vector.
/// Only rows marked `true` are actually being removed; `false` rows are untouched files that
/// happen to be in the same batch.
///
/// Example: a batch of 4 files with selection vector [true, false, true, false]:
///
///   row 0: file_a.parquet  size=500  sv=true   -> counted (removed)
///   row 1: file_b.parquet  size=300  sv=false  -> skipped (not removed)
///   row 2: file_c.parquet  size=200  sv=true   -> counted (removed)
///   row 3: file_d.parquet  size=400  sv=false  -> skipped (not removed)
///
///   result: count=2, total_size=700
struct SelectionVectorFileStatsVisitor<'sv> {
    /// Guaranteed non-empty; the caller uses `FileStatsVisitor` directly when the SV is empty.
    selection_vector: &'sv [bool],
    /// Offset into the selection vector (tracks position across multiple visit calls).
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
            // If the selection vector doesn't cover this row, it's implicitly selected
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
        expected: FileStatsDelta { num_adds: 0, num_removes: 0, size_bytes_adds: 0, size_bytes_removes: 0 },
    })]
    #[case::adds_only(TryComputeCase {
        add_batches: vec![vec![100, 200, 300]],
        remove_batches: vec![],
        expected: FileStatsDelta { num_adds: 3, num_removes: 0, size_bytes_adds: 600, size_bytes_removes: 0 },
    })]
    #[case::multiple_add_batches(TryComputeCase {
        add_batches: vec![vec![100, 200], vec![300, 400, 500]],
        remove_batches: vec![],
        expected: FileStatsDelta { num_adds: 5, num_removes: 0, size_bytes_adds: 1500, size_bytes_removes: 0 },
    })]
    #[case::removes_only(TryComputeCase {
        add_batches: vec![],
        remove_batches: vec![vec![500, 700]],
        expected: FileStatsDelta { num_adds: 0, num_removes: 2, size_bytes_adds: 0, size_bytes_removes: 1200 },
    })]
    #[case::adds_and_removes(TryComputeCase {
        add_batches: vec![vec![100, 200], vec![300, 400]],
        remove_batches: vec![vec![500], vec![600, 700]],
        expected: FileStatsDelta { num_adds: 4, num_removes: 3, size_bytes_adds: 1000, size_bytes_removes: 1800 },
    })]
    fn test_try_compute(#[case] case: TryComputeCase) {
        let adds: Vec<_> = case.add_batches.into_iter().map(size_batch).collect();
        let removes: Vec<_> = case
            .remove_batches
            .into_iter()
            .map(|sizes| FilteredEngineData::with_all_rows_selected(size_batch(sizes)))
            .collect();
        let stats = FileStatsDelta::try_compute(&adds, &removes).unwrap();
        assert_eq!(stats, case.expected);
    }

    #[test]
    fn test_try_compute_removes_with_selection_vector() {
        let data = size_batch(vec![500, 700, 900]); // 700 is not selected
        let filtered = FilteredEngineData::try_new(data, vec![true, false, true]).unwrap();
        let stats = FileStatsDelta::try_compute(&[], &[filtered]).unwrap();
        assert_eq!(stats.num_removes, 2);
        assert_eq!(stats.size_bytes_removes, 1400); // 500 + 900
    }
}
