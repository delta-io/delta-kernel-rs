//! Typed state enums for CRC tracking.
//!
//! Each enum encodes both *what we know* about a slice of CRC state and *the trustworthiness*
//! of those values. Variants carry data exactly when it makes sense for that state, making
//! invalid reads unrepresentable: the compiler prevents reading an absolute count from a
//! degraded state because the field does not exist there.
//!
//! Today this module hosts [`FileStatsState`]; follow-up PRs add `DomainMetadataState` and
//! `SetTransactionState` (Complete / Partial variants).

use super::file_stats::FileStats;

/// The state of file statistics for a CRC.
///
/// # State transitions during `Crc::apply`
///
/// | Current       | + safe op     | + unsafe op or missing remove.size |
/// |---------------|---------------|------------------------------------|
/// | Complete      | Complete      | Indeterminate                      |
/// | Indeterminate | Indeterminate | Indeterminate                      |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileStatsState {
    /// File stats are known-correct absolute totals. The carried [`FileStats`]'s
    /// `file_size_histogram` is optional: `Some` when the CRC source had a histogram (or full
    /// replay produced one), `None` when the source lacked one. Safe to write to disk (with or
    /// without histogram).
    Complete(FileStats),
    /// File stats cannot be determined incrementally. Reasons include a non-incremental
    /// operation (like `ANALYZE STATS`) that re-adds files without corresponding removes,
    /// or a remove action with a missing `size` field. A full add/remove reconciliation pass
    /// can recover `Complete`.
    Indeterminate,
}

impl FileStatsState {
    /// Returns absolute file stats only when `Complete`. Returns `None` for `Indeterminate`.
    pub fn file_stats(&self) -> Option<&FileStats> {
        match self {
            Self::Complete(stats) => Some(stats),
            _ => None,
        }
    }

    /// Returns `true` if file stats are known-correct absolute totals. Also gates whether
    /// the CRC is safe to write to disk: only `Complete` CRCs have well-defined on-disk
    /// representations.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }

    /// Returns `true` if file stats cannot be determined incrementally and require a full
    /// add/remove reconciliation pass to recover.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn is_indeterminate(&self) -> bool {
        self == &Self::Indeterminate
    }
}

// TODO(#2568): make `Default` test-only. `Crc::default()` produces a Complete-zero CRC
//              that passes `is_complete()` and could be silently written.
impl Default for FileStatsState {
    fn default() -> Self {
        Self::Complete(FileStats::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc::FileSizeHistogram;

    #[test]
    fn complete_with_histogram_returns_file_stats_with_histogram() {
        let state = FileStatsState::Complete(FileStats {
            num_files: 10,
            table_size_bytes: 5000,
            file_size_histogram: Some(FileSizeHistogram::create_default()),
        });
        let stats = state.file_stats().unwrap();
        assert_eq!(stats.num_files(), 10);
        assert_eq!(stats.table_size_bytes(), 5000);
        assert!(stats.file_size_histogram().is_some());
    }

    #[test]
    fn complete_without_histogram_returns_file_stats_without_histogram() {
        let state = FileStatsState::Complete(FileStats {
            num_files: 10,
            table_size_bytes: 5000,
            file_size_histogram: None,
        });
        let stats = state.file_stats().unwrap();
        assert_eq!(stats.num_files(), 10);
        assert_eq!(stats.table_size_bytes(), 5000);
        assert!(stats.file_size_histogram().is_none());
    }

    #[test]
    fn indeterminate_returns_none_for_file_stats() {
        assert!(FileStatsState::Indeterminate.file_stats().is_none());
    }

    #[test]
    fn is_predicates_match_variants() {
        let complete = FileStatsState::default();
        assert!(complete.is_complete());
        assert!(!complete.is_indeterminate());

        let indet = FileStatsState::Indeterminate;
        assert!(!indet.is_complete());
        assert!(indet.is_indeterminate());
    }

    #[test]
    fn default_is_complete_zero() {
        assert_eq!(
            FileStatsState::default(),
            FileStatsState::Complete(FileStats {
                num_files: 0,
                table_size_bytes: 0,
                file_size_histogram: None,
            })
        );
    }
}
