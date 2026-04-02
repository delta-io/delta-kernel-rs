//! State enums for CRC tracking: file stats, domain metadata, and set transactions.
//!
//! [`FileStatsState`] encodes both the validity AND the values of file statistics,
//! eliminating the ambiguity where `num_files` could mean an absolute count, a relative
//! delta, or garbage depending on a separate validity flag.
//!
//! [`DomainMetadataState`] and [`SetTransactionState`] encode the completeness of cached
//! domain metadata and set transactions. No artificial bounding. Partial state arises
//! naturally when the base CRC lacks these fields but the replay range found some entries.

use std::collections::HashMap;

use crate::actions::{DomainMetadata, SetTransaction};

use super::file_stats::FileStats;
use super::FileSizeHistogram;

// ============================================================================
// File stats state
// ============================================================================

/// The state of file statistics for a CRC. Encodes both what we know and how to recover
/// what we don't know.
///
/// Each variant carries exactly the data that makes sense for that state. You cannot
/// accidentally use a commit delta as an absolute file count because they live in
/// different variants with different field names.
#[allow(dead_code)] // Variants used in follow-up steps (CrcProcessor, Snapshot integration).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileStatsState {
    /// File stats are known-correct absolute totals. Histogram is optional: `Some` when the
    /// base CRC had a histogram (or we built one during full replay), `None` when the base
    /// CRC lacked a histogram. Safe to write CRC to disk (with or without histogram).
    Valid {
        num_files: i64,
        table_size_bytes: i64,
        histogram: Option<FileSizeHistogram>,
    },

    /// Relative deltas from commits only. Checkpoint file actions were not read during
    /// snapshot load. Recovery: read checkpoint adds via checkpoint add scanner, merge with
    /// these deltas.
    RequiresCheckpointRead {
        /// Net file count change from commits (NOT an absolute count).
        commit_delta_files: i64,
        /// Net byte change from commits (NOT an absolute byte total).
        commit_delta_bytes: i64,
        /// Delta histogram from commits (adds inserted, removes removed). Merge with the
        /// checkpoint baseline histogram during recovery to get the complete histogram.
        /// `None` when the replay did not produce histogram data.
        commit_delta_histogram: Option<FileSizeHistogram>,
    },

    /// A non-incremental operation (like ANALYZE STATS) was encountered during replay. File
    /// stats cannot be determined incrementally because files may have been re-added without
    /// corresponding removes. Recovery: full add/remove deduplication via
    /// `CrcProcessor(FullReconciliation)`.
    Indeterminate,

    /// A remove action had a missing `size` field. Byte-level file stats are permanently
    /// unrecoverable. No amount of replay can recover the missing data.
    Untrackable,
}

#[allow(dead_code)] // Methods used in follow-up steps (Snapshot, CRC write).
impl FileStatsState {
    /// Returns absolute file stats if available.
    ///
    /// Returns `Some` for `Valid` (with or without histogram). Returns `None` for
    /// `RequiresCheckpointRead`, `Indeterminate`, and `Untrackable`.
    pub fn file_stats(&self) -> Option<FileStats> {
        match self {
            Self::Valid {
                num_files,
                table_size_bytes,
                histogram,
            } => Some(FileStats {
                num_files: *num_files,
                table_size_bytes: *table_size_bytes,
                file_size_histogram: histogram.clone(),
            }),
            _ => None,
        }
    }

    /// Returns true if a CRC can be written to disk in this state. Only `Valid` is safe
    /// to write. The CRC will include the histogram if present, or omit it if `None`.
    pub fn is_writable(&self) -> bool {
        matches!(self, Self::Valid { .. })
    }

    /// Returns the simplified validity classification for cost inspection by connectors.
    ///
    /// Connectors use this to decide whether to write CRC synchronously, asynchronously,
    /// or call `load_file_stats()` first.
    pub fn validity(&self) -> FileStatsValidity {
        match self {
            Self::Valid { .. } => FileStatsValidity::Valid,
            Self::RequiresCheckpointRead { .. } => FileStatsValidity::RequiresCheckpointRead,
            Self::Indeterminate => FileStatsValidity::Indeterminate,
            Self::Untrackable => FileStatsValidity::Untrackable,
        }
    }
}

impl Default for FileStatsState {
    /// Default is `Valid` with zero counts and no histogram. This is the correct initial
    /// state for a brand new table (CREATE TABLE) before any files are added.
    fn default() -> Self {
        Self::Valid {
            num_files: 0,
            table_size_bytes: 0,
            histogram: None,
        }
    }
}

/// Simplified validity classification for cost inspection. Connectors use this to decide
/// whether to write CRC synchronously, asynchronously, or call `load_file_stats()` first.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileStatsValidity {
    /// Free: file stats are complete. Just call `write_checksum()`.
    #[default]
    Valid,
    /// Medium: need to read checkpoint file actions. Call `load_file_stats()`.
    RequiresCheckpointRead,
    /// Expensive: need full add/remove deduplication. Call `load_file_stats()`.
    Indeterminate,
    /// Impossible: missing remove.size, permanently unrecoverable.
    Untrackable,
}

// ============================================================================
// Domain metadata state
// ============================================================================

/// The completeness state of domain metadata in a CRC. Owns the underlying HashMap.
///
/// Serde behavior for CRC JSON files:
/// - `Complete(map)` serializes as `[...]` (array of DM entries)
/// - `Partial` and `Untracked` both serialize as `null` (partial data is not persisted)
/// - Deserialization from `[...]` produces `Complete(map)`, from `null`/absent produces
///   `Untracked` (CRC files on disk always have complete data or no data)
#[allow(dead_code)] // Variants used in follow-up steps (Snapshot DM fast path).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DomainMetadataState {
    /// All domain metadata is known. The map is the complete set of active (non-removed)
    /// domains. If a domain is not in the map, it does not exist at this table version.
    Complete(HashMap<String, DomainMetadata>),

    /// We have some domain metadata from the replay range (commits after the base CRC), but
    /// the base CRC lacked a domain metadata field. Entries in the map are guaranteed correct
    /// (they come from newer commits). Querying a domain NOT in the map requires log replay
    /// to determine if it exists in older commits.
    Partial(HashMap<String, DomainMetadata>),

    /// Domain metadata is not tracked. The CRC JSON field was absent, and no domain metadata
    /// was found during replay. All queries require log replay.
    #[default]
    Untracked,
}

#[allow(dead_code)]
impl DomainMetadataState {
    /// Returns the map if available (Complete or Partial), `None` if Untracked.
    pub fn map(&self) -> Option<&HashMap<String, DomainMetadata>> {
        match self {
            Self::Complete(map) | Self::Partial(map) => Some(map),
            Self::Untracked => None,
        }
    }

    /// Returns true if the map is exhaustive (no log replay needed for misses).
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }
}

// ============================================================================
// Set transaction state
// ============================================================================

/// The completeness state of set transactions in a CRC. Owns the underlying HashMap.
///
/// Same serde and completeness semantics as [`DomainMetadataState`].
#[allow(dead_code)] // Variants used in follow-up steps (Snapshot txn fast path).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SetTransactionState {
    /// All set transactions are known. If an app_id is not in the map, no transaction
    /// exists for it at this table version.
    Complete(HashMap<String, SetTransaction>),

    /// We have some set transactions from the replay range, but the base CRC lacked a
    /// set transactions field. Entries are guaranteed correct. Querying an app_id NOT in
    /// the map requires log replay.
    Partial(HashMap<String, SetTransaction>),

    /// Set transactions are not tracked. All queries require log replay.
    #[default]
    Untracked,
}

#[allow(dead_code)]
impl SetTransactionState {
    /// Returns the map if available (Complete or Partial), `None` if Untracked.
    pub fn map(&self) -> Option<&HashMap<String, SetTransaction>> {
        match self {
            Self::Complete(map) | Self::Partial(map) => Some(map),
            Self::Untracked => None,
        }
    }

    /// Returns true if the map is exhaustive (no log replay needed for misses).
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== FileStatsState tests =====

    #[test]
    fn test_valid_with_histogram_returns_file_stats() {
        let histogram = FileSizeHistogram::create_default();
        let state = FileStatsState::Valid {
            num_files: 10,
            table_size_bytes: 5000,
            histogram: Some(histogram),
        };
        let stats = state.file_stats().unwrap();
        assert_eq!(stats.num_files, 10);
        assert_eq!(stats.table_size_bytes, 5000);
        assert!(stats.file_size_histogram.is_some());
    }

    #[test]
    fn test_valid_without_histogram_returns_file_stats_with_no_histogram() {
        let state = FileStatsState::Valid {
            num_files: 10,
            table_size_bytes: 5000,
            histogram: None,
        };
        let stats = state.file_stats().unwrap();
        assert_eq!(stats.num_files, 10);
        assert_eq!(stats.table_size_bytes, 5000);
        assert!(stats.file_size_histogram.is_none());
    }

    #[test]
    fn test_requires_checkpoint_read_returns_none_for_file_stats() {
        let state = FileStatsState::RequiresCheckpointRead {
            commit_delta_files: 5,
            commit_delta_bytes: 2000,
            commit_delta_histogram: None,
        };
        assert!(state.file_stats().is_none());
    }

    #[test]
    fn test_indeterminate_returns_none_for_file_stats() {
        assert!(FileStatsState::Indeterminate.file_stats().is_none());
    }

    #[test]
    fn test_untrackable_returns_none_for_file_stats() {
        assert!(FileStatsState::Untrackable.file_stats().is_none());
    }

    #[test]
    fn test_only_valid_is_writable() {
        assert!(FileStatsState::Valid {
            num_files: 0,
            table_size_bytes: 0,
            histogram: None,
        }
        .is_writable());

        assert!(!FileStatsState::RequiresCheckpointRead {
            commit_delta_files: 0,
            commit_delta_bytes: 0,
            commit_delta_histogram: None,
        }
        .is_writable());

        assert!(!FileStatsState::Indeterminate.is_writable());
        assert!(!FileStatsState::Untrackable.is_writable());
    }

    #[test]
    fn test_validity_classification_matches_variant() {
        assert_eq!(
            FileStatsState::Valid {
                num_files: 0,
                table_size_bytes: 0,
                histogram: None,
            }
            .validity(),
            FileStatsValidity::Valid
        );
        assert_eq!(
            FileStatsState::RequiresCheckpointRead {
                commit_delta_files: 0,
                commit_delta_bytes: 0,
                commit_delta_histogram: None,
            }
            .validity(),
            FileStatsValidity::RequiresCheckpointRead
        );
        assert_eq!(
            FileStatsState::Indeterminate.validity(),
            FileStatsValidity::Indeterminate
        );
        assert_eq!(
            FileStatsState::Untrackable.validity(),
            FileStatsValidity::Untrackable
        );
    }

    #[test]
    fn test_default_is_valid_with_zero_counts_and_no_histogram() {
        let state = FileStatsState::default();
        assert!(matches!(
            state,
            FileStatsState::Valid {
                num_files: 0,
                table_size_bytes: 0,
                histogram: None,
            }
        ));
    }
}
