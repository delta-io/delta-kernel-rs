//! State enums for CRC tracking: file stats, domain metadata, and set transactions.
//!
//! Each enum encodes both *what we know* and *how to recover what we don't*.
//!
//! - [`FileStatsState`] tracks the validity AND values of file statistics. The variant carries
//!   exactly the data that variant requires: `Valid` carries `num_files`/`table_size_bytes`,
//!   `RequiresCheckpointRead` carries `commit_delta_files`/`commit_delta_bytes`, etc. You cannot
//!   accidentally read an absolute file count from a delta because they live in different variants
//!   under different field names.
//! - [`DomainMetadataState`] / [`SetTransactionState`] track the *completeness* of cached domain
//!   metadata and set transactions. `Complete` is authoritative for misses (a domain not in the map
//!   does not exist at this version). `Partial` means the map has known-correct entries from newer
//!   commits but the older base did not track them, so a miss requires log replay. `Untracked`
//!   means we have nothing -- every query requires log replay.

use std::collections::HashMap;

use super::file_size_histogram::FileSizeHistogram;
use super::file_stats::FileStats;
use crate::actions::{DomainMetadata, SetTransaction};

// ============================================================================
// File stats state
// ============================================================================

/// The state of file statistics for a CRC. Encodes both what we know and how to recover what
/// we don't.
///
/// Each variant carries exactly the data that makes sense for that state. You cannot
/// accidentally use a commit delta as an absolute file count because they live in different
/// variants with different field names.
///
/// State transitions during [`Crc::apply`](super::Crc::apply):
///
/// | Current                | + safe op | + unsafe op   | + missing remove.size |
/// |------------------------|-----------|---------------|-----------------------|
/// | Valid                  | Valid     | Indeterminate | Untrackable           |
/// | RequiresCheckpointRead | RCR       | Indeterminate | Untrackable           |
/// | Indeterminate          | Indet.    | Indet.        | Untrackable           |
/// | Untrackable            | Untrack.  | Untrack.      | Untrackable           |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileStatsState {
    /// File stats are known-correct absolute totals. Histogram is optional: `Some` when the
    /// base CRC had a histogram (or full replay produced one), `None` when the base CRC lacked
    /// a histogram. Safe to write CRC to disk (with or without histogram).
    Valid {
        num_files: i64,
        table_size_bytes: i64,
        histogram: Option<FileSizeHistogram>,
    },

    /// Relative deltas from commits only. Checkpoint file actions were not read during snapshot
    /// load. Recovery: read checkpoint adds via a checkpoint add scanner, merge with these
    /// deltas. Currently unreachable in normal load paths -- reserved for future
    /// connector-hint and time-travel optimisations.
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
    /// corresponding removes. Recovery: full add/remove deduplication (deferred --
    /// [`Snapshot::load_file_stats`](crate::snapshot::Snapshot::load_file_stats) stub).
    Indeterminate,

    /// A remove action had a missing `size` field. Byte-level file stats are permanently
    /// unrecoverable -- no amount of replay can recover the missing data.
    Untrackable,
}

impl FileStatsState {
    /// Returns absolute file stats if available.
    ///
    /// Returns `Some` only for [`Valid`](Self::Valid) (with or without histogram). Returns
    /// `None` for all other variants.
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

    /// Returns true if a CRC can be written to disk in this state. Only [`Valid`](Self::Valid)
    /// is safe to write. The CRC will include the histogram if present, or omit it if `None`.
    pub fn is_writable(&self) -> bool {
        matches!(self, Self::Valid { .. })
    }

    /// Returns the simplified validity classification for cost inspection by connectors.
    ///
    /// Connectors use this to decide whether to write CRC synchronously, asynchronously, or
    /// call [`Snapshot::load_file_stats`](crate::snapshot::Snapshot::load_file_stats) first.
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
    /// Default is `Valid` with zero counts and no histogram. This is the correct initial state
    /// for a brand new table (CREATE TABLE) before any files are added.
    fn default() -> Self {
        Self::Valid {
            num_files: 0,
            table_size_bytes: 0,
            histogram: None,
        }
    }
}

/// Simplified validity classification of file stats for cost inspection. Connectors use this
/// to decide whether to write CRC synchronously, asynchronously, or recover stats first.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileStatsValidity {
    /// Free: file stats are complete. Just call
    /// [`Snapshot::write_checksum`](crate::snapshot::Snapshot::write_checksum).
    #[default]
    Valid,
    /// Medium: need to read checkpoint file actions before writing CRC.
    RequiresCheckpointRead,
    /// Expensive: need full add/remove deduplication before writing CRC.
    Indeterminate,
    /// Impossible: missing `remove.size`, permanently unrecoverable.
    Untrackable,
}

// ============================================================================
// Domain metadata state
// ============================================================================

/// The completeness state of domain metadata in a CRC. Owns the underlying [`HashMap`].
///
/// Serde behaviour for CRC JSON files (handled at the [`Crc`](super::Crc) struct level):
/// - `Complete(map)` serialises as `[...]` (array of DM entries)
/// - `Partial` and `Untracked` both serialise as the field being absent (writers can only write
///   `Complete` -- enforced in [`try_write_crc_file`](super::try_write_crc_file) indirectly via
///   [`FileStatsState::is_writable`])
/// - Deserialisation from `[...]` produces `Complete(map)`, from `null`/absent produces `Untracked`
///   (CRC files on disk always have complete data or no data)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DomainMetadataState {
    /// All domain metadata is known. The map is the complete set of active (non-removed)
    /// domains. If a domain is not in the map, it does not exist at this table version.
    Complete(HashMap<String, DomainMetadata>),

    /// We have some domain metadata from the replay range (commits after the base CRC), but
    /// the base CRC lacked a domain metadata field. Entries in the map are guaranteed correct
    /// (they come from newer commits, which take priority). Querying a domain NOT in the map
    /// requires log replay to determine if it exists in older commits.
    Partial(HashMap<String, DomainMetadata>),

    /// Domain metadata is not tracked. The CRC JSON field was absent, and no domain metadata
    /// was found during replay. All queries require log replay.
    #[default]
    Untracked,
}

impl DomainMetadataState {
    /// Returns the map if available (`Complete` or `Partial`), `None` if `Untracked`.
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

/// The completeness state of set transactions in a CRC. Owns the underlying [`HashMap`].
///
/// Same serde and completeness semantics as [`DomainMetadataState`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SetTransactionState {
    /// All set transactions are known. If an `app_id` is not in the map, no transaction
    /// exists for it at this table version.
    Complete(HashMap<String, SetTransaction>),

    /// We have some set transactions from the replay range, but the base CRC lacked a set
    /// transactions field. Entries are guaranteed correct (newest commits take priority).
    /// Querying an `app_id` NOT in the map requires log replay.
    Partial(HashMap<String, SetTransaction>),

    /// Set transactions are not tracked. All queries require log replay.
    #[default]
    Untracked,
}

impl SetTransactionState {
    /// Returns the map if available (`Complete` or `Partial`), `None` if `Untracked`.
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ===== FileStatsState =====

    #[test]
    fn test_valid_with_histogram_returns_file_stats() {
        let state = FileStatsState::Valid {
            num_files: 10,
            table_size_bytes: 5000,
            histogram: Some(FileSizeHistogram::create_default()),
        };
        let stats = state.file_stats().unwrap();
        assert_eq!(stats.num_files(), 10);
        assert_eq!(stats.table_size_bytes(), 5000);
        assert!(stats.file_size_histogram().is_some());
    }

    #[test]
    fn test_valid_without_histogram_returns_file_stats_with_no_histogram() {
        let state = FileStatsState::Valid {
            num_files: 10,
            table_size_bytes: 5000,
            histogram: None,
        };
        let stats = state.file_stats().unwrap();
        assert_eq!(stats.num_files(), 10);
        assert_eq!(stats.table_size_bytes(), 5000);
        assert!(stats.file_size_histogram().is_none());
    }

    #[test]
    fn test_non_valid_states_return_none_for_file_stats_and_are_not_writable() {
        let states = [
            FileStatsState::RequiresCheckpointRead {
                commit_delta_files: 5,
                commit_delta_bytes: 2000,
                commit_delta_histogram: None,
            },
            FileStatsState::Indeterminate,
            FileStatsState::Untrackable,
        ];
        for state in states {
            assert!(state.file_stats().is_none(), "{state:?}");
            assert!(!state.is_writable(), "{state:?}");
        }
    }

    #[test]
    fn test_valid_is_writable() {
        assert!(FileStatsState::default().is_writable());
    }

    #[test]
    fn test_validity_classification_matches_variant() {
        assert_eq!(
            FileStatsState::default().validity(),
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
    fn test_default_is_valid_zero() {
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

    // ===== DomainMetadataState =====

    #[test]
    fn test_dm_state_map_accessor() {
        let map = HashMap::from([(
            "d".to_string(),
            DomainMetadata::new("d".to_string(), "{}".to_string()),
        )]);
        assert!(DomainMetadataState::Complete(map.clone()).map().is_some());
        assert!(DomainMetadataState::Partial(map).map().is_some());
        assert!(DomainMetadataState::Untracked.map().is_none());
    }

    #[test]
    fn test_dm_state_is_complete() {
        assert!(DomainMetadataState::Complete(HashMap::new()).is_complete());
        assert!(!DomainMetadataState::Partial(HashMap::new()).is_complete());
        assert!(!DomainMetadataState::Untracked.is_complete());
    }

    // ===== SetTransactionState =====

    #[test]
    fn test_txn_state_map_accessor() {
        let map = HashMap::from([(
            "app".to_string(),
            SetTransaction::new("app".to_string(), 1, None),
        )]);
        assert!(SetTransactionState::Complete(map.clone()).map().is_some());
        assert!(SetTransactionState::Partial(map).map().is_some());
        assert!(SetTransactionState::Untracked.map().is_none());
    }

    #[test]
    fn test_txn_state_is_complete() {
        assert!(SetTransactionState::Complete(HashMap::new()).is_complete());
        assert!(!SetTransactionState::Partial(HashMap::new()).is_complete());
        assert!(!SetTransactionState::Untracked.is_complete());
    }
}
