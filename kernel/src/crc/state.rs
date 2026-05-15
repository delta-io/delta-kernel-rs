//! Typed state enums for CRC tracking.
//!
//! Each enum encodes both *what we know* about a slice of CRC state and *the trustworthiness*
//! of those values. Variants carry data exactly when it makes sense for that state, making
//! invalid reads unrepresentable: the compiler prevents reading an absolute count from a
//! degraded state because the field does not exist there.
//!
//! - [`FileStatsState`] tracks file-stat validity (Complete / Indeterminate / Untrackable).
//! - [`DomainMetadataState`] tracks domain-metadata completeness (Complete / Partial).
//!
//! TODO: introduce `SetTransactionState` with the same shape as `DomainMetadataState`.

use std::collections::HashMap;

use super::file_stats::FileStats;
use crate::actions::DomainMetadata;

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

// ============================================================================
// Domain metadata state
// ============================================================================

/// The completeness state of cached domain metadata in a CRC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainMetadataState {
    /// The CRC file had the `domainMetadata` field (possibly as an empty array). The map
    /// is the full set of active (non-removed) domains at this version; a domain not in the
    /// map does not exist.
    Complete(HashMap<String, DomainMetadata>),

    /// The CRC file did not have the `domainMetadata` field. The map starts empty and gets
    /// populated by incremental CRC replay over the JSON commits after the latest stale CRC.
    /// Hits are authoritative (definitely present at this version); misses are NOT
    /// (the domain may exist in older commits), so callers must do a further log scan.
    Partial(HashMap<String, DomainMetadata>),
}

impl Default for DomainMetadataState {
    fn default() -> Self {
        Self::Partial(HashMap::new())
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(clippy::panic)]
impl DomainMetadataState {
    /// Test-only: returns the map if `Complete`, panics otherwise.
    pub fn expect_complete(&self) -> &HashMap<String, DomainMetadata> {
        match self {
            Self::Complete(map) => map,
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    /// Test-only: returns the map if `Partial`, panics otherwise.
    pub fn expect_partial(&self) -> &HashMap<String, DomainMetadata> {
        match self {
            Self::Partial(map) => map,
            other => panic!("expected Partial, got {other:?}"),
        }
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

    // ===== DomainMetadataState =====

    #[test]
    fn dm_state_default_is_empty_partial() {
        assert_eq!(
            DomainMetadataState::default(),
            DomainMetadataState::Partial(HashMap::new())
        );
    }
}
