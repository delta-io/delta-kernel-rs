//! State enums for CRC tracking: file stats validity, domain metadata, and set transactions.
//!
//! [`DomainMetadataState`] and [`SetTransactionState`] encode the completeness of cached
//! domain metadata and set transactions. No artificial bounding. Partial state arises
//! naturally when the base CRC lacks these fields but the replay range found some entries.

use std::collections::HashMap;

use crate::actions::{DomainMetadata, SetTransaction};

// ============================================================================
// File stats validity
// ============================================================================

/// Tracks whether file stats (`num_files`, `table_size_bytes`) are trustworthy.
///
/// Defaults to [`Valid`](Self::Valid), which is the correct state when deserializing a CRC file
/// from disk (a CRC file's stats are correct by definition).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileStatsValidity {
    /// File stats are known-correct absolute totals. Safe to write to disk.
    #[default]
    Valid,
    /// File stats are relative deltas, not absolute totals. Checkpoint file actions were not
    /// read during snapshot load. Recovery: read checkpoint adds, merge with deltas.
    RequiresCheckpointRead,
    /// A non-incremental operation was seen: file stats cannot be determined incrementally.
    /// Recovery: full add/remove deduplication via log replay.
    Indeterminate,
    /// A file action had a missing size field: correct file stats are impossible to compute.
    /// No amount of replay can recover the missing data.
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
