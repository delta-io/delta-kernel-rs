//! Incremental CRC state updates via commit deltas.
//!
//! A [`CrcDelta`] captures CRC-relevant changes from a single commit (produced by reading a
//! `.json` commit file during log replay, or from in-memory transaction state during writes).
//! [`Crc::apply`] advances a CRC forward one commit at a time by applying a delta.
//!
//! A CRC tracks two categories of fields, updated differently:
//! - **Metadata fields** (protocol, metadata, domain metadata, set transactions, in-commit
//!   timestamp): always kept up-to-date -- every `apply` unconditionally merges these from the
//!   delta.
//! - **File stats** ([`FileStatsState`]): only updated when the state is `Complete` and the
//!   commit's operation is incremental-safe. Once the state degrades (e.g. a non-incremental
//!   operation like ANALYZE STATS, or a missing file size), file stats stop updating for the
//!   lifetime of that CRC.

use tracing::warn;

use super::file_stats::FileStatsDelta;
use super::{Crc, DomainMetadataState, FileSizeHistogram, FileStats, FileStatsState};
use crate::actions::{DomainMetadata, Metadata, Protocol, SetTransaction};

/// The CRC-relevant changes ("delta") from a single commit. Produced either by reading a
/// `.json` commit file during log replay, or from in-memory transaction state during writes.
#[derive(Debug, Clone, Default)]
pub(crate) struct CrcDelta {
    /// Net file count, size changes and histograms.
    pub(crate) file_stats: FileStatsDelta,
    /// New protocol action, if this commit changed it.
    pub(crate) protocol: Option<Protocol>,
    /// New metadata action, if this commit changed it.
    pub(crate) metadata: Option<Metadata>,
    /// All DM actions in this commit (additions and removals). `apply()` processes these
    /// unconditionally; the base CRC's `DomainMetadataState` variant (`Complete` or
    /// `Partial`) is preserved.
    pub(crate) domain_metadata_changes: Vec<DomainMetadata>,
    /// All SetTransaction actions in this commit. `apply()` only processes these when the base
    /// CRC's `set_transactions` is `Some` (tracked).
    pub(crate) set_transaction_changes: Vec<SetTransaction>,
    /// In-commit timestamp, if present in this commit.
    pub(crate) in_commit_timestamp: Option<i64>,
    /// Must be `Some` with an incremental-safe value for file stats to update. `None` or
    /// unrecognized values transition the [`FileStatsState`] to `Indeterminate`.
    pub(crate) operation: Option<String>,
    /// A file action in this commit had a missing `size` field, making byte-level file stats
    /// impossible to compute.
    pub(crate) has_missing_file_size: bool,
}

impl CrcDelta {
    /// Convert this delta into a fresh [`Crc`]. Used when the delta represents the entire table
    /// state (e.g. CREATE TABLE or the first commit in a forward replay from version zero).
    ///
    /// Returns `None` if protocol or metadata are missing (both are required for a valid CRC).
    pub(crate) fn into_crc_for_version_zero(self) -> Option<Crc> {
        let protocol = self.protocol?;
        let metadata = self.metadata?;
        // For CREATE TABLE we know the full domain metadata state: the transaction either
        // included domain metadata actions or it didn't. Always Complete.
        let domain_metadata_state = DomainMetadataState::Complete(
            self.domain_metadata_changes
                .into_iter()
                .filter(|dm| !dm.is_removed())
                .map(|dm| (dm.domain().to_string(), dm))
                .collect(),
        );
        // CREATE TABLE starts with a known-complete set of transactions (possibly empty),
        // so we always track them.
        let set_transactions = Some(
            self.set_transaction_changes
                .into_iter()
                .map(|txn| (txn.app_id.clone(), txn))
                .collect(),
        );
        // For version zero the delta IS the full table histogram. Validate that all bins
        // are non-negative (a real table can't have negative file counts). If validation
        // fails, drop the histogram.
        let initial_histogram = self.file_stats.net_histogram.and_then(|delta| {
            delta
                .check_non_negative()
                .inspect_err(|e| {
                    warn!("Non-negative file count check failed, dropping file size histogram for version zero: {e}");
                })
                .ok()
        });
        Some(Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: self.file_stats.net_files,
                table_size_bytes: self.file_stats.net_bytes,
                file_size_histogram: initial_histogram,
            }),
            protocol,
            metadata,
            domain_metadata_state,
            set_transactions,
            in_commit_timestamp_opt: self.in_commit_timestamp,
            ..Default::default()
        })
    }
}

/// Commit delta application for [`Crc`]. See the [module-level docs](self) for details.
impl Crc {
    /// Apply a commit delta. Protocol, metadata, ICT, and domain metadata update
    /// unconditionally (DM preserves its `Complete`/`Partial` variant); set transactions
    /// update only when already tracked (`Some`); file stats follow the [`FileStatsState`]
    /// state machine.
    pub(crate) fn apply(&mut self, delta: CrcDelta) {
        // Protocol and metadata: replace if present.
        if let Some(p) = delta.protocol {
            self.protocol = p;
        }
        if let Some(m) = delta.metadata {
            self.metadata = m;
        }

        // Apply the delta onto the CRC's existing map: upsert each non-removed entry
        // (newest wins), drop each tombstone. The variant (Complete or Partial) stays the
        // same since a delta never changes whether the base was authoritative.
        let map = match &mut self.domain_metadata_state {
            DomainMetadataState::Complete(m) | DomainMetadataState::Partial(m) => m,
        };
        for dm in delta.domain_metadata_changes {
            if dm.is_removed() {
                map.remove(dm.domain());
            } else {
                map.insert(dm.domain().to_string(), dm);
            }
        }

        // Set transactions: upsert by app_id. Only update if the base CRC tracks set
        // transactions (Some). If None ("not tracked"), leave it as None.
        if let Some(map) = &mut self.set_transactions {
            map.extend(
                delta
                    .set_transaction_changes
                    .into_iter()
                    .map(|txn| (txn.app_id.clone(), txn)),
            );
        }

        // In-commit timestamp: unconditional replace (not guarded by `if let Some`).
        // If ICT was disabled after being enabled, the delta carries None, which correctly
        // clears the previous value.
        self.in_commit_timestamp_opt = delta.in_commit_timestamp;

        let is_incremental_safe = delta
            .operation
            .as_deref()
            .is_some_and(FileStatsDelta::is_incremental_safe);
        self.file_stats_state = transition_file_stats(
            &self.file_stats_state,
            &delta.file_stats,
            is_incremental_safe,
            delta.has_missing_file_size,
        );
    }
}

/// Compute the next [`FileStatsState`] given the current state and an incoming delta.
///
/// See [`FileStatsState`] for the full transition table.
fn transition_file_stats(
    current: &FileStatsState,
    delta: &FileStatsDelta,
    is_incremental_safe: bool,
    has_missing_file_size: bool,
) -> FileStatsState {
    match current {
        // Indeterminate is terminal in incremental replay. A future full add/remove dedup
        // pass could recover Complete.
        FileStatsState::Indeterminate => FileStatsState::Indeterminate,
        // Either a non-incremental op (e.g. ANALYZE STATS) or a missing remove.size makes
        // incremental tracking impossible: a full add/remove reconciliation can recover.
        _ if !is_incremental_safe || has_missing_file_size => FileStatsState::Indeterminate,
        FileStatsState::Complete(stats) => FileStatsState::Complete(FileStats {
            // Counts and bytes have no non-negative check.
            num_files: stats.num_files + delta.net_files,
            table_size_bytes: stats.table_size_bytes + delta.net_bytes,
            // Histogram: per-bin merge; drop on failure. See `merge_histogram`.
            file_size_histogram: merge_histogram(
                stats.file_size_histogram.as_ref(),
                delta.net_histogram.as_ref(),
            ),
        }),
    }
}

/// Merge a base histogram with a delta histogram. Returns `Some(merged)` only when both are
/// present and [`FileSizeHistogram::try_apply_delta`] succeeds; otherwise returns `None`.
/// `try_apply_delta` fails on boundary mismatch or negative per-bin sums.
fn merge_histogram(
    base: Option<&FileSizeHistogram>,
    delta: Option<&FileSizeHistogram>,
) -> Option<FileSizeHistogram> {
    match (base, delta) {
        (Some(base_hist), Some(delta_hist)) => match base_hist.try_apply_delta(delta_hist) {
            Ok(merged) => Some(merged),
            Err(e) => {
                warn!("Histogram merge failed, dropping file size histogram: {e}");
                None
            }
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;
    use crate::actions::{DomainMetadata, Metadata, Protocol};
    use crate::crc::FileSizeHistogram;

    fn base_crc() -> Crc {
        Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 10,
                table_size_bytes: 1000,
                file_size_histogram: None,
            }),
            ..Default::default()
        }
    }

    fn seed_dm_map() -> HashMap<String, DomainMetadata> {
        HashMap::from([
            (
                "keep".to_string(),
                DomainMetadata::new("keep".to_string(), "old".to_string()),
            ),
            (
                "drop".to_string(),
                DomainMetadata::new("drop".to_string(), "x".to_string()),
            ),
        ])
    }

    fn write_delta(net_files: i64, net_bytes: i64) -> CrcDelta {
        CrcDelta {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
                ..Default::default()
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        }
    }

    // ===== is_incremental_safe tests =====

    #[test]
    fn test_incremental_safe_operations() {
        for op in [
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
        ] {
            assert!(
                FileStatsDelta::is_incremental_safe(op),
                "{op} should be incremental-safe"
            );
        }
    }

    #[test]
    fn test_non_incremental_safe_operations() {
        assert!(!FileStatsDelta::is_incremental_safe("ANALYZE STATS"));
        assert!(!FileStatsDelta::is_incremental_safe("UNKNOWN"));
    }

    // ===== Crc deserialized from CRC file (default state) =====

    #[test]
    fn test_deserialized_crc_has_complete_stats() {
        let crc = base_crc();
        let stats = crc.file_stats().unwrap();
        assert!(crc.file_stats_state.is_complete());
        assert_eq!(stats.num_files(), 10);
        assert_eq!(stats.table_size_bytes(), 1000);
    }

    // ===== Crc::apply tests =====

    #[test]
    fn test_apply_updates_file_stats() {
        let mut crc = base_crc();
        crc.apply(write_delta(3, 600));
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files(), 13); // 10 + 3
        assert_eq!(stats.table_size_bytes(), 1600); // 1000 + 600
        assert!(crc.file_stats_state.is_complete());
    }

    /// Applies multiple commit deltas sequentially.
    #[test]
    fn test_apply_multiple_deltas() {
        let mut crc = base_crc();
        crc.apply(write_delta(3, 600));
        crc.apply(write_delta(-2, -400));
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files(), 11); // 10 + 3 - 2
        assert_eq!(stats.table_size_bytes(), 1200); // 1000 + 600 - 400
        assert!(crc.file_stats_state.is_complete());
    }

    #[test]
    fn test_apply_unsafe_op_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_change);
        assert!(crc.file_stats_state.is_indeterminate());
    }

    #[test]
    fn test_apply_none_op_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let unknown_delta = CrcDelta {
            operation: None,
            ..write_delta(1, 100)
        };
        crc.apply(unknown_delta);
        assert!(crc.file_stats_state.is_indeterminate());
    }

    #[test]
    fn test_indeterminate_stays_indeterminate() {
        let mut crc = base_crc();
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_change);
        assert!(crc.file_stats_state.is_indeterminate());

        // Subsequent safe op doesn't recover the state.
        crc.apply(write_delta(5, 500));
        assert!(crc.file_stats_state.is_indeterminate());
    }

    // ===== apply: missing-file-size tests =====

    #[test]
    fn test_missing_file_size_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        crc.apply(delta);
        assert!(crc.file_stats_state.is_indeterminate());
    }

    // ===== apply: non-file-stats field updates =====

    #[test]
    fn test_apply_replaces_protocol() {
        let mut crc = base_crc();
        let new_protocol = Protocol::try_new(
            2,
            5,
            None::<Vec<crate::table_features::TableFeature>>,
            None::<Vec<crate::table_features::TableFeature>>,
        )
        .unwrap();
        let delta = CrcDelta {
            protocol: Some(new_protocol.clone()),
            ..write_delta(0, 0)
        };
        crc.apply(delta);
        assert_eq!(crc.protocol, new_protocol);
        assert_eq!(crc.metadata, Metadata::default()); // unchanged
    }

    #[test]
    fn test_apply_adds_domain_metadata_to_complete_map() {
        let mut crc = base_crc();
        crc.domain_metadata_state = DomainMetadataState::Complete(HashMap::new());
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let DomainMetadataState::Complete(map) = &crc.domain_metadata_state else {
            panic!("expected Complete, got {:?}", crc.domain_metadata_state);
        };
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
    }

    /// `apply` never upgrades `Partial` to `Complete`: the variant only changes when the
    /// caller has full knowledge, which a single delta does not provide.
    #[test]
    fn test_apply_does_not_upgrade_partial_to_complete() {
        let mut crc = base_crc();
        assert_eq!(crc.domain_metadata_state, DomainMetadataState::default());
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let DomainMetadataState::Partial(map) = &crc.domain_metadata_state else {
            panic!("expected Partial, got {:?}", crc.domain_metadata_state);
        };
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
    }

    #[test]
    fn test_apply_upserts_domain_metadata_in_complete_map() {
        let mut crc = base_crc();
        crc.domain_metadata_state = DomainMetadataState::Complete(HashMap::from([(
            "my.domain".to_string(),
            DomainMetadata::new("my.domain".to_string(), "old_config".to_string()),
        )]));

        let dm = DomainMetadata::new("my.domain".to_string(), "new_config".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let DomainMetadataState::Complete(map) = &crc.domain_metadata_state else {
            panic!("expected Complete, got {:?}", crc.domain_metadata_state);
        };
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "new_config");
    }

    #[test]
    fn test_apply_upserts_domain_metadata_in_partial_map_stays_partial() {
        let mut crc = base_crc();
        crc.domain_metadata_state = DomainMetadataState::Partial(HashMap::from([(
            "my.domain".to_string(),
            DomainMetadata::new("my.domain".to_string(), "old_config".to_string()),
        )]));

        let dm = DomainMetadata::new("my.domain".to_string(), "new_config".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let DomainMetadataState::Partial(map) = &crc.domain_metadata_state else {
            panic!("expected Partial, got {:?}", crc.domain_metadata_state);
        };
        assert_eq!(map["my.domain"].configuration(), "new_config");
    }

    #[test]
    fn test_apply_removes_domain_metadata_from_complete_map() {
        let mut crc = base_crc();
        crc.domain_metadata_state = DomainMetadataState::Complete(HashMap::from([(
            "my.domain".to_string(),
            DomainMetadata::new("my.domain".to_string(), "config1".to_string()),
        )]));

        let dm = DomainMetadata::remove("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let DomainMetadataState::Complete(map) = &crc.domain_metadata_state else {
            panic!("expected Complete, got {:?}", crc.domain_metadata_state);
        };
        assert!(map.is_empty());
    }

    #[test]
    fn test_apply_removes_domain_metadata_from_partial_map_stays_partial() {
        let mut crc = base_crc();
        crc.domain_metadata_state = DomainMetadataState::Partial(HashMap::from([(
            "my.domain".to_string(),
            DomainMetadata::new("my.domain".to_string(), "config1".to_string()),
        )]));

        let dm = DomainMetadata::remove("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let DomainMetadataState::Partial(map) = &crc.domain_metadata_state else {
            panic!("expected Partial, got {:?}", crc.domain_metadata_state);
        };
        assert!(map.is_empty());
    }

    /// A single delta carrying both adds and tombstones is the realistic shape of a commit.
    /// Applied to either `Complete` or `Partial`, upserts and removals coexist correctly and
    /// the variant is preserved.
    #[rstest]
    #[case::complete(DomainMetadataState::Complete(seed_dm_map()))]
    #[case::partial(DomainMetadataState::Partial(seed_dm_map()))]
    fn test_apply_mixed_changes_upserts_and_drops_in_one_delta(
        #[case] base: DomainMetadataState,
    ) {
        let was_complete = matches!(base, DomainMetadataState::Complete(_));
        let mut crc = base_crc();
        crc.domain_metadata_state = base;
        let delta = CrcDelta {
            domain_metadata_changes: vec![
                DomainMetadata::new("keep".to_string(), "new".to_string()),
                DomainMetadata::new("add".to_string(), "y".to_string()),
                DomainMetadata::remove("drop".to_string(), "x".to_string()),
            ],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let map = match &crc.domain_metadata_state {
            DomainMetadataState::Complete(m) if was_complete => m,
            DomainMetadataState::Partial(m) if !was_complete => m,
            other => panic!("variant changed unexpectedly: {other:?}"),
        };
        assert_eq!(map.len(), 2);
        assert_eq!(map["keep"].configuration(), "new");
        assert_eq!(map["add"].configuration(), "y");
        assert!(!map.contains_key("drop"));
    }

    #[test]
    fn test_apply_replaces_in_commit_timestamp() {
        let mut crc = base_crc();
        let delta = CrcDelta {
            in_commit_timestamp: Some(9999),
            ..write_delta(0, 0)
        };
        crc.apply(delta);
        assert_eq!(crc.in_commit_timestamp_opt, Some(9999));
    }

    #[test]
    fn test_apply_clears_in_commit_timestamp_when_ict_disabled() {
        let mut crc = base_crc();
        crc.in_commit_timestamp_opt = Some(1000);

        // Delta without ICT (e.g. ICT was disabled) clears the previous value.
        let delta = CrcDelta {
            in_commit_timestamp: None,
            ..write_delta(0, 0)
        };
        crc.apply(delta);
        assert_eq!(crc.in_commit_timestamp_opt, None);
    }

    // ===== CrcDelta::into_crc_for_version_zero tests =====

    fn test_protocol() -> Protocol {
        Protocol::try_new(
            1,
            2,
            None::<Vec<crate::table_features::TableFeature>>,
            None::<Vec<crate::table_features::TableFeature>>,
        )
        .unwrap()
    }

    #[test]
    fn test_into_crc_for_version_zero_with_protocol_and_metadata() {
        let protocol = test_protocol();
        let metadata = Metadata::default();
        let delta = CrcDelta {
            protocol: Some(protocol.clone()),
            metadata: Some(metadata.clone()),
            ..write_delta(5, 1000)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let stats = crc.file_stats().unwrap();
        assert_eq!(crc.protocol, protocol);
        assert_eq!(crc.metadata, metadata);
        assert_eq!(stats.num_files(), 5);
        assert_eq!(stats.table_size_bytes(), 1000);
        assert!(crc.file_stats_state.is_complete());
        assert_eq!(
            crc.domain_metadata_state,
            DomainMetadataState::Complete(HashMap::new())
        );
        assert_eq!(crc.in_commit_timestamp_opt, None);
    }

    #[test]
    fn test_into_crc_for_version_zero_returns_none_without_protocol() {
        let delta = CrcDelta {
            metadata: Some(Metadata::default()),
            ..write_delta(5, 1000)
        };
        assert!(delta.into_crc_for_version_zero().is_none());
    }

    #[test]
    fn test_into_crc_for_version_zero_returns_none_without_metadata() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            ..write_delta(5, 1000)
        };
        assert!(delta.into_crc_for_version_zero().is_none());
    }

    #[test]
    fn test_into_crc_for_version_zero_with_domain_metadata() {
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let DomainMetadataState::Complete(map) = &crc.domain_metadata_state else {
            panic!("expected Complete, got {:?}", crc.domain_metadata_state);
        };
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
    }

    /// Defensive contract: even a v0 delta carrying tombstones (degenerate but possible)
    /// drops them before materializing the Complete map.
    #[test]
    fn test_into_crc_for_version_zero_drops_tombstones() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            domain_metadata_changes: vec![
                DomainMetadata::new("keep".to_string(), "x".to_string()),
                DomainMetadata::remove("ghost".to_string(), "y".to_string()),
            ],
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let DomainMetadataState::Complete(map) = &crc.domain_metadata_state else {
            panic!("expected Complete, got {:?}", crc.domain_metadata_state);
        };
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("keep"));
        assert!(!map.contains_key("ghost"));
    }

    #[test]
    fn test_into_crc_for_version_zero_with_in_commit_timestamp() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            in_commit_timestamp: Some(12345),
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        assert_eq!(crc.in_commit_timestamp_opt, Some(12345));
    }

    // ===== apply: set transaction tests =====

    #[test]
    fn test_apply_adds_set_transaction_to_tracked_map() {
        let mut crc = base_crc();
        crc.set_transactions = Some(HashMap::new());
        let txn = SetTransaction::new("my-app".to_string(), 1, Some(1000));
        let delta = CrcDelta {
            set_transaction_changes: vec![txn],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let map = crc.set_transactions.as_ref().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my-app"].version, 1);
        assert_eq!(map["my-app"].last_updated, Some(1000));
    }

    #[test]
    fn test_apply_with_untracked_set_transactions_skips_changes() {
        let mut crc = base_crc();
        assert!(crc.set_transactions.is_none()); // Not tracked (default)
        let txn = SetTransaction::new("my-app".to_string(), 1, Some(1000));
        let delta = CrcDelta {
            set_transaction_changes: vec![txn],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        // set_transactions stays None -- apply() must not create a partial map.
        assert!(crc.set_transactions.is_none());
    }

    #[test]
    fn test_apply_upserts_set_transaction() {
        let mut crc = base_crc();
        crc.set_transactions = Some(HashMap::from([(
            "my-app".to_string(),
            SetTransaction::new("my-app".to_string(), 1, Some(1000)),
        )]));

        let txn = SetTransaction::new("my-app".to_string(), 2, Some(2000));
        let delta = CrcDelta {
            set_transaction_changes: vec![txn],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let map = crc.set_transactions.as_ref().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my-app"].version, 2);
        assert_eq!(map["my-app"].last_updated, Some(2000));
    }

    // ===== into_crc_for_version_zero: set transaction tests =====

    #[test]
    fn test_into_crc_for_version_zero_with_set_transactions() {
        let txn = SetTransaction::new("my-app".to_string(), 5, Some(3000));
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            set_transaction_changes: vec![txn],
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let map = crc.set_transactions.as_ref().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my-app"].version, 5);
        assert_eq!(map["my-app"].last_updated, Some(3000));
    }

    #[test]
    fn test_into_crc_for_version_zero_with_no_set_transactions() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        // Empty map, not None -- we always know the full state at version zero.
        assert_eq!(crc.set_transactions, Some(HashMap::new()));
    }

    // ===== Histogram tests =====

    /// Helper: creates a default-boundary histogram populated with the given file sizes.
    fn histogram_from_sizes(sizes: &[i64]) -> FileSizeHistogram {
        let mut hist = FileSizeHistogram::create_default();
        for &size in sizes {
            hist.insert(size).unwrap();
        }
        hist
    }

    /// Helper: creates a CRC with a histogram containing the given file sizes.
    fn base_crc_with_histogram(file_sizes: &[i64]) -> Crc {
        let hist = histogram_from_sizes(file_sizes);
        Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: file_sizes.len() as i64,
                table_size_bytes: file_sizes.iter().sum(),
                file_size_histogram: Some(hist),
            }),
            ..Default::default()
        }
    }

    /// Helper: creates a CrcDelta with a delta histogram built from adds and removes.
    fn write_delta_with_histograms(add_sizes: &[i64], remove_sizes: &[i64]) -> CrcDelta {
        let mut hist = FileSizeHistogram::create_default();
        for &s in add_sizes {
            hist.insert(s).unwrap();
        }
        for &s in remove_sizes {
            hist.remove(s).unwrap();
        }
        let net_files = add_sizes.len() as i64 - remove_sizes.len() as i64;
        let net_bytes: i64 = add_sizes.iter().sum::<i64>() - remove_sizes.iter().sum::<i64>();
        CrcDelta {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
                net_histogram: Some(hist),
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        }
    }

    /// Histogram bins used in tests (default boundaries):
    ///   Bin 0: [0, 8KB)     -- e.g. 100, 200, 300, 500
    ///   Bin 1: [8KB, 16KB)  -- e.g. 10_000
    ///   Bin 2: [16KB, 32KB) -- e.g. 20_000
    ///   Bin 10: [4MB, 8MB)  -- e.g. 5_000_000
    #[rstest]
    #[case::single_bin(&[100, 200, 300], &[500], &[200], &[(0, 3, 900)])]
    #[case::adds_only(&[100], &[200, 300], &[], &[(0, 3, 600)])]
    #[case::removes_only(&[100, 200, 300], &[], &[100, 200], &[(0, 1, 300)])]
    #[case::empty_delta(&[100, 10_000], &[], &[], &[(0, 1, 100), (1, 1, 10_000)])]
    #[case::multi_bin(
        &[100, 10_000, 20_000],
        &[200, 10_500],
        &[100, 20_000],
        &[(0, 1, 200), (1, 2, 20_500), (2, 0, 0)]
    )]
    #[case::large_files(
        &[100, 5_000_000],
        &[10_000, 5_500_000],
        &[100],
        &[(0, 0, 0), (1, 1, 10_000), (10, 2, 10_500_000)]
    )]
    fn apply_merges_histogram(
        #[case] base: &[i64],
        #[case] add: &[i64],
        #[case] remove: &[i64],
        #[case] expected_bins: &[(usize, i64, i64)],
    ) {
        let mut crc = base_crc_with_histogram(base);
        let delta = write_delta_with_histograms(add, remove);
        crc.apply(delta);

        let stats = crc.file_stats().unwrap();
        let hist = stats.file_size_histogram().unwrap();
        for &(bin, count, bytes) in expected_bins {
            assert_eq!(hist.file_counts[bin], count, "file_counts[{bin}]");
            assert_eq!(hist.total_bytes[bin], bytes, "total_bytes[{bin}]");
        }
    }

    #[rstest]
    #[case::base_none_delta_none(None)]
    #[case::base_some_delta_none(Some(vec![100i64, 200]))]
    fn apply_drops_histogram_when_delta_missing_histogram(#[case] base_files: Option<Vec<i64>>) {
        let mut crc = match &base_files {
            Some(sizes) => base_crc_with_histogram(sizes),
            None => base_crc(),
        };
        let delta = CrcDelta {
            file_stats: FileStatsDelta {
                net_files: 1,
                net_bytes: 100,
                net_histogram: None,
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        crc.apply(delta);
        let stats = crc.file_stats().unwrap();
        assert!(
            stats.file_size_histogram().is_none(),
            "histogram should be None when delta doesn't provide a histogram"
        );
    }

    #[test]
    fn apply_drops_histogram_on_indeterminate() {
        let mut crc = base_crc_with_histogram(&[100, 200]);
        let unsafe_delta = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_delta);
        // Indeterminate has no histogram field; the histogram data is gone.
        assert!(crc.file_stats_state.is_indeterminate());
        assert!(crc.file_stats().is_none());
    }

    #[test]
    fn apply_remove_without_size_transitions_to_indeterminate() {
        // A remove action with missing size makes incremental tracking impossible;
        // file_stats returns None because Indeterminate carries no data.
        let mut crc = base_crc_with_histogram(&[100, 200]);
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        crc.apply(delta);
        assert!(crc.file_stats_state.is_indeterminate());
        assert!(crc.file_stats().is_none());
    }

    #[test]
    fn into_crc_for_version_zero_includes_histogram() {
        let delta_hist = histogram_from_sizes(&[500, 1000]);
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            file_stats: FileStatsDelta {
                net_files: 2,
                net_bytes: 1500,
                net_histogram: Some(delta_hist),
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let stats = crc.file_stats().unwrap();
        let hist = stats.file_size_histogram().unwrap();
        assert_eq!(hist.file_counts[0], 2);
        assert_eq!(hist.total_bytes[0], 1500);
    }

    #[test]
    fn into_crc_for_version_zero_without_histogram() {
        // write_delta() produces a CrcDelta with no histogram delta, so
        // into_crc_for_version_zero cannot construct a file size histogram.
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let stats = crc.file_stats().unwrap();
        assert!(stats.file_size_histogram().is_none());
    }

    #[test]
    fn apply_merges_histogram_with_non_default_boundaries() {
        // Base CRC with custom 3-bin histogram: [0, 200) [200, 1000) [1000, inf)
        let boundaries = vec![0, 200, 1000];
        let base_hist = FileSizeHistogram::try_new(
            boundaries.clone(),
            vec![2, 1, 0], // 2 files in bin 0, 1 in bin 1
            vec![300, 500, 0],
        )
        .unwrap();
        let mut crc = Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 3,
                table_size_bytes: 800,
                file_size_histogram: Some(base_hist),
            }),
            ..Default::default()
        };

        // Delta with matching non-default boundaries: +100 and +1500, -150
        let mut delta_hist = FileSizeHistogram::create_empty_with_boundaries(boundaries).unwrap();
        delta_hist.insert(100).unwrap(); // bin 0
        delta_hist.insert(1500).unwrap(); // bin 2
        delta_hist.remove(150).unwrap(); // bin 0

        let delta = CrcDelta {
            file_stats: FileStatsDelta {
                net_files: 1,    // +2 - 1
                net_bytes: 1450, // (100 + 1500) - 150
                net_histogram: Some(delta_hist),
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };

        crc.apply(delta);

        // Histogram should be preserved (boundaries match)
        let stats = crc.file_stats().unwrap();
        let hist = stats.file_size_histogram().unwrap();
        assert_eq!(hist.sorted_bin_boundaries, vec![0, 200, 1000]);
        assert_eq!(hist.file_counts, vec![2, 1, 1]); // (2+1-1), (1+0-0), (0+1-0)
        assert_eq!(hist.total_bytes, vec![250, 500, 1500]); // (300+100-150), (500+0-0), (0+1500-0)
        assert_eq!(stats.num_files(), 4);
        assert_eq!(stats.table_size_bytes(), 2250);
    }
}
