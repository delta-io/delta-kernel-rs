//! Incremental CRC state updates.
//!
//! A [`CrcUpdate`] captures CRC-relevant changes. It is produced either from a single commit
//! (in-memory transaction state during writes) or from accumulated reverse log replay (reading
//! committed `.json` files during snapshot loading). [`Crc::apply`] advances a CRC by applying
//! an update.
//!
//! A CRC tracks two categories of fields, updated differently:
//! - **Metadata fields** (protocol, metadata, domain metadata, set transactions, in-commit
//!   timestamp): always kept up-to-date -- every `apply` unconditionally merges these from
//!   the delta.
//! - **File stats**: only updated when the current [`FileStatsState`] is not terminal and the
//!   commit's operation is incremental-safe. Once the state degrades (e.g. a non-incremental
//!   operation like ANALYZE STATS, or a missing file size), file stats stop updating for the
//!   lifetime of that CRC.

use std::collections::HashMap;

use crate::actions::{DomainMetadata, Metadata, Protocol, SetTransaction};

use super::file_stats::FileStatsDelta;
use super::{Crc, DomainMetadataState, FileSizeHistogram, FileStatsState, SetTransactionState};

/// CRC-relevant changes to apply to a base [`Crc`]. Produced either from a single commit
/// (transaction path) or from accumulated reverse log replay (snapshot loading path).
///
/// **Transaction path:** `domain_metadata` and `set_transactions` contain the changes from
/// one commit. `operation_safe` is always true, `has_missing_file_size` is always false.
///
/// **Replay path:** `domain_metadata` and `set_transactions` contain first-seen-wins
/// accumulated state across many commits. `operation_safe` may be false if any commit in
/// the replay range had a non-incremental operation.
#[derive(Debug, Clone, Default)]
pub(crate) struct CrcUpdate {
    /// Net file count, size changes and histograms.
    pub(crate) file_stats: FileStatsDelta,
    /// New protocol action, if present. In replay mode, this is the first (newest) protocol
    /// found going backwards.
    pub(crate) protocol: Option<Protocol>,
    /// New metadata action, if present. In replay mode, this is the first (newest) metadata
    /// found going backwards.
    pub(crate) metadata: Option<Metadata>,
    /// Domain metadata changes keyed by domain name. In replay mode, first-seen-wins across
    /// all commits in the range. `apply()` only processes these when the base CRC's
    /// `domain_metadata` is not `Untracked`. Tombstones (`removed=true`) are included and
    /// handled by `apply()`.
    pub(crate) domain_metadata: HashMap<String, DomainMetadata>,
    /// Set transaction changes keyed by app_id. In replay mode, first-seen-wins across all
    /// commits in the range. `apply()` only processes these when the base CRC's
    /// `set_transactions` is not `Untracked`.
    pub(crate) set_transactions: HashMap<String, SetTransaction>,
    /// In-commit timestamp, if present. In replay mode, from the newest commit.
    pub(crate) in_commit_timestamp: Option<i64>,
    /// Whether all operations in this update are incremental-safe. `false` or unknown
    /// operations transition file stats to `Indeterminate`.
    pub(crate) operation_safe: bool,
    /// A file action had a missing `size` field, making byte-level file stats impossible
    /// to compute.
    pub(crate) has_missing_file_size: bool,
}

impl CrcUpdate {
    /// Convert this update into a fresh [`Crc`] representing a complete table state.
    ///
    /// Used when the update represents the entire table state (CREATE TABLE, full log replay
    /// from version zero, or build-from-scratch). Protocol and metadata must be present.
    ///
    /// The resulting CRC has:
    /// - DM/txns: Complete (the full state was captured)
    /// - File stats: derived from `has_missing_file_size` and `operation_safe`
    /// - Histogram: computed from added/removed histograms when available
    ///
    /// # Errors
    ///
    /// Returns [`Error::MissingProtocol`] or [`Error::MissingMetadata`] if either is absent.
    pub(crate) fn into_fresh_crc(self) -> crate::DeltaResult<Crc> {
        let protocol = self.protocol.ok_or(crate::Error::MissingProtocol)?;
        let metadata = self.metadata.ok_or(crate::Error::MissingMetadata)?;

        // Full state was captured, so DM and txns are Complete. Filter DM tombstones.
        let domain_metadata = DomainMetadataState::Complete(
            self.domain_metadata
                .into_iter()
                .filter(|(_, dm)| !dm.is_removed())
                .collect(),
        );
        let set_transactions = SetTransactionState::Complete(self.set_transactions);

        // Determine file stats state from the update flags.
        let file_stats = if self.has_missing_file_size {
            FileStatsState::Untrackable
        } else if !self.operation_safe {
            FileStatsState::Indeterminate
        } else {
            // Compute the initial histogram from the file stats. If both added and removed
            // histograms are present, subtract removals from additions. If only the added
            // histogram is present, use it directly. Otherwise, we cannot produce a histogram.
            //
            // If try_sub fails for any reason (e.g. histogram goes negative) then the histogram
            // is silently dropped.
            let histogram = match (
                self.file_stats.added_histogram,
                self.file_stats.removed_histogram,
            ) {
                (Some(added), Some(removed)) => added.try_sub(&removed).ok(),
                (Some(added), None) => Some(added),
                _ => None,
            };
            FileStatsState::Valid {
                num_files: self.file_stats.net_files,
                table_size_bytes: self.file_stats.net_bytes,
                histogram,
            }
        };

        Ok(Crc {
            protocol,
            metadata,
            file_stats,
            domain_metadata,
            set_transactions,
            in_commit_timestamp_opt: self.in_commit_timestamp,
        })
    }

    /// Convert this update into a fresh [`Crc`] for version zero (CREATE TABLE).
    ///
    /// Returns `None` if protocol or metadata are missing (both are required for a valid CRC).
    pub(crate) fn into_crc_for_version_zero(self) -> Option<Crc> {
        self.into_fresh_crc().ok()
    }
}

/// Merge a base histogram with added/removed delta histograms. Returns the merged histogram
/// if successful, `None` if the merge fails (e.g. boundary mismatch, negative counts) or if
/// the delta does not provide both add/remove histograms. When the base has a histogram but
/// the delta cannot provide both sides, the histogram is dropped (stale data is worse than
/// no data).
fn merge_histogram(
    base: &Option<FileSizeHistogram>,
    delta: &FileStatsDelta,
) -> Option<FileSizeHistogram> {
    if let (Some(base_hist), Some(added), Some(removed)) = (
        base.as_ref(),
        &delta.added_histogram,
        &delta.removed_histogram,
    ) {
        base_hist
            .try_add(added)
            .and_then(|h| h.try_sub(removed))
            .ok()
    } else if base.is_some() {
        // The base had a histogram but the update could not provide add/remove histograms.
        // Drop it rather than leaving a stale value.
        None
    } else {
        // Base had no histogram and delta did not provide one. Stay at None.
        None
    }
}

/// Commit delta application for [`Crc`]. See the [module-level docs](self) for details.
impl Crc {
    /// Apply a commit delta, updating all CRC fields and adjusting file stats state.
    ///
    /// Metadata fields are always updated. File stats are only updated when:
    /// - The current state is not already terminal ([`Untrackable`](FileStatsState::Untrackable)
    ///   or [`Indeterminate`](FileStatsState::Indeterminate))
    /// - The delta has no missing file sizes
    /// - The operation is incremental-safe
    pub(crate) fn apply(&mut self, update: CrcUpdate) {
        // Protocol and metadata: replace if present.
        if let Some(p) = update.protocol {
            self.protocol = p;
        }
        if let Some(m) = update.metadata {
            self.metadata = m;
        }

        // Domain metadata: insert or remove by domain name. Only update if the base CRC
        // tracks domain metadata (Complete or Partial). Untracked stays Untracked.
        if !update.domain_metadata.is_empty() {
            match &mut self.domain_metadata {
                DomainMetadataState::Complete(map) | DomainMetadataState::Partial(map) => {
                    for (domain, dm) in update.domain_metadata {
                        if dm.is_removed() {
                            map.remove(&domain);
                        } else {
                            map.insert(domain, dm);
                        }
                    }
                }
                DomainMetadataState::Untracked => {}
            }
        }

        // Set transactions: upsert by app_id. Only update if the base CRC tracks them.
        match &mut self.set_transactions {
            SetTransactionState::Complete(map) | SetTransactionState::Partial(map) => {
                map.extend(update.set_transactions);
            }
            SetTransactionState::Untracked => {}
        }

        // In-commit timestamp: unconditional replace (not guarded by `if let Some`).
        // If ICT was disabled after being enabled, the update carries None, which correctly
        // clears the previous value.
        self.in_commit_timestamp_opt = update.in_commit_timestamp;

        // File stats: transition based on current state.
        self.file_stats = match &self.file_stats {
            FileStatsState::Untrackable => {
                // Terminal: nothing can recover missing file stats.
                return;
            }
            _ if update.has_missing_file_size => {
                // Missing file size poisons stats permanently.
                FileStatsState::Untrackable
            }
            FileStatsState::Indeterminate => {
                // Terminal for file stats (theoretically recoverable via full replay).
                return;
            }
            _ if !update.operation_safe => {
                // Non-incremental operation makes file stats indeterminate.
                FileStatsState::Indeterminate
            }
            FileStatsState::Valid {
                num_files,
                table_size_bytes,
                histogram,
            } => {
                let new_histogram = merge_histogram(histogram, &update.file_stats);
                FileStatsState::Valid {
                    num_files: num_files + update.file_stats.net_files,
                    table_size_bytes: table_size_bytes + update.file_stats.net_bytes,
                    histogram: new_histogram,
                }
            }
            FileStatsState::RequiresCheckpointRead {
                commit_delta_files,
                commit_delta_bytes,
                commit_delta_histogram,
            } => FileStatsState::RequiresCheckpointRead {
                commit_delta_files: commit_delta_files + update.file_stats.net_files,
                commit_delta_bytes: commit_delta_bytes + update.file_stats.net_bytes,
                commit_delta_histogram: merge_histogram(commit_delta_histogram, &update.file_stats),
            },
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;
    use crate::actions::{DomainMetadata, Metadata, Protocol};
    use crate::crc::{
        DomainMetadataState, FileSizeHistogram, FileStatsValidity, SetTransactionState,
    };

    fn base_crc() -> Crc {
        Crc {
            file_stats: FileStatsState::Valid {
                num_files: 10,
                table_size_bytes: 1000,
                histogram: None,
            },
            ..Default::default()
        }
    }

    fn write_update(net_files: i64, net_bytes: i64) -> CrcUpdate {
        CrcUpdate {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
                ..Default::default()
            },
            operation_safe: true,
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

    // ===== Crc deserialized from CRC file (default validity) =====

    #[test]
    fn test_deserialized_crc_has_valid_stats() {
        let crc = base_crc();
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Valid);
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files, 10);
        assert_eq!(stats.table_size_bytes, 1000);
    }

    // ===== Crc::apply tests =====

    #[test]
    fn test_apply_updates_file_stats() {
        let mut crc = base_crc();
        crc.apply(write_update(3, 600));
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files, 13); // 10 + 3
        assert_eq!(stats.table_size_bytes, 1600); // 1000 + 600
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Valid);
    }

    /// Simulates forward log replay: apply multiple commit deltas sequentially.
    #[test]
    fn test_apply_multiple_deltas() {
        let mut crc = base_crc();
        crc.apply(write_update(3, 600));
        crc.apply(write_update(-2, -400));
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files, 11); // 10 + 3 - 2
        assert_eq!(stats.table_size_bytes, 1200); // 1000 + 600 - 400
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Valid);
    }

    #[test]
    fn test_apply_unsafe_op_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let unsafe_change = CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_apply_none_op_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let unknown_delta = CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        };
        crc.apply(unknown_delta);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_indeterminate_stays_indeterminate() {
        let mut crc = base_crc();
        let unsafe_change = CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);

        // Subsequent safe op doesn't recover validity.
        crc.apply(write_update(5, 500));
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);
    }

    // ===== apply: Untrackable (missing file size) tests =====

    #[test]
    fn test_missing_file_size_transitions_to_untrackable() {
        let mut crc = base_crc();
        let delta = CrcUpdate {
            has_missing_file_size: true,
            ..write_update(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
    }

    #[test]
    fn test_untrackable_stays_untrackable() {
        let mut crc = base_crc();
        let delta = CrcUpdate {
            has_missing_file_size: true,
            ..write_update(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);

        // Applying a safe delta does not recover from Untrackable.
        crc.apply(write_update(5, 500));
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);

        // Applying an unsafe delta also stays Untrackable (does not downgrade to Indeterminate).
        crc.apply(CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        });
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
    }

    #[test]
    fn test_indeterminate_transitions_to_untrackable_on_missing_size() {
        let mut crc = base_crc();
        let unsafe_change = CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);

        // Missing size escalates Indeterminate to Untrackable.
        let delta = CrcUpdate {
            has_missing_file_size: true,
            ..write_update(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
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
        let delta = CrcUpdate {
            protocol: Some(new_protocol.clone()),
            ..write_update(0, 0)
        };
        crc.apply(delta);
        assert_eq!(crc.protocol, new_protocol);
        assert_eq!(crc.metadata, Metadata::default()); // unchanged
    }

    #[test]
    fn test_apply_adds_domain_metadata_to_tracked_map() {
        let mut crc = base_crc();
        crc.domain_metadata = DomainMetadataState::Complete(HashMap::new());
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let update = CrcUpdate {
            domain_metadata: HashMap::from([("my.domain".to_string(), dm)]),
            ..write_update(0, 0)
        };
        crc.apply(update);

        let map = crc.domain_metadata.map().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
    }

    #[test]
    fn test_apply_with_untracked_domain_metadata_skips_changes() {
        let mut crc = base_crc();
        assert!(matches!(
            crc.domain_metadata,
            DomainMetadataState::Untracked
        ));
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let update = CrcUpdate {
            domain_metadata: HashMap::from([("my.domain".to_string(), dm)]),
            ..write_update(0, 0)
        };
        crc.apply(update);

        // domain_metadata stays Untracked. apply() must not create a partial map.
        assert!(matches!(
            crc.domain_metadata,
            DomainMetadataState::Untracked
        ));
    }

    #[test]
    fn test_apply_upserts_domain_metadata() {
        let mut crc = base_crc();
        crc.domain_metadata = DomainMetadataState::Complete(HashMap::from([(
            "my.domain".to_string(),
            DomainMetadata::new("my.domain".to_string(), "old_config".to_string()),
        )]));

        let dm = DomainMetadata::new("my.domain".to_string(), "new_config".to_string());
        let update = CrcUpdate {
            domain_metadata: HashMap::from([("my.domain".to_string(), dm)]),
            ..write_update(0, 0)
        };
        crc.apply(update);

        let map = crc.domain_metadata.map().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "new_config");
    }

    #[test]
    fn test_apply_removes_domain_metadata() {
        let mut crc = base_crc();
        crc.domain_metadata = DomainMetadataState::Complete(HashMap::from([(
            "my.domain".to_string(),
            DomainMetadata::new("my.domain".to_string(), "config1".to_string()),
        )]));

        let dm = DomainMetadata::remove("my.domain".to_string(), "config1".to_string());
        let update = CrcUpdate {
            domain_metadata: HashMap::from([("my.domain".to_string(), dm)]),
            ..write_update(0, 0)
        };
        crc.apply(update);

        let map = crc.domain_metadata.map().unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn test_apply_replaces_in_commit_timestamp() {
        let mut crc = base_crc();
        let delta = CrcUpdate {
            in_commit_timestamp: Some(9999),
            ..write_update(0, 0)
        };
        crc.apply(delta);
        assert_eq!(crc.in_commit_timestamp_opt, Some(9999));
    }

    #[test]
    fn test_apply_clears_in_commit_timestamp_when_ict_disabled() {
        let mut crc = base_crc();
        crc.in_commit_timestamp_opt = Some(1000);

        // Delta without ICT (e.g. ICT was disabled) clears the previous value.
        let delta = CrcUpdate {
            in_commit_timestamp: None,
            ..write_update(0, 0)
        };
        crc.apply(delta);
        assert_eq!(crc.in_commit_timestamp_opt, None);
    }

    // ===== CrcUpdate::into_crc_for_version_zero tests =====

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
        let delta = CrcUpdate {
            protocol: Some(protocol.clone()),
            metadata: Some(metadata.clone()),
            ..write_update(5, 1000)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        assert_eq!(crc.protocol, protocol);
        assert_eq!(crc.metadata, metadata);
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files, 5);
        assert_eq!(stats.table_size_bytes, 1000);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Valid);
        assert_eq!(
            crc.domain_metadata,
            DomainMetadataState::Complete(HashMap::new())
        );
        assert_eq!(crc.in_commit_timestamp_opt, None);
    }

    #[test]
    fn test_into_crc_for_version_zero_returns_none_without_protocol() {
        let delta = CrcUpdate {
            metadata: Some(Metadata::default()),
            ..write_update(5, 1000)
        };
        assert!(delta.into_crc_for_version_zero().is_none());
    }

    #[test]
    fn test_into_crc_for_version_zero_returns_none_without_metadata() {
        let delta = CrcUpdate {
            protocol: Some(test_protocol()),
            ..write_update(5, 1000)
        };
        assert!(delta.into_crc_for_version_zero().is_none());
    }

    #[test]
    fn test_into_crc_for_version_zero_with_domain_metadata() {
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            domain_metadata: HashMap::from([("my.domain".to_string(), dm)]),
            ..write_update(0, 0)
        };
        let crc = update.into_crc_for_version_zero().unwrap();
        let map = crc.domain_metadata.map().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
    }

    #[test]
    fn test_into_crc_for_version_zero_with_in_commit_timestamp() {
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            in_commit_timestamp: Some(12345),
            ..write_update(0, 0)
        };
        let crc = update.into_crc_for_version_zero().unwrap();
        assert_eq!(crc.in_commit_timestamp_opt, Some(12345));
    }

    // ===== apply: set transaction tests =====

    #[test]
    fn test_apply_adds_set_transaction_to_tracked_map() {
        let mut crc = base_crc();
        crc.set_transactions = SetTransactionState::Complete(HashMap::new());
        let txn = SetTransaction::new("my-app".to_string(), 1, Some(1000));
        let update = CrcUpdate {
            set_transactions: HashMap::from([("my-app".to_string(), txn)]),
            ..write_update(0, 0)
        };
        crc.apply(update);

        let map = crc.set_transactions.map().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my-app"].version, 1);
        assert_eq!(map["my-app"].last_updated, Some(1000));
    }

    #[test]
    fn test_apply_with_untracked_set_transactions_skips_changes() {
        let mut crc = base_crc();
        assert!(matches!(
            crc.set_transactions,
            SetTransactionState::Untracked
        ));
        let txn = SetTransaction::new("my-app".to_string(), 1, Some(1000));
        let update = CrcUpdate {
            set_transactions: HashMap::from([("my-app".to_string(), txn)]),
            ..write_update(0, 0)
        };
        crc.apply(update);

        // set_transactions stays Untracked. apply() must not create a partial map.
        assert!(matches!(
            crc.set_transactions,
            SetTransactionState::Untracked
        ));
    }

    #[test]
    fn test_apply_upserts_set_transaction() {
        let mut crc = base_crc();
        crc.set_transactions = SetTransactionState::Complete(HashMap::from([(
            "my-app".to_string(),
            SetTransaction::new("my-app".to_string(), 1, Some(1000)),
        )]));

        let txn = SetTransaction::new("my-app".to_string(), 2, Some(2000));
        let update = CrcUpdate {
            set_transactions: HashMap::from([("my-app".to_string(), txn)]),
            ..write_update(0, 0)
        };
        crc.apply(update);

        let map = crc.set_transactions.map().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my-app"].version, 2);
        assert_eq!(map["my-app"].last_updated, Some(2000));
    }

    // ===== into_crc_for_version_zero: set transaction tests =====

    #[test]
    fn test_into_crc_for_version_zero_with_set_transactions() {
        let txn = SetTransaction::new("my-app".to_string(), 5, Some(3000));
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            set_transactions: HashMap::from([("my-app".to_string(), txn)]),
            ..write_update(0, 0)
        };
        let crc = update.into_crc_for_version_zero().unwrap();
        let map = crc.set_transactions.map().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my-app"].version, 5);
        assert_eq!(map["my-app"].last_updated, Some(3000));
    }

    #[test]
    fn test_into_crc_for_version_zero_with_no_set_transactions() {
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            ..write_update(0, 0)
        };
        let crc = update.into_crc_for_version_zero().unwrap();
        // Complete with empty map, not Untracked. We always know the full state at version zero.
        assert_eq!(
            crc.set_transactions,
            SetTransactionState::Complete(HashMap::new())
        );
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
            file_stats: FileStatsState::Valid {
                num_files: file_sizes.len() as i64,
                table_size_bytes: file_sizes.iter().sum(),
                histogram: Some(hist),
            },
            ..Default::default()
        }
    }

    /// Helper: creates a CrcUpdate with histogram data for adds and removes.
    fn write_delta_with_histograms(add_sizes: &[i64], remove_sizes: &[i64]) -> CrcUpdate {
        let added = histogram_from_sizes(add_sizes);
        let removed = histogram_from_sizes(remove_sizes);
        let net_files = add_sizes.len() as i64 - remove_sizes.len() as i64;
        let net_bytes: i64 = add_sizes.iter().sum::<i64>() - remove_sizes.iter().sum::<i64>();
        CrcUpdate {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
                added_histogram: Some(added),
                removed_histogram: Some(removed),
            },
            operation_safe: true,
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

        let hist = crc
            .file_stats()
            .expect("file stats should be Valid")
            .file_size_histogram
            .expect("histogram should be present");
        for &(bin, count, bytes) in expected_bins {
            assert_eq!(hist.file_counts[bin], count, "file_counts[{bin}]");
            assert_eq!(hist.total_bytes[bin], bytes, "total_bytes[{bin}]");
        }
    }

    #[rstest]
    #[case::base_none_delta_none(None, None, None)]
    #[case::base_some_delta_both_none(Some(vec![100i64, 200]), None, None)]
    #[case::base_some_delta_added_only(Some(vec![100i64, 200]), Some(vec![500i64]), None)]
    #[case::base_some_delta_removed_only(Some(vec![100i64, 200]), None, Some(vec![100i64]))]
    fn apply_drops_histogram_when_delta_missing_histograms(
        #[case] base_files: Option<Vec<i64>>,
        #[case] added_sizes: Option<Vec<i64>>,
        #[case] removed_sizes: Option<Vec<i64>>,
    ) {
        let mut crc = match &base_files {
            Some(sizes) => base_crc_with_histogram(sizes),
            None => base_crc(),
        };
        let added_histogram = added_sizes.map(|sizes| histogram_from_sizes(&sizes));
        let removed_histogram = removed_sizes.map(|sizes| histogram_from_sizes(&sizes));
        let delta = CrcUpdate {
            file_stats: FileStatsDelta {
                net_files: 1,
                net_bytes: 100,
                added_histogram,
                removed_histogram,
            },
            operation_safe: true,
            ..Default::default()
        };
        crc.apply(delta);
        let stats = crc.file_stats().expect("file stats should be Valid");
        assert!(
            stats.file_size_histogram.is_none(),
            "histogram should be None when delta can't provide both add/remove histograms"
        );
    }

    #[test]
    fn apply_drops_histogram_on_indeterminate() {
        let mut crc = base_crc_with_histogram(&[100, 200]);
        let unsafe_delta = CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        };
        crc.apply(unsafe_delta);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);
        assert!(crc.file_stats().is_none());
    }

    #[test]
    fn apply_drops_histogram_on_untrackable() {
        let mut crc = base_crc_with_histogram(&[100, 200]);
        // A missing file size makes byte-level stats impossible, so the histogram is dropped.
        let delta = CrcUpdate {
            has_missing_file_size: true,
            ..write_update(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
        assert!(crc.file_stats().is_none());
    }

    #[test]
    fn into_crc_for_version_zero_includes_histogram() {
        let added = histogram_from_sizes(&[500, 1000]);
        let delta = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            file_stats: FileStatsDelta {
                net_files: 2,
                net_bytes: 1500,
                added_histogram: Some(added),
                removed_histogram: Some(FileSizeHistogram::create_default()),
            },
            operation_safe: true,
            ..Default::default()
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let hist = crc
            .file_stats()
            .unwrap()
            .file_size_histogram
            .expect("histogram should be present");
        assert_eq!(hist.file_counts[0], 2);
        assert_eq!(hist.total_bytes[0], 1500);
    }

    #[test]
    fn into_crc_for_version_zero_without_histogram() {
        // write_update() produces a CrcUpdate with no added/removed histograms, so
        // into_crc_for_version_zero cannot construct a file size histogram.
        let delta = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            ..write_update(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        assert!(crc.file_stats().unwrap().file_size_histogram.is_none());
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
            file_stats: FileStatsState::Valid {
                num_files: 3,
                table_size_bytes: 800,
                histogram: Some(base_hist),
            },
            ..Default::default()
        };

        // Delta with matching non-default boundaries
        let mut added =
            FileSizeHistogram::create_empty_with_boundaries(boundaries.clone()).unwrap();
        added.insert(100).unwrap(); // bin 0
        added.insert(1500).unwrap(); // bin 2
        let mut removed = FileSizeHistogram::create_empty_with_boundaries(boundaries).unwrap();
        removed.insert(150).unwrap(); // bin 0

        let delta = CrcUpdate {
            file_stats: FileStatsDelta {
                net_files: 1,    // +2 - 1
                net_bytes: 1450, // (100 + 1500) - 150
                added_histogram: Some(added),
                removed_histogram: Some(removed),
            },
            operation_safe: true,
            ..Default::default()
        };

        crc.apply(delta);

        // Histogram should be preserved (boundaries match)
        let stats = crc.file_stats().unwrap();
        let hist = stats.file_size_histogram.as_ref().unwrap();
        assert_eq!(hist.sorted_bin_boundaries, vec![0, 200, 1000]);
        assert_eq!(hist.file_counts, vec![2, 1, 1]); // (2+1-1), (1+0-0), (0+1-0)
        assert_eq!(hist.total_bytes, vec![250, 500, 1500]); // (300+100-150), (500+0-0), (0+1500-0)
        assert_eq!(stats.num_files, 4);
        assert_eq!(stats.table_size_bytes, 2250);
    }

    // ===== into_fresh_crc: file stats state tests =====

    #[test]
    fn into_fresh_crc_with_unsafe_operation_produces_indeterminate() {
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            operation_safe: false,
            ..write_update(5, 1000)
        };
        let crc = update.into_fresh_crc().unwrap();
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);
    }

    #[test]
    fn into_fresh_crc_with_missing_file_size_produces_untrackable() {
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            has_missing_file_size: true,
            ..write_update(5, 1000)
        };
        let crc = update.into_fresh_crc().unwrap();
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
    }
}
