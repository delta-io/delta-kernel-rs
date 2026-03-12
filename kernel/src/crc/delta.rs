//! Incremental CRC state updates via commit deltas.
//!
//! A [`CrcDelta`] captures CRC-relevant changes from a single commit (produced by reading a
//! `.json` commit file during log replay, or from in-memory transaction state during writes).
//! [`Crc::apply`] advances a CRC forward one commit at a time by applying a delta.
//!
//! A CRC tracks two categories of fields, updated differently:
//! - **Metadata fields** (protocol, metadata, domain metadata, set transactions, in-commit
//!   timestamp): always kept up-to-date -- every `apply` unconditionally merges these from
//!   the delta.
//! - **File stats** (`num_files`, `table_size_bytes`): only updated when the current
//!   [`FileStatsValidity`] is not terminal and the commit's operation is incremental-safe.
//!   Once validity degrades (e.g. a non-incremental operation like ANALYZE STATS, or a
//!   missing file size), file stats stop updating for the lifetime of that CRC.

use crate::actions::{DomainMetadata, Metadata, Protocol, SetTransaction};

use super::file_stats::FileStatsDelta;
use super::{Crc, FileStatsValidity};

/// The CRC-relevant changes ("delta") from a single commit. Produced either by reading a
/// `.json` commit file during log replay, or from in-memory transaction state during writes.
#[derive(Debug, Clone, Default)]
pub(crate) struct CrcDelta {
    /// Net file count and size changes.
    pub(crate) file_stats: FileStatsDelta,
    /// New protocol action, if this commit changed it.
    pub(crate) protocol: Option<Protocol>,
    /// New metadata action, if this commit changed it.
    pub(crate) metadata: Option<Metadata>,
    /// All DM actions in this commit (additions and removals). `apply()` only processes these
    /// when the base CRC's `domain_metadata` is `Some` (tracked).
    pub(crate) domain_metadata_changes: Vec<DomainMetadata>,
    /// All SetTransaction actions in this commit. `apply()` only processes these when the base
    /// CRC's `set_transactions` is `Some` (tracked).
    pub(crate) set_transaction_changes: Vec<SetTransaction>,
    /// In-commit timestamp, if present in this commit.
    pub(crate) in_commit_timestamp: Option<i64>,
    /// Must be `Some` with an incremental-safe value for file stats to update. `None` or
    /// unrecognized values transition validity to `Indeterminate`.
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
        // For CREATE TABLE we always know the full domain metadata state: the transaction
        // either included domain metadata actions or it didn't. So this is always `Some` --
        // an empty map means "no domain metadata", not "unknown".
        let domain_metadata = Some(
            self.domain_metadata_changes
                .into_iter()
                .filter(|dm| !dm.is_removed())
                .map(|dm| (dm.domain().to_string(), dm))
                .collect(),
        );
        // Same reasoning as domain metadata: we always know the full set transaction state
        // for CREATE TABLE, so this is always `Some`.
        let set_transactions = Some(
            self.set_transaction_changes
                .into_iter()
                .map(|txn| (txn.app_id.clone(), txn))
                .collect(),
        );
        Some(Crc {
            table_size_bytes: self.file_stats.net_bytes,
            num_files: self.file_stats.net_files,
            num_metadata: 1,
            num_protocol: 1,
            protocol,
            metadata,
            domain_metadata,
            set_transactions,
            in_commit_timestamp_opt: self.in_commit_timestamp,
            ..Default::default()
        })
    }
}

/// Commit delta application for [`Crc`]. See the [module-level docs](self) for details.
impl Crc {
    /// Apply a commit delta, updating all CRC fields and adjusting file stats validity.
    ///
    /// Metadata fields are always updated. File stats are only updated when:
    /// - Validity is not already terminal ([`Untrackable`](FileStatsValidity::Untrackable) or
    ///   [`Indeterminate`](FileStatsValidity::Indeterminate))
    /// - The delta has no missing file sizes
    /// - The operation is incremental-safe
    pub(crate) fn apply(&mut self, delta: CrcDelta) {
        // Protocol and metadata: replace if present.
        if let Some(p) = delta.protocol {
            self.protocol = p;
        }
        if let Some(m) = delta.metadata {
            self.metadata = m;
        }

        // Domain metadata: insert or remove by domain name. Only update if the base CRC
        // tracks domain metadata (Some). If None ("not tracked"), leave it as None --
        // applying partial changes would create an incomplete map.
        if !delta.domain_metadata_changes.is_empty() {
            if let Some(map) = &mut self.domain_metadata {
                for dm in delta.domain_metadata_changes {
                    if dm.is_removed() {
                        map.remove(dm.domain());
                    } else {
                        let domain = dm.domain().to_string();
                        map.insert(domain, dm);
                    }
                }
            }
        }

        // Set transactions: upsert by app_id. Only update if the base CRC tracks set
        // transactions (Some). If None ("not tracked"), leave it as None.
        if !delta.set_transaction_changes.is_empty() {
            if let Some(map) = &mut self.set_transactions {
                for txn in delta.set_transaction_changes {
                    let app_id = txn.app_id.clone();
                    map.insert(app_id, txn);
                }
            }
        }

        // In-commit timestamp: unconditional replace (not guarded by `if let Some`).
        // If ICT was disabled after being enabled, the delta carries None, which correctly
        // clears the previous value.
        self.in_commit_timestamp_opt = delta.in_commit_timestamp;

        // Bail if already Untrackable -- nothing can recover missing file stats.
        if self.file_stats_validity == FileStatsValidity::Untrackable {
            return;
        }

        // Missing file size poisons stats permanently. Checked after the Untrackable bail-out
        // so that Untrackable can never transition to Indeterminate below.
        if delta.has_missing_file_size {
            self.file_stats_validity = FileStatsValidity::Untrackable;
            return;
        }

        // Bail if already Indeterminate (theoretically recoverable via full replay).
        if self.file_stats_validity == FileStatsValidity::Indeterminate {
            return;
        }

        let is_safe = delta
            .operation
            .as_deref()
            .is_some_and(FileStatsDelta::is_incremental_safe);
        if !is_safe {
            self.file_stats_validity = FileStatsValidity::Indeterminate;
            return;
        }
        self.num_files += delta.file_stats.net_files;
        self.table_size_bytes += delta.file_stats.net_bytes;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::actions::{DomainMetadata, Metadata, Protocol};

    fn base_crc() -> Crc {
        Crc {
            table_size_bytes: 1000,
            num_files: 10,
            num_metadata: 1,
            num_protocol: 1,
            ..Default::default()
        }
    }

    fn write_delta(net_files: i64, net_bytes: i64) -> CrcDelta {
        CrcDelta {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
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

    // ===== Crc deserialized from CRC file (default validity) =====

    #[test]
    fn test_deserialized_crc_has_valid_stats() {
        let crc = base_crc();
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Valid);
        assert_eq!(crc.num_files, 10);
        assert_eq!(crc.table_size_bytes, 1000);
    }

    // ===== Crc::apply tests =====

    #[test]
    fn test_apply_updates_file_stats() {
        let mut crc = base_crc();
        crc.apply(write_delta(3, 600));
        assert_eq!(crc.num_files, 13); // 10 + 3
        assert_eq!(crc.table_size_bytes, 1600); // 1000 + 600
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Valid);
    }

    /// Simulates forward log replay: apply multiple commit deltas sequentially.
    #[test]
    fn test_apply_multiple_deltas() {
        let mut crc = base_crc();
        crc.apply(write_delta(3, 600));
        crc.apply(write_delta(-2, -400));
        assert_eq!(crc.num_files, 11); // 10 + 3 - 2
        assert_eq!(crc.table_size_bytes, 1200); // 1000 + 600 - 400
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Valid);
    }

    #[test]
    fn test_apply_unsafe_op_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_apply_none_op_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let unknown_delta = CrcDelta {
            operation: None,
            ..write_delta(1, 100)
        };
        crc.apply(unknown_delta);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_indeterminate_stays_indeterminate() {
        let mut crc = base_crc();
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Indeterminate);

        // Subsequent safe op doesn't recover validity.
        crc.apply(write_delta(5, 500));
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Indeterminate);
    }

    // ===== apply: Untrackable (missing file size) tests =====

    #[test]
    fn test_missing_file_size_transitions_to_untrackable() {
        let mut crc = base_crc();
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Untrackable);
    }

    #[test]
    fn test_untrackable_stays_untrackable() {
        let mut crc = base_crc();
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Untrackable);

        // Applying a safe delta does not recover from Untrackable.
        crc.apply(write_delta(5, 500));
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Untrackable);

        // Applying an unsafe delta also stays Untrackable (does not downgrade to Indeterminate).
        crc.apply(CrcDelta {
            operation: None,
            ..write_delta(1, 100)
        });
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Untrackable);
    }

    #[test]
    fn test_indeterminate_transitions_to_untrackable_on_missing_size() {
        let mut crc = base_crc();
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Indeterminate);

        // Missing size escalates Indeterminate to Untrackable.
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Untrackable);
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
    fn test_apply_adds_domain_metadata_to_tracked_map() {
        let mut crc = base_crc();
        crc.domain_metadata = Some(HashMap::new());
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let map = crc.domain_metadata.as_ref().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
    }

    #[test]
    fn test_apply_with_untracked_domain_metadata_skips_changes() {
        let mut crc = base_crc();
        assert!(crc.domain_metadata.is_none()); // Not tracked (default)
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        // domain_metadata stays None -- apply() must not create a partial map.
        assert!(crc.domain_metadata.is_none());
    }

    #[test]
    fn test_apply_upserts_domain_metadata() {
        let mut crc = base_crc();
        crc.domain_metadata = Some(HashMap::from([(
            "my.domain".to_string(),
            DomainMetadata::new("my.domain".to_string(), "old_config".to_string()),
        )]));

        let dm = DomainMetadata::new("my.domain".to_string(), "new_config".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let map = crc.domain_metadata.as_ref().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "new_config");
    }

    #[test]
    fn test_apply_removes_domain_metadata() {
        let mut crc = base_crc();
        crc.domain_metadata = Some(HashMap::from([(
            "my.domain".to_string(),
            DomainMetadata::new("my.domain".to_string(), "config1".to_string()),
        )]));

        let dm = DomainMetadata::remove("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        crc.apply(delta);

        let map = crc.domain_metadata.as_ref().unwrap();
        assert!(map.is_empty());
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
        assert_eq!(crc.protocol, protocol);
        assert_eq!(crc.metadata, metadata);
        assert_eq!(crc.num_files, 5);
        assert_eq!(crc.table_size_bytes, 1000);
        assert_eq!(crc.num_metadata, 1);
        assert_eq!(crc.num_protocol, 1);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Valid);
        assert_eq!(crc.domain_metadata, Some(HashMap::new()));
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
        let map = crc.domain_metadata.as_ref().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
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
}
