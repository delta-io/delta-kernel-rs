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
    /// Net file count, size changes and histograms.
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
        // CREATE TABLE starts with a known-complete set of transactions (possibly empty),
        // so we always track them.
        let set_transactions = Some(
            self.set_transaction_changes
                .into_iter()
                .map(|txn| (txn.app_id.clone(), txn))
                .collect(),
        );
        // Compute the initial histogram from the file stats. If both added and removed
        // histograms are present, subtract removals from additions. If only the added
        // histogram is present, use it directly. Otherwise, we cannot produce a histogram.
        //
        // If try_sub fails for any reason (ie. histogram goes negative) then the histogram
        // is silently dropped.
        let initial_histogram = match (
            self.file_stats.added_histogram,
            self.file_stats.removed_histogram,
        ) {
            (Some(added), Some(removed)) => added.try_sub(&removed).ok(),
            (Some(added), None) => Some(added),
            _ => None,
        };
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
            file_size_histogram: initial_histogram,
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

        // Bail if already Untrackable -- nothing can recover missing file stats or histograms.
        if self.file_stats_validity == FileStatsValidity::Untrackable {
            return;
        }

        // Missing file size poisons stats permanently. Checked after the Untrackable bail-out
        // so that Untrackable can never transition to Indeterminate below.
        if delta.has_missing_file_size {
            self.file_stats_validity = FileStatsValidity::Untrackable;
            self.file_size_histogram = None;
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
            self.file_size_histogram = None;
            return;
        }
        self.num_files += delta.file_stats.net_files;
        self.table_size_bytes += delta.file_stats.net_bytes;

        // Histogram: merge base + added - removed.
        // Only update if the base CRC has a histogram AND the delta provides both histograms.
        // If the merge fails (e.g. negative counts from corrupted data) or the delta is missing
        // either histogram, drop it rather than leaving stale data.
        if let (Some(base_hist), Some(added), Some(removed)) = (
            self.file_size_histogram.as_ref(),
            &delta.file_stats.added_histogram,
            &delta.file_stats.removed_histogram,
        ) {
            match base_hist.try_add(added).and_then(|h| h.try_sub(removed)) {
                Ok(merged) => self.file_size_histogram = Some(merged),
                Err(_) => self.file_size_histogram = None,
            }
        } else if self.file_size_histogram.is_some() {
            // The base had a histogram but the delta couldn't provide add/remove histograms
            // (e.g. forward log replay). Drop it rather than leaving a stale value.
            self.file_size_histogram = None;
        }
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

    // ===== Histogram tests =====

    /// Helper: creates a CRC with a histogram containing the given file sizes.
    fn base_crc_with_histogram(file_sizes: &[i64]) -> Crc {
        let mut hist = FileSizeHistogram::create_default();
        for &size in file_sizes {
            hist.insert(size).unwrap();
        }
        Crc {
            table_size_bytes: file_sizes.iter().sum(),
            num_files: file_sizes.len() as i64,
            num_metadata: 1,
            num_protocol: 1,
            file_size_histogram: Some(hist),
            ..Default::default()
        }
    }

    /// Helper: creates a write delta with histogram data for adds and removes.
    fn write_delta_with_histograms(add_sizes: &[i64], remove_sizes: &[i64]) -> CrcDelta {
        let mut added = FileSizeHistogram::create_default();
        for &size in add_sizes {
            added.insert(size).unwrap();
        }
        let mut removed = FileSizeHistogram::create_default();
        for &size in remove_sizes {
            removed.insert(size).unwrap();
        }
        let net_files = add_sizes.len() as i64 - remove_sizes.len() as i64;
        let net_bytes: i64 = add_sizes.iter().sum::<i64>() - remove_sizes.iter().sum::<i64>();
        CrcDelta {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
                added_histogram: Some(added),
                removed_histogram: Some(removed),
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn apply_merges_histogram_when_both_present() {
        // Base: 3 files [100, 200, 300] all in bin 0
        let mut crc = base_crc_with_histogram(&[100, 200, 300]);
        // Delta: add [500], remove [200]
        let delta = write_delta_with_histograms(&[500], &[200]);
        crc.apply(delta);

        let hist = crc.file_size_histogram.as_ref().unwrap();
        // Bin 0: was 3 files/600 bytes, +1 file/+500 bytes, -1 file/-200 bytes = 3 files/900 bytes
        assert_eq!(hist.file_counts[0], 3);
        assert_eq!(hist.total_bytes[0], 900);
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
        let added_histogram = added_sizes.map(|sizes| {
            let mut h = FileSizeHistogram::create_default();
            for s in &sizes {
                h.insert(*s).unwrap();
            }
            h
        });
        let removed_histogram = removed_sizes.map(|sizes| {
            let mut h = FileSizeHistogram::create_default();
            for s in &sizes {
                h.insert(*s).unwrap();
            }
            h
        });
        let delta = CrcDelta {
            file_stats: FileStatsDelta {
                net_files: 1,
                net_bytes: 100,
                added_histogram,
                removed_histogram,
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        crc.apply(delta);
        assert!(
            crc.file_size_histogram.is_none(),
            "histogram should be None when delta can't provide both add/remove histograms"
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
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Indeterminate);
        assert!(crc.file_size_histogram.is_none());
    }

    #[test]
    fn apply_drops_histogram_on_untrackable() {
        let mut crc = base_crc_with_histogram(&[100, 200]);
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.file_stats_validity, FileStatsValidity::Untrackable);
        assert!(crc.file_size_histogram.is_none());
    }

    #[test]
    fn into_crc_for_version_zero_includes_histogram() {
        let mut added = FileSizeHistogram::create_default();
        added.insert(500).unwrap();
        added.insert(1000).unwrap();
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            file_stats: FileStatsDelta {
                net_files: 2,
                net_bytes: 1500,
                added_histogram: Some(added),
                removed_histogram: Some(FileSizeHistogram::create_default()),
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let hist = crc.file_size_histogram.as_ref().unwrap();
        assert_eq!(hist.file_counts[0], 2);
        assert_eq!(hist.total_bytes[0], 1500);
    }

    #[test]
    fn into_crc_for_version_zero_without_histogram() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        assert!(crc.file_size_histogram.is_none());
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
            table_size_bytes: 800,
            num_files: 3,
            num_metadata: 1,
            num_protocol: 1,
            file_size_histogram: Some(base_hist),
            ..Default::default()
        };

        // Delta with matching non-default boundaries
        let mut added =
            FileSizeHistogram::create_empty_with_boundaries(boundaries.clone()).unwrap();
        added.insert(100).unwrap(); // bin 0
        added.insert(1500).unwrap(); // bin 2
        let mut removed = FileSizeHistogram::create_empty_with_boundaries(boundaries).unwrap();
        removed.insert(150).unwrap(); // bin 0

        let delta = CrcDelta {
            file_stats: FileStatsDelta {
                net_files: 1,    // +2 - 1
                net_bytes: 1450, // (100 + 1500) - 150
                added_histogram: Some(added),
                removed_histogram: Some(removed),
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };

        crc.apply(delta);

        // Histogram should be preserved (boundaries match)
        let hist = crc.file_size_histogram.as_ref().unwrap();
        assert_eq!(hist.sorted_bin_boundaries, vec![0, 200, 1000]);
        assert_eq!(hist.file_counts, vec![2, 1, 1]); // (2+1-1), (1+0-0), (0+1-0)
        assert_eq!(hist.total_bytes, vec![250, 500, 1500]); // (300+100-150), (500+0-0), (0+1500-0)
        assert_eq!(crc.num_files, 4);
        assert_eq!(crc.table_size_bytes, 2250);
    }
}
