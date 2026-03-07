// No consumers yet -- will be integrated in a follow-up PR.
#![allow(dead_code)]
//! Incremental CRC state updates via commit deltas.
//!
//! A [`CrcDelta`] captures CRC-relevant changes from a single commit (produced by reading a
//! `.json` commit file during log replay, or from in-memory transaction state during writes).
//! [`Crc::apply`] advances a CRC forward one commit at a time by applying a delta.
//!
//! A CRC tracks two categories of fields, updated differently:
//! - **Metadata fields** (protocol, metadata, domain metadata, in-commit timestamp, and
//!   eventually set transactions): always kept up-to-date -- every `apply` unconditionally
//!   merges these from the delta.
//! - **File stats** (`num_files`, `table_size_bytes`): only updated when the current
//!   [`FileStatsValidity`] is not terminal and the commit's operation is incremental-safe.
//!   Once validity degrades (e.g. a non-incremental operation like ANALYZE STATS, or a
//!   missing file size), file stats stop updating for the lifetime of that CRC.

use std::collections::HashMap;

use crate::actions::{DomainMetadata, Metadata, Protocol};

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
    /// Domain metadata additions and removals in this commit.
    pub(crate) domain_metadata_changes: Vec<DomainMetadata>,
    /// In-commit timestamp, if present in this commit.
    pub(crate) in_commit_timestamp: Option<i64>,
    /// The operation that produced this commit (e.g. "WRITE", "MERGE").
    pub(crate) operation: Option<String>,
    /// A file action in this commit had a missing `size` field, making byte-level file stats
    /// impossible to compute.
    pub(crate) has_missing_file_size: bool,
}

impl CrcDelta {
    /// Convert this delta into a fresh [`Crc`], consuming the delta.
    ///
    /// Returns `Some(Crc)` if the delta contains both protocol and metadata (the minimum fields
    /// required to form a valid CRC). Returns `None` otherwise.
    ///
    /// The resulting CRC has [`FileStatsValidity::Valid`] by default. File stats come from the
    /// delta's `net_files`/`net_bytes`. The caller is responsible for adjusting validity afterward
    /// if needed (e.g. for checkpoint-rooted tables).
    pub(crate) fn into_crc(self) -> Option<Crc> {
        let protocol = self.protocol?;
        let metadata = self.metadata?;

        let mut crc = Crc {
            protocol,
            metadata,
            num_files: self.file_stats.net_files,
            table_size_bytes: self.file_stats.net_bytes,
            num_metadata: 1,
            num_protocol: 1,
            in_commit_timestamp_opt: self.in_commit_timestamp,
            validity: FileStatsValidity::Valid,
            ..Default::default()
        };

        apply_domain_metadata_changes(&mut crc.domain_metadata, self.domain_metadata_changes);

        // Apply file stats validity degradation.
        if self.has_missing_file_size {
            crc.validity = FileStatsValidity::Untrackable;
        } else {
            let is_safe = self
                .operation
                .as_deref()
                .is_some_and(FileStatsDelta::is_incremental_safe);
            if !is_safe {
                crc.validity = FileStatsValidity::Indeterminate;
            }
        }

        Some(crc)
    }
}

/// Apply domain metadata changes (inserts and removals) to an optional domain metadata map.
/// Initializes the map if it is `None` and there are changes to apply.
fn apply_domain_metadata_changes(
    domain_metadata: &mut Option<HashMap<String, DomainMetadata>>,
    changes: Vec<DomainMetadata>,
) {
    if changes.is_empty() {
        return;
    }
    let map = domain_metadata.get_or_insert_with(HashMap::new);
    for dm in changes {
        if dm.is_removed() {
            map.remove(dm.domain());
        } else {
            let domain = dm.domain().to_string();
            map.insert(domain, dm);
        }
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

        apply_domain_metadata_changes(&mut self.domain_metadata, delta.domain_metadata_changes);

        // In-commit timestamp: unconditional replace (not guarded by `if let Some`).
        // If ICT was disabled after being enabled, the delta carries None, which correctly
        // clears the previous value.
        self.in_commit_timestamp_opt = delta.in_commit_timestamp;

        // Bail if already Untrackable -- nothing can recover missing file stats.
        if self.validity == FileStatsValidity::Untrackable {
            return;
        }

        // Missing file size poisons stats permanently. Checked after the Untrackable bail-out
        // so that Untrackable can never transition to Indeterminate below.
        if delta.has_missing_file_size {
            self.validity = FileStatsValidity::Untrackable;
            return;
        }

        // Bail if already Indeterminate (theoretically recoverable via full replay).
        if self.validity == FileStatsValidity::Indeterminate {
            return;
        }

        let is_safe = delta
            .operation
            .as_deref()
            .is_some_and(FileStatsDelta::is_incremental_safe);
        if !is_safe {
            self.validity = FileStatsValidity::Indeterminate;
            return;
        }
        self.num_files += delta.file_stats.net_files;
        self.table_size_bytes += delta.file_stats.net_bytes;
    }
}

#[cfg(test)]
mod tests {
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

    fn test_protocol() -> Protocol {
        Protocol::try_new(1, 2, None::<Vec<String>>, None::<Vec<String>>).unwrap()
    }

    fn test_metadata() -> Metadata {
        use crate::actions::Format;
        Metadata::new_unchecked(
            "test-id",
            None,
            None,
            Format::default(),
            r#"{"type":"struct","fields":[]}"#,
            vec![],
            Some(1000),
            std::collections::HashMap::new(),
        )
    }

    // ===== CrcDelta::into_crc tests =====

    #[test]
    fn into_crc_with_protocol_and_metadata() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(test_metadata()),
            file_stats: FileStatsDelta {
                net_files: 3,
                net_bytes: 500,
            },
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        let crc = delta.into_crc().unwrap();
        assert_eq!(crc.protocol, test_protocol());
        assert_eq!(crc.metadata, test_metadata());
        assert_eq!(crc.num_files, 3);
        assert_eq!(crc.table_size_bytes, 500);
        assert_eq!(crc.validity, FileStatsValidity::Valid);
        assert_eq!(crc.num_metadata, 1);
        assert_eq!(crc.num_protocol, 1);
    }

    #[test]
    fn into_crc_without_protocol_returns_none() {
        let delta = CrcDelta {
            metadata: Some(test_metadata()),
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        assert!(delta.into_crc().is_none());
    }

    #[test]
    fn into_crc_without_metadata_returns_none() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        assert!(delta.into_crc().is_none());
    }

    #[test]
    fn into_crc_with_domain_metadata() {
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(test_metadata()),
            domain_metadata_changes: vec![dm],
            operation: Some("CREATE TABLE".to_string()),
            ..Default::default()
        };
        let crc = delta.into_crc().unwrap();
        let map = crc.domain_metadata.as_ref().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
    }

    #[test]
    fn into_crc_with_ict() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(test_metadata()),
            in_commit_timestamp: Some(9999),
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        let crc = delta.into_crc().unwrap();
        assert_eq!(crc.in_commit_timestamp_opt, Some(9999));
    }

    #[test]
    fn into_crc_with_missing_file_size_is_untrackable() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(test_metadata()),
            has_missing_file_size: true,
            operation: Some("WRITE".to_string()),
            ..Default::default()
        };
        let crc = delta.into_crc().unwrap();
        assert_eq!(crc.validity, FileStatsValidity::Untrackable);
    }

    #[test]
    fn into_crc_with_unsafe_op_is_indeterminate() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(test_metadata()),
            operation: Some("ANALYZE STATS".to_string()),
            ..Default::default()
        };
        let crc = delta.into_crc().unwrap();
        assert_eq!(crc.validity, FileStatsValidity::Indeterminate);
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
        assert_eq!(crc.validity, FileStatsValidity::Valid);
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
        assert_eq!(crc.validity, FileStatsValidity::Valid);
    }

    /// Simulates forward log replay: apply multiple commit deltas sequentially.
    #[test]
    fn test_apply_multiple_deltas() {
        let mut crc = base_crc();
        crc.apply(write_delta(3, 600));
        crc.apply(write_delta(-2, -400));
        assert_eq!(crc.num_files, 11); // 10 + 3 - 2
        assert_eq!(crc.table_size_bytes, 1200); // 1000 + 600 - 400
        assert_eq!(crc.validity, FileStatsValidity::Valid);
    }

    #[test]
    fn test_apply_unsafe_op_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.validity, FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_apply_none_op_transitions_to_indeterminate() {
        let mut crc = base_crc();
        let unknown_delta = CrcDelta {
            operation: None,
            ..write_delta(1, 100)
        };
        crc.apply(unknown_delta);
        assert_eq!(crc.validity, FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_indeterminate_stays_indeterminate() {
        let mut crc = base_crc();
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.validity, FileStatsValidity::Indeterminate);

        // Subsequent safe op doesn't recover validity.
        crc.apply(write_delta(5, 500));
        assert_eq!(crc.validity, FileStatsValidity::Indeterminate);
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
        assert_eq!(crc.validity, FileStatsValidity::Untrackable);
    }

    #[test]
    fn test_untrackable_stays_untrackable() {
        let mut crc = base_crc();
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.validity, FileStatsValidity::Untrackable);

        // Applying a safe delta does not recover from Untrackable.
        crc.apply(write_delta(5, 500));
        assert_eq!(crc.validity, FileStatsValidity::Untrackable);

        // Applying an unsafe delta also stays Untrackable (does not downgrade to Indeterminate).
        crc.apply(CrcDelta {
            operation: None,
            ..write_delta(1, 100)
        });
        assert_eq!(crc.validity, FileStatsValidity::Untrackable);
    }

    #[test]
    fn test_indeterminate_transitions_to_untrackable_on_missing_size() {
        let mut crc = base_crc();
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        crc.apply(unsafe_change);
        assert_eq!(crc.validity, FileStatsValidity::Indeterminate);

        // Missing size escalates Indeterminate to Untrackable.
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        crc.apply(delta);
        assert_eq!(crc.validity, FileStatsValidity::Untrackable);
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
    fn test_apply_adds_domain_metadata() {
        let mut crc = base_crc();
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
}
