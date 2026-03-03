// No consumers yet -- will be integrated in a follow-up PR.
#![allow(dead_code)]
//! CRC commit delta types and replay context.
//!
//! [`CrcDelta`] captures all CRC-relevant changes from a single commit: protocol, metadata,
//! domain metadata, in-commit timestamp, and file stats.
//!
//! [`CrcContext`] wraps a [`Crc`] with a [`FileStatsValidity`] tag to track whether
//! incremental file stats are trustworthy during forward log replay.

use std::collections::HashMap;

use crate::actions::{DomainMetadata, Metadata, Protocol};

use super::file_stats::FileStatsDelta;
use super::Crc;

/// The CRC-relevant changes ("delta") from a single commit. Produced either by reading a
/// `.json` commit file during log replay, or from in-memory transaction state during writes.
#[derive(Debug, Clone)]
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
}

/// Tracks whether file stats (`num_files`, `table_size_bytes`) are trustworthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileStatsValidity {
    /// CRC-rooted: file stats are correct totals from a valid CRC file.
    Valid,
    /// No CRC base: file stats are only deltas accumulated from zero.
    Incomplete,
    /// A non-incremental operation was seen: file stats cannot be determined incrementally.
    Indeterminate,
}

/// Wrapper around [`Crc`] that tracks file stats validity.
///
/// A `CrcContext` is created representing a known state, then [`apply`](Self::apply) advances it
/// forward one commit at a time -- during log replay, after a transaction commit, or (in the
/// future) during conflict resolution/rebasing. Replay always starts at the commit *after* the
/// context's starting point:
///
/// - [`from_crc`](Self::from_crc): context is at the CRC's version, replay starts after.
/// - [`for_zero_rooted_table`](Self::for_zero_rooted_table): context is before any version, replay
///   starts at 000.json.
/// - [`from_checkpoint`](Self::from_checkpoint): context is at the checkpoint version, replay
///   starts after.
///
/// Non-file-stats fields (e.g. protocol, metadata, domain metadata, set transactions, ICT) are
/// always kept up-to-date. File stats (`num_files`, `table_size_bytes`) are only updated when
/// the operation is known to be incremental-safe.
#[derive(Debug, Clone)]
pub(crate) struct CrcContext {
    /// The CRC state being built up during replay.
    pub(crate) crc: Crc,
    /// Whether the file stats in `crc` are trustworthy.
    pub(crate) validity: FileStatsValidity,
}

impl CrcContext {
    /// CRC-rooted: file stats are known-correct.
    pub(crate) fn from_crc(crc: Crc) -> Self {
        Self {
            crc,
            validity: FileStatsValidity::Valid,
        }
    }

    /// No checkpoint, no CRC. Replay begins at 000.json, which will establish P/M and initial
    /// file stats via the first [`apply`](Self::apply) call.
    pub(crate) fn for_zero_rooted_table() -> Self {
        Self {
            crc: Crc::default(),
            validity: FileStatsValidity::Valid,
        }
    }

    /// Checkpoint-rooted: P/M/DM are seeded from the checkpoint, but file stats are unknown.
    pub(crate) fn from_checkpoint(mut seed: Crc) -> Self {
        seed.num_files = 0;
        seed.table_size_bytes = 0;
        Self {
            crc: seed,
            validity: FileStatsValidity::Incomplete,
        }
    }

    /// Apply a commit delta, updating all CRC fields and adjusting file stats validity.
    ///
    /// Non-file-stats fields are always updated. File stats are only updated if validity is
    /// not already [`FileStatsValidity::Indeterminate`] and the operation is incremental-safe.
    pub(crate) fn apply(&mut self, delta: &CrcDelta) {
        // Protocol and metadata: replace if present.
        if let Some(ref p) = delta.protocol {
            self.crc.protocol = p.clone();
        }
        if let Some(ref m) = delta.metadata {
            self.crc.metadata = m.clone();
        }

        // Domain metadata: insert or remove by domain name.
        if !delta.domain_metadata_changes.is_empty() {
            let map = self.crc.domain_metadata.get_or_insert_with(HashMap::new);
            for dm in &delta.domain_metadata_changes {
                if dm.is_removed() {
                    map.remove(dm.domain());
                } else {
                    map.insert(dm.domain().to_string(), dm.clone());
                }
            }
        }

        // In-commit timestamp: replace if present.
        if let Some(ict) = delta.in_commit_timestamp {
            self.crc.in_commit_timestamp_opt = Some(ict);
        }

        // File stats: skip if already indeterminate.
        if matches!(self.validity, FileStatsValidity::Indeterminate) {
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
        self.crc.num_files += delta.file_stats.net_files;
        self.crc.table_size_bytes += delta.file_stats.net_bytes;
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
            protocol: None,
            metadata: None,
            domain_metadata_changes: vec![],
            in_commit_timestamp: None,
            operation: Some("WRITE".to_string()),
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

    // ===== CrcContext::from_crc tests =====

    #[test]
    fn test_from_crc_preserves_stats() {
        let crc = base_crc();
        let ctx = CrcContext::from_crc(crc.clone());
        assert_eq!(ctx.validity, FileStatsValidity::Valid);
        assert_eq!(ctx.crc.num_files, 10);
        assert_eq!(ctx.crc.table_size_bytes, 1000);
    }

    // ===== CrcContext::for_zero_rooted_table tests =====

    #[test]
    fn test_for_zero_rooted_table_zeros_stats_and_is_valid() {
        let ctx = CrcContext::for_zero_rooted_table();
        assert_eq!(ctx.validity, FileStatsValidity::Valid);
        assert_eq!(ctx.crc.num_files, 0);
        assert_eq!(ctx.crc.table_size_bytes, 0);
    }

    // ===== CrcContext::from_checkpoint tests =====

    #[test]
    fn test_from_checkpoint_zeros_stats_and_is_incomplete() {
        let ctx = CrcContext::from_checkpoint(base_crc());
        assert_eq!(ctx.validity, FileStatsValidity::Incomplete);
        assert_eq!(ctx.crc.num_files, 0);
        assert_eq!(ctx.crc.table_size_bytes, 0);
    }

    // ===== CrcContext::apply tests =====

    #[test]
    fn test_apply_updates_file_stats() {
        let mut ctx = CrcContext::from_crc(base_crc());
        ctx.apply(&write_delta(3, 600));
        assert_eq!(ctx.crc.num_files, 13); // 10 + 3
        assert_eq!(ctx.crc.table_size_bytes, 1600); // 1000 + 600
        assert_eq!(ctx.validity, FileStatsValidity::Valid);
    }

    /// Simulates forward log replay: apply multiple commit deltas sequentially.
    #[test]
    fn test_apply_multiple_deltas() {
        let mut ctx = CrcContext::from_crc(base_crc());
        ctx.apply(&write_delta(3, 600));
        ctx.apply(&write_delta(-2, -400));
        assert_eq!(ctx.crc.num_files, 11); // 10 + 3 - 2
        assert_eq!(ctx.crc.table_size_bytes, 1200); // 1000 + 600 - 400
        assert_eq!(ctx.validity, FileStatsValidity::Valid);
    }

    #[test]
    fn test_apply_unsafe_op_transitions_to_indeterminate() {
        let mut ctx = CrcContext::from_crc(base_crc());
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        ctx.apply(&unsafe_change);
        assert_eq!(ctx.validity, FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_apply_none_op_transitions_to_indeterminate() {
        let mut ctx = CrcContext::from_crc(base_crc());
        let unknown_delta = CrcDelta {
            operation: None,
            ..write_delta(1, 100)
        };
        ctx.apply(&unknown_delta);
        assert_eq!(ctx.validity, FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_indeterminate_stays_indeterminate() {
        let mut ctx = CrcContext::from_crc(base_crc());
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        ctx.apply(&unsafe_change);
        assert_eq!(ctx.validity, FileStatsValidity::Indeterminate);

        // Subsequent safe op doesn't recover validity.
        ctx.apply(&write_delta(5, 500));
        assert_eq!(ctx.validity, FileStatsValidity::Indeterminate);
    }

    #[test]
    fn test_checkpoint_stays_incomplete_on_safe_ops() {
        let mut ctx = CrcContext::from_checkpoint(base_crc());
        ctx.apply(&write_delta(3, 600));
        assert_eq!(ctx.validity, FileStatsValidity::Incomplete);
        assert_eq!(ctx.crc.num_files, 3); // 0 + 3
        assert_eq!(ctx.crc.table_size_bytes, 600); // 0 + 600
    }

    #[test]
    fn test_checkpoint_transitions_to_indeterminate_on_unsafe_op() {
        let mut ctx = CrcContext::from_checkpoint(base_crc());
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        ctx.apply(&unsafe_change);
        assert_eq!(ctx.validity, FileStatsValidity::Indeterminate);
    }

    // ===== apply: non-file-stats field updates =====

    #[test]
    fn test_apply_replaces_protocol() {
        let mut ctx = CrcContext::from_crc(base_crc());
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
        ctx.apply(&delta);
        assert_eq!(ctx.crc.protocol, new_protocol);
        assert_eq!(ctx.crc.metadata, Metadata::default()); // unchanged
    }

    #[test]
    fn test_apply_adds_domain_metadata() {
        let mut ctx = CrcContext::from_crc(base_crc());
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        ctx.apply(&delta);

        let map = ctx.crc.domain_metadata.as_ref().unwrap();
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
        let mut ctx = CrcContext::from_crc(crc);

        let dm = DomainMetadata::new("my.domain".to_string(), "new_config".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        ctx.apply(&delta);

        let map = ctx.crc.domain_metadata.as_ref().unwrap();
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
        let mut ctx = CrcContext::from_crc(crc);

        let dm = DomainMetadata::remove("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            domain_metadata_changes: vec![dm],
            ..write_delta(0, 0)
        };
        ctx.apply(&delta);

        let map = ctx.crc.domain_metadata.as_ref().unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn test_apply_replaces_in_commit_timestamp() {
        let mut ctx = CrcContext::from_crc(base_crc());
        let delta = CrcDelta {
            in_commit_timestamp: Some(9999),
            ..write_delta(0, 0)
        };
        ctx.apply(&delta);
        assert_eq!(ctx.crc.in_commit_timestamp_opt, Some(9999));
    }
}
