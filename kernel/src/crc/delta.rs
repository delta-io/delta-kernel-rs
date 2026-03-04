// No consumers yet -- will be integrated in a follow-up PR.
#![allow(dead_code)]
//! CRC commit delta types and replay context.
//!
//! A CRC tracks two categories of fields:
//! - **Metadata fields**: protocol, metadata, domain metadata, in-commit timestamp (and
//!   eventually set transactions). Always kept up-to-date during replay regardless of file
//!   stats validity.
//! - **File stats fields**: `num_files` and `table_size_bytes`. Only updated when the operation
//!   is incremental-safe and current file stats validity permits it (e.g. an ANALYZE STATS
//!   commit breaks validity because it re-adds files without removing them).
//!
//! [`CrcDelta`] captures changes from a single commit across both categories.
//! [`CrcContext`] wraps a [`Crc`] with a [`FileStatsValidity`] tag to track whether
//! file stats are trustworthy during forward log replay.

use std::collections::HashMap;

use crate::actions::{DomainMetadata, Metadata, Protocol};

use super::file_stats::FileStatsDelta;
use super::Crc;

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

/// Tracks whether file stats (`num_files`, `table_size_bytes`) are trustworthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileStatsValidity {
    /// File stats are known-correct absolute totals. This is the case when seeded from a CRC
    /// file (which contains `num_files` and `table_size_bytes`) or when replay starts from
    /// version zero (where the initial state is trivially zero).
    Valid,
    /// File stats are relative deltas, not absolute totals. This happens when seeding from a
    /// checkpoint: we extract metadata fields but not file counts (reading all add actions from
    /// a checkpoint just for counts is too expensive). The accumulated deltas are correct, but
    /// without a baseline they cannot produce final totals.
    Incomplete,
    /// A non-incremental operation was seen: file stats cannot be determined incrementally.
    /// For example, ANALYZE STATS re-adds existing files with updated statistics but no
    /// corresponding removes, so naively counting adds would double-count.
    /// A full log replay from scratch could recover correct file stats.
    Indeterminate,
    /// A file action had a missing size field: correct file stats are impossible to compute.
    /// For example, the Delta protocol allows `remove.size` to be null -- when encountered,
    /// we can no longer track byte totals. Unlike [`Indeterminate`](Self::Indeterminate), no
    /// amount of replay can recover the missing data.
    Untrackable,
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
/// Metadata fields are always kept up-to-date. File stats are only updated when the operation
/// is incremental-safe and current file stats validity permits it (see [`FileStatsValidity`]).
#[derive(Debug, Clone)]
pub(crate) struct CrcContext {
    /// The CRC state being built up during replay.
    pub(crate) crc: Crc,
    /// Whether the file stats in `crc` are trustworthy.
    pub(crate) validity: FileStatsValidity,
}

impl CrcContext {
    /// CRC-rooted: all fields including file stats are known-correct absolute totals.
    pub(crate) fn from_crc(crc: Crc) -> Self {
        Self {
            crc,
            validity: FileStatsValidity::Valid,
        }
    }

    /// No checkpoint, no CRC. Replay begins at 000.json, which will establish metadata fields
    /// and initial file stats via the first [`apply`](Self::apply) call.
    pub(crate) fn for_zero_rooted_table() -> Self {
        Self {
            crc: Crc::default(),
            validity: FileStatsValidity::Valid,
        }
    }

    /// Checkpoint-rooted: metadata fields are seeded from the checkpoint, but file stats are
    /// unknown.
    ///
    /// To avoid the performance cost of reading all add/remove file actions from the checkpoint,
    /// we read only metadata fields (not file counts). Cost varies by checkpoint format:
    /// - Classic (single parquet): must scan the full file (metadata fields are mixed in with
    ///   add/remove actions)
    /// - V2 single-file: same as classic
    /// - V2 manifest + sidecars: only the manifest needs reading (sidecars contain only
    ///   add/remove actions)
    /// - Multi-part classic: must scan all parts, which can be expensive for large tables
    ///
    /// TODO(#2010): skip sidecar reads for V2 manifest checkpoints.
    /// TODO(#2011): for single-file checkpoints, also extract file stats to produce Valid
    ///              instead of Incomplete.
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
    /// Metadata fields are always updated. File stats are only updated when:
    /// - Validity is not already terminal ([`Untrackable`](FileStatsValidity::Untrackable) or
    ///   [`Indeterminate`](FileStatsValidity::Indeterminate))
    /// - The delta has no missing file sizes
    /// - The operation is incremental-safe
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

        // File stats: Untrackable is terminal -- nothing can recover missing data.
        if matches!(self.validity, FileStatsValidity::Untrackable) {
            return;
        }

        // Missing file size poisons stats permanently.
        if delta.has_missing_file_size {
            self.validity = FileStatsValidity::Untrackable;
            return;
        }

        // Indeterminate is also terminal (though theoretically recoverable via full replay).
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

    // ===== apply: Untrackable (missing file size) tests =====

    #[test]
    fn test_missing_file_size_transitions_to_untrackable() {
        let mut ctx = CrcContext::from_crc(base_crc());
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        ctx.apply(&delta);
        assert_eq!(ctx.validity, FileStatsValidity::Untrackable);
    }

    #[test]
    fn test_untrackable_stays_untrackable() {
        let mut ctx = CrcContext::from_crc(base_crc());
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        ctx.apply(&delta);
        assert_eq!(ctx.validity, FileStatsValidity::Untrackable);

        // Neither safe ops nor unsafe ops recover from Untrackable.
        ctx.apply(&write_delta(5, 500));
        assert_eq!(ctx.validity, FileStatsValidity::Untrackable);
    }

    #[test]
    fn test_indeterminate_transitions_to_untrackable_on_missing_size() {
        let mut ctx = CrcContext::from_crc(base_crc());
        let unsafe_change = CrcDelta {
            operation: Some("ANALYZE STATS".to_string()),
            ..write_delta(1, 100)
        };
        ctx.apply(&unsafe_change);
        assert_eq!(ctx.validity, FileStatsValidity::Indeterminate);

        // Missing size escalates Indeterminate to Untrackable.
        let delta = CrcDelta {
            has_missing_file_size: true,
            ..write_delta(1, 100)
        };
        ctx.apply(&delta);
        assert_eq!(ctx.validity, FileStatsValidity::Untrackable);
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
