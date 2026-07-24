//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use std::sync::Arc;

use crate::crc::Crc;
use crate::utils::require;
use crate::{DeltaResult, Error, Version};

/// The newest [`Crc`] a snapshot resolved, at or before the snapshot's version. It is one CRC,
/// read from disk (or computed) at most once; keeping it lets later queries and CRC writes reuse
/// the already-parsed state instead of re-reading it. A `Some(crc)` is one of:
/// - at the snapshot version: authoritative, servable at zero I/O via [`Self::at_version`];
/// - stale (older): kept only as a base for [`Self::base`], never served as authoritative.
///
/// [`Snapshot`]: super::Snapshot
#[derive(Debug)]
pub(super) struct SnapshotCrc {
    crc: Option<Arc<Crc>>,
    snapshot_version: Version,
}

impl SnapshotCrc {
    /// Wrap the CRC a snapshot at `snapshot_version` resolved. A `Some(crc)` must sit in
    /// `[checkpoint_version, snapshot_version]`.
    pub(super) fn try_new(
        crc: Option<Arc<Crc>>,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
    ) -> DeltaResult<Self> {
        if let Some(crc) = crc.as_ref() {
            require!(
                crc.version <= snapshot_version,
                Error::internal_error(format!(
                    "CRC version {} is ahead of snapshot version {snapshot_version}",
                    crc.version
                ))
            );
            require!(
                checkpoint_version.is_none_or(|ckpt| crc.version >= ckpt),
                Error::internal_error(format!(
                    "CRC version {} is below checkpoint version {checkpoint_version:?}",
                    crc.version
                ))
            );
        }
        Ok(Self {
            crc,
            snapshot_version,
        })
    }

    /// The CRC iff it sits exactly at the snapshot version, so its cached values are authoritative
    /// at zero I/O. Returns `None` for a stale CRC.
    pub(super) fn at_version(&self) -> Option<&Arc<Crc>> {
        self.crc
            .as_ref()
            .filter(|crc| crc.version == self.snapshot_version)
    }

    /// The held CRC, at-version or stale, for callers that reuse it regardless of version
    /// (e.g. checksum writes).
    pub(super) fn base(&self) -> Option<&Arc<Crc>> {
        self.crc.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::crc::{FileStats, FileStatsState};

    fn crc_at(version: Version) -> Arc<Crc> {
        Arc::new(Crc {
            version,
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 1,
                table_size_bytes: 100,
                file_size_histogram: None,
            }),
            ..Default::default()
        })
    }

    #[test]
    fn new_rejects_crc_ahead_of_snapshot() {
        assert_result_error_with_message(
            SnapshotCrc::try_new(Some(crc_at(6)), 5, None),
            "is ahead of snapshot version",
        );
    }

    #[test]
    fn new_rejects_crc_below_checkpoint() {
        assert_result_error_with_message(
            SnapshotCrc::try_new(Some(crc_at(2)), 5, Some(3)),
            "is below checkpoint version",
        );
    }

    #[test]
    fn new_accepts_crc_at_checkpoint() {
        let holder = SnapshotCrc::try_new(Some(crc_at(3)), 5, Some(3)).unwrap();
        assert_eq!(holder.base().map(|c| c.version), Some(3));
    }

    #[test]
    fn at_version_serves_crc_matching_snapshot_version() {
        let holder = SnapshotCrc::try_new(Some(crc_at(5)), 5, None).unwrap();
        assert_eq!(holder.at_version().map(|c| c.version), Some(5));
        assert_eq!(holder.base().map(|c| c.version), Some(5));
    }

    #[test]
    fn stale_crc_is_base_but_not_at_version() {
        let holder = SnapshotCrc::try_new(Some(crc_at(3)), 5, None).unwrap();
        assert!(holder.at_version().is_none());
        assert_eq!(holder.base().map(|c| c.version), Some(3));
    }

    #[test]
    fn none_holder_serves_nothing() {
        let holder = SnapshotCrc::try_new(None, 5, None).unwrap();
        assert!(holder.at_version().is_none());
        assert!(holder.base().is_none());
    }
}
