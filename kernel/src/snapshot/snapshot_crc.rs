//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use std::sync::Arc;

use crate::crc::{Crc, CrcWrapper};
use crate::utils::require;
use crate::{KernelError, KernelResult, Version};

/// How the in-memory checksum relates to an observed checksum file and the snapshot version.
///
/// Connectors reporting checksum availability can group the variants as follows:
/// - Usable checksum at the snapshot version: [`AtVersion`](Self::AtVersion) and
///   [`Reconstructed`](Self::Reconstructed).
/// - No usable checksum resolved at the snapshot version: [`Stale`](Self::Stale),
///   [`Missing`](Self::Missing), and [`Unusable`](Self::Unusable). Each variant describes only the
///   versions loading considered.
/// - Not established: [`Unknown`](Self::Unknown).
///
/// The value records observations made while resolving the snapshot, not the current contents of
/// storage. An in-memory CRC at the snapshot version does not establish that a checksum file exists
/// on disk at that version.
///
/// The source is independent of table-level file statistics: a snapshot can have a usable checksum
/// that lacks complete file statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrcSource {
    /// A usable on-disk checksum file was resolved at the snapshot version.
    AtVersion {
        /// Version of both the checksum file and the in-memory CRC, equal to the snapshot version.
        crc_version: Version,
    },
    /// An older on-disk checksum was advanced in memory to the snapshot version using committed
    /// actions.
    Reconstructed {
        /// Version of the original on-disk checksum, preserved across in-memory updates.
        /// Subtract this from `crc_version` to obtain the cumulative number of incorporated
        /// commits.
        on_disk_version: Version,
        /// Version of the in-memory CRC, equal to the snapshot version.
        crc_version: Version,
    },
    /// A usable in-memory checksum is older than the snapshot version.
    /// It may already incorporate commits replayed after its original on-disk version.
    Stale {
        /// Version of the on-disk checksum from which the in-memory CRC was read or reconstructed.
        /// Subtract this from `crc_version` to obtain the cumulative number of incorporated
        /// commits.
        on_disk_version: Version,
        /// Version of the in-memory CRC, strictly less than the snapshot version.
        /// Subtract this from the snapshot version to obtain the remaining version gap.
        crc_version: Version,
    },
    /// Loading found no checksum candidate in its listed version range and kept no in-memory
    /// CRC. Fresh loads consider the selected checkpoint version (or zero without a checkpoint)
    /// through the snapshot version, inclusive. Incremental loads may list only after the previous
    /// checkpoint, or after the previous snapshot when new checkpoints are ignored. Files below
    /// the selected checkpoint are not candidates. This does not describe unlisted versions.
    Missing,
    /// An observed checksum candidate was unusable and no usable baseline was resolved.
    /// This includes read failures as well as invalid checksum contents.
    Unusable,
    /// No observation of an on-disk checksum is available. Examples include snapshots constructed
    /// directly by CREATE TABLE without a checksum-file lookup, connector hints without
    /// provenance, and derived snapshots that discard a CRC at a checkpoint. It does not indicate
    /// that loading searched for a checksum and failed to find or read it.
    Unknown,
}

/// The newest [`Crc`] a snapshot resolved, at or before the snapshot's version. It is one CRC,
/// read from disk (or computed) at most once; keeping it lets later queries and CRC writes reuse
/// the already-parsed state instead of re-reading it. A retained CRC is one of:
/// - at the snapshot version: authoritative, servable at zero I/O via [`Self::at_version`];
/// - stale (older): kept only as a base for [`Self::base`], never served as authoritative.
///
/// The wrapper carries the original checksum-file version. The snapshot derives its source from
/// that origin and records lookup outcomes when no CRC is present.
///
/// [`Snapshot`]: super::Snapshot
#[derive(Debug, Clone)]
pub(super) struct SnapshotCrc {
    crc: CrcWrapper,
    snapshot_version: Version,
}

impl SnapshotCrc {
    /// Wrap the CRC a snapshot at `snapshot_version` resolved. A retained CRC must sit in
    /// `[checkpoint_version, snapshot_version]`. Its original on-disk version may predate the
    /// checkpoint but cannot exceed the retained CRC version.
    pub(super) fn try_new(
        crc: CrcWrapper,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
    ) -> KernelResult<Self> {
        if let CrcWrapper::Present {
            crc,
            on_disk_version,
        } = &crc
        {
            if let Some(on_disk_version) = on_disk_version {
                require!(
                    *on_disk_version <= crc.version,
                    KernelError::internal_error(format!(
                        "On-disk CRC version {on_disk_version} is ahead of retained CRC version {}",
                        crc.version
                    ))
                );
            }
            require!(
                crc.version <= snapshot_version,
                KernelError::internal_error(format!(
                    "CRC version {} is ahead of snapshot version {snapshot_version}",
                    crc.version
                ))
            );
            require!(
                checkpoint_version.is_none_or(|ckpt| crc.version >= ckpt),
                KernelError::internal_error(format!(
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

    pub(super) fn source(&self) -> CrcSource {
        match &self.crc {
            CrcWrapper::Present {
                crc,
                on_disk_version: Some(on_disk_version),
            } => {
                let crc_version = crc.version;
                let on_disk_version = *on_disk_version;
                if crc_version < self.snapshot_version {
                    CrcSource::Stale {
                        on_disk_version,
                        crc_version,
                    }
                } else if on_disk_version < crc_version {
                    CrcSource::Reconstructed {
                        on_disk_version,
                        crc_version,
                    }
                } else {
                    CrcSource::AtVersion { crc_version }
                }
            }
            CrcWrapper::Present {
                on_disk_version: None,
                ..
            }
            | CrcWrapper::Unknown => CrcSource::Unknown,
            CrcWrapper::Missing => CrcSource::Missing,
            CrcWrapper::Unusable => CrcSource::Unusable,
        }
    }

    pub(super) fn retained(&self) -> &CrcWrapper {
        &self.crc
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

    #[rstest::rstest]
    #[case::ahead_of_snapshot(6, None, None, "is ahead of snapshot version")]
    #[case::below_checkpoint(2, Some(3), None, "is below checkpoint version")]
    #[case::ahead_of_retained_crc(5, None, Some(6), "is ahead of retained CRC version")]
    fn new_rejects_invalid_argument_combinations(
        #[case] crc_version: Version,
        #[case] checkpoint_version: Option<Version>,
        #[case] on_disk_version: Option<Version>,
        #[case] expected_error: &str,
    ) {
        let crc = CrcWrapper::Present {
            crc: crc_at(crc_version),
            on_disk_version,
        };
        assert_result_error_with_message(
            SnapshotCrc::try_new(crc, 5, checkpoint_version),
            expected_error,
        );
    }

    #[test]
    fn new_accepts_crc_at_checkpoint() {
        let holder = SnapshotCrc::try_new(Some(crc_at(3)).into(), 5, Some(3)).unwrap();
        assert_eq!(holder.base().map(|c| c.version), Some(3));
    }

    #[test]
    fn at_version_serves_crc_matching_snapshot_version() {
        let holder = SnapshotCrc::try_new(Some(crc_at(5)).into(), 5, None).unwrap();
        assert_eq!(holder.at_version().map(|c| c.version), Some(5));
        assert_eq!(holder.base().map(|c| c.version), Some(5));
    }

    #[test]
    fn stale_crc_is_base_but_not_at_version() {
        let holder = SnapshotCrc::try_new(Some(crc_at(3)).into(), 5, None).unwrap();
        assert!(holder.at_version().is_none());
        assert_eq!(holder.base().map(|c| c.version), Some(3));
    }

    #[test]
    fn none_holder_serves_nothing() {
        let holder = SnapshotCrc::try_new(CrcWrapper::Unknown, 5, None).unwrap();
        assert!(holder.at_version().is_none());
        assert!(holder.base().is_none());
    }

    #[test]
    fn replayed_crc_carries_origin_across_snapshot_versions_without_copying() {
        // Given a CRC read at version 10 and advanced in memory to version 13.
        let crc = CrcWrapper::from_file(crc_at(10)).map_crc(|_| crc_at(13));
        let advanced = SnapshotCrc::try_new(crc.clone(), 13, Some(12)).unwrap();
        assert_eq!(
            advanced.source(),
            CrcSource::Reconstructed {
                on_disk_version: 10,
                crc_version: 13
            }
        );
        // When a newer snapshot uses the same CRC without replaying it again.
        let stale = SnapshotCrc::try_new(crc, 14, Some(12)).unwrap();

        // Then classification changes without copying or changing the CRC or its disk origin.
        assert_eq!(
            stale.source(),
            CrcSource::Stale {
                on_disk_version: 10,
                crc_version: 13
            }
        );
        assert!(stale.at_version().is_none());
        assert!(Arc::ptr_eq(
            stale.base().unwrap(),
            advanced.at_version().unwrap()
        ));
    }
}
