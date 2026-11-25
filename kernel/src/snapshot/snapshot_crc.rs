//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use crate::crc::{CrcDelta, CrcWithProvenance};
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

/// The newest [`Crc`](crate::crc::Crc) a snapshot resolved, at or before the snapshot's version. It
/// is one CRC, read from disk (or computed) at most once; keeping it lets later queries and CRC
/// writes reuse the already-parsed state instead of re-reading it. A retained CRC is one of:
/// - at the snapshot version: authoritative, servable at zero I/O via [`Self::at_version`];
/// - stale (older): kept only as a base for [`Self::base`], never served as authoritative.
///
/// The in-memory CRC carries its origin and any observed checksum-file version. The snapshot
/// derives its source from that information and records lookup outcomes when no CRC is present.
///
/// [`Snapshot`]: super::Snapshot
#[derive(Debug, Clone)]
pub(crate) enum SnapshotCrc {
    /// The CRC describes this snapshot's version.
    Current(CrcWithProvenance),
    /// The CRC predates this snapshot and is available only as a replay base.
    Stale(CrcWithProvenance),
    /// No checksum candidate was found in the listed range.
    Missing,
    /// An observed checksum candidate could not provide a usable baseline.
    Unusable,
    /// No payload or applicable checksum-file observation is available.
    NoObservation,
}

impl SnapshotCrc {
    /// Classifies an absent lookup using the version range's checksum-file observation.
    pub(crate) fn from_lookup(crc: Option<CrcWithProvenance>, candidate_listed: bool) -> Self {
        match crc {
            Some(crc) => Self::Current(crc),
            None if candidate_listed => Self::Unusable,
            None => Self::Missing,
        }
    }

    /// Wrap the CRC a snapshot at `snapshot_version` resolved. A retained CRC must sit in
    /// `[checkpoint_version, snapshot_version]`. Its original on-disk version may predate the
    /// checkpoint but cannot exceed the retained CRC version.
    pub(crate) fn try_new(
        state: Self,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
    ) -> KernelResult<Self> {
        let Some(crc) = state.base() else {
            return Ok(state);
        };
        if let Some(on_disk_version) = crc.on_disk_version() {
            require!(
                on_disk_version <= crc.version(),
                KernelError::internal_error(format!(
                    "On-disk CRC version {on_disk_version} is ahead of retained CRC version {}",
                    crc.version()
                ))
            );
        }
        require!(
            crc.version() <= snapshot_version,
            KernelError::internal_error(format!(
                "CRC version {} is ahead of snapshot version {snapshot_version}",
                crc.version()
            ))
        );
        require!(
            checkpoint_version.is_none_or(|checkpoint| crc.version() >= checkpoint),
            KernelError::internal_error(format!(
                "CRC version {} is below checkpoint version {checkpoint_version:?}",
                crc.version()
            ))
        );
        Ok(match state {
            Self::Current(crc) | Self::Stale(crc) if crc.version() == snapshot_version => {
                Self::Current(crc)
            }
            Self::Current(crc) | Self::Stale(crc) => Self::Stale(crc),
            absent => absent,
        })
    }

    /// Public checksum-file telemetry, flattened from snapshot state and CRC provenance.
    pub(crate) fn source(&self) -> CrcSource {
        match self {
            Self::Current(crc) | Self::Stale(crc) => {
                let Some(on_disk_version) = crc.on_disk_version() else {
                    return CrcSource::Unknown;
                };
                let crc_version = crc.version();
                match self {
                    Self::Stale(_) => CrcSource::Stale {
                        on_disk_version,
                        crc_version,
                    },
                    _ if on_disk_version < crc_version => CrcSource::Reconstructed {
                        on_disk_version,
                        crc_version,
                    },
                    _ => CrcSource::AtVersion { crc_version },
                }
            }
            Self::Missing => CrcSource::Missing,
            Self::Unusable => CrcSource::Unusable,
            Self::NoObservation => CrcSource::Unknown,
        }
    }

    /// The CRC iff it sits exactly at the snapshot version, so its cached values are authoritative
    /// at zero I/O. Returns `None` for a stale CRC.
    pub(crate) fn at_version(&self) -> Option<&CrcWithProvenance> {
        match self {
            Self::Current(crc) => Some(crc),
            _ => None,
        }
    }

    /// The held CRC, at-version or stale, for callers that reuse it regardless of version
    /// (e.g. checksum writes).
    pub(crate) fn base(&self) -> Option<&CrcWithProvenance> {
        match self {
            Self::Current(crc) | Self::Stale(crc) => Some(crc),
            _ => None,
        }
    }

    /// Applies a committed delta only to a current CRC.
    pub(crate) fn apply_commit(&self, delta: CrcDelta, version: Version) -> Self {
        match self {
            Self::Current(crc) => Self::Current(crc.apply(delta, version)),
            _ => self.clone(),
        }
    }

    /// Omits stale CRCs when deriving hints and checkpoints.
    pub(crate) fn only_current(&self) -> Self {
        match self {
            Self::Stale(_) => Self::NoObservation,
            _ => self.clone(),
        }
    }
}

impl From<CrcWithProvenance> for SnapshotCrc {
    fn from(crc: CrcWithProvenance) -> Self {
        Self::Current(crc)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::crc::{Crc, CrcDelta, FileStats, FileStatsState};

    // Tests can supply a CRC directly, with the same unknown disk origin as a connector hint.
    impl From<Option<Arc<Crc>>> for SnapshotCrc {
        fn from(crc: Option<Arc<Crc>>) -> Self {
            crc.map_or(Self::NoObservation, |crc| {
                Self::Current(CrcWithProvenance::from_snapshot_hint(crc, None))
            })
        }
    }

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
        let crc = CrcWithProvenance::from_snapshot_hint(crc_at(crc_version), on_disk_version);
        assert_result_error_with_message(
            SnapshotCrc::try_new(crc.into(), 5, checkpoint_version),
            expected_error,
        );
    }

    #[test]
    fn new_accepts_crc_at_checkpoint() {
        let holder = SnapshotCrc::try_new(Some(crc_at(3)).into(), 5, Some(3)).unwrap();
        assert_eq!(holder.base().map(|c| c.version()), Some(3));
    }

    #[test]
    fn at_version_serves_crc_matching_snapshot_version() {
        let holder = SnapshotCrc::try_new(Some(crc_at(5)).into(), 5, None).unwrap();
        assert_eq!(holder.at_version().map(|c| c.version()), Some(5));
        assert_eq!(holder.base().map(|c| c.version()), Some(5));
    }

    #[test]
    fn stale_crc_is_base_but_not_at_version() {
        let holder = SnapshotCrc::try_new(Some(crc_at(3)).into(), 5, None).unwrap();
        assert!(holder.at_version().is_none());
        assert_eq!(holder.base().map(|c| c.version()), Some(3));
    }

    #[rstest::rstest]
    #[case(SnapshotCrc::NoObservation, CrcSource::Unknown)]
    #[case(SnapshotCrc::Missing, CrcSource::Missing)]
    #[case(SnapshotCrc::Unusable, CrcSource::Unusable)]
    fn none_holder_serves_nothing(#[case] state: SnapshotCrc, #[case] source: CrcSource) {
        let holder = SnapshotCrc::try_new(state, 5, None).unwrap();
        assert_eq!(
            holder
                .apply_commit(CrcDelta::default(), 6)
                .only_current()
                .source(),
            source
        );
        assert!(holder.at_version().is_none());
        assert!(holder.base().is_none());
    }

    #[test]
    fn replayed_crc_carries_origin_across_snapshot_versions_without_copying() {
        // Given a CRC read at version 10 and advanced in memory to version 13.
        let crc = CrcWithProvenance::from_file(crc_at(10)).apply(CrcDelta::default(), 13);
        let advanced = SnapshotCrc::try_new(crc.clone().into(), 13, Some(12)).unwrap();
        assert_eq!(
            advanced.source(),
            CrcSource::Reconstructed {
                on_disk_version: 10,
                crc_version: 13
            }
        );
        // When a newer snapshot uses the same CRC without replaying it again.
        let stale = SnapshotCrc::try_new(crc.into(), 14, Some(12)).unwrap();

        // Then classification changes without copying or changing the CRC or its disk origin.
        assert_eq!(
            stale.source(),
            CrcSource::Stale {
                on_disk_version: 10,
                crc_version: 13
            }
        );
        assert!(stale.at_version().is_none());
        let stale_provenance = stale.base().unwrap().provenance();
        assert_eq!(
            stale_provenance,
            advanced.at_version().unwrap().provenance()
        );
        assert!(stale.only_current().base().is_none());
        assert_eq!(stale.only_current().source(), CrcSource::Unknown);
        assert!(Arc::ptr_eq(
            stale.base().unwrap().crc(),
            advanced.at_version().unwrap().crc()
        ));
    }
}
