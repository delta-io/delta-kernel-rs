//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use crate::crc::{CrcDelta, CrcProvenance, CrcWithProvenance};
use crate::utils::require;
use crate::{KernelError, KernelResult, Version};

/// The CRC retained by a snapshot, classified relative to the snapshot's version.
///
/// A current CRC whose known checksum-file version is older than its in-memory version
/// incorporates commits since that file. A stale CRC can also have incorporated commits.
/// CRC provenance is independent of file-statistics completeness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotCrcProvenance {
    /// The in-memory CRC describes this snapshot's version.
    Current(CrcProvenance),
    /// The in-memory CRC predates this snapshot and is available as a replay base.
    Stale(CrcProvenance),
    /// The snapshot holds no CRC and has no report of an unusable candidate. This includes a
    /// listing with no eligible checksum file, a snapshot hint without a CRC or a failure claim,
    /// and a stale CRC discarded at a checkpoint.
    Absent,
    /// A checksum candidate could not be read or validated, or a snapshot hint reported such a
    /// failure, and no CRC was retained.
    Unusable,
}

/// The newest [`Crc`](crate::crc::Crc) a snapshot resolved, at or before the snapshot's version. It
/// is one CRC, read from disk (or computed) at most once; keeping it lets later queries and CRC
/// writes reuse the already-parsed state instead of re-reading it. A retained CRC is one of:
/// - at the snapshot version: authoritative, servable at zero I/O via [`Self::at_version`];
/// - stale (older): kept only as a base for [`Self::base`], never served as authoritative.
///
/// The in-memory CRC carries its origin and any observed checksum-file version. The snapshot
/// derives its status from that information. With no CRC it records whether a candidate was
/// reported unusable.
///
/// [`Snapshot`]: super::Snapshot
#[derive(Debug, Clone)]
pub(crate) enum SnapshotCrc {
    /// The CRC describes this snapshot's version.
    Current(CrcWithProvenance),
    /// The CRC predates this snapshot and is available only as a replay base.
    Stale(CrcWithProvenance),
    /// The snapshot holds no in-memory CRC and has no report of an unusable checksum candidate.
    Absent,
    /// The storage lookup or snapshot hint reported an unusable candidate, and no CRC was retained.
    Unusable,
}

impl SnapshotCrc {
    /// Classifies an absent lookup using the version range's checksum-file observation.
    pub(crate) fn from_lookup(crc: Option<CrcWithProvenance>, candidate_listed: bool) -> Self {
        match crc {
            Some(crc) => Self::Current(crc),
            None if candidate_listed => Self::Unusable,
            None => Self::Absent,
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

    /// Returns the snapshot's checksum provenance without exposing the retained CRC.
    pub(crate) fn provenance(&self) -> SnapshotCrcProvenance {
        match self {
            Self::Current(crc) => SnapshotCrcProvenance::Current(crc.provenance()),
            Self::Stale(crc) => SnapshotCrcProvenance::Stale(crc.provenance()),
            Self::Absent => SnapshotCrcProvenance::Absent,
            Self::Unusable => SnapshotCrcProvenance::Unusable,
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

    /// Omits stale CRCs when deriving snapshot hints and checkpoints.
    pub(crate) fn only_current(&self) -> Self {
        match self {
            Self::Stale(_) => Self::Absent,
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

    // Tests can supply a CRC directly, with no known disk version, as in a snapshot hint without
    // CRC provenance.
    impl From<Option<Arc<Crc>>> for SnapshotCrc {
        fn from(crc: Option<Arc<Crc>>) -> Self {
            crc.map_or(Self::Absent, |crc| {
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
    #[case(SnapshotCrc::Absent, SnapshotCrcProvenance::Absent)]
    #[case(SnapshotCrc::Unusable, SnapshotCrcProvenance::Unusable)]
    fn none_holder_serves_nothing(
        #[case] state: SnapshotCrc,
        #[case] status: SnapshotCrcProvenance,
    ) {
        let holder = SnapshotCrc::try_new(state, 5, None).unwrap();
        assert_eq!(
            holder
                .apply_commit(CrcDelta::default(), 6)
                .only_current()
                .provenance(),
            status
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
            advanced.provenance(),
            SnapshotCrcProvenance::Current(CrcProvenance::File {
                crc_version: 13,
                on_disk_version: 10
            })
        );
        // When a newer snapshot uses the same CRC without replaying it again.
        let stale = SnapshotCrc::try_new(crc.into(), 14, Some(12)).unwrap();

        // Then classification changes without copying or changing the CRC or its disk origin.
        assert_eq!(
            stale.provenance(),
            SnapshotCrcProvenance::Stale(CrcProvenance::File {
                crc_version: 13,
                on_disk_version: 10
            })
        );
        assert!(stale.at_version().is_none());
        let stale_provenance = stale.base().unwrap().provenance();
        assert_eq!(
            stale_provenance,
            advanced.at_version().unwrap().provenance()
        );
        assert!(stale.only_current().base().is_none());
        assert_eq!(
            stale.only_current().provenance(),
            SnapshotCrcProvenance::Absent
        );
        assert!(Arc::ptr_eq(
            stale.base().unwrap().crc(),
            advanced.at_version().unwrap().crc()
        ));
    }
}
