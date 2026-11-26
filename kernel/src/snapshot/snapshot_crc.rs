//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use std::sync::Arc;

use crate::crc::{Crc, CrcDelta, CrcProvenance, CrcWithProvenance};
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

impl SnapshotCrcProvenance {
    pub(super) fn retained(self) -> Option<CrcProvenance> {
        match self {
            Self::Current(p) | Self::Stale(p) => Some(p),
            Self::Absent | Self::Unusable => None,
        }
    }
}

/// The newest [`Crc`](crate::crc::Crc) a snapshot resolved, at or before the snapshot's version. It
/// is one CRC, read from disk (or computed) at most once; keeping it lets later queries and CRC
/// writes reuse the already-parsed state instead of re-reading it. A retained CRC is one of:
/// - at the snapshot version: authoritative, servable at zero I/O via [`Self::at_version`];
/// - stale (older): kept only as a base for [`Self::base`], never served as authoritative.
///
/// A retained CRC is always paired with its `Current` or `Stale` provenance. With no CRC the
/// snapshot records whether a candidate was reported unusable.
///
/// [`Snapshot`]: super::Snapshot
#[derive(Debug, Clone)]
pub(crate) struct SnapshotCrc {
    crc: Option<Arc<Crc>>,
    provenance: SnapshotCrcProvenance,
}

impl SnapshotCrc {
    pub(crate) const ABSENT: Self = Self {
        crc: None,
        provenance: SnapshotCrcProvenance::Absent,
    };
    pub(crate) const UNUSABLE: Self = Self {
        crc: None,
        provenance: SnapshotCrcProvenance::Unusable,
    };

    /// Classifies an absent lookup using the version range's checksum-file observation.
    pub(crate) fn from_lookup(crc: Option<CrcWithProvenance>, candidate_listed: bool) -> Self {
        match crc {
            Some(crc) => crc.into(),
            None if candidate_listed => Self::UNUSABLE,
            None => Self::ABSENT,
        }
    }

    /// Wrap the CRC a snapshot at `snapshot_version` resolved. A retained CRC must sit in
    /// `[checkpoint_version, snapshot_version]`. Its original on-disk version may predate the
    /// checkpoint but cannot exceed the retained CRC version.
    pub(crate) fn try_new(
        mut state: Self,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
    ) -> KernelResult<Self> {
        let retained = state.provenance.retained();
        require!(
            state.crc.as_ref().map(|crc| crc.version)
                == retained.as_ref().map(CrcProvenance::crc_version),
            KernelError::internal_error(
                "Snapshot CRC provenance must match the retained CRC's presence and version"
            )
        );
        let Some(provenance) = retained else {
            return Ok(state);
        };
        let crc_version = provenance.crc_version();
        if let Some(on_disk_version) = provenance.on_disk_version() {
            require!(
                on_disk_version <= crc_version,
                KernelError::internal_error(format!(
                    "On-disk CRC version {on_disk_version} is ahead of retained CRC version \
                     {crc_version}"
                ))
            );
        }
        require!(
            crc_version <= snapshot_version,
            KernelError::internal_error(format!(
                "CRC version {crc_version} is ahead of snapshot version {snapshot_version}"
            ))
        );
        require!(
            checkpoint_version.is_none_or(|checkpoint| crc_version >= checkpoint),
            KernelError::internal_error(format!(
                "CRC version {crc_version} is below checkpoint version {checkpoint_version:?}"
            ))
        );
        state.provenance = if crc_version == snapshot_version {
            SnapshotCrcProvenance::Current(provenance)
        } else {
            SnapshotCrcProvenance::Stale(provenance)
        };
        Ok(state)
    }

    /// Returns the snapshot's checksum provenance without exposing the retained CRC.
    pub(crate) fn provenance(&self) -> SnapshotCrcProvenance {
        self.provenance
    }

    /// The CRC iff it sits exactly at the snapshot version, so its cached values are authoritative
    /// at zero I/O. Returns `None` for a stale CRC.
    pub(crate) fn at_version(&self) -> Option<&Arc<Crc>> {
        self.crc
            .as_ref()
            .filter(|_| matches!(self.provenance, SnapshotCrcProvenance::Current(_)))
    }

    /// The held CRC, at-version or stale, for callers that reuse it regardless of version
    /// (e.g. checksum writes).
    pub(crate) fn base(&self) -> Option<&Arc<Crc>> {
        self.crc.as_ref()
    }

    /// Shares the retained CRC and its provenance with incremental replay or a checksum write.
    pub(crate) fn base_with_provenance(&self) -> Option<CrcWithProvenance> {
        Some(CrcWithProvenance::from_parts(
            self.crc.as_ref()?.clone(),
            self.provenance.retained()?,
        ))
    }

    /// Applies a committed delta only to a current CRC.
    pub(crate) fn apply_commit(&self, delta: CrcDelta, version: Version) -> Self {
        match (&self.crc, self.provenance) {
            (Some(crc), SnapshotCrcProvenance::Current(p)) => {
                CrcWithProvenance::from_parts(crc.clone(), p)
                    .apply(delta, version)
                    .into()
            }
            _ => self.clone(),
        }
    }

    /// Omits stale CRCs when deriving snapshot hints and checkpoints.
    pub(crate) fn only_current(&self) -> Self {
        match self.provenance {
            SnapshotCrcProvenance::Stale(_) => Self::ABSENT,
            _ => self.clone(),
        }
    }
}

impl From<CrcWithProvenance> for SnapshotCrc {
    fn from(crc: CrcWithProvenance) -> Self {
        let (crc, provenance) = crc.into_parts();
        Self {
            crc: Some(crc),
            provenance: SnapshotCrcProvenance::Current(provenance),
        }
    }
}

#[cfg(test)]
mod tests {
    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::crc::{FileStats, FileStatsState};

    // Tests can supply a CRC directly, with no known disk version, as in a snapshot hint without
    // CRC provenance.
    impl From<Option<Arc<Crc>>> for SnapshotCrc {
        fn from(crc: Option<Arc<Crc>>) -> Self {
            crc.map_or(Self::ABSENT, |crc| {
                CrcWithProvenance::from_snapshot_hint(crc, None).into()
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

    #[rstest::rstest]
    #[case::missing_crc(
        None,
        SnapshotCrcProvenance::Current(CrcProvenance::File { crc_version: 5, on_disk_version: 5 }),
    )]
    #[case::unexpected_crc(Some(5), SnapshotCrcProvenance::Unusable)]
    #[case::version_mismatch(
        Some(4),
        SnapshotCrcProvenance::Stale(CrcProvenance::File { crc_version: 3, on_disk_version: 3 }),
    )]
    fn new_rejects_mismatched_crc_and_provenance(
        #[case] version: Option<Version>,
        #[case] provenance: SnapshotCrcProvenance,
    ) {
        let state = SnapshotCrc {
            crc: version.map(crc_at),
            provenance,
        };
        assert_result_error_with_message(
            SnapshotCrc::try_new(state, 5, None),
            "must match the retained CRC's presence and version",
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

    #[rstest::rstest]
    #[case(SnapshotCrc::ABSENT, SnapshotCrcProvenance::Absent)]
    #[case(SnapshotCrc::UNUSABLE, SnapshotCrcProvenance::Unusable)]
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
        let (replay_base, provenance) = stale.base_with_provenance().unwrap().into_parts();
        assert_eq!(Some(provenance), advanced.provenance().retained());
        assert!(Arc::ptr_eq(&replay_base, stale.base().unwrap()));
        assert!(stale.only_current().base().is_none());
        assert_eq!(
            stale.only_current().provenance(),
            SnapshotCrcProvenance::Absent
        );
        assert!(Arc::ptr_eq(
            stale.base().unwrap(),
            advanced.at_version().unwrap()
        ));
    }
}
