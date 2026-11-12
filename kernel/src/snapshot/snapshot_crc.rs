//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use std::sync::Arc;

use super::{CrcProvenance, CrcSource};
use crate::crc::Crc;
use crate::utils::require;
use crate::{KernelError, KernelResult, Version};

/// The newest [`Crc`] a snapshot resolved, at or before the snapshot's version. It is one CRC,
/// read from disk (or computed) at most once; keeping it lets later queries and CRC writes reuse
/// the already-parsed state instead of re-reading it. A `Some(crc)` is one of:
/// - at the snapshot version: authoritative, servable at zero I/O via [`Self::at_version`];
/// - stale (older): available through [`Self::retained`] for reuse, never served as authoritative.
///
/// [`Snapshot`]: super::Snapshot
#[derive(Debug, Clone)]
pub(super) struct SnapshotCrc {
    crc: Option<Arc<Crc>>,
    snapshot_version: Version,
    provenance: CrcProvenance,
}

impl SnapshotCrc {
    /// Wrap the CRC a snapshot at `snapshot_version` resolved. A `Some(crc)` must sit in
    /// `[checkpoint_version, snapshot_version]`, and its provenance must agree with its version
    /// and the original disk baseline.
    pub(super) fn try_new(
        crc: Option<Arc<Crc>>,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
        provenance: CrcProvenance,
    ) -> KernelResult<Self> {
        if let Some(crc) = crc.as_ref() {
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
        Self::validate_provenance(crc.as_deref(), snapshot_version, provenance)?;
        Ok(Self {
            crc,
            snapshot_version,
            provenance,
        })
    }

    /// Checks the checksum's provenance independently of its log-segment bounds.
    pub(super) fn validate_provenance(
        crc: Option<&Crc>,
        snapshot_version: Version,
        provenance: CrcProvenance,
    ) -> KernelResult<()> {
        let valid_provenance = match (provenance.source, provenance.base_version, crc) {
            (CrcSource::Unknown, None, _) => true,
            (CrcSource::Missing | CrcSource::Unusable, None, None) => true,
            (CrcSource::AtVersion, Some(base_version), Some(crc)) => {
                base_version == snapshot_version && crc.version == snapshot_version
            }
            (CrcSource::Stale, Some(base_version), Some(crc)) => {
                base_version <= crc.version && crc.version < snapshot_version
            }
            (CrcSource::Reconstructed { num_commits }, Some(base_version), Some(crc)) => {
                base_version < snapshot_version
                    && crc.version == snapshot_version
                    && num_commits == snapshot_version - base_version
            }
            _ => false,
        };
        require!(
            valid_provenance,
            KernelError::internal_error("Checksum provenance conflicts with retained checksum")
        );
        Ok(())
    }

    pub(super) fn provenance(&self) -> CrcProvenance {
        self.provenance
    }

    /// Updates provenance for a derived snapshot, preserving the original disk baseline.
    /// Dropping a known checksum also drops its provenance.
    pub(super) fn updated_provenance(
        &self,
        crc: Option<&Arc<Crc>>,
        snapshot_version: Version,
    ) -> CrcProvenance {
        Self::provenance_for_crc(self.provenance, crc, snapshot_version)
    }

    /// Resolves provenance after choosing and optionally advancing a replay baseline.
    /// `LogSegment::pick_latest_base_crc` preserves the in-memory Arc's identity when it wins.
    pub(super) fn resolved_provenance(
        base: Option<&Arc<Crc>>,
        previous: Option<&Self>,
        crc_at_version: Option<&Arc<Crc>>,
        snapshot_version: Version,
        candidate_present: bool,
    ) -> CrcProvenance {
        let provenance = match base {
            Some(base) => previous
                .filter(|old| old.retained().is_some_and(|crc| Arc::ptr_eq(crc, base)))
                .map_or(
                    CrcProvenance::new(CrcSource::AtVersion, Some(base.version)),
                    Self::provenance,
                ),
            None if candidate_present => CrcProvenance::new(CrcSource::Unusable, None),
            None => CrcProvenance::new(CrcSource::Missing, None),
        };
        Self::provenance_for_crc(provenance, crc_at_version.or(base), snapshot_version)
    }

    /// The CRC iff it sits exactly at the snapshot version, so its cached values are authoritative
    /// at zero I/O. Returns `None` for a stale CRC.
    pub(super) fn at_version(&self) -> Option<&Arc<Crc>> {
        self.crc
            .as_ref()
            .filter(|crc| crc.version == self.snapshot_version)
    }

    /// Returns the CRC kept in memory, whether at the snapshot version or stale.
    /// It may have been read from disk or advanced by replaying commits.
    pub(super) fn retained(&self) -> Option<&Arc<Crc>> {
        self.crc.as_ref()
    }

    fn provenance_for_crc(
        provenance: CrcProvenance,
        crc: Option<&Arc<Crc>>,
        snapshot_version: Version,
    ) -> CrcProvenance {
        match (provenance.base_version, crc) {
            (Some(base_version), Some(crc)) => {
                let source = if crc.version < snapshot_version {
                    CrcSource::Stale
                } else if base_version == snapshot_version {
                    CrcSource::AtVersion
                } else {
                    CrcSource::Reconstructed {
                        num_commits: snapshot_version - base_version,
                    }
                };
                CrcProvenance::new(source, Some(base_version))
            }
            (None, None) => provenance,
            _ => CrcProvenance::default(),
        }
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
            SnapshotCrc::try_new(Some(crc_at(6)), 5, None, CrcProvenance::default()),
            "is ahead of snapshot version",
        );
    }

    #[test]
    fn new_rejects_crc_below_checkpoint() {
        assert_result_error_with_message(
            SnapshotCrc::try_new(Some(crc_at(2)), 5, Some(3), CrcProvenance::default()),
            "is below checkpoint version",
        );
    }

    #[test]
    fn new_accepts_crc_at_checkpoint() {
        let holder =
            SnapshotCrc::try_new(Some(crc_at(3)), 5, Some(3), CrcProvenance::default()).unwrap();
        assert_eq!(holder.retained().map(|c| c.version), Some(3));
    }

    #[test]
    fn at_version_serves_crc_matching_snapshot_version() {
        let holder =
            SnapshotCrc::try_new(Some(crc_at(5)), 5, None, CrcProvenance::default()).unwrap();
        assert_eq!(holder.at_version().map(|c| c.version), Some(5));
        assert_eq!(holder.retained().map(|c| c.version), Some(5));
    }

    #[test]
    fn stale_crc_is_retained_but_not_at_version() {
        let holder =
            SnapshotCrc::try_new(Some(crc_at(3)), 5, None, CrcProvenance::default()).unwrap();
        assert!(holder.at_version().is_none());
        assert_eq!(holder.retained().map(|c| c.version), Some(3));
    }

    #[test]
    fn none_holder_serves_nothing() {
        let holder = SnapshotCrc::try_new(None, 5, None, CrcProvenance::default()).unwrap();
        assert!(holder.at_version().is_none());
        assert!(holder.retained().is_none());
    }

    #[rstest::rstest]
    #[case::wrong_count(Some(5), CrcSource::Reconstructed { num_commits: 1 }, Some(3))]
    #[case::zero_count(Some(5), CrcSource::Reconstructed { num_commits: 0 }, Some(5))]
    #[case::missing_with_crc(Some(5), CrcSource::Missing, None)]
    #[case::unusable_with_crc(Some(3), CrcSource::Unusable, None)]
    #[case::at_version_without_crc(None, CrcSource::AtVersion, Some(5))]
    #[case::at_version_with_stale_crc(Some(3), CrcSource::AtVersion, Some(5))]
    #[case::at_version_with_older_base(Some(5), CrcSource::AtVersion, Some(3))]
    #[case::future_base(Some(5), CrcSource::AtVersion, Some(6))]
    #[case::stale_without_base(Some(3), CrcSource::Stale, None)]
    #[case::stale_with_newer_base(Some(3), CrcSource::Stale, Some(4))]
    #[case::stale_at_snapshot(Some(5), CrcSource::Stale, Some(3))]
    #[case::reconstructed_with_stale_crc(Some(4), CrcSource::Reconstructed { num_commits: 2 }, Some(3))]
    #[case::reconstructed_without_base(Some(5), CrcSource::Reconstructed { num_commits: 2 }, None)]
    #[case::missing_with_base(None, CrcSource::Missing, Some(3))]
    #[case::unusable_with_base(None, CrcSource::Unusable, Some(3))]
    #[case::unknown_with_base(Some(5), CrcSource::Unknown, Some(3))]
    fn rejects_inconsistent_provenance(
        #[case] crc_version: Option<Version>,
        #[case] source: CrcSource,
        #[case] base_version: Option<Version>,
    ) {
        // Given a retained checksum and an incompatible provenance claim.
        let crc = crc_version.map(crc_at);

        // When the checksum and provenance are used to construct a snapshot's CRC state.
        let error = SnapshotCrc::try_new(crc, 5, None, CrcProvenance::new(source, base_version))
            .unwrap_err();

        // Then an invalid internal derivation is reported as an internal error.
        assert!(matches!(
            error.without_backtrace(),
            KernelError::InternalError(_)
        ));
    }
}
