//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use std::sync::Arc;

use crate::crc::{Crc, CrcDelta};
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
/// kept in memory so later queries and CRC writes can reuse it. A present CRC is either:
/// - at the snapshot version: authoritative, servable at zero I/O via [`Self::at_version`];
/// - stale (older): available through [`Self::as_ref`] for reuse, never served as authoritative.
///
/// The CRC carries its disk origin; this type derives its source relative to the snapshot version
/// and records lookup outcomes when no CRC is present.
///
/// [`Snapshot`]: super::Snapshot
#[derive(Debug, Clone)]
pub(crate) enum SnapshotCrc {
    Present {
        crc: Arc<Crc>,
        snapshot_version: Version,
    },
    Missing,
    Unusable,
    Unknown,
}

impl SnapshotCrc {
    /// Wraps an in-memory CRC without changing its disk origin.
    pub(crate) fn new(crc: Arc<Crc>) -> Self {
        Self::Present {
            snapshot_version: crc.version,
            crc,
        }
    }

    /// Validates a connector's source claim without changing CRC acceptance or contents.
    pub(crate) fn from_parts(
        crc: Option<Arc<Crc>>,
        snapshot_version: Version,
        source: CrcSource,
    ) -> Self {
        match crc {
            Some(mut crc) => {
                let on_disk_version = match source {
                    CrcSource::AtVersion { crc_version }
                        if crc_version == crc.version && crc.version == snapshot_version =>
                    {
                        Some(crc.version)
                    }
                    CrcSource::Reconstructed {
                        on_disk_version,
                        crc_version,
                    } if crc_version == crc.version
                        && on_disk_version < crc.version
                        && crc.version == snapshot_version =>
                    {
                        Some(on_disk_version)
                    }
                    CrcSource::Stale {
                        on_disk_version,
                        crc_version,
                    } if crc_version == crc.version
                        && on_disk_version <= crc.version
                        && crc.version < snapshot_version =>
                    {
                        Some(on_disk_version)
                    }
                    _ => None,
                };
                if crc.on_disk_version != on_disk_version {
                    Arc::make_mut(&mut crc).on_disk_version = on_disk_version;
                }
                Self::Present {
                    crc,
                    snapshot_version,
                }
            }
            None => match source {
                CrcSource::Missing => Self::Missing,
                CrcSource::Unusable => Self::Unusable,
                _ => Self::Unknown,
            },
        }
    }

    /// Records a checksum file successfully written at the CRC's version.
    pub(crate) fn from_written_crc(mut crc: Arc<Crc>) -> Self {
        if crc.on_disk_version != Some(crc.version) {
            Arc::make_mut(&mut crc).on_disk_version = Some(crc.version);
        }
        Self::new(crc)
    }

    /// Checks the existing CRC version bounds for a snapshot, preserving its disk origin.
    pub(crate) fn for_snapshot(
        mut self,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
    ) -> KernelResult<Self> {
        if let Self::Present {
            crc,
            snapshot_version: version,
        } = &mut self
        {
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
            *version = snapshot_version;
        }
        Ok(self)
    }

    /// Applies a committed delta only to an at-version CRC, keeping stale CRCs unchanged.
    pub(crate) fn apply_commit(&self, delta: CrcDelta, version: Version) -> Self {
        match self.at_version() {
            Some(crc) => Self::new(Arc::new(crc.as_ref().clone().apply(delta, version))),
            None => self.clone(),
        }
    }

    /// Keeps only the at-version CRC for checkpoint and hint derivations.
    pub(crate) fn retain_at_version(&self) -> Self {
        match self {
            Self::Present { .. } if self.at_version().is_none() => Self::Unknown,
            _ => self.clone(),
        }
    }

    pub(crate) fn source(&self) -> CrcSource {
        match self {
            Self::Present {
                crc,
                snapshot_version,
            } => match crc.on_disk_version {
                Some(on_disk_version) if crc.version < *snapshot_version => CrcSource::Stale {
                    on_disk_version,
                    crc_version: crc.version,
                },
                Some(on_disk_version) if on_disk_version == *snapshot_version => {
                    CrcSource::AtVersion {
                        crc_version: crc.version,
                    }
                }
                Some(on_disk_version) => CrcSource::Reconstructed {
                    on_disk_version,
                    crc_version: crc.version,
                },
                None => CrcSource::Unknown,
            },
            Self::Missing => CrcSource::Missing,
            Self::Unusable => CrcSource::Unusable,
            Self::Unknown => CrcSource::Unknown,
        }
    }

    /// Returns the in-memory CRC only when its version matches the snapshot version.
    pub(crate) fn at_version(&self) -> Option<&Arc<Crc>> {
        match self {
            Self::Present {
                crc,
                snapshot_version,
            } if crc.version == *snapshot_version => Some(crc),
            _ => None,
        }
    }

    /// Returns the in-memory CRC, which may be older than the snapshot version.
    pub(crate) fn as_ref(&self) -> Option<&Arc<Crc>> {
        match self {
            Self::Present { crc, .. } => Some(crc),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_new(
        crc: Option<Arc<Crc>>,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
        source: CrcSource,
    ) -> KernelResult<Self> {
        Self::from_parts(crc, snapshot_version, source)
            .for_snapshot(snapshot_version, checkpoint_version)
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
            SnapshotCrc::try_new(Some(crc_at(6)), 5, None, CrcSource::Unknown),
            "is ahead of snapshot version",
        );
    }

    #[test]
    fn new_rejects_crc_below_checkpoint() {
        assert_result_error_with_message(
            SnapshotCrc::try_new(Some(crc_at(2)), 5, Some(3), CrcSource::Unknown),
            "is below checkpoint version",
        );
    }

    #[test]
    fn new_accepts_crc_at_checkpoint() {
        let holder = SnapshotCrc::try_new(Some(crc_at(3)), 5, Some(3), CrcSource::Unknown).unwrap();
        assert_eq!(holder.as_ref().map(|c| c.version), Some(3));
    }

    #[test]
    fn at_version_serves_crc_matching_snapshot_version() {
        let holder = SnapshotCrc::try_new(Some(crc_at(5)), 5, None, CrcSource::Unknown).unwrap();
        assert_eq!(holder.at_version().map(|c| c.version), Some(5));
        assert_eq!(holder.as_ref().map(|c| c.version), Some(5));
    }

    #[test]
    fn stale_crc_is_base_but_not_at_version() {
        let holder = SnapshotCrc::try_new(Some(crc_at(3)), 5, None, CrcSource::Unknown).unwrap();
        assert!(holder.at_version().is_none());
        assert_eq!(holder.as_ref().map(|c| c.version), Some(3));
    }

    #[test]
    fn none_holder_serves_nothing() {
        let holder = SnapshotCrc::try_new(None, 5, None, CrcSource::Unknown).unwrap();
        assert!(holder.at_version().is_none());
        assert!(holder.as_ref().is_none());
    }

    #[rstest::rstest]
    #[case::reconstructed_at_disk_version(Some(5), CrcSource::Reconstructed { on_disk_version: 5, crc_version: 5 })]
    #[case::reconstructed_with_future_disk_version(Some(5), CrcSource::Reconstructed { on_disk_version: 6, crc_version: 5 })]
    #[case::missing_with_crc(Some(5), CrcSource::Missing)]
    #[case::unusable_with_crc(Some(3), CrcSource::Unusable)]
    #[case::at_version_without_crc(None, CrcSource::AtVersion { crc_version: 5 })]
    #[case::at_version_with_stale_crc(Some(3), CrcSource::AtVersion { crc_version: 3 })]
    #[case::stale_with_newer_disk_version(Some(3), CrcSource::Stale { on_disk_version: 4, crc_version: 3 })]
    #[case::stale_at_snapshot(Some(5), CrcSource::Stale { on_disk_version: 3, crc_version: 5 })]
    #[case::stale_without_crc(None, CrcSource::Stale { on_disk_version: 3, crc_version: 5 })]
    #[case::reconstructed_with_stale_crc(Some(4), CrcSource::Reconstructed { on_disk_version: 3, crc_version: 4 })]
    #[case::reconstructed_without_crc(None, CrcSource::Reconstructed { on_disk_version: 3, crc_version: 5 })]
    #[case::at_version_wrong_crc_version(Some(5), CrcSource::AtVersion { crc_version: 4 })]
    #[case::reconstructed_wrong_crc_version(Some(5), CrcSource::Reconstructed { on_disk_version: 1, crc_version: 4 })]
    #[case::stale_wrong_crc_version(Some(3), CrcSource::Stale { on_disk_version: 1, crc_version: 2 })]
    fn inconsistent_provenance_is_unknown_without_changing_crc(
        #[case] crc_version: Option<Version>,
        #[case] source: CrcSource,
    ) {
        // Given an in-memory checksum and an incompatible provenance claim.
        let crc = crc_version.map(crc_at);

        // When the checksum and provenance are used to construct a snapshot's CRC state.
        let state = SnapshotCrc::try_new(crc.clone(), 5, None, source).unwrap();

        // Then only telemetry is discarded; CRC retention and construction success are unchanged.
        assert_eq!(state.source(), CrcSource::Unknown);
        assert_eq!(state.as_ref(), crc.as_ref());
    }

    #[test]
    fn replayed_crc_carries_origin_across_snapshot_versions_without_copying() {
        // Given a CRC read at version 10 and advanced in memory to version 13.
        let mut crc = crc_at(10);
        Arc::make_mut(&mut crc).on_disk_version = Some(10);
        let original = SnapshotCrc::new(crc);
        let advanced = original.apply_commit(CrcDelta::default(), 13);
        assert_eq!(
            advanced.source(),
            CrcSource::Reconstructed {
                on_disk_version: 10,
                crc_version: 13
            }
        );

        // When a newer snapshot uses the same CRC without replaying it again.
        let stale = advanced.clone().for_snapshot(14, None).unwrap();

        // Then classification changes without copying or changing the CRC or its disk origin.
        assert_eq!(
            stale.source(),
            CrcSource::Stale {
                on_disk_version: 10,
                crc_version: 13
            }
        );
        assert!(Arc::ptr_eq(
            stale.as_ref().unwrap(),
            advanced.as_ref().unwrap()
        ));
        assert_eq!(stale.as_ref().unwrap().on_disk_version, Some(10));
        assert_eq!(stale.as_ref().unwrap().version, 13);
        assert_eq!(original.source(), CrcSource::AtVersion { crc_version: 10 });
        assert_eq!(
            advanced.source(),
            CrcSource::Reconstructed {
                on_disk_version: 10,
                crc_version: 13
            }
        );
    }

    #[rstest::rstest]
    #[case::unchanged(CrcSource::Reconstructed { on_disk_version: 1, crc_version: 3 }, Some(1))]
    #[case::omitted(CrcSource::Unknown, None)]
    #[case::inconsistent(CrcSource::Missing, None)]
    fn hint_claim_does_not_change_shared_crc_origin(
        #[case] claim: CrcSource,
        #[case] expected_origin: Option<Version>,
    ) {
        // Given a shared CRC with known disk origin.
        let mut crc = crc_at(3);
        Arc::make_mut(&mut crc).on_disk_version = Some(1);

        // When a hint supplies matching, omitted, or inconsistent telemetry.
        let imported = SnapshotCrc::from_parts(Some(crc.clone()), 3, claim);
        let imported_crc = imported.as_ref().unwrap();

        // Then only the imported origin changes; matching telemetry preserves sharing.
        assert_eq!(imported_crc.on_disk_version, expected_origin);
        assert_eq!(crc.on_disk_version, Some(1));
        assert_eq!(imported_crc, &crc);
        assert_eq!(Arc::ptr_eq(imported_crc, &crc), expected_origin == Some(1));
    }

    #[test]
    fn writing_crc_updates_origin_without_changing_original_snapshot() {
        // Given an in-memory CRC reconstructed from a version 1 checksum.
        let mut crc = crc_at(3);
        Arc::make_mut(&mut crc).on_disk_version = Some(1);
        let original = SnapshotCrc::new(crc.clone());

        // When a checksum file is successfully written at version 3.
        let written = SnapshotCrc::from_written_crc(crc);

        // Then the new snapshot reports that file, while the original keeps its observation.
        assert_eq!(written.source(), CrcSource::AtVersion { crc_version: 3 });
        assert_eq!(written.as_ref().unwrap().on_disk_version, Some(3));
        assert_eq!(
            original.source(),
            CrcSource::Reconstructed {
                on_disk_version: 1,
                crc_version: 3
            }
        );
        assert_eq!(original.as_ref(), written.as_ref());
    }
}
