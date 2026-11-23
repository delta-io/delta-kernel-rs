//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use std::sync::Arc;

use crate::crc::{Crc, CrcDelta};
use crate::utils::require;
use crate::{KernelError, KernelResult, Version};

/// How the retained checksum relates to an observed checksum file and the snapshot version.
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
/// The source is independent of aggregate availability: a snapshot can have a usable checksum
/// that lacks complete file statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrcSource {
    /// A usable on-disk checksum file was resolved at the snapshot version.
    AtVersion,
    /// An older on-disk checksum was advanced in memory to the snapshot version using committed
    /// actions.
    Reconstructed {
        /// Version of the original on-disk checksum, preserved across in-memory updates.
        /// The cumulative number of incorporated commits is the retained CRC's version minus
        /// this version. Here the retained CRC is at the snapshot version.
        on_disk_version: Version,
    },
    /// A usable checksum is retained behind the snapshot version.
    Stale {
        /// Version of the on-disk checksum from which the retained CRC was read or reconstructed.
        /// This preserves its origin even when an already-reconstructed CRC becomes stale.
        /// The retained CRC's version, not the snapshot version, determines how many commits
        /// have been incorporated since this on-disk version.
        on_disk_version: Version,
    },
    /// Loading found no checksum candidate in its listed version range and retained no in-memory
    /// CRC. Fresh loads consider the selected checkpoint version (or zero without a checkpoint)
    /// through the snapshot version, inclusive. Incremental loads may list only after the previous
    /// checkpoint, or after the previous snapshot when new checkpoints are ignored. Files below
    /// the selected checkpoint are not candidates. This does not describe unlisted versions.
    Missing,
    /// An observed checksum candidate was unusable and no usable baseline was resolved.
    /// This includes read failures as well as invalid checksum contents.
    Unusable,
    /// No observation of an on-disk checksum is available. Examples include newly created tables,
    /// connector hints without provenance, and derived snapshots that discard a CRC at a
    /// checkpoint. It does not indicate that loading searched for a checksum and failed to find
    /// or read it.
    Unknown,
}

/// The newest [`Crc`] a snapshot resolved, at or before the snapshot's version. It is one CRC,
/// read from disk (or computed) at most once; keeping it lets later queries and CRC writes reuse
/// the already-parsed state instead of re-reading it. A `Some(crc)` is one of:
/// - at the snapshot version: authoritative, servable at zero I/O via [`Self::at_version`];
/// - stale (older): available through [`Self::as_ref`] for reuse, never served as authoritative.
///
/// [`Snapshot`]: super::Snapshot
#[derive(Debug, Clone)]
pub(crate) struct SnapshotCrc {
    crc: Option<Arc<Crc>>,
    snapshot_version: Version,
    source: CrcSource,
}

impl SnapshotCrc {
    /// Wrap the CRC a snapshot at `snapshot_version` resolved. A `Some(crc)` must sit in
    /// `[checkpoint_version, snapshot_version]`. Inconsistent telemetry is replaced with `Unknown`
    /// without changing the retained CRC or the construction result.
    pub(crate) fn try_new(
        crc: Option<Arc<Crc>>,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
        source: CrcSource,
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
        Ok(Self::from_parts(crc, snapshot_version, source))
    }

    /// Captures supplied telemetry without changing CRC acceptance or retention.
    pub(crate) fn from_parts(
        crc: Option<Arc<Crc>>,
        snapshot_version: Version,
        source: CrcSource,
    ) -> Self {
        let source = if Self::source_matches(crc.as_deref(), snapshot_version, source) {
            source
        } else {
            CrcSource::Unknown
        };
        Self {
            crc,
            snapshot_version,
            source,
        }
    }

    /// Records a successful checksum-file read at the file's version.
    pub(crate) fn from_file(crc: Arc<Crc>) -> Self {
        Self {
            snapshot_version: crc.version,
            crc: Some(crc),
            source: CrcSource::AtVersion,
        }
    }

    /// Classifies the retained CRC for a snapshot and checks the existing version bounds.
    pub(crate) fn for_snapshot(
        self,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
    ) -> KernelResult<Self> {
        let source = self.updated_source(self.crc.as_ref(), snapshot_version);
        Self::try_new(self.crc, snapshot_version, checkpoint_version, source)
    }

    /// Advances a CRC through replay while preserving its original disk version.
    pub(crate) fn with_replayed_crc(&self, crc: Arc<Crc>, version: Version) -> Self {
        let source = self.updated_source(Some(&crc), version);
        Self::from_parts(Some(crc), version, source)
    }

    /// Applies a committed delta only to an at-version CRC, retaining stale CRCs unchanged.
    pub(crate) fn apply_commit(&self, delta: CrcDelta, version: Version) -> Self {
        match self.at_version() {
            Some(crc) => self.with_replayed_crc(
                Arc::new(crc.as_ref().clone().apply(delta, version)),
                version,
            ),
            None => self.clone(),
        }
    }

    /// Keeps only the at-version CRC for checkpoint and hint derivations.
    pub(crate) fn retain_at_version(&self) -> Self {
        let crc = self.at_version().cloned();
        let source = self.updated_source(crc.as_ref(), self.snapshot_version);
        Self::from_parts(crc, self.snapshot_version, source)
    }

    pub(crate) fn source(&self) -> CrcSource {
        self.source
    }

    /// The CRC iff it sits exactly at the snapshot version, so its cached values are authoritative
    /// at zero I/O. Returns `None` for a stale CRC.
    pub(crate) fn at_version(&self) -> Option<&Arc<Crc>> {
        self.crc
            .as_ref()
            .filter(|crc| crc.version == self.snapshot_version)
    }

    /// Returns the CRC kept in memory, whether at the snapshot version or stale.
    /// It may have been read from disk or advanced by replaying commits.
    pub(crate) fn as_ref(&self) -> Option<&Arc<Crc>> {
        self.crc.as_ref()
    }

    /// Updates provenance for a derived snapshot, preserving the original disk baseline.
    /// Dropping a known checksum also drops its provenance.
    fn updated_source(&self, crc: Option<&Arc<Crc>>, snapshot_version: Version) -> CrcSource {
        let on_disk_version = match self.source {
            CrcSource::AtVersion => self.snapshot_version,
            CrcSource::Reconstructed { on_disk_version } | CrcSource::Stale { on_disk_version } => {
                on_disk_version
            }
            CrcSource::Missing | CrcSource::Unusable if crc.is_none() => return self.source,
            _ => return CrcSource::Unknown,
        };
        Self::source_for_crc(on_disk_version, crc, snapshot_version)
    }

    /// Checks the checksum's provenance independently of its log-segment bounds.
    fn source_matches(crc: Option<&Crc>, snapshot_version: Version, source: CrcSource) -> bool {
        match (source, crc) {
            (CrcSource::Unknown, _) => true,
            (CrcSource::Missing | CrcSource::Unusable, None) => true,
            (CrcSource::AtVersion, Some(crc)) => crc.version == snapshot_version,
            (CrcSource::Stale { on_disk_version }, Some(crc)) => {
                on_disk_version <= crc.version && crc.version < snapshot_version
            }
            (CrcSource::Reconstructed { on_disk_version }, Some(crc)) => {
                on_disk_version < crc.version && crc.version == snapshot_version
            }
            _ => false,
        }
    }

    fn source_for_crc(
        on_disk_version: Version,
        crc: Option<&Arc<Crc>>,
        snapshot_version: Version,
    ) -> CrcSource {
        match crc {
            Some(crc) if crc.version < snapshot_version => CrcSource::Stale { on_disk_version },
            Some(_) if on_disk_version == snapshot_version => CrcSource::AtVersion,
            Some(_) => CrcSource::Reconstructed { on_disk_version },
            None => CrcSource::Unknown,
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
    #[case::reconstructed_at_disk_version(Some(5), CrcSource::Reconstructed { on_disk_version: 5 })]
    #[case::reconstructed_with_future_disk_version(Some(5), CrcSource::Reconstructed { on_disk_version: 6 })]
    #[case::missing_with_crc(Some(5), CrcSource::Missing)]
    #[case::unusable_with_crc(Some(3), CrcSource::Unusable)]
    #[case::at_version_without_crc(None, CrcSource::AtVersion)]
    #[case::at_version_with_stale_crc(Some(3), CrcSource::AtVersion)]
    #[case::stale_with_newer_disk_version(Some(3), CrcSource::Stale { on_disk_version: 4 })]
    #[case::stale_at_snapshot(Some(5), CrcSource::Stale { on_disk_version: 3 })]
    #[case::stale_without_crc(None, CrcSource::Stale { on_disk_version: 3 })]
    #[case::reconstructed_with_stale_crc(Some(4), CrcSource::Reconstructed { on_disk_version: 3 })]
    #[case::reconstructed_without_crc(None, CrcSource::Reconstructed { on_disk_version: 3 })]
    fn inconsistent_provenance_is_unknown_without_changing_crc(
        #[case] crc_version: Option<Version>,
        #[case] source: CrcSource,
    ) {
        // Given a retained checksum and an incompatible provenance claim.
        let crc = crc_version.map(crc_at);

        // When the checksum and provenance are used to construct a snapshot's CRC state.
        let state = SnapshotCrc::try_new(crc.clone(), 5, None, source).unwrap();

        // Then only telemetry is discarded; CRC retention and construction success are unchanged.
        assert_eq!(state.source(), CrcSource::Unknown);
        assert_eq!(state.as_ref(), crc.as_ref());
    }
}
