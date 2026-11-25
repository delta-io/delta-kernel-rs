//! Provenance of individual in-memory CRCs, independent of snapshot resolution.

use std::sync::Arc;

use super::{Crc, CrcDelta};
use crate::Version;

/// Where Kernel obtained the in-memory checksum it used as the starting point for replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrcOrigin {
    /// Kernel read a checksum file.
    File,
    /// A connector supplied the checksum contents in a snapshot hint.
    SnapshotHint,
    /// Kernel computed the initial checksum from the CREATE TABLE transaction.
    CreateTable,
    /// Kernel reconstructed the checksum from a checkpoint.
    Checkpoint,
    /// Kernel reconstructed the checksum by replaying commits starting at version zero.
    CommitHistory,
}

/// Origin and versions of a checksum held in memory by a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrcProvenance {
    /// How Kernel obtained the initial in-memory checksum.
    pub origin: CrcOrigin,
    /// Version described by the retained checksum, including subsequent incremental replay.
    pub crc_version: Version,
    /// Checksum-file version used by [`Snapshot::crc_source`](crate::Snapshot::crc_source).
    /// A successful checksum write sets this to the written version. A hint may supply an
    /// earlier disk version even when Kernel received the in-memory checksum at a later version.
    pub on_disk_version: Option<Version>,
}

/// A shared checksum and the observations made while resolving it.
#[derive(Debug, Clone)]
pub(crate) struct CrcWithProvenance {
    crc: Arc<Crc>,
    origin: CrcOrigin,
    on_disk_version: Option<Version>,
}

impl CrcWithProvenance {
    fn new(crc: Arc<Crc>, origin: CrcOrigin, on_disk_version: Option<Version>) -> Self {
        Self {
            crc,
            origin,
            on_disk_version,
        }
    }

    pub(crate) fn from_file(crc: Arc<Crc>) -> Self {
        let version = crc.version;
        Self::new(crc, CrcOrigin::File, Some(version))
    }

    pub(crate) fn from_snapshot_hint(crc: Arc<Crc>, on_disk_version: Option<Version>) -> Self {
        Self::new(crc, CrcOrigin::SnapshotHint, on_disk_version)
    }

    pub(crate) fn from_create_table(crc: Arc<Crc>) -> Self {
        Self::new(crc, CrcOrigin::CreateTable, None)
    }

    pub(crate) fn from_checkpoint(crc: Crc) -> Self {
        Self::new(Arc::new(crc), CrcOrigin::Checkpoint, None)
    }

    pub(crate) fn from_commit_history(crc: Crc) -> Self {
        Self::new(Arc::new(crc), CrcOrigin::CommitHistory, None)
    }

    pub(crate) fn version(&self) -> Version {
        self.crc.version
    }

    pub(crate) fn crc(&self) -> &Arc<Crc> {
        &self.crc
    }

    pub(crate) fn on_disk_version(&self) -> Option<Version> {
        self.on_disk_version
    }

    pub(crate) fn provenance(&self) -> CrcProvenance {
        CrcProvenance {
            origin: self.origin,
            crc_version: self.crc.version,
            on_disk_version: self.on_disk_version,
        }
    }

    /// Records the newly written checksum file while preserving how Kernel obtained its contents.
    pub(crate) fn written_to_disk(mut self) -> Self {
        self.on_disk_version = Some(self.crc.version);
        self
    }

    /// Applies committed actions while preserving the CRC origin and known disk version.
    pub(crate) fn apply(&self, delta: CrcDelta, version: Version) -> Self {
        Self {
            crc: Arc::new(self.crc.as_ref().clone().apply(delta, version)),
            origin: self.origin,
            on_disk_version: self.on_disk_version,
        }
    }
}
