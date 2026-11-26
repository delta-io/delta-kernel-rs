//! Provenance of individual in-memory CRCs, independent of snapshot resolution.

use std::sync::Arc;

use super::{Crc, CrcDelta};
use crate::Version;

/// How Kernel obtained an in-memory CRC and which checksum-file version is known for it.
///
/// Applying committed actions advances the CRC version and preserves the checksum-file version.
/// Successfully writing a checksum file records the written version on the returned snapshot.
/// Subtract the known checksum-file version from the CRC version to count the commits incorporated
/// since that file, possibly across multiple snapshot builds. A CRC supplied in a snapshot hint or
/// computed in memory can have no known checksum-file version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrcProvenance {
    /// Kernel obtained the initial CRC by parsing a checksum file read through the engine.
    File {
        /// Version described by the in-memory CRC.
        crc_version: Version,
        /// Known checksum-file version.
        on_disk_version: Version,
    },
    /// A connector supplied the initial CRC contents in a snapshot hint.
    SnapshotHint {
        /// Version described by the in-memory CRC.
        crc_version: Version,
        /// Checksum-file version, if known.
        on_disk_version: Option<Version>,
    },
    /// Kernel computed the initial CRC from the CREATE TABLE transaction.
    CreateTable {
        /// Version described by the in-memory CRC.
        crc_version: Version,
        /// Checksum-file version, if known.
        on_disk_version: Option<Version>,
    },
    /// Kernel reconstructed the initial CRC from checkpoint contents.
    Checkpoint {
        /// Version described by the in-memory CRC.
        crc_version: Version,
        /// Checksum-file version, if known.
        on_disk_version: Option<Version>,
    },
    /// Kernel reconstructed the initial CRC from commits starting at version zero.
    CommitHistory {
        /// Version described by the in-memory CRC.
        crc_version: Version,
        /// Checksum-file version, if known.
        on_disk_version: Option<Version>,
    },
}

impl CrcProvenance {
    /// Returns the version described by the in-memory CRC.
    pub fn crc_version(&self) -> Version {
        match self {
            Self::File { crc_version, .. }
            | Self::SnapshotHint { crc_version, .. }
            | Self::CreateTable { crc_version, .. }
            | Self::Checkpoint { crc_version, .. }
            | Self::CommitHistory { crc_version, .. } => *crc_version,
        }
    }

    /// Returns the checksum-file version known from a file read, write, or snapshot hint claim.
    pub fn on_disk_version(&self) -> Option<Version> {
        match self {
            Self::File {
                on_disk_version, ..
            } => Some(*on_disk_version),
            Self::SnapshotHint {
                on_disk_version, ..
            }
            | Self::CreateTable {
                on_disk_version, ..
            }
            | Self::Checkpoint {
                on_disk_version, ..
            }
            | Self::CommitHistory {
                on_disk_version, ..
            } => *on_disk_version,
        }
    }
}

/// A shared checksum and the observations made while resolving it.
#[derive(Debug, Clone)]
pub(crate) struct CrcWithProvenance {
    crc: Arc<Crc>,
    provenance: CrcProvenance,
}

impl CrcWithProvenance {
    pub(crate) fn from_file(crc: Arc<Crc>) -> Self {
        Self {
            provenance: CrcProvenance::File {
                crc_version: crc.version,
                on_disk_version: crc.version,
            },
            crc,
        }
    }

    pub(crate) fn from_snapshot_hint(crc: Arc<Crc>, on_disk_version: Option<Version>) -> Self {
        Self {
            provenance: CrcProvenance::SnapshotHint {
                crc_version: crc.version,
                on_disk_version,
            },
            crc,
        }
    }

    pub(crate) fn from_create_table(crc: Arc<Crc>) -> Self {
        Self {
            provenance: CrcProvenance::CreateTable {
                crc_version: crc.version,
                on_disk_version: None,
            },
            crc,
        }
    }

    pub(crate) fn from_checkpoint(crc: Crc) -> Self {
        Self {
            provenance: CrcProvenance::Checkpoint {
                crc_version: crc.version,
                on_disk_version: None,
            },
            crc: Arc::new(crc),
        }
    }

    pub(crate) fn from_commit_history(crc: Crc) -> Self {
        Self {
            provenance: CrcProvenance::CommitHistory {
                crc_version: crc.version,
                on_disk_version: None,
            },
            crc: Arc::new(crc),
        }
    }

    pub(crate) fn version(&self) -> Version {
        self.crc.version
    }

    pub(crate) fn crc(&self) -> &Arc<Crc> {
        &self.crc
    }

    pub(crate) fn on_disk_version(&self) -> Option<Version> {
        self.provenance.on_disk_version()
    }

    pub(crate) fn provenance(&self) -> CrcProvenance {
        self.provenance
    }

    /// Records the newly written checksum file while preserving how Kernel obtained its contents.
    pub(crate) fn written_to_disk(mut self) -> Self {
        match &mut self.provenance {
            CrcProvenance::File {
                on_disk_version, ..
            } => *on_disk_version = self.crc.version,
            CrcProvenance::SnapshotHint {
                on_disk_version, ..
            }
            | CrcProvenance::CreateTable {
                on_disk_version, ..
            }
            | CrcProvenance::Checkpoint {
                on_disk_version, ..
            }
            | CrcProvenance::CommitHistory {
                on_disk_version, ..
            } => *on_disk_version = Some(self.crc.version),
        }
        self
    }

    /// Applies committed actions while preserving the CRC origin and known disk version.
    pub(crate) fn apply(&self, delta: CrcDelta, version: Version) -> Self {
        let crc = self.crc.as_ref().clone().apply(delta, version);
        let mut provenance = self.provenance;
        match &mut provenance {
            CrcProvenance::File { crc_version, .. }
            | CrcProvenance::SnapshotHint { crc_version, .. }
            | CrcProvenance::CreateTable { crc_version, .. }
            | CrcProvenance::Checkpoint { crc_version, .. }
            | CrcProvenance::CommitHistory { crc_version, .. } => *crc_version = crc.version,
        }
        Self {
            crc: Arc::new(crc),
            provenance,
        }
    }
}
