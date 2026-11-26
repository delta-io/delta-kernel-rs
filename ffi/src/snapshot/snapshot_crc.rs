//! Snapshot checksum provenance, available without engine I/O.

use delta_kernel::snapshot::{CrcProvenance, SnapshotCrcProvenance};

use crate::{Handle, OptionalValue, SharedSnapshot};

/// The status of the CRC retained by a snapshot. See [`SnapshotCrcProvenance`].
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, PartialEq)]
#[repr(C)]
pub enum FfiSnapshotCrcProvenance {
    /// The retained CRC describes the snapshot version.
    Current(FfiCrcProvenance),
    /// The retained CRC predates the snapshot version.
    Stale(FfiCrcProvenance),
    /// No CRC was retained and no unusable candidate was reported.
    Absent,
    /// A candidate was unusable and no CRC was retained.
    Unusable,
}

/// Origin and versions of a retained in-memory CRC. See [`CrcProvenance`].
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, PartialEq)]
#[repr(C)]
pub enum FfiCrcProvenance {
    /// Kernel parsed a checksum file read through the engine.
    File {
        /// Version described by the in-memory CRC.
        crc_version: u64,
        /// Known checksum-file version.
        on_disk_version: u64,
    },
    /// A connector supplied the CRC contents in a snapshot hint.
    SnapshotHint {
        /// Version described by the in-memory CRC.
        crc_version: u64,
        /// Checksum-file version, if known.
        on_disk_version: OptionalValue<u64>,
    },
    /// Kernel computed the initial CRC from the CREATE TABLE transaction.
    CreateTable {
        /// Version described by the in-memory CRC.
        crc_version: u64,
        /// Checksum-file version, if known.
        on_disk_version: OptionalValue<u64>,
    },
    /// Kernel reconstructed the initial CRC from checkpoint contents.
    Checkpoint {
        /// Version described by the in-memory CRC.
        crc_version: u64,
        /// Checksum-file version, if known.
        on_disk_version: OptionalValue<u64>,
    },
    /// Kernel reconstructed the initial CRC from commits starting at version zero.
    CommitHistory {
        /// Version described by the in-memory CRC.
        crc_version: u64,
        /// Checksum-file version, if known.
        on_disk_version: OptionalValue<u64>,
    },
}

/// Writes the snapshot-relative CRC provenance without engine I/O.
///
/// # Safety
/// `snapshot` must remain valid for the call. `out` must be aligned and writable for one
/// `FfiSnapshotCrcProvenance`. Its previous contents may be uninitialized.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_crc_provenance(
    snapshot: Handle<SharedSnapshot>,
    out: *mut FfiSnapshotCrcProvenance,
) {
    unsafe { out.write(snapshot.as_ref().crc_provenance().into()) };
}

impl From<SnapshotCrcProvenance> for FfiSnapshotCrcProvenance {
    fn from(value: SnapshotCrcProvenance) -> Self {
        match value {
            SnapshotCrcProvenance::Current(p) => Self::Current(p.into()),
            SnapshotCrcProvenance::Stale(p) => Self::Stale(p.into()),
            SnapshotCrcProvenance::Absent => Self::Absent,
            SnapshotCrcProvenance::Unusable => Self::Unusable,
        }
    }
}

impl From<&FfiSnapshotCrcProvenance> for SnapshotCrcProvenance {
    fn from(value: &FfiSnapshotCrcProvenance) -> Self {
        match value {
            FfiSnapshotCrcProvenance::Current(p) => Self::Current(p.into()),
            FfiSnapshotCrcProvenance::Stale(p) => Self::Stale(p.into()),
            FfiSnapshotCrcProvenance::Absent => Self::Absent,
            FfiSnapshotCrcProvenance::Unusable => Self::Unusable,
        }
    }
}

impl From<CrcProvenance> for FfiCrcProvenance {
    fn from(value: CrcProvenance) -> Self {
        match value {
            CrcProvenance::File {
                crc_version,
                on_disk_version,
            } => Self::File {
                crc_version,
                on_disk_version,
            },
            CrcProvenance::SnapshotHint {
                crc_version,
                on_disk_version,
            } => Self::SnapshotHint {
                crc_version,
                on_disk_version: on_disk_version.into(),
            },
            CrcProvenance::CreateTable {
                crc_version,
                on_disk_version,
            } => Self::CreateTable {
                crc_version,
                on_disk_version: on_disk_version.into(),
            },
            CrcProvenance::Checkpoint {
                crc_version,
                on_disk_version,
            } => Self::Checkpoint {
                crc_version,
                on_disk_version: on_disk_version.into(),
            },
            CrcProvenance::CommitHistory {
                crc_version,
                on_disk_version,
            } => Self::CommitHistory {
                crc_version,
                on_disk_version: on_disk_version.into(),
            },
        }
    }
}

impl From<&FfiCrcProvenance> for CrcProvenance {
    fn from(value: &FfiCrcProvenance) -> Self {
        match value {
            FfiCrcProvenance::File {
                crc_version,
                on_disk_version,
            } => Self::File {
                crc_version: *crc_version,
                on_disk_version: *on_disk_version,
            },
            FfiCrcProvenance::SnapshotHint {
                crc_version,
                on_disk_version,
            } => Self::SnapshotHint {
                crc_version: *crc_version,
                on_disk_version: Option::<&u64>::from(on_disk_version).copied(),
            },
            FfiCrcProvenance::CreateTable {
                crc_version,
                on_disk_version,
            } => Self::CreateTable {
                crc_version: *crc_version,
                on_disk_version: Option::<&u64>::from(on_disk_version).copied(),
            },
            FfiCrcProvenance::Checkpoint {
                crc_version,
                on_disk_version,
            } => Self::Checkpoint {
                crc_version: *crc_version,
                on_disk_version: Option::<&u64>::from(on_disk_version).copied(),
            },
            FfiCrcProvenance::CommitHistory {
                crc_version,
                on_disk_version,
            } => Self::CommitHistory {
                crc_version: *crc_version,
                on_disk_version: Option::<&u64>::from(on_disk_version).copied(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case(
        CrcProvenance::File { crc_version: 3, on_disk_version: 1 },
        FfiCrcProvenance::File { crc_version: 3, on_disk_version: 1 },
    )]
    #[case(
        CrcProvenance::SnapshotHint { crc_version: 3, on_disk_version: None },
        FfiCrcProvenance::SnapshotHint { crc_version: 3, on_disk_version: OptionalValue::None },
    )]
    #[case(
        CrcProvenance::CreateTable { crc_version: 3, on_disk_version: Some(0) },
        FfiCrcProvenance::CreateTable { crc_version: 3, on_disk_version: OptionalValue::Some(0) },
    )]
    #[case(
        CrcProvenance::Checkpoint { crc_version: 3, on_disk_version: Some(2) },
        FfiCrcProvenance::Checkpoint { crc_version: 3, on_disk_version: OptionalValue::Some(2) },
    )]
    #[case(
        CrcProvenance::CommitHistory { crc_version: 3, on_disk_version: None },
        FfiCrcProvenance::CommitHistory { crc_version: 3, on_disk_version: OptionalValue::None },
    )]
    fn provenance_conversion_preserves_variant_and_versions(
        #[case] provenance: CrcProvenance,
        #[case] ffi: FfiCrcProvenance,
        #[values(false, true)] stale: bool,
    ) {
        // Given equivalent Rust and C checksum statuses with distinct disk and in-memory versions.
        let (status, expected) = if stale {
            (
                SnapshotCrcProvenance::Stale(provenance),
                FfiSnapshotCrcProvenance::Stale(ffi),
            )
        } else {
            (
                SnapshotCrcProvenance::Current(provenance),
                FfiSnapshotCrcProvenance::Current(ffi),
            )
        };
        // When converting each representation independently.
        // Then both conversions preserve the variant and each version.
        assert_eq!(FfiSnapshotCrcProvenance::from(status), expected);
        assert_eq!(SnapshotCrcProvenance::from(&expected), status);
    }
}
