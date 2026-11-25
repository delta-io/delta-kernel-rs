//! Snapshot checksum provenance, available without engine I/O.

use delta_kernel::snapshot::{CrcOrigin, CrcProvenance, CrcSource};

use crate::{Handle, OptionalValue, SharedSnapshot};

/// How an in-memory checksum relates to an observed checksum file. See [`CrcSource`].
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub enum FfiCrcSource {
    /// A usable on-disk checksum file was resolved at the snapshot version.
    AtVersion {
        /// Version of both the checksum file and the in-memory CRC, equal to the snapshot version.
        crc_version: u64,
    },
    /// An on-disk checksum was advanced in memory to the snapshot version.
    Reconstructed {
        /// Version of the original on-disk checksum.
        /// Subtract this from `crc_version` to obtain the cumulative number of incorporated
        /// commits.
        on_disk_version: u64,
        /// Version of the in-memory CRC, equal to the snapshot version.
        crc_version: u64,
    },
    /// A usable in-memory checksum is older than the snapshot version.
    Stale {
        /// Version of the on-disk checksum from which the in-memory CRC was read or reconstructed.
        /// Subtract this from `crc_version` to obtain the cumulative number of incorporated
        /// commits.
        on_disk_version: u64,
        /// Version of the in-memory CRC, strictly less than the snapshot version.
        /// Subtract this from the snapshot version to obtain the remaining version gap.
        crc_version: u64,
    },
    /// No checksum file was found in the snapshot's selected version range.
    Missing,
    /// The observed checksum candidate could not be read or validated.
    Unusable,
    /// No observation of an on-disk checksum is available.
    Unknown,
}

impl From<CrcSource> for FfiCrcSource {
    fn from(value: CrcSource) -> Self {
        match value {
            CrcSource::AtVersion { crc_version } => Self::AtVersion { crc_version },
            CrcSource::Reconstructed {
                on_disk_version,
                crc_version,
            } => Self::Reconstructed {
                on_disk_version,
                crc_version,
            },
            CrcSource::Stale {
                on_disk_version,
                crc_version,
            } => Self::Stale {
                on_disk_version,
                crc_version,
            },
            CrcSource::Missing => Self::Missing,
            CrcSource::Unusable => Self::Unusable,
            _ => Self::Unknown,
        }
    }
}

impl From<FfiCrcSource> for CrcSource {
    fn from(value: FfiCrcSource) -> Self {
        match value {
            FfiCrcSource::AtVersion { crc_version } => Self::AtVersion { crc_version },
            FfiCrcSource::Reconstructed {
                on_disk_version,
                crc_version,
            } => Self::Reconstructed {
                on_disk_version,
                crc_version,
            },
            FfiCrcSource::Stale {
                on_disk_version,
                crc_version,
            } => Self::Stale {
                on_disk_version,
                crc_version,
            },
            FfiCrcSource::Missing => Self::Missing,
            FfiCrcSource::Unusable => Self::Unusable,
            FfiCrcSource::Unknown => Self::Unknown,
        }
    }
}

/// Returns the checksum provenance without engine I/O.
///
/// # Safety
/// `snapshot` must be valid and remain owned by the caller.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_crc_source(snapshot: Handle<SharedSnapshot>) -> FfiCrcSource {
    unsafe { snapshot.as_ref() }.crc_source().into()
}

/// How Kernel acquired the CRC contents before incremental replay.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum FfiCrcOrigin {
    /// Read from a checksum file.
    File,
    /// Supplied in a connector hint.
    SnapshotHint,
    /// Computed by CREATE TABLE.
    CreateTable,
    /// Reconstructed from a checkpoint.
    Checkpoint,
    /// Reconstructed from commit history starting at version zero.
    CommitHistory,
}

/// Owned version information for a checksum retained by a snapshot.
#[repr(C)]
pub struct FfiCrcProvenance {
    /// Where Kernel acquired the initial in-memory CRC.
    pub origin: FfiCrcOrigin,
    /// Version described by the retained CRC, including subsequent incremental replay.
    pub crc_version: u64,
    /// Checksum-file version known from reads, successful writes, or the hint's disk claim.
    pub on_disk_version: OptionalValue<u64>,
}

impl From<CrcProvenance> for FfiCrcProvenance {
    fn from(value: CrcProvenance) -> Self {
        let origin = match value.origin {
            CrcOrigin::File => FfiCrcOrigin::File,
            CrcOrigin::SnapshotHint => FfiCrcOrigin::SnapshotHint,
            CrcOrigin::CreateTable => FfiCrcOrigin::CreateTable,
            CrcOrigin::Checkpoint => FfiCrcOrigin::Checkpoint,
            CrcOrigin::CommitHistory => FfiCrcOrigin::CommitHistory,
        };
        Self {
            origin,
            crc_version: value.crc_version,
            on_disk_version: value.on_disk_version.into(),
        }
    }
}

/// Copies the retained in-memory CRC's provenance to `out`, including for a stale CRC.
/// Returns true when a value was written. With no retained CRC it returns false and leaves
/// `out` untouched. See [`snapshot_crc_source`] for the snapshot's disk observation.
///
/// # Safety
/// The snapshot must remain valid for the call. `out` must be aligned and writable for one
/// `FfiCrcProvenance`. Its previous contents may be uninitialized.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_crc_provenance(
    snapshot: Handle<SharedSnapshot>,
    out: *mut FfiCrcProvenance,
) -> bool {
    let Some(provenance) = (unsafe { snapshot.as_ref() }).crc_provenance() else {
        return false;
    };
    unsafe { out.write(provenance.into()) };
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::at_version(CrcSource::AtVersion { crc_version: 3 }, FfiCrcSource::AtVersion { crc_version: 3 })]
    #[case::reconstructed(CrcSource::Reconstructed { on_disk_version: 1, crc_version: 3 }, FfiCrcSource::Reconstructed { on_disk_version: 1, crc_version: 3 })]
    #[case::stale(CrcSource::Stale { on_disk_version: 1, crc_version: 3 }, FfiCrcSource::Stale { on_disk_version: 1, crc_version: 3 })]
    #[case::missing(CrcSource::Missing, FfiCrcSource::Missing)]
    #[case::unusable(CrcSource::Unusable, FfiCrcSource::Unusable)]
    #[case::unknown(CrcSource::Unknown, FfiCrcSource::Unknown)]
    fn provenance_conversion_preserves_variant_and_versions(
        #[case] source: CrcSource,
        #[case] expected: FfiCrcSource,
    ) {
        // Given equivalent Rust and C checksum sources with distinct disk and in-memory versions.
        // When converting each representation independently.
        // Then both conversions preserve the variant and each version.
        assert_eq!(FfiCrcSource::from(source), expected);
        assert_eq!(CrcSource::from(expected), source);
    }
}
