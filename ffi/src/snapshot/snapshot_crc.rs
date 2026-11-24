//! Snapshot checksum provenance, available without engine I/O.

use delta_kernel::snapshot::CrcSource;

use crate::{Handle, SharedSnapshot};

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
