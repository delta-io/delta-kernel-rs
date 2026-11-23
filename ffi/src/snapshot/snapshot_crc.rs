//! Snapshot checksum provenance, available without engine I/O.

use delta_kernel::snapshot::CrcSource;

use crate::{Handle, SharedSnapshot};

/// How a retained checksum relates to an observed checksum file. See [`CrcSource`].
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub enum FfiCrcSource {
    /// A usable on-disk checksum file was resolved at the snapshot version.
    AtVersion,
    /// An on-disk checksum was advanced in memory to the snapshot version.
    Reconstructed {
        /// Version of the original on-disk checksum. Subtract this from the snapshot version
        /// to obtain the cumulative number of incorporated commits.
        on_disk_version: u64,
    },
    /// A usable checksum is retained behind the snapshot version.
    Stale {
        /// Version of the on-disk checksum from which the retained CRC was read or reconstructed.
        /// This may be older than the retained CRC's own version.
        /// The C API does not expose that retained version, so the number of incorporated
        /// commits cannot be derived. Subtracting this from the snapshot version would overcount.
        on_disk_version: u64,
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
            CrcSource::AtVersion => Self::AtVersion,
            CrcSource::Reconstructed { on_disk_version } => Self::Reconstructed { on_disk_version },
            CrcSource::Stale { on_disk_version } => Self::Stale { on_disk_version },
            CrcSource::Missing => Self::Missing,
            CrcSource::Unusable => Self::Unusable,
            _ => Self::Unknown,
        }
    }
}

impl From<FfiCrcSource> for CrcSource {
    fn from(value: FfiCrcSource) -> Self {
        match value {
            FfiCrcSource::AtVersion => Self::AtVersion,
            FfiCrcSource::Reconstructed { on_disk_version } => {
                Self::Reconstructed { on_disk_version }
            }
            FfiCrcSource::Stale { on_disk_version } => Self::Stale { on_disk_version },
            FfiCrcSource::Missing => Self::Missing,
            FfiCrcSource::Unusable => Self::Unusable,
            FfiCrcSource::Unknown => Self::Unknown,
        }
    }
}

/// Writes the retained checksum provenance to `out` without engine I/O.
///
/// # Safety
/// `snapshot` must be valid and remain owned by the caller. `out` must be non-null, aligned,
/// writable storage for one `FfiCrcSource`. It may be uninitialized before the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_crc_source(
    snapshot: Handle<SharedSnapshot>,
    out: *mut FfiCrcSource,
) {
    let value = unsafe { snapshot.as_ref() }.crc_source().into();
    unsafe { out.write(value) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::at_version(CrcSource::AtVersion)]
    #[case::reconstructed(CrcSource::Reconstructed { on_disk_version: 0 })]
    #[case::stale(CrcSource::Stale { on_disk_version: 0 })]
    #[case::missing(CrcSource::Missing)]
    #[case::unusable(CrcSource::Unusable)]
    #[case::unknown(CrcSource::Unknown)]
    fn provenance_conversion_preserves_source(#[case] source: CrcSource) {
        // Given a checksum source, including its on-disk version when present.
        let ffi = FfiCrcSource::from(source);

        // When the source crosses the FFI boundary and is converted back.
        let round_trip = CrcSource::from(ffi);

        // Then the source and its payload are preserved.
        assert_eq!(round_trip, source);
    }
}
