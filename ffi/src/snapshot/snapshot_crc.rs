//! Snapshot checksum provenance, available without engine I/O.

use delta_kernel::snapshot::{CrcProvenance, CrcSource};

use crate::{Handle, OptionalValue, SharedSnapshot};

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
        /// Total commits incorporated since the original on-disk checksum version.
        num_commits: u64,
    },
    /// A usable checksum is retained behind the snapshot version.
    Stale,
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
            CrcSource::Reconstructed { num_commits } => Self::Reconstructed { num_commits },
            CrcSource::Stale => Self::Stale,
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
            FfiCrcSource::Reconstructed { num_commits } => Self::Reconstructed { num_commits },
            FfiCrcSource::Stale => Self::Stale,
            FfiCrcSource::Missing => Self::Missing,
            FfiCrcSource::Unusable => Self::Unusable,
            FfiCrcSource::Unknown => Self::Unknown,
        }
    }
}

/// Retained checksum provenance. Contains no owned memory and needs no release.
/// `Missing` and `Unusable` preserve observations even when no checksum is retained.
#[derive(Debug, PartialEq)]
#[repr(C)]
pub struct FfiCrcProvenance {
    /// How the checksum was resolved.
    pub source: FfiCrcSource,
    /// Version of the on-disk CRC from which the retained CRC was read or reconstructed.
    /// Preserved across replay and while the CRC is stale, so later reconstruction can count all
    /// commits since that on-disk version. See [`CrcProvenance::base_version`].
    pub base_version: OptionalValue<u64>,
}

impl From<CrcProvenance> for FfiCrcProvenance {
    fn from(value: CrcProvenance) -> Self {
        let source = value.source.into();
        Self {
            source,
            base_version: if source == FfiCrcSource::Unknown {
                None
            } else {
                value.base_version
            }
            .into(),
        }
    }
}

impl FfiCrcProvenance {
    pub(crate) fn to_kernel(&self) -> CrcProvenance {
        CrcProvenance::new(
            self.source.into(),
            Option::<&u64>::from(&self.base_version).copied(),
        )
    }
}

/// Writes the retained checksum provenance to `out` without engine I/O.
///
/// # Safety
/// `snapshot` must be valid and remain owned by the caller. `out` must be non-null, aligned,
/// writable storage for one `FfiCrcProvenance`. It may be uninitialized before the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_crc_provenance(
    snapshot: Handle<SharedSnapshot>,
    out: *mut FfiCrcProvenance,
) {
    let value = unsafe { snapshot.as_ref() }.crc_provenance().into();
    unsafe { out.write(value) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::at_version(CrcSource::AtVersion, Some(2), Some(2))]
    #[case::reconstructed(CrcSource::Reconstructed { num_commits: 2 }, Some(0), Some(0))]
    #[case::stale(CrcSource::Stale, Some(0), Some(0))]
    #[case::missing(CrcSource::Missing, None, None)]
    #[case::unusable(CrcSource::Unusable, None, None)]
    #[case::unknown(CrcSource::Unknown, None, None)]
    #[case::unknown_with_baseline(CrcSource::Unknown, Some(2), None)]
    fn provenance_conversion_preserves_source_and_normalizes_unknown_baseline(
        #[case] source: CrcSource,
        #[case] base_version: Option<u64>,
        #[case] expected_base_version: Option<u64>,
    ) {
        let provenance = CrcProvenance::new(source, base_version);
        let ffi = FfiCrcProvenance::from(provenance);
        assert_eq!(
            ffi.to_kernel(),
            CrcProvenance::new(source, expected_base_version)
        );
    }
}
