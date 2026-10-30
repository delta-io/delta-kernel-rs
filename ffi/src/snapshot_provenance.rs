//! Snapshot provenance observations, available without engine I/O.

use delta_kernel::snapshot::{CrcProvenance, CrcSource, SelectedCheckpointKind};

use crate::{Handle, OptionalValue, SharedSnapshot};

/// The specification of the selected checkpoint, independently of table policy.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum FfiSelectedCheckpointKind {
    /// No checkpoint selected.
    None,
    /// V1 specification.
    V1,
    /// V2 specification, including checkpoints without sidecars.
    V2,
    /// Not established by ordinary processing.
    Unknown,
}

/// How the retained checksum was resolved.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum FfiCrcSource {
    /// Usable checksum file at the snapshot version.
    AtVersion,
    /// Older checksum file advanced through all commits up to the snapshot version.
    Reconstructed {
        /// Total commits since the original disk checksum, across incremental updates.
        num_commits: u64,
    },
    /// Retained checksum is behind the snapshot version.
    Stale,
    /// No applicable checksum file found during normal loading.
    Missing,
    /// Observed candidate unusable, without a usable fallback.
    Unusable,
    /// Disk provenance not established.
    Unknown,
}

/// Retained checksum provenance. Contains no owned memory and needs no release.
#[derive(Debug, PartialEq)]
#[repr(C)]
pub struct FfiCrcProvenance {
    /// How the checksum was resolved.
    pub source: FfiCrcSource,
    /// Version of the original usable checksum file, if known.
    pub base_version: OptionalValue<u64>,
}

impl From<SelectedCheckpointKind> for FfiSelectedCheckpointKind {
    fn from(value: SelectedCheckpointKind) -> Self {
        match value {
            SelectedCheckpointKind::None => Self::None,
            SelectedCheckpointKind::V1 => Self::V1,
            SelectedCheckpointKind::V2 => Self::V2,
            SelectedCheckpointKind::Unknown => Self::Unknown,
        }
    }
}

impl From<FfiSelectedCheckpointKind> for SelectedCheckpointKind {
    fn from(value: FfiSelectedCheckpointKind) -> Self {
        match value {
            FfiSelectedCheckpointKind::None => Self::None,
            FfiSelectedCheckpointKind::V1 => Self::V1,
            FfiSelectedCheckpointKind::V2 => Self::V2,
            FfiSelectedCheckpointKind::Unknown => Self::Unknown,
        }
    }
}

impl From<CrcProvenance> for FfiCrcProvenance {
    fn from(value: CrcProvenance) -> Self {
        Self {
            source: match value.source {
                CrcSource::AtVersion => FfiCrcSource::AtVersion,
                CrcSource::Reconstructed { num_commits } => {
                    FfiCrcSource::Reconstructed { num_commits }
                }
                CrcSource::Stale => FfiCrcSource::Stale,
                CrcSource::Missing => FfiCrcSource::Missing,
                CrcSource::Unusable => FfiCrcSource::Unusable,
                CrcSource::Unknown => FfiCrcSource::Unknown,
            },
            base_version: value.base_version.into(),
        }
    }
}

impl FfiCrcProvenance {
    pub(crate) fn to_kernel(&self) -> CrcProvenance {
        CrcProvenance {
            source: match self.source {
                FfiCrcSource::AtVersion => CrcSource::AtVersion,
                FfiCrcSource::Reconstructed { num_commits } => {
                    CrcSource::Reconstructed { num_commits }
                }
                FfiCrcSource::Stale => CrcSource::Stale,
                FfiCrcSource::Missing => CrcSource::Missing,
                FfiCrcSource::Unusable => CrcSource::Unusable,
                FfiCrcSource::Unknown => CrcSource::Unknown,
            },
            base_version: Option::<&u64>::from(&self.base_version).copied(),
        }
    }
}

/// Returns the selected checkpoint specification without engine I/O.
///
/// # Safety
/// `snapshot` must be valid and remain owned by the caller throughout the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_selected_checkpoint_kind(
    snapshot: Handle<SharedSnapshot>,
) -> FfiSelectedCheckpointKind {
    unsafe { snapshot.as_ref() }
        .selected_checkpoint_kind()
        .into()
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
