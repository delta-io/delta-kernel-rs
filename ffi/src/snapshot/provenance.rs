//! Snapshot provenance observations, available without engine I/O.

use delta_kernel::snapshot::{CheckpointType, CrcProvenance, CrcSource};

use crate::{Handle, OptionalValue, SharedSnapshot};

/// The selected checkpoint's specification. See [`CheckpointType`].
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub enum FfiCheckpointType {
    /// A checkpoint was selected, but its specification was not established.
    Unresolved,
    /// The checkpoint follows the V1 specification.
    V1,
    /// The checkpoint follows the V2 specification, with or without sidecars.
    V2,
}

impl From<CheckpointType> for FfiCheckpointType {
    fn from(value: CheckpointType) -> Self {
        match value {
            CheckpointType::V1 => Self::V1,
            CheckpointType::V2 => Self::V2,
            _ => Self::Unresolved,
        }
    }
}

impl From<FfiCheckpointType> for CheckpointType {
    fn from(value: FfiCheckpointType) -> Self {
        match value {
            FfiCheckpointType::Unresolved => Self::Unresolved,
            FfiCheckpointType::V1 => Self::V1,
            FfiCheckpointType::V2 => Self::V2,
        }
    }
}

/// How a retained checksum relates to an observed checksum file. See [`CrcSource`].
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub enum FfiCrcSource {
    /// A usable checksum file was resolved at the snapshot version.
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
    /// Version of the on-disk checksum file used as the original baseline, if known.
    pub base_version: OptionalValue<u64>,
}

impl From<CrcProvenance> for FfiCrcProvenance {
    fn from(value: CrcProvenance) -> Self {
        Self {
            source: value.source.into(),
            base_version: value.base_version.into(),
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

/// Returns the selected checkpoint specification, when known, without engine I/O.
/// Returns absent if no checkpoint was selected, or `Unresolved` if its type was not retained
/// during construction. The observation remains fixed for the snapshot's lifetime.
///
/// # Safety
/// `snapshot` must be valid and remain owned by the caller throughout the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_checkpoint_type(
    snapshot: Handle<SharedSnapshot>,
) -> OptionalValue<FfiCheckpointType> {
    unsafe { snapshot.as_ref() }
        .checkpoint_type()
        .map(Into::into)
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
