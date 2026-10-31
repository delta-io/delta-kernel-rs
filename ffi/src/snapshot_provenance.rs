//! Snapshot provenance observations, available without engine I/O.

use delta_kernel::snapshot::{CrcProvenance, CrcSource, SelectedCheckpointKind};

use crate::{Handle, OptionalValue, SharedSnapshot};

/// Retained checksum provenance. Contains no owned memory and needs no release.
#[derive(Debug, PartialEq)]
#[repr(C)]
pub struct FfiCrcProvenance {
    /// How the checksum was resolved.
    pub source: CrcSource,
    /// Version of the original usable checksum file, if known.
    pub base_version: OptionalValue<u64>,
}

impl From<CrcProvenance> for FfiCrcProvenance {
    fn from(value: CrcProvenance) -> Self {
        Self {
            source: value.source,
            base_version: value.base_version.into(),
        }
    }
}

impl FfiCrcProvenance {
    pub(crate) fn to_kernel(&self) -> CrcProvenance {
        CrcProvenance::new(
            self.source,
            Option::<&u64>::from(&self.base_version).copied(),
        )
    }
}

/// Returns the selected checkpoint specification without engine I/O.
///
/// # Safety
/// `snapshot` must be valid and remain owned by the caller throughout the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_selected_checkpoint_kind(
    snapshot: Handle<SharedSnapshot>,
) -> SelectedCheckpointKind {
    unsafe { snapshot.as_ref() }.selected_checkpoint_kind()
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
