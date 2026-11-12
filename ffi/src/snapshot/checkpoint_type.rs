//! The type of the selected checkpoint, available without engine I/O.

use delta_kernel::snapshot::SelectedCheckpointType;

use crate::{Handle, OptionalValue, SharedSnapshot};

/// The selected checkpoint's specification. See [`SelectedCheckpointType`].
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub enum FfiSelectedCheckpointType {
    /// A checkpoint was selected, but its specification was not established.
    Unresolved,
    /// The checkpoint follows the V1 specification.
    V1,
    /// The checkpoint follows the V2 specification, with or without sidecars.
    V2,
}

impl From<SelectedCheckpointType> for FfiSelectedCheckpointType {
    fn from(value: SelectedCheckpointType) -> Self {
        match value {
            SelectedCheckpointType::V1 => Self::V1,
            SelectedCheckpointType::V2 => Self::V2,
            _ => Self::Unresolved,
        }
    }
}

impl From<FfiSelectedCheckpointType> for SelectedCheckpointType {
    fn from(value: FfiSelectedCheckpointType) -> Self {
        match value {
            FfiSelectedCheckpointType::Unresolved => Self::Unresolved,
            FfiSelectedCheckpointType::V1 => Self::V1,
            FfiSelectedCheckpointType::V2 => Self::V2,
        }
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
) -> OptionalValue<FfiSelectedCheckpointType> {
    unsafe { snapshot.as_ref() }
        .checkpoint_type()
        .map(Into::into)
        .into()
}
