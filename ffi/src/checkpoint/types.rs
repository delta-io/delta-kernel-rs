//! C-compatible checkpoint format types.

use delta_kernel::checkpoint::SelectedCheckpointType;

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
