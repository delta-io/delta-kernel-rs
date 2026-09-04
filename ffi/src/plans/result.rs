//! FFI types for mimicking the kernel's `PlanResult` enum.

use super::iter::{CBytesIterator, CEngineDataIterator, CFileMetaIterator};
use crate::handle::Handle;
use crate::ExclusiveRustBytes;

/// C-compatible equivalent of the kernel's `ParquetFooter` struct.
///
/// The schema is delivered as a proto-serialized `delta.kernel.schema.StructType` message. The
/// engine should call [`allocate_kernel_bytes`](crate::allocate_kernel_bytes) to copy those bytes
/// into a kernel-owned [`ExclusiveRustBytes`] handle that can then be embedded into this struct.
#[repr(C)]
pub struct CParquetFooter {
    pub schema_proto: Handle<ExclusiveRustBytes>,
}

/// C-compatible equivalent of the kernel's `PlanResult` enum.
///
/// We instruct cbindgen to prefix enum variants with enum name (e.g. `CPlanResult_Unit`)
/// so they don't collide with other identifiers (e.g. with the `FileMeta` struct)
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum CPlanResult {
    Unit,
    Data(CEngineDataIterator),
    FileMeta(CFileMetaIterator),
    Bytes(CBytesIterator),
    ParquetFooter(CParquetFooter),
}
