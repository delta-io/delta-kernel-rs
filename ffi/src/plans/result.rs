//! FFI types for mimicking the kernel's `PlanResult` enum.

use super::iter::{CBytesIterator, CEngineDataIterator, CFileMetaIterator};
use crate::scan::EngineSchema;

/// C-compatible equivalent of the kernel's `ParquetFooter` struct.
///
/// The schema is delivered as an [`EngineSchema`] - an opaque engine-owned schema which will be
/// materialized on the kernel-side via vistor pattern.
#[repr(C)]
pub struct CParquetFooter {
    pub schema: EngineSchema,
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
