//! Helpers to validate Engine implementations.
//!
//! ## DAT read harness modes
//!
//! DAT reader test cases can optionally declare `"read_mode"` in
//! `test_case_info.json`:
//!
//! - `"scan"` (default): validate via `Snapshot::scan`.
//! - `"fsr_add_only"`: validate DataFusion-driven `Snapshot::full_state` by comparing its terminal
//!   add-action path set against selected scan files.
//!
//! Missing `read_mode` is treated as `"scan"` for backward compatibility.

pub mod acceptance_workloads;
pub mod data;
pub mod meta;
pub use meta::*;
