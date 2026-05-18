//! Helpers to validate Engine implementations. DAT reader cases may set `read_mode`
//! on `TestCaseInfo`; the DataFusion harness honors `"fsr_add_only"`.

pub mod acceptance_workloads;
pub mod data;
pub mod meta;
pub use meta::*;
