//! Consolidated entry point for protocol-feature integration tests.
//!
//! All tests covering reader-side behavior of individual table features share a single
//! binary so they pay the Arrow / Parquet / object_store link cost once.

#[macro_use]
#[path = "../common/mod.rs"]
mod common;

mod alter_table;
mod cdf;
mod clustering_e2e;
mod dv;
mod maintenance_ops;
mod row_tracking;
