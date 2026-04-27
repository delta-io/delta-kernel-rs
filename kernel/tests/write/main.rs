//! Consolidated entry point for write-path integration tests.
//!
//! All write-related integration tests are linked into a single binary to keep
//! incremental build/link times reasonable. Each topic lives in its own file
//! under this directory and is declared as a module below.

#[path = "../common/mod.rs"]
mod common;

mod append;
mod cdf;
mod clustered;
mod column_mapping;
mod commit_info;
mod domain_metadata;
mod ict;
mod partitioned;
mod post_commit;
mod relative_paths;
mod remove_dv;
mod row_tracking;
mod stats;
mod txn;
mod types;
