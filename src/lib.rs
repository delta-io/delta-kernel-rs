//! The delta-kernel crate provides a query engine-agnostic interface for reading (and soon
//! writing) Delta tables.

#![warn(
    unreachable_pub,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    rust_2018_idioms,
    rust_2021_compatibility,
    missing_debug_implementations
)]

/// Includes top-level DeltaTable type which can construct Snapshots
pub mod delta_table;

/// defines a common expression language for use in data skipping predicates
pub mod expressions;

/// generic parquet interface
pub mod parquet_reader;

/// Implements snapshots reads/scans via iterator API yielding arrow record batches
pub mod scan;

/// In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
/// has schema etc.) pub mod snapshot;
pub mod snapshot;

/// generic storage interface
pub mod storage;

/// delta_log module for defining schema of log files, actions, etc.
mod delta_log;

/// Delta table version is 8 byte unsigned int
pub type Version = u64;
