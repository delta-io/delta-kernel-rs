//! Single integration test crate for delta_kernel.
//!
//! All integration tests are linked into one binary so they share a single
//! compile + link cycle. Each topic lives in its own submodule below and is
//! organized by file/directory.

#[macro_use]
mod common;

mod create_table;
mod cross_product;
mod features;
mod golden_tables;
mod hdfs;
mod log;
mod metrics;
mod read;
mod write;
