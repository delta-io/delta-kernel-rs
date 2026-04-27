//! Consolidated entry point for log-replay and checkpoint-structure integration tests.
//!
//! All tests covering log structure (commits, checkpoints, log compaction, CRC files)
//! share a single binary so they pay the Arrow / Parquet / object_store link cost once.

#[macro_use]
#[path = "../common/mod.rs"]
mod common;

mod checkpoint_transform;
mod crc;
mod empty_log_files;
mod log_compaction;
mod log_tail;
mod v2_checkpoints;
