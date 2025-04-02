//! # Delta Kernel Checkpoint API
//!
//! This module implements the API for writing single-file checkpoints in Delta tables.
//! Checkpoints provide a compact summary of the table state, enabling faster recovery by
//! avoiding full log replay. This API supports multiple checkpoint types:
//!
//! 1. **Single-file Classic-named V1 Checkpoint** – for legacy tables that do not support
//!    the v2Checkpoints feature.
//! 2. **Single-file Classic-named V2 Checkpoint** – for backwards compatibility when the
//!    v2Checkpoints feature is enabled.
//! 3. **Single-file UUID-named V2 Checkpoint** – the recommended option for small to medium
//!    tables with v2Checkpoints support.
//!
//! TODO!(seb): API WIP
//! The API is designed using a builder pattern via the `CheckpointBuilder`, which performs
//! table feature detection and configuration validation before constructing a `CheckpointWriter`.
//!
//! The `CheckpointWriter` then orchestrates the process of:
//! - Replaying Delta log actions (via the `checkpoint/log_replay.rs` module) to filter, deduplicate,
//!   and select the actions that represent the table's current state.
//! - Writing the consolidated checkpoint data to a single file.
//! - Finalizing the checkpoint by generating a `_last_checkpoint` file with metadata.
//!
//! ## Example
//!
//! ```ignore
//! let path = "./tests/data/app-txn-no-checkpoint";
//! let engine = Arc::new(SyncEngine::new());
//! let table = Table::try_from_uri(path)?;
//!
//! // Create a checkpoint builder for the table at a specific version
//! let builder = table.checkpoint(&engine, Some(2))?;
//!
//! // Optionally configure the builder (e.g., force classic naming)
//! let writer = builder.with_classic_naming(true);
//!
//! // Build the checkpoint writer
//! let mut writer = builder.build(&engine)?;
//!
//! // Retrieve checkpoint data (ensuring single consumption)
//! let checkpoint_data = writer.get_checkpoint_info()?;
//!
//! // Write checkpoint data to file and collect metadata before finalizing
//! writer.finalize_checkpoint(&engine, &checkpoint_metadata)?;
//! ```
//!
//! This module, along with its submodule `checkpoint/log_replay.rs`, provides the full
//! API and implementation for generating checkpoints. See `checkpoint/log_replay.rs` for details
//! on how log replay is used to filter and deduplicate actions for checkpoint creation.
pub mod log_replay;
