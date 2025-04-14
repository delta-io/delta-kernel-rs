//! # Delta Kernel Checkpoint API
//!
//! This module implements the API for writing checkpoints in delta tables.
//! Checkpoints provide a compact summary of the table state, enabling faster recovery by
//! avoiding full log replay. This API supports three checkpoint types:
//!
//! TODO!(seb): Include docs when implemented
pub(crate) mod log_replay;
