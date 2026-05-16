//! On-disk record structs used as schema sources and as the output types
//! of kernel-internal readers.
//!
//! Each struct here describes a JSON or Parquet record shape the kernel
//! parses. Deriving [`ToSchema`] lets callers derive the scan schema at
//! plan-build time (`T::to_schema()`). The same struct doubles as the
//! `KdfOutput::Output` for the consumer that parses it — callers receive a
//! value of this type (or `Option`/`Vec`/`HashMap` of it) when the pipeline
//! terminates via `.consume(state)`.

use delta_kernel_derive::ToSchema;
use serde::{Deserialize, Serialize};

/// `_last_checkpoint` JSON hint file. Parsed by the snapshot SM's checkpoint
/// hint reader.
#[derive(ToSchema, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointHintRecord {
    /// Commit version the checkpoint covers.
    pub version: i64,
    /// Total logical size of the checkpoint (bytes), if known.
    pub size: Option<i64>,
    /// Number of parts for multipart checkpoints, if any.
    pub parts: Option<i32>,
    /// Total on-disk size of the checkpoint (bytes), if known.
    pub size_in_bytes: Option<i64>,
    /// Count of add-file actions in the checkpoint, if tracked.
    pub num_of_add_files: Option<i64>,
}
