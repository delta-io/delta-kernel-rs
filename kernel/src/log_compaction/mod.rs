//! # Log Compaction
//!
//! This module provides an API for writing log compaction files that aggregate
//! multiple commit JSON files into single compacted files. This improves performance
//! by reducing the number of individual log files that need to be processed during
//! log replay operations.
//!
//! ## Overview
//!
//! Log compaction creates files with the naming pattern `{start_version}.{end_version}.compacted.json`
//! that contain the reconciled actions from all commit files in the specified version range.
//! Only commit/compaction files that intersect with [start_version, end_version] are processed.
//! Note that `end_version` must be greater than `start_version` (equal versions are not allowed).
//! This is similar to checkpoints but operates on a subset of versions rather than the entire table.
//!
//! ## Usage
//!
//! The log compaction API follows a similar pattern to the checkpoint API:
//!
//! 1. Create a [`LogCompactionWriter`] using [`crate::Snapshot::log_compaction_writer`] to compact the log
//!    from a given start_version to end_version (inclusive)
//! 2. Get the compaction path from [`LogCompactionWriter::compaction_path`]
//! 3. Get the compaction data from [`LogCompactionWriter::compaction_data`]
//! 4. Write the data to the path in cloud storage (engine-specific)
//!
//! ## Example
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use delta_kernel::{LogCompactionDataIterator, LogCompactionWriter};
//! # use delta_kernel::{Engine, Snapshot, DeltaResult, Error, FileMeta};
//! # use url::Url;
//!
//! // Engine-specific function to write compaction data
//! fn write_compaction_file(path: &Url, data: LogCompactionDataIterator) -> DeltaResult<FileMeta> {
//!     // In a real implementation, this would write the data to cloud storage
//!     todo!("Write data batches to storage at path: {}", path)
//! }
//!
//! # fn example(engine: &dyn Engine) -> DeltaResult<()> {
//! // Create a snapshot for the table
//! let table_root = Url::parse("file:///path/to/table")?;
//! let snapshot = Snapshot::builder_for(table_root).build(engine)?;
//!
//! // Create a log compaction writer for versions 10-20
//! let mut writer = snapshot.log_compaction_writer(10, 20)?;
//!
//! let compaction_data = writer.compaction_data(engine)?;
//! let compaction_path = writer.compaction_path();
//!
//! // Write the compaction data to cloud storage
//! let _metadata: FileMeta = write_compaction_file(compaction_path, compaction_data)?;
//! # Ok(())
//! # }
//! ```
//!
//! ## When to Use Log Compaction
//!
//! Log compaction is beneficial when:
//! - Table has many small commit files that slow down log replay
//! - Reduce the number of files without creating a full checkpoint
//! - Optimize specific version ranges that are frequently accessed
//!
//! The [`should_compact`] utility function can help determine when compaction is appropriate
//! based on version intervals.
//!
//! Please see <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#log-compaction-files>
//! for more details
//!
//! ## Relationship to Checkpoints
//!
//! - **Checkpoints**: Aggregate the entire table state up to a specific version
//! - **Log Compaction**: Aggregates only a specific range of commit files
//! - Both use similar action reconciliation logic but serve different use cases

use std::sync::{Arc, LazyLock};

use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
    SIDECAR_NAME,
};
use crate::schema::{SchemaRef, StructField, StructType, ToSchema as _};

mod writer;

pub use writer::{should_compact, LogCompactionDataIterator, LogCompactionWriter};

#[cfg(test)]
mod tests;

/// Schema for extracting relevant actions from log files for compaction.
/// CommitInfo is excluded as it's not needed in compaction files.
static COMPACTION_ACTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
        StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()),
    ]))
});
