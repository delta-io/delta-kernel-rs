//! # Log Compaction
//!
//! **NOTE:** Compaction reads are supported. With `declarative-plans`, use `Snapshot::compact_log`
//! for writes. The imperative writer remains disabled (#2337).
//!
//! This module provides an API for writing log compaction files that aggregate
//! multiple commit JSON files into single compacted files. This improves performance
//! by reducing the number of individual log files that need to be processed during
//! log replay operations.
//!
//! ## Overview
//!
//! Log compaction creates files with the naming pattern
//! `{start_version}.{end_version}.compacted.json` that contain the reconciled actions from all
//! commit files in the specified version range. Reads commits in [start_version, end_version],
//! reusing existing compactions fully contained in that range. Note that `end_version` must be
//! greater than `start_version` (equal versions are not allowed). This is similar to checkpoints
//! but operates on a subset of versions rather than the entire table.
//!
//! ## Usage
//!
//! With `declarative-plans`, call `Snapshot::compact_log` using an engine with a plan executor.
//! Build a fresh latest snapshot with checkpoint discovery enabled. Kernel writes the compacted
//! file and returns its metadata; see the method's rustdoc for interval requirements and errors.
//!
//! ## Example
//!
//! ```no_run
//! # #[cfg(feature = "declarative-plans")]
//! # fn example(engine: &dyn delta_kernel::Engine) -> delta_kernel::Result<()> {
//! use delta_kernel::Snapshot;
//!
//! let snapshot = Snapshot::builder_for("file:///path/to/table/").build(engine)?;
//! let _metadata = snapshot.compact_log(engine, 10, 20)?;
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

use std::sync::LazyLock;

use crate::actions::{
    ADD_FIELD, DOMAIN_METADATA_FIELD, METADATA_FIELD, PROTOCOL_FIELD, REMOVE_FIELD,
    SET_TRANSACTION_FIELD, SIDECAR_FIELD,
};
use crate::schema::{SchemaRef, lazy_schema_ref};
#[cfg(feature = "declarative-plans")]
use crate::{
    DeltaError, Error, KernelError, Result, Snapshot, Version,
    log_segment::{LogSegment, validate_start_version_available},
    log_segment_files::LogSegmentFiles,
    path::LogPathFileType,
    utils::require,
};

mod writer;

pub use writer::{LogCompactionWriter, should_compact};

#[cfg(feature = "declarative-plans")]
impl Snapshot {
    pub(crate) fn log_compaction_segment(
        &self,
        start_version: Version,
        end_version: Version,
    ) -> Result<LogSegment> {
        require!(
            start_version < end_version && end_version <= self.version(),
            KernelError::generic(
                "Log compaction requires start_version < end_version <= snapshot version"
            )
            .into()
        );
        require!(
            self.is_built_as_latest(),
            KernelError::generic(format!(
                "Log compaction requires a snapshot built as latest (snapshot version {})",
                self.version()
            ))
            .into()
        );
        require!(
            !self.skipped_new_checkpoints(),
            KernelError::generic("Log compaction requires checkpoint discovery").into()
        );
        // Reuse the snapshot's log segment because:
        // - Log compactions must not cross checkpoints.
        // - A checkpoint is always a better replay base than a log compaction.
        // - The caller supplies a fresh latest snapshot.
        // => Every commit worth compacting is already in this snapshot's log segment.
        let source = self.log_segment();
        require!(
            source
                .checkpoint_version
                .is_none_or(|version| start_version > version),
            KernelError::generic("Log compaction must start after the latest checkpoint").into()
        );
        let commits = &source.listed.ascending_commit_files;
        let commits_to_compact = &commits[commits
            .partition_point(|file| file.version < start_version)
            ..commits.partition_point(|file| file.version <= end_version)];
        require!(
            commits_to_compact
                .iter()
                .all(|file| file.file_type == LogPathFileType::Commit),
            KernelError::unsupported("Log compaction requires published commits").into()
        );
        validate_start_version_available(start_version, commits_to_compact.first())
            .and_then(|()| {
                LogSegment::try_new(
                    LogSegmentFiles {
                        ascending_commit_files: commits_to_compact.to_vec(),
                        ascending_compaction_files: source
                            .listed
                            .ascending_compaction_files
                            .iter()
                            .filter(|file| {
                                matches!(
                                    file.file_type,
                                    LogPathFileType::CompactedCommit { hi }
                                        if file.version >= start_version && hi <= end_version
                                )
                            })
                            .cloned()
                            .collect(),
                        latest_commit_file: commits_to_compact.last().cloned(),
                        max_published_version: commits_to_compact.last().map(|file| file.version),
                        // CRCs describe full snapshots, not an interval.
                        ..Default::default()
                    },
                    source.log_root.clone(),
                    Some(end_version),
                    None,
                )
            })
            .map_err(|error| match error.without_backtrace() {
                KernelError::EmptyLog
                | KernelError::StartVersionNotFound { .. }
                | KernelError::MissingVersion(_) => {
                    Error::Delta(DeltaError::versions_not_contiguous(
                        commits_to_compact.iter().map(|file| file.version),
                        start_version,
                        end_version,
                        self.version(),
                        Some(Box::new(error)),
                    ))
                }
                _ => Error::Kernel(error),
            })
    }
}

#[cfg(all(test, feature = "declarative-plans"))]
mod declarative_tests;
#[cfg(test)]
mod tests;

/// Schema for extracting relevant actions from log files for compaction.
/// CommitInfo is excluded as it's not needed in compaction files.
static COMPACTION_ACTIONS_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    (&ADD_FIELD),
    (&REMOVE_FIELD),
    (&METADATA_FIELD),
    (&PROTOCOL_FIELD),
    (&SET_TRANSACTION_FIELD),
    (&DOMAIN_METADATA_FIELD),
    (&SIDECAR_FIELD),
};
