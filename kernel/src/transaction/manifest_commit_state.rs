//! State for an in-progress manifest (content-tree) commit.

use std::sync::Arc;

use delta_kernel_derive::internal_api;

use super::leaf_writer::{LeafNodeWriter, LeafNodeWriterResult};
use crate::error::Error;
use crate::snapshot::SnapshotRef;
use crate::{DeltaResult, Engine, Version};

/// State for an in-progress manifest (content-tree) commit.
///
/// Obtained from
/// [`Transaction::with_manifest_commit`](crate::transaction::Transaction::with_manifest_commit).
/// It hands out [`LeafNodeWriter`]s (via [`new_leaf_node_writer`](Self::new_leaf_node_writer)) that
/// accept file changes, and folds their results back in via [`add_leaf`](Self::add_leaf) before the
/// owning transaction commits.
#[internal_api]
pub(crate) struct ManifestCommitState {
    /// Version this commit will write.
    version_to_write: Version,
    /// Snapshot the commit updates.
    read_snapshot: SnapshotRef,
}

impl ManifestCommitState {
    pub(super) fn new(version_to_write: Version, read_snapshot: SnapshotRef) -> Self {
        ManifestCommitState {
            version_to_write,
            read_snapshot,
        }
    }

    /// Creates a [`LeafNodeWriter`] for writing a new leaf manifest in this commit.
    ///
    /// # Errors
    ///
    /// Returns an error if the table's physical schema cannot be derived.
    #[internal_api]
    pub(crate) fn new_leaf_node_writer(
        &mut self,
        _engine: &dyn Engine,
    ) -> DeltaResult<LeafNodeWriter> {
        let column_mapping_mode = self
            .read_snapshot
            .table_configuration()
            .column_mapping_mode();
        let physical_schema = Arc::new(
            self.read_snapshot
                .schema()
                .make_physical(column_mapping_mode)?,
        );
        Ok(LeafNodeWriter::new(self.version_to_write, physical_schema))
    }

    /// Folds a finished leaf's [`LeafNodeWriterResult`] into this commit.
    ///
    /// # Errors
    ///
    /// Currently always [`Error::Unsupported`]: the manifest-commit write path is not yet built.
    #[internal_api]
    pub(crate) fn add_leaf(&mut self, _result: LeafNodeWriterResult) -> DeltaResult<()> {
        Err(Error::unsupported(
            "manifest commit add_leaf is not yet supported",
        ))
    }
}
