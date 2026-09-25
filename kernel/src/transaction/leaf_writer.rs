//! Writers for individual leaf manifests within a manifest (content-tree) commit.

use delta_kernel_derive::internal_api;

use crate::error::Error;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine, EngineData, Version};

/// Writes a single leaf manifest for a manifest (content-tree) commit.
///
/// Obtained from
/// [`ManifestCommitState::new_leaf_node_writer`](super::manifest_commit_state::ManifestCommitState::new_leaf_node_writer).
/// A leaf accepts file changes, then [`finish`](Self::finish) writes the manifest and returns a
/// [`LeafNodeWriterResult`] the caller folds back into the owning `ManifestCommitState`.
#[internal_api]
#[derive(Debug)]
pub(crate) struct LeafNodeWriter {
    // TODO(#2866): these are the state the write path needs; read them once appends are
    // implemented in add_files/finish.
    /// Table version this leaf is written for.
    #[allow(dead_code)]
    version: Version,
    /// Physical (column-mapped) schema of the table's data.
    #[allow(dead_code)]
    physical_schema: SchemaRef,
}

/// Output of finishing a [`LeafNodeWriter`], folded back into the commit via
/// [`ManifestCommitState::add_leaf`](super::manifest_commit_state::ManifestCommitState::add_leaf).
///
/// Opaque: its contents are an implementation detail filled in as the manifest-commit write path
/// is built out.
#[internal_api]
#[derive(Debug)]
pub(crate) struct LeafNodeWriterResult {}

impl LeafNodeWriter {
    pub(super) fn new(version: Version, physical_schema: SchemaRef) -> Self {
        LeafNodeWriter {
            version,
            physical_schema,
        }
    }

    /// Buffers new data files described by `add_metadata` for writing into this leaf manifest.
    ///
    /// `add_metadata` follows the add-file metadata schema
    /// ([`Transaction::add_files_schema`](crate::transaction::Transaction::add_files_schema)).
    ///
    /// # Errors
    ///
    /// Currently always [`Error::Unsupported`]: the manifest-commit write path is not yet built.
    // TODO(#2866): implement buffering appends, and add the other update kinds a leaf must accept
    // (existing-file moves/removals and deletion-vector updates).
    #[internal_api]
    pub(crate) fn add_files(
        &mut self,
        _engine: &dyn Engine,
        _add_metadata: Box<dyn EngineData>,
    ) -> DeltaResult<()> {
        Err(Error::unsupported(
            "manifest commit leaf writer add_files is not yet supported",
        ))
    }

    /// Writes the buffered changes as a leaf manifest and returns its [`LeafNodeWriterResult`].
    ///
    /// # Errors
    ///
    /// Currently always [`Error::Unsupported`]: the manifest-commit write path is not yet built.
    #[internal_api]
    pub(crate) fn finish(self, _engine: &dyn Engine) -> DeltaResult<LeafNodeWriterResult> {
        Err(Error::unsupported(
            "manifest commit leaf writer finish is not yet supported",
        ))
    }
}
