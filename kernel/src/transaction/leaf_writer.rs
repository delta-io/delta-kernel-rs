//! Writers for individual leaf manifests within a manifest (content-tree) commit.

use delta_kernel_derive::internal_api;

use crate::error::KernelError;
use crate::schema::SchemaRef;
use crate::{Engine, EngineData, Error, Result};

/// Writes a single leaf manifest for a manifest (content-tree) commit.
///
/// A `LeafNodeWriter` accepts file changes, then [`finish`](Self::finish) writes the manifest.
#[internal_api]
#[derive(Debug)]
pub(crate) struct LeafNodeWriter {
    /// Physical (column-mapped) schema of the table's data.
    // TODO(#3352): read this once appends are implemented in add_files/finish.
    #[allow(dead_code)]
    physical_schema: SchemaRef,
}

/// Output of finishing a [`LeafNodeWriter`], folded back into the commit via
/// [`ManifestCommitState::add_leaf`](super::ManifestCommitState::add_leaf).
///
/// Opaque: its contents are an implementation detail filled in as the manifest-commit write path
/// is built out.
#[internal_api]
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct LeafNodeWriterResult {}

impl LeafNodeWriter {
    pub(super) fn new(physical_schema: SchemaRef) -> Self {
        LeafNodeWriter { physical_schema }
    }

    #[cfg(test)]
    pub(crate) fn physical_schema(&self) -> &SchemaRef {
        &self.physical_schema
    }

    /// Buffers new data files described by `add_metadata` for writing into this leaf manifest.
    ///
    /// `add_metadata` follows the add-file metadata schema
    /// ([`Transaction::add_files_schema`](crate::transaction::Transaction::add_files_schema)).
    #[internal_api]
    pub(crate) fn add_files(
        &mut self,
        _engine: &dyn Engine,
        _add_metadata: Box<dyn EngineData>,
    ) -> Result<()> {
        // TODO(#3352): implement buffering appends, and add the other update kinds a leaf must
        // accept (existing-file moves/removals and deletion-vector updates).
        Err(Error::Kernel(KernelError::unsupported(
            "manifest commit leaf writer add_files is not yet supported",
        )))
    }

    /// Writes the buffered changes as a leaf manifest and returns its [`LeafNodeWriterResult`].
    #[internal_api]
    pub(crate) fn finish(self, _engine: &dyn Engine) -> Result<LeafNodeWriterResult> {
        // TODO(#3352): write the buffered changes as a leaf manifest.
        Err(Error::Kernel(KernelError::unsupported(
            "manifest commit leaf writer finish is not yet supported",
        )))
    }
}
