//! State for an in-progress manifest (content-tree) commit.

use delta_kernel_derive::internal_api;

use super::leaf_writer::{LeafNodeWriter, LeafNodeWriterResult};
use crate::error::KernelError;
use crate::schema::SchemaRef;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::utils::require;
use crate::{version_as_i64, Engine, Error, KernelResult, Result, Version};

/// State for an in-progress manifest (content-tree) commit.
#[internal_api]
pub(crate) struct ManifestCommitState {
    // TODO(#3352): read these once the manifest-commit write path lands.
    /// Version this commit will write.
    #[allow(dead_code)]
    version_to_write: Version,
    /// Snapshot the commit updates.
    #[allow(dead_code)]
    read_snapshot: SnapshotRef,
    /// Effective config's physical schema at staging time; used by leaf writers.
    physical_schema: SchemaRef,
}

impl ManifestCommitState {
    /// Validates that a manifest commit can be started against `read_snapshot`, then constructs
    /// the state.
    ///
    /// # Errors
    ///
    /// Returns an error if `table_config` does not support the `adaptiveMetadata-preview` feature,
    /// or if delta log commits exist after the last manifest commit (not yet supported).
    pub(super) fn try_new(
        engine: &dyn Engine,
        read_snapshot: SnapshotRef,
        version_to_write: Version,
        table_config: &TableConfiguration,
    ) -> KernelResult<Self> {
        require!(
            table_config.is_feature_supported(&TableFeature::AdaptiveMetadataPreview),
            KernelError::unsupported(
                "manifest commit requires the adaptiveMetadata-preview feature"
            )
        );
        // TODO(#2866): tighten this check (checkpoints that spill to sidecars, log compaction, and
        // the precise "since the last manifest commit" semantics) once the manifest-commit write
        // path lands.
        if let Some(checkpoint) = read_snapshot
            .log_segment()
            .find_last_checkpoint_action(engine)?
        {
            let snapshot_version = version_as_i64(read_snapshot.version())?;
            require!(
                checkpoint.version() >= snapshot_version,
                KernelError::unsupported(format!(
                    "manifest commit does not currently support delta log commits after the last \
                     manifest commit; the latest checkpoint covers version {} but the snapshot is \
                     at {snapshot_version}",
                    checkpoint.version()
                ))
            );
        }
        Ok(ManifestCommitState {
            version_to_write,
            physical_schema: table_config.physical_schema(),
            read_snapshot,
        })
    }

    /// Creates a [`LeafNodeWriter`] for writing a new leaf manifest in this commit.
    ///
    /// The `finish -> LeafNodeWriterResult -> add_leaf` handshake and the `engine` argument are
    /// deliberate (rather than folding a leaf in when its writer drops): they keep the door open to
    /// writing leaf manifests on executors for large appends/CTAS, so a finished leaf's result is
    /// handed back explicitly and `engine` is reserved for that write I/O.
    #[internal_api]
    pub(crate) fn new_leaf_node_writer(&self, _engine: &dyn Engine) -> LeafNodeWriter {
        LeafNodeWriter::new(self.physical_schema.clone())
    }

    /// Folds a finished leaf's [`LeafNodeWriterResult`] into this commit.
    #[internal_api]
    pub(crate) fn add_leaf(&mut self, _result: LeafNodeWriterResult) -> Result<()> {
        // TODO(#3352): fold the finished leaf's result into the commit.
        Err(Error::Kernel(KernelError::unsupported(
            "manifest commit add_leaf is not yet supported",
        )))
    }
}
