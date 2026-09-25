//! State for an in-progress manifest (content-tree) commit.

use std::sync::Arc;

use delta_kernel_derive::internal_api;

use super::leaf_writer::{LeafNodeWriter, LeafNodeWriterResult};
use crate::error::Error;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::utils::require;
use crate::{version_as_i64, DeltaResult, Engine, Version};

/// State for an in-progress manifest (content-tree) commit.
#[internal_api]
pub(crate) struct ManifestCommitState {
    /// Version this commit will write.
    version_to_write: Version,
    /// Snapshot the commit updates.
    read_snapshot: SnapshotRef,
}

impl ManifestCommitState {
    /// Validates that a manifest commit can be started against `read_snapshot`, then constructs
    /// the state.
    ///
    /// # Errors
    ///
    /// Returns an error if `table_config` does not support the `adaptiveMetadata-preview` feature,
    /// if an explicit root manifest was already staged (`has_explicit_root_manifest`), or if delta
    /// log commits exist after the last manifest commit (not yet supported).
    pub(super) fn try_new(
        engine: &dyn Engine,
        read_snapshot: SnapshotRef,
        version_to_write: Version,
        table_config: &TableConfiguration,
        has_explicit_root_manifest: bool,
    ) -> DeltaResult<Self> {
        require!(
            table_config.is_feature_supported(&TableFeature::AdaptiveMetadataPreview),
            Error::unsupported("manifest commit requires the adaptiveMetadata-preview feature")
        );
        require!(
            !has_explicit_root_manifest,
            Error::invalid_transaction_state(
                "explicit root manifest and manifest commit are mutually exclusive"
            )
        );
        // TODO(#2866): tighten this check (checkpoints that spill to sidecars, log compaction, and
        // the precise "since the last manifest commit" semantics) once the manifest-commit write
        // path lands.
        // TODO: the last checkpoint action should ultimately be cached on the Snapshot (resolved at
        // construction), which would let us remove LogSegment::find_last_checkpoint_action.
        if let Some(checkpoint) = read_snapshot
            .log_segment()
            .find_last_checkpoint_action(engine)?
        {
            let snapshot_version = version_as_i64(read_snapshot.version())?;
            require!(
                checkpoint.version() >= snapshot_version,
                Error::unsupported(format!(
                    "manifest commit does not currently support delta log commits after the last \
                     manifest commit; the latest checkpoint covers version {} but the snapshot is \
                     at {snapshot_version}",
                    checkpoint.version()
                ))
            );
        }
        Ok(ManifestCommitState {
            version_to_write,
            read_snapshot,
        })
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
