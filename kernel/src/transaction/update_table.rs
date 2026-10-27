//! Mode-independent configuration for update-table transactions.

use delta_kernel_derive::internal_api;

#[cfg(feature = "adaptive-metadata-in-dev")]
use super::{
    manifest_commit_state::ManifestCommitState, root_manifest_file::RootManifestFile, ManifestWrite,
};
use super::{ExecutionMode, Transaction, UpdateTable};
use crate::error::KernelError;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::table_features::TableFeature;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::utils::require;
use crate::KernelResult;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::{Engine, FileMeta};

// =============================================================================
// Update table transactions only
// =============================================================================
impl<MODE: ExecutionMode> Transaction<UpdateTable, MODE> {
    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /// Remove domain metadata from the Delta log.
    /// If the domain exists in the Delta log, this creates a tombstone to logically delete
    /// the domain. The tombstone preserves the previous configuration value.
    /// If the domain does not exist in the Delta log, this is a no-op.
    /// Note that each domain can only appear once per transaction. That is, multiple operations
    /// on the same domain are disallowed in a single transaction, as well as setting and removing
    /// the same domain in a single transaction. If a duplicate domain is included, the `commit`
    /// will fail (that is, we don't eagerly check domain validity here).
    /// Removing metadata for multiple distinct domains is allowed.
    pub fn with_domain_metadata_removed(mut self, domain: String) -> Self {
        self.user_domain_removals.push(domain);
        self
    }

    /// Acknowledges that the connector correctly preserves Stable Row IDs and Stable Row Commit
    /// Versions. That is:
    ///
    /// - Copied or updated rows retain their Stable Row IDs.
    /// - Copied rows retain their Stable Row Commit Versions.
    /// - The connector preserves these values in the materialized Row ID and Row Commit Version
    ///   columns.
    /// - The connector also satisfies all protocol MUST requirements for those columns.
    ///
    /// See [Row Tracking] in the Delta protocol for more details.
    ///
    /// Kernel does not validate rewritten files or materialized values. Calling this method asserts
    /// that the connector has satisfied these requirements.
    ///
    /// The Delta protocol specifies this preservation as a SHOULD requirement. Kernel requires it
    /// for compatibility.
    ///
    /// This acknowledgment is required before committing Remove actions or deletion-vector updates
    /// on tables with Row Tracking enabled.
    ///
    /// [Row Tracking]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-tracking
    pub fn ack_row_tracking_preservation(&mut self) {
        self.row_tracking_preservation_acknowledged = true;
    }

    /// Set an explicit row-tracking high-water mark for this transaction.
    ///
    /// Use this when row IDs must also be coordinated with another system. Kernel still assigns
    /// row-tracking fields to added files and rejects this value if it is less than the high-water
    /// mark calculated from those files. Callers cannot use [`Self::with_domain_metadata`] to
    /// modify `delta.rowTracking` or other `delta.*` domains. Table-feature and table-state
    /// validation occurs during commit.
    #[internal_api]
    #[allow(dead_code)] // used in FFI
    pub(crate) fn with_row_tracking_high_water_mark(
        mut self,
        high_water_mark: i64,
    ) -> KernelResult<Self> {
        if self.provided_row_tracking_high_water_mark.is_some() {
            return Err(KernelError::generic(
                "Row-tracking high-water mark already specified in this transaction",
            ));
        }
        self.provided_row_tracking_high_water_mark = Some(high_water_mark);
        Ok(self)
    }

    /// Stages `file` to be committed as the table's root manifest.
    ///
    /// Mutually exclusive with [`with_manifest_commit`](Transaction::with_manifest_commit), which
    /// has kernel build the tree instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not support the `adaptiveMetadata-preview` feature, or if
    /// a manifest (content-tree) commit was already staged.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub fn with_root_manifest_file(mut self, file: FileMeta) -> KernelResult<Self> {
        require!(
            !matches!(self.manifest_write, Some(ManifestWrite::Commit(_))),
            KernelError::invalid_transaction_state(
                "explicit root manifest and manifest commit are mutually exclusive"
            )
        );
        require!(
            self.effective_table_config
                .is_feature_supported(&TableFeature::AdaptiveMetadataPreview),
            KernelError::unsupported(
                "root manifest file commit requires the adaptiveMetadata-preview feature"
            )
        );
        let read_snapshot = self.read_snapshot_opt.clone().ok_or_else(|| {
            KernelError::internal_error("existing-table transaction unexpectedly has no snapshot")
        })?;
        self.manifest_write = Some(ManifestWrite::RootFile(RootManifestFile::new(
            file,
            read_snapshot,
        )));
        Ok(self)
    }

    /// Enables a manifest (content-tree) commit for this transaction, returning the
    /// [`ManifestCommitState`] that hands out leaf writers accepting file changes.
    ///
    /// Mutually exclusive with [`with_root_manifest_file`](Self::with_root_manifest_file), which
    /// commits a caller-supplied root manifest instead of having kernel build the tree. Repeated
    /// calls return the state initialized by the first call. Configure schema changes on the
    /// transaction builder before building the transaction and staging a manifest commit.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not support the `adaptiveMetadata-preview` feature, if a
    /// root manifest file was already staged, or if delta log commits exist after the last manifest
    /// commit (not yet supported).
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[internal_api]
    pub(crate) fn with_manifest_commit(
        &mut self,
        engine: &dyn Engine,
    ) -> KernelResult<&mut ManifestCommitState> {
        match &self.manifest_write {
            Some(ManifestWrite::RootFile(_)) => {
                return Err(KernelError::invalid_transaction_state(
                    "explicit root manifest and manifest commit are mutually exclusive",
                ))
            }
            // Repeated calls reuse the state from the first call.
            Some(ManifestWrite::Commit(_)) => {}
            None => {
                let read_snapshot = self.read_snapshot_opt.clone().ok_or_else(|| {
                    KernelError::internal_error(
                        "existing-table transaction unexpectedly has no snapshot",
                    )
                })?;
                let state = ManifestCommitState::try_new(
                    engine,
                    read_snapshot,
                    self.get_commit_version(),
                    &self.effective_table_config,
                )?;
                self.manifest_write = Some(ManifestWrite::Commit(state));
            }
        }
        match &mut self.manifest_write {
            Some(ManifestWrite::Commit(state)) => Ok(state),
            _ => Err(KernelError::internal_error(
                "manifest commit state missing after initialization",
            )),
        }
    }
}
