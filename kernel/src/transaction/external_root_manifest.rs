//! Commits a caller-supplied root manifest as the table's content root.

use crate::actions::{
    CheckpointAction, ContentRoot, DomainMetadata, Metadata, Protocol, SetTransaction,
};
use crate::error::Error;
use crate::snapshot::Snapshot;
use crate::table_features::TableFeature;
use crate::utils::require;
use crate::{version_as_i64, DeltaResult, FileMeta, Version};

/// A caller-supplied root manifest committed as the table's content root.
pub(super) struct ExternalRootManifest {
    pub(super) file: FileMeta,
}

impl ExternalRootManifest {
    /// Validates `file` and constructs an `ExternalRootManifest`.
    pub(super) fn new(file: FileMeta, read_snapshot: &Snapshot) -> DeltaResult<Self> {
        let table_config = read_snapshot.table_configuration();
        require!(
            table_config.is_feature_supported(&TableFeature::AdaptiveMetadataPreview),
            Error::generic(
                "external root manifest commit requires the adaptiveMetadata-preview feature"
            )
        );

        let table_root = read_snapshot.table_root();
        require!(
            file.location.scheme() == table_root.scheme()
                && file.location.host_str() == table_root.host_str()
                && file.location.path().starts_with(table_root.path()),
            Error::generic(format!(
                "manifest location {} is not under the table root {table_root}",
                file.location
            ))
        );

        Ok(ExternalRootManifest { file })
    }

    /// Builds the checkpoint action referencing this commit's file at `commit_version`, carrying
    /// `protocol`, `metadata`, `domain_metadata`, and `transactions`.
    ///
    /// Errors if `existing_checkpoint_action` doesn't cover `read_snapshot_version`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn checkpoint_action(
        &self,
        existing_checkpoint_action: Option<&CheckpointAction>,
        read_snapshot_version: Version,
        commit_version: Version,
        protocol: Protocol,
        metadata: Metadata,
        domain_metadata: Vec<DomainMetadata>,
        transactions: Vec<SetTransaction>,
    ) -> DeltaResult<CheckpointAction> {
        if let Some(existing) = existing_checkpoint_action {
            let read_snapshot_version = version_as_i64(read_snapshot_version)?;
            require!(
                existing.version() >= read_snapshot_version,
                Error::generic(format!(
                    "external root manifest commit requires no delta log commits pending replay \
                     since the last checkpoint; existing checkpoint covers version {} but \
                     snapshot is at {read_snapshot_version}",
                    existing.version()
                ))
            );
        }

        let version = version_as_i64(commit_version)?;
        let size_in_bytes = i64::try_from(self.file.size).map_err(|_| {
            Error::generic(format!(
                "manifest size {} does not fit an i64",
                self.file.size
            ))
        })?;
        let content_root = ContentRoot::new(self.file.location.to_string(), size_in_bytes, version);

        Ok(CheckpointAction {
            version,
            content_root,
            protocol,
            metadata,
            transactions,
            domain_metadata,
            txn_sidecars: vec![],
            domain_metadata_sidecars: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use super::*;
    use crate::actions::{LOG_CHECKPOINT_SCHEMA, LOG_DOMAIN_METADATA_SCHEMA, LOG_TXN_SCHEMA};
    use crate::committer::FileSystemCommitter;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::FilteredEngineData;
    use crate::object_store::memory::InMemory;
    use crate::path::LogRoot;
    use crate::schema::schema_ref;
    use crate::snapshot::Snapshot;
    use crate::transaction::create_table::create_table;
    use crate::unit_test_utils::{
        assert_result_error_with_message, MockProtocolBuilder, MockTableConfigurationBuilder,
    };
    use crate::{Engine, IntoEngineData};

    fn mock_protocol_and_metadata() -> (Protocol, Metadata) {
        let table_config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::AdaptiveMetadataPreview])
                    .build(),
            )
            .build();
        (
            table_config.protocol().clone(),
            table_config.metadata().clone(),
        )
    }

    fn mock_checkpoint_action(path: &str, version: Version) -> DeltaResult<CheckpointAction> {
        let (protocol, metadata) = mock_protocol_and_metadata();
        let version = version_as_i64(version)?;
        Ok(CheckpointAction {
            version,
            content_root: ContentRoot::new(path.to_string(), 1024, version),
            protocol,
            metadata,
            transactions: vec![],
            domain_metadata: vec![],
            txn_sidecars: vec![],
            domain_metadata_sidecars: vec![],
        })
    }

    #[test]
    fn checkpoint_action_builds_the_expected_action() -> DeltaResult<()> {
        let (protocol, metadata) = mock_protocol_and_metadata();
        let file = FileMeta {
            location: url::Url::parse("memory:///table/metadata/root-v1.parquet")?,
            last_modified: 0,
            size: 1024,
        };
        let commit = ExternalRootManifest { file: file.clone() };

        let domain_metadata = vec![DomainMetadata::new(
            "delta.rowTracking".to_string(),
            "{}".to_string(),
        )];
        let transactions = vec![SetTransaction::new("app-1".to_string(), 1, Some(0))];

        let checkpoint = commit.checkpoint_action(
            None,
            0,
            1,
            protocol.clone(),
            metadata.clone(),
            domain_metadata.clone(),
            transactions.clone(),
        )?;

        assert_eq!(checkpoint.version(), 1);
        assert_eq!(checkpoint.path(), file.location.as_str());
        assert_eq!(checkpoint.protocol(), &protocol);
        assert_eq!(checkpoint.metadata(), &metadata);
        assert_eq!(checkpoint.domain_metadata, domain_metadata);
        assert_eq!(checkpoint.transactions, transactions);
        Ok(())
    }

    #[test]
    fn checkpoint_action_allows_replacing_a_checkpoint_that_covers_the_snapshot() -> DeltaResult<()>
    {
        let (protocol, metadata) = mock_protocol_and_metadata();
        let existing = mock_checkpoint_action("metadata/root-v1.parquet", 1)?;
        let file = FileMeta {
            location: url::Url::parse("memory:///table/metadata/root-v2.parquet")?,
            last_modified: 0,
            size: 2048,
        };
        let commit = ExternalRootManifest { file: file.clone() };

        // The existing checkpoint covers version 1 and the snapshot is at version 1, so there
        // are no delta log commits pending replay since it.
        let checkpoint =
            commit.checkpoint_action(Some(&existing), 1, 2, protocol, metadata, vec![], vec![])?;
        assert_eq!(checkpoint.path(), file.location.as_str());
        Ok(())
    }

    #[test]
    fn checkpoint_action_rejects_a_stale_checkpoint() -> DeltaResult<()> {
        let (protocol, metadata) = mock_protocol_and_metadata();
        let existing = mock_checkpoint_action("metadata/root-v1.parquet", 1)?;
        let file = FileMeta {
            location: url::Url::parse("memory:///table/metadata/root-v3.parquet")?,
            last_modified: 0,
            size: 4096,
        };
        let commit = ExternalRootManifest { file };

        // The existing checkpoint covers version 1 but the snapshot is at version 2, so a delta
        // log commit at version 2 is pending replay.
        let result =
            commit.checkpoint_action(Some(&existing), 2, 3, protocol, metadata, vec![], vec![]);
        assert_result_error_with_message(result, "commits pending replay");
        Ok(())
    }

    /// Writes `data` as a single-action commit at `version`.
    fn write_commit(
        engine: &SyncEngine,
        table_root: &url::Url,
        version: Version,
        data: Box<dyn crate::EngineData>,
    ) -> DeltaResult<()> {
        let filtered = FilteredEngineData::with_all_rows_selected(data);
        let commit_path = LogRoot::new(table_root.clone())?.new_commit_path(version)?;
        engine.json_handler().write_json_file(
            &commit_path.location,
            Box::new(iter::once(Ok(filtered))),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn scan_non_content_metadata_combines_domain_metadata_transactions_and_checkpoint(
    ) -> DeltaResult<()> {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let schema = schema_ref! { nullable "id": INTEGER };
        let _ = create_table("memory:///", schema, "test")
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?;
        let table_root = Snapshot::builder_for("memory:///")
            .build(&engine)?
            .table_root()
            .clone();

        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write_commit(
            &engine,
            &table_root,
            1,
            domain_metadata.into_engine_data(LOG_DOMAIN_METADATA_SCHEMA.clone(), &engine)?,
        )?;

        let transaction = SetTransaction::new("app-1".to_string(), 5, None);
        write_commit(
            &engine,
            &table_root,
            2,
            transaction.into_engine_data(LOG_TXN_SCHEMA.clone(), &engine)?,
        )?;

        let checkpoint = mock_checkpoint_action("metadata/root-v3.parquet", 3)?;
        write_commit(
            &engine,
            &table_root,
            3,
            checkpoint.into_engine_data(LOG_CHECKPOINT_SCHEMA.clone(), &engine)?,
        )?;

        let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
        let (domain_metadata, transactions, existing_checkpoint) =
            snapshot.scan_non_content_metadata(&engine)?;

        assert_eq!(domain_metadata.len(), 1);
        assert_eq!(domain_metadata[0].domain(), "test.domain");
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].app_id, "app-1");
        assert_eq!(existing_checkpoint.map(|c| c.version()), Some(3));
        Ok(())
    }
}
