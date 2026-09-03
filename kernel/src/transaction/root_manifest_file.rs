//! Commits a caller-supplied root manifest file as the table's content root.

use crate::action_reconciliation::calculate_transaction_expiration_timestamp;
use crate::actions::visitors::{DomainMetadataVisitor, SetTransactionMap, SetTransactionVisitor};
use crate::actions::{
    CheckpointAction, ContentRoot, DomainMetadata, SetTransaction, CHECKPOINT_ACTION_FIELD,
    DOMAIN_METADATA_FIELD, SET_TRANSACTION_FIELD,
};
use crate::crc::{merge_domain_metadata, DomainMetadataState, SetTransactionState};
use crate::error::Error;
use crate::log_segment::DomainMetadataMap;
use crate::schema::StructType;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::utils::require;
use crate::{version_as_i64, DeltaResult, Engine, FileMeta, RowVisitor as _, Version};

/// A pointer to an on-disk root manifest file, produced by some other system, to be committed as
/// the table's content root via a `checkpoint` action.
pub(super) struct RootManifestFile {
    pub(super) file: FileMeta,
    /// The snapshot `file` was validated against, whose active content the checkpoint action folds
    /// in.
    pub(super) read_snapshot: SnapshotRef,
}

impl RootManifestFile {
    /// Constructs a `RootManifestFile`, checking `file` sits under the table root.
    pub(super) fn new(file: FileMeta, read_snapshot: SnapshotRef) -> DeltaResult<Self> {
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
        Ok(RootManifestFile {
            file,
            read_snapshot,
        })
    }

    /// Builds the self-contained `checkpoint` action committing this root manifest at
    /// `commit_version`. A reader restores from it without replaying earlier commits, so it carries
    /// the table's protocol and metadata plus every active domain metadata and set transaction
    /// (this transaction's `dm_changes` and `set_transactions` merged in, newest wins).
    ///
    /// Errors if an existing checkpoint does not already cover the read snapshot's version, meaning
    /// delta log commits are still pending replay since it.
    pub(super) fn compute_checkpoint_action(
        &self,
        engine: &dyn Engine,
        commit_version: Version,
        table_config: &TableConfiguration,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> DeltaResult<CheckpointAction> {
        let (mut domain_metadata, mut transactions, existing_checkpoint) =
            self.scan_non_content_metadata(engine)?;

        merge_domain_metadata(
            &mut domain_metadata,
            dm_changes
                .iter()
                .cloned()
                .map(|dm| (dm.domain().to_string(), dm)),
        );
        transactions.extend(
            set_transactions
                .iter()
                .cloned()
                .map(|txn| (txn.app_id.clone(), txn)),
        );
        let expiration_timestamp =
            calculate_transaction_expiration_timestamp(table_config.table_properties())?;
        transactions.retain(|_, txn| !txn.is_expired(expiration_timestamp));

        let read_snapshot_version = version_as_i64(self.read_snapshot.version())?;
        if let Some(existing) = &existing_checkpoint {
            require!(
                existing.version() >= read_snapshot_version,
                Error::generic(format!(
                    "root manifest file commit requires no delta log commits pending replay since \
                     the last checkpoint; existing checkpoint covers version {} but snapshot is \
                     at {read_snapshot_version}",
                    existing.version()
                ))
            );
        }

        let version = version_as_i64(commit_version)?;
        let content_root = ContentRoot::new(
            self.file.location.to_string(),
            self.file.size as i64,
            version,
        );

        Ok(CheckpointAction::new(
            version,
            content_root,
            table_config.protocol().clone(),
            table_config.metadata().clone(),
            transactions.into_values().collect(),
            domain_metadata.into_values().collect(),
        ))
    }

    /// Returns the read snapshot's active domain metadata, set transactions, and latest checkpoint
    /// action. Domain metadata and set transactions come from a `Complete` CRC when present, else a
    /// log scan; the checkpoint action is always scanned, since the snapshot does not cache it.
    fn scan_non_content_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(
        DomainMetadataMap,
        SetTransactionMap,
        Option<CheckpointAction>,
    )> {
        let snapshot = self.read_snapshot.as_ref();
        let crc = snapshot.crc_at_version();

        let mut fields = vec![CHECKPOINT_ACTION_FIELD.clone()];
        let domain_metadata_from_crc = match crc.map(|crc| &crc.domain_metadata_state) {
            Some(DomainMetadataState::Complete(map)) => Some(map.clone()),
            _ => {
                fields.push(DOMAIN_METADATA_FIELD.clone());
                None
            }
        };
        let transactions_from_crc = match crc.map(|crc| &crc.set_transaction_state) {
            Some(SetTransactionState::Complete(map)) => Some(map.clone()),
            _ => {
                fields.push(SET_TRANSACTION_FIELD.clone());
                None
            }
        };

        let schema = StructType::try_new(fields)?.into();
        let mut checkpoint_action = None;
        let mut domain_metadata_visitor = DomainMetadataVisitor::new(None);
        let mut set_transaction_visitor = SetTransactionVisitor::new(None);
        for batch in snapshot.log_segment().read_actions(engine, schema)? {
            let batch = batch?;
            let data = batch.actions.as_ref();
            if checkpoint_action.is_none() {
                checkpoint_action = CheckpointAction::try_new_from_data(data)?;
            }
            if domain_metadata_from_crc.is_none() {
                domain_metadata_visitor.visit_rows_of(data)?;
            }
            if transactions_from_crc.is_none() {
                set_transaction_visitor.visit_rows_of(data)?;
            }
        }

        let domain_metadata =
            domain_metadata_from_crc.unwrap_or(domain_metadata_visitor.into_domain_metadatas());
        let transactions =
            transactions_from_crc.unwrap_or(set_transaction_visitor.set_transactions);

        Ok((domain_metadata, transactions, checkpoint_action))
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use super::*;
    use crate::actions::{
        Metadata, Protocol, LOG_CHECKPOINT_SCHEMA, LOG_DOMAIN_METADATA_SCHEMA, LOG_TXN_SCHEMA,
    };
    use crate::committer::FileSystemCommitter;
    use crate::crc::{Crc, DomainMetadataState, SetTransactionState};
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::FilteredEngineData;
    use crate::object_store::memory::InMemory;
    use crate::path::LogRoot;
    use crate::schema::schema_ref;
    use crate::snapshot::Snapshot;
    use crate::table_features::TableFeature;
    use crate::transaction::create_table::create_table;
    use crate::unit_test_utils::{
        assert_result_error_with_message, MockProtocolBuilder, MockTableConfigurationBuilder,
    };
    use crate::{Engine, IntoEngineData};

    fn adaptive_metadata_protocol_and_metadata() -> (Protocol, Metadata) {
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

    fn minimal_checkpoint_action(path: &str, version: Version) -> DeltaResult<CheckpointAction> {
        let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
        let version = version_as_i64(version)?;
        Ok(CheckpointAction::new(
            version,
            ContentRoot::new(path.to_string(), 1024, version),
            protocol,
            metadata,
            vec![],
            vec![],
        ))
    }

    fn setup_table() -> DeltaResult<(SyncEngine, url::Url)> {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let schema = schema_ref! { nullable "id": INTEGER };
        let _ = create_table("memory:///", schema, "test")
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?;
        let table_root = Snapshot::builder_for("memory:///")
            .build(&engine)?
            .table_root()
            .clone();
        Ok((engine, table_root))
    }

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

    fn manifest_file(location: &str, size: u64) -> DeltaResult<FileMeta> {
        Ok(FileMeta {
            location: url::Url::parse(location)?,
            last_modified: 0,
            size,
        })
    }

    fn root_manifest(
        table_root: &url::Url,
        name: &str,
        size: u64,
        snapshot: SnapshotRef,
    ) -> RootManifestFile {
        RootManifestFile {
            file: FileMeta {
                location: table_root.join(name).unwrap(),
                last_modified: 0,
                size,
            },
            read_snapshot: snapshot,
        }
    }

    #[test]
    fn compute_checkpoint_action_builds_a_self_contained_action() -> DeltaResult<()> {
        let (engine, table_root) = setup_table()?;
        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let manifest = root_manifest(
            &table_root,
            "metadata/root-v1.parquet",
            1024,
            snapshot.clone(),
        );

        let dm_changes = vec![DomainMetadata::new(
            "my.domain".to_string(),
            "{}".to_string(),
        )];
        let set_transactions = vec![SetTransaction::new("app-1".to_string(), 1, Some(0))];
        let checkpoint = manifest.compute_checkpoint_action(
            &engine,
            1,
            snapshot.table_configuration(),
            &dm_changes,
            &set_transactions,
        )?;

        assert_eq!(checkpoint.version(), 1);
        assert_eq!(checkpoint.path(), manifest.file.location.as_str());
        assert_eq!(
            checkpoint.protocol(),
            snapshot.table_configuration().protocol()
        );
        assert_eq!(
            checkpoint.metadata(),
            snapshot.table_configuration().metadata()
        );
        assert_eq!(checkpoint.domain_metadata, dm_changes);
        assert_eq!(checkpoint.transactions, set_transactions);
        Ok(())
    }

    #[test]
    fn compute_checkpoint_action_allows_replacing_a_checkpoint_that_covers_the_snapshot(
    ) -> DeltaResult<()> {
        let (engine, table_root) = setup_table()?;
        let existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        write_commit(
            &engine,
            &table_root,
            1,
            existing.into_engine_data(LOG_CHECKPOINT_SCHEMA.clone(), &engine)?,
        )?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        assert_eq!(snapshot.version(), 1);
        let manifest = root_manifest(
            &table_root,
            "metadata/root-v2.parquet",
            2048,
            snapshot.clone(),
        );

        let checkpoint = manifest.compute_checkpoint_action(
            &engine,
            2,
            snapshot.table_configuration(),
            &[],
            &[],
        )?;
        assert_eq!(checkpoint.path(), manifest.file.location.as_str());
        Ok(())
    }

    #[test]
    fn compute_checkpoint_action_rejects_a_stale_checkpoint() -> DeltaResult<()> {
        let (engine, table_root) = setup_table()?;
        let existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        write_commit(
            &engine,
            &table_root,
            1,
            existing.into_engine_data(LOG_CHECKPOINT_SCHEMA.clone(), &engine)?,
        )?;
        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write_commit(
            &engine,
            &table_root,
            2,
            domain_metadata.into_engine_data(LOG_DOMAIN_METADATA_SCHEMA.clone(), &engine)?,
        )?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        assert_eq!(snapshot.version(), 2);
        let manifest = root_manifest(
            &table_root,
            "metadata/root-v3.parquet",
            4096,
            snapshot.clone(),
        );

        let result = manifest.compute_checkpoint_action(
            &engine,
            3,
            snapshot.table_configuration(),
            &[],
            &[],
        );
        assert_result_error_with_message(result, "commits pending replay");
        Ok(())
    }

    #[test]
    fn compute_checkpoint_action_prunes_expired_transactions() -> DeltaResult<()> {
        let (engine, table_root) = setup_table()?;
        let expired = SetTransaction::new("app-1".to_string(), 5, Some(0));
        write_commit(
            &engine,
            &table_root,
            1,
            expired.into_engine_data(LOG_TXN_SCHEMA.clone(), &engine)?,
        )?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let manifest = root_manifest(&table_root, "metadata/root-v2.parquet", 1024, snapshot);
        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([(
                "delta.setTransactionRetentionDuration",
                "interval 60 seconds",
            )])
            .build();

        let checkpoint = manifest.compute_checkpoint_action(&engine, 2, &table_config, &[], &[])?;
        assert!(checkpoint.transactions.is_empty());
        Ok(())
    }

    #[test]
    fn new_rejects_a_same_scheme_file_outside_the_table_root() -> DeltaResult<()> {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let schema = schema_ref! { nullable "id": INTEGER };
        let _ = create_table("memory:///t/", schema, "test")
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?;
        let snapshot = Snapshot::builder_for("memory:///t/").build(&engine)?;

        let outside = manifest_file("memory:///elsewhere/root-v1.parquet", 1024)?;
        let result = RootManifestFile::new(outside, snapshot);
        assert_result_error_with_message(result, "not under the table root");
        Ok(())
    }

    #[test]
    fn scan_non_content_metadata_combines_domain_metadata_transactions_and_checkpoint(
    ) -> DeltaResult<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write(
            1,
            domain_metadata.into_engine_data(LOG_DOMAIN_METADATA_SCHEMA.clone(), &engine)?,
        )?;
        let transaction = SetTransaction::new("app-1".to_string(), 5, None);
        write(
            2,
            transaction.into_engine_data(LOG_TXN_SCHEMA.clone(), &engine)?,
        )?;
        let checkpoint = minimal_checkpoint_action("metadata/root-v3.parquet", 3)?;
        write(
            3,
            checkpoint.into_engine_data(LOG_CHECKPOINT_SCHEMA.clone(), &engine)?,
        )?;

        let manifest = root_manifest(
            &table_root,
            "metadata/root-v3.parquet",
            1024,
            Snapshot::builder_for(table_root.clone()).build(&engine)?,
        );
        let (domain_metadata, transactions, existing_checkpoint) =
            manifest.scan_non_content_metadata(&engine)?;

        assert_eq!(domain_metadata.len(), 1);
        assert!(domain_metadata.contains_key("test.domain"));
        assert_eq!(transactions.len(), 1);
        assert!(transactions.contains_key("app-1"));
        assert_eq!(existing_checkpoint.map(|c| c.version()), Some(3));
        Ok(())
    }

    #[test]
    fn scan_non_content_metadata_uses_crc_fast_path_with_existing_checkpoint() -> DeltaResult<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        // `write_checksum` below validates protocol continuity, so this embeds the table's real
        // protocol/metadata rather than `minimal_checkpoint_action`'s synthetic one.
        let table_snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let checkpoint = CheckpointAction::new(
            1,
            ContentRoot::new("metadata/root-v1.parquet".to_string(), 1024, 1),
            table_snapshot.table_configuration().protocol().clone(),
            table_snapshot.table_configuration().metadata().clone(),
            vec![],
            vec![],
        );
        write(
            1,
            checkpoint.into_engine_data(LOG_CHECKPOINT_SCHEMA.clone(), &engine)?,
        )?;
        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write(
            2,
            domain_metadata.into_engine_data(LOG_DOMAIN_METADATA_SCHEMA.clone(), &engine)?,
        )?;
        let transaction = SetTransaction::new("app-1".to_string(), 5, None);
        write(
            3,
            transaction.into_engine_data(LOG_TXN_SCHEMA.clone(), &engine)?,
        )?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let (_, snapshot) = snapshot.write_checksum(&engine)?;
        assert!(snapshot.crc_at_version().is_some());

        let manifest = root_manifest(&table_root, "metadata/root-v1.parquet", 1024, snapshot);
        let (domain_metadata, transactions, existing_checkpoint) =
            manifest.scan_non_content_metadata(&engine)?;

        assert_eq!(domain_metadata.len(), 1);
        assert!(domain_metadata.contains_key("test.domain"));
        assert_eq!(transactions.len(), 1);
        assert!(transactions.contains_key("app-1"));
        assert_eq!(existing_checkpoint.map(|c| c.version()), Some(1));
        Ok(())
    }

    #[test]
    fn scan_non_content_metadata_uses_crc_fast_path() -> DeltaResult<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write(
            1,
            domain_metadata.into_engine_data(LOG_DOMAIN_METADATA_SCHEMA.clone(), &engine)?,
        )?;
        let transaction = SetTransaction::new("app-1".to_string(), 5, None);
        write(
            2,
            transaction.into_engine_data(LOG_TXN_SCHEMA.clone(), &engine)?,
        )?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let (_, snapshot) = snapshot.write_checksum(&engine)?;
        assert!(snapshot.crc_at_version().is_some());

        let manifest = root_manifest(&table_root, "metadata/root-v1.parquet", 1024, snapshot);
        let (domain_metadata, transactions, existing_checkpoint) =
            manifest.scan_non_content_metadata(&engine)?;

        assert_eq!(domain_metadata.len(), 1);
        assert!(domain_metadata.contains_key("test.domain"));
        assert_eq!(transactions.len(), 1);
        assert!(transactions.contains_key("app-1"));
        assert_eq!(existing_checkpoint, None);
        Ok(())
    }

    #[test]
    fn scan_non_content_metadata_scans_past_a_partial_crc() -> DeltaResult<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write(
            1,
            domain_metadata.into_engine_data(LOG_DOMAIN_METADATA_SCHEMA.clone(), &engine)?,
        )?;
        let transaction = SetTransaction::new("app-1".to_string(), 5, None);
        write(
            2,
            transaction.into_engine_data(LOG_TXN_SCHEMA.clone(), &engine)?,
        )?;

        let built = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let crc = Arc::new(Crc {
            version: built.version(),
            set_transaction_state: SetTransactionState::Partial(Default::default()),
            domain_metadata_state: DomainMetadataState::Partial(Default::default()),
            ..Default::default()
        });
        let snapshot = Snapshot::new_with_crc(
            built.log_segment().clone(),
            built.table_configuration().clone(),
            Some(crc),
            true,
        )?;
        assert!(snapshot.crc_at_version().is_some());

        let manifest = root_manifest(
            &table_root,
            "metadata/root-v1.parquet",
            1024,
            Arc::new(snapshot),
        );
        let (domain_metadata, transactions, existing_checkpoint) =
            manifest.scan_non_content_metadata(&engine)?;

        assert_eq!(domain_metadata.len(), 1);
        assert!(domain_metadata.contains_key("test.domain"));
        assert_eq!(transactions.len(), 1);
        assert!(transactions.contains_key("app-1"));
        assert_eq!(existing_checkpoint, None);
        Ok(())
    }
}
