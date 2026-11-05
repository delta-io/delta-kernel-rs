//! Commits a caller-supplied root manifest file as the table's content root.

use std::sync::{Arc, LazyLock};

use crate::action_reconciliation::calculate_transaction_expiration_timestamp;
use crate::actions::visitors::{AddVisitor, CdcVisitor, RemoveVisitor, SetTransactionMap};
use crate::actions::{
    ADD_FIELD, ADD_NAME, CDC_FIELD, CDC_NAME, CheckpointAction, ContentRoot, DomainMetadata,
    REMOVE_FIELD, REMOVE_NAME, SetTransaction,
};
use crate::crc::{DomainMetadataState, SetTransactionState, merge_domain_metadata};
use crate::engine_data::RowVisitor as _;
use crate::error::KernelError;
use crate::log_segment::DomainMetadataMap;
use crate::schema::{SchemaRef, lazy_schema_ref};
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::utils::require;
use crate::{Engine, FileMeta, KernelResult, Version, version_as_i64};

/// A pointer to an on-disk root manifest file to be committed as the table's content root via a
/// `checkpoint` action.
pub(super) struct RootManifestFile {
    pub(super) file: FileMeta,
    /// The snapshot being updated, whose active content the checkpoint action folds in.
    pub(super) read_snapshot: SnapshotRef,
}

impl RootManifestFile {
    /// Constructs a `RootManifestFile` for the snapshot being updated.
    pub(super) fn new(file: FileMeta, read_snapshot: SnapshotRef) -> Self {
        RootManifestFile {
            file,
            read_snapshot,
        }
    }

    /// Builds the self-contained `checkpoint` action committing this root manifest at
    /// `commit_version`. A reader restores from it without replaying earlier commits, so it carries
    /// the table's protocol and metadata plus every active domain metadata and set transaction
    /// (this transaction's `dm_changes` and `set_transactions` merged in, newest wins).
    ///
    /// Errors if delta log commits with file actions (`add`, `remove`, or `cdc`) are still pending
    /// replay since an existing checkpoint.
    pub(super) fn compute_checkpoint_action(
        &self,
        engine: &dyn Engine,
        commit_version: Version,
        table_config: &TableConfiguration,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> KernelResult<CheckpointAction> {
        let (mut domain_metadata, mut transactions, existing_checkpoint) =
            self.scan_non_content_metadata(engine)?;

        // Domains can be tombstoned, so merging applies removals. The transactions map is keyed by
        // app id, so extend overwrites with the newest entry per app.
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

        if let Some(existing) = &existing_checkpoint {
            let checkpoint_version = Version::try_from(existing.version()).map_err(|_| {
                KernelError::invalid_checkpoint(format!(
                    "checkpoint action has negative version {}",
                    existing.version()
                ))
            })?;
            self.validate_no_file_actions_since_checkpoint(engine, checkpoint_version)?;
        }

        let version = version_as_i64(commit_version)?;
        let size = i64::try_from(self.file.size)
            .map_err(|_| KernelError::generic("root manifest file size exceeds i64::MAX"))?;
        let content_root = ContentRoot::new(self.file.location.to_string(), size, version);

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
    /// action.
    fn scan_non_content_metadata(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<(
        DomainMetadataMap,
        SetTransactionMap,
        Option<Arc<CheckpointAction>>,
    )> {
        let snapshot = self.read_snapshot.as_ref();
        let crc = snapshot.crc_at_version();
        let domain_metadata_complete_in_crc = matches!(
            crc.map(|crc| &crc.domain_metadata_state),
            Some(DomainMetadataState::Complete(_))
        );
        let transactions_complete_in_crc = matches!(
            crc.map(|crc| &crc.set_transaction_state),
            Some(SetTransactionState::Complete(_))
        );

        let checkpoint_action = snapshot.latest_checkpoint_action(engine)?;

        // Reject a checkpoint that spilled txns/domain metadata to sidecars, since sidecars aren't
        // read yet and that state would be lost.
        if let Some(checkpoint) = &checkpoint_action {
            require!(
                checkpoint.txn_sidecars.is_empty()
                    && checkpoint.domain_metadata_sidecars.is_empty(),
                KernelError::generic(
                    "root manifest file commit cannot yet replace a checkpoint that spills txns \
                     or domain metadata to sidecars"
                )
            );
        }

        // TODO(#3642, #3643): this misses txns and domain metadata that only the checkpoint action
        // holds, as after log cleanup, until their replay reads the checkpoint action.
        let read_snapshot_version = version_as_i64(snapshot.version())?;
        let no_log_commits_since_checkpoint = checkpoint_action
            .as_ref()
            .is_some_and(|c| c.version() == read_snapshot_version);

        let domain_metadata = match &checkpoint_action {
            Some(checkpoint)
                if no_log_commits_since_checkpoint && !domain_metadata_complete_in_crc =>
            {
                let mut domain_metadata = DomainMetadataMap::new();
                merge_domain_metadata(
                    &mut domain_metadata,
                    checkpoint
                        .domain_metadata
                        .iter()
                        .cloned()
                        .map(|dm| (dm.domain().to_string(), dm)),
                );
                domain_metadata
            }
            _ => snapshot
                .reader()
                .get_domain_metadatas_internal(engine, None)?,
        };
        let transactions = match &checkpoint_action {
            Some(checkpoint)
                if no_log_commits_since_checkpoint && !transactions_complete_in_crc =>
            {
                checkpoint
                    .transactions
                    .iter()
                    .cloned()
                    .map(|txn| (txn.app_id.clone(), txn))
                    .collect()
            }
            _ => snapshot.get_app_id_versions(engine)?,
        };

        Ok((domain_metadata, transactions, checkpoint_action))
    }

    /// Errors if a log commit after `checkpoint_version` has a file action.
    fn validate_no_file_actions_since_checkpoint(
        &self,
        engine: &dyn Engine,
        checkpoint_version: Version,
    ) -> KernelResult<()> {
        let log_segment = self.read_snapshot.log_segment();
        // A classic checkpoint after the checkpoint action drops earlier commits from the segment.
        if let Some(classic) = log_segment.checkpoint_version {
            require!(
                classic <= checkpoint_version,
                KernelError::unsupported(format!(
                    "root manifest file commit after the adaptiveMetadata standalone checkpoint at \
                     version {classic} is not yet supported"
                ))
            );
        }
        let commits = log_segment
            .listed
            .ascending_commit_files
            .iter()
            .filter(|commit| commit.version > checkpoint_version);
        for commit in commits {
            let batches = engine.json_handler().read_json_files(
                std::slice::from_ref(&commit.location),
                LOG_FILE_ACTIONS_SCHEMA.clone(),
                None,
            )?;
            let mut adds = AddVisitor::default();
            let mut removes = RemoveVisitor::default();
            let mut cdcs = CdcVisitor::default();
            for batch in batches {
                let batch = batch?;
                adds.visit_rows_of(batch.as_ref())?;
                removes.visit_rows_of(batch.as_ref())?;
                cdcs.visit_rows_of(batch.as_ref())?;
            }
            for (action, found) in [
                (ADD_NAME, !adds.adds.is_empty()),
                (REMOVE_NAME, !removes.removes.is_empty()),
                (CDC_NAME, !cdcs.cdcs.is_empty()),
            ] {
                require!(
                    !found,
                    KernelError::unsupported(format!(
                        "root manifest file commit cannot follow log commit {} with '{action}' \
                         actions, since kernel cannot verify the root manifest includes them",
                        commit.version
                    ))
                );
            }
        }
        Ok(())
    }
}

static LOG_FILE_ACTIONS_SCHEMA: LazyLock<SchemaRef> =
    lazy_schema_ref! { (&ADD_FIELD), (&REMOVE_FIELD), (&CDC_FIELD) };

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools as _;
    use rstest::rstest;
    use serde_json::json;
    use test_utils::{TestAction, actions_to_string};

    use super::*;
    use crate::actions::{LOG_DOMAIN_METADATA_SCHEMA, LOG_TXN_SCHEMA, Sidecar};
    use crate::committer::FileSystemCommitter;
    use crate::crc::{Crc, DomainMetadataState, SetTransactionState};
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::schema::schema_ref;
    use crate::snapshot::Snapshot;
    use crate::transaction::create_table::create_table;
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        adaptive_metadata_protocol_and_metadata, minimal_checkpoint_action, setup_table,
        write_commit, write_commit_after_manifest_commit, write_manifest_commit,
    };
    use crate::unit_test_utils::{
        MockTableConfigurationBuilder, assert_result_error_with_message, parse_json_batch,
    };
    use crate::{KernelResult, create_row};

    fn manifest_file(location: &str, size: u64) -> KernelResult<FileMeta> {
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
    fn compute_checkpoint_action_builds_a_self_contained_action() -> KernelResult<()> {
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

    // === Log commits after the last checkpoint action ===

    /// Computes the checkpoint action for a root manifest commit on a table whose manifest commit
    /// at v2 has domain `a` = `1` and txn `app-1` = `5` in its checkpoint action, followed by
    /// one log commit per entry of `log_commits`. With `set_in_log`, v1 also sets them as top-level
    /// actions; otherwise the checkpoint action is their only copy, as after log cleanup.
    fn compute_after_log_commits(
        set_in_log: bool,
        log_commits: &[String],
    ) -> KernelResult<CheckpointAction> {
        let (engine, table_root) = setup_table()?;
        let base_domain = domain("a", "1");
        let base_txn = SetTransaction::new("app-1".to_string(), 5, None);
        let v1 = if set_in_log {
            vec![domain_line(base_domain.clone()), txn_line("app-1", 5)]
        } else {
            vec![json!({ "commitInfo": { "timestamp": 1 } }).to_string()]
        };
        let v1: Vec<_> = v1.iter().map(String::as_str).collect();
        write_commit(&engine, &table_root, 1, parse_json_batch(v1.into()))?;
        let mut checkpoint = minimal_checkpoint_action("metadata/root-v2.parquet", 2)?;
        checkpoint.domain_metadata = vec![base_domain];
        checkpoint.transactions = vec![base_txn];
        write_manifest_commit(&engine, &table_root, 2, checkpoint)?;
        for (version, action) in (3..).zip(log_commits) {
            let data = parse_json_batch(vec![action.as_str()].into());
            write_commit_after_manifest_commit(&engine, &table_root, version, 2, data)?;
        }
        compute_at_latest(&engine, &table_root)
    }

    fn compute_at_latest(
        engine: &SyncEngine,
        table_root: &url::Url,
    ) -> KernelResult<CheckpointAction> {
        let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
        let manifest = root_manifest(table_root, "metadata/root.parquet", 2048, snapshot.clone());
        manifest.compute_checkpoint_action(
            engine,
            snapshot.version() + 1,
            snapshot.table_configuration(),
            &[],
            &[],
        )
    }

    fn domain(name: &str, configuration: &str) -> DomainMetadata {
        DomainMetadata::new(name.to_string(), configuration.to_string())
    }

    fn domain_line(dm: DomainMetadata) -> String {
        json!({ "domainMetadata": dm }).to_string()
    }

    fn txn_line(app_id: &str, version: i64) -> String {
        json!({ "txn": SetTransaction::new(app_id.to_string(), version, None) }).to_string()
    }

    #[rstest]
    #[case::no_log_commits(vec![], vec![("a", "1")])]
    #[case::adds_domain(vec![domain_line(domain("b", "1"))], vec![("a", "1"), ("b", "1")])]
    #[case::updates_domain(vec![domain_line(domain("a", "2"))], vec![("a", "2")])]
    #[case::removes_domain(
        vec![domain_line(DomainMetadata::remove("a".into(), "1".into()))],
        vec![]
    )]
    #[case::removed_then_re_added(
        vec![
            domain_line(DomainMetadata::remove("a".into(), "1".into())),
            domain_line(domain("a", "3")),
        ],
        vec![("a", "3")]
    )]
    fn compute_checkpoint_action_applies_log_commit_domains_to_checkpoint(
        #[case] log_commits: Vec<String>,
        #[case] expected: Vec<(&str, &str)>,
    ) -> KernelResult<()> {
        let checkpoint = compute_after_log_commits(true, &log_commits)?;
        let domains: Vec<_> = checkpoint
            .domain_metadata
            .iter()
            .map(|dm| (dm.domain(), dm.configuration()))
            .sorted()
            .collect();
        assert_eq!(domains, expected);
        Ok(())
    }

    #[rstest]
    #[case::no_log_commits(vec![], vec![("app-1", 5)])]
    #[case::adds_app(vec![txn_line("app-2", 1)], vec![("app-1", 5), ("app-2", 1)])]
    #[case::updates_app(vec![txn_line("app-1", 7)], vec![("app-1", 7)])]
    fn compute_checkpoint_action_applies_log_commit_txns_to_checkpoint(
        #[case] log_commits: Vec<String>,
        #[case] expected: Vec<(&str, i64)>,
    ) -> KernelResult<()> {
        let checkpoint = compute_after_log_commits(true, &log_commits)?;
        let txns: Vec<_> = checkpoint
            .transactions
            .iter()
            .map(|txn| (txn.app_id.as_str(), txn.version))
            .sorted()
            .collect();
        assert_eq!(txns, expected);
        Ok(())
    }

    #[rstest]
    #[case::add(vec![actions_to_string(vec![TestAction::Add("a".into())])], "add")]
    #[case::remove(vec![actions_to_string(vec![TestAction::Remove("a".into())])], "remove")]
    #[case::cdc(
        vec![r#"{"cdc":{"path":"a","partitionValues":{},"size":1,"dataChange":false}}"#.into()],
        "cdc"
    )]
    #[case::add_followed_by_non_file_commit(
        vec![
            actions_to_string(vec![TestAction::Add("a".into())]),
            domain_line(domain("a", "1")),
        ],
        "add"
    )]
    fn compute_checkpoint_action_rejects_file_actions_after_checkpoint(
        #[case] log_commits: Vec<String>,
        #[case] action: &str,
    ) {
        assert_result_error_with_message(
            compute_after_log_commits(true, &log_commits),
            &format!("cannot follow log commit 3 with '{action}' actions"),
        );
    }

    #[test]
    #[ignore = "needs txn and domain metadata replay from checkpoint actions (#3642, #3643)"]
    fn compute_checkpoint_action_keeps_checkpoint_only_entries_after_log_commit() -> KernelResult<()>
    {
        let checkpoint = compute_after_log_commits(false, &[txn_line("app-2", 1)])?;
        assert_eq!(
            checkpoint
                .domain_metadata
                .iter()
                .map(|dm| (dm.domain(), dm.configuration()))
                .collect_vec(),
            [("a", "1")]
        );
        let txns = checkpoint
            .transactions
            .iter()
            .map(|txn| (txn.app_id.as_str(), txn.version))
            .sorted()
            .collect_vec();
        assert_eq!(txns, [("app-1", 5), ("app-2", 1)]);
        Ok(())
    }

    #[test]
    fn compute_checkpoint_action_ignores_file_actions_before_checkpoint() -> KernelResult<()> {
        let (engine, table_root) = setup_table()?;
        let add = actions_to_string(vec![TestAction::Add("a".into())]);
        write_commit(
            &engine,
            &table_root,
            1,
            parse_json_batch(vec![add.as_str()].into()),
        )?;
        let checkpoint = minimal_checkpoint_action("metadata/root-v2.parquet", 2)?;
        write_manifest_commit(&engine, &table_root, 2, checkpoint)?;
        compute_at_latest(&engine, &table_root)?;
        Ok(())
    }

    #[test]
    fn compute_checkpoint_action_prunes_expired_transactions() -> KernelResult<()> {
        let (engine, table_root) = setup_table()?;
        let expired = SetTransaction::new("app-1".to_string(), 5, Some(0));
        write_commit(
            &engine,
            &table_root,
            1,
            create_row(&engine, LOG_TXN_SCHEMA.clone(), expired)?,
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
    fn compute_checkpoint_action_new_change_wins() -> KernelResult<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let old_domain_metadata = DomainMetadata::new("test.domain".to_string(), "old".to_string());
        write(
            1,
            create_row(
                &engine,
                LOG_DOMAIN_METADATA_SCHEMA.clone(),
                old_domain_metadata,
            )?,
        )?;
        let old_transaction = SetTransaction::new("app-1".to_string(), 1, None);
        write(
            2,
            create_row(&engine, LOG_TXN_SCHEMA.clone(), old_transaction)?,
        )?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let manifest = root_manifest(
            &table_root,
            "metadata/root-v3.parquet",
            1024,
            snapshot.clone(),
        );

        let new_domain_metadata = DomainMetadata::new("test.domain".to_string(), "new".to_string());
        let new_transaction = SetTransaction::new("app-1".to_string(), 2, None);
        let checkpoint = manifest.compute_checkpoint_action(
            &engine,
            3,
            snapshot.table_configuration(),
            std::slice::from_ref(&new_domain_metadata),
            std::slice::from_ref(&new_transaction),
        )?;

        assert_eq!(checkpoint.domain_metadata, vec![new_domain_metadata]);
        assert_eq!(checkpoint.transactions, vec![new_transaction]);
        Ok(())
    }

    #[test]
    fn new_preserves_an_absolute_file_location() -> KernelResult<()> {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let schema = schema_ref! { nullable "id": INTEGER };
        let _ = create_table("memory:///t/", schema, "test", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?;
        let snapshot = Snapshot::builder_for("memory:///t/").build(&engine)?;

        let file = manifest_file("s3://bucket/metadata/root-v1.parquet", 1024)?;
        let manifest = RootManifestFile::new(file.clone(), snapshot);
        assert_eq!(manifest.file, file);
        Ok(())
    }

    // A checkpoint holds complete state, so its inline entries win and stale top-level entries
    // from before it are ignored.
    #[test]
    fn scan_non_content_metadata_prefers_checkpoint_inline_over_stale_top_level() -> KernelResult<()>
    {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let stale_domain = DomainMetadata::new("stale.domain".to_string(), "{}".to_string());
        write(
            1,
            create_row(&engine, LOG_DOMAIN_METADATA_SCHEMA.clone(), stale_domain)?,
        )?;
        let stale_transaction = SetTransaction::new("stale-app".to_string(), 5, None);
        write(
            2,
            create_row(&engine, LOG_TXN_SCHEMA.clone(), stale_transaction)?,
        )?;
        let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
        let checkpoint = CheckpointAction::new(
            3,
            ContentRoot::new("metadata/root-v3.parquet".to_string(), 1024, 3),
            protocol,
            metadata,
            vec![SetTransaction::new("ckpt-app".to_string(), 1, None)],
            vec![DomainMetadata::new(
                "ckpt.domain".to_string(),
                "{}".to_string(),
            )],
        );
        write_manifest_commit(&engine, &table_root, 3, checkpoint)?;

        let manifest = root_manifest(
            &table_root,
            "metadata/root-v3.parquet",
            1024,
            Snapshot::builder_for(table_root.clone()).build(&engine)?,
        );
        let (domain_metadata, transactions, existing_checkpoint) =
            manifest.scan_non_content_metadata(&engine)?;

        assert_eq!(domain_metadata.keys().collect::<Vec<_>>(), ["ckpt.domain"]);
        assert_eq!(transactions.keys().collect::<Vec<_>>(), ["ckpt-app"]);
        assert_eq!(existing_checkpoint.map(|c| c.version()), Some(3));
        Ok(())
    }

    // A domain and txn active in top-level commits, then dropped by omission in a later checkpoint
    // (as an external writer would, with no tombstone), must not come back in the rebuilt
    // checkpoint from those older log entries.
    #[test]
    fn compute_checkpoint_action_does_not_resurrect_entries_the_checkpoint_dropped()
    -> KernelResult<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let domain_metadata = DomainMetadata::new("dropped.domain".to_string(), "{}".to_string());
        write(
            1,
            create_row(&engine, LOG_DOMAIN_METADATA_SCHEMA.clone(), domain_metadata)?,
        )?;
        let transaction = SetTransaction::new("dropped-app".to_string(), 5, None);
        write(2, create_row(&engine, LOG_TXN_SCHEMA.clone(), transaction)?)?;
        // Complete checkpoint at the tip that omits both.
        let existing = minimal_checkpoint_action("metadata/root-v3.parquet", 3)?;
        write_manifest_commit(&engine, &table_root, 3, existing)?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        assert_eq!(snapshot.version(), 3);
        let manifest = root_manifest(
            &table_root,
            "metadata/root-v4.parquet",
            2048,
            snapshot.clone(),
        );

        let checkpoint = manifest.compute_checkpoint_action(
            &engine,
            4,
            snapshot.table_configuration(),
            &[],
            &[],
        )?;
        assert!(checkpoint.domain_metadata.is_empty());
        assert!(checkpoint.transactions.is_empty());
        Ok(())
    }

    // The existing checkpoint spilled txns/domain metadata into sidecar files that can't be read
    // yet; replacing it would drop that state, so the commit is refused.
    #[test]
    fn compute_checkpoint_action_rejects_a_checkpoint_that_spills_to_sidecars() -> KernelResult<()>
    {
        let (engine, table_root) = setup_table()?;
        let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
        let mut existing = CheckpointAction::new(
            1,
            ContentRoot::new("metadata/root-v1.parquet".to_string(), 1024, 1),
            protocol,
            metadata,
            vec![],
            vec![],
        );
        let sidecar = || Sidecar {
            path: "sidecar.parquet".to_string(),
            size_in_bytes: 1024,
            modification_time: 0,
            tags: None,
        };
        existing.txn_sidecars = vec![sidecar()];
        existing.domain_metadata_sidecars = vec![sidecar()];
        write_manifest_commit(&engine, &table_root, 1, existing)?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let manifest = root_manifest(
            &table_root,
            "metadata/root-v2.parquet",
            2048,
            snapshot.clone(),
        );

        let result = manifest.compute_checkpoint_action(
            &engine,
            2,
            snapshot.table_configuration(),
            &[],
            &[],
        );
        assert_result_error_with_message(result, "spills txns or domain metadata to sidecars");
        Ok(())
    }

    // An AMT table from v0 whose manifest commit at v1 carries the checkpoint action, followed by
    // domain metadata and a transaction. Every commit records an In-Commit Timestamp and, from v1
    // on, the `lastManifestCommit` that locates the checkpoint action, as an AMT writer does.
    #[test]
    fn scan_non_content_metadata_uses_crc_fast_path_with_existing_checkpoint() -> KernelResult<()> {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let table_root = url::Url::parse("memory:///")?;
        let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
        let commit_info = |ict: i64, last_manifest_commit: Option<i64>| {
            let mut commit_info = serde_json::json!({ "timestamp": ict, "inCommitTimestamp": ict });
            if let Some(version) = last_manifest_commit {
                commit_info["lastManifestCommit"] =
                    serde_json::json!({ "version": version, "contentRootVersion": version });
            }
            serde_json::json!({ "commitInfo": commit_info })
        };
        let commits = [
            vec![
                commit_info(1, None),
                serde_json::json!({ "protocol": protocol }),
                serde_json::json!({ "metaData": metadata }),
            ],
            vec![
                commit_info(2, Some(1)),
                serde_json::json!({ "checkpoint": [
                    { "checkpointMetadata": { "version": 1 } },
                    { "contentRoot": {
                        "path": "metadata/root-v1.parquet", "sizeInBytes": 1024, "version": 1,
                    } },
                    { "protocol": protocol },
                    { "metaData": metadata },
                ] }),
            ],
            vec![
                commit_info(3, Some(1)),
                serde_json::json!({ "domainMetadata": {
                    "domain": "test.domain", "configuration": "{}", "removed": false,
                } }),
            ],
            vec![
                commit_info(4, Some(1)),
                serde_json::json!({ "txn": { "appId": "app-1", "version": 5 } }),
            ],
        ];
        let log_root = table_root.join("_delta_log/")?;
        for (version, actions) in commits.iter().enumerate() {
            let lines = actions.iter().map(|action| action.to_string()).join("\n");
            engine.storage_handler().put(
                &log_root.join(&format!("{version:020}.json"))?,
                lines.into(),
                false,
            )?;
        }

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
    fn scan_non_content_metadata_uses_crc_fast_path() -> KernelResult<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write(
            1,
            create_row(&engine, LOG_DOMAIN_METADATA_SCHEMA.clone(), domain_metadata)?,
        )?;
        let transaction = SetTransaction::new("app-1".to_string(), 5, None);
        write(2, create_row(&engine, LOG_TXN_SCHEMA.clone(), transaction)?)?;

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

    // A domain/txn living only in a prior checkpoint's nested set must survive into the new one; a
    // tombstone in this txn drops the domain.
    #[rstest]
    #[case::kept(vec![], true)]
    #[case::removed(vec![DomainMetadata::remove("test.domain".into(), "{}".into())], false)]
    fn compute_checkpoint_action_folds_prior_checkpoint_nested_set(
        #[case] dm_changes: Vec<DomainMetadata>,
        #[case] domain_kept: bool,
    ) -> KernelResult<()> {
        let (engine, table_root) = setup_table()?;
        let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
        let existing = CheckpointAction::new(
            1,
            ContentRoot::new("metadata/root-v1.parquet".to_string(), 1024, 1),
            protocol,
            metadata,
            vec![SetTransaction::new("app-1".to_string(), 5, None)],
            vec![DomainMetadata::new(
                "test.domain".to_string(),
                "{}".to_string(),
            )],
        );
        write_manifest_commit(&engine, &table_root, 1, existing)?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        assert!(snapshot.crc_at_version().is_none());

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
            &dm_changes,
            &[],
        )?;

        assert_eq!(
            checkpoint
                .domain_metadata
                .iter()
                .any(|d| d.domain() == "test.domain"),
            domain_kept
        );
        assert!(checkpoint.transactions.iter().any(|t| t.app_id == "app-1"));
        Ok(())
    }

    #[test]
    fn scan_non_content_metadata_scans_past_a_partial_crc() -> KernelResult<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write(
            1,
            create_row(&engine, LOG_DOMAIN_METADATA_SCHEMA.clone(), domain_metadata)?,
        )?;
        let transaction = SetTransaction::new("app-1".to_string(), 5, None);
        write(2, create_row(&engine, LOG_TXN_SCHEMA.clone(), transaction)?)?;

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
            false,
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
