//! Commits a caller-supplied root manifest file as the table's content root.

use std::collections::HashMap;

use crate::action_reconciliation::calculate_transaction_expiration_timestamp;
use crate::actions::{CheckpointAction, ContentRoot, DomainMetadata, SetTransaction, Sidecar};
use crate::crc::merge_domain_metadata;
use crate::error::KernelError;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
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
    /// Domain metadata and set transactions are handled separately. When no log commits follow the
    /// existing checkpoint action and this transaction doesn't change one of them, the action's
    /// entries and sidecars are kept as they are. A change keeps the sidecars and edits only the
    /// inline entries, unless it touches an entry stored in a sidecar, in which case the whole set
    /// is written inline. Log commits after the action write the whole set inline.
    pub(super) fn compute_checkpoint_action(
        &self,
        engine: &dyn Engine,
        commit_version: Version,
        table_config: &TableConfiguration,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> KernelResult<CheckpointAction> {
        let snapshot = self.read_snapshot.as_ref();
        // Log commits after the checkpoint action can change any entry, so its entries and sidecars
        // are reused only when it is at the read snapshot's version.
        let read_snapshot_version = version_as_i64(snapshot.version())?;
        let existing = snapshot
            .latest_checkpoint_action(engine)?
            .filter(|c| c.version() == read_snapshot_version);
        let existing = existing.as_deref();

        let expiration_timestamp =
            calculate_transaction_expiration_timestamp(table_config.table_properties())?;
        let (transactions, txn_sidecars) = fold_entries(
            existing.map(|c| (c.transactions.as_slice(), c.txn_sidecars.as_slice())),
            set_transactions,
            |txn| txn.app_id.as_str(),
            || snapshot.get_app_id_versions(engine),
            // Keyed by app id, so extend overwrites with the newest entry per app.
            |transactions| {
                transactions.extend(
                    set_transactions
                        .iter()
                        .cloned()
                        .map(|txn| (txn.app_id.clone(), txn)),
                )
            },
            |txn| !txn.is_expired(expiration_timestamp),
        )?;
        let (domain_metadata, domain_metadata_sidecars) = fold_entries(
            existing.map(|c| {
                (
                    c.domain_metadata.as_slice(),
                    c.domain_metadata_sidecars.as_slice(),
                )
            }),
            dm_changes,
            DomainMetadata::domain,
            || {
                snapshot
                    .reader()
                    .get_domain_metadatas_internal(engine, None)
            },
            // Domains can be tombstoned, so merging applies removals.
            |domain_metadata| {
                merge_domain_metadata(
                    domain_metadata,
                    dm_changes
                        .iter()
                        .cloned()
                        .map(|dm| (dm.domain().to_string(), dm)),
                )
            },
            |_| true,
        )?;

        let version = version_as_i64(commit_version)?;
        let size = i64::try_from(self.file.size)
            .map_err(|_| KernelError::generic("root manifest file size exceeds i64::MAX"))?;
        let content_root = ContentRoot::new(self.file.location.to_string(), size, version);

        let mut checkpoint = CheckpointAction::new(
            version,
            content_root,
            table_config.protocol().clone(),
            table_config.metadata().clone(),
            transactions,
            domain_metadata,
        );
        checkpoint.txn_sidecars = txn_sidecars;
        checkpoint.domain_metadata_sidecars = domain_metadata_sidecars;
        Ok(checkpoint)
    }
}

/// Builds one kind of checkpoint-action state (set transactions or domain metadata) as inline
/// entries plus sidecars, keeping the `existing` checkpoint action's entries and sidecars where
/// `changes` allow it (see [`RootManifestFile::compute_checkpoint_action`]).
///
/// `read_all` returns every active entry of the kind at the read snapshot's version,
/// `apply_changes` merges `changes` into a set of entries, and `is_live` drops expired entries
/// from what is written inline. Entries in kept sidecars are not pruned.
fn fold_entries<T: Clone>(
    existing: Option<(&[T], &[Sidecar])>,
    changes: &[T],
    key: fn(&T) -> &str,
    read_all: impl FnOnce() -> KernelResult<HashMap<String, T>>,
    apply_changes: impl FnOnce(&mut HashMap<String, T>),
    is_live: impl Fn(&T) -> bool,
) -> KernelResult<(Vec<T>, Vec<Sidecar>)> {
    let (mut entries, sidecars) = match existing {
        None => (read_all()?, Vec::new()),
        Some((inline, sidecars)) if changes.is_empty() => {
            let inline = inline.iter().filter(|entry| is_live(entry)).cloned();
            return Ok((inline.collect(), sidecars.to_vec()));
        }
        Some((inline, sidecars)) => {
            let inline: HashMap<_, _> = inline
                .iter()
                .map(|entry| (key(entry).to_string(), entry.clone()))
                .collect();
            if sidecars.is_empty() {
                (inline, Vec::new())
            } else {
                // The existing checkpoint covers the read snapshot, so an active entry that isn't
                // inline lives in a sidecar. Kernel can't rewrite sidecars, so changing one of
                // those entries writes the whole set inline instead.
                let all = read_all()?;
                let touches_sidecar = changes
                    .iter()
                    .map(key)
                    .any(|key| all.contains_key(key) && !inline.contains_key(key));
                if touches_sidecar {
                    (all, Vec::new())
                } else {
                    (inline, sidecars.to_vec())
                }
            }
        }
    };
    apply_changes(&mut entries);
    entries.retain(|_, entry| is_live(entry));
    Ok((entries.into_values().collect(), sidecars))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools as _;
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::actions::{
        LOG_DOMAIN_METADATA_SCHEMA, LOG_TXN_SCHEMA, METADATA_NAME, PROTOCOL_NAME, get_commit_schema,
    };
    use crate::committer::FileSystemCommitter;
    use crate::crc::{Crc, DomainMetadataState, SetTransactionState};
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::schema::schema_ref;
    use crate::snapshot::Snapshot;
    use crate::transaction::create_table::create_table;
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        adaptive_metadata_protocol_and_metadata, domain, domain_metadata_line,
        minimal_checkpoint_action, setup_table, txn_line, write_commit,
        write_commit_after_manifest_commit, write_manifest_commit, write_sidecar,
    };
    use crate::unit_test_utils::{
        MockTableConfigurationBuilder, assert_result_error_with_message, parse_json_batch,
        string_array_to_engine_data,
    };
    use crate::{KernelResult, Result, create_row};

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
    fn compute_checkpoint_action_builds_a_self_contained_action() -> Result<()> {
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
    fn compute_checkpoint_action_allows_replacing_a_checkpoint_that_covers_the_snapshot()
    -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        write_manifest_commit(&engine, &table_root, 1, existing)?;

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

    // === Log commits after the last checkpoint action ===

    fn txn(app_id: &str, version: i64) -> SetTransaction {
        SetTransaction::new(app_id.to_string(), version, None)
    }

    /// Commits a manifest whose checkpoint action holds domain `a` and txn `app-1`, then one log
    /// commit per entry in `log_domains` followed by one per entry in `log_txns`, and computes the
    /// checkpoint action of the next root manifest commit.
    fn checkpoint_action_after(
        log_domains: Vec<DomainMetadata>,
        log_txns: Vec<SetTransaction>,
    ) -> Result<CheckpointAction> {
        let (engine, table_root) = setup_table()?;
        let mut checkpoint = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        checkpoint.domain_metadata = vec![domain("a", "1")];
        checkpoint.transactions = vec![txn("app-1", 5)];
        write_manifest_commit(&engine, &table_root, 1, checkpoint)?;
        let log_commits = log_domains
            .into_iter()
            .map(domain_metadata_line)
            .chain(log_txns.iter().map(|t| txn_line(&t.app_id, t.version)));
        for (version, line) in (2..).zip(log_commits) {
            let data = parse_json_batch(vec![line.as_str()].into());
            write_commit_after_manifest_commit(&engine, &table_root, version, 1, data)?;
        }
        compute_at_latest(&engine, &table_root)
    }

    fn compute_at_latest(engine: &SyncEngine, table_root: &url::Url) -> Result<CheckpointAction> {
        let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
        let manifest = root_manifest(table_root, "metadata/root.parquet", 2048, snapshot.clone());
        Ok(manifest.compute_checkpoint_action(
            engine,
            snapshot.version() + 1,
            snapshot.table_configuration(),
            &[],
            &[],
        )?)
    }

    #[rstest]
    #[case::no_log_commits(vec![], vec![], vec![domain("a", "1")], vec![txn("app-1", 5)])]
    #[case::domain_added(
        vec![domain("b", "1")],
        vec![],
        vec![domain("a", "1"), domain("b", "1")],
        vec![txn("app-1", 5)]
    )]
    #[case::domain_updated(
        vec![domain("a", "2")],
        vec![],
        vec![domain("a", "2")],
        vec![txn("app-1", 5)]
    )]
    #[case::domain_removed(
        vec![DomainMetadata::remove("a".into(), "1".into())],
        vec![],
        vec![],
        vec![txn("app-1", 5)]
    )]
    #[case::domain_removed_then_re_added(
        vec![DomainMetadata::remove("a".into(), "1".into()), domain("a", "3")],
        vec![],
        vec![domain("a", "3")],
        vec![txn("app-1", 5)]
    )]
    #[case::txn_added(
        vec![],
        vec![txn("app-2", 1)],
        vec![domain("a", "1")],
        vec![txn("app-1", 5), txn("app-2", 1)]
    )]
    #[case::txn_updated(
        vec![],
        vec![txn("app-1", 7)],
        vec![domain("a", "1")],
        vec![txn("app-1", 7)]
    )]
    #[case::domain_and_txn_mixed(
        vec![domain("a", "2"), domain("b", "1")],
        vec![txn("app-1", 7), txn("app-2", 1)],
        vec![domain("a", "2"), domain("b", "1")],
        vec![txn("app-1", 7), txn("app-2", 1)]
    )]
    fn compute_checkpoint_action_applies_log_commits(
        #[case] log_domains: Vec<DomainMetadata>,
        #[case] log_txns: Vec<SetTransaction>,
        #[case] expected_domain_metadata: Vec<DomainMetadata>,
        #[case] expected_txns: Vec<SetTransaction>,
    ) -> Result<()> {
        let mut checkpoint = checkpoint_action_after(log_domains, log_txns)?;
        checkpoint
            .domain_metadata
            .sort_by(|a, b| a.domain().cmp(b.domain()));
        checkpoint
            .transactions
            .sort_by(|a, b| a.app_id.cmp(&b.app_id));
        assert_eq!(checkpoint.domain_metadata, expected_domain_metadata);
        assert_eq!(checkpoint.transactions, expected_txns);
        Ok(())
    }

    #[rstest]
    #[case::classic_before_checkpoint_action(1, None)]
    #[case::standalone_after_checkpoint_action(
        3,
        Some("lastManifestCommit names manifest commit version 2")
    )]
    fn compute_checkpoint_action_with_classic_checkpoint(
        #[case] classic_version: Version,
        #[case] expected_error: Option<&str>,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let txn = |version| parse_json_batch(vec![txn_line("app", version).as_str()].into());
        let checkpoint = minimal_checkpoint_action("metadata/root-v2.parquet", 2)?;
        write_commit(&engine, &table_root, 1, txn(1))?;
        if classic_version == 1 {
            Snapshot::builder_for(table_root.clone())
                .build(&engine)?
                .checkpoint(&engine, None)?;
        }
        write_manifest_commit(&engine, &table_root, 2, checkpoint.clone())?;
        write_commit_after_manifest_commit(&engine, &table_root, 3, 2, txn(3))?;
        if classic_version == 3 {
            // Hand-written since kernel can't checkpoint AMT tables yet (#3683). P&M are in one row
            // because with a row per action, the parquet write fails on null non-nullable fields.
            let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
            let row = json!({
                "protocol": snapshot.table_configuration().protocol(),
                "metaData": snapshot.table_configuration().metadata(),
            });
            let data = engine.json_handler().parse_json(
                string_array_to_engine_data(vec![row.to_string().as_str()].into()),
                get_commit_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?,
            )?;
            let location = table_root
                .join("_delta_log/00000000000000000003.checkpoint.parquet")
                .map_err(KernelError::from)?;
            engine
                .parquet_handler()
                .write_parquet_file(location, Box::new(std::iter::once(Ok(data))))?;
        }
        write_commit_after_manifest_commit(&engine, &table_root, 4, 2, txn(4))?;

        let result = compute_at_latest(&engine, &table_root);
        match expected_error {
            Some(message) => assert_result_error_with_message(result, message),
            None => {
                result?;
            }
        }
        Ok(())
    }

    // Expired txns are pruned whether they come from commits or sit inline in an unchanged
    // checkpoint action.
    #[rstest]
    fn compute_checkpoint_action_prunes_expired_transactions(
        #[values(false, true)] in_checkpoint: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let expired = SetTransaction::new("expired-app".to_string(), 5, Some(0));
        let live = SetTransaction::new("live-app".to_string(), 5, None);
        if in_checkpoint {
            let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
            let existing = CheckpointAction::new(
                1,
                ContentRoot::new("metadata/root-v1.parquet".to_string(), 1024, 1),
                protocol,
                metadata,
                vec![expired, live.clone()],
                vec![],
            );
            write_manifest_commit(&engine, &table_root, 1, existing)?;
        } else {
            for (version, txn) in [(1, expired), (2, live.clone())] {
                let row = create_row(&engine, LOG_TXN_SCHEMA.clone(), txn)?;
                write_commit(&engine, &table_root, version, row)?;
            }
        }

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let commit_version = snapshot.version() + 1;
        let manifest = root_manifest(&table_root, "metadata/root-v3.parquet", 1024, snapshot);
        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([(
                "delta.setTransactionRetentionDuration",
                "interval 60 seconds",
            )])
            .build();

        let checkpoint =
            manifest.compute_checkpoint_action(&engine, commit_version, &table_config, &[], &[])?;
        assert_eq!(checkpoint.transactions, [live]);
        Ok(())
    }

    #[test]
    fn compute_checkpoint_action_new_change_wins() -> Result<()> {
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
    fn new_preserves_an_absolute_file_location() -> Result<()> {
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

    // A checkpoint holds complete state, so top-level entries from commits before it are ignored.
    #[test]
    fn compute_checkpoint_action_ignores_top_level_entries_before_checkpoint() -> Result<()> {
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
        let checkpoint_transaction = SetTransaction::new("ckpt-app".to_string(), 1, None);
        let checkpoint_domain = DomainMetadata::new("ckpt.domain".to_string(), "{}".to_string());
        let existing = CheckpointAction::new(
            3,
            ContentRoot::new("metadata/root-v3.parquet".to_string(), 1024, 3),
            protocol,
            metadata,
            vec![checkpoint_transaction.clone()],
            vec![checkpoint_domain.clone()],
        );
        write_manifest_commit(&engine, &table_root, 3, existing)?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let manifest = root_manifest(
            &table_root,
            "metadata/root-v4.parquet",
            1024,
            snapshot.clone(),
        );
        let checkpoint = manifest.compute_checkpoint_action(
            &engine,
            4,
            snapshot.table_configuration(),
            &[],
            &[],
        )?;

        assert_eq!(checkpoint.domain_metadata, [checkpoint_domain]);
        assert_eq!(checkpoint.transactions, [checkpoint_transaction]);
        Ok(())
    }

    // A domain and txn active in top-level commits, then dropped by omission in a later checkpoint
    // (as an external writer would, with no tombstone), must not come back in the rebuilt
    // checkpoint from those older log entries.
    #[test]
    fn compute_checkpoint_action_does_not_resurrect_entries_the_checkpoint_dropped() -> Result<()> {
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

    // Sidecars hold the `kept` and `spilled` app ids and domains. A change to `spilled` can't edit
    // its sidecar, and a log commit after the manifest commit makes the sidecars stale, so both
    // move the whole set inline; any other change keeps the sidecars.
    #[rstest]
    #[case::unchanged(None, None, &[], true)]
    #[case::new_entry(Some("new"), None, &["new"], true)]
    #[case::sidecar_entry(Some("spilled"), None, &["kept", "spilled"], false)]
    #[case::log_commit(None, Some("logged"), &["kept", "logged", "spilled"], false)]
    fn compute_checkpoint_action_keeps_sidecars_unless_a_change_touches_them(
        #[case] changed: Option<&str>,
        #[case] logged: Option<&str>,
        #[case] inline_keys: &[&str],
        #[case] sidecars_kept: bool,
    ) -> Result<()> {
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
        let sidecar_keys = ["kept", "spilled"];
        existing.txn_sidecars = vec![write_sidecar(
            &engine,
            &table_root,
            "txn.parquet",
            LOG_TXN_SCHEMA.clone(),
            sidecar_keys
                .map(|key| SetTransaction::new(key.to_string(), 1, None).into())
                .into(),
        )];
        existing.domain_metadata_sidecars = vec![write_sidecar(
            &engine,
            &table_root,
            "domains.parquet",
            LOG_DOMAIN_METADATA_SCHEMA.clone(),
            sidecar_keys
                .map(|key| DomainMetadata::new(key.to_string(), "{}".to_string()).into())
                .into(),
        )];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
        if let Some(key) = logged {
            let lines = [domain_metadata_line(domain(key, "{}")), txn_line(key, 1)];
            for (version, line) in (2..).zip(lines) {
                let data = parse_json_batch(vec![line.as_str()].into());
                write_commit_after_manifest_commit(&engine, &table_root, version, 1, data)?;
            }
        }

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let manifest = root_manifest(
            &table_root,
            "metadata/root-v2.parquet",
            2048,
            snapshot.clone(),
        );
        let set_transactions: Vec<_> = changed
            .map(|key| SetTransaction::new(key.to_string(), 2, None))
            .into_iter()
            .collect();
        let dm_changes: Vec<_> = changed
            .map(|key| DomainMetadata::new(key.to_string(), r#"{"changed":true}"#.to_string()))
            .into_iter()
            .collect();
        let checkpoint = manifest.compute_checkpoint_action(
            &engine,
            snapshot.version() + 1,
            snapshot.table_configuration(),
            &dm_changes,
            &set_transactions,
        )?;

        let app_ids = checkpoint.transactions.iter().map(|t| t.app_id.as_str());
        assert_eq!(app_ids.sorted().collect_vec(), inline_keys);
        let domains = checkpoint.domain_metadata.iter().map(|d| d.domain());
        assert_eq!(domains.sorted().collect_vec(), inline_keys);
        assert!(
            set_transactions
                .iter()
                .all(|t| checkpoint.transactions.contains(t))
        );
        assert!(
            dm_changes
                .iter()
                .all(|d| checkpoint.domain_metadata.contains(d))
        );
        let (txn_sidecars, domain_metadata_sidecars) = if sidecars_kept {
            (existing.txn_sidecars, existing.domain_metadata_sidecars)
        } else {
            (vec![], vec![])
        };
        assert_eq!(checkpoint.txn_sidecars, txn_sidecars);
        assert_eq!(
            checkpoint.domain_metadata_sidecars,
            domain_metadata_sidecars
        );
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
    ) -> Result<()> {
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

    // With no checkpoint action to keep, the entries come from the snapshot, through a complete CRC
    // or by scanning past a partial one.
    #[rstest]
    fn compute_checkpoint_action_reads_entries_without_a_checkpoint(
        #[values(true, false)] crc_complete: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let write = |version, data| write_commit(&engine, &table_root, version, data);

        let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
        write(
            1,
            create_row(
                &engine,
                LOG_DOMAIN_METADATA_SCHEMA.clone(),
                domain_metadata.clone(),
            )?,
        )?;
        let transaction = SetTransaction::new("app-1".to_string(), 5, None);
        write(
            2,
            create_row(&engine, LOG_TXN_SCHEMA.clone(), transaction.clone())?,
        )?;

        let built = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let snapshot = if crc_complete {
            built.write_checksum(&engine)?.1
        } else {
            let crc = Arc::new(Crc {
                version: built.version(),
                set_transaction_state: SetTransactionState::Partial(Default::default()),
                domain_metadata_state: DomainMetadataState::Partial(Default::default()),
                ..Default::default()
            });
            Arc::new(Snapshot::new_with_crc(
                built.log_segment().clone(),
                built.table_configuration().clone(),
                Some(crc),
                true,
                false,
            )?)
        };
        assert!(snapshot.crc_at_version().is_some());

        let manifest = root_manifest(
            &table_root,
            "metadata/root-v3.parquet",
            1024,
            snapshot.clone(),
        );
        let checkpoint = manifest.compute_checkpoint_action(
            &engine,
            3,
            snapshot.table_configuration(),
            &[],
            &[],
        )?;

        assert_eq!(checkpoint.domain_metadata, [domain_metadata]);
        assert_eq!(checkpoint.transactions, [transaction]);
        Ok(())
    }
}
