//! Commits a caller-supplied root manifest file as the table's content root.

use std::collections::HashMap;

use crate::action_reconciliation::calculate_transaction_expiration_timestamp;
use crate::actions::{CheckpointAction, ContentRoot, DomainMetadata, SetTransaction, Sidecar};
use crate::content_tree::{
    read_checkpoint_action_domain_metadata, read_checkpoint_action_transactions,
};
use crate::crc::{CrcDelta, merge_domain_metadata};
use crate::error::KernelError;
use crate::path::LogRoot;
use crate::snapshot::{Snapshot, SnapshotRef};
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
    /// (this transaction's `dm_changes` and `set_transactions` merged in, newest wins). Expired set
    /// transactions are dropped from the inline entries but carried forward in a reused sidecar.
    pub(super) fn compute_checkpoint_action(
        &self,
        engine: &dyn Engine,
        commit_version: Version,
        table_config: &TableConfiguration,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> KernelResult<CheckpointAction> {
        let snapshot = self.read_snapshot.as_ref();
        let existing = snapshot.latest_checkpoint_action(engine)?;
        let existing = existing.as_deref();
        let base = existing
            .map(|c| {
                Version::try_from(c.version()).map_err(|_| {
                    KernelError::invalid_checkpoint(format!(
                        "checkpoint action version {} is negative",
                        c.version()
                    ))
                })
            })
            .transpose()?;
        let mut changes = match base {
            Some(base) if base < snapshot.version() => snapshot
                .log_segment()
                .build_crc_delta_from_base(engine, base, None)?,
            _ => CrcDelta::default(),
        };
        // This transaction's changes override those from log commits after the checkpoint action.
        changes.set_transactions.extend(keyed(set_transactions));
        changes.domain_metadata.extend(keyed(dm_changes));

        let log_root = LogRoot::new(snapshot.table_root().clone())?;
        let (mut transactions, txn_sidecars) = apply_changes_to_checkpoint_entries(
            engine,
            snapshot,
            existing,
            &log_root,
            changes.set_transactions,
        )?;
        // Only inline txns are expired: pruning a kept sidecar would mean reading and rewriting it.
        // Its expired txns are pruned once a change moves the sidecar inline.
        // TODO(#2866): prune kept sidecars once kernel can rewrite them.
        let expiration_timestamp =
            calculate_transaction_expiration_timestamp(table_config.table_properties())?;
        transactions.retain(|_, txn| !txn.is_expired(expiration_timestamp));
        let (domain_metadata, domain_metadata_sidecars) = apply_changes_to_checkpoint_entries(
            engine,
            snapshot,
            existing,
            &log_root,
            changes.domain_metadata,
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
            transactions.into_values().collect(),
            domain_metadata.into_values().collect(),
        );
        checkpoint.txn_sidecars = txn_sidecars;
        checkpoint.domain_metadata_sidecars = domain_metadata_sidecars;
        Ok(checkpoint)
    }
}

/// A kind of checkpoint-action state that may be stored inline or in sidecars.
trait CheckpointEntry: Clone {
    fn key(&self) -> &str;
    fn inline(checkpoint: &CheckpointAction) -> &[Self];
    fn sidecars(checkpoint: &CheckpointAction) -> &[Sidecar];
    /// Returns every entry of this kind in `checkpoint`, inline or in a sidecar.
    fn read_from_action(
        engine: &dyn Engine,
        checkpoint: &CheckpointAction,
        log_root: &LogRoot,
    ) -> KernelResult<HashMap<String, Self>>;
    /// Returns every active entry of this kind at `snapshot`'s version.
    fn read_from_snapshot(
        snapshot: &Snapshot,
        engine: &dyn Engine,
    ) -> KernelResult<HashMap<String, Self>>;
    /// Applies `changes` to `entries`, newest wins.
    fn apply(entries: &mut HashMap<String, Self>, changes: HashMap<String, Self>);
}

impl CheckpointEntry for SetTransaction {
    fn key(&self) -> &str {
        &self.app_id
    }

    fn inline(checkpoint: &CheckpointAction) -> &[Self] {
        &checkpoint.transactions
    }

    fn sidecars(checkpoint: &CheckpointAction) -> &[Sidecar] {
        &checkpoint.txn_sidecars
    }

    fn read_from_action(
        engine: &dyn Engine,
        checkpoint: &CheckpointAction,
        log_root: &LogRoot,
    ) -> KernelResult<HashMap<String, Self>> {
        read_checkpoint_action_transactions(engine, checkpoint, log_root)
    }

    fn read_from_snapshot(
        snapshot: &Snapshot,
        engine: &dyn Engine,
    ) -> KernelResult<HashMap<String, Self>> {
        snapshot.get_app_id_versions(engine)
    }

    fn apply(entries: &mut HashMap<String, Self>, changes: HashMap<String, Self>) {
        entries.extend(changes);
    }
}

impl CheckpointEntry for DomainMetadata {
    fn key(&self) -> &str {
        self.domain()
    }

    fn inline(checkpoint: &CheckpointAction) -> &[Self] {
        &checkpoint.domain_metadata
    }

    fn sidecars(checkpoint: &CheckpointAction) -> &[Sidecar] {
        &checkpoint.domain_metadata_sidecars
    }

    fn read_from_action(
        engine: &dyn Engine,
        checkpoint: &CheckpointAction,
        log_root: &LogRoot,
    ) -> KernelResult<HashMap<String, Self>> {
        read_checkpoint_action_domain_metadata(engine, checkpoint, log_root)
    }

    fn read_from_snapshot(
        snapshot: &Snapshot,
        engine: &dyn Engine,
    ) -> KernelResult<HashMap<String, Self>> {
        snapshot
            .reader()
            .get_domain_metadatas_internal(engine, None)
    }

    // Domains can be tombstoned, so merging applies removals.
    fn apply(entries: &mut HashMap<String, Self>, changes: HashMap<String, Self>) {
        merge_domain_metadata(entries, changes);
    }
}

/// Builds one kind of checkpoint-action state by applying `changes` to the `existing` checkpoint
/// action's inline entries, keeping its sidecars unless a change touches an entry stored in one.
/// Without an existing action, `changes` apply to the snapshot's state.
fn apply_changes_to_checkpoint_entries<T: CheckpointEntry>(
    engine: &dyn Engine,
    snapshot: &Snapshot,
    existing: Option<&CheckpointAction>,
    log_root: &LogRoot,
    changes: HashMap<String, T>,
) -> KernelResult<(HashMap<String, T>, Vec<Sidecar>)> {
    let Some(existing) = existing else {
        let mut entries = T::read_from_snapshot(snapshot, engine)?;
        T::apply(&mut entries, changes);
        return Ok((entries, Vec::new()));
    };
    let inline: HashMap<_, _> = keyed(T::inline(existing)).collect();
    let sidecars = T::sidecars(existing);
    // A changed entry that isn't inline is new or in a sidecar, and kernel can't rewrite sidecars.
    // TODO(#2866): rewrite only the affected sidecars once kernel can write them.
    let not_inline = || changes.keys().filter(|key| !inline.contains_key(*key));
    let needs_read = !sidecars.is_empty() && not_inline().next().is_some();
    let action_entries = needs_read
        .then(|| T::read_from_action(engine, existing, log_root))
        .transpose()?;
    let (mut entries, sidecars) = match action_entries {
        Some(all) if not_inline().any(|key| all.contains_key(key)) => (all, Vec::new()),
        _ => (inline, sidecars.to_vec()),
    };
    T::apply(&mut entries, changes);
    Ok((entries, sidecars))
}

fn keyed<T: CheckpointEntry>(entries: &[T]) -> impl Iterator<Item = (String, T)> + '_ {
    entries
        .iter()
        .map(|entry| (entry.key().to_string(), entry.clone()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools as _;
    use rstest::rstest;
    use serde_json::json;
    use url::Url;

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
        write_log_lines(&engine, &table_root, log_commits)?;
        compute_at_latest(&engine, &table_root, &[], &[])
    }

    /// Writes one log commit per line, starting at version 2, after the manifest commit at 1.
    fn write_log_lines(
        engine: &SyncEngine,
        table_root: &Url,
        lines: impl IntoIterator<Item = String>,
    ) -> Result<()> {
        for (version, line) in (2..).zip(lines) {
            let data = parse_json_batch(vec![line.as_str()].into());
            write_commit_after_manifest_commit(engine, table_root, version, 1, data)?;
        }
        Ok(())
    }

    fn compute_at_latest(
        engine: &SyncEngine,
        table_root: &Url,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> Result<CheckpointAction> {
        let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
        let manifest = root_manifest(table_root, "metadata/root.parquet", 2048, snapshot.clone());
        Ok(manifest.compute_checkpoint_action(
            engine,
            snapshot.version() + 1,
            snapshot.table_configuration(),
            dm_changes,
            set_transactions,
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

        let result = compute_at_latest(&engine, &table_root, &[], &[]);
        match expected_error {
            Some(message) => assert_result_error_with_message(result, message),
            // The checkpoint action at version 2 holds no txns, so `app` comes from the commits
            // after it.
            None => assert_eq!(
                result?.transactions,
                [SetTransaction::new("app".to_string(), 4, None)]
            ),
        }
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum ExpiredTxnSource {
        LogCommit,
        InlineInAction,
        LogCommitAfterAction,
        Sidecar,
    }

    // `expired-app` has expired and `live-app` never expires. A change to `live-app` moves its
    // sidecar inline; a change to `new-app` keeps it, and txns in a kept sidecar are not pruned.
    #[rstest]
    #[case::log_commit(ExpiredTxnSource::LogCommit, None, &["live-app"], false)]
    #[case::inline_in_action(ExpiredTxnSource::InlineInAction, None, &["live-app"], false)]
    #[case::log_commit_after_action(
        ExpiredTxnSource::LogCommitAfterAction,
        None,
        &["live-app"],
        false
    )]
    #[case::sidecar_moved_inline(ExpiredTxnSource::Sidecar, Some("live-app"), &["live-app"], false)]
    #[case::sidecar_kept(ExpiredTxnSource::Sidecar, Some("new-app"), &["new-app"], true)]
    fn compute_checkpoint_action_prunes_expired_transactions(
        #[case] source: ExpiredTxnSource,
        #[case] changed: Option<&str>,
        #[case] expected_inline: &[&str],
        #[case] sidecar_kept: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let expired = SetTransaction::new("expired-app".to_string(), 5, Some(0));
        let live = SetTransaction::new("live-app".to_string(), 5, None);
        let txn_row = |txn| create_row(&engine, LOG_TXN_SCHEMA.clone(), txn);
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        match source {
            ExpiredTxnSource::LogCommit => {
                write_commit(&engine, &table_root, 1, txn_row(expired)?)?;
                write_commit(&engine, &table_root, 2, txn_row(live)?)?;
            }
            ExpiredTxnSource::InlineInAction => {
                existing.transactions = vec![expired, live];
                write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
            }
            ExpiredTxnSource::LogCommitAfterAction => {
                existing.transactions = vec![live];
                write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
                write_commit_after_manifest_commit(&engine, &table_root, 2, 1, txn_row(expired)?)?;
            }
            ExpiredTxnSource::Sidecar => {
                let rows = vec![expired.into(), live.into()];
                let schema = LOG_TXN_SCHEMA.clone();
                existing.txn_sidecars = vec![write_sidecar(
                    &engine,
                    &table_root,
                    "txn.parquet",
                    schema,
                    rows,
                )];
                write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
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
        let set_transactions: Vec<_> = changed.into_iter().map(|key| txn(key, 6)).collect();
        let checkpoint = manifest.compute_checkpoint_action(
            &engine,
            commit_version,
            &table_config,
            &[],
            &set_transactions,
        )?;

        let app_ids = checkpoint.transactions.iter().map(|t| t.app_id.as_str());
        assert_eq!(app_ids.sorted().collect_vec(), expected_inline);
        let expected_sidecars = if sidecar_kept {
            existing.txn_sidecars
        } else {
            vec![]
        };
        assert_eq!(checkpoint.txn_sidecars, expected_sidecars);
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

    fn domain_metadata_sidecar(
        engine: &SyncEngine,
        table_root: &Url,
        domains: impl IntoIterator<Item = DomainMetadata>,
    ) -> Sidecar {
        let rows = domains.into_iter().map(Into::into).collect();
        let schema = LOG_DOMAIN_METADATA_SCHEMA.clone();
        write_sidecar(engine, table_root, "domains.parquet", schema, rows)
    }

    // The checkpoint action holds the `inline` app id and domain inline, and `kept` and `spilled`
    // in sidecars.
    #[rstest]
    #[case::unchanged(None, None, &["inline"], true)]
    #[case::new_entry(Some("new"), None, &["inline", "new"], true)]
    #[case::sidecar_entry(Some("spilled"), None, &["inline", "kept", "spilled"], false)]
    #[case::logged_new_entry(None, Some("logged"), &["inline", "logged"], true)]
    #[case::logged_sidecar_entry(None, Some("spilled"), &["inline", "kept", "spilled"], false)]
    fn compute_checkpoint_action_keeps_sidecars_unless_a_change_touches_them(
        #[case] changed: Option<&str>,
        #[case] logged: Option<&str>,
        #[case] inline_keys: &[&str],
        #[case] sidecars_kept: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.transactions = vec![txn("inline", 1)];
        existing.domain_metadata = vec![domain("inline", "{}")];
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
        existing.domain_metadata_sidecars = vec![domain_metadata_sidecar(
            &engine,
            &table_root,
            sidecar_keys.map(|key| domain(key, "{}")),
        )];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
        let logged_domain = logged.map(|key| domain(key, r#"{"logged":true}"#));
        let logged_txn = logged.map(|key| SetTransaction::new(key.to_string(), 3, None));
        if let Some((dm, txn)) = logged_domain.clone().zip(logged_txn.clone()) {
            let lines = [domain_metadata_line(dm), txn_line(&txn.app_id, txn.version)];
            write_log_lines(&engine, &table_root, lines)?;
        }

        let set_transactions: Vec<_> = changed
            .map(|key| SetTransaction::new(key.to_string(), 2, None))
            .into_iter()
            .collect();
        let dm_changes: Vec<_> = changed
            .map(|key| DomainMetadata::new(key.to_string(), r#"{"changed":true}"#.to_string()))
            .into_iter()
            .collect();
        let checkpoint = compute_at_latest(&engine, &table_root, &dm_changes, &set_transactions)?;

        let app_ids = checkpoint.transactions.iter().map(|t| t.app_id.as_str());
        assert_eq!(app_ids.sorted().collect_vec(), inline_keys);
        let domains = checkpoint.domain_metadata.iter().map(|d| d.domain());
        assert_eq!(domains.sorted().collect_vec(), inline_keys);
        assert!(
            set_transactions
                .iter()
                .chain(&logged_txn)
                .all(|t| checkpoint.transactions.contains(t))
        );
        assert!(
            dm_changes
                .iter()
                .chain(&logged_domain)
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

    // The checkpoint action holds domain `inline` inline and `spilled` in a sidecar. Cases that
    // must not read the sidecar point the action at a missing file.
    #[rstest]
    #[case::inline_entry_updated(
        vec![domain("inline", "2")],
        vec![],
        vec![domain("inline", "2")],
        true,
        false
    )]
    #[case::transaction_overrides_log_commit(
        vec![domain("inline", "2")],
        vec![domain("inline", "3")],
        vec![domain("inline", "3")],
        true,
        false
    )]
    #[case::sidecar_entry_removed(
        vec![DomainMetadata::remove("spilled".into(), "1".into())],
        vec![],
        vec![domain("inline", "1")],
        false,
        true
    )]
    #[case::new_entry_added_then_removed(
        vec![domain("new", "1"), DomainMetadata::remove("new".into(), "1".into())],
        vec![],
        vec![domain("inline", "1")],
        true,
        true
    )]
    fn compute_checkpoint_action_applies_domain_log_commits_over_sidecars(
        #[case] log_domains: Vec<DomainMetadata>,
        #[case] dm_changes: Vec<DomainMetadata>,
        #[case] expected_inline: Vec<DomainMetadata>,
        #[case] sidecar_kept: bool,
        #[case] sidecar_read: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.domain_metadata = vec![domain("inline", "1")];
        existing.domain_metadata_sidecars = vec![if sidecar_read {
            domain_metadata_sidecar(&engine, &table_root, [domain("spilled", "1")])
        } else {
            Sidecar {
                path: "missing.parquet".to_string(),
                size_in_bytes: 1024,
                modification_time: 0,
                tags: None,
            }
        }];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
        let lines = log_domains.into_iter().map(domain_metadata_line);
        write_log_lines(&engine, &table_root, lines)?;

        let mut checkpoint = compute_at_latest(&engine, &table_root, &dm_changes, &[])?;

        checkpoint
            .domain_metadata
            .sort_by(|a, b| a.domain().cmp(b.domain()));
        assert_eq!(checkpoint.domain_metadata, expected_inline);
        let expected_sidecars = if sidecar_kept {
            existing.domain_metadata_sidecars
        } else {
            vec![]
        };
        assert_eq!(checkpoint.domain_metadata_sidecars, expected_sidecars);
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
