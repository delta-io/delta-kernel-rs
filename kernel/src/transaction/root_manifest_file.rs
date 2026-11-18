//! Commits a caller-supplied root manifest file as the table's content root.

use std::collections::HashMap;
use std::sync::Arc;

use crate::action_reconciliation::calculate_transaction_expiration_timestamp;
use crate::actions::set_transaction::scan_application_transactions;
use crate::actions::{
    CheckpointAction, ContentRoot, DomainMetadata, LOG_DOMAIN_METADATA_SCHEMA, LOG_TXN_SCHEMA,
    SetTransaction, Sidecar,
};
use crate::content_tree::read_non_add_actions;
use crate::crc::{Crc, CrcDelta, DomainMetadataState, SetTransactionState, merge_domain_metadata};
use crate::error::KernelError;
use crate::log_segment::scan_domain_metadata_actions;
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
    /// (this transaction's `dm_changes` and `set_transactions` merged in, newest wins).
    ///
    /// The latest checkpoint action's sidecars are kept unless a change touches an entry stored in
    /// one. When the snapshot's CRC holds every set transaction or every domain metadata, that kind
    /// is written inline instead. Expired set transactions are dropped from the inline entries but
    /// carried forward in a kept sidecar.
    pub(super) fn compute_checkpoint_action(
        &self,
        engine: &dyn Engine,
        commit_version: Version,
        table_config: &TableConfiguration,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> KernelResult<CheckpointAction> {
        let snapshot = self.read_snapshot.as_ref();
        let crc = snapshot.crc_at_version().map(Arc::as_ref);
        let txns_in_crc = crc.is_some_and(SetTransaction::complete_in_crc);
        let domains_in_crc = crc.is_some_and(DomainMetadata::complete_in_crc);
        // The checkpoint action is only needed for a kind the CRC doesn't hold in full.
        let latest_checkpoint_action = if txns_in_crc && domains_in_crc {
            None
        } else {
            snapshot.latest_checkpoint_action(engine)?
        };
        let existing = latest_checkpoint_action.as_deref();
        let txn_base = existing.filter(|_| !txns_in_crc);
        let domain_base = existing.filter(|_| !domains_in_crc);
        let log_changes = match txn_base.or(domain_base) {
            Some(action) => changes_after_checkpoint_action(engine, snapshot, action)?,
            None => CrcDelta::default(),
        };

        let log_root = LogRoot::new(snapshot.table_root().clone())?;
        let (mut transactions, txn_sidecars) = checkpoint_entries(
            engine,
            snapshot,
            txn_base,
            &log_root,
            log_changes.set_transactions,
            set_transactions,
        )?;
        let (domain_metadata, domain_metadata_sidecars) = checkpoint_entries(
            engine,
            snapshot,
            domain_base,
            &log_root,
            log_changes.domain_metadata,
            dm_changes,
        )?;
        // TODO(#2866): prune expired txns in kept sidecars. Until then, whether an expired txn
        // survives depends on whether it was inline or in a kept sidecar.
        let expiration_timestamp =
            calculate_transaction_expiration_timestamp(table_config.table_properties())?;
        transactions.retain(|_, txn| !txn.is_expired(expiration_timestamp));

        let version = version_as_i64(commit_version)?;
        let size = i64::try_from(self.file.size)
            .map_err(|_| KernelError::generic("root manifest file size exceeds i64::MAX"))?;
        Ok(CheckpointAction {
            version,
            content_root: ContentRoot::new(self.file.location.to_string(), size, version),
            protocol: table_config.protocol().clone(),
            metadata: table_config.metadata().clone(),
            transactions: transactions.into_values().collect(),
            domain_metadata: domain_metadata.into_values().collect(),
            txn_sidecars,
            domain_metadata_sidecars,
        })
    }
}

/// A set transaction or domain metadata entry, which a checkpoint action stores inline or in
/// sidecars.
trait CheckpointActionEntry: Clone {
    fn key(&self) -> &str;
    fn inline(checkpoint: &CheckpointAction) -> &[Self];
    fn sidecars(checkpoint: &CheckpointAction) -> &[Sidecar];
    /// Whether `crc` holds every active entry of this type.
    fn complete_in_crc(crc: &Crc) -> bool;
    /// Returns every entry of this type in `checkpoint`, inline or in a sidecar.
    fn read_from_action(
        engine: &dyn Engine,
        checkpoint: &CheckpointAction,
        log_root: &LogRoot,
    ) -> KernelResult<HashMap<String, Self>>;
    /// Returns every active entry of this type at `snapshot`'s version.
    fn read_from_snapshot(
        snapshot: &Snapshot,
        engine: &dyn Engine,
    ) -> KernelResult<HashMap<String, Self>>;
    /// Applies `changes` to `entries`, newest wins.
    fn apply(entries: &mut HashMap<String, Self>, changes: HashMap<String, Self>);
}

impl CheckpointActionEntry for SetTransaction {
    fn key(&self) -> &str {
        &self.app_id
    }

    fn inline(checkpoint: &CheckpointAction) -> &[Self] {
        &checkpoint.transactions
    }

    fn sidecars(checkpoint: &CheckpointAction) -> &[Sidecar] {
        &checkpoint.txn_sidecars
    }

    fn complete_in_crc(crc: &Crc) -> bool {
        matches!(crc.set_transaction_state, SetTransactionState::Complete(_))
    }

    fn read_from_action(
        engine: &dyn Engine,
        checkpoint: &CheckpointAction,
        log_root: &LogRoot,
    ) -> KernelResult<HashMap<String, Self>> {
        let actions = read_non_add_actions(engine, checkpoint, log_root, LOG_TXN_SCHEMA.clone())?;
        scan_application_transactions(actions, None)
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

impl CheckpointActionEntry for DomainMetadata {
    fn key(&self) -> &str {
        self.domain()
    }

    fn inline(checkpoint: &CheckpointAction) -> &[Self] {
        &checkpoint.domain_metadata
    }

    fn sidecars(checkpoint: &CheckpointAction) -> &[Sidecar] {
        &checkpoint.domain_metadata_sidecars
    }

    fn complete_in_crc(crc: &Crc) -> bool {
        matches!(crc.domain_metadata_state, DomainMetadataState::Complete(_))
    }

    fn read_from_action(
        engine: &dyn Engine,
        checkpoint: &CheckpointAction,
        log_root: &LogRoot,
    ) -> KernelResult<HashMap<String, Self>> {
        let schema = LOG_DOMAIN_METADATA_SCHEMA.clone();
        let actions = read_non_add_actions(engine, checkpoint, log_root, schema)?;
        scan_domain_metadata_actions(actions, None)
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

/// Returns the set transaction and domain metadata changes in the log commits after `action`.
fn changes_after_checkpoint_action(
    engine: &dyn Engine,
    snapshot: &Snapshot,
    action: &CheckpointAction,
) -> KernelResult<CrcDelta> {
    let base = action.checkpoint_version()?;
    if base == snapshot.version() {
        return Ok(CrcDelta::default());
    }
    snapshot
        .log_segment()
        .build_crc_delta_from_base(engine, base, None)
}

/// Returns the `T` entries for the new checkpoint action, and the sidecars it keeps.
///
/// With a `base` checkpoint action, `log_changes` (the log commits after `base`) and then
/// `txn_changes` are applied to it. Without one, `txn_changes` apply to the snapshot's state.
fn checkpoint_entries<T: CheckpointActionEntry>(
    engine: &dyn Engine,
    snapshot: &Snapshot,
    base: Option<&CheckpointAction>,
    log_root: &LogRoot,
    log_changes: HashMap<String, T>,
    txn_changes: &[T],
) -> KernelResult<(HashMap<String, T>, Vec<Sidecar>)> {
    let Some(base) = base else {
        // There's no base when no checkpoint action exists or the CRC holds this type in full. A
        // complete CRC doesn't say what changed since the action, so keeping the action's sidecars
        // would mean reading them to find touched entries.
        // TODO: keep them by diffing the CRC against the action if inlining proves costly.
        let mut entries = T::read_from_snapshot(snapshot, engine)?;
        T::apply(&mut entries, keyed(txn_changes).collect());
        return Ok((entries, Vec::new()));
    };
    let mut changes = log_changes;
    changes.extend(keyed(txn_changes));
    fold_into_checkpoint_action(engine, base, log_root, changes)
}

/// Applies `changes` to `action`'s inline `T` entries, keeping its sidecars unless a change
/// touches an entry stored in one.
fn fold_into_checkpoint_action<T: CheckpointActionEntry>(
    engine: &dyn Engine,
    action: &CheckpointAction,
    log_root: &LogRoot,
    changes: HashMap<String, T>,
) -> KernelResult<(HashMap<String, T>, Vec<Sidecar>)> {
    // Readers keep the first occurrence of a key, so this must too.
    let mut inline = HashMap::new();
    for (key, entry) in keyed(T::inline(action)) {
        inline.entry(key).or_insert(entry);
    }
    let sidecars = T::sidecars(action);
    // Kernel can't rewrite a sidecar, so only a change to an entry stored in one forces reading
    // the sidecars and moving them inline.
    // TODO(#2866): rewrite only the affected sidecars.
    let not_inline = || changes.keys().filter(|key| !inline.contains_key(*key));
    if !sidecars.is_empty() && not_inline().next().is_some() {
        let mut action_entries = T::read_from_action(engine, action, log_root)?;
        if not_inline().any(|key| action_entries.contains_key(key)) {
            T::apply(&mut action_entries, changes);
            return Ok((action_entries, Vec::new()));
        }
    }
    T::apply(&mut inline, changes);
    Ok((inline, sidecars.to_vec()))
}

fn keyed<T: CheckpointActionEntry>(entries: &[T]) -> impl Iterator<Item = (String, T)> + '_ {
    entries
        .iter()
        .map(|entry| (entry.key().to_string(), entry.clone()))
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use itertools::Itertools as _;
    use rstest::rstest;
    use serde_json::json;
    use url::Url;

    use super::*;
    use crate::actions::{LastManifestCommit, METADATA_NAME, PROTOCOL_NAME, get_commit_schema};
    use crate::committer::FileSystemCommitter;
    use crate::engine::sync::SyncEngine;
    use crate::schema::schema_ref;
    use crate::snapshot::Snapshot;
    use crate::transaction::UpdateTableOperation;
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
    /// checkpoint action of the next root manifest commit, which sets `set_transactions`.
    fn checkpoint_action_after(
        log_domains: Vec<DomainMetadata>,
        log_txns: Vec<SetTransaction>,
        set_transactions: &[SetTransaction],
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
        compute_at_latest(&engine, &table_root, &[], set_transactions)
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
        let mut checkpoint = checkpoint_action_after(log_domains, log_txns, &[])?;
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

    #[test]
    fn compute_checkpoint_action_transaction_overrides_logged_txn() -> Result<()> {
        let checkpoint =
            checkpoint_action_after(vec![], vec![txn("app-1", 7)], &[txn("app-1", 9)])?;
        assert_eq!(checkpoint.transactions, [txn("app-1", 9)]);
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
        let txn_commit = |version| parse_json_batch(vec![txn_line("app", version).as_str()].into());
        let checkpoint = minimal_checkpoint_action("metadata/root-v2.parquet", 2)?;
        write_commit(&engine, &table_root, 1, txn_commit(1))?;
        if classic_version == 1 {
            Snapshot::builder_for(table_root.clone())
                .build(&engine)?
                .checkpoint(&engine, None)?;
        }
        write_manifest_commit(&engine, &table_root, 2, checkpoint.clone())?;
        write_commit_after_manifest_commit(&engine, &table_root, 3, 2, txn_commit(3))?;
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
        write_commit_after_manifest_commit(&engine, &table_root, 4, 2, txn_commit(4))?;

        let result = compute_at_latest(&engine, &table_root, &[], &[]);
        match expected_error {
            Some(message) => assert_result_error_with_message(result, message),
            // The checkpoint action at version 2 holds no txns, so `app` comes from the commits
            // after it.
            None => assert_eq!(result?.transactions, [txn("app", 4)]),
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
        let live = txn("live-app", 5);
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
                existing.txn_sidecars = vec![txn_sidecar(
                    &engine,
                    &table_root,
                    "txn.parquet",
                    [expired, live],
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

        assert_eq!(sorted_keys(&checkpoint.transactions), expected_inline);
        assert_eq!(
            checkpoint.txn_sidecars,
            kept_or_empty(&existing.txn_sidecars, sidecar_kept)
        );
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
        let engine = SyncEngine::in_memory();
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

    fn txn_sidecar(
        engine: &SyncEngine,
        table_root: &Url,
        name: &str,
        txns: impl IntoIterator<Item = SetTransaction>,
    ) -> Sidecar {
        let rows = txns.into_iter().map(Into::into).collect();
        write_sidecar(engine, table_root, name, LOG_TXN_SCHEMA.clone(), rows)
    }

    fn domain_metadata_sidecar(
        engine: &SyncEngine,
        table_root: &Url,
        name: &str,
        domains: impl IntoIterator<Item = DomainMetadata>,
    ) -> Sidecar {
        let rows = domains.into_iter().map(Into::into).collect();
        let schema = LOG_DOMAIN_METADATA_SCHEMA.clone();
        write_sidecar(engine, table_root, name, schema, rows)
    }

    /// A sidecar whose file was never written, so reading it fails.
    fn missing_sidecar(name: &str) -> Sidecar {
        Sidecar::new(name.to_string(), 1024, 0, None)
    }

    fn kept_or_empty(sidecars: &[Sidecar], kept: bool) -> Vec<Sidecar> {
        if kept { sidecars.to_vec() } else { vec![] }
    }

    fn sorted_keys<T: CheckpointActionEntry>(entries: &[T]) -> Vec<&str> {
        entries.iter().map(|entry| entry.key()).sorted().collect()
    }

    // The checkpoint action holds the `inline` app id and domain inline, and `kept` and `spilled`
    // in sidecars. A case's changes apply only to the kinds selected by `txns_change` and
    // `domains_change`, so the other kind must come through untouched.
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
        #[values(true, false)] txns_change: bool,
        #[values(true, false)] domains_change: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.transactions = vec![txn("inline", 1)];
        existing.domain_metadata = vec![domain("inline", "{}")];
        let sidecar_keys = ["kept", "spilled"];
        existing.txn_sidecars = vec![txn_sidecar(
            &engine,
            &table_root,
            "txn.parquet",
            sidecar_keys.map(|key| txn(key, 1)),
        )];
        existing.domain_metadata_sidecars = vec![domain_metadata_sidecar(
            &engine,
            &table_root,
            "domains.parquet",
            sidecar_keys.map(|key| domain(key, "{}")),
        )];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
        let logged_domain = logged
            .filter(|_| domains_change)
            .map(|key| domain(key, r#"{"logged":true}"#));
        let logged_txn = logged.filter(|_| txns_change).map(|key| txn(key, 3));
        let lines = logged_domain
            .clone()
            .map(domain_metadata_line)
            .into_iter()
            .chain(
                logged_txn
                    .as_ref()
                    .map(|txn| txn_line(&txn.app_id, txn.version)),
            );
        write_log_lines(&engine, &table_root, lines)?;

        let set_transactions: Vec<_> = changed
            .filter(|_| txns_change)
            .map(|key| txn(key, 2))
            .into_iter()
            .collect();
        let dm_changes: Vec<_> = changed
            .filter(|_| domains_change)
            .map(|key| domain(key, r#"{"changed":true}"#))
            .into_iter()
            .collect();
        let checkpoint = compute_at_latest(&engine, &table_root, &dm_changes, &set_transactions)?;

        let expected = |kind_changes| match kind_changes {
            true => (inline_keys, sidecars_kept),
            false => (&["inline"][..], true),
        };
        let (keys, kept) = expected(txns_change);
        let txn_changes = set_transactions.iter().chain(&logged_txn);
        assert_checkpoint_entries(&checkpoint, &existing, txn_changes, keys, kept);
        let (keys, kept) = expected(domains_change);
        let domain_changes = dm_changes.iter().chain(&logged_domain);
        assert_checkpoint_entries(&checkpoint, &existing, domain_changes, keys, kept);
        Ok(())
    }

    /// Asserts `checkpoint` holds exactly `inline_keys` of kind `T` inline, including every entry
    /// in `changes`, and carries `existing`'s sidecars of that kind iff `sidecars_kept`.
    fn assert_checkpoint_entries<'a, T: CheckpointActionEntry + PartialEq + Debug + 'a>(
        checkpoint: &CheckpointAction,
        existing: &CheckpointAction,
        changes: impl IntoIterator<Item = &'a T>,
        inline_keys: &[&str],
        sidecars_kept: bool,
    ) {
        let inline = T::inline(checkpoint);
        assert_eq!(sorted_keys(inline), inline_keys);
        assert!(changes.into_iter().all(|entry| inline.contains(entry)));
        let expected_sidecars = kept_or_empty(T::sidecars(existing), sidecars_kept);
        assert_eq!(T::sidecars(checkpoint), expected_sidecars);
    }

    #[derive(Clone, Copy)]
    enum SidecarFile {
        Written,
        Missing,
    }

    // The checkpoint action holds domain `inline` inline and `spilled` in a sidecar. Cases that
    // must not read the sidecar point the action at a missing file.
    #[rstest]
    #[case::inline_entry_updated(
        vec![domain("inline", "2")],
        vec![],
        vec![domain("inline", "2")],
        true,
        SidecarFile::Missing
    )]
    #[case::transaction_overrides_log_commit(
        vec![domain("inline", "2")],
        vec![domain("inline", "3")],
        vec![domain("inline", "3")],
        true,
        SidecarFile::Missing
    )]
    #[case::sidecar_entry_removed(
        vec![DomainMetadata::remove("spilled".into(), "1".into())],
        vec![],
        vec![domain("inline", "1")],
        false,
        SidecarFile::Written
    )]
    #[case::new_entry_added_then_removed(
        vec![domain("new", "1"), DomainMetadata::remove("new".into(), "1".into())],
        vec![],
        vec![domain("inline", "1")],
        true,
        SidecarFile::Written
    )]
    fn compute_checkpoint_action_applies_domain_log_commits_over_sidecars(
        #[case] log_domains: Vec<DomainMetadata>,
        #[case] dm_changes: Vec<DomainMetadata>,
        #[case] expected_inline: Vec<DomainMetadata>,
        #[case] sidecar_kept: bool,
        #[case] sidecar_file: SidecarFile,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.domain_metadata = vec![domain("inline", "1")];
        existing.domain_metadata_sidecars = vec![match sidecar_file {
            SidecarFile::Written => domain_metadata_sidecar(
                &engine,
                &table_root,
                "domains.parquet",
                [domain("spilled", "1")],
            ),
            SidecarFile::Missing => missing_sidecar("missing.parquet"),
        }];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
        let lines = log_domains.into_iter().map(domain_metadata_line);
        write_log_lines(&engine, &table_root, lines)?;

        let mut checkpoint = compute_at_latest(&engine, &table_root, &dm_changes, &[])?;

        checkpoint
            .domain_metadata
            .sort_by(|a, b| a.domain().cmp(b.domain()));
        assert_eq!(checkpoint.domain_metadata, expected_inline);
        assert_eq!(
            checkpoint.domain_metadata_sidecars,
            kept_or_empty(&existing.domain_metadata_sidecars, sidecar_kept)
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

    // The checkpoint action holds `a` and `b` of each type in two sidecars. Touching `b` must
    // inline the entries of both sidecars, and leaving them untouched must keep both, in order.
    #[rstest]
    #[case::untouched("new", &["new"], true)]
    #[case::one_touched("b", &["a", "b"], false)]
    fn compute_checkpoint_action_inlines_every_sidecar_when_one_is_touched(
        #[case] changed: &str,
        #[case] inline_keys: &[&str],
        #[case] sidecars_kept: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.txn_sidecars = ["a", "b"]
            .map(|key| {
                txn_sidecar(
                    &engine,
                    &table_root,
                    &format!("txn-{key}.parquet"),
                    [txn(key, 1)],
                )
            })
            .into();
        existing.domain_metadata_sidecars = ["a", "b"]
            .map(|key| {
                let name = format!("domains-{key}.parquet");
                domain_metadata_sidecar(&engine, &table_root, &name, [domain(key, "1")])
            })
            .into();
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;

        let set_transactions = [txn(changed, 2)];
        let dm_changes = [domain(changed, "2")];
        let checkpoint = compute_at_latest(&engine, &table_root, &dm_changes, &set_transactions)?;

        let (keys, kept) = (inline_keys, sidecars_kept);
        assert_checkpoint_entries(&checkpoint, &existing, &set_transactions, keys, kept);
        assert_checkpoint_entries(&checkpoint, &existing, &dm_changes, keys, kept);
        Ok(())
    }

    // Both sidecars point at files that were never written, so reading either fails. No change
    // here touches a sidecar entry, so neither is read.
    #[rstest]
    #[case::no_changes(vec![], vec![])]
    #[case::inline_entries_updated(vec![txn("inline", 2)], vec![domain("inline", "2")])]
    fn compute_checkpoint_action_reads_no_sidecar_for_inline_changes(
        #[case] set_transactions: Vec<SetTransaction>,
        #[case] dm_changes: Vec<DomainMetadata>,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.transactions = vec![txn("inline", 1)];
        existing.domain_metadata = vec![domain("inline", "1")];
        existing.txn_sidecars = vec![missing_sidecar("txn.parquet")];
        existing.domain_metadata_sidecars = vec![missing_sidecar("domains.parquet")];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;

        let checkpoint = compute_at_latest(&engine, &table_root, &dm_changes, &set_transactions)?;

        assert_checkpoint_entries(&checkpoint, &existing, &set_transactions, &["inline"], true);
        assert_checkpoint_entries(&checkpoint, &existing, &dm_changes, &["inline"], true);
        Ok(())
    }

    // System domains sit in sidecars alongside user domains and survive both a kept sidecar and
    // one moved inline.
    #[rstest]
    #[case::sidecar_kept("new", &["new"], true)]
    #[case::sidecar_moved_inline("spilled", &["delta.rowTracking", "spilled"], false)]
    fn compute_checkpoint_action_preserves_system_domains_in_sidecars(
        #[case] changed: &str,
        #[case] inline_keys: &[&str],
        #[case] sidecar_kept: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let row_tracking = domain("delta.rowTracking", r#"{"rowIdHighWaterMark":5}"#);
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        let domains = [row_tracking.clone(), domain("spilled", "1")];
        existing.domain_metadata_sidecars = vec![domain_metadata_sidecar(
            &engine,
            &table_root,
            "domains.parquet",
            domains,
        )];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;

        let dm_changes = [domain(changed, "2")];
        let checkpoint = compute_at_latest(&engine, &table_root, &dm_changes, &[])?;

        assert_checkpoint_entries(
            &checkpoint,
            &existing,
            &dm_changes,
            inline_keys,
            sidecar_kept,
        );
        assert_eq!(
            checkpoint.domain_metadata.contains(&row_tracking),
            !sidecar_kept
        );
        Ok(())
    }

    // Readers keep the first occurrence of a duplicated key, so the new checkpoint action must
    // too.
    #[test]
    fn compute_checkpoint_action_keeps_the_first_of_duplicate_inline_entries() -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.transactions = vec![txn("app", 10), txn("app", 5)];
        existing.domain_metadata = vec![
            domain("d", "active"),
            DomainMetadata::remove("d".into(), "active".into()),
        ];
        write_manifest_commit(&engine, &table_root, 1, existing)?;
        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        assert_eq!(
            snapshot.get_app_id_versions(&engine)?["app"],
            txn("app", 10)
        );
        let domains = snapshot
            .reader()
            .get_domain_metadatas_internal(&engine, None)?;
        assert_eq!(domains["d"], domain("d", "active"));

        let checkpoint = compute_at_latest(&engine, &table_root, &[], &[])?;

        assert_eq!(checkpoint.transactions, [txn("app", 10)]);
        assert_eq!(checkpoint.domain_metadata, [domain("d", "active")]);
        Ok(())
    }

    // A type the CRC holds in full is written inline from the CRC, dropping that type's
    // sidecars. A type it doesn't is folded from the checkpoint action and the log commits after
    // it. Both sidecars point at unwritten files, so reading either fails.
    #[rstest]
    fn compute_checkpoint_action_uses_a_complete_crc_over_the_checkpoint_action(
        #[values(true, false)] txns_complete: bool,
        #[values(true, false)] domains_complete: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.transactions = vec![txn("inline", 1)];
        existing.domain_metadata = vec![domain("inline", "1")];
        existing.txn_sidecars = vec![missing_sidecar("txn.parquet")];
        existing.domain_metadata_sidecars = vec![missing_sidecar("domains.parquet")];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
        let (logged_txn, logged_domain) = (txn("inline", 2), domain("inline", "2"));
        let logged = [
            txn_line(&logged_txn.app_id, logged_txn.version),
            domain_metadata_line(logged_domain.clone()),
        ];
        write_log_lines(&engine, &table_root, logged)?;

        // The CRC holds `from-crc` instead of the action's entries, so the source is visible.
        let built = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let crc = Crc {
            version: built.version(),
            protocol: built.table_configuration().protocol().clone(),
            metadata: built.table_configuration().metadata().clone(),
            set_transaction_state: match txns_complete {
                true => SetTransactionState::Complete(HashMap::from([(
                    "from-crc".to_string(),
                    txn("from-crc", 1),
                )])),
                false => SetTransactionState::Partial(Default::default()),
            },
            domain_metadata_state: match domains_complete {
                true => DomainMetadataState::Complete(HashMap::from([(
                    "from-crc".to_string(),
                    domain("from-crc", "1"),
                )])),
                false => DomainMetadataState::Partial(Default::default()),
            },
            last_manifest_commit_opt: Some(LastManifestCommit {
                version: 1,
                content_root_version: 1,
            }),
            ..Default::default()
        };
        let snapshot = Arc::new(Snapshot::new_with_crc(
            built.log_segment().clone(),
            built.table_configuration().clone(),
            Some(Arc::new(crc)),
            true,
            false,
        )?);
        if txns_complete && domains_complete {
            // Neither the checkpoint action nor the commits after it are needed, so any log read
            // fails.
            for version in 1..=built.version() {
                let commit = table_root
                    .join(&format!("_delta_log/{version:020}.json"))
                    .map_err(KernelError::from)?;
                engine.storage_handler().delete(&commit)?;
            }
        }
        let commit_version = built.version() + 1;
        let manifest = root_manifest(&table_root, "metadata/root-v4.parquet", 1024, snapshot);
        let table_config = manifest.read_snapshot.table_configuration().clone();
        let checkpoint =
            manifest.compute_checkpoint_action(&engine, commit_version, &table_config, &[], &[])?;

        // A type folded from the checkpoint action keeps its sidecars and takes the logged update.
        let keys = |complete| if complete { ["from-crc"] } else { ["inline"] };
        let txn_logged = (!txns_complete).then_some(&logged_txn);
        let txn_keys = keys(txns_complete);
        assert_checkpoint_entries(
            &checkpoint,
            &existing,
            txn_logged,
            &txn_keys,
            !txns_complete,
        );
        let domain_logged = (!domains_complete).then_some(&logged_domain);
        let domain_keys = keys(domains_complete);
        assert_checkpoint_entries(
            &checkpoint,
            &existing,
            domain_logged,
            &domain_keys,
            !domains_complete,
        );
        Ok(())
    }

    // A manifest committed through a transaction keeps the untouched sidecars, and the next
    // snapshot reads their entries alongside the transaction's own.
    #[test]
    fn committed_manifest_keeps_sidecars_that_the_next_snapshot_reads() -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.txn_sidecars = vec![txn_sidecar(
            &engine,
            &table_root,
            "txn.parquet",
            [txn("kept", 1)],
        )];
        let kept_domain = [domain("kept", "1")];
        existing.domain_metadata_sidecars = vec![domain_metadata_sidecar(
            &engine,
            &table_root,
            "domains.parquet",
            kept_domain,
        )];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;

        let root_manifest = manifest_file(&format!("{table_root}metadata/root-v2.parquet"), 1024)?;
        Snapshot::builder_for(table_root.clone())
            .build(&engine)?
            .update_table(UpdateTableOperation::Write, "test", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .with_transaction_id("new".to_string(), 2)
            .with_domain_metadata("new".to_string(), "2".to_string())
            .with_root_manifest_file(root_manifest)?
            .commit(&engine)?
            .unwrap_committed();

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let checkpoint = snapshot
            .latest_checkpoint_action(&engine)?
            .expect("the commit is a manifest commit");
        assert_eq!(checkpoint.txn_sidecars, existing.txn_sidecars);
        assert_eq!(
            checkpoint.domain_metadata_sidecars,
            existing.domain_metadata_sidecars
        );
        let app_ids = snapshot.get_app_id_versions(&engine)?;
        assert_eq!(
            sorted_keys(&app_ids.into_values().collect_vec()),
            ["kept", "new"]
        );
        let domains = snapshot
            .reader()
            .get_domain_metadatas_internal(&engine, None)?;
        assert_eq!(domains["kept"], domain("kept", "1"));
        assert_eq!(domains["new"], domain("new", "2"));
        Ok(())
    }
}
