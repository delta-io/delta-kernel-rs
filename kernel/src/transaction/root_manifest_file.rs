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
use crate::metrics::CheckpointEntrySource;
use crate::path::LogRoot;
use crate::snapshot::{Snapshot, SnapshotRef};
use crate::table_configuration::TableConfiguration;
use crate::{Engine, FileMeta, KernelResult, Version, version_as_i64};

/// A `checkpoint` action computed by [`RootManifestFile::compute_checkpoint_action`], with the
/// `CheckpointActionComputed` metrics only the computation knows.
pub(super) struct ComputedCheckpointAction {
    pub(super) action: CheckpointAction,
    pub(super) txn_source: CheckpointEntrySource,
    pub(super) domain_metadata_source: CheckpointEntrySource,
    pub(super) num_log_commits_replayed: u64,
    pub(super) num_txn_sidecars_read: u64,
    pub(super) num_domain_metadata_sidecars_read: u64,
}

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
    pub(super) fn compute_checkpoint_action(
        &self,
        engine: &dyn Engine,
        commit_version: Version,
        table_config: &TableConfiguration,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> KernelResult<ComputedCheckpointAction> {
        let snapshot = self.read_snapshot.as_ref();
        let crc = snapshot.crc_at_version().map(Arc::as_ref);
        let txns_in_crc = crc.is_some_and(SetTransaction::complete_in_crc);
        let domains_in_crc = crc.is_some_and(DomainMetadata::complete_in_crc);
        // The checkpoint action is only needed for a kind the CRC doesn't hold in full.
        let action = if txns_in_crc && domains_in_crc {
            None
        } else {
            snapshot.latest_checkpoint_action(engine)?
        };
        let txn_base = CheckpointEntryBase::new(txns_in_crc, action.as_deref());
        let domain_base = CheckpointEntryBase::new(domains_in_crc, action.as_deref());
        let (log_changes, num_log_commits_replayed) =
            match txn_base.action().or(domain_base.action()) {
                Some(action) => {
                    let changes = changes_after_checkpoint_action(engine, snapshot, action)?;
                    (changes, snapshot.version() - action.checkpoint_version()?)
                }
                None => (CrcDelta::default(), 0),
            };

        let log_root = LogRoot::new(snapshot.table_root().clone())?;
        let mut transactions = checkpoint_entries(
            engine,
            snapshot,
            txn_base,
            &log_root,
            log_changes.set_transactions,
            set_transactions,
        )?;
        let domain_metadata = checkpoint_entries(
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
        transactions
            .entries
            .retain(|_, txn| !txn.is_expired(expiration_timestamp));

        let version = version_as_i64(commit_version)?;
        let size = i64::try_from(self.file.size)
            .map_err(|_| KernelError::generic("root manifest file size exceeds i64::MAX"))?;
        let action = CheckpointAction {
            version,
            content_root: ContentRoot::new(self.file.location.to_string(), size, version),
            protocol: table_config.protocol().clone(),
            metadata: table_config.metadata().clone(),
            transactions: transactions.entries.into_values().collect(),
            domain_metadata: domain_metadata.entries.into_values().collect(),
            txn_sidecars: transactions.sidecars,
            domain_metadata_sidecars: domain_metadata.sidecars,
        };
        Ok(ComputedCheckpointAction {
            action,
            txn_source: transactions.source,
            domain_metadata_source: domain_metadata.source,
            num_log_commits_replayed,
            num_txn_sidecars_read: transactions.num_sidecars_read,
            num_domain_metadata_sidecars_read: domain_metadata.num_sidecars_read,
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

/// Where a new checkpoint action's entries of one kind start from.
#[derive(Clone, Copy)]
enum CheckpointEntryBase<'a> {
    /// The CRC, which holds every entry of this kind.
    Crc,
    /// The snapshot's state, because the table has no checkpoint action.
    Snapshot,
    /// The latest checkpoint action.
    Action(&'a CheckpointAction),
}

impl<'a> CheckpointEntryBase<'a> {
    fn new(in_crc: bool, action: Option<&'a CheckpointAction>) -> Self {
        match (in_crc, action) {
            (true, _) => Self::Crc,
            (false, None) => Self::Snapshot,
            (false, Some(action)) => Self::Action(action),
        }
    }

    fn action(self) -> Option<&'a CheckpointAction> {
        match self {
            Self::Action(action) => Some(action),
            Self::Crc | Self::Snapshot => None,
        }
    }
}

/// The `T` entries for a new checkpoint action, the sidecars it keeps, where they came from, and
/// how many of the checkpoint action's sidecars were read to compute them.
struct CheckpointEntries<T> {
    entries: HashMap<String, T>,
    sidecars: Vec<Sidecar>,
    source: CheckpointEntrySource,
    num_sidecars_read: u64,
}

/// Returns the new checkpoint action's `T` entries, the sidecars it keeps, and their source.
///
/// From a [`CheckpointEntryBase::Action`] base, `log_changes` (the log commits after the action)
/// and then `txn_changes` are applied to it. Otherwise `txn_changes` apply to the snapshot's state.
fn checkpoint_entries<T: CheckpointActionEntry>(
    engine: &dyn Engine,
    snapshot: &Snapshot,
    base: CheckpointEntryBase<'_>,
    log_root: &LogRoot,
    log_changes: HashMap<String, T>,
    txn_changes: &[T],
) -> KernelResult<CheckpointEntries<T>> {
    let source = match base {
        CheckpointEntryBase::Action(action) => {
            let mut changes = log_changes;
            changes.extend(keyed(txn_changes));
            return fold_into_checkpoint_action(engine, action, log_root, changes);
        }
        // A complete CRC doesn't say what changed since the checkpoint action, so keeping the
        // action's sidecars would mean reading them to find touched entries.
        CheckpointEntryBase::Crc => CheckpointEntrySource::Crc,
        CheckpointEntryBase::Snapshot => CheckpointEntrySource::Snapshot,
    };
    let mut entries = T::read_from_snapshot(snapshot, engine)?;
    T::apply(&mut entries, keyed(txn_changes).collect());
    Ok(CheckpointEntries {
        entries,
        sidecars: Vec::new(),
        source,
        num_sidecars_read: 0,
    })
}

/// Applies `changes` to `action`'s inline `T` entries, keeping its sidecars unless a change
/// touches an entry stored in one.
fn fold_into_checkpoint_action<T: CheckpointActionEntry>(
    engine: &dyn Engine,
    action: &CheckpointAction,
    log_root: &LogRoot,
    changes: HashMap<String, T>,
) -> KernelResult<CheckpointEntries<T>> {
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
    let num_sidecars_read = match not_inline().next() {
        Some(_) => sidecars.len() as u64,
        None => 0,
    };
    if num_sidecars_read > 0 {
        let mut action_entries = T::read_from_action(engine, action, log_root)?;
        if not_inline().any(|key| action_entries.contains_key(key)) {
            T::apply(&mut action_entries, changes);
            return Ok(CheckpointEntries {
                entries: action_entries,
                sidecars: Vec::new(),
                source: CheckpointEntrySource::SidecarsInlined,
                num_sidecars_read,
            });
        }
    }
    T::apply(&mut inline, changes);
    Ok(CheckpointEntries {
        entries: inline,
        sidecars: sidecars.to_vec(),
        source: CheckpointEntrySource::CheckpointAction,
        num_sidecars_read,
    })
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
    use crate::metrics::{MetricEvent, TableType};
    use crate::schema::schema_ref;
    use crate::snapshot::Snapshot;
    use crate::transaction::create_table::create_table;
    use crate::transaction::{Transaction, UpdateTableOperation};
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        adaptive_metadata_protocol_and_metadata, domain, domain_metadata_line,
        minimal_checkpoint_action, setup_table, txn_line, write_commit,
        write_commit_after_manifest_commit, write_domain_metadata_sidecar, write_manifest_commit,
        write_txn_sidecar,
    };
    use crate::unit_test_utils::{
        CapturingReporter, MockTableConfigurationBuilder, assert_result_error_with_message,
        install_thread_local_metrics_reporter, parse_json_batch, string_array_to_engine_data,
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
        let checkpoint = compute_next(&manifest, &engine, &dm_changes, &set_transactions)?.action;

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

        let checkpoint = compute_next(&manifest, &engine, &[], &[])?.action;
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
    ) -> Result<ComputedCheckpointAction> {
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
        compute_at_latest(&engine, &table_root, &[], set_transactions)
    }

    fn compute_at_latest(
        engine: &SyncEngine,
        table_root: &Url,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> Result<ComputedCheckpointAction> {
        let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
        let manifest = root_manifest(table_root, "metadata/root.parquet", 2048, snapshot);
        Ok(compute_next(
            &manifest,
            engine,
            dm_changes,
            set_transactions,
        )?)
    }

    /// Computes the checkpoint action of the commit right after `manifest`'s read snapshot.
    fn compute_next(
        manifest: &RootManifestFile,
        engine: &SyncEngine,
        dm_changes: &[DomainMetadata],
        set_transactions: &[SetTransaction],
    ) -> KernelResult<ComputedCheckpointAction> {
        let snapshot = &manifest.read_snapshot;
        manifest.compute_checkpoint_action(
            engine,
            snapshot.version() + 1,
            snapshot.table_configuration(),
            dm_changes,
            set_transactions,
        )
    }

    /// A write transaction on the latest snapshot that sets `txns` and `domains`.
    fn transaction(
        engine: &SyncEngine,
        table_root: &Url,
        txns: &[SetTransaction],
        domains: &[DomainMetadata],
    ) -> Result<Transaction> {
        let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
        let mut txn = snapshot
            .update_table(UpdateTableOperation::Write, "test", true)
            .build(engine, Box::new(FileSystemCommitter::new()))?;
        for set_transaction in txns {
            txn = txn.with_transaction_id(set_transaction.app_id.clone(), set_transaction.version);
        }
        for dm in domains {
            let name = dm.domain().to_string();
            txn = if dm.is_removed() {
                txn.with_domain_metadata_removed(name)
            } else {
                txn.with_domain_metadata(name, dm.configuration().to_string())
            };
        }
        Ok(txn)
    }

    /// Commits `txns` and `domains` in one transaction on the latest snapshot, if there are any.
    fn commit_transaction(
        engine: &SyncEngine,
        table_root: &Url,
        txns: &[SetTransaction],
        domains: &[DomainMetadata],
    ) -> Result<()> {
        if txns.is_empty() && domains.is_empty() {
            return Ok(());
        }
        transaction(engine, table_root, txns, domains)?
            .commit(engine)?
            .unwrap_committed();
        Ok(())
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
        let num_log_commits = (log_domains.len() + log_txns.len()) as u64;
        let computed = checkpoint_action_after(log_domains, log_txns, &[])?;
        assert_eq!(computed.num_log_commits_replayed, num_log_commits);
        // The action has no sidecars, so even changes to keys that aren't inline read none.
        assert_eq!(
            (
                computed.num_txn_sidecars_read,
                computed.num_domain_metadata_sidecars_read
            ),
            (0, 0)
        );
        let mut checkpoint = computed.action;
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
            checkpoint_action_after(vec![], vec![txn("app-1", 7)], &[txn("app-1", 9)])?.action;
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
        let Some(message) = expected_error else {
            // The checkpoint action at version 2 holds no txns, so `app` comes from the commits
            // after it.
            assert_eq!(result?.action.transactions, [txn("app", 4)]);
            return Ok(());
        };
        assert_result_error_with_message(result, message);

        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let root_manifest = manifest_file(&format!("{table_root}metadata/root-v5.parquet"), 1024)?;
        let commit = Snapshot::builder_for(table_root.clone())
            .build(&engine)?
            .update_table(UpdateTableOperation::Write, "test", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .with_root_manifest_file(root_manifest)?
            .commit(&engine);
        assert_result_error_with_message(commit, message);
        let events = reporter.events();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, MetricEvent::CheckpointActionComputed(_))),
            "a failed computation reports no event: {events:?}"
        );
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
                existing.txn_sidecars = vec![write_txn_sidecar(
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
        let checkpoint = manifest
            .compute_checkpoint_action(
                &engine,
                commit_version,
                &table_config,
                &[],
                &set_transactions,
            )?
            .action;

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
        let checkpoint = compute_next(
            &manifest,
            &engine,
            std::slice::from_ref(&new_domain_metadata),
            std::slice::from_ref(&new_transaction),
        )?
        .action;

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

    /// Which kinds a [`compute_checkpoint_action_reuses_or_inlines_sidecars`] row changes.
    #[derive(Clone, Copy, PartialEq)]
    enum Changed {
        Txns,
        Domains,
        Both,
    }

    /// A set transaction or domain metadata entry that a test can build, log and spill to a
    /// sidecar. An entry's value is the txn version or the domain configuration.
    trait TestEntry: CheckpointActionEntry {
        const NAME: &'static str;
        fn at(key: &str, value: i64) -> Self;
        fn value(&self) -> i64;
        fn line(&self) -> String;
        fn write_sidecar(
            engine: &SyncEngine,
            table_root: &Url,
            name: &str,
            entries: Vec<Self>,
        ) -> Sidecar;
        fn set(checkpoint: &mut CheckpointAction, inline: Vec<Self>, sidecars: Vec<Sidecar>);
    }

    impl TestEntry for SetTransaction {
        const NAME: &'static str = "txn";
        fn at(key: &str, value: i64) -> Self {
            txn(key, value)
        }
        fn value(&self) -> i64 {
            self.version
        }
        fn line(&self) -> String {
            txn_line(&self.app_id, self.version)
        }
        fn write_sidecar(
            engine: &SyncEngine,
            table_root: &Url,
            name: &str,
            entries: Vec<Self>,
        ) -> Sidecar {
            write_txn_sidecar(engine, table_root, name, entries)
        }
        fn set(checkpoint: &mut CheckpointAction, inline: Vec<Self>, sidecars: Vec<Sidecar>) {
            checkpoint.transactions = inline;
            checkpoint.txn_sidecars = sidecars;
        }
    }

    impl TestEntry for DomainMetadata {
        const NAME: &'static str = "domains";
        fn at(key: &str, value: i64) -> Self {
            domain(key, &value.to_string())
        }
        fn value(&self) -> i64 {
            let configuration = self.configuration();
            configuration.parse().unwrap_or_else(|_| {
                panic!(
                    "domain {} has non-integer configuration {configuration:?}",
                    self.domain()
                )
            })
        }
        fn line(&self) -> String {
            domain_metadata_line(self.clone())
        }
        fn write_sidecar(
            engine: &SyncEngine,
            table_root: &Url,
            name: &str,
            entries: Vec<Self>,
        ) -> Sidecar {
            write_domain_metadata_sidecar(engine, table_root, name, entries)
        }
        fn set(checkpoint: &mut CheckpointAction, inline: Vec<Self>, sidecars: Vec<Sidecar>) {
            checkpoint.domain_metadata = inline;
            checkpoint.domain_metadata_sidecars = sidecars;
        }
    }

    /// One row of [`compute_checkpoint_action_reuses_or_inlines_sidecars`]: keys per source, and
    /// the entries and sidecars expected after it.
    #[derive(Clone, Copy)]
    struct SidecarRow {
        before: &'static [&'static str],
        inline: &'static [&'static str],
        sidecars: &'static [&'static [&'static str]],
        logged: &'static [&'static str],
        changed: &'static [&'static str],
        /// The (key, value) pairs the new checkpoint action holds inline.
        expected_inline: &'static [(&'static str, i64)],
        sidecars_kept: bool,
        sidecars_read: bool,
    }

    // Each source writes a distinct value, so the assertions show which one won.
    const BEFORE: i64 = 0;
    const EXISTING: i64 = 1;
    const LOGGED: i64 = 2;
    const CHANGED: i64 = 3;

    /// The row a kind runs when the case doesn't change it: existing entries only, nothing logged
    /// or changed.
    const UNTOUCHED: SidecarRow = SidecarRow {
        before: &[],
        inline: &["untouched"],
        sidecars: &[&["untouched-spilled"]],
        logged: &[],
        changed: &[],
        expected_inline: &[("untouched", EXISTING)],
        sidecars_kept: true,
        sidecars_read: false,
    };

    /// The `T` side of a row: the action's sidecars, the lines logged before the action, the
    /// entries logged after it, and this transaction's changes. Sidecars that must not be read
    /// point at files that were never written.
    struct KindSetup<T> {
        sidecars: Vec<Sidecar>,
        before: Vec<String>,
        logged: Vec<T>,
        changes: Vec<T>,
    }

    impl SidecarRow {
        fn setup<T: TestEntry>(
            &self,
            engine: &SyncEngine,
            table_root: &Url,
            checkpoint: &mut CheckpointAction,
        ) -> KindSetup<T> {
            let at = |keys: &[&str], value| keys.iter().map(|key| T::at(key, value)).collect_vec();
            let sidecars = (self.sidecars.iter().enumerate())
                .map(|(i, keys)| {
                    let name = format!("{}-{i}.parquet", T::NAME);
                    if self.sidecars_read {
                        T::write_sidecar(engine, table_root, &name, at(keys, EXISTING))
                    } else {
                        missing_sidecar(&name)
                    }
                })
                .collect_vec();
            T::set(checkpoint, at(self.inline, EXISTING), sidecars.clone());
            KindSetup {
                sidecars,
                before: at(self.before, BEFORE).iter().map(T::line).collect(),
                logged: at(self.logged, LOGGED),
                changes: at(self.changed, CHANGED),
            }
        }

        /// Asserts `checkpoint` holds exactly the row's expected `T` entries inline, and keeps or
        /// drops the sidecars as the row expects.
        fn assert_entries<T: TestEntry>(
            &self,
            checkpoint: &CheckpointAction,
            setup: &KindSetup<T>,
        ) {
            let inline = T::inline(checkpoint)
                .iter()
                .map(|entry| (entry.key(), entry.value()))
                .sorted();
            let expected = self.expected_inline.iter().copied().sorted();
            assert_eq!(inline.collect_vec(), expected.collect_vec());
            assert_eq!(
                T::sidecars(checkpoint),
                kept_or_empty(&setup.sidecars, self.sidecars_kept)
            );
        }

        fn expected_source(&self) -> CheckpointEntrySource {
            if self.sidecars_kept {
                CheckpointEntrySource::CheckpointAction
            } else {
                CheckpointEntrySource::SidecarsInlined
            }
        }

        fn expected_sidecars_read(&self) -> u64 {
            if self.sidecars_read {
                self.sidecars.len() as u64
            } else {
                0
            }
        }
    }

    // A change is this transaction's change or a log commit after the action. Existing sidecars
    // are kept unless a change touches a key stored in one; then every sidecar of that kind is
    // read and its entries move inline. Sidecars are read only when a change touches a key that
    // isn't inline. Each entry takes its newest value: this transaction's change, then a commit
    // after the action, then the action itself; commits before the action are ignored. Each row
    // runs for txns, domains, or both; a kind the row doesn't change runs `UNTOUCHED`.
    #[rstest]
    #[case::unchanged(SidecarRow {
        before: &[], inline: &["inline"], sidecars: &[&["kept", "spilled"]], logged: &[],
        changed: &[],
        expected_inline: &[("inline", EXISTING)], sidecars_kept: true, sidecars_read: false,
    })]
    #[case::inline_changed(SidecarRow {
        before: &[], inline: &["inline"], sidecars: &[&["kept", "spilled"]], logged: &[],
        changed: &["inline"],
        expected_inline: &[("inline", CHANGED)], sidecars_kept: true, sidecars_read: false,
    })]
    #[case::inline_logged(SidecarRow {
        before: &[], inline: &["inline"], sidecars: &[&["kept", "spilled"]], logged: &["inline"],
        changed: &[],
        expected_inline: &[("inline", LOGGED)], sidecars_kept: true, sidecars_read: false,
    })]
    #[case::transaction_overrides_log_commit(SidecarRow {
        before: &[], inline: &["inline"], sidecars: &[&["kept", "spilled"]], logged: &["inline"],
        changed: &["inline"],
        expected_inline: &[("inline", CHANGED)], sidecars_kept: true, sidecars_read: false,
    })]
    #[case::new_entry(SidecarRow {
        before: &[], inline: &["inline"], sidecars: &[&["kept", "spilled"]], logged: &[],
        changed: &["new"],
        expected_inline: &[("inline", EXISTING), ("new", CHANGED)],
        sidecars_kept: true, sidecars_read: true,
    })]
    #[case::logged_new_entry(SidecarRow {
        before: &[], inline: &["inline"], sidecars: &[&["kept", "spilled"]], logged: &["new"],
        changed: &[],
        expected_inline: &[("inline", EXISTING), ("new", LOGGED)],
        sidecars_kept: true, sidecars_read: true,
    })]
    #[case::sidecar_entry(SidecarRow {
        before: &[], inline: &["inline"], sidecars: &[&["kept", "spilled"]], logged: &[],
        changed: &["spilled"],
        expected_inline: &[("inline", EXISTING), ("kept", EXISTING), ("spilled", CHANGED)],
        sidecars_kept: false, sidecars_read: true,
    })]
    #[case::logged_sidecar_entry(SidecarRow {
        before: &[], inline: &["inline"], sidecars: &[&["kept", "spilled"]], logged: &["spilled"],
        changed: &[],
        expected_inline: &[("inline", EXISTING), ("kept", EXISTING), ("spilled", LOGGED)],
        sidecars_kept: false, sidecars_read: true,
    })]
    #[case::two_sidecars_kept(SidecarRow {
        before: &[], inline: &[], sidecars: &[&["a"], &["b"]], logged: &[], changed: &["new"],
        expected_inline: &[("new", CHANGED)], sidecars_kept: true, sidecars_read: true,
    })]
    #[case::one_of_two_sidecars_touched(SidecarRow {
        before: &[], inline: &[], sidecars: &[&["a"], &["b"]], logged: &[], changed: &["b"],
        expected_inline: &[("a", EXISTING), ("b", CHANGED)],
        sidecars_kept: false, sidecars_read: true,
    })]
    #[case::logged_before_action(SidecarRow {
        before: &["stale"], inline: &["inline"], sidecars: &[&["kept"]], logged: &["logged"],
        changed: &[],
        expected_inline: &[("inline", EXISTING), ("logged", LOGGED)],
        sidecars_kept: true, sidecars_read: true,
    })]
    #[case::logged_before_action_at_tip(SidecarRow {
        before: &["stale"], inline: &["inline"], sidecars: &[&["kept"]], logged: &[],
        changed: &[],
        expected_inline: &[("inline", EXISTING)], sidecars_kept: true, sidecars_read: false,
    })]
    #[case::empty_action_ignores_entries_before_it(SidecarRow {
        before: &["dropped"], inline: &[], sidecars: &[], logged: &[], changed: &[],
        expected_inline: &[], sidecars_kept: true, sidecars_read: false,
    })]
    fn compute_checkpoint_action_reuses_or_inlines_sidecars(
        #[case] row: SidecarRow,
        #[values(Changed::Txns, Changed::Domains, Changed::Both)] changed: Changed,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let (txns_change, domains_change) = (changed != Changed::Domains, changed != Changed::Txns);
        // Entries logged before the action share one commit at version 1.
        let manifest_version = if row.before.is_empty() { 1 } else { 2 };
        let path = format!("metadata/root-v{manifest_version}.parquet");
        let mut existing = minimal_checkpoint_action(&path, manifest_version)?;
        let txn_row = if txns_change { row } else { UNTOUCHED };
        let domain_row = if domains_change { row } else { UNTOUCHED };
        let txns: KindSetup<SetTransaction> = txn_row.setup(&engine, &table_root, &mut existing);
        let domains: KindSetup<DomainMetadata> =
            domain_row.setup(&engine, &table_root, &mut existing);
        if !row.before.is_empty() {
            let before = txns.before.iter().chain(&domains.before);
            let data = parse_json_batch(before.map(String::as_str).collect_vec().into());
            write_commit(&engine, &table_root, 1, data)?;
        }
        write_manifest_commit(&engine, &table_root, manifest_version, existing)?;
        commit_transaction(&engine, &table_root, &txns.logged, &domains.logged)?;

        // The logged entries share one commit.
        let num_logged = u64::from(!txns.logged.is_empty() || !domains.logged.is_empty());
        let computed = compute_at_latest(&engine, &table_root, &domains.changes, &txns.changes)?;

        txn_row.assert_entries(&computed.action, &txns);
        domain_row.assert_entries(&computed.action, &domains);
        assert_eq!(computed.txn_source, txn_row.expected_source());
        assert_eq!(
            computed.domain_metadata_source,
            domain_row.expected_source()
        );
        assert_eq!(computed.num_log_commits_replayed, num_logged);
        assert_eq!(
            computed.num_txn_sidecars_read,
            txn_row.expected_sidecars_read()
        );
        assert_eq!(
            computed.num_domain_metadata_sidecars_read,
            domain_row.expected_sidecars_read()
        );
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

    // The checkpoint action holds domain `inline` inline and `spilled` in a sidecar. Txns have no
    // tombstones, so removals are only covered for domains.
    #[rstest]
    #[case::sidecar_entry_removed(
        vec![DomainMetadata::remove("spilled".into(), "1".into())],
        false
    )]
    #[case::new_entry_added_then_removed(
        vec![domain("new", "1"), DomainMetadata::remove("new".into(), "1".into())],
        true
    )]
    fn compute_checkpoint_action_applies_domain_removals_over_sidecars(
        #[case] log_domains: Vec<DomainMetadata>,
        #[case] sidecar_kept: bool,
    ) -> Result<()> {
        let (engine, table_root) = setup_table()?;
        let mut existing = minimal_checkpoint_action("metadata/root-v1.parquet", 1)?;
        existing.domain_metadata = vec![domain("inline", "1")];
        existing.domain_metadata_sidecars = vec![write_domain_metadata_sidecar(
            &engine,
            &table_root,
            "domains.parquet",
            [domain("spilled", "1")],
        )];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;
        for log_domain in log_domains {
            commit_transaction(&engine, &table_root, &[], &[log_domain])?;
        }

        let checkpoint = compute_at_latest(&engine, &table_root, &[], &[])?.action;

        assert_eq!(checkpoint.domain_metadata, [domain("inline", "1")]);
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
        let checkpoint = compute_next(&manifest, &engine, &dm_changes, &[])?.action;

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
        #[values(true, false)] txns_complete: bool,
        #[values(true, false)] domains_complete: bool,
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
        let crc = Crc {
            version: built.version(),
            protocol: built.table_configuration().protocol().clone(),
            metadata: built.table_configuration().metadata().clone(),
            set_transaction_state: match txns_complete {
                true => SetTransactionState::Complete(HashMap::from([(
                    transaction.app_id.clone(),
                    transaction.clone(),
                )])),
                false => SetTransactionState::Partial(Default::default()),
            },
            domain_metadata_state: match domains_complete {
                true => DomainMetadataState::Complete(HashMap::from([(
                    domain_metadata.domain().to_string(),
                    domain_metadata.clone(),
                )])),
                false => DomainMetadataState::Partial(Default::default()),
            },
            ..Default::default()
        };
        let snapshot = Arc::new(Snapshot::new_with_crc(
            built.log_segment().clone(),
            built.table_configuration().clone(),
            Some(Arc::new(crc)),
            true,
            false,
        )?);
        let manifest = root_manifest(&table_root, "metadata/root-v3.parquet", 1024, snapshot);
        let computed = compute_next(&manifest, &engine, &[], &[])?;

        assert_eq!(computed.action.domain_metadata, [domain_metadata]);
        assert_eq!(computed.action.transactions, [transaction]);
        let source = |complete| match complete {
            true => CheckpointEntrySource::Crc,
            false => CheckpointEntrySource::Snapshot,
        };
        assert_eq!(computed.txn_source, source(txns_complete));
        assert_eq!(computed.domain_metadata_source, source(domains_complete));
        assert_eq!(computed.num_log_commits_replayed, 0);
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

        let checkpoint = compute_at_latest(&engine, &table_root, &[], &[])?.action;

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
        let data = parse_json_batch(logged.iter().map(String::as_str).collect_vec().into());
        write_commit_after_manifest_commit(&engine, &table_root, 2, 1, data)?;

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
        let manifest = root_manifest(&table_root, "metadata/root-v4.parquet", 1024, snapshot);
        let computed = compute_next(&manifest, &engine, &[], &[])?;

        let source = |complete| match complete {
            true => CheckpointEntrySource::Crc,
            false => CheckpointEntrySource::CheckpointAction,
        };
        assert_eq!(computed.txn_source, source(txns_complete));
        assert_eq!(computed.domain_metadata_source, source(domains_complete));
        // Both logged entries share the commit after the action.
        let replayed = if txns_complete && domains_complete {
            0
        } else {
            1
        };
        assert_eq!(computed.num_log_commits_replayed, replayed);
        let checkpoint = computed.action;
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
        // Two txn sidecars and one domain sidecar, so a txn/domain mix-up in the metric shows.
        existing.txn_sidecars = ["kept", "kept2"]
            .map(|key| {
                let name = format!("txn-{key}.parquet");
                write_txn_sidecar(&engine, &table_root, &name, [txn(key, 1)])
            })
            .into();
        existing.domain_metadata_sidecars = vec![write_domain_metadata_sidecar(
            &engine,
            &table_root,
            "domains.parquet",
            [domain("kept", "1")],
        )];
        write_manifest_commit(&engine, &table_root, 1, existing.clone())?;

        let root_manifest = manifest_file(&format!("{table_root}metadata/root-v2.parquet"), 1024)?;
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        transaction(
            &engine,
            &table_root,
            &[txn("new", 2)],
            &[domain("new", "2")],
        )?
        .with_root_manifest_file(root_manifest)?
        .commit(&engine)?
        .unwrap_committed();

        let events = reporter.events();
        let computed = events
            .iter()
            .find_map(|event| match event {
                MetricEvent::CheckpointActionComputed(e) => Some(e),
                _ => None,
            })
            .expect("a checkpoint action computed event");
        let commit_id = events.iter().find_map(|event| match event {
            MetricEvent::TransactionCommitSuccess(e) => Some(e.operation_id),
            _ => None,
        });
        assert_eq!(Some(computed.operation_id), commit_id);
        assert_eq!(computed.commit_version, 2);
        assert_eq!(computed.txn_source, CheckpointEntrySource::CheckpointAction);
        assert_eq!(
            computed.domain_metadata_source,
            CheckpointEntrySource::CheckpointAction
        );
        assert_eq!(computed.num_log_commits_replayed, 0);
        assert_eq!(computed.table_type, TableType::PathBased);
        assert_eq!(computed.num_txn_sidecars_kept, 2);
        assert_eq!(computed.num_domain_metadata_sidecars_kept, 1);
        // `new` isn't inline, so every sidecar is read to check whether it holds `new`.
        assert_eq!(computed.num_txn_sidecars_read, 2);
        assert_eq!(computed.num_domain_metadata_sidecars_read, 1);

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
            ["kept", "kept2", "new"]
        );
        let domains = snapshot
            .reader()
            .get_domain_metadatas_internal(&engine, None)?;
        assert_eq!(domains["kept"], domain("kept", "1"));
        assert_eq!(domains["new"], domain("new", "2"));
        Ok(())
    }
}
