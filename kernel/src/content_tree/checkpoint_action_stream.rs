//! Log-replay batches read from an adaptiveMetadata (AMT) `checkpoint` action.

use std::iter;

use super::action_projector::ActionProjector;
use super::checkpoint_sidecar::read_sidecar_actions;
use super::reader::{read_add_actions_from_amt, ReadContext};
use crate::actions::{
    CheckpointAction, Sidecar, ADD_NAME, DOMAIN_METADATA_NAME, LOG_ADD_SCHEMA,
    LOG_DOMAIN_METADATA_SCHEMA, LOG_METADATA_SCHEMA, LOG_PROTOCOL_SCHEMA, LOG_TXN_SCHEMA,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::expressions::Scalar;
use crate::log_replay::ActionsBatch;
use crate::path::LogRoot;
use crate::schema::SchemaRef;
use crate::utils::require;
use crate::{Engine, KernelError, KernelResult, KernelResultIterator};

/// The action kinds a read schema may ask a checkpoint action for.
const CHECKPOINT_ACTION_KINDS: &[&str] = &[
    ADD_NAME,
    PROTOCOL_NAME,
    METADATA_NAME,
    SET_TRANSACTION_NAME,
    DOMAIN_METADATA_NAME,
    REMOVE_NAME,
];

/// Reads the actions `read_schema` asks for from `checkpoint`, as checkpoint batches
/// (`is_log_batch = false`) matching `read_schema`. `log_root` resolves the action's file
/// references.
///
/// `add` rows come from the action's content tree, `protocol` and `metaData` rows from the action
/// itself, and `txn` and `domainMetadata` rows from the action and its sidecars of that kind. A
/// checkpoint holds reconciled state, so `remove` rows are never produced.
pub(crate) fn read_checkpoint_action(
    engine: &dyn Engine,
    checkpoint: &CheckpointAction,
    log_root: &LogRoot,
    read_schema: SchemaRef,
) -> KernelResult<Box<dyn KernelResultIterator<ActionsBatch>>> {
    if let Some(other) = read_schema
        .fields()
        .map(|field| field.name().as_str())
        .find(|name| !CHECKPOINT_ACTION_KINDS.contains(name))
    {
        return Err(KernelError::unsupported(format!(
            "reading `{other}` actions from an adaptiveMetadata checkpoint action is not yet \
             supported"
        )));
    }
    let mut streams = Vec::new();
    for field in read_schema.fields() {
        let stream = match field.name().as_str() {
            ADD_NAME => read_tree_adds(engine, checkpoint, log_root, read_schema.clone())?,
            PROTOCOL_NAME => inline_actions(
                engine,
                PROTOCOL_NAME,
                LOG_PROTOCOL_SCHEMA.clone(),
                iter::once(checkpoint.protocol().clone()),
                read_schema.clone(),
            )?,
            METADATA_NAME => inline_actions(
                engine,
                METADATA_NAME,
                LOG_METADATA_SCHEMA.clone(),
                iter::once(checkpoint.metadata().clone()),
                read_schema.clone(),
            )?,
            SET_TRANSACTION_NAME => inline_and_sidecar_actions(
                engine,
                log_root,
                SET_TRANSACTION_NAME,
                LOG_TXN_SCHEMA.clone(),
                checkpoint.transactions.iter().cloned(),
                &checkpoint.txn_sidecars,
                read_schema.clone(),
            )?,
            DOMAIN_METADATA_NAME => inline_and_sidecar_actions(
                engine,
                log_root,
                DOMAIN_METADATA_NAME,
                LOG_DOMAIN_METADATA_SCHEMA.clone(),
                checkpoint.domain_metadata.iter().cloned(),
                &checkpoint.domain_metadata_sidecars,
                read_schema.clone(),
            )?,
            // `remove`: with adaptiveMetadata, a remove is applied during replay and then dropped,
            // so no checkpoint or reconciled state holds one. Change data feed reads removes from
            // the log commits instead.
            _ => continue,
        };
        streams.push(stream);
    }
    Ok(Box::new(streams.into_iter().flatten()))
}

/// The `add` actions of `checkpoint`'s content tree, projected onto `read_schema`.
fn read_tree_adds(
    engine: &dyn Engine,
    checkpoint: &CheckpointAction,
    log_root: &LogRoot,
    read_schema: SchemaRef,
) -> KernelResult<Box<dyn KernelResultIterator<ActionsBatch>>> {
    // TODO(#3320): the content tree does not yet carry partition values, so its adds would read
    // with empty `partitionValues`.
    let partition_columns = checkpoint.metadata().partition_columns();
    require!(
        partition_columns.is_empty(),
        KernelError::unsupported(format!(
            "reading adds from a partitioned adaptiveMetadata checkpoint action is not yet \
             supported; partition columns: {partition_columns:?}"
        ))
    );
    let ctx = ReadContext {
        table_root: log_root.table_root().clone(),
        // TODO(#2866): inherit each file's commit timestamp instead of a non-zero constant.
        modification_time: 1,
        data_change: false,
    };
    let projector =
        ActionProjector::try_new(engine, ADD_NAME, LOG_ADD_SCHEMA.clone(), read_schema)?;
    // The engine's handlers carry any cancellation token bound to this read.
    let adds = read_add_actions_from_amt(engine, checkpoint, &ctx, None)?;
    Ok(Box::new(adds.map(move |batch| {
        // `ActionsBatch` does not take a `FilteredEngineData` yet, so drop the deselected rows
        // here; otherwise replay would read them as live adds.
        let selected = batch?.apply_selection_vector()?;
        Ok(ActionsBatch::new(
            projector.project(selected.as_ref())?,
            false,
        ))
    })))
}

/// One batch of the `action` rows `rows` (each a value of `source`'s single `action` field),
/// projected onto `read_schema`, or no batch when there are no rows.
fn inline_actions(
    engine: &dyn Engine,
    action: &str,
    source: SchemaRef,
    rows: impl IntoIterator<Item = impl Into<Scalar>>,
    read_schema: SchemaRef,
) -> KernelResult<Box<dyn KernelResultIterator<ActionsBatch>>> {
    let rows: Vec<_> = rows.into_iter().map(|row| vec![row.into()]).collect();
    if rows.is_empty() {
        return Ok(Box::new(iter::empty()));
    }
    let projector = ActionProjector::try_new(engine, action, source.clone(), read_schema)?;
    let data = engine.evaluation_handler().create_many(source, rows)?;
    let batch = ActionsBatch::new(projector.project(data.as_ref())?, false);
    Ok(Box::new(iter::once(Ok(batch))))
}

/// The `action` rows held inline (`rows`, each a value of `source`'s single `action` field) and in
/// `sidecars`, projected onto `read_schema`.
fn inline_and_sidecar_actions(
    engine: &dyn Engine,
    log_root: &LogRoot,
    action: &str,
    source: SchemaRef,
    rows: impl IntoIterator<Item = impl Into<Scalar>>,
    sidecars: &[Sidecar],
    read_schema: SchemaRef,
) -> KernelResult<Box<dyn KernelResultIterator<ActionsBatch>>> {
    let inline = inline_actions(engine, action, source.clone(), rows, read_schema.clone())?;
    let spilled = read_sidecar_actions(engine, log_root, sidecars, action, source, read_schema)?;
    Ok(Box::new(inline.chain(spilled)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::{
        get_commit_schema, Add, ContentRoot, DomainMetadata, Metadata, Protocol, SetTransaction,
        ADD_SCHEMA, COMMIT_INFO_NAME, REMOVE_FIELD,
    };
    use crate::arrow::array::{AsArray as _, RecordBatch};
    use crate::arrow::datatypes::Int64Type;
    use crate::content_tree::test_utils::{action_paths, added_data_entry, write_manifest};
    use crate::content_tree::{ContentTreeNodeEntry, TrackingStatus};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::engine::CancellableEngine;
    use crate::log_segment::tests::add_checkpoint_to_store;
    use crate::log_segment::LogSegment;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::scan::test_utils::add_batch_simple;
    use crate::scan::{CHECKPOINT_READ_SCHEMA, COMMIT_READ_SCHEMA};
    use crate::schema::schema_ref;
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        adaptive_metadata_pm, adaptive_metadata_protocol_and_metadata, checkpoint_action_with_root,
        commit_files, create_adaptive_metadata_table, write_sidecar,
    };
    use crate::unit_test_utils::{
        assert_result_error_with_message, create_log_path_with_size,
        test_schema_flat_with_column_mapping, Action, TestCancellationToken,
    };
    use crate::{CancellationTokenRef, Version};

    const TABLE_ROOT: &str = "memory:///";
    const ROOT_PATH: &str = "metadata/root.parquet";
    /// The version [`two_adds_then_add_and_remove`] has committed `a` and `b` by.
    const TWO_ADDS_VERSION: Version = 2;

    fn table_root() -> Url {
        Url::parse(TABLE_ROOT).unwrap()
    }

    fn log_root() -> LogRoot {
        LogRoot::new(table_root()).unwrap()
    }

    /// An adaptiveMetadata table that adds `a` and `b` (by [`TWO_ADDS_VERSION`]), then adds `c`
    /// and removes `a`.
    fn two_adds_then_add_and_remove(store: &Arc<InMemory>) -> SyncEngine {
        let engine = SyncEngine::new_with_store(store.clone());
        create_adaptive_metadata_table(&engine, &table_root(), &[]);
        commit_files(&engine, &table_root(), &["a.parquet"], &[]);
        commit_files(&engine, &table_root(), &["b.parquet"], &[]);
        commit_files(&engine, &table_root(), &["c.parquet"], &["a.parquet"]);
        engine
    }

    /// Root entries for `a` and `b`, with a deleted `gone` between them that is not a live file.
    fn tree_with_deleted_entry() -> [ContentTreeNodeEntry; 3] {
        let mut gone = added_data_entry("gone.parquet", 1, 1, 0, 0);
        gone.tracking.status = TrackingStatus::Deleted;
        [
            added_data_entry("a.parquet", 1, 1, 0, 0),
            gone,
            added_data_entry("b.parquet", 1, 1, 1, 1),
        ]
    }

    /// A checkpoint action at `version` whose root manifest holds `entries`.
    fn checkpoint_with_tree(
        engine: &SyncEngine,
        entries: &[ContentTreeNodeEntry],
        version: Version,
    ) -> CheckpointAction {
        let root = write_manifest(engine, table_root().join(ROOT_PATH).unwrap(), entries);
        checkpoint_action_with_root(ROOT_PATH, root.size as i64, version).unwrap()
    }

    fn log_segment(engine: &SyncEngine) -> LogSegment {
        LogSegment::for_snapshot_impl(
            engine.storage_handler().as_ref(),
            log_root().log_root().clone(),
            vec![],
            None,
            None,
        )
        .unwrap()
    }

    /// A read batch's `(is_log_batch, add paths, remove paths)`.
    type BatchPaths = (bool, Vec<String>, Vec<String>);

    /// Reads `log_segment` with `checkpoint`, returning each batch's [`BatchPaths`].
    fn read_paths(
        engine: &SyncEngine,
        log_segment: &LogSegment,
        checkpoint: &CheckpointAction,
        commit_schema: SchemaRef,
        checkpoint_schema: SchemaRef,
    ) -> KernelResult<Vec<BatchPaths>> {
        log_segment
            .read_actions_with_checkpoint_action(
                engine,
                checkpoint,
                &table_root(),
                commit_schema,
                checkpoint_schema,
                None,
            )?
            .actions
            .map(|batch| {
                let ActionsBatch {
                    actions,
                    is_log_batch,
                } = batch?;
                let batch = actions.try_into_record_batch()?;
                Ok((
                    is_log_batch,
                    action_paths(&batch, ADD_NAME),
                    action_paths(&batch, REMOVE_NAME),
                ))
            })
            .collect()
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    /// The batches a read of [`two_adds_then_add_and_remove`] yields with a checkpoint action at
    /// [`TWO_ADDS_VERSION`] over [`tree_with_deleted_entry`]: the later commit, then the tree.
    fn commit_then_tree_batches() -> Vec<BatchPaths> {
        vec![
            (true, strings(&["c.parquet"]), strings(&["a.parquet"])),
            (false, strings(&["a.parquet", "b.parquet"]), vec![]),
        ]
    }

    #[test]
    fn reads_commits_after_checkpoint_version_then_tree_adds() {
        let store = Arc::new(InMemory::new());
        let engine = two_adds_then_add_and_remove(&store);
        let checkpoint =
            checkpoint_with_tree(&engine, &tree_with_deleted_entry(), TWO_ADDS_VERSION);

        let batches = read_paths(
            &engine,
            &log_segment(&engine),
            &checkpoint,
            COMMIT_READ_SCHEMA.clone(),
            CHECKPOINT_READ_SCHEMA.clone(),
        )
        .unwrap();

        assert_eq!(batches, commit_then_tree_batches());
    }

    /// A compacted delta that starts at or before the checkpoint action's version spans it and is
    /// never read; one that starts after it is.
    #[rstest]
    #[case::spans_checkpoint_version(
        TWO_ADDS_VERSION,
        TWO_ADDS_VERSION,
        commit_then_tree_batches()
    )]
    #[case::starts_after_checkpoint_version(
        TWO_ADDS_VERSION - 1,
        TWO_ADDS_VERSION,
        vec![
            (true, strings(&["compacted.parquet"]), vec![]),
            (false, strings(&["a.parquet", "b.parquet"]), vec![]),
        ]
    )]
    #[tokio::test]
    async fn compacted_delta_is_read_only_when_it_starts_after_checkpoint_version(
        #[case] checkpoint_version: Version,
        #[case] compaction_start: Version,
        #[case] expected: Vec<BatchPaths>,
    ) {
        let store = Arc::new(InMemory::new());
        let engine = two_adds_then_add_and_remove(&store);
        let checkpoint =
            checkpoint_with_tree(&engine, &tree_with_deleted_entry(), checkpoint_version);
        let name = format!(
            "{compaction_start:020}.{:020}.compacted.json",
            TWO_ADDS_VERSION + 1
        );
        let compacted = serde_json::to_string(&Action::Add(Add {
            path: "compacted.parquet".to_string(),
            size: 1,
            modification_time: 1,
            data_change: true,
            ..Default::default()
        }))
        .unwrap();
        let path = Path::from(format!("_delta_log/{name}"));
        store.put(&path, compacted.clone().into()).await.unwrap();
        let mut log_segment = log_segment(&engine);
        // Listing skips compacted deltas while log compaction is disabled (#2337).
        log_segment
            .listed
            .ascending_compaction_files
            .push(create_log_path_with_size(
                log_root().log_root().join(&name).unwrap().as_str(),
                compacted.len() as u64,
            ));

        let batches = read_paths(
            &engine,
            &log_segment,
            &checkpoint,
            COMMIT_READ_SCHEMA.clone(),
            CHECKPOINT_READ_SCHEMA.clone(),
        )
        .unwrap();

        assert_eq!(batches, expected);
    }

    #[test]
    fn tree_adds_have_constant_modification_time_and_no_data_change() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let checkpoint =
            checkpoint_with_tree(&engine, &[added_data_entry("a.parquet", 1, 1, 0, 0)], 1);

        let batches: Vec<_> = read_checkpoint_action(
            &engine,
            &checkpoint,
            &log_root(),
            CHECKPOINT_READ_SCHEMA.clone(),
        )
        .unwrap()
        .map(|batch| batch.unwrap().actions.try_into_record_batch().unwrap())
        .collect();

        let [batch] = batches.as_slice() else {
            panic!("expected one batch, got {}", batches.len());
        };
        let add = batch.column_by_name(ADD_NAME).unwrap().as_struct();
        let modification_time = add.column_by_name("modificationTime").unwrap();
        let data_change = add.column_by_name("dataChange").unwrap().as_boolean();
        assert_eq!(modification_time.as_primitive::<Int64Type>().value(0), 1);
        assert!(!data_change.value(0));
    }

    #[test]
    fn schema_without_adds_reads_no_manifest() {
        let store = Arc::new(InMemory::new());
        let engine = two_adds_then_add_and_remove(&store);
        // The root manifest does not exist, so reading it would fail.
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, TWO_ADDS_VERSION).unwrap();
        let removes = schema_ref! { (&REMOVE_FIELD) };

        let batches = read_paths(
            &engine,
            &log_segment(&engine),
            &checkpoint,
            removes.clone(),
            removes,
        )
        .unwrap();

        // The only batch is the log batch of the commit after the checkpoint action's version,
        // which removes `a`. The checkpoint action yields no batch, since a checkpoint holds no
        // removes and the schema asks for nothing else.
        assert_eq!(batches, vec![(true, vec![], strings(&["a.parquet"]))]);
    }

    #[rstest]
    #[tokio::test]
    async fn checkpoint_file_at_or_after_checkpoint_action_is_unsupported(
        #[values(TWO_ADDS_VERSION, TWO_ADDS_VERSION + 1)] file_version: Version,
    ) {
        let store = Arc::new(InMemory::new());
        let engine = two_adds_then_add_and_remove(&store);
        add_checkpoint_to_store(
            &store,
            add_batch_simple(get_commit_schema().clone()),
            &format!("{file_version:020}.checkpoint.parquet"),
        )
        .await
        .unwrap();
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, TWO_ADDS_VERSION).unwrap();

        let result = read_paths(
            &engine,
            &log_segment(&engine),
            &checkpoint,
            COMMIT_READ_SCHEMA.clone(),
            CHECKPOINT_READ_SCHEMA.clone(),
        );

        assert_result_error_with_message(
            result,
            &format!("standalone checkpoint at version {file_version} is not yet supported"),
        );
    }

    #[rstest]
    #[case::negative(-1, -1, "version -1 is outside the log segment's versions 0..=3")]
    #[case::after_segment_end(4, 4, "version 4 is outside the log segment's versions 0..=3")]
    #[case::older_content_root(2, 1, "content root version 1 is older than its version 2")]
    fn rejects_checkpoint_action_versions_it_cannot_read(
        #[case] version: i64,
        #[case] content_root_version: i64,
        #[case] message: &str,
    ) {
        let store = Arc::new(InMemory::new());
        let engine = two_adds_then_add_and_remove(&store);
        let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
        let checkpoint = CheckpointAction::new(
            version,
            ContentRoot::new(ROOT_PATH.to_string(), 1, content_root_version),
            protocol,
            metadata,
            vec![],
            vec![],
        );

        let result = read_paths(
            &engine,
            &log_segment(&engine),
            &checkpoint,
            COMMIT_READ_SCHEMA.clone(),
            CHECKPOINT_READ_SCHEMA.clone(),
        );

        assert_result_error_with_message(result, message);
    }

    #[test]
    fn tree_adds_project_onto_a_subset_read_schema_with_removes() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let checkpoint = checkpoint_with_tree(&engine, &tree_with_deleted_entry(), 1);
        let read_schema = schema_ref! {
            nullable ADD_NAME: { ..(ADD_SCHEMA.fields().filter(|f| f.name() != "stats")) },
            (&REMOVE_FIELD),
        };

        let batches: Vec<_> =
            read_checkpoint_action(&engine, &checkpoint, &log_root(), read_schema.clone())
                .unwrap()
                .map(|batch| batch.unwrap().actions.try_into_record_batch().unwrap())
                .collect();

        let [batch] = batches.as_slice() else {
            panic!("expected one batch, got {}", batches.len());
        };
        let field_names: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let expected_names: Vec<_> = read_schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(field_names, expected_names);
        assert_eq!(
            action_paths(batch, ADD_NAME),
            strings(&["a.parquet", "b.parquet"])
        );
        assert_eq!(batch.column_by_name(REMOVE_NAME).unwrap().null_count(), 2);
    }

    #[test]
    fn cancelled_read_stops() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let checkpoint = checkpoint_with_tree(&engine, &tree_with_deleted_entry(), 1);
        let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
        let result = read_checkpoint_action(
            &CancellableEngine::new(&engine, token),
            &checkpoint,
            &log_root(),
            CHECKPOINT_READ_SCHEMA.clone(),
        );
        assert!(matches!(result, Err(KernelError::Cancelled)));
    }

    #[test]
    fn partitioned_checkpoint_action_rejects_add_reads() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let (protocol, metadata) =
            adaptive_metadata_pm(test_schema_flat_with_column_mapping(), &["id"]);
        let checkpoint = CheckpointAction::new(
            1,
            ContentRoot::new(ROOT_PATH.to_string(), 1, 1),
            protocol,
            metadata,
            vec![],
            vec![],
        );
        assert_result_error_with_message(
            read_checkpoint_action(
                &engine,
                &checkpoint,
                &log_root(),
                CHECKPOINT_READ_SCHEMA.clone(),
            ),
            "partitioned adaptiveMetadata checkpoint action is not yet supported; partition \
             columns: [\"id\"]",
        );
    }

    #[test]
    fn protocol_and_metadata_come_from_the_checkpoint_action() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        // The root manifest does not exist, so reading it would fail.
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();
        let read_schema = get_commit_schema()
            .project(&[PROTOCOL_NAME, METADATA_NAME])
            .unwrap();

        let batches: Vec<_> =
            read_checkpoint_action(&engine, &checkpoint, &log_root(), read_schema)
                .unwrap()
                .map(|batch| batch.unwrap())
                .collect();

        let [protocol, metadata] = batches.as_slice() else {
            panic!("expected two batches, got {}", batches.len());
        };
        assert!(!protocol.is_log_batch && !metadata.is_log_batch);
        assert_eq!(
            Protocol::try_new_from_data(protocol.actions.as_ref()).unwrap(),
            Some(checkpoint.protocol().clone())
        );
        assert_eq!(
            Metadata::try_new_from_data(metadata.actions.as_ref()).unwrap(),
            Some(checkpoint.metadata().clone())
        );
    }

    /// Sets a checkpoint action's inline entries of one action kind, by key, and its sidecars of
    /// that kind.
    type SetActions = fn(&mut CheckpointAction, &[&str], Vec<Sidecar>);

    /// Sets the inline `txn` entries of `checkpoint` to `app_ids` and its `txn` sidecars to
    /// `sidecars`.
    fn set_transactions(
        checkpoint: &mut CheckpointAction,
        app_ids: &[&str],
        sidecars: Vec<Sidecar>,
    ) {
        checkpoint.transactions = app_ids.iter().map(|id| transaction(id)).collect();
        checkpoint.txn_sidecars = sidecars;
    }

    fn transaction(app_id: &str) -> SetTransaction {
        SetTransaction::new(app_id.to_string(), 1, None)
    }

    /// Sets the inline `domainMetadata` entries of `checkpoint` to `domains` and its
    /// `domainMetadata` sidecars to `sidecars`.
    fn set_domain_metadata(
        checkpoint: &mut CheckpointAction,
        domains: &[&str],
        sidecars: Vec<Sidecar>,
    ) {
        checkpoint.domain_metadata = domains.iter().map(|d| domain_metadata(d)).collect();
        checkpoint.domain_metadata_sidecars = sidecars;
    }

    fn domain_metadata(domain: &str) -> DomainMetadata {
        DomainMetadata::new(domain.to_string(), format!("{domain}-cfg"))
    }

    /// The non-null `<action>.<key>` values of `batch`.
    fn action_keys(batch: &RecordBatch, action: &str, key: &str) -> Vec<String> {
        let action = batch.column_by_name(action).unwrap().as_struct();
        let keys = action.column_by_name(key).unwrap().as_string::<i32>();
        keys.iter().flatten().map(str::to_string).collect()
    }

    #[rstest]
    #[case::transactions(
        SET_TRANSACTION_NAME,
        LOG_TXN_SCHEMA.clone(),
        "appId",
        set_transactions as SetActions,
        |id: &str| transaction(id).into(),
    )]
    #[case::domain_metadata(
        DOMAIN_METADATA_NAME,
        LOG_DOMAIN_METADATA_SCHEMA.clone(),
        "domain",
        set_domain_metadata as SetActions,
        |domain: &str| domain_metadata(domain).into(),
    )]
    fn actions_come_from_the_checkpoint_action_and_its_sidecars(
        #[case] action: &str,
        #[case] source_schema: SchemaRef,
        #[case] key: &str,
        #[case] set_actions: SetActions,
        #[case] sidecar_row: fn(&str) -> Scalar,
    ) {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        // The root manifest does not exist, so reading it would fail.
        let mut checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();
        let sidecar = write_sidecar(
            &engine,
            &table_root(),
            "spilled.parquet",
            source_schema.clone(),
            vec![sidecar_row("c")],
        );
        set_actions(&mut checkpoint, &["a", "b"], vec![sidecar]);

        let keys: Vec<_> = read_checkpoint_action(&engine, &checkpoint, &log_root(), source_schema)
            .unwrap()
            .flat_map(|batch| {
                let batch = batch.unwrap();
                assert!(!batch.is_log_batch);
                action_keys(&batch.actions.try_into_record_batch().unwrap(), action, key)
            })
            .collect();

        assert_eq!(keys, ["a", "b", "c"]);
    }

    #[test]
    fn unsupported_action_kind_errors() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();
        let schema = get_commit_schema()
            .project(&[ADD_NAME, COMMIT_INFO_NAME])
            .unwrap();
        assert_result_error_with_message(
            read_checkpoint_action(&engine, &checkpoint, &log_root(), schema),
            &format!("reading `{COMMIT_INFO_NAME}` actions"),
        );
    }
}
