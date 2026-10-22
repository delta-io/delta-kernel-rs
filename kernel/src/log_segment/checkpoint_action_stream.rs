//! Log-replay batches read from an adaptiveMetadata (AMT) `checkpoint` action.

use std::iter;

use super::action_projector::ActionProjector;
use crate::actions::{CheckpointAction, ADD_NAME, LOG_ADD_SCHEMA, REMOVE_NAME};
use crate::cancellation::CancellationTokenRef;
use crate::content_tree::{read_add_actions_from_amt, ReadContext};
use crate::log_replay::ActionsBatch;
use crate::path::LogRoot;
use crate::schema::SchemaRef;
use crate::utils::require;
use crate::{Engine, KernelError, KernelResult, KernelResultIteratorStatic};

/// Reads the actions `read_schema` asks for from `checkpoint`, as checkpoint batches
/// (`is_log_batch = false`) matching `read_schema`. `log_root` resolves the action's file
/// references, and `cancellation_token` stops their reads.
///
/// `add` rows come from the action's content tree. A checkpoint holds reconciled state, so `remove`
/// rows are never produced.
pub(super) fn read_checkpoint_action(
    engine: &dyn Engine,
    checkpoint: &CheckpointAction,
    log_root: &LogRoot,
    read_schema: SchemaRef,
    cancellation_token: Option<&CancellationTokenRef>,
) -> KernelResult<KernelResultIteratorStatic<ActionsBatch>> {
    let mut reads_adds = false;
    for field in read_schema.fields() {
        match field.name().as_str() {
            ADD_NAME => reads_adds = true,
            // With adaptiveMetadata, a `remove` is applied during replay and then dropped, so no
            // checkpoint or reconciled state holds one. Change data feed reads removes from the
            // log commits instead.
            REMOVE_NAME => {}
            other => {
                return Err(KernelError::unsupported(format!(
                    "reading `{other}` actions from an adaptiveMetadata checkpoint action is not \
                     yet supported"
                )))
            }
        }
    }
    if !reads_adds {
        return Ok(Box::new(iter::empty()));
    }
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
        // TODO(#2866): inherit each file's commit timestamp instead of a non-zero constant.
        modification_time: 1,
        data_change: false,
    };
    let projector =
        ActionProjector::try_new(engine, ADD_NAME, LOG_ADD_SCHEMA.clone(), read_schema)?;
    let adds = read_add_actions_from_amt(
        engine,
        checkpoint,
        log_root.table_root(),
        &ctx,
        cancellation_token,
    )?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::{
        get_commit_schema, Add, ContentRoot, ADD_SCHEMA, LOG_TXN_SCHEMA, REMOVE_FIELD,
        SET_TRANSACTION_NAME,
    };
    use crate::arrow::array::{AsArray as _, RecordBatch};
    use crate::arrow::datatypes::Int64Type;
    use crate::content_tree::test_utils::{added_data_entry, write_manifest};
    use crate::content_tree::{ContentTreeNodeEntry, TrackingStatus};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::tests::add_checkpoint_to_store;
    use crate::log_segment::LogSegment;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::scan::test_utils::add_batch_simple;
    use crate::scan::{CHECKPOINT_READ_SCHEMA, COMMIT_READ_SCHEMA};
    use crate::schema::{schema_ref, StructType};
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        adaptive_metadata_pm, adaptive_metadata_protocol_and_metadata, checkpoint_action_with_root,
        commit_files, create_adaptive_metadata_table,
    };
    use crate::unit_test_utils::{
        assert_result_error_with_message, create_log_path_with_size,
        test_schema_flat_with_column_mapping, Action, TestCancellationToken,
    };
    use crate::Version;

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
            None,
        )
        .unwrap()
    }

    /// The non-null `<action>.path` leaf values of `batch`, as replay reads them, or none if it
    /// lacks the action column.
    fn action_paths(batch: &RecordBatch, action: &str) -> Vec<String> {
        let Some(column) = batch.column_by_name(action) else {
            return vec![];
        };
        let paths = column
            .as_struct()
            .column_by_name("path")
            .unwrap()
            .as_string::<i32>();
        paths.iter().flatten().map(str::to_string).collect()
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
            None,
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
            read_checkpoint_action(&engine, &checkpoint, &log_root(), read_schema.clone(), None)
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
            &engine,
            &checkpoint,
            &log_root(),
            CHECKPOINT_READ_SCHEMA.clone(),
            Some(&token),
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
                None,
            ),
            "partitioned adaptiveMetadata checkpoint action is not yet supported; partition \
             columns: [\"id\"]",
        );
    }

    #[test]
    fn unsupported_action_kind_errors() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();
        let schema = Arc::new(
            StructType::try_new(
                CHECKPOINT_READ_SCHEMA
                    .fields()
                    .cloned()
                    .chain(LOG_TXN_SCHEMA.fields().cloned()),
            )
            .unwrap(),
        );
        assert_result_error_with_message(
            read_checkpoint_action(&engine, &checkpoint, &log_root(), schema, None),
            &format!("reading `{SET_TRANSACTION_NAME}` actions"),
        );
    }
}
