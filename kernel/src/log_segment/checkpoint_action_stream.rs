//! Log-replay batches read from an adaptiveMetadata (AMT) `checkpoint` action.

use std::iter;
use std::sync::Arc;

use url::Url;

use crate::actions::{
    CheckpointAction, ADD_NAME, LOG_ADD_SCHEMA, LOG_METADATA_SCHEMA, LOG_PROTOCOL_SCHEMA,
    LOG_TXN_SCHEMA, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::content_tree::{read_actions_from_amt, struct_expr_from_schema, ReadContext};
use crate::expressions::{Expression, Scalar};
use crate::log_replay::ActionsBatch;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{
    Engine, EngineData, ExpressionEvaluator, KernelError, KernelResult, KernelResultIteratorStatic,
};

/// The action kinds a read schema may ask a checkpoint action for.
const CHECKPOINT_ACTION_KINDS: &[&str] = &[
    ADD_NAME,
    PROTOCOL_NAME,
    METADATA_NAME,
    SET_TRANSACTION_NAME,
    REMOVE_NAME,
];

/// Reads the actions `read_schema` asks for from `checkpoint`, as checkpoint batches
/// (`is_log_batch = false`) matching `read_schema`.
///
/// `add` rows come from the action's content tree, and `protocol`, `metaData`, and `txn` rows from
/// the action itself. A checkpoint holds reconciled state, so `remove` rows are never produced.
///
/// # Errors
/// Returns an error if `read_schema` asks for an action kind the checkpoint action cannot supply,
/// or if the content tree cannot be read.
pub(super) fn read_checkpoint_action(
    engine: &dyn Engine,
    checkpoint: &CheckpointAction,
    table_root: &Url,
    read_schema: SchemaRef,
) -> KernelResult<KernelResultIteratorStatic<ActionsBatch>> {
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
    if let (Some(_), Some(sidecar)) = (
        read_schema.field(SET_TRANSACTION_NAME),
        checkpoint.txn_sidecars.first(),
    ) {
        return Err(unsupported_sidecar(SET_TRANSACTION_NAME, &sidecar.path));
    }
    let mut streams = Vec::new();
    for field in read_schema.fields() {
        let stream = match field.name().as_str() {
            ADD_NAME => read_tree_adds(engine, checkpoint, table_root, read_schema.clone())?,
            PROTOCOL_NAME => inline_actions(
                engine,
                PROTOCOL_NAME,
                LOG_PROTOCOL_SCHEMA.clone(),
                vec![checkpoint.protocol().clone().into()],
                read_schema.clone(),
            )?,
            METADATA_NAME => inline_actions(
                engine,
                METADATA_NAME,
                LOG_METADATA_SCHEMA.clone(),
                vec![checkpoint.metadata().clone().into()],
                read_schema.clone(),
            )?,
            SET_TRANSACTION_NAME => inline_actions(
                engine,
                SET_TRANSACTION_NAME,
                LOG_TXN_SCHEMA.clone(),
                checkpoint
                    .transactions
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect(),
                read_schema.clone(),
            )?,
            // `remove`, which a checkpoint never holds.
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
    table_root: &Url,
    read_schema: SchemaRef,
) -> KernelResult<KernelResultIteratorStatic<ActionsBatch>> {
    let ctx = ReadContext {
        // Roots written without a timestamp report 0, the value such writers use themselves.
        modification_time: checkpoint.content_root.timestamp().unwrap_or(0),
        data_change: false,
    };
    let projector =
        ActionProjector::try_new(engine, ADD_NAME, LOG_ADD_SCHEMA.clone(), read_schema)?;
    let adds = read_actions_from_amt(engine, checkpoint, table_root, &ctx)?;
    // The tree marks the rows that are not live files only in the selection vector, but replay
    // reads every row whose `add.path` is non-null, so only the selected rows are passed on.
    Ok(Box::new(adds.map(move |batch| {
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
    rows: Vec<Scalar>,
    read_schema: SchemaRef,
) -> KernelResult<KernelResultIteratorStatic<ActionsBatch>> {
    if rows.is_empty() {
        return Ok(Box::new(iter::empty()));
    }
    let projector = ActionProjector::try_new(engine, action, source.clone(), read_schema)?;
    let rows = rows.into_iter().map(|row| vec![row]).collect();
    let data = engine.evaluation_handler().create_many(source, rows)?;
    let batch = ActionsBatch::new(projector.project(data.as_ref())?, false);
    Ok(Box::new(iter::once(Ok(batch))))
}

/// Projects batches of one top-level action onto a replay read schema, whose other top-level
/// fields are null.
struct ActionProjector {
    evaluator: Arc<dyn ExpressionEvaluator>,
}

impl ActionProjector {
    /// Builds a projector from `source`, the schema of the batches to project, onto `read_schema`.
    /// Fields of `read_schema.<action>` that `source.<action>` has are copied; every other nullable
    /// field is null.
    ///
    /// # Errors
    /// Returns an error if `action` is not a struct in both schemas, or if `read_schema` has a
    /// non-nullable field `source` cannot supply.
    fn try_new(
        engine: &dyn Engine,
        action: &str,
        source: SchemaRef,
        read_schema: SchemaRef,
    ) -> KernelResult<Self> {
        let source_fields = action_struct(&source, action)?;
        let action_expr = struct_expr_from_schema(action_struct(&read_schema, action)?, |name| {
            source_fields
                .field(name)
                .is_some()
                .then(|| Expression::column([action, name]))
        })?;
        let expr = struct_expr_from_schema(&read_schema, |name| {
            (name == action).then(|| action_expr.clone())
        })?;
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            source,
            Arc::new(expr),
            read_schema.as_ref().clone().into(),
        )?;
        Ok(ActionProjector { evaluator })
    }

    fn project(&self, data: &dyn EngineData) -> KernelResult<Box<dyn EngineData>> {
        self.evaluator.evaluate(data)
    }
}

/// The `action` struct of `schema`.
fn action_struct<'a>(schema: &'a StructType, action: &str) -> KernelResult<&'a StructType> {
    match schema.field(action).map(StructField::data_type) {
        Some(DataType::Struct(fields)) => Ok(fields),
        _ => Err(KernelError::internal_error(format!(
            "schema has no `{action}` struct: {schema:?}"
        ))),
    }
}

// TODO(#2866): read sidecars once the adaptiveMetadata sidecar file format is defined. Until then
// a checkpoint action that spills `action` rows into one fails rather than silently dropping them.
fn unsupported_sidecar(action: &str, path: &str) -> KernelError {
    KernelError::unsupported(format!(
        "reading `{action}` actions from adaptiveMetadata checkpoint sidecar `{path}` is not yet \
         supported"
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use test_utils::add_commit;

    use super::*;
    use crate::actions::visitors::{SetTransactionMap, SetTransactionVisitor};
    use crate::actions::{
        get_commit_schema, Metadata, Protocol, SetTransaction, Sidecar, ADD_SCHEMA,
        DOMAIN_METADATA_NAME, LOG_DOMAIN_METADATA_SCHEMA, REMOVE_FIELD,
    };
    use crate::arrow::array::{AsArray as _, RecordBatch};
    use crate::arrow::datatypes::Int64Type;
    use crate::content_tree::test_utils::{added_data_entry, write_manifest};
    use crate::content_tree::TrackingStatus;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::tests::add_checkpoint_to_store;
    use crate::log_segment::LogSegment;
    use crate::object_store::memory::InMemory;
    use crate::scan::test_utils::add_batch_simple;
    use crate::scan::{CHECKPOINT_READ_SCHEMA, COMMIT_READ_SCHEMA};
    use crate::schema::{schema_ref, StructType};
    use crate::unit_test_utils::adaptive_metadata_fixtures::checkpoint_action_with_root;
    use crate::unit_test_utils::assert_result_error_with_message;
    use crate::RowVisitor as _;

    const TABLE_ROOT: &str = "memory:///";
    const ROOT_PATH: &str = "metadata/root.parquet";

    fn table_root() -> Url {
        Url::parse(TABLE_ROOT).unwrap()
    }

    fn add_line(path: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":1,"modificationTime":1,"dataChange":true}}}}"#
        )
    }

    fn remove_line(path: &str) -> String {
        format!(r#"{{"remove":{{"path":"{path}","deletionTimestamp":1,"dataChange":true}}}}"#)
    }

    /// Commits `a` and `b` at versions 0 and 1, then adds `c` and removes `a` at version 2.
    async fn commits_after_two_adds(store: &Arc<InMemory>) {
        let commits = [
            add_line("a.parquet"),
            add_line("b.parquet"),
            format!("{}\n{}", add_line("c.parquet"), remove_line("a.parquet")),
        ];
        for (version, commit) in commits.into_iter().enumerate() {
            add_commit(TABLE_ROOT, store.as_ref(), version as u64, commit)
                .await
                .unwrap();
        }
    }

    fn log_segment(engine: &SyncEngine) -> LogSegment {
        LogSegment::for_snapshot_impl(
            engine.storage_handler().as_ref(),
            table_root().join("_delta_log/").unwrap(),
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

    #[tokio::test]
    async fn reads_commits_after_checkpoint_version_then_tree_adds() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        commits_after_two_adds(&store).await;
        let gone = {
            let mut entry = added_data_entry("gone.parquet", 1, 1, 0, 0);
            entry.tracking.status = TrackingStatus::Deleted;
            entry
        };
        let root = write_manifest(
            &engine,
            table_root().join(ROOT_PATH).unwrap(),
            &[
                added_data_entry("a.parquet", 1, 1, 0, 0),
                gone,
                added_data_entry("b.parquet", 1, 1, 1, 1),
            ],
        );
        let mut checkpoint = checkpoint_action_with_root(ROOT_PATH, root.size as i64, 1).unwrap();
        checkpoint.content_root = checkpoint.content_root.with_timestamp(Some(77));
        let log_segment = log_segment(&engine);

        let batches = read_paths(
            &engine,
            &log_segment,
            &checkpoint,
            COMMIT_READ_SCHEMA.clone(),
            CHECKPOINT_READ_SCHEMA.clone(),
        )
        .unwrap();

        assert_eq!(
            batches,
            vec![
                (true, strings(&["c.parquet"]), strings(&["a.parquet"])),
                (false, strings(&["a.parquet", "b.parquet"]), vec![]),
            ]
        );
    }

    #[rstest]
    #[case::content_root_timestamp(Some(77), 77)]
    #[case::no_content_root_timestamp(None, 0)]
    fn tree_adds_take_modification_time_from_content_root(
        #[case] timestamp: Option<i64>,
        #[case] expected: i64,
    ) {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let root = write_manifest(
            &engine,
            table_root().join(ROOT_PATH).unwrap(),
            &[added_data_entry("a.parquet", 1, 1, 0, 0)],
        );
        let mut checkpoint = checkpoint_action_with_root(ROOT_PATH, root.size as i64, 1).unwrap();
        checkpoint.content_root = checkpoint.content_root.with_timestamp(timestamp);

        let batches: Vec<_> = read_checkpoint_action(
            &engine,
            &checkpoint,
            &table_root(),
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
        assert_eq!(
            modification_time.as_primitive::<Int64Type>().value(0),
            expected
        );
        assert!(!data_change.value(0));
    }

    #[tokio::test]
    async fn schema_without_adds_reads_no_manifest() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        commits_after_two_adds(&store).await;
        // The root manifest does not exist, so reading it would fail.
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();
        let removes = schema_ref! { (&REMOVE_FIELD) };

        let batches = read_paths(
            &engine,
            &log_segment(&engine),
            &checkpoint,
            removes.clone(),
            removes,
        )
        .unwrap();

        assert_eq!(batches, vec![(true, vec![], strings(&["a.parquet"]))]);
    }

    #[tokio::test]
    async fn classic_checkpoint_at_or_after_checkpoint_action_is_read_instead() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        commits_after_two_adds(&store).await;
        add_checkpoint_to_store(
            &store,
            add_batch_simple(get_commit_schema().clone()),
            "00000000000000000001.checkpoint.parquet",
        )
        .await
        .unwrap();
        // The root manifest does not exist, so reading it would fail.
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();

        let batches = read_paths(
            &engine,
            &log_segment(&engine),
            &checkpoint,
            COMMIT_READ_SCHEMA.clone(),
            CHECKPOINT_READ_SCHEMA.clone(),
        )
        .unwrap();

        assert_eq!(
            batches,
            vec![
                (true, strings(&["c.parquet"]), strings(&["a.parquet"])),
                (
                    false,
                    strings(&[
                        "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
                    ]),
                    vec![]
                ),
            ]
        );
    }

    #[test]
    fn tree_adds_project_onto_a_subset_read_schema_with_removes() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let gone = {
            let mut entry = added_data_entry("gone.parquet", 1, 1, 0, 0);
            entry.tracking.status = TrackingStatus::Deleted;
            entry
        };
        let root = write_manifest(
            &engine,
            table_root().join(ROOT_PATH).unwrap(),
            &[
                added_data_entry("a.parquet", 1, 1, 0, 0),
                gone,
                added_data_entry("b.parquet", 1, 1, 1, 1),
            ],
        );
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, root.size as i64, 1).unwrap();
        let read_schema = schema_ref! {
            nullable ADD_NAME: { ..(ADD_SCHEMA.fields().filter(|f| f.name() != "stats")) },
            (&REMOVE_FIELD),
        };

        let batches: Vec<_> =
            read_checkpoint_action(&engine, &checkpoint, &table_root(), read_schema.clone())
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
    fn action_projector_rejects_read_schema_without_the_action() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let read_schema = schema_ref! { (&REMOVE_FIELD) };
        assert_result_error_with_message(
            ActionProjector::try_new(&engine, ADD_NAME, LOG_ADD_SCHEMA.clone(), read_schema),
            "schema has no `add` struct",
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
            read_checkpoint_action(&engine, &checkpoint, &table_root(), read_schema)
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
        assert_eq!(
            Metadata::try_new_from_data(protocol.actions.as_ref()).unwrap(),
            None
        );
    }

    #[test]
    fn transactions_come_from_the_checkpoint_action() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        // The root manifest does not exist, so reading it would fail.
        let mut checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();
        let txn = |app_id: &str, version| SetTransaction {
            app_id: app_id.to_string(),
            version,
            last_updated: None,
        };
        checkpoint.transactions = vec![txn("a", 1), txn("b", 2)];

        let mut visitor = SetTransactionVisitor::new(None);
        for batch in
            read_checkpoint_action(&engine, &checkpoint, &table_root(), LOG_TXN_SCHEMA.clone())
                .unwrap()
        {
            let batch = batch.unwrap();
            assert!(!batch.is_log_batch);
            visitor.visit_rows_of(batch.actions.as_ref()).unwrap();
        }

        let expected: SetTransactionMap = [("a", txn("a", 1)), ("b", txn("b", 2))]
            .into_iter()
            .map(|(app_id, txn)| (app_id.to_string(), txn))
            .collect();
        assert_eq!(visitor.set_transactions, expected);
    }

    #[test]
    fn transaction_sidecar_is_unsupported_before_any_read() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        // The root manifest does not exist, so reading it first would fail differently.
        let mut checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();
        checkpoint.txn_sidecars = vec![Sidecar::new("txn.parquet".to_string(), 1, 1, None)];
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
            read_checkpoint_action(&engine, &checkpoint, &table_root(), schema),
            "checkpoint sidecar `txn.parquet`",
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
                    .chain(LOG_DOMAIN_METADATA_SCHEMA.fields().cloned()),
            )
            .unwrap(),
        );
        assert_result_error_with_message(
            read_checkpoint_action(&engine, &checkpoint, &table_root(), schema),
            &format!("reading `{DOMAIN_METADATA_NAME}` actions"),
        );
    }
}
