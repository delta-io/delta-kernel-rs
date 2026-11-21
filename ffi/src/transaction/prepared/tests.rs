use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use delta_kernel::actions::{DomainMetadata, Metadata, Protocol, get_commit_schema};
use delta_kernel::committer::{CommitMetadata, Committer, FileSystemCommitter};
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::engine::sync::plan::{ScopedSyncPlanExecutor, SyncPlanExecutor};
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::plans::proto::{operation, plan, schema};
use delta_kernel::plans::{PlanBuilder, RelationRef};
use delta_kernel::schema::{SchemaRef, StructType, schema_ref};
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{EngineData, Operation, PlanExecutor};
use delta_kernel_default_engine::DefaultEngineBuilder;
use prost::Message;
use rstest::rstest;

use super::*;
use crate::commit_range::FfiCommitFile;
use crate::delta_types::{FfiMetadata, FfiProtocol};
use crate::error::{EngineExecError, EngineExecResult};
use crate::ffi_test_utils::{TEST_ERROR_ALLOCATORS, ok_or_panic, recover_error};
use crate::plans::iter::CEngineDataIterator;
use crate::plans::result::CPlanResult;
use crate::plans::scoped::{
    free_relation_ref, free_scoped_plan_executor, get_scoped_plan_executor,
};
use crate::transaction::committer::*;
use crate::{
    ExclusiveEngineData, FfiSlice, KernelBytesSlice, KernelStringSlice, NullableCvoid,
    engine_to_handle,
};

struct Context {
    engine: Arc<dyn delta_kernel::Engine>,
    snapshot: SnapshotRef,
    calls: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    failures: AtomicUsize,
    outcome: u8,
}

extern "C" fn commit(
    context: NullableCvoid,
    actions: FfiSlice<FfiCommitRelation>,
    metadata: *const FfiCommitMetadata,
    out: *mut EngineExecResult<FfiCommitResponse>,
) {
    let context = unsafe { &*context.unwrap().as_ptr().cast::<Context>() };
    let metadata = unsafe { metadata.as_ref() }.unwrap();
    context.calls.fetch_add(1, Ordering::Relaxed);
    let actions = unsafe { actions.try_as_slice() }.unwrap();
    assert_eq!(actions.len(), 2);
    assert_relation_views(actions);
    let protocol = Option::<&FfiProtocol>::from(&metadata.read_protocol).unwrap();
    let read_metadata = Option::<&FfiMetadata>::from(&metadata.read_metadata).unwrap();
    assert_eq!(
        unsafe { protocol.try_to_kernel() }.unwrap(),
        *context.snapshot.table_configuration().protocol()
    );
    assert_eq!(
        unsafe { read_metadata.try_to_kernel() }.unwrap(),
        *context.snapshot.table_configuration().metadata()
    );
    assert!(matches!(&metadata.new_protocol, OptionalValue::None));
    assert!(matches!(&metadata.new_metadata, OptionalValue::None));
    assert!(
        unsafe { metadata.domain_metadata.try_as_slice() }
            .unwrap()
            .is_empty()
    );
    let result = match context.outcome {
        0 => EngineExecResult::Success(FfiCommitResponse::Conflict {
            version: metadata.version,
        }),
        1 => EngineExecResult::Success(FfiCommitResponse::Retryable(EngineExecError {
            etype: FFIKernelError::IOErrorError,
            message: Box::new("rejected before acceptance".to_string()).into(),
        })),
        2 => EngineExecResult::Failure(EngineExecError {
            etype: FFIKernelError::IOErrorError,
            message: Box::new("acceptance unknown".to_string()).into(),
        }),
        _ => return,
    };
    unsafe { out.write(result) }
}

extern "C" fn history(
    context: NullableCvoid,
    root: KernelStringSlice,
    version: u64,
    out: *mut EngineExecResult<Handle<SharedWinningCommits>>,
) {
    let context = unsafe { &*context.unwrap().as_ptr().cast::<Context>() };
    context.reads.fetch_add(1, Ordering::Relaxed);
    if context.outcome == 3 {
        return;
    }
    if context
        .failures
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
        .is_ok()
    {
        unsafe {
            out.write(EngineExecResult::Failure(EngineExecError {
                etype: FFIKernelError::IOErrorError,
                message: Box::new("history unavailable".to_string()).into(),
            }))
        };
        return;
    }
    let root = unsafe { root.try_to_string() }.unwrap();
    let commits = FileSystemCommitter::new()
        .get_winning_commits(context.engine.as_ref(), &root.parse().unwrap(), version)
        .unwrap();
    let paths: Vec<delta_kernel::path::ParsedLogPath> =
        commits.iter().map(|c| c.log_path.clone().into()).collect();
    let views: Vec<_> = commits
        .iter()
        .zip(&paths)
        .map(|(commit, path)| FfiCommitFile {
            version: commit.version,
            file: crate::log_path::FfiLogPath::from_parsed(path),
        })
        .collect();
    let engine = engine_to_handle(context.engine.clone(), TEST_ERROR_ALLOCATORS);
    let handle = unsafe {
        ok_or_panic(create_winning_commits(
            KernelStringSlice::new_unsafe(&root),
            version,
            commits.end_version(),
            FfiSlice::new_unsafe(&views),
            engine.shallow_copy(),
        ))
    };
    unsafe {
        crate::free_engine(engine);
        out.write(EngineExecResult::Success(handle));
    }
}

extern "C" fn free(context: NullableCvoid) {
    let context = unsafe { Box::from_raw(context.unwrap().as_ptr().cast::<Context>()) };
    context.drops.fetch_add(1, Ordering::Relaxed);
}

extern "C" fn publish(
    context: NullableCvoid,
    version: u64,
    commits: FfiSlice<FfiCommitToPublish>,
    out: *mut EngineExecResult<bool>,
) {
    let context = unsafe { &*context.unwrap().as_ptr().cast::<Context>() };
    assert_eq!(version, 1);
    let commits = unsafe { commits.try_as_slice() }.unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].version, 1);
    assert!(
        unsafe { commits[0].source.try_to_string() }
            .unwrap()
            .contains("_staged_commits/")
    );
    assert!(
        unsafe { commits[0].destination.try_to_string() }
            .unwrap()
            .ends_with("00000000000000000001.json")
    );
    let result = match context.outcome {
        0 => EngineExecResult::Success(true),
        1 => EngineExecResult::Success(false),
        2 => EngineExecResult::Failure(EngineExecError {
            etype: FFIKernelError::IOErrorError,
            message: Box::new("publication failed".to_string()).into(),
        }),
        _ => return,
    };
    unsafe { out.write(result) }
}

extern "C" fn visit_attempt(context: NullableCvoid, attempt: CommitAttemptView) {
    let calls = unsafe { &*context.unwrap().as_ptr().cast::<AtomicUsize>() };
    calls.fetch_add(1, Ordering::Relaxed);
    assert_eq!(attempt.version, 1);
    assert!(attempt.timestamp > 0);
    let actions = unsafe { attempt.actions.try_as_slice() }.unwrap();
    assert_eq!(actions.len(), 2);
    assert_relation_views(actions);
}

fn assert_relation_views(actions: &[FfiCommitRelation]) {
    for action in actions {
        assert!(!unsafe { action.id.try_to_string() }.unwrap().is_empty());
        let encoded = unsafe { action.schema.try_as_slice() }.unwrap();
        let schema = StructType::try_from(schema::StructType::decode(encoded).unwrap()).unwrap();
        assert_eq!(&schema, get_commit_schema().as_ref());
    }
}

fn table() -> (Arc<InMemory>, Arc<dyn delta_kernel::Engine>, SnapshotRef) {
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
    let snapshot = create_table(
        "memory:///table/",
        schema_ref! { nullable "id": INTEGER },
        "test",
        true,
    )
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
    .unwrap()
    .commit(engine.as_ref())
    .unwrap()
    .unwrap_post_commit_snapshot();
    (store, engine, snapshot)
}

fn free_view(view: PreparedTransactionView) {
    unsafe {
        free_relation_ref(view.actions);
        if let OptionalValue::Some(snapshot) = view.read_snapshot {
            crate::free_snapshot(snapshot);
        }
    }
}

fn take_optional<T>(value: OptionalValue<T>) -> T {
    match value {
        OptionalValue::Some(value) => value,
        OptionalValue::None => panic!("Expected Some"),
    }
}

struct ScopeContext {
    executor: ScopedSyncPlanExecutor,
    relations: Mutex<HashMap<String, SchemaRef>>,
    drops: Arc<AtomicUsize>,
}

impl ScopeContext {
    fn decode_plan(&self, bytes: KernelBytesSlice) -> plan::Plan {
        let operation =
            operation::Operation::decode(unsafe { bytes.try_as_slice() }.unwrap()).unwrap();
        let Some(operation::operation::Op::QueryPlan(plan)) = operation.op else {
            panic!("The ownership fixture only executes in-memory queries");
        };
        let relations = self.relations.lock().unwrap();
        for node in &plan.nodes {
            if let Some(plan::operator::Op::RelationSource(source)) =
                node.op.as_ref().and_then(|op| op.op.as_ref())
            {
                let schema = StructType::try_from(source.schema.clone().unwrap()).unwrap();
                assert_eq!(relations.get(&source.id).unwrap().as_ref(), &schema);
            }
        }
        plan
    }
}

fn scope_plan_schema(plan: &plan::Plan, index: usize) -> SchemaRef {
    let node = &plan.nodes[index];
    let schema = match node.op.as_ref().unwrap().op.as_ref().unwrap() {
        plan::operator::Op::Values(node) => node.schema.as_ref().unwrap(),
        plan::operator::Op::RelationSource(node) => node.schema.as_ref().unwrap(),
        plan::operator::Op::Aggregate(node) => node.schema.as_ref().unwrap(),
        plan::operator::Op::UnionAll(_) => {
            return scope_plan_schema(plan, node.inputs[0] as usize);
        }
        _ => panic!("Unexpected ownership fixture operator"),
    };
    Arc::new(StructType::try_from(schema.clone()).unwrap())
}

extern "C" fn retain_scope(
    context: NullableCvoid,
    name: KernelStringSlice,
    operation: KernelBytesSlice,
    out: *mut EngineExecResult<Handle<SharedRelationRef>>,
) {
    let context = unsafe { &*context.unwrap().as_ptr().cast::<ScopeContext>() };
    let plan = context.decode_plan(operation);
    let schema = scope_plan_schema(&plan, plan.nodes.len() - 1);
    let name = unsafe { name.try_to_string() }.unwrap();
    // This fixture tests scope ownership with empty file actions; CommitInfo rows are not read.
    let empty = PlanBuilder::values(schema.clone(), vec![])
        .unwrap()
        .build()
        .unwrap();
    let relation = context.executor.execute_and_retain(&name, empty).unwrap();
    context
        .relations
        .lock()
        .unwrap()
        .insert(relation.id().to_string(), schema);
    unsafe { out.write(EngineExecResult::Success(Arc::new(relation).into())) }
}

extern "C" fn execute_scope(
    context: NullableCvoid,
    operation: KernelBytesSlice,
    out: *mut EngineExecResult<CPlanResult>,
) {
    let context = unsafe { &*context.unwrap().as_ptr().cast::<ScopeContext>() };
    let plan = context.decode_plan(operation);
    let schema = scope_plan_schema(&plan, plan.nodes.len() - 1);
    let terminal = plan
        .nodes
        .last()
        .unwrap()
        .op
        .as_ref()
        .unwrap()
        .op
        .as_ref()
        .unwrap();
    let query = match terminal {
        plan::operator::Op::Aggregate(_) => {
            // All file relations are empty, so their count and size summaries are zero.
            let row = schema.fields().map(|_| Scalar::from(0i64)).collect();
            PlanBuilder::values(schema, vec![row]).unwrap()
        }
        plan::operator::Op::RelationSource(source) => {
            PlanBuilder::relation_source(RelationRef::new(source.id.clone(), schema))
        }
        _ => panic!("Unexpected ownership fixture query"),
    };
    let batches = context
        .executor
        .execute_op(Operation::QueryPlan(query.build().unwrap()))
        .unwrap()
        .into_data()
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect::<Vec<_>>()
        .into_iter();
    let iterator = CEngineDataIterator {
        state: NonNull::new(Box::into_raw(Box::new(batches)).cast()),
        next: scope_data_next,
        free: free_scope_data,
    };
    unsafe { out.write(EngineExecResult::Success(CPlanResult::Data(iterator))) }
}

extern "C" fn scope_data_next(
    state: NullableCvoid,
    out: *mut OptionalValue<EngineExecResult<Handle<ExclusiveEngineData>>>,
) {
    let batches = unsafe {
        &mut *state
            .unwrap()
            .as_ptr()
            .cast::<std::vec::IntoIter<Box<dyn EngineData>>>()
    };
    let next = batches
        .next()
        .map(|batch| EngineExecResult::Success(batch.into()));
    unsafe { out.write(next.into()) }
}

extern "C" fn free_scope_data(state: NullableCvoid) {
    drop(unsafe {
        Box::from_raw(
            state
                .unwrap()
                .as_ptr()
                .cast::<std::vec::IntoIter<Box<dyn EngineData>>>(),
        )
    });
}

extern "C" fn free_scope(context: NullableCvoid) {
    let context = unsafe { Box::from_raw(context.unwrap().as_ptr().cast::<ScopeContext>()) };
    context.drops.fetch_add(1, Ordering::Relaxed);
}

fn assert_pending_rows(pending: &Handle<ExclusivePreparedTransaction>) {
    let pending = unsafe { pending.as_ref() };
    assert_scope_rows(pending.executor.as_ref(), pending_relation(pending));
}

fn pending_relation(pending: &FfiPreparedTransaction) -> RelationRef {
    match &pending.state {
        PreparedState::Update(p) => p.get_delta_actions().clone(),
        PreparedState::Create(p) => p.get_delta_actions().clone(),
    }
}

fn assert_scope_rows(executor: &dyn ScopedPlanExecutor, relation: RelationRef) {
    let plan = PlanBuilder::relation_source(relation).build().unwrap();
    let rows: usize = executor
        .execute_op(Operation::QueryPlan(plan))
        .unwrap()
        .into_data()
        .unwrap()
        .map(|batch| batch.unwrap().len())
        .sum();
    assert_eq!(rows, 0);
}

fn free_pending(
    pending: Handle<ExclusivePreparedTransaction>,
    outcome: u8,
    engine: Handle<SharedExternEngine>,
) {
    if outcome >= 2 {
        match unsafe { prepared_transaction_commit(pending, engine) } {
            ExternResult::Err(error) => assert_eq!(
                unsafe { recover_error(error) }.etype,
                FFIKernelError::InvalidTransactionStateError
            ),
            _ => panic!("Unknown attempt must block blind resubmission"),
        }
    } else {
        unsafe { free_prepared_transaction(pending) };
    }
}

struct MetadataContext {
    read_protocol: Option<Protocol>,
    read_metadata: Option<Metadata>,
    new_protocol: Option<Protocol>,
    new_metadata: Option<Metadata>,
    domains: Vec<DomainMetadata>,
    calls: AtomicUsize,
}

extern "C" fn inspect_metadata(
    context: NullableCvoid,
    actions: FfiSlice<FfiCommitRelation>,
    metadata: *const FfiCommitMetadata,
    out: *mut EngineExecResult<FfiCommitResponse>,
) {
    let expected = unsafe { &*context.unwrap().as_ptr().cast::<MetadataContext>() };
    expected.calls.fetch_add(1, Ordering::Relaxed);
    let metadata = unsafe { metadata.as_ref() }.unwrap();
    assert!(unsafe { actions.try_as_slice() }.unwrap().is_empty());
    for (actual, expected) in [
        (&metadata.read_protocol, &expected.read_protocol),
        (&metadata.new_protocol, &expected.new_protocol),
    ] {
        let actual = Option::<&FfiProtocol>::from(actual)
            .map(|value| unsafe { value.try_to_kernel() }.unwrap());
        assert_eq!(&actual, expected);
    }
    for (actual, expected) in [
        (&metadata.read_metadata, &expected.read_metadata),
        (&metadata.new_metadata, &expected.new_metadata),
    ] {
        let actual = Option::<&FfiMetadata>::from(actual)
            .map(|value| unsafe { value.try_to_kernel() }.unwrap());
        assert_eq!(&actual, expected);
    }
    let domains: Vec<_> = unsafe { metadata.domain_metadata.try_as_slice() }
        .unwrap()
        .iter()
        .map(|value| unsafe { value.try_to_kernel() }.unwrap())
        .collect();
    assert_eq!(domains, expected.domains);
    assert_eq!(
        unsafe { metadata.table_root.try_to_string() }.unwrap(),
        "memory:///table/"
    );
    assert_eq!(metadata.version, 1);
    assert!(matches!(
        &metadata.commit_type,
        FfiCommitType::PathBasedWrite
    ));
    assert_eq!(
        unsafe { metadata.published_path.try_to_string() }.unwrap(),
        "memory:///table/_delta_log/00000000000000000001.json"
    );
    assert!(
        unsafe { metadata.staged_path.try_to_string() }
            .unwrap()
            .contains("_staged_commits/")
    );
    unsafe {
        out.write(EngineExecResult::Success(FfiCommitResponse::Conflict {
            version: 1,
        }))
    }
}

extern "C" fn borrowed_context_free(_: NullableCvoid) {}

#[rstest]
fn commit_callback_preserves_typed_metadata_payloads_and_presence(
    #[values(false, true)] changed: bool,
) {
    let metadata = CommitMetadata::new_unchecked_with(
        "memory:///table/".parse().unwrap(),
        1,
        vec![],
        vec!["domainMetadata"],
        HashMap::from([("custom.property".to_string(), "value".to_string())]),
    )
    .unwrap()
    .with_domain_change("test.domain");
    let metadata = if changed {
        metadata.with_protocol_change().with_metadata_change()
    } else {
        metadata
    };
    let pm = metadata.protocol_metadata();
    let expected = MetadataContext {
        read_protocol: pm.read_protocol().cloned(),
        read_metadata: pm.read_metadata().cloned(),
        new_protocol: pm.new_protocol().cloned(),
        new_metadata: pm.new_metadata().cloned(),
        domains: metadata.domain_metadata_changes().to_vec(),
        calls: AtomicUsize::new(0),
    };
    let context = NonNull::new((&expected as *const MetadataContext).cast_mut().cast());
    let committer = unsafe {
        get_declarative_committer(
            context,
            false,
            DeclarativeCommitterCallbacks {
                commit: inspect_metadata,
                get_winning_commits: OptionalValue::None,
                publish: OptionalValue::None,
                free: borrowed_context_free,
            },
        )
    };
    let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new()))).get_scoped();
    let result = unsafe { committer.as_ref() }
        .commit_declarative(&executor, Box::new(std::iter::empty()), metadata)
        .unwrap();
    assert!(matches!(
        result,
        delta_kernel::committer::CommitResponse::Conflict { version: 1 }
    ));
    assert_eq!(expected.calls.load(Ordering::Relaxed), 1);
    unsafe { free_declarative_committer(committer) };
}

#[rstest]
fn committer_publication_callback_validates_acknowledgment_and_releases_context(
    #[values(0, 1, 2, 3)] outcome: u8,
) {
    let (_, engine, snapshot) = table();
    let drops = Arc::new(AtomicUsize::new(0));
    let context = NonNull::new(
        Box::into_raw(Box::new(Context {
            engine: engine.clone(),
            snapshot,
            calls: Arc::new(AtomicUsize::new(0)),
            reads: Arc::new(AtomicUsize::new(0)),
            drops: drops.clone(),
            failures: AtomicUsize::new(0),
            outcome,
        }))
        .cast(),
    );
    let committer = unsafe {
        get_declarative_committer(
            context,
            true,
            DeclarativeCommitterCallbacks {
                commit,
                get_winning_commits: OptionalValue::Some(history),
                free,
                publish: OptionalValue::Some(publish),
            },
        )
    };
    let commit = delta_kernel::committer::CatalogCommit::new_unchecked(
        1,
        "memory:///table/_delta_log/_staged_commits/00000000000000000001.id.json"
            .parse()
            .unwrap(),
        "memory:///table/_delta_log/00000000000000000001.json"
            .parse()
            .unwrap(),
    );
    let metadata = delta_kernel::committer::PublishMetadata::try_new(1, vec![commit]).unwrap();
    let result = unsafe { committer.as_ref() }.publish(engine.as_ref(), metadata);
    assert_eq!(result.is_ok(), outcome == 0);
    if outcome == 2 {
        assert!(matches!(result, Err(KernelError::IOError(_))));
    }
    unsafe { free_declarative_committer(committer) };
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[rstest]
fn declarative_ffi_prepares_and_commits_files_with_an_independently_retained_scope(
    #[values(false, true)] create: bool,
) {
    let (store, rust_engine, snapshot) = table();
    let engine = engine_to_handle(rust_engine, TEST_ERROR_ALLOCATORS);
    let executor = Arc::new(SyncPlanExecutor::new(Some(store)).get_scoped());
    let scope: Arc<dyn ScopedPlanExecutor> = executor.clone();
    let scope: Handle<SharedScopedPlanExecutor> = scope.into();
    let prepared = if create {
        let builder = create_table(
            "memory:///created/",
            schema_ref! { nullable "id": INTEGER },
            "test",
            true,
        );
        let builder: Handle<ExclusiveCreateTableTransactionBuilder> = Box::new(builder).into();
        let mut txn = unsafe {
            ok_or_panic(create_table_txn_builder_build_declarative(
                builder,
                OptionalValue::None,
                engine.shallow_copy(),
            ))
        };
        let schema = unsafe { txn.as_ref() }.add_files_schema().clone();
        let batch =
            test_utils::create_add_files_metadata(&schema, vec![("a.parquet", 100, 10, Some(5))])
                .unwrap()
                .try_into_record_batch()
                .unwrap();
        let relation = executor.create_ref("adds", schema, batch).unwrap();
        let relation: Handle<SharedRelationRef> = Arc::new(relation).into();
        unsafe {
            declarative_create_table_txn_add_files(&mut txn, relation.shallow_copy());
            free_relation_ref(relation);
        }
        unsafe {
            ok_or_panic(declarative_create_table_txn_prepare(
                txn,
                scope.shallow_copy(),
                engine.shallow_copy(),
            ))
        }
    } else {
        let builder = snapshot.update_table(UpdateTableOperation::Write, "test", true);
        let builder: Handle<ExclusiveUpdateTableTransactionBuilder> = Box::new(builder).into();
        let mut txn = unsafe {
            ok_or_panic(update_table_txn_builder_build_declarative(
                builder,
                OptionalValue::None,
                engine.shallow_copy(),
            ))
        };
        let schema = unsafe { txn.as_ref() }.add_files_schema().clone();
        let batch =
            test_utils::create_add_files_metadata(&schema, vec![("a.parquet", 100, 10, Some(5))])
                .unwrap()
                .try_into_record_batch()
                .unwrap();
        let relation = executor.create_ref("adds", schema, batch).unwrap();
        let relation: Handle<SharedRelationRef> = Arc::new(relation).into();
        unsafe {
            declarative_update_table_txn_add_files(&mut txn, relation.shallow_copy());
            free_relation_ref(relation);
        }
        unsafe {
            ok_or_panic(declarative_update_table_txn_prepare(
                txn,
                scope.shallow_copy(),
                engine.shallow_copy(),
            ))
        }
    };
    drop(executor);
    unsafe { free_scoped_plan_executor(scope) };
    let view = unsafe { prepared_transaction_inspect(&prepared) };
    assert_eq!(view.target_version, u64::from(!create));
    assert!(!view.has_unknown_attempt);
    free_view(view);
    let result =
        unsafe { ok_or_panic(prepared_transaction_commit(prepared, engine.shallow_copy())) };
    assert_eq!(
        unsafe { prepared_transaction_commit_result_outcome(&result) },
        PreparedTransactionOutcome::Committed
    );
    assert!(matches!(
        unsafe { prepared_transaction_commit_result_pending(&result) },
        OptionalValue::None
    ));
    assert!(matches!(
        unsafe {
            ok_or_panic(prepared_transaction_commit_result_get_winning_commits(
                &result,
                engine.shallow_copy(),
            ))
        },
        OptionalValue::None
    ));
    let committed = unsafe {
        ok_or_panic(prepared_transaction_commit_result_into_committed(
            result,
            engine.shallow_copy(),
        ))
    };
    assert_eq!(
        unsafe { crate::transaction::committed_transaction_version(&committed) },
        u64::from(!create)
    );
    let post = take_optional(unsafe {
        crate::transaction::committed_transaction_post_commit_snapshot(&committed)
    });
    assert_eq!(unsafe { post.as_ref() }.version(), u64::from(!create));
    unsafe {
        crate::free_snapshot(post);
        crate::transaction::free_committed_transaction(committed);
        crate::free_engine(engine);
    }
}

#[rstest]
#[case::fetched_conflict(0, 0, true)]
#[case::failed_history_then_explicit_retry(0, 1, true)]
#[case::extraction_before_fetch(0, 0, false)]
#[case::failed_history_then_extraction(0, 1, false)]
#[case::retryable(1, 0, false)]
#[case::unknown(2, 0, false)]
#[case::uninitialized_callback(3, 0, false)]
fn ffi_outcomes_preserve_pending_work_and_history_across_ownership_transfers(
    #[case] outcome: u8,
    #[case] failures: usize,
    #[case] fetch_history: bool,
    #[values(false, true)] result_first: bool,
) {
    let (store, rust_engine, snapshot) = table();
    let mut latest = snapshot.clone();
    for _ in 0..3 {
        latest = latest
            .update_table(UpdateTableOperation::Write, "winner", true)
            .build(rust_engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap()
            .commit(rust_engine.as_ref())
            .unwrap()
            .unwrap_post_commit_snapshot();
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let context = NonNull::new(
        Box::into_raw(Box::new(Context {
            engine: rust_engine.clone(),
            snapshot: snapshot.clone(),
            calls: calls.clone(),
            reads: reads.clone(),
            drops: drops.clone(),
            failures: AtomicUsize::new(failures),
            outcome,
        }))
        .cast(),
    );
    let committer = unsafe {
        get_declarative_committer(
            context,
            false,
            DeclarativeCommitterCallbacks {
                commit,
                get_winning_commits: OptionalValue::Some(history),
                free,
                publish: OptionalValue::None,
            },
        )
    };
    let engine = engine_to_handle(rust_engine, TEST_ERROR_ALLOCATORS);
    let scope_drops = Arc::new(AtomicUsize::new(0));
    let scope_context = NonNull::new(
        Box::into_raw(Box::new(ScopeContext {
            executor: SyncPlanExecutor::new(Some(store)).get_scoped(),
            relations: Mutex::new(HashMap::new()),
            drops: scope_drops.clone(),
        }))
        .cast(),
    );
    let scope =
        unsafe { get_scoped_plan_executor(scope_context, execute_scope, retain_scope, free_scope) };
    let builder: Handle<ExclusiveUpdateTableTransactionBuilder> =
        Box::new(snapshot.update_table(UpdateTableOperation::Write, "test", true)).into();
    let txn = unsafe {
        ok_or_panic(update_table_txn_builder_build_declarative(
            builder,
            OptionalValue::Some(committer),
            engine.shallow_copy(),
        ))
    };
    let prepared = unsafe {
        ok_or_panic(declarative_update_table_txn_prepare(
            txn,
            scope.shallow_copy(),
            engine.shallow_copy(),
        ))
    };
    unsafe { free_scoped_plan_executor(scope) };
    assert_eq!(scope_drops.load(Ordering::Relaxed), 0);
    let mut result =
        unsafe { ok_or_panic(prepared_transaction_commit(prepared, engine.shallow_copy())) };
    assert_eq!(scope_drops.load(Ordering::Relaxed), 0);
    let expected = [
        PreparedTransactionOutcome::Conflicted,
        PreparedTransactionOutcome::Retryable,
        PreparedTransactionOutcome::Unknown,
        PreparedTransactionOutcome::Unknown,
    ][usize::from(outcome)];
    assert_eq!(
        unsafe { prepared_transaction_commit_result_outcome(&result) },
        expected
    );
    free_view(take_optional(unsafe {
        prepared_transaction_commit_result_pending(&result)
    }));
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    assert_eq!(
        Option::<u64>::from(unsafe {
            prepared_transaction_commit_result_conflict_version(&result)
        }),
        (outcome == 0).then_some(1)
    );
    if failures > 0 {
        match unsafe {
            prepared_transaction_commit_result_get_winning_commits(&result, engine.shallow_copy())
        } {
            ExternResult::Err(error) => assert!(
                unsafe { recover_error(error) }
                    .message
                    .contains("history unavailable")
            ),
            _ => panic!("Expected explicit discovery to fail"),
        }
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert!(matches!(
            unsafe { prepared_transaction_commit_result_outcome(&result) },
            PreparedTransactionOutcome::Conflicted
        ));
    }
    let retained_history = if fetch_history {
        let winners = take_optional(unsafe {
            ok_or_panic(prepared_transaction_commit_result_get_winning_commits(
                &result,
                engine.shallow_copy(),
            ))
        });
        assert_history(&winners);
        assert_eq!(reads.load(Ordering::Relaxed), failures + 1);
        let second = take_optional(unsafe {
            ok_or_panic(prepared_transaction_commit_result_get_winning_commits(
                &result,
                engine.shallow_copy(),
            ))
        });
        assert_history(&second);
        assert_eq!(reads.load(Ordering::Relaxed), failures + 2);
        unsafe { free_winning_commits(second) };
        Some(winners)
    } else {
        None
    };
    let expected_reads = reads.load(Ordering::Relaxed);
    let mut pending =
        take_optional(unsafe { prepared_transaction_commit_result_take_pending(&mut result) });
    assert_eq!(scope_drops.load(Ordering::Relaxed), 0);
    assert!(matches!(
        unsafe { prepared_transaction_commit_result_take_pending(&mut result) },
        OptionalValue::None
    ));
    assert!(matches!(
        unsafe { prepared_transaction_commit_result_pending(&result) },
        OptionalValue::None
    ));
    let view = unsafe { prepared_transaction_inspect(&pending) };
    assert_eq!(view.target_version, 1);
    assert_eq!(view.has_unknown_attempt, outcome >= 2);
    free_view(view);
    let inspected = AtomicUsize::new(0);
    let context = NonNull::new((&inspected as *const AtomicUsize).cast_mut().cast());
    assert_eq!(
        unsafe { prepared_transaction_visit_unresolved_attempt(&pending, context, visit_attempt) },
        outcome >= 2
    );
    assert_eq!(inspected.load(Ordering::Relaxed), usize::from(outcome >= 2));
    if outcome == 0 {
        for _ in 0..2 {
            let history = unsafe {
                prepared_transaction_commit_result_get_winning_commits(
                    &result,
                    engine.shallow_copy(),
                )
            };
            match history {
                ExternResult::Err(error) => assert_eq!(
                    unsafe { recover_error(error) }.etype,
                    FFIKernelError::InvalidTransactionStateError
                ),
                _ => panic!("History discovery must fail after extraction"),
            }
        }
        if let Some(winners) = &retained_history {
            match unsafe {
                prepared_transaction_rebase(
                    &mut pending,
                    winners.shallow_copy(),
                    engine.shallow_copy(),
                )
            } {
                ExternResult::Err(error) => assert_eq!(
                    unsafe { recover_error(error) }.etype,
                    FFIKernelError::UnsupportedError
                ),
                _ => panic!("Expected unsupported rebase"),
            }
            let view = unsafe { prepared_transaction_inspect(&pending) };
            assert_eq!(view.target_version, 1);
            free_view(view);
        }
    } else {
        assert!(matches!(
            unsafe {
                ok_or_panic(prepared_transaction_commit_result_get_winning_commits(
                    &result,
                    engine.shallow_copy(),
                ))
            },
            OptionalValue::None
        ));
        let error = take_optional(unsafe { prepared_transaction_commit_result_error(&result) });
        assert_eq!(
            error.code,
            if outcome == 3 {
                FFIKernelError::InternalError
            } else {
                FFIKernelError::IOErrorError
            }
        );
        unsafe { free_prepared_transaction_error(error) };
    }
    assert_eq!(reads.load(Ordering::Relaxed), expected_reads);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    if result_first {
        unsafe { free_prepared_transaction_commit_result(result) };
        assert_eq!(scope_drops.load(Ordering::Relaxed), 0);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        assert_pending_rows(&pending);
        free_pending(pending, outcome, unsafe { engine.shallow_copy() });
    } else {
        let relation = pending_relation(unsafe { pending.as_ref() });
        free_pending(pending, outcome, unsafe { engine.shallow_copy() });
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(scope_drops.load(Ordering::Relaxed), 0);
        assert_eq!(reads.load(Ordering::Relaxed), expected_reads);
        assert_scope_rows(unsafe { result.as_ref() }.executor.as_ref(), relation);
        unsafe { free_prepared_transaction_commit_result(result) };
    }
    if let Some(history) = retained_history {
        assert_history(&history);
        unsafe { free_winning_commits(history) };
    }
    assert_eq!(scope_drops.load(Ordering::Relaxed), 1);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    unsafe { crate::free_engine(engine) };
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

fn assert_history(history: &Handle<SharedWinningCommits>) {
    let visits = AtomicUsize::new(0);
    let context = NonNull::new((&visits as *const AtomicUsize).cast_mut().cast());
    unsafe { visit_winning_commits(history.shallow_copy(), context, inspect_history) };
    assert_eq!(visits.load(Ordering::Relaxed), 1);
}

extern "C" fn inspect_history(context: NullableCvoid, commits: FfiSlice<FfiCommitFile>) {
    let visits = unsafe { &*context.unwrap().as_ptr().cast::<AtomicUsize>() };
    visits.fetch_add(1, Ordering::Relaxed);
    let commits = unsafe { commits.try_as_slice() }.unwrap();
    assert_eq!(commits.len(), 3);
    for (commit, version) in commits.iter().zip(1..=3) {
        assert_eq!(commit.version, version);
        assert_eq!(
            unsafe { commit.file.location().try_to_string() }.unwrap(),
            format!("memory:///table/_delta_log/{version:020}.json")
        );
        assert!(commit.file.size() > 0);
    }
}

#[rstest]
#[case::published(false)]
#[case::staged(true)]
fn conflicted_commit_collection_copies_and_preserves_file_descriptors(#[case] staged: bool) {
    let (_, rust_engine, _) = table();
    let engine = engine_to_handle(rust_engine, TEST_ERROR_ALLOCATORS);
    let (commits, expected) = {
        let location = if staged {
            "memory:///table/_delta_log/_staged_commits/00000000000000000001.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json".to_string()
        } else {
            "memory:///table/_delta_log/00000000000000000001.json".to_string()
        };
        let views = vec![FfiCommitFile {
            version: 1,
            file: crate::log_path::FfiLogPath::new(
                unsafe { KernelStringSlice::new_unsafe(&location) },
                1234,
                5678,
            ),
        }];
        let expected = views
            .iter()
            .map(|view| delta_kernel::committer::WinningCommit {
                version: view.version,
                log_path: unsafe { view.file.log_path() }.unwrap(),
            })
            .collect::<Vec<_>>();
        let commits = unsafe {
            ok_or_panic(create_winning_commits(
                KernelStringSlice::new_unsafe("memory:///table/"),
                1,
                1,
                FfiSlice::new_unsafe(&views),
                engine.shallow_copy(),
            ))
        };
        (commits, expected)
    };
    unsafe { crate::free_engine(engine) };
    let mut actual: Vec<delta_kernel::committer::WinningCommit> = Vec::new();
    let context = NonNull::new((&mut actual as *mut Vec<_>).cast());
    unsafe { visit_winning_commits(commits.shallow_copy(), context, copy_history) };
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(&expected) {
        assert_eq!(actual.version, expected.version);
        assert_eq!(actual.log_path, expected.log_path);
    }
    unsafe { free_winning_commits(commits) };
}

extern "C" fn copy_history(context: NullableCvoid, commits: FfiSlice<FfiCommitFile>) {
    let actual = unsafe {
        &mut *context
            .unwrap()
            .as_ptr()
            .cast::<Vec<delta_kernel::committer::WinningCommit>>()
    };
    actual.extend(
        unsafe { commits.try_as_slice() }
            .unwrap()
            .iter()
            .map(|commit| delta_kernel::committer::WinningCommit {
                version: commit.version,
                log_path: unsafe { commit.file.log_path() }.unwrap(),
            }),
    );
}

#[rstest]
#[case::null_nonempty(0, FFIKernelError::GenericError)]
#[case::invalid_log_path(1, FFIKernelError::InvalidLogPath)]
#[case::invalid_utf8(2, FFIKernelError::Utf8Error)]
#[case::null_empty_location(3, FFIKernelError::InvalidUrlError)]
#[case::null_nonempty_location(4, FFIKernelError::GenericError)]
fn conflicted_commit_collection_rejects_invalid_descriptors(
    #[case] input: u8,
    #[case] expected: FFIKernelError,
) {
    let (_, rust_engine, _) = table();
    let engine = engine_to_handle(rust_engine, TEST_ERROR_ALLOCATORS);
    let invalid = [0xffu8];
    let location = if input >= 3 {
        KernelStringSlice {
            ptr: std::ptr::null(),
            len: usize::from(input == 4),
        }
    } else if input == 2 {
        KernelStringSlice {
            ptr: invalid.as_ptr().cast(),
            len: invalid.len(),
        }
    } else {
        unsafe { KernelStringSlice::new_unsafe("memory:///table/not-a-log.json") }
    };
    let views = [FfiCommitFile {
        version: 1,
        file: crate::log_path::FfiLogPath::new(location, 1, 1),
    }];
    let commits = if input == 0 {
        FfiSlice {
            ptr: std::ptr::null(),
            len: 1,
        }
    } else {
        unsafe { FfiSlice::new_unsafe(&views) }
    };
    match unsafe {
        create_winning_commits(
            KernelStringSlice::new_unsafe("memory:///table/"),
            1,
            1,
            commits,
            engine.shallow_copy(),
        )
    } {
        ExternResult::Err(error) => assert_eq!(unsafe { recover_error(error) }.etype, expected),
        _ => panic!("Expected invalid descriptors to fail"),
    }
    unsafe { crate::free_engine(engine) };
}

#[rstest]
#[case::absent(0, FFIKernelError::UnsupportedError)]
#[case::uninitialized(3, FFIKernelError::InternalError)]
#[case::failed(1, FFIKernelError::IOErrorError)]
fn winning_history_callback_reports_missing_or_invalid_results_and_releases_context(
    #[case] outcome: u8,
    #[case] expected: FFIKernelError,
) {
    let (_, engine, snapshot) = table();
    let reads = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let context = NonNull::new(
        Box::into_raw(Box::new(Context {
            engine: engine.clone(),
            snapshot,
            calls: Arc::new(AtomicUsize::new(0)),
            reads: reads.clone(),
            drops: drops.clone(),
            failures: AtomicUsize::new(1),
            outcome,
        }))
        .cast(),
    );
    let committer = unsafe {
        get_declarative_committer(
            context,
            false,
            DeclarativeCommitterCallbacks {
                commit,
                get_winning_commits: if outcome == 0 {
                    OptionalValue::None
                } else {
                    OptionalValue::Some(history)
                },
                publish: OptionalValue::None,
                free,
            },
        )
    };
    let result = unsafe { committer.as_ref() }.get_winning_commits(
        engine.as_ref(),
        &"memory:///table/".parse().unwrap(),
        1,
    );
    assert_eq!(FFIKernelError::from(&result.unwrap_err()), expected);
    assert_eq!(reads.load(Ordering::Relaxed), usize::from(outcome != 0));
    unsafe { free_declarative_committer(committer) };
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[rstest]
#[case::empty(&[], 1, 1)]
#[case::missing_end(&[1], 1, 2)]
#[case::gap(&[1, 3], 1, 3)]
#[case::duplicate(&[1, 1], 1, 1)]
#[case::wrong_start(&[2], 1, 2)]
fn conflicted_commit_collection_rejects_incomplete_history(
    #[case] versions: &[u64],
    #[case] start: u64,
    #[case] end: u64,
) {
    let (_, rust_engine, _) = table();
    let engine = engine_to_handle(rust_engine, TEST_ERROR_ALLOCATORS);
    let locations: Vec<_> = versions
        .iter()
        .map(|v| format!("memory:///table/_delta_log/{v:020}.json"))
        .collect();
    let views: Vec<_> = versions
        .iter()
        .zip(&locations)
        .map(|(version, location)| FfiCommitFile {
            version: *version,
            file: crate::log_path::FfiLogPath::new(
                unsafe { KernelStringSlice::new_unsafe(location) },
                0,
                1,
            ),
        })
        .collect();
    match unsafe {
        create_winning_commits(
            KernelStringSlice::new_unsafe("memory:///table/"),
            start,
            end,
            FfiSlice::new_unsafe(&views),
            engine.shallow_copy(),
        )
    } {
        ExternResult::Err(error) => {
            let _ = unsafe { recover_error(error) };
        }
        ExternResult::Ok(commits) => {
            unsafe { free_winning_commits(commits) };
            panic!("Expected incomplete winning history to be rejected");
        }
    }
    unsafe { crate::free_engine(engine) };
}
