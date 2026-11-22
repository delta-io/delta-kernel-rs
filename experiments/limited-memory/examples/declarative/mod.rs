use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::DelegatingEngine;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::plans::{Operation, PlanExecutor, PlanResult, RelationRef};
use delta_kernel::scan::StatsOptions;
use delta_kernel::transaction::{Declarative, UpdateTableOperation};
use delta_kernel::{Engine, KernelError, KernelResult, Snapshot};
use serde::Serialize;

#[derive(Serialize)]
pub struct Outcome {
    snapshot_version: u64,
    metadata_plan_nodes: usize,
    metadata_plan_proto_bytes: usize,
    write_state_bytes: usize,
    staged_relations: usize,
    engine_ops: usize,
    largest_engine_op_proto_bytes: usize,
    declarative_commit_supported: bool,
}

struct Executor {
    inner: Arc<dyn PlanExecutor>,
    calls: AtomicUsize,
    max_wire: AtomicUsize,
}

impl PlanExecutor for Executor {
    fn execute_op(&self, operation: Operation) -> KernelResult<PlanResult> {
        // Exercise the same IR -> proto mirror -> bytes conversion used by FfiPlanExecutor.
        // The local executor is an engine stand-in; its decoding/replay allocations are not
        // evidence about Kernel's allocation footprint in an external declarative engine.
        let wire = operation.try_to_proto_bytes()?;
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.max_wire.fetch_max(wire.len(), Ordering::Relaxed);
        let result = self.inner.execute_op(operation);
        drop(wire);
        result
    }
}

pub fn run(root: &str, record: &mut impl FnMut(&'static str)) -> delta_kernel::Result<Outcome> {
    let inner = Arc::new(SyncEngine::new());
    let executor = Arc::new(Executor {
        inner: inner
            .plan_executor()
            .ok_or_else(|| KernelError::internal_error("missing declarative executor"))?,
        calls: AtomicUsize::new(0),
        max_wire: AtomicUsize::new(0),
    });
    let engine = DelegatingEngine::new(inner).with_plan_executor(executor.clone());
    let snapshot = Snapshot::builder_for(root).build(&engine)?;
    record("declarative_snapshot_ready_mock_engine_included");

    // Resolve IO before the pure planning windows. The callback count below verifies that the
    // metadata plan and its protobuf serialization do not execute the returned scan plan.
    let _checkpoint = snapshot.reader().get_checkpoint_info(&engine)?;
    record("declarative_checkpoint_info_ready_mock_engine_included");
    let stats = match std::env::var("KERNEL_DECLARATIVE_STATS").as_deref() {
        Ok("json") => StatsOptions::json_only(),
        Ok("both") => StatsOptions::all(),
        _ => StatsOptions::all_struct(),
    };
    let scan = snapshot.clone().scan_builder().with_stats(stats).build()?;
    record("declarative_scan_state_built");
    let calls = executor.calls.load(Ordering::Relaxed);
    let plan = scan
        .declarative_metadata_scan_plan(&engine)?
        .ok_or_else(|| KernelError::internal_error("fixture produced an empty metadata plan"))?;
    if executor.calls.load(Ordering::Relaxed) != calls {
        return Err(KernelError::internal_error(
            "plan construction unexpectedly executed an engine operation",
        )
        .into());
    }
    let metadata_plan_nodes = plan.nodes.len();
    record("declarative_metadata_plan_built_no_execution");
    let operation = Operation::QueryPlan(plan);
    let wire = operation.try_to_proto_bytes()?;
    let metadata_plan_proto_bytes = wire.len();
    record("declarative_metadata_plan_proto_live_no_execution");
    drop(operation);
    record("declarative_metadata_plan_owned_wire_live");
    drop(wire);
    record("declarative_metadata_plan_released");

    let mut transaction = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "declarative-memory-poc", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    let relations: usize = std::env::var("KERNEL_DECLARATIVE_RELATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    for index in 0..relations {
        transaction.add_files_ref(RelationRef::new(
            format!("engine-owned-adds-{index}"),
            transaction.add_files_schema().clone(),
        ));
    }
    record("declarative_transaction_relations_staged");
    let state = transaction.write_state()?;
    let wire = state.encode()?;
    let write_state_bytes = wire.len();
    record("declarative_write_state_encoded_live");
    drop((wire, state, transaction, scan));
    record("declarative_read_write_owners_released");
    Ok(Outcome {
        snapshot_version: snapshot.version(),
        metadata_plan_nodes,
        metadata_plan_proto_bytes,
        write_state_bytes,
        staged_relations: relations,
        engine_ops: executor.calls.load(Ordering::Relaxed),
        largest_engine_op_proto_bytes: executor.max_wire.load(Ordering::Relaxed),
        // Current mainline's commit_declarative returns Unsupported. Do not substitute an
        // imperative commit and report it as coverage of a declarative write lifecycle.
        declarative_commit_supported: false,
    })
}
