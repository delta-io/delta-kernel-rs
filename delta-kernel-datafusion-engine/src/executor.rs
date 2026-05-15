//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.
//!
//! Relation sinks ([`delta_kernel::plans::ir::nodes::SinkType::Relation`]) materialize batches into
//! [`crate::exec::RelationBatchRegistry`] when their stream is drained; subsequent plans read via a
//! [`DeclarativePlanNode::RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef)
//! leaf. [`SinkType::ConsumeByKdf`](delta_kernel::plans::ir::nodes::SinkType::ConsumeByKdf) drains
//! through a [`KernelConsumeByKdfExec`](crate::exec::KernelConsumeByKdfExec); harvest the finalized
//! handle with [`DataFusionExecutor::take_last_kdf_finished`] after fully draining the stream.
//! [`SinkType::Write`](delta_kernel::plans::ir::nodes::SinkType::Write) lowers to DataFusion file
//! sinks (`ParquetSink` / `JsonSink`) behind `DataSinkExec` in single-file mode. Bridging Parquet
//! footer statistics into Delta-style `Add.stats` (`numRecords`, min/max, ...) is not implemented
//! yet.
//! [`SinkType::PartitionedWrite`](delta_kernel::plans::ir::nodes::SinkType::PartitionedWrite)
//! (`KernelPartitionedWriteExec`) writes Hive-style partitions under a `file://` URL and yields no
//! output batches once drained.
//! [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) materializes per-row parquet
//! or JSON reads via [`KernelLoadSinkExec`](crate::exec::KernelLoadSinkExec) and the kernel's
//! parquet/json handlers.
//!
//! Phase 3.2 submits [`SinkType::ConsumeByKdf`] finalized handles into [`PhaseState`] during
//! [`Self::execute_phase_operation`] using [`crate::compile::CompileContext::phase_state`].
//! Phase 3.3 wires classic checkpoint parquet materialization via
//! [`Self::checkpoint_write_classic_parquet_and_finalize`] (kernel
//! [`delta_kernel::plans::state_machines::df::checkpoint_write`] plans + SM phase
//! `checkpoint_parquet_write`).

use std::sync::{Arc, Mutex};

use datafusion::execution::context::SessionContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::SinkType;
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};
use delta_kernel::plans::kdf::FinishedHandle;
use delta_kernel::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use delta_kernel::plans::state_machines::framework::engine_error::{EngineError, EngineErrorKind};
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel::plans::state_machines::framework::phase_state::PhaseState;
use delta_kernel::plans::state_machines::framework::state_machine::{AdvanceResult, StateMachine};
use delta_kernel::{Engine, Error as KernelError};
use futures::TryStreamExt;
use url::Url;

use crate::compile::{
    compile_plan, compile_plan_logical, node_output_schema, physical_read_schema, CompileContext,
};
use crate::error::{datafusion_err_to_delta, LiftDeltaErr};
use crate::exec::{
    KernelAssertExec, KernelConsumeByKdfExec, KernelLoadSinkExec, NullabilityValidationExec,
    RelationBatchRegistry,
};

fn default_kernel_engine() -> Arc<dyn Engine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

fn resolve_schema_query_url(path: &str) -> Result<Url, EngineError> {
    Url::parse(path).or_else(|_| {
        Url::from_file_path(std::path::Path::new(path)).map_err(|_| {
            EngineError::new(EngineErrorKind::IoError {
                message: format!("invalid schema-query location string: {path}"),
            })
        })
    })
}

fn map_kernel_err(err: KernelError) -> EngineError {
    match err {
        KernelError::FileNotFound(path) => EngineError::new(EngineErrorKind::FileNotFound { path }),
        other => EngineError::internal(other),
    }
}

fn nested_non_null_validations_from_field(
    path: String,
    field: &ArrowField,
    out: &mut Vec<(String, String)>,
) {
    if let ArrowDataType::Struct(fields) = field.data_type() {
        if field.is_nullable() {
            for child in fields {
                if !child.is_nullable() {
                    out.push((path.clone(), child.name().to_string()));
                }
            }
        }
        for child in fields {
            nested_non_null_validations_from_field(
                format!("{path}.{}", child.name()),
                child.as_ref(),
                out,
            );
        }
    }
}

fn nullability_validations_for_target_schema(
    schema: &delta_kernel::arrow::datatypes::Schema,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for field in schema.fields() {
        nested_non_null_validations_from_field(field.name().to_string(), field.as_ref(), &mut out);
    }
    out
}

fn execute_schema_query_phase(
    engine: &Arc<dyn Engine>,
    node: SchemaQueryNode,
) -> Result<PhaseState, EngineError> {
    let url = resolve_schema_query_url(&node.file_path)?;
    let meta = engine
        .storage_handler()
        .head(&url)
        .map_err(map_kernel_err)?;
    let footer = engine
        .parquet_handler()
        .read_parquet_footer(&meta)
        .map_err(map_kernel_err)?;
    let state = PhaseState::empty();
    state.submit_schema(footer.schema);
    Ok(state)
}

/// Minimal executor: holds a [`TaskContext`] for [`ExecutionPlan::execute`] calls.
///
/// Phase 1.1 does not yet wire a full [`SessionContext`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html);
/// literal execution only needs the default task context.
pub struct DataFusionExecutor {
    task_ctx: Arc<TaskContext>,
    session_ctx: SessionContext,
    session_config: SessionConfig,
    physical_optimizer: PhysicalOptimizer,
    relation_registry: Arc<RelationBatchRegistry>,
    kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
    engine: Arc<dyn Engine>,
}

impl DataFusionExecutor {
    /// Builds an executor backed by [`TaskContext::default()`] and a local-filesystem
    /// [`DefaultEngine`](delta_kernel::engine::default::DefaultEngine).
    pub fn try_new() -> Result<Self, DeltaError> {
        Self::try_new_with_engine(default_kernel_engine())
    }

    /// Builds an executor that uses the provided kernel [`Engine`] for IO helpers (object-store,
    /// parquet handler, etc).
    pub fn try_new_with_engine(engine: Arc<dyn Engine>) -> Result<Self, DeltaError> {
        let mut session_config = SessionConfig::new();
        // Disable DataFusion's leaf-expression-pushdown optimizer pass (the
        // `extract_leaf_expressions` + `push_down_leaf_projections` pair). It
        // miscompiles plans where a Filter sits above a Projection that
        // constructs a struct via `named_struct(...)` and the Filter accesses
        // fields of that struct: the rule substitutes the column reference
        // with the full `named_struct(...)` definition for every leaf access,
        // creating a combinatorial expansion that interacts badly with
        // `CommonSubexprEliminate` and ultimately produces
        // `Projection::try_new` failures of the form
        // "Schema contains duplicate unqualified field name __common_expr_N".
        // This pattern is generated by FSR scan replay (Filter on parsed-stats
        // fields above the augmented-add Projection). The optimization is not
        // valuable for that path anyway because the source is an in-memory
        // `RelationRef`; the loss matters only for parquet data scans, which
        // are still future work.
        // TODO: file an upstream issue mirroring apache/datafusion#20432
        // (the dedup gap in `build_extraction_projection_impl`'s
        // pass-through-column loop) and re-enable when fixed upstream.
        session_config
            .options_mut()
            .optimizer
            .enable_leaf_expression_pushdown = false;
        let session_ctx = SessionContext::new_with_config(session_config.clone());
        let _ = session_ctx.remove_optimizer_rule("optimize_projections");
        let _ = session_ctx.remove_optimizer_rule("optimize_unions");
        let relation_registry = Arc::new(RelationBatchRegistry::new());
        let kdf_harvest_slot = Arc::new(Mutex::new(None));
        Ok(Self {
            task_ctx: Arc::new(TaskContext::default()),
            session_ctx,
            session_config,
            physical_optimizer: PhysicalOptimizer::new(),
            relation_registry,
            kdf_harvest_slot,
            engine,
        })
    }

    /// Compile a [`Plan`] into a physical node when Phase 1.1 dispatch accepts it.
    pub fn compile_plan(&self, plan: &Plan) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
        let physical = compile_plan(
            plan,
            &CompileContext::new(
                Arc::clone(&self.relation_registry),
                Arc::clone(&self.kdf_harvest_slot),
                Arc::clone(&self.engine),
            ),
        )?;
        self.optimize_physical_plan(physical)
    }

    async fn compile_plan_for_execution(
        &self,
        plan: &Plan,
        ctx: &CompileContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
        // SinkType::Write / SinkType::PartitionedWrite always go through the physical compile
        // path: their lowering is bound to DataSinkExec / KernelPartitionedWriteExec which the
        // logical compiler does not model. Read-path sinks (Results/Relation/ConsumeByKdf/Load)
        // prefer the logical path.
        if matches!(
            plan.sink.sink_type,
            SinkType::Write(_) | SinkType::PartitionedWrite(_)
        ) {
            return self.compile_plan(plan);
        }
        if let Some(logical) = compile_plan_logical(plan, ctx)? {
            let state = self.session_ctx.state();
            let optimized = state.optimize(&logical).map_err(datafusion_err_to_delta)?;
            let physical_root = state
                .create_physical_plan(&optimized)
                .await
                .map_err(datafusion_err_to_delta);
            return match (&plan.sink.sink_type, physical_root) {
                (SinkType::Results(result_schema_opt), Ok(root)) => {
                    let root = if let DeclarativePlanNode::Assert { child, node } = &plan.root {
                        let input_schema = node_output_schema(child.as_ref())?;
                        Arc::new(KernelAssertExec::try_new(root, input_schema, &node.checks)?)
                            as Arc<dyn ExecutionPlan>
                    } else {
                        root
                    };
                    let target_kernel = result_schema_opt
                        .clone()
                        .unwrap_or(node_output_schema(&plan.root)?);
                    let target_arrow: delta_kernel::arrow::datatypes::SchemaRef =
                        Arc::new(target_kernel.as_ref().try_into_arrow().map_err(|e| {
                            crate::error::plan_compilation(format!(
                                "results target schema conversion failed: {e}"
                            ))
                        })?);
                    let validations =
                        nullability_validations_for_target_schema(target_arrow.as_ref());
                    Ok(Arc::new(NullabilityValidationExec::new(
                        root,
                        validations,
                        target_arrow,
                    )))
                }
                (SinkType::Relation(handle), Ok(root)) => {
                    let target_arrow: delta_kernel::arrow::datatypes::SchemaRef =
                        Arc::new(handle.schema.as_ref().try_into_arrow().map_err(|e| {
                            crate::error::plan_compilation(format!(
                                "relation target schema conversion failed: {e}"
                            ))
                        })?);
                    let validations =
                        nullability_validations_for_target_schema(target_arrow.as_ref());
                    let root = Arc::new(NullabilityValidationExec::new(
                        root,
                        validations,
                        target_arrow,
                    )) as Arc<dyn ExecutionPlan>;
                    Ok(root)
                }
                (SinkType::ConsumeByKdf(sink), Ok(root)) => {
                    Ok(Arc::new(KernelConsumeByKdfExec::try_new(
                        root,
                        sink.clone(),
                        Arc::clone(&ctx.kdf_harvest_slot),
                        ctx.phase_state.clone(),
                    )?))
                }
                (SinkType::Load(sink), Ok(root)) => {
                    let root = if root.output_partitioning().partition_count() > 1 {
                        Arc::new(CoalescePartitionsExec::new(root)) as Arc<dyn ExecutionPlan>
                    } else {
                        root
                    };
                    Ok(Arc::new(KernelLoadSinkExec::try_new(
                        root,
                        sink.clone(),
                        Arc::clone(&ctx.engine),
                        Arc::clone(&ctx.relation_registry),
                        physical_read_schema(sink)?,
                    )?))
                }
                (_, Ok(_)) => Err(crate::error::unsupported(
                    "logical compile returned plan for unsupported sink wrapper",
                )),
                (_, Err(e)) => Err(e),
            };
        }
        // Logical compiler doesn't yet cover this plan shape (e.g. ordered Union, certain
        // Write paths). Fall back to the physical compile path. User directive: Window MUST
        // be logical-only, and the physical compile arm explicitly errors out for Window —
        // anything else falls back here.
        self.compile_plan(plan)
    }

    fn optimize_physical_plan(
        &self,
        mut plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
        for rule in &self.physical_optimizer.rules {
            plan = rule
                .optimize(plan, self.session_config.options())
                .map_err(|e| {
                    crate::error::internal_error(format!(
                        "DataFusion physical optimizer `{}` failed: {e}",
                        rule.name()
                    ))
                })?;
        }
        Ok(plan)
    }

    /// Drain a [`PhaseOperation`] against the executor and return the resulting [`PhaseState`].
    pub async fn execute_phase_operation(
        &self,
        op: PhaseOperation,
    ) -> Result<PhaseState, EngineError> {
        self.clear_kdf_harvest_slot();
        match op {
            PhaseOperation::Plans(plans) => {
                let state = PhaseState::empty();
                let ctx = CompileContext {
                    relation_registry: Arc::clone(&self.relation_registry),
                    kdf_harvest_slot: Arc::clone(&self.kdf_harvest_slot),
                    phase_state: Some(state.clone()),
                    engine: Arc::clone(&self.engine),
                };
                for plan in plans {
                    let physical = self
                        .compile_plan_for_execution(&plan, &ctx)
                        .await
                        .map_err(EngineError::internal)?;
                    if matches!(plan.sink.sink_type, SinkType::Results(_)) {
                        self.drain_results_stream(physical)
                            .await
                            .map_err(EngineError::internal)?;
                    } else if let SinkType::Relation(handle) = &plan.sink.sink_type {
                        let batches = self
                            .collect_all_partitions(physical)
                            .await
                            .map_err(EngineError::internal)?;
                        self.register_relation_batches(handle, batches)
                            .map_err(EngineError::internal)?;
                    } else {
                        self.drain_plan_stream(physical)
                            .await
                            .map_err(EngineError::internal)?;
                    }
                }
                Ok(state)
            }
            PhaseOperation::SchemaQuery(node) => execute_schema_query_phase(&self.engine, node),
        }
    }

    /// Same draining semantics as [`Self::execute_phase_operation`], plus capture of the batches
    /// produced by the **last** [`SinkType::Results`] plan in the [`PhaseOperation::Plans`] slice
    /// (when present). Used by [`Self::drive_coroutine_sm_collecting_results`] and in-crate tests
    /// that need to verify row content of an SM-driven `Results` sink without rewriting the SM's
    /// `Output` contract.
    async fn execute_phase_operation_with_results_capture(
        &self,
        op: PhaseOperation,
    ) -> Result<(PhaseState, Option<Vec<RecordBatch>>), EngineError> {
        self.clear_kdf_harvest_slot();

        match op {
            PhaseOperation::Plans(plans) => {
                let state = PhaseState::empty();
                let ctx = CompileContext {
                    relation_registry: Arc::clone(&self.relation_registry),
                    kdf_harvest_slot: Arc::clone(&self.kdf_harvest_slot),
                    phase_state: Some(state.clone()),
                    engine: Arc::clone(&self.engine),
                };
                let mut last_results_batches: Option<Vec<RecordBatch>> = None;
                for plan in plans {
                    let physical = self
                        .compile_plan_for_execution(&plan, &ctx)
                        .await
                        .map_err(EngineError::internal)?;
                    if matches!(plan.sink.sink_type, SinkType::Results(_)) {
                        let mut stream = self
                            .single_results_stream(physical)
                            .map_err(EngineError::internal)?;
                        let mut batches = Vec::new();
                        while let Some(batch) = stream
                            .try_next()
                            .await
                            .map_err(datafusion_err_to_delta)
                            .map_err(EngineError::internal)?
                        {
                            batches.push(batch);
                        }
                        last_results_batches = Some(batches);
                    } else if let SinkType::Relation(handle) = &plan.sink.sink_type {
                        let batches = self
                            .collect_all_partitions(physical)
                            .await
                            .map_err(EngineError::internal)?;
                        self.register_relation_batches(handle, batches)
                            .map_err(EngineError::internal)?;
                    } else {
                        self.drain_plan_stream(physical)
                            .await
                            .map_err(EngineError::internal)?;
                    }
                }
                Ok((state, last_results_batches))
            }
            PhaseOperation::SchemaQuery(node) => {
                let state = execute_schema_query_phase(&self.engine, node)?;
                Ok((state, None))
            }
        }
    }

    fn clear_kdf_harvest_slot(&self) {
        let mut guard = self
            .kdf_harvest_slot
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *guard = None;
    }

    /// Drive a [`CoroutineSM`] until [`AdvanceResult::Done`].
    pub async fn drive_coroutine_sm<R: Send + 'static>(
        &self,
        mut sm: CoroutineSM<R>,
    ) -> Result<R, DeltaError> {
        loop {
            let op = sm.get_operation()?;
            let phase_result = self.execute_phase_operation(op).await;
            match sm.advance(phase_result)? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(v) => return Ok(v),
            }
        }
    }

    /// Drive a [`CoroutineSM`] until [`AdvanceResult::Done`] *and* capture the
    /// [`RecordBatch`] stream of the **last** [`SinkType::Results`] plan executed across all
    /// `PhaseOperation::Plans` yields. SMs that never yield a `Results` sink return
    /// `(value, vec![])`.
    ///
    /// Intended for callers (e.g. `Snapshot::full_state` consumers) that want both the SM
    /// outcome and the materialized scan-row batches without having to inspect the relation
    /// registry or rewrite the SM's `Output` contract.
    pub async fn drive_coroutine_sm_collecting_results<R: Send + 'static>(
        &self,
        mut sm: CoroutineSM<R>,
    ) -> Result<(R, Vec<RecordBatch>), DeltaError> {
        let mut last_results: Vec<RecordBatch> = Vec::new();
        loop {
            let op = sm.get_operation()?;
            let phase_result = self
                .execute_phase_operation_with_results_capture(op)
                .await
                .map(|(state, batches)| {
                    if let Some(b) = batches {
                        last_results = b;
                    }
                    state
                });
            match sm.advance(phase_result)? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(v) => return Ok((v, last_results)),
            }
        }
    }

    /// Drive a [`CoroutineSM`] to completion while streaming `Results` sink output batches to a
    /// caller callback as they are produced. Unlike
    /// [`Self::drive_coroutine_sm_collecting_results`], this does not materialize all result
    /// batches in memory first.
    pub async fn drive_coroutine_sm_streaming_results<R, F>(
        &self,
        mut sm: CoroutineSM<R>,
        mut on_batch: F,
    ) -> Result<R, DeltaError>
    where
        R: Send + 'static,
        F: FnMut(RecordBatch) -> Result<(), DeltaError> + Send,
    {
        loop {
            let op = sm.get_operation()?;
            let phase_result = self
                .execute_phase_operation_streaming_results(op, &mut on_batch)
                .await;
            match sm.advance(phase_result)? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(v) => return Ok(v),
            }
        }
    }

    /// Phase 3.4 helper — drive
    /// [`delta_kernel::plans::state_machines::df::insert_write_sm`] to completion.
    pub async fn drive_insert_write_sm(&self, plan: Plan) -> Result<(), DeltaError> {
        let sm = delta_kernel::plans::state_machines::df::insert_write_sm(plan)?;
        self.drive_coroutine_sm(sm).await
    }

    /// Phase 3.3 helper — drive
    /// [`delta_kernel::plans::state_machines::df::checkpoint_classic_parquet_write_sm`] to
    /// completion.
    pub async fn drive_checkpoint_classic_parquet_write_sm(
        &self,
        plan: Plan,
    ) -> Result<(), DeltaError> {
        let sm =
            delta_kernel::plans::state_machines::df::checkpoint_classic_parquet_write_sm(plan)?;
        self.drive_coroutine_sm(sm).await
    }

    /// Classic single-file checkpoint parquet via declarative [`Plan`] plus `_last_checkpoint`
    /// finalize.
    ///
    /// Requires this executor's [`Engine`] to match the [`CheckpointWriter`] snapshot storage (same
    /// as [`Snapshot::checkpoint`](delta_kernel::Snapshot::checkpoint)).
    ///
    /// `num_sidecars` stays `0` until multipart checkpoint shard plans land (see kernel
    /// `plans::state_machines::df::checkpoint_write` module docs).
    pub async fn checkpoint_write_classic_parquet_and_finalize(
        &self,
        writer: delta_kernel::checkpoint::CheckpointWriter,
    ) -> Result<(), DeltaError> {
        use delta_kernel::checkpoint::LastCheckpointHintStats;
        use delta_kernel::plans::state_machines::df::{
            checkpoint_classic_parquet_write_plan,
            prepare_classic_checkpoint_parquet_materialization,
        };

        let engine = self.engine.as_ref();
        let (handle, batches, dest, state) =
            prepare_classic_checkpoint_parquet_materialization(engine, &writer)
                .map_err(|e| crate::error::internal_error(e.to_string()))?;
        self.relation_registry.register(handle.id, batches);
        let plan = checkpoint_classic_parquet_write_plan(handle, dest.clone());
        self.drive_checkpoint_classic_parquet_write_sm(plan).await?;
        let meta = engine
            .storage_handler()
            .head(&dest)
            .map_err(|e| crate::error::internal_error(e.to_string()))?;
        let state = std::sync::Arc::into_inner(state).ok_or_else(|| {
            crate::error::internal_error(
                "checkpoint reconciliation state Arc still referenced after draining checkpoint iterator",
            )
        })?;
        let stats = LastCheckpointHintStats::from_reconciliation_state(state, meta.size, 0)
            .map_err(|e| crate::error::internal_error(e.to_string()))?;
        writer
            .finalize(engine, &stats)
            .map_err(|e| crate::error::internal_error(e.to_string()))?;
        Ok(())
    }

    /// Compile and execute partition `0`.
    pub async fn execute_plan_to_stream(
        &self,
        plan: Plan,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        self.clear_kdf_harvest_slot();
        let ctx = CompileContext::new(
            Arc::clone(&self.relation_registry),
            Arc::clone(&self.kdf_harvest_slot),
            Arc::clone(&self.engine),
        );
        let mut physical = self.compile_plan_for_execution(&plan, &ctx).await?;
        if physical.output_partitioning().partition_count() > 1 {
            physical = Arc::new(CoalescePartitionsExec::new(physical));
        }
        physical.execute(0, Arc::clone(&self.task_ctx)).lift()
    }

    /// Take the finalized [`FinishedHandle`] produced by the last fully-drained `ConsumeByKdf` sink
    /// plan.
    pub fn take_last_kdf_finished(&self) -> Option<FinishedHandle> {
        self.kdf_harvest_slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    /// Convenience helper for tests / tiny literals.
    pub async fn execute_plan_collect(
        &self,
        plan: Plan,
    ) -> Result<Vec<delta_kernel::arrow::array::RecordBatch>, DeltaError> {
        self.clear_kdf_harvest_slot();
        let ctx = CompileContext::new(
            Arc::clone(&self.relation_registry),
            Arc::clone(&self.kdf_harvest_slot),
            Arc::clone(&self.engine),
        );
        let physical = self.compile_plan_for_execution(&plan, &ctx).await?;
        let batches = self.collect_all_partitions(physical).await?;
        if let SinkType::Relation(handle) = &plan.sink.sink_type {
            self.register_relation_batches(handle, batches.clone())?;
        }
        Ok(batches)
    }

    pub fn relation_batch_registry(&self) -> &Arc<RelationBatchRegistry> {
        &self.relation_registry
    }

    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    async fn collect_all_partitions(
        &self,
        physical: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>, DeltaError> {
        let mut stream = self.single_results_stream(physical)?;
        let mut out = Vec::new();
        while let Some(batch) = stream.try_next().await.map_err(datafusion_err_to_delta)? {
            out.push(batch);
        }
        Ok(out)
    }

    fn register_relation_batches(
        &self,
        handle: &delta_kernel::plans::ir::nodes::RelationHandle,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DeltaError> {
        self.relation_registry.register(handle.id, batches);
        Ok(())
    }

    fn single_results_stream(
        &self,
        mut physical: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        if physical.output_partitioning().partition_count() > 1 {
            physical = Arc::new(CoalescePartitionsExec::new(physical));
        }
        physical.execute(0, Arc::clone(&self.task_ctx)).lift()
    }

    async fn drain_plan_stream(&self, physical: Arc<dyn ExecutionPlan>) -> Result<(), DeltaError> {
        let mut stream = self.root_partition0_stream(physical)?;
        while stream
            .try_next()
            .await
            .map_err(datafusion_err_to_delta)?
            .is_some()
        {}
        Ok(())
    }

    fn root_partition0_stream(
        &self,
        physical: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        physical.execute(0, Arc::clone(&self.task_ctx)).lift()
    }

    async fn drain_results_stream(
        &self,
        physical: Arc<dyn ExecutionPlan>,
    ) -> Result<(), DeltaError> {
        let mut stream = self.single_results_stream(physical)?;
        while stream
            .try_next()
            .await
            .map_err(datafusion_err_to_delta)?
            .is_some()
        {}
        Ok(())
    }

    async fn execute_phase_operation_streaming_results<F>(
        &self,
        op: PhaseOperation,
        on_batch: &mut F,
    ) -> Result<PhaseState, EngineError>
    where
        F: FnMut(RecordBatch) -> Result<(), DeltaError> + Send,
    {
        self.clear_kdf_harvest_slot();
        match op {
            PhaseOperation::Plans(plans) => {
                let state = PhaseState::empty();
                let ctx = CompileContext {
                    relation_registry: Arc::clone(&self.relation_registry),
                    kdf_harvest_slot: Arc::clone(&self.kdf_harvest_slot),
                    phase_state: Some(state.clone()),
                    engine: Arc::clone(&self.engine),
                };
                for plan in plans {
                    let physical = self
                        .compile_plan_for_execution(&plan, &ctx)
                        .await
                        .map_err(EngineError::internal)?;
                    if matches!(plan.sink.sink_type, SinkType::Results(_)) {
                        let mut stream = self
                            .single_results_stream(physical)
                            .map_err(EngineError::internal)?;
                        while let Some(batch) = stream
                            .try_next()
                            .await
                            .map_err(datafusion_err_to_delta)
                            .map_err(EngineError::internal)?
                        {
                            on_batch(batch).map_err(EngineError::internal)?;
                        }
                    } else if let SinkType::Relation(handle) = &plan.sink.sink_type {
                        let batches = self
                            .collect_all_partitions(physical)
                            .await
                            .map_err(EngineError::internal)?;
                        self.register_relation_batches(handle, batches)
                            .map_err(EngineError::internal)?;
                    } else {
                        self.drain_plan_stream(physical)
                            .await
                            .map_err(EngineError::internal)?;
                    }
                }
                Ok(state)
            }
            PhaseOperation::SchemaQuery(node) => execute_schema_query_phase(&self.engine, node),
        }
    }
}
