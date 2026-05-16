//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.
//!
//! [`SinkType::Results`] streams batches to the caller as-is.
//!
//! [`SinkType::Relation`](delta_kernel::plans::ir::nodes::SinkType::Relation) and
//! [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) materialize their drained
//! batches as a [`MemTable`] registered in [`Self::session_ctx`] under
//! `__dk_rel_{handle_id}`; downstream
//! [`DeclarativePlanNode::RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef)
//! leaves resolve to that provider during compile via a per-plan prefetched
//! [`CompileContext::relation_providers`] map. For [`SinkType::Load`] the executor first runs
//! [`load::materialize_upstream_batch`] over each upstream batch to translate file references
//! into the per-file row batches that get registered.
//!
//! [`SinkType::ConsumeByKdf`](delta_kernel::plans::ir::nodes::SinkType::ConsumeByKdf) drains
//! the physical plan through a [`ConsumerKdf`](delta_kernel::plans::kdf::ConsumerKdf) handle
//! inside [`Self::drain_consume_by_kdf`]; finalized handles submit into
//! [`CompileContext::phase_state`] (when present) or land in
//! [`Self::kdf_harvest_slot`] for [`Self::take_last_kdf_finished`].
//!
//! Sinks are annotations on the kernel [`Plan`], not [`ExecutionPlan`] envelopes — the executor
//! handles each sink type as a post-drain side effect on top of the lowered physical plan.

mod load;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion_common::error::DataFusionError;
use datafusion_common::TableReference;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{ConsumeByKdfSink, LoadSink, RelationHandle, SinkType};
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};
use delta_kernel::plans::kdf::{FinishedHandle, KdfControl, TraceContext};
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

use crate::compile::{compile_plan_logical, CompileContext};
use crate::error::df_to_delta;
use crate::executor::load::{materialize_upstream_batch, physical_read_schema};

fn relation_table_name(handle_id: u64) -> String {
    format!("__dk_rel_{handle_id}")
}

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
pub struct DataFusionExecutor {
    task_ctx: Arc<TaskContext>,
    session_ctx: SessionContext,
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
        // DataFusion's leaf-expression-pushdown pass interacts badly with our FSR scan replay
        // shape (Filter over a Projection that builds a struct via named_struct). The rule
        // inlines the full struct definition into every Filter leaf, CommonSubexprEliminate
        // then dedups badly and ultimately fails Projection::try_new with duplicate
        // `__common_expr_N` fields. The source for that shape is an in-memory RelationRef so
        // the optimization buys nothing; keep it disabled (apache/datafusion#20432 tracks the
        // upstream `build_extraction_projection_impl` dedup gap).
        session_config
            .options_mut()
            .optimizer
            .enable_leaf_expression_pushdown = false;
        let session_ctx = SessionContext::new_with_config(session_config);
        let _ = session_ctx.remove_optimizer_rule("optimize_projections");
        let _ = session_ctx.remove_optimizer_rule("optimize_unions");
        let kdf_harvest_slot = Arc::new(Mutex::new(None));
        Ok(Self {
            task_ctx: Arc::new(TaskContext::default()),
            session_ctx,
            kdf_harvest_slot,
            engine,
        })
    }

    /// Compile a kernel [`Plan`] to a DataFusion [`LogicalPlan`] for inspection. Used by
    /// debug/inspection callers (e.g. benchmark plan printers) that do not have any registered
    /// relations to resolve against; production execution goes through
    /// [`Self::prepare_execution_plan`].
    pub fn compile_plan_logical_for_inspection(
        &self,
        plan: &Plan,
    ) -> Result<LogicalPlan, DeltaError> {
        compile_plan_logical(
            plan,
            &CompileContext::new(
                Arc::new(HashMap::new()),
                Arc::clone(&self.kdf_harvest_slot),
                Arc::clone(&self.engine),
            ),
        )
        .map_err(df_to_delta)
    }

    /// Compile a kernel [`Plan`] to a DataFusion [`ExecutionPlan`]. Sinks are not wrapped on the
    /// physical plan; per-sink side effects happen in the executor's drain helpers
    /// ([`Self::drain_consume_by_kdf`], [`Self::drain_load`]).
    async fn prepare_execution_plan(
        &self,
        plan: &Plan,
        ctx: &CompileContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let logical = compile_plan_logical(plan, ctx)?;
        self.lower_to_physical(&logical).await
    }

    async fn lower_to_physical(
        &self,
        logical: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let state = self.session_ctx.state();
        let optimized = state.optimize(logical)?;
        state.create_physical_plan(&optimized).await
    }

    /// Walk `plan` and return an `Arc<HashMap>` keyed by handle id covering every
    /// `RelationRef` leaf, with each value resolved against [`Self::session_ctx`]'s catalog.
    /// Used to seed [`CompileContext::relation_providers`] before sync compile.
    async fn prefetch_relation_providers(
        &self,
        plan: &Plan,
    ) -> Result<Arc<HashMap<u64, Arc<dyn TableProvider>>>, DataFusionError> {
        let mut handles: Vec<RelationHandle> = Vec::new();
        collect_relation_handles(&plan.root, &mut handles);
        let mut providers: HashMap<u64, Arc<dyn TableProvider>> =
            HashMap::with_capacity(handles.len());
        for handle in handles {
            if providers.contains_key(&handle.id) {
                continue;
            }
            let table_name = relation_table_name(handle.id);
            let provider = self
                .session_ctx
                .table_provider(TableReference::bare(table_name.clone()))
                .await
                .map_err(|e| {
                    crate::error::plan_compilation(format!(
                        "RelationRef references handle id {} (name `{}`), but no \
                         provider is registered as `{}` in the session catalog: {e}",
                        handle.id, handle.name, table_name
                    ))
                })?;
            providers.insert(handle.id, provider);
        }
        Ok(Arc::new(providers))
    }

    /// Materialize `batches` as a [`MemTable`] under `__dk_rel_{handle_id}` in
    /// [`Self::session_ctx`], replacing any prior registration for that handle.
    fn register_relation_into_session(
        &self,
        handle: &RelationHandle,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DataFusionError> {
        let schema: delta_kernel::arrow::datatypes::SchemaRef =
            Arc::new(handle.schema.as_ref().try_into_arrow().map_err(|e| {
                crate::error::plan_compilation(format!("relation schema conversion: {e}"))
            })?);
        let mem = MemTable::try_new(schema, vec![batches])?;
        let name = relation_table_name(handle.id);
        let _ = self
            .session_ctx
            .deregister_table(TableReference::bare(name.clone()));
        self.session_ctx
            .register_table(TableReference::bare(name), Arc::new(mem))?;
        Ok(())
    }

    /// Drain a [`PhaseOperation`] against the executor and return the resulting [`PhaseState`].
    pub async fn execute_phase_operation(
        &self,
        op: PhaseOperation,
    ) -> Result<PhaseState, EngineError> {
        self.clear_kdf_harvest_slot();
        match op {
            PhaseOperation::Plans(plans) => self
                .execute_plans(plans)
                .await
                .map_err(EngineError::internal),
            PhaseOperation::SchemaQuery(node) => execute_schema_query_phase(&self.engine, node),
        }
    }

    async fn execute_plans(&self, plans: Vec<Plan>) -> Result<PhaseState, DataFusionError> {
        let state = PhaseState::empty();
        for plan in plans {
            let providers = self.prefetch_relation_providers(&plan).await?;
            let ctx = CompileContext {
                relation_providers: providers,
                kdf_harvest_slot: Arc::clone(&self.kdf_harvest_slot),
                phase_state: Some(state.clone()),
                engine: Arc::clone(&self.engine),
            };
            let physical = self.prepare_execution_plan(&plan, &ctx).await?;
            self.drain_and_register(&plan, physical, &ctx).await?;
        }
        Ok(state)
    }

    /// Drain `physical` according to `plan`'s sink type. For [`SinkType::Relation`] the collected
    /// batches are registered into [`Self::session_ctx`] as a [`MemTable`] keyed by the sink's
    /// output [`RelationHandle`]; [`SinkType::Load`] runs the upstream batches through
    /// [`load::materialize_upstream_batch`] first.
    async fn drain_and_register(
        &self,
        plan: &Plan,
        physical: Arc<dyn ExecutionPlan>,
        ctx: &CompileContext,
    ) -> Result<(), DataFusionError> {
        match &plan.sink.sink_type {
            SinkType::Results(_) => self.drain_results_stream(physical).await,
            SinkType::Relation(handle) => {
                let batches = self.collect_all_partitions(physical).await?;
                self.register_relation_into_session(handle, batches)
            }
            SinkType::Load(sink) => {
                let batches = self.drain_load(physical, sink, ctx).await?;
                self.register_relation_into_session(&sink.output_relation, batches)
            }
            SinkType::ConsumeByKdf(sink) => self.drain_consume_by_kdf(physical, sink, ctx).await,
        }
    }

    /// Drain `physical` through a [`ConsumerKdf`](delta_kernel::plans::kdf::ConsumerKdf) handle
    /// minted from `sink`. The finalized handle goes to [`CompileContext::phase_state`] when set,
    /// otherwise it lands in [`Self::kdf_harvest_slot`].
    async fn drain_consume_by_kdf(
        &self,
        physical: Arc<dyn ExecutionPlan>,
        sink: &ConsumeByKdfSink,
        ctx: &CompileContext,
    ) -> Result<(), DataFusionError> {
        if sink.requires_ordering.is_some() {
            return Err(crate::error::unsupported(
                "ConsumeByKdf with requires_ordering is not implemented for the DataFusion engine",
            ));
        }

        let mut handle = sink.new_handle(TraceContext::new("datafusion-engine", "execute"), 0);
        let mut stream = self.root_partition0_stream(physical)?;
        while let Some(batch) = stream.try_next().await? {
            let arrow = ArrowEngineData::new(batch);
            match handle
                .apply_consumer(&arrow)
                .map_err(crate::error::wrap_delta_err)?
            {
                KdfControl::Continue => {}
                KdfControl::Break => break,
            }
        }
        let finished = handle.finish();
        if let Some(state) = ctx.phase_state.as_ref() {
            state.submit_kdf_handle(finished);
        } else {
            let mut guard = self
                .kdf_harvest_slot
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = Some(finished);
        }
        Ok(())
    }

    /// Drain `physical` and run each upstream batch through
    /// [`materialize_upstream_batch`], returning the per-file row batches the caller registers
    /// under the sink's output [`RelationHandle`].
    async fn drain_load(
        &self,
        physical: Arc<dyn ExecutionPlan>,
        sink: &LoadSink,
        ctx: &CompileContext,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut stream = self.single_results_stream(physical)?;
        let read_schema = physical_read_schema(sink)?;
        let engine = ctx.engine.as_ref();
        let mut out = Vec::new();
        while let Some(upstream_batch) = stream.try_next().await? {
            let materialized =
                materialize_upstream_batch(&upstream_batch, sink, engine, read_schema.clone())?;
            out.extend(materialized);
        }
        Ok(out)
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
            PhaseOperation::Plans(plans) => self
                .execute_plans_with_results_capture(plans)
                .await
                .map_err(EngineError::internal),
            PhaseOperation::SchemaQuery(node) => {
                let state = execute_schema_query_phase(&self.engine, node)?;
                Ok((state, None))
            }
        }
    }

    async fn execute_plans_with_results_capture(
        &self,
        plans: Vec<Plan>,
    ) -> Result<(PhaseState, Option<Vec<RecordBatch>>), DataFusionError> {
        let state = PhaseState::empty();
        let mut last_results_batches: Option<Vec<RecordBatch>> = None;
        for plan in plans {
            let providers = self.prefetch_relation_providers(&plan).await?;
            let ctx = CompileContext {
                relation_providers: providers,
                kdf_harvest_slot: Arc::clone(&self.kdf_harvest_slot),
                phase_state: Some(state.clone()),
                engine: Arc::clone(&self.engine),
            };
            let physical = self.prepare_execution_plan(&plan, &ctx).await?;
            if matches!(plan.sink.sink_type, SinkType::Results(_)) {
                let mut stream = self.single_results_stream(physical)?;
                let mut batches = Vec::new();
                while let Some(batch) = stream.try_next().await? {
                    batches.push(batch);
                }
                last_results_batches = Some(batches);
            } else {
                self.drain_and_register(&plan, physical, &ctx).await?;
            }
        }
        Ok((state, last_results_batches))
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

    /// Compile and execute `plan` as a single-partition stream. Sink-specific behavior:
    ///
    /// - [`SinkType::Results`] / [`SinkType::Relation`]: stream upstream batches verbatim.
    /// - [`SinkType::ConsumeByKdf`]: eagerly drain the upstream into the consumer + harvest the
    ///   finalized handle into [`Self::kdf_harvest_slot`], then return an empty stream over the
    ///   upstream schema (legacy parity).
    /// - [`SinkType::Load`]: eagerly drain + materialize the per-file rows via
    ///   [`Self::drain_load`], then surface them as a [`MemoryStream`].
    pub async fn execute_plan_to_stream(
        &self,
        plan: Plan,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        self.execute_plan_to_stream_inner(plan)
            .await
            .map_err(df_to_delta)
    }

    async fn execute_plan_to_stream_inner(
        &self,
        plan: Plan,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.clear_kdf_harvest_slot();
        let ctx = CompileContext::new(
            self.prefetch_relation_providers(&plan).await?,
            Arc::clone(&self.kdf_harvest_slot),
            Arc::clone(&self.engine),
        );
        let physical = self.prepare_execution_plan(&plan, &ctx).await?;
        match &plan.sink.sink_type {
            SinkType::Results(_) | SinkType::Relation(_) => {
                let stream = self.single_results_stream(physical)?;
                Ok(stream)
            }
            SinkType::ConsumeByKdf(sink) => {
                let schema = physical.schema();
                self.drain_consume_by_kdf(physical, sink, &ctx).await?;
                Ok(Box::pin(MemoryStream::try_new(Vec::new(), schema, None)?))
            }
            SinkType::Load(sink) => {
                let arrow_schema: delta_kernel::arrow::datatypes::SchemaRef = Arc::new(
                    sink.output_relation
                        .schema
                        .as_ref()
                        .try_into_arrow()
                        .map_err(|e| {
                            crate::error::plan_compilation(format!(
                                "Load output schema conversion: {e}"
                            ))
                        })?,
                );
                let batches = self.drain_load(physical, sink, &ctx).await?;
                Ok(Box::pin(MemoryStream::try_new(
                    batches,
                    arrow_schema,
                    None,
                )?))
            }
        }
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
        self.execute_plan_collect_inner(plan)
            .await
            .map_err(df_to_delta)
    }

    async fn execute_plan_collect_inner(
        &self,
        plan: Plan,
    ) -> Result<Vec<delta_kernel::arrow::array::RecordBatch>, DataFusionError> {
        self.clear_kdf_harvest_slot();
        let ctx = CompileContext::new(
            self.prefetch_relation_providers(&plan).await?,
            Arc::clone(&self.kdf_harvest_slot),
            Arc::clone(&self.engine),
        );
        let physical = self.prepare_execution_plan(&plan, &ctx).await?;
        match &plan.sink.sink_type {
            SinkType::Relation(handle) => {
                let batches = self.collect_all_partitions(physical).await?;
                self.register_relation_into_session(handle, batches.clone())?;
                Ok(batches)
            }
            SinkType::Load(sink) => {
                let batches = self.drain_load(physical, sink, &ctx).await?;
                self.register_relation_into_session(&sink.output_relation, batches.clone())?;
                Ok(batches)
            }
            SinkType::ConsumeByKdf(sink) => {
                self.drain_consume_by_kdf(physical, sink, &ctx).await?;
                Ok(Vec::new())
            }
            SinkType::Results(_) => self.collect_all_partitions(physical).await,
        }
    }

    /// Collect all batches for a previously-registered relation from the session catalog.
    /// Used by tests that need to inspect the materialized output of a relation/load sink.
    pub async fn collect_relation(
        &self,
        handle: &RelationHandle,
    ) -> Result<Vec<RecordBatch>, DeltaError> {
        self.collect_relation_inner(handle)
            .await
            .map_err(df_to_delta)
    }

    async fn collect_relation_inner(
        &self,
        handle: &RelationHandle,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let name = relation_table_name(handle.id);
        let df = self
            .session_ctx
            .table(TableReference::bare(name.clone()))
            .await
            .map_err(|e| {
                crate::error::plan_compilation(format!(
                    "no relation registered for handle id {} (name `{}`) as `{}`: {e}",
                    handle.id, handle.name, name
                ))
            })?;
        df.collect().await
    }

    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    async fn collect_all_partitions(
        &self,
        physical: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut stream = self.single_results_stream(physical)?;
        let mut out = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            out.push(batch);
        }
        Ok(out)
    }

    fn single_results_stream(
        &self,
        mut physical: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if physical.output_partitioning().partition_count() > 1 {
            physical = Arc::new(CoalescePartitionsExec::new(physical));
        }
        physical.execute(0, Arc::clone(&self.task_ctx))
    }

    fn root_partition0_stream(
        &self,
        physical: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        physical.execute(0, Arc::clone(&self.task_ctx))
    }

    async fn drain_results_stream(
        &self,
        physical: Arc<dyn ExecutionPlan>,
    ) -> Result<(), DataFusionError> {
        let mut stream = self.single_results_stream(physical)?;
        while stream.try_next().await?.is_some() {}
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
            PhaseOperation::Plans(plans) => self
                .execute_plans_streaming_results(plans, on_batch)
                .await
                .map_err(EngineError::internal),
            PhaseOperation::SchemaQuery(node) => execute_schema_query_phase(&self.engine, node),
        }
    }

    async fn execute_plans_streaming_results<F>(
        &self,
        plans: Vec<Plan>,
        on_batch: &mut F,
    ) -> Result<PhaseState, DataFusionError>
    where
        F: FnMut(RecordBatch) -> Result<(), DeltaError> + Send,
    {
        let state = PhaseState::empty();
        for plan in plans {
            let providers = self.prefetch_relation_providers(&plan).await?;
            let ctx = CompileContext {
                relation_providers: providers,
                kdf_harvest_slot: Arc::clone(&self.kdf_harvest_slot),
                phase_state: Some(state.clone()),
                engine: Arc::clone(&self.engine),
            };
            let physical = self.prepare_execution_plan(&plan, &ctx).await?;
            if matches!(plan.sink.sink_type, SinkType::Results(_)) {
                let mut stream = self.single_results_stream(physical)?;
                while let Some(batch) = stream.try_next().await? {
                    on_batch(batch).map_err(crate::error::wrap_delta_err)?;
                }
            } else {
                self.drain_and_register(&plan, physical, &ctx).await?;
            }
        }
        Ok(state)
    }
}

fn collect_relation_handles(node: &DeclarativePlanNode, out: &mut Vec<RelationHandle>) {
    match node {
        DeclarativePlanNode::RelationRef(handle) => out.push(handle.clone()),
        DeclarativePlanNode::Filter { child, .. }
        | DeclarativePlanNode::Project { child, .. }
        | DeclarativePlanNode::Window { child, .. } => collect_relation_handles(child, out),
        DeclarativePlanNode::Union { children, .. } => {
            for c in children {
                collect_relation_handles(c, out);
            }
        }
        DeclarativePlanNode::Join { build, probe, .. } => {
            collect_relation_handles(build, out);
            collect_relation_handles(probe, out);
        }
        DeclarativePlanNode::Scan(_)
        | DeclarativePlanNode::FileListing(_)
        | DeclarativePlanNode::Values(_) => {}
    }
}
