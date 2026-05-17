//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.
//!
//! [`SinkType::Relation`] and [`SinkType::Load`] materialize their drained batches as a
//! [`MemTable`] registered in [`Self::session_ctx`] under `__dk_rel_{handle_id}`; downstream
//! [`DeclarativePlanNode::RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef)
//! leaves resolve to that provider during compile via a per-plan prefetched
//! [`CompileContext::relation_providers`] map. For [`SinkType::Load`] the executor first runs
//! [`load::materialize_upstream_batch`] over each upstream batch to translate file references
//! into the per-file row batches that get registered.
//!
//! [`SinkType::Consume`](delta_kernel::plans::ir::nodes::SinkType::Consume) drains
//! the physical plan through a [`ConsumerKdf`](delta_kernel::plans::kdf::ConsumerKdf) handle
//! inside [`Self::drain_consume_sink`]; finalized handles submit into the active phase's
//! [`PhaseState`].
//!
//! Sinks are annotations on the kernel [`Plan`], not [`ExecutionPlan`] envelopes -- the executor
//! handles each sink type as a post-drain side effect on top of the lowered physical plan.
//!
//! # API surface
//!
//! - [`DataFusionExecutor::drive_to_completion`] drives a [`CoroutineSM`] until it terminates,
//!   handling any intermediate phase operations the SM yields (kernel-side decision plans, schema
//!   queries). Returns the SM's terminal value.
//! - [`DataFusionExecutor::execute_plans`] runs every plan in a slice in order, draining sinks and
//!   registering relations as it goes.
//! - [`DataFusionExecutor::collect_relation`] / [`DataFusionExecutor::stream_relation`] read a
//!   previously-materialized relation back from the session catalog.
//! - [`DataFusionExecutor::collect_result`] / [`DataFusionExecutor::stream_result`] compose the
//!   above two: execute the [`ResultPlan`]'s plans, then read its result relation.
//!
//! [`SinkType::Relation`]: delta_kernel::plans::ir::nodes::SinkType::Relation
//! [`SinkType::Load`]: delta_kernel::plans::ir::nodes::SinkType::Load

mod load;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion_common::error::DataFusionError;
use datafusion_common::TableReference;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{ConsumeSink, LoadSink, RelationHandle, SinkType};
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan, ResultPlan};
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
use crate::error::DfResultIntoDelta;
use crate::executor::load::materialize_upstream_batch;

fn relation_table_name(handle_id: &str) -> String {
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
        Ok(Self {
            task_ctx: Arc::new(TaskContext::default()),
            session_ctx,
            engine,
        })
    }

    /// Reference to the kernel [`Engine`] this executor uses for IO helpers.
    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    /// Compile a kernel [`Plan`] to a DataFusion [`LogicalPlan`] for inspection. Used by
    /// debug/inspection callers (e.g. benchmark plan printers) that do not have any registered
    /// relations to resolve against; production execution goes through
    /// [`Self::execute_plans`].
    pub fn compile_plan_logical_for_inspection(
        &self,
        plan: &Plan,
    ) -> Result<LogicalPlan, DeltaError> {
        compile_plan_logical(
            plan,
            &CompileContext::new(Arc::new(HashMap::new()), Arc::clone(&self.engine)),
        )
        .into_delta()
    }

    // ================================================================
    // High-level SM and result-plan driving
    // ================================================================

    /// Drive `sm` until it terminates, executing any intermediate phase operations it yields
    /// (kernel-side decision plans, schema queries) and returning the SM's terminal value.
    ///
    /// The terminal value is whatever `R` the SM was constructed for: for read-style SMs that
    /// is typically a [`ResultPlan`] that the caller then runs with [`Self::collect_result`] /
    /// [`Self::stream_result`] (or by hand with [`Self::execute_plans`] +
    /// [`Self::collect_relation`]).
    pub async fn drive_to_completion<R: Send + 'static>(
        &self,
        mut sm: CoroutineSM<R>,
    ) -> Result<R, DeltaError> {
        loop {
            // Zero-yield SMs have no operation to fetch; the first `advance` hands the stored
            // terminal value back directly. Fall back to an empty `PhaseState` in that case so
            // `advance` has a valid (unused) input.
            let phase_result = match sm.get_operation() {
                Ok(op) => self.execute_phase_operation(op).await,
                Err(_) => Ok(PhaseState::empty()),
            };
            match sm.advance(phase_result)? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(value) => return Ok(value),
            }
        }
    }

    /// Execute every plan in `plans` in order. Each plan terminates in [`SinkType::Relation`],
    /// [`SinkType::Load`], or [`SinkType::Consume`]; the executor drains the physical plan
    /// and routes the result into the session catalog (for `Relation` / `Load`) or into the
    /// active phase's [`PhaseState`] (for `Consume`).
    pub async fn execute_plans(&self, plans: &[Plan]) -> Result<(), DeltaError> {
        let state = PhaseState::empty();
        self.run_plans(plans, &state).await.into_delta()
    }

    /// Run [`ResultPlan::plans`] and then collect every batch in the result relation.
    pub async fn collect_result(&self, rp: ResultPlan) -> Result<Vec<RecordBatch>, DeltaError> {
        self.execute_plans(&rp.plans).await?;
        self.collect_relation(&rp.result_relation).await
    }

    /// Run [`ResultPlan::plans`] and return a stream over the result relation's batches.
    pub async fn stream_result(
        &self,
        rp: ResultPlan,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        self.execute_plans(&rp.plans).await?;
        self.stream_relation(&rp.result_relation).await
    }

    // ================================================================
    // Relation registry I/O
    // ================================================================

    /// Collect every batch of a previously-materialized relation by name.
    pub async fn collect_relation(
        &self,
        handle: &RelationHandle,
    ) -> Result<Vec<RecordBatch>, DeltaError> {
        let df = self.relation_dataframe(handle).await.into_delta()?;
        df.collect().await.into_delta()
    }

    /// Open a stream over a previously-materialized relation's batches.
    pub async fn stream_relation(
        &self,
        handle: &RelationHandle,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        let df = self.relation_dataframe(handle).await.into_delta()?;
        df.execute_stream().await.into_delta()
    }

    /// Register a pre-materialized batch set as a relation under `name`, returning a fresh
    /// [`RelationHandle`] downstream plans can reference. Useful when tests or external
    /// fixtures want to inject relation data without running a producer plan.
    pub fn register_batches(
        &self,
        name: &str,
        schema: delta_kernel::schema::SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<RelationHandle, DeltaError> {
        let handle = RelationHandle::fresh(name, schema);
        self.register_relation_into_session(&handle, batches)
            .into_delta()?;
        Ok(handle)
    }

    // ================================================================
    // Internal helpers
    // ================================================================

    async fn relation_dataframe(
        &self,
        handle: &RelationHandle,
    ) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
        let name = relation_table_name(handle.id.as_str());
        self.session_ctx
            .table(TableReference::bare(name.clone()))
            .await
            .map_err(|e| {
                crate::error::plan_compilation(format!(
                    "no relation registered for handle id {} (name `{}`) as `{}`: {e}",
                    handle.id, handle.name, name
                ))
            })
    }

    /// Execute a single [`PhaseOperation`] against the executor and return the resulting
    /// [`PhaseState`]. Used internally by [`Self::drive_to_completion`] and exposed for
    /// callers (typically tests) that need to drive an individual phase op directly --
    /// for example, draining a [`Consume`](SinkType::Consume) plan and inspecting
    /// the [`PhaseState`] for its finalized handle.
    pub async fn execute_phase_operation(
        &self,
        op: PhaseOperation,
    ) -> Result<PhaseState, EngineError> {
        match op {
            PhaseOperation::Plans(plans) => {
                let state = PhaseState::empty();
                self.run_plans(&plans, &state)
                    .await
                    .map_err(EngineError::internal)?;
                Ok(state)
            }
            PhaseOperation::SchemaQuery(node) => execute_schema_query_phase(&self.engine, node),
        }
    }

    async fn run_plans(&self, plans: &[Plan], state: &PhaseState) -> Result<(), DataFusionError> {
        for plan in plans {
            let providers = self.prefetch_relation_providers(plan).await?;
            let ctx = CompileContext {
                relation_providers: providers,
                phase_state: Some(state.clone()),
                engine: Arc::clone(&self.engine),
            };
            let physical = self.prepare_execution_plan(plan, &ctx).await?;
            self.drain_to_sink(plan, physical, &ctx).await?;
        }
        Ok(())
    }

    /// Drain `physical` according to `plan`'s sink type, registering any materialized rows back
    /// into [`Self::session_ctx`] or routing them into the active phase's KDF state.
    async fn drain_to_sink(
        &self,
        plan: &Plan,
        physical: Arc<dyn ExecutionPlan>,
        ctx: &CompileContext,
    ) -> Result<(), DataFusionError> {
        match &plan.sink {
            SinkType::Relation(handle) => {
                let batches = self.collect_all_partitions(physical).await?;
                self.register_relation_into_session(handle, batches)
            }
            SinkType::Load(sink) => {
                let batches = self.drain_load(physical, sink, ctx).await?;
                self.register_relation_into_session(&sink.output_relation, batches)
            }
            SinkType::Consume(sink) => self.drain_consume_sink(physical, sink, ctx).await,
        }
    }

    /// Compile a kernel [`Plan`] to a DataFusion [`ExecutionPlan`]. Sinks are not wrapped on the
    /// physical plan; per-sink side effects happen in the executor's drain helpers
    /// ([`Self::drain_consume_sink`], [`Self::drain_load`]).
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
    ) -> Result<Arc<HashMap<String, Arc<dyn TableProvider>>>, DataFusionError> {
        let mut handles: Vec<RelationHandle> = Vec::new();
        collect_relation_handles(&plan.root, &mut handles);
        let mut providers: HashMap<String, Arc<dyn TableProvider>> =
            HashMap::with_capacity(handles.len());
        for handle in handles {
            if providers.contains_key(handle.id.as_str()) {
                continue;
            }
            let table_name = relation_table_name(handle.id.as_str());
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
            providers.insert(handle.id.clone(), provider);
        }
        Ok(Arc::new(providers))
    }

    /// Materialize `batches` as a [`MemTable`] under `__dk_rel_{handle_id}` in
    /// [`Self::session_ctx`], replacing any prior registration for that handle.
    fn register_relation_into_session(
        &self,
        handle: &RelationHandle,
        mut batches: Vec<RecordBatch>,
    ) -> Result<(), DataFusionError> {
        let schema: delta_kernel::arrow::datatypes::SchemaRef =
            Arc::new(handle.schema.as_ref().try_into_arrow().map_err(|e| {
                crate::error::plan_compilation(format!("relation schema conversion: {e}"))
            })?);
        if let Some(first) = batches.first() {
            if first.schema() != schema {
                batches = batches
                    .into_iter()
                    .map(|batch| RecordBatch::try_new(schema.clone(), batch.columns().to_vec()))
                    .collect::<Result<Vec<_>, _>>()?;
            }
        }
        let mem = MemTable::try_new(schema, vec![batches])?;
        let name = relation_table_name(handle.id.as_str());
        let _ = self
            .session_ctx
            .deregister_table(TableReference::bare(name.clone()));
        self.session_ctx
            .register_table(TableReference::bare(name), Arc::new(mem))?;
        Ok(())
    }

    /// Drain `physical` through a [`ConsumerKdf`](delta_kernel::plans::kdf::ConsumerKdf) handle
    /// minted from `sink`. The finalized handle submits into the active phase's
    /// [`PhaseState`].
    async fn drain_consume_sink(
        &self,
        physical: Arc<dyn ExecutionPlan>,
        sink: &ConsumeSink,
        ctx: &CompileContext,
    ) -> Result<(), DataFusionError> {
        let mut handle = sink.new_handle(TraceContext::new("datafusion-engine", "execute"));
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
            // Consume sink called outside of an active phase has nowhere to land its handle.
            // Drop the handle and surface a clear error so the caller can wire phase state.
            let _: FinishedHandle = finished;
            return Err(crate::error::internal_error(
                "Consume sink drained without an active phase state to submit into",
            ));
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
        let read_schema = sink.file_schema.clone();
        let engine = ctx.engine.as_ref();
        let mut out = Vec::new();
        while let Some(upstream_batch) = stream.try_next().await? {
            let materialized =
                materialize_upstream_batch(&upstream_batch, sink, engine, read_schema.clone())?;
            out.extend(materialized);
        }
        Ok(out)
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
}

// ============================================================================
// Helpers
// ============================================================================

/// Walk `node` and append every [`RelationHandle`] referenced by a `RelationRef` leaf into
/// `out`. Used to prefetch session-catalog providers for the relations a plan consumes.
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
