//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.
//!
//! [`SinkType::Relation`] and [`SinkType::Load`] register a *lazy* [`TableProvider`] into the
//! executor's [`Self::relation_providers`] map keyed by [`RelationHandle::id`]; the upstream
//! pipeline is never run at registration time. Downstream
//! [`DeclarativePlanNode::RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef)
//! leaves resolve to that provider during compile, and DataFusion's analyzer/optimizer drives
//! execution lazily when the consumer reads the relation. Concretely:
//!
//! - `Relation` -> [`datafusion::datasource::ViewTable`] wrapping the upstream `LogicalPlan`.
//!   DataFusion's `InlineTableScan` analyzer rule inlines the wrapped plan into the consumer's tree
//!   so predicate / projection pushdown and CSE flow across the boundary.
//! - `Load`     -> [`crate::exec::LoadTableProvider`] capturing the upstream `LogicalPlan` +
//!   [`LoadSink`] + kernel [`Engine`]. Its `scan()` builds the upstream physical plan and wraps it
//!   in [`crate::exec::LoadExec`], which streams per-row file batches incrementally via kernel's
//!   parquet/json handler.
//!
//! [`SinkType::Consume`](delta_kernel::plans::ir::nodes::SinkType::Consume) is the only sink
//! that drains eagerly: the physical plan runs and feeds a
//! [`ConsumerKdf`](delta_kernel::plans::kdf::ConsumerKdf) handle inside
//! [`Self::drain_consume_sink`]; finalized handles submit into the active phase's
//! [`PhaseState`].
//!
//! # API surface
//!
//! - [`DataFusionExecutor::drive_to_completion`] drives a [`CoroutineSM`] until it terminates,
//!   handling any intermediate phase operations the SM yields (kernel-side decision plans, schema
//!   queries). Returns the SM's terminal value.
//! - [`DataFusionExecutor::execute_plans`] runs every plan in a slice in order, registering
//!   `Relation` / `Load` providers and draining `Consume` sinks as it goes.
//! - [`DataFusionExecutor::stream_relation`] / [`DataFusionExecutor::collect_relation`] read a
//!   previously-registered relation as a stream or a buffered `Vec`. The actual upstream execution
//!   happens here on demand.
//! - [`DataFusionExecutor::collect_result`] composes the above: execute the [`ResultPlan`]'s plans,
//!   then collect its result relation.
//!
//! [`SinkType::Relation`]: delta_kernel::plans::ir::nodes::SinkType::Relation
//! [`SinkType::Load`]: delta_kernel::plans::ir::nodes::SinkType::Load

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::catalog::TableProvider;
use datafusion::datasource::ViewTable;
use datafusion::execution::context::SessionContext;
use datafusion_common::error::DataFusionError;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::datatypes::SchemaRef;
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{ConsumeSink, LoadSink, RelationHandle, SinkType};
use delta_kernel::plans::ir::{Plan, ResultPlan};
use delta_kernel::plans::kdf::{FinishedHandle, KdfControl};
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
use uuid::Uuid;

use crate::compile::{compile_plan_logical, CompileContext};
use crate::error::DfResultIntoDelta;
use crate::exec::LoadTableProvider;

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

fn execute_schema_query_phase(
    engine: &Arc<dyn Engine>,
    node: SchemaQueryNode,
) -> Result<PhaseState, EngineError> {
    let map_kernel_err = |err: KernelError| match err {
        KernelError::FileNotFound(path) => EngineError::new(EngineErrorKind::FileNotFound { path }),
        other => EngineError::internal(other),
    };
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

/// Minimal executor: a [`TaskContext`] for [`ExecutionPlan::execute`] calls, a [`SessionContext`]
/// for DataFusion compile/optimize/lower, a kernel [`Engine`] for IO helpers, and a
/// [`Mutex`]-guarded map of lazily-registered relations the compiler resolves
/// [`RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef) leaves against.
pub struct DataFusionExecutor {
    task_ctx: Arc<TaskContext>,
    session_ctx: SessionContext,
    engine: Arc<dyn Engine>,
    relation_providers: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
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
            relation_providers: Mutex::new(HashMap::new()),
        })
    }

    /// Reference to the kernel [`Engine`] this executor uses for IO helpers.
    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    /// Snapshot the provider registered under `handle_id`, if any. Used by tests to assert
    /// lazy-registration shape (e.g. that the provider is a [`ViewTable`] and not a materialized
    /// `MemTable`).
    #[cfg(test)]
    pub(crate) fn relation_provider(&self, handle_id: &str) -> Option<Arc<dyn TableProvider>> {
        self.relation_providers
            .lock()
            .expect("relation_providers mutex poisoned")
            .get(handle_id)
            .cloned()
    }

    /// Snapshot of the underlying [`SessionContext`]'s state. Used by tests to invoke
    /// [`datafusion::catalog::TableProvider::scan`] directly so they can assert what projection /
    /// limit a provider actually pushed down.
    #[cfg(test)]
    pub(crate) fn session_state(&self) -> datafusion::execution::SessionState {
        self.session_ctx.state()
    }

    // ================================================================
    // High-level SM and result-plan driving
    // ================================================================

    /// Drive `sm` until it terminates, executing any intermediate phase operations it yields
    /// (kernel-side decision plans, schema queries) and returning the SM's terminal value.
    ///
    /// The terminal value is whatever `R` the SM was constructed for: for read-style SMs that
    /// is typically a [`ResultPlan`] that the caller then runs with [`Self::collect_result`]
    /// (or by hand with [`Self::execute_plans`] + [`Self::collect_relation`]).
    pub async fn drive_to_completion<R: Send + 'static>(
        &self,
        mut sm: CoroutineSM<R>,
    ) -> Result<R, DeltaError> {
        let sm_id = sm.sm_id();
        let sm_kind = sm.sm_kind();
        loop {
            // Zero-yield SMs have no operation to fetch; the first `advance` hands the stored
            // terminal value back directly. Fall back to an empty `PhaseState` in that case so
            // `advance` has a valid (unused) input.
            let phase_name = sm.phase_name();
            let phase_result = match sm.get_operation() {
                Ok(op) => self.run_phase(op, sm_id, sm_kind, phase_name).await,
                Err(_) => Ok(PhaseState::empty()),
            };
            match sm.advance(phase_result)? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(value) => return Ok(value),
            }
        }
    }

    /// Execute every plan in `plans` in order. `Relation` / `Load` plans register lazy table
    /// providers (no I/O); `Consume` plans drain physically and feed the active phase's
    /// [`PhaseState`].
    pub async fn execute_plans(&self, plans: &[Plan]) -> Result<(), DeltaError> {
        let state = PhaseState::empty();
        self.run_plans(plans, &state, Uuid::new_v4(), "standalone", "execute")
            .await
            .into_delta()
    }

    /// Run [`ResultPlan::plans`] and then collect every batch in the result relation.
    pub async fn collect_result(&self, rp: ResultPlan) -> Result<Vec<RecordBatch>, DeltaError> {
        self.execute_plans(&rp.plans).await?;
        self.collect_relation(&rp.result_relation).await
    }

    /// Stream every batch of a previously-registered relation by handle. Returns a single
    /// coalesced [`SendableRecordBatchStream`]; callers that want a buffered `Vec` should use
    /// [`Self::collect_relation`].
    pub async fn stream_relation(
        &self,
        handle: &RelationHandle,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        let provider = self
            .relation_providers
            .lock()
            .expect("relation_providers mutex poisoned")
            .get(handle.id.as_str())
            .cloned()
            .ok_or_else(|| {
                crate::error::plan_compilation(format!(
                    "no relation registered for handle id {} (name `{}`); the producing plan \
                     must run before any consumer reads",
                    handle.id, handle.name
                ))
            })
            .into_delta()?;
        let df = self.session_ctx.read_table(provider).into_delta()?;
        df.execute_stream().await.into_delta()
    }

    /// Collect every batch of a previously-registered relation. Thin wrapper over
    /// [`Self::stream_relation`] that drains the stream into a `Vec` and re-stamps each
    /// batch's nested field-declaration metadata to match the relation handle's declared
    /// schema (see [`crate::exec::stamp_batch_metadata`] for why).
    pub async fn collect_relation(
        &self,
        handle: &RelationHandle,
    ) -> Result<Vec<RecordBatch>, DeltaError> {
        let target_schema: SchemaRef = Arc::new(
            handle
                .schema
                .as_ref()
                .try_into_arrow()
                .map_err(|e: ArrowError| {
                    crate::error::internal_error(format!(
                        "collect_relation: failed to convert handle schema to arrow: {e}"
                    ))
                })
                .into_delta()?,
        );
        let batches: Vec<RecordBatch> = self
            .stream_relation(handle)
            .await?
            .try_collect()
            .await
            .into_delta()?;
        batches
            .iter()
            .map(|b| crate::exec::stamp_batch_metadata(b, &target_schema))
            .collect::<Result<_, _>>()
            .into_delta()
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
        self.run_phase(op, Uuid::new_v4(), "standalone", "execute")
            .await
    }

    /// Execute one [`PhaseOperation`] stamping any `Consume` handles minted during the run with
    /// `(sm_id, sm_kind, phase_name)`. Shared by [`Self::drive_to_completion`] (real SM identity)
    /// and [`Self::execute_phase_operation`] (standalone fallback).
    async fn run_phase(
        &self,
        op: PhaseOperation,
        sm_id: Uuid,
        sm_kind: &'static str,
        phase_name: &'static str,
    ) -> Result<PhaseState, EngineError> {
        match op {
            PhaseOperation::Plans(plans) => {
                let state = PhaseState::empty();
                self.run_plans(&plans, &state, sm_id, sm_kind, phase_name)
                    .await
                    .map_err(EngineError::internal)?;
                Ok(state)
            }
            PhaseOperation::SchemaQuery(node) => execute_schema_query_phase(&self.engine, node),
        }
    }

    /// Walk each plan in order, dispatching on its [`SinkType`]:
    /// - `Relation` -> compile the upstream to a `LogicalPlan`, wrap in a [`ViewTable`], and
    ///   register under the handle id. No physical plan or execution.
    /// - `Load`     -> compile the upstream to a `LogicalPlan`, wrap in a [`LoadTableProvider`]
    ///   (which captures sink + engine), and register under the sink's output handle id. No
    ///   physical plan or execution; the provider's `scan()` lowers + streams on first read.
    /// - `Consume`  -> compile, optimize, lower to a physical plan, and drain through
    ///   [`Self::drain_consume_sink`] (the only sink with eager side effects).
    async fn run_plans(
        &self,
        plans: &[Plan],
        state: &PhaseState,
        sm_id: Uuid,
        sm_kind: &'static str,
        phase_name: &'static str,
    ) -> Result<(), DataFusionError> {
        for plan in plans {
            // Snapshot the live relation registry into the compile context. The map is built
            // incrementally as plans run, so plan N sees every relation produced by plans 0..N.
            let providers = Arc::new(
                self.relation_providers
                    .lock()
                    .expect("relation_providers mutex poisoned")
                    .clone(),
            );
            let ctx = CompileContext {
                relation_providers: providers,
                phase_state: Some(state.clone()),
                engine: Arc::clone(&self.engine),
                sm_id,
                sm_kind,
                phase_name,
            };
            let logical = compile_plan_logical(plan, &ctx)?;
            match &plan.sink {
                SinkType::Relation(handle) => self.register_view_relation(handle, logical)?,
                SinkType::Load(sink) => self.register_load_relation(sink, logical, &ctx)?,
                SinkType::Consume(sink) => {
                    let df_state = self.session_ctx.state();
                    let physical = df_state
                        .create_physical_plan(&df_state.optimize(&logical)?)
                        .await?;
                    self.drain_consume_sink(physical, sink, &ctx).await?;
                }
            }
        }
        Ok(())
    }

    /// Wrap `logical` in a [`ViewTable`] and register it under `handle.id`. The view is opaque
    /// to plan-level execution: DataFusion's `InlineTableScan` analyzer inlines it into the
    /// consumer's tree when the consumer reads the relation, exposing every upstream node for
    /// predicate / projection pushdown + CSE.
    ///
    /// The upstream plan's runtime output is expected to match the kernel-declared relation
    /// schema field-for-field at every nesting level. Parquet scans get there via
    /// [`crate::exec::FieldIdPhysicalExprAdapterFactory`] (column-mapping-aware decode
    /// reshape); `LoadSink` outputs get there by handing kernel's parquet handler the
    /// kernel-declared logical schema so its built-in field-id matching + nested coerce step
    /// produces logically-shaped batches.
    fn register_view_relation(
        &self,
        handle: &RelationHandle,
        logical: datafusion_expr::LogicalPlan,
    ) -> Result<(), DataFusionError> {
        let provider: Arc<dyn TableProvider> = Arc::new(ViewTable::new(logical, None));
        self.relation_providers
            .lock()
            .expect("relation_providers mutex poisoned")
            .insert(handle.id.clone(), provider);
        Ok(())
    }

    /// Capture `(upstream_logical, sink, engine)` in a [`LoadTableProvider`] and register it
    /// under the sink's output handle id. The provider streams file rows lazily on
    /// [`TableProvider::scan`] via [`crate::exec::LoadExec`]; no I/O happens at registration.
    fn register_load_relation(
        &self,
        sink: &LoadSink,
        logical: datafusion_expr::LogicalPlan,
        ctx: &CompileContext,
    ) -> Result<(), DataFusionError> {
        let provider =
            LoadTableProvider::try_new(logical, Arc::new(sink.clone()), ctx.engine.clone())?;
        let provider: Arc<dyn TableProvider> = Arc::new(provider);
        self.relation_providers
            .lock()
            .expect("relation_providers mutex poisoned")
            .insert(sink.output_relation.id.clone(), provider);
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
        let mut handle = sink.new_handle(ctx.sm_id, ctx.sm_kind, ctx.phase_name);
        // Consume sinks are single-partition by construction; read partition 0 directly without
        // coalesce.
        let mut stream = physical.execute(0, Arc::clone(&self.task_ctx))?;
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
        let Some(state) = ctx.phase_state.as_ref() else {
            // Consume sink called outside of an active phase has nowhere to land its handle.
            let _: FinishedHandle = finished;
            return Err(crate::error::internal_error(
                "Consume sink drained without an active phase state to submit into",
            ));
        };
        state.submit_kdf_handle(finished);
        Ok(())
    }
}
