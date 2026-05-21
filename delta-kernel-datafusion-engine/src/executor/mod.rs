//! [`DataFusionExecutor`]: compiles and runs [`delta_kernel::plans::ir::Plan`] trees.
//!
//! `Relation` / `Load` sinks register lazy [`TableProvider`]s into `relation_providers`
//! keyed by `RelationHandle.id`; downstream `RelationRef` leaves resolve through that map.
//! `Consume` is the only sink with eager side effects -- it drains the physical plan into a
//! [`KernelConsumer`](delta_kernel::plans::kernel_consumers::KernelConsumer) handle.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::catalog::TableProvider;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::ViewTable;
use datafusion::execution::context::SessionContext;
use datafusion_common::arrow::datatypes::{FieldRef, Schema as ArrowSchema};
use datafusion_common::error::DataFusionError;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{ConsumeSink, LoadSink, RelationHandle, SinkType};
use delta_kernel::plans::ir::{Plan, ResultPlan};
use delta_kernel::plans::kernel_consumers::{FinishedHandle, KdfControl};
use delta_kernel::plans::operations::framework::coroutine::driver::Coroutine;
use delta_kernel::plans::operations::framework::engine_error::{EngineError, EngineErrorKind};
use delta_kernel::plans::operations::framework::state_machine::{NextStep, StateMachine};
use delta_kernel::plans::operations::framework::step::{SchemaQueryNode, Step};
use delta_kernel::plans::operations::framework::step_result::StepResult;
use delta_kernel::plans::operations::scan::FullState;
use delta_kernel::scan::Scan;
use delta_kernel::schema::StructType;
use delta_kernel::{Engine, Error as KernelError};
use futures::TryStreamExt;
use url::Url;
use uuid::Uuid;

use crate::compile::expr_translator::build_logical_projection;
use crate::compile::stamp_udf::StampFieldUdf;
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
) -> Result<StepResult, EngineError> {
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
    let state = StepResult::empty();
    state.submit_schema(footer.schema);
    Ok(state)
}

/// Minimal executor: a [`TaskContext`] for [`ExecutionPlan::execute`] calls, a [`SessionContext`]
/// for DataFusion compile/optimize/lower, a kernel [`Engine`] for IO helpers, and a
/// [`Mutex`]-guarded map of lazily-registered relations the compiler resolves
/// [`RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef) leaves against.
type RelationProviders = HashMap<String, Arc<dyn TableProvider>>;

pub struct DataFusionExecutor {
    task_ctx: Arc<TaskContext>,
    session_ctx: SessionContext,
    engine: Arc<dyn Engine>,
    relation_providers: Mutex<RelationProviders>,
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
        // `__common_expr_N` fields. Keep it disabled (apache/datafusion#20432 tracks the
        // upstream `build_extraction_projection_impl` dedup gap).
        session_config
            .options_mut()
            .optimizer
            .enable_leaf_expression_pushdown = false;
        let session_ctx = SessionContext::new_with_config(session_config);
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

    /// Locked view of the lazy-relation map. Maps mutex poison (only possible if a previous
    /// holder panicked) to a [`DataFusionError::Internal`] so the no-panic rule holds.
    fn providers_lock(&self) -> Result<MutexGuard<'_, RelationProviders>, DataFusionError> {
        self.relation_providers
            .lock()
            .map_err(|_| crate::error::internal_error("relation_providers mutex poisoned"))
    }

    /// Snapshot the provider registered under `handle_id`, if any. Lets callers inspect the
    /// provider's concrete type (e.g. distinguish a [`ViewTable`] from a materialized
    /// `MemTable`).
    #[cfg(test)]
    pub(crate) fn relation_provider(&self, handle_id: &str) -> Option<Arc<dyn TableProvider>> {
        self.providers_lock().ok()?.get(handle_id).cloned()
    }

    /// Snapshot of the underlying [`SessionContext`]'s state. Exposes the
    /// [`datafusion::execution::SessionState`] for direct
    /// [`datafusion::catalog::TableProvider::scan`] invocation to observe pushdown.
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
    /// is typically a [`ResultPlan`] the caller opens via [`Self::drive_to_dataframe`]
    /// (or, equivalently, by hand with [`Self::execute_plans`] + [`Self::read_relation`]).
    ///
    /// # `!Send` future
    ///
    /// The kernel state machine is a CPU-only sequencer (see
    /// [`Coroutine`](delta_kernel::plans::operations::framework::coroutine::driver::Coroutine)
    /// module docs); it intentionally does not implement `Send`. The future returned here
    /// inherits that and is therefore `!Send`. Callers needing a `Send` future drive this on a
    /// single-threaded runtime (`tokio::runtime::Builder::new_current_thread()` +
    /// `block_on`) or wrap the call in a [`tokio::task::LocalSet`].
    pub async fn drive_to_completion<R: 'static>(
        &self,
        mut sm: Coroutine<R>,
    ) -> Result<R, DeltaError> {
        let sm_id = sm.sm_id();
        let sm_kind = sm.sm_kind();
        loop {
            // Zero-yield SMs have no step to fetch; the first `submit` hands the stored
            // terminal value back directly. Fall back to an empty `StepResult` in that case so
            // `submit` has a valid (unused) input.
            let step_name = sm.step_name();
            let phase_result = match sm.get_step() {
                Ok(op) => self.run_phase(op, sm_id, sm_kind, step_name).await,
                Err(_) => Ok(StepResult::empty()),
            };
            match sm.submit(phase_result)? {
                NextStep::Continue => {}
                NextStep::Done(value) => return Ok(value),
            }
        }
    }

    /// Execute every plan in `plans` in order. `Relation` / `Load` plans register lazy table
    /// providers (no I/O); `Consume` plans drain physically and feed the active phase's
    /// [`StepResult`].
    pub async fn execute_plans(&self, plans: &[Plan]) -> Result<(), DeltaError> {
        let state = StepResult::empty();
        self.run_plans(plans, &state, Uuid::new_v4(), "standalone", "execute")
            .await
            .into_delta()
    }

    /// Drive a [`ResultPlan`]-returning SM and open its result relation as a [`DataFrame`].
    ///
    /// Combines [`Self::drive_to_completion`], [`Self::execute_plans`], and
    /// [`Self::read_relation`] into one call so callers that just want a DataFrame over an
    /// SM's output don't have to thread the intermediate [`ResultPlan`] through their own
    /// code. Useful for read-style SMs (`scan_state_machine`, `full_state`, ...) whose
    /// terminal value is always a `ResultPlan`.
    pub async fn drive_to_dataframe(
        &self,
        sm: Coroutine<ResultPlan>,
    ) -> Result<DataFrame, DeltaError> {
        let rp = self.drive_to_completion(sm).await?;
        self.execute_plans(&rp.plans).await?;
        self.read_relation(&rp.result_relation).await
    }

    /// Open a registered relation as a [`DataFrame`] whose logical schema matches
    /// `handle.schema` byte-for-byte: top-level column names, nested struct field
    /// names, *and* `delta.columnMapping.*` / `parquet.field.id` field metadata.
    /// Callers can run `.collect()`, `.execute_stream()`, `.filter(...)`, or further
    /// `.select(...)` directly against the kernel-logical schema without re-stamping.
    ///
    /// The underlying provider exposes the *physical* arrow schema (column-mapping
    /// id-renamed `col-<uuid>` names on nested fields, no Delta metadata). To bridge
    /// the gap this method wraps the raw provider DataFrame in a per-top-level-field
    /// projection built by [`build_logical_projection`]:
    ///
    /// - Primitive top-level fields pass through bare; the [`StampFieldUdf`] declares the
    ///   projection's output [`FieldRef`] so metadata flows into the schema.
    /// - Nested struct fields are reshaped into a `named_struct(get_field(...))` tree that emits
    ///   logical names at every depth.
    /// - `List<Struct>` / `Map<*, Struct>` (where supported) get an `array_transform` lambda that
    ///   recursively reshapes the element struct.
    ///
    /// This path replaces the historical post-collect `stamp_batch_metadata` shim --
    /// every reshape happens inside the logical plan so the projection is composable
    /// with downstream `df.filter` / `df.select` / coalesce ops without schema
    /// mismatches.
    pub async fn read_relation(&self, handle: &RelationHandle) -> Result<DataFrame, DeltaError> {
        let provider = self
            .providers_lock()
            .and_then(|g| {
                g.get(handle.id.as_str()).cloned().ok_or_else(|| {
                    crate::error::plan_compilation(format!(
                        "no relation registered for handle id {} (name `{}`); the producing \
                         plan must run before any consumer reads",
                        handle.id, handle.name
                    ))
                })
            })
            .into_delta()?;
        let raw_df = self.session_ctx.read_table(provider).into_delta()?;
        let projection =
            build_stamped_logical_projection(&raw_df, handle.schema.as_ref()).into_delta()?;
        raw_df.select(projection).into_delta()
    }

    /// Drive a combined metadata + data scan and return the data DataFrame.
    ///
    /// Sugar for `self.drive_to_dataframe(scan.scan_state_machine()?)`. The returned
    /// DataFrame carries the scan's logical schema (column-mapping renames + Delta
    /// metadata fully applied -- see [`Self::read_relation`]).
    pub async fn scan_data(&self, scan: &Scan) -> Result<DataFrame, DeltaError> {
        self.drive_to_dataframe(scan.scan_state_machine()?).await
    }

    /// Drive a metadata-only scan and return the live-actions DataFrame.
    ///
    /// Sugar for `self.drive_to_dataframe(scan.scan_metadata_state_machine()?)`. The
    /// returned DataFrame's rows are the reconciled `add` actions (path / size /
    /// `partitionValues_parsed` / `deletionVector` / ...). Callers wanting to inspect
    /// or cache the file set before opening the data scan should use this directly and
    /// feed the resulting relation to
    /// `scan.scan_data_from_metadata_state_machine(handle)` via
    /// [`Self::drive_to_dataframe`].
    pub async fn scan_metadata(&self, scan: &Scan) -> Result<DataFrame, DeltaError> {
        self.drive_to_dataframe(scan.scan_metadata_state_machine()?)
            .await
    }

    /// Drive a Full State Reconstruction and return the reconciled-actions DataFrame.
    ///
    /// Sugar for `self.drive_to_dataframe(fsr.state_machine()?)`. The result rows are
    /// the dedup'd action union (`Add` / `Remove` / `Metadata` / `Protocol` / `Txn` /
    /// `CDC` / `DomainMetadata`) that survives `_last_checkpoint` resolution plus
    /// commit-tail dedup. Useful for snapshot inspection and protocol/metadata
    /// queries.
    pub async fn full_state(&self, fsr: &FullState) -> Result<DataFrame, DeltaError> {
        self.drive_to_dataframe(fsr.state_machine()?).await
    }

    /// Execute a single [`Step`] against the executor and return the resulting
    /// [`StepResult`]. Used internally by [`Self::drive_to_completion`] and exposed for
    /// callers (typically tests) that need to drive an individual phase op directly --
    /// for example, draining a [`Consume`](SinkType::Consume) plan and inspecting
    /// the [`StepResult`] for its finalized handle.
    pub async fn execute_step(&self, op: Step) -> Result<StepResult, EngineError> {
        self.run_phase(op, Uuid::new_v4(), "standalone", "execute")
            .await
    }

    /// Execute one [`Step`], stamping any `Consume` handles minted during the run
    /// with `(sm_id, sm_kind, step_name)`.
    async fn run_phase(
        &self,
        op: Step,
        sm_id: Uuid,
        sm_kind: &'static str,
        step_name: &'static str,
    ) -> Result<StepResult, EngineError> {
        match op {
            Step::Plans(plans) => {
                let state = StepResult::empty();
                self.run_plans(&plans, &state, sm_id, sm_kind, step_name)
                    .await
                    .map_err(EngineError::internal)?;
                Ok(state)
            }
            Step::SchemaQuery(node) => execute_schema_query_phase(&self.engine, node),
            // SSA `Step::Consume` lands on the executor in a follow-on PR. Until kernel
            // SMs start emitting it (PR6+), the variant is unreachable in practice; we
            // surface a clear engine-level error so the SM body sees an actionable
            // failure rather than a panic if a future kernel build emits it ahead of
            // executor support.
            Step::Consume { .. } => Err(EngineError::internal(delta_kernel::delta_error!(
                delta_kernel::plans::errors::DeltaErrorCode::DeltaCommandInvariantViolation,
                "Step::Consume is not yet supported by the DataFusion executor",
            ))),
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
        state: &StepResult,
        sm_id: Uuid,
        sm_kind: &'static str,
        step_name: &'static str,
    ) -> Result<(), DataFusionError> {
        for plan in plans {
            // Snapshot the live relation registry into the compile context. The map is built
            // incrementally as plans run, so plan N sees every relation produced by plans 0..N.
            let providers = Arc::new(self.providers_lock()?.clone());
            let ctx = CompileContext {
                relation_providers: providers,
                step_result: Some(state.clone()),
                engine: Arc::clone(&self.engine),
                sm_id,
                sm_kind,
                step_name,
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

    /// Register `logical` as a [`ViewTable`] under `handle.id`; DataFusion's
    /// `InlineTableScan` analyzer inlines it into consumer trees so pushdown + CSE cross
    /// the boundary.
    fn register_view_relation(
        &self,
        handle: &RelationHandle,
        logical: datafusion_expr::LogicalPlan,
    ) -> Result<(), DataFusionError> {
        let provider: Arc<dyn TableProvider> = Arc::new(ViewTable::new(logical, None));
        self.providers_lock()?.insert(handle.id.clone(), provider);
        Ok(())
    }

    /// Bare `Values` upstream + no DV → register an eager [`EagerLoadTableProvider`] that
    /// produces a single [`DataSourceExec`] (DataFusion's native fan-out / pushdown apply).
    /// Anything else (non-Values upstream, DV present) → streaming [`LoadTableProvider`].
    /// On eager-build failure we fall through to streaming so the input still gets a
    /// diagnostic error from the same code path.
    fn register_load_relation(
        &self,
        sink: &LoadSink,
        logical: datafusion_expr::LogicalPlan,
        ctx: &CompileContext,
    ) -> Result<(), DataFusionError> {
        let lazy = || -> Result<Arc<dyn TableProvider>, DataFusionError> {
            Ok(Arc::new(LoadTableProvider::try_new(
                logical.clone(),
                Arc::new(sink.clone()),
                ctx.engine.clone(),
            )?))
        };
        let provider: Arc<dyn TableProvider> = match (&logical, sink.dv_ref.is_none()) {
            (LogicalPlan::Values(values), true) => {
                match crate::exec::EagerLoadTableProvider::try_new(sink, values) {
                    Ok(eager) => Arc::new(eager),
                    Err(_) => lazy()?,
                }
            }
            _ => lazy()?,
        };
        self.providers_lock()?
            .insert(sink.output_relation.id.clone(), provider);
        Ok(())
    }

    /// Drain `physical` through a
    /// [`KernelConsumer`](delta_kernel::plans::kernel_consumers::KernelConsumer) handle
    /// minted from `sink`. The finalized handle submits into the active phase's
    /// [`StepResult`].
    async fn drain_consume_sink(
        &self,
        physical: Arc<dyn ExecutionPlan>,
        sink: &ConsumeSink,
        ctx: &CompileContext,
    ) -> Result<(), DataFusionError> {
        let mut handle = sink.new_handle(ctx.sm_id, ctx.sm_kind, ctx.step_name);
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
        let Some(state) = ctx.step_result.as_ref() else {
            // Consume sink called outside of an active phase has nowhere to land its handle.
            let _: FinishedHandle = finished;
            return Err(crate::error::internal_error(
                "Consume sink drained without an active phase state to submit into",
            ));
        };
        state.submit_consumer_handle(finished);
        Ok(())
    }
}

/// Bridge `raw_df`'s physical arrow schema to `target`'s logical kernel schema.
///
/// Builds one projection [`Expr`] per top-level kernel field by combining:
///
/// 1. The recursive rename walker (`build_logical_projection`) which emits
///    `named_struct(get_field(...))` for nested struct renames and `array_transform(_, lambda)` for
///    `List<Struct>` element renames.
/// 2. A per-top-level [`StampFieldUdf`] wrapper that declares the kernel's logical Arrow
///    [`FieldRef`] (name, datatype, recursive field metadata including `delta.columnMapping.*` /
///    `parquet.field.id`) on the projection's output schema.
/// 3. An `Expr::alias(target_name)` so the projection's column name matches the kernel logical name
///    even for primitive fields (where [`ScalarUDFImpl::return_field_from_args`] discards the
///    top-level field name per its rustdoc).
///
/// Returns one `Expr` per top-level kernel field in declared order, ready to feed
/// directly to [`DataFrame::select`].
fn build_stamped_logical_projection(
    raw_df: &DataFrame,
    target: &StructType,
) -> Result<Vec<Expr>, DataFusionError> {
    let source_schema: &ArrowSchema = raw_df.schema().as_arrow();
    let rename_exprs = build_logical_projection(source_schema, target)?;

    // Convert the kernel target schema to an arrow schema once so each top-level field
    // surfaces as a fully-populated `FieldRef` with `delta.columnMapping.*` /
    // `parquet.field.id` metadata applied recursively (the kernel -> arrow conversion
    // walks into nested struct / list / map fields and stamps each level).
    let arrow_target: ArrowSchema = target.try_into_arrow().map_err(|e| {
        crate::error::plan_compilation(format!(
            "read_relation: failed to convert kernel target schema to arrow: {e}"
        ))
    })?;

    let mut stamped: Vec<Expr> = Vec::with_capacity(rename_exprs.len());
    for (rename, target_field) in rename_exprs.into_iter().zip(arrow_target.fields().iter()) {
        let target_field: FieldRef = Arc::clone(target_field);
        let logical_name = target_field.name().to_string();
        let stamp = StampFieldUdf::new(target_field).call(rename);
        stamped.push(stamp.alias(logical_name));
    }
    Ok(stamped)
}
