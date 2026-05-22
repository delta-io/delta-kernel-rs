//! [`DataFusionExecutor`]: drives kernel SSA coroutine state machines and compiles their
//! step payloads to DataFusion plans.
//!
//! Every step the SM yields is either a `EngineRequest::SchemaQuery` (parquet footer read) or a
//! `EngineRequest::Consume` (SSA dataflow drained into a [`ConsumeSink`]). Terminal `ResultPlan`s
//! describe a single self-contained dataflow DAG that compiles to a `LogicalPlan`.
//!
//! [`ConsumeSink`]: delta_kernel::plans::ir::nodes::ConsumeSink

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion_common::error::DataFusionError;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::ConsumeSink;
use delta_kernel::plans::kernel_consumers::{FinishedHandle, KdfControl};
use delta_kernel::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use delta_kernel::plans::state_machines::framework::engine_error::{EngineError, EngineErrorKind};
use delta_kernel::plans::state_machines::framework::state_machine::{NextStep, StateMachine};
use delta_kernel::plans::state_machines::framework::step::{EngineRequest, SchemaQuery};
use delta_kernel::plans::state_machines::framework::step_payload::EngineResponse;
use delta_kernel::plans::state_machines::scan::FullState;
use delta_kernel::scan::Scan;
use delta_kernel::{Engine, Error as KernelError};
use futures::TryStreamExt;
use url::Url;
use uuid::Uuid;

use crate::compile::{compile_ssa, CompileContext};
use crate::error::DfResultIntoDelta;

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
    node: SchemaQuery,
) -> Result<EngineResponse, EngineError> {
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
    Ok(EngineResponse::Schema(footer.schema))
}

/// Minimal executor: a [`TaskContext`] for [`ExecutionPlan::execute`] calls, a [`SessionContext`]
/// for DataFusion compile/optimize/lower, and a kernel [`Engine`] for IO helpers.
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
        })
    }

    /// Reference to the kernel [`Engine`] this executor uses for IO helpers.
    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    // ================================================================
    // High-level SM and result-plan driving
    // ================================================================

    /// Drive `sm` until it terminates, executing any intermediate phase operations it yields
    /// (kernel-side decision plans, schema queries) and returning the SM's terminal value.
    ///
    /// The terminal value is whatever `R` the SM was constructed for: for read-style SMs that
    /// is typically a [`ResultPlan`] the caller compiles via
    /// [`Self::ssa_result_to_dataframe`].
    ///
    /// # `!Send` future
    ///
    /// The kernel state machine is a CPU-only sequencer (see
    /// [`delta_kernel::plans::state_machines::framework::coroutine::driver::CoroutineSM`] module
    /// docs); it intentionally does not implement `Send`. The future returned here
    /// inherits that and is therefore `!Send`. Callers needing a `Send` future drive this on a
    /// single-threaded runtime (`tokio::runtime::Builder::new_current_thread()` +
    /// `block_on`) or wrap the call in a [`tokio::task::LocalSet`].
    ///
    /// [`ResultPlan`]: delta_kernel::plans::ir::plan::ResultPlan
    pub async fn drive_to_completion<R: 'static>(
        &self,
        mut sm: CoroutineSM<R>,
    ) -> Result<R, DeltaError> {
        let sm_id = sm.sm_id();
        let sm_kind = sm.sm_kind();
        loop {
            // Zero-yield SMs have no step to fetch; the first `submit` hands the stored
            // terminal value back directly. Pass [`EngineResponse::Empty`] in that case so
            // `submit` has a valid (unused) input.
            let step_name = sm.step_name();
            let phase_result = match sm.get_step() {
                Ok(op) => self.run_phase(op, sm_id, sm_kind, step_name).await,
                Err(_) => Ok(EngineResponse::Empty),
            };
            match sm.submit(phase_result)? {
                NextStep::Continue => {}
                NextStep::Done(value) => return Ok(value),
            }
        }
    }

    /// Drive a coroutine that yields an [`ResultPlan`] and open its terminal output as a
    /// [`DataFrame`]. SSA plans describe a single self-contained dataflow DAG; the compiled
    /// `LogicalPlan` is wrapped directly in a [`DataFrame`] for the caller.
    ///
    /// [`ResultPlan`]: delta_kernel::plans::ir::plan::ResultPlan
    pub async fn drive_ssa_to_dataframe(
        &self,
        sm: CoroutineSM<delta_kernel::plans::ir::plan::ResultPlan>,
    ) -> Result<DataFrame, DeltaError> {
        let rp = self.drive_to_completion(sm).await?;
        self.ssa_result_to_dataframe(&rp)
    }

    /// Compile an [`ResultPlan`] to a [`DataFrame`]. Useful for callers that already hold
    /// a `ResultPlan` (for example after driving a coroutine by hand) and don't need the
    /// `CoroutineSM` wrapping that [`Self::drive_ssa_to_dataframe`] provides; also the
    /// canonical entry point for tests that construct SSA plans directly without an SM.
    ///
    /// [`ResultPlan`]: delta_kernel::plans::ir::plan::ResultPlan
    pub fn ssa_result_to_dataframe(
        &self,
        rp: &delta_kernel::plans::ir::plan::ResultPlan,
    ) -> Result<DataFrame, DeltaError> {
        let ctx = CompileContext {
            engine: Arc::clone(&self.engine),
            sm_id: Uuid::new_v4(),
            sm_kind: "standalone",
            step_name: "ssa_result_to_dataframe",
        };
        let logical = compile_ssa(&rp.plan.stmts, rp.result, &ctx).into_delta()?;
        Ok(DataFrame::new(self.session_ctx.state(), logical))
    }

    /// Drive a combined metadata + data scan and return the data DataFrame.
    ///
    /// Sugar for `self.drive_ssa_to_dataframe(scan.scan_state_machine()?)`.
    pub async fn scan_data(&self, scan: &Scan) -> Result<DataFrame, DeltaError> {
        self.drive_ssa_to_dataframe(scan.scan_state_machine()?)
            .await
    }

    /// Drive a metadata-only scan and return the live-actions DataFrame.
    ///
    /// Sugar for `self.drive_ssa_to_dataframe(scan.scan_metadata_state_machine()?)`.
    pub async fn scan_metadata(&self, scan: &Scan) -> Result<DataFrame, DeltaError> {
        self.drive_ssa_to_dataframe(scan.scan_metadata_state_machine()?)
            .await
    }

    /// Drive a Full State Reconstruction and return the reconciled-actions DataFrame.
    ///
    /// Sugar for `self.drive_ssa_to_dataframe(fsr.state_machine()?)`.
    pub async fn full_state(&self, fsr: &FullState) -> Result<DataFrame, DeltaError> {
        self.drive_ssa_to_dataframe(fsr.state_machine()?).await
    }

    /// Execute a single [`EngineRequest`] against the executor and return the resulting
    /// [`EngineResponse`]. Used internally by [`Self::drive_to_completion`] and exposed for
    /// callers (typically tests) that need to drive an individual phase op directly.
    pub async fn execute_step(&self, op: EngineRequest) -> Result<EngineResponse, EngineError> {
        self.run_phase(op, Uuid::new_v4(), "standalone", "execute")
            .await
    }

    /// Execute one [`EngineRequest`], stamping any `Consume` handles minted during the run
    /// with `(sm_id, sm_kind, step_name)`.
    async fn run_phase(
        &self,
        op: EngineRequest,
        sm_id: Uuid,
        sm_kind: &'static str,
        step_name: &'static str,
    ) -> Result<EngineResponse, EngineError> {
        match op {
            EngineRequest::SchemaQuery(node) => execute_schema_query_phase(&self.engine, node),
            EngineRequest::Consume {
                stmts,
                terminal,
                sink,
            } => {
                let finished = self
                    .run_consume(&stmts, terminal, &sink, sm_id, sm_kind, step_name)
                    .await
                    .map_err(EngineError::internal)?;
                Ok(EngineResponse::Consumer(finished))
            }
        }
    }

    /// Compile an SSA [`EngineRequest::Consume`] into a DataFusion physical plan, drain it through
    /// the consume sink, and return the finalized handle.
    async fn run_consume(
        &self,
        stmts: &[delta_kernel::plans::ir::plan::PlanNode],
        terminal: delta_kernel::plans::ir::plan::Ref,
        sink: &ConsumeSink,
        sm_id: Uuid,
        sm_kind: &'static str,
        step_name: &'static str,
    ) -> Result<FinishedHandle, DataFusionError> {
        let ctx = CompileContext {
            engine: Arc::clone(&self.engine),
            sm_id,
            sm_kind,
            step_name,
        };
        let logical = compile_ssa(stmts, terminal, &ctx)?;
        let df_state = self.session_ctx.state();
        let physical = df_state
            .create_physical_plan(&df_state.optimize(&logical)?)
            .await?;
        self.drain_consume_sink(physical, sink, &ctx).await
    }

    /// Drain `physical` through a
    /// [`KernelConsumer`](delta_kernel::plans::kernel_consumers::KernelConsumer) handle
    /// minted from `sink` and return the finalized handle.
    async fn drain_consume_sink(
        &self,
        physical: Arc<dyn ExecutionPlan>,
        sink: &ConsumeSink,
        ctx: &CompileContext,
    ) -> Result<FinishedHandle, DataFusionError> {
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
        Ok(handle.finish())
    }
}
