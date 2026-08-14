//! A DataFusion-based [`PlanExecutor`](delta_kernel::PlanExecutor) for delta_kernel declarative
//! plans.
//!
//! Kernel emits executor-independent logical [`Plan`](delta_kernel::plans::ir::plan::Plan)s; this
//! crate executes them by lowering each plan to a DataFusion `LogicalPlan`, optimizing it, and
//! running the resulting `ExecutionPlan`.

// TODO: remove once `session_ctx` and `storage_handler` are consumed by the query-execution path.
#![allow(dead_code)]

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use delta_kernel::StorageHandler;

mod expression;
mod predicate;
mod scalar;

pub use expression::to_df_expr;
pub use predicate::to_df_predicate_expr;
pub use scalar::to_df_scalar;

/// Executes kernel declarative plans on DataFusion.
///
/// Holds two handles, each owning a distinct part of the work:
/// - `session_ctx` -- *plan it, then run it*: DataFusion's `SessionContext` is the front door to
///   the query engine. It holds the session-scoped state needed to turn a query into something
///   runnable: configuration, registered tables/catalogs and functions, the logical/physical
///   optimizer rules, and a handle to the shared runtime environment (memory pool, object-store
///   registry). We use it to compile and optimize a kernel plan into a DataFusion `LogicalPlan`,
///   then lower it to a physical `ExecutionPlan`. It is heavyweight and meant to be long-lived and
///   shared. At execution time we derive a fresh per-run `TaskContext` from it via
///   `session_ctx.task_ctx()` and pass that to `ExecutionPlan::execute`.
/// - `storage_handler` -- *fetch the bytes the query engine can't*: a kernel [`StorageHandler`] for
///   the storage I/O DataFusion cannot do itself (deletion-vector resolution, footer reads,
///   listing). This is the file-system subset of a kernel [`Engine`](delta_kernel::Engine) -- the
///   executor needs nothing else from the engine, so it holds only this.
pub struct DataFusionExecutor {
    session_ctx: SessionContext,
    storage_handler: Arc<dyn StorageHandler>,
}

impl DataFusionExecutor {
    pub fn new(storage_handler: Arc<dyn StorageHandler>) -> Self {
        Self {
            session_ctx: SessionContext::new(),
            storage_handler,
        }
    }
}
