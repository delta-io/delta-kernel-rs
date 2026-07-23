//! A DataFusion-based [`PlanExecutor`] for delta_kernel declarative plans.
//!
//! Kernel emits executor-independent logical [`Plan`]s; this crate executes them by lowering
//! each plan to a DataFusion `LogicalPlan`, optimizing it, and running the resulting

// TODO: remove once `task_ctx` and `engine` are consumed by the query-execution path.
#![allow(dead_code)]

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::TaskContext;
use delta_kernel::Engine;

mod expression;
mod scalar;

pub use expression::kernel_to_df_expr;
pub use scalar::kernel_to_df_scalar;

/// Executes kernel declarative plans on DataFusion.
///
/// Holds three handles, each owning a distinct part of the work:
/// - `session_ctx` -- *plan it*: compiles and optimizes a kernel plan into a DataFusion
///   `LogicalPlan`, then lowers it to a physical `ExecutionPlan`.
/// - `task_ctx` -- *run it*: the per-execution runtime handle `ExecutionPlan::execute` requires.
/// - `engine` -- *fetch the bytes the query engine can't*: a kernel [`Engine`] for storage the
///   DataFusion query cannot perform itself (deletion-vector resolution, footer reads, listing).
pub struct DataFusionExecutor {
    session_ctx: SessionContext,
    task_ctx: Arc<TaskContext>,
    engine: Arc<dyn Engine>,
}

impl DataFusionExecutor {
    pub fn new(engine: Arc<dyn Engine>) -> Self {
        Self {
            session_ctx: SessionContext::new(),
            task_ctx: Arc::new(TaskContext::default()),
            engine,
        }
    }
}
