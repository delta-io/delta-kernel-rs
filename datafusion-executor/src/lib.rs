//! A DataFusion-based [`PlanExecutor`](delta_kernel::PlanExecutor) for delta_kernel declarative
//! plans.
//!
//! Kernel emits executor-independent logical [`Plan`](delta_kernel::plans::ir::plan::Plan)s; this
//! crate executes them by lowering each plan to a DataFusion `LogicalPlan`, optimizing it, and
//! running the resulting `ExecutionPlan`.
