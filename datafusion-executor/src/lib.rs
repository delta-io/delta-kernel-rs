//! A DataFusion-based [`PlanExecutor`] for delta_kernel declarative plans.
//!
//! Kernel emits executor-independent logical [`Plan`]s; this crate executes them by lowering
//! each plan to a DataFusion `LogicalPlan`, optimizing it, and running the resulting
//! `ExecutionPlan`.
