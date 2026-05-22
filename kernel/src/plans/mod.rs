//! SSA plan IR and execution framework.
//!
//! The `plans` module defines an engine-agnostic intermediate representation for the work an
//! engine must perform on behalf of the kernel (scan files, apply filters, project columns,
//! collect results) along with the state-machine framework that drives execution.
//!
//! Entry points:
//! - [`crate::plans::ir::ssa`] -- SSA `Plan` / `Stmt` / `Node` / `Ref` and the terminal
//!   `ResultPlan` the engine compiles to a single dataflow DAG.
//! - [`crate::plans::operations::framework::plan_context`] -- the SSA `Context` / `Cursor` API SM
//!   bodies use to construct plans.
//! - [`crate::plans::operations::framework::coroutine`] -- coroutine-backed `StateMachine`
//!   implementation.
//!
//! # Feature gate
//!
//! This module is opt-in behind `declarative-plans`. The kernel's existing
//! `Scan`/`Snapshot`/`Transaction` APIs continue to work without it.

pub mod errors;
pub mod ir;
pub mod kernel_consumers;
pub mod operations;
pub mod record_types;
