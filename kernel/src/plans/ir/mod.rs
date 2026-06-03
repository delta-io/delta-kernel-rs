//! Plan intermediate representation.
//!
//! - [`operation`] -- top-level [`Operation`] dispatch (I/O vs query) consumed by
//!   [`PlanExecutor`](super::PlanExecutor).
//! - [`nodes`] -- the plan nodes: [`nodes::NodeKind`] and its payload structs.
//! - [`plan`] -- plan topology: [`plan::Plan`] holds a sequence of [`plan::PlanNode`]s wired into a
//!   DAG by their [`plan::RefId`] inputs and outputs. The plan's terminal node is its last node;
//!   [`plan::Plan::result`] returns the [`plan::RefId`] whose rows the engine streams to the
//!   caller.

pub mod nodes;
pub mod operation;
pub mod plan;

pub use operation::{IoOperation, Operation, QueryPlan, QueryPlanNode};
