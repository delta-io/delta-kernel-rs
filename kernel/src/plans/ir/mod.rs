//! Plan intermediate representation.
//!
//! - [`operation`] -- top-level [`Operation`] dispatch (I/O vs query) consumed by
//!   [`PlanExecutor`](super::PlanExecutor).
//! - [`expression`] -- resolved expression and predicate nodes that may cross the plan boundary.
//! - [`nodes`] -- the plan nodes: [`nodes::Operator`] and its payload structs.
//! - [`plan`] -- plan topology: [`plan::Plan`] holds a sequence of [`plan::PlanNode`]s wired into a
//!   DAG by their input node indices.
pub mod expression;
pub mod nodes;
pub mod operation;
pub mod plan;

pub use operation::{IoOperation, Operation};
