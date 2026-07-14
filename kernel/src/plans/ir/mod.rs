//! Plan intermediate representation.
//!
//! - [`operation`] -- top-level [`Operation`] dispatch (I/O vs query) consumed by
//!   [`PlanExecutor`](super::PlanExecutor).
//! - [`nodes`] -- the plan nodes: [`nodes::Operator`] and its payload structs.
//! - [`plan`] -- plan topology: [`plan::Plan`] holds a sequence of [`plan::PlanNode`]s wired into a
//!   DAG by their input node indices.
pub mod nodes;
pub mod operation;
pub mod plan;

pub use operation::{IoOperation, Operation};
