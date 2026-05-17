//! Declarative plan intermediate representation.
//!
//! - [`nodes`] -- individual node types (leaves, transforms, sink), including
//!   [`LoadSink`](nodes::LoadSink).
//! - [`declarative`] -- the [`DeclarativePlanNode`] tree enum, its chain construction API (leaf
//!   constructors, transforms, terminals), and the free-function `relation_ref` entry point.
//! - [`plan`] -- the [`Plan`] envelope handed to the engine plus [`PlanCollector`] for assembling
//!   plan vectors that mint fresh [`RelationHandle`]s and bind chains to sinks.
pub mod declarative;
pub mod nodes;
pub mod plan;

pub use declarative::DeclarativePlanNode;
pub use nodes::{ConsumeSink, RelationHandle};
pub use plan::{identity_project_struct, Plan, PlanCollector, ResultPlan};
pub use crate::plans::kdf::Extractor;
