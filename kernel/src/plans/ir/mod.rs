//! Declarative plan intermediate representation.
//!
//! - [`nodes`] -- individual node types (leaves, transforms, sink), including
//!   [`LoadSink`](nodes::LoadSink).
//! - [`declarative`] -- the [`DeclarativePlanNode`] tree enum and its chain construction API (leaf
//!   constructors, transforms, terminals).
//! - [`plan`] -- the [`Plan`] envelope handed to the engine.
//! - [`relation_registry`] -- registry for relation refs and sink registration.
pub mod declarative;
pub mod nodes;
pub mod plan;
pub mod relation_registry;

pub use declarative::{DeclarativePlanNode, PlanBuilder};
pub use nodes::{ConsumeSink, RelationHandle};
pub use plan::{Plan, ResultPlan};
pub use relation_registry::RelationRegistry;

pub use crate::plans::kdf::Extractor;
