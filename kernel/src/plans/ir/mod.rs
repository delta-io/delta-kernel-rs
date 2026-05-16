//! Declarative plan intermediate representation.
//!
//! - [`nodes`] -- individual node types (leaves, transforms, sink).
//! - [`declarative`] -- the [`DeclarativePlanNode`] tree enum and its chain construction API (leaf
//!   constructors, transforms, terminals).
//! - [`plan`] -- the [`Plan`] envelope handed to the engine.

pub mod declarative;
pub mod nodes;
pub mod plan;

pub use declarative::{DeclarativePlanNode, Extractor};
pub use nodes::{ConsumeByKdfSink, RelationHandle};
pub use plan::Plan;
