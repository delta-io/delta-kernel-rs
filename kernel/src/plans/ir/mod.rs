//! Declarative plan intermediate representation.
//!
//! - [`nodes`] — individual node types (leaves, transforms, sink).
//! - [`declarative`] — the [`DeclarativePlanNode`] tree enum and its chain construction API (leaf
//!   constructors, transforms, terminals).
//! - [`plan`] — the [`Plan`] envelope handed to the engine.
//! - [`expr_ext`] — fluent extension traits and free functions for [`Expression`] / [`Predicate`]
//!   construction. Plan builders use these so the IR tree reads as method chains rather than
//!   static-fn forests.
//!
//! [`Expression`]: crate::expressions::Expression
//! [`Predicate`]: crate::expressions::Predicate

pub mod declarative;
pub mod expr_ext;
pub mod nodes;
pub mod plan;

pub use declarative::{DeclarativePlanNode, Extractor};
pub use nodes::{ConsumeByKdfSink, RelationHandle};
pub use plan::Plan;
