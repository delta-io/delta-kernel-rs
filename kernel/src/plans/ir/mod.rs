//! Declarative plan intermediate representation.
//!
//! - [`nodes`] -- individual node types (leaves, transforms, sink).
//! - [`declarative`] -- the [`DeclarativePlanNode`] tree enum and its chain construction API (leaf
//!   constructors, transforms, terminals).
//! - [`plan`] -- the [`Plan`] envelope handed to the engine.
//! - [`fluent`] -- top-level combinators (`relation_ref`, `scan_json`, [`PlanCollector`]) that read
//!   as Delta protocol prose. Thin wrappers around [`DeclarativePlanNode`]; no new IR variants.

pub mod declarative;
pub mod fluent;
pub mod nodes;
pub mod plan;

pub use declarative::{DeclarativePlanNode, Extractor};
pub use fluent::{PlanCollector, RelationRefBuilder, ScanLoadBuilder, TerminalBuilder};
pub use nodes::{ConsumeByKdfSink, RelationHandle};
pub use plan::Plan;
