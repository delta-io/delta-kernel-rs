//! Declarative plan intermediate representation.
//!
//! - [`nodes`] -- individual node types (leaves, transforms, sink).
//! - [`declarative`] -- the [`DeclarativePlanNode`] tree enum and its chain construction API (leaf
//!   constructors, transforms, terminals).
//! - [`plan`] -- the [`Plan`] envelope handed to the engine.
//! - [`fluent`] -- plan-collector plus scan / load entry points. All transformation methods live
//!   directly on [`DeclarativePlanNode`]; this module supplies only the assembly surface that mints
//!   fresh [`RelationHandle`]s and binds chains to sinks.

pub mod declarative;
pub mod fluent;
pub mod nodes;
pub mod plan;

pub use declarative::{DeclarativePlanNode, Extractor};
pub use fluent::{
    load_files, load_json, load_parquet, relation_ref, scan_files, LoadSpec, PlanCollector,
};
pub use nodes::{ConsumeByKdfSink, RelationHandle};
pub use plan::Plan;
