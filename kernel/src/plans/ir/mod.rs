//! Plan intermediate representation.
//!
//! - [`nodes`] — node payload types shared with the engine compile path ([`nodes::ConsumeSink`],
//!   [`nodes::LoadSink`], [`nodes::ScanNode`], etc.).
//! - [`plan`] — typed [`plan::Plan`] / [`plan::PlanNode`] / [`plan::NodeKind`] / [`plan::Ref`] /
//!   [`plan::ResultPlan`]. Pure compute; no sinks, no named relations, no engine state side
//!   effects. The canonical kernel plan IR.
pub mod nodes;
pub mod plan;
pub mod schema_inference;

pub use crate::plans::kernel_consumers::Extractor;
