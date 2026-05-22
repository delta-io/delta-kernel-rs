//! Plan intermediate representation.
//!
//! - [`nodes`] — node payload types shared with the engine compile path ([`nodes::ConsumeSink`],
//!   [`nodes::LoadSink`], [`nodes::ScanNode`], etc.).
//! - [`ssa`] — typed [`ssa::Plan`] / [`ssa::Stmt`] / [`ssa::Node`] / [`ssa::Ref`] /
//!   [`ssa::ResultPlan`]. Pure compute; no sinks, no named relations, no engine state side effects.
//!   The canonical kernel plan IR.
pub mod nodes;
pub mod schema_inference;
pub mod ssa;

pub use crate::plans::kernel_consumers::Extractor;
