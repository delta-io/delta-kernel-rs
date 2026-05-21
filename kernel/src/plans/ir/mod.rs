//! Plan intermediate representation.
//!
//! Two IR shapes coexist during the SSA migration. The recursive tree shape is the
//! original kernel IR; the SSA shape is the migration target (see RENAME_PROPOSAL.md).
//!
//! Recursive tree (legacy):
//! - [`nodes`] -- individual node types (leaves, transforms, sink), including
//!   [`LoadSink`](nodes::LoadSink).
//! - [`declarative`] -- the [`DeclarativePlanNode`] tree enum and its chain construction API (leaf
//!   constructors, transforms, terminals).
//! - [`plan`] -- the [`Plan`] envelope handed to the engine.
//! - [`relation_registry`] -- registry for relation refs and sink registration.
//!
//! SSA (target):
//! - [`ssa`] -- typed [`ssa::Plan`] / [`ssa::Stmt`] / [`ssa::Node`] / [`ssa::Ref`] /
//!   [`ssa::ResultPlan`]. Pure compute; no sinks, no named relations, no engine state side effects.
//!   Has no callers yet; cursor builders land in a follow-on PR.
pub mod declarative;
pub mod nodes;
pub mod plan;
pub mod relation_registry;
pub mod schema_inference;
pub mod ssa;

pub use declarative::{DeclarativePlanNode, PlanBuilder};
pub use nodes::{ConsumeSink, RelationHandle};
pub use plan::{Plan, ResultPlan};
pub use relation_registry::RelationRegistry;

pub use crate::plans::kernel_consumers::Extractor;
