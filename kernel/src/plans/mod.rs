//! Declarative plan IR and construction API.
//!
//! The `plans` module defines a typed, engine-agnostic intermediate representation
//! for the work an engine must perform on behalf of the kernel: scan files, apply
//! filters, project columns, collect results. See [`ir::DeclarativePlanNode`] for
//! the tree representation, [`ir::Plan`] for the envelope the kernel hands to the
//! engine, and [`ir::PlanBuilder`] for the fluent building API that produces these
//! trees with their cumulative output schema.
//!
//! # Feature gate
//!
//! This module is opt-in behind `declarative-plans`. The kernel's existing
//! `Scan`/`Snapshot`/`Transaction` APIs continue to work without it.
//!
//! # Overview
//!
//! A [`ir::Plan`] is `{ root: DeclarativePlanNode, sink: SinkType }`: a
//! transforms-only tree terminated by a sink describing how the engine should
//! consume the row stream. Trees are built bottom-up with chain methods:
//!
//! ```ignore
//! use delta_kernel::plans::ir::PlanBuilder;
//! use delta_kernel::plans::ir::RelationRegistry;
//!
//! let mut registry = RelationRegistry::new();
//! let plan = PlanBuilder::scan_json(files, schema.clone())
//!     .filter(predicate)
//!     .project(projection, output_schema)
//!     .into_relation("results", &mut registry)?;
//! ```

pub mod errors;
pub mod ir;
pub mod kdf;
pub mod record_schemas;
pub mod state_machines;
