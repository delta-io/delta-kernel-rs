//! SSA-plan -> DataFusion [`datafusion_expr::LogicalPlan`] lowering.
//!
//! See [`ssa::compile_ssa`] for the entry point. The submodules host per-shape lowering
//! helpers (file listings, scans, projections, ordered union, output canonicalization).

mod canonicalize;
mod ordered_union;
mod project;
mod providers;
mod scan;
mod ssa;

pub use ssa::compile_ssa;
