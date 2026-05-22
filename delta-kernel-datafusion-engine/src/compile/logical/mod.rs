//! SSA-plan -> DataFusion [`datafusion_expr::LogicalPlan`] lowering.
//!
//! See [`ssa::compile_ssa`] for the entry point. The submodules host the per-shape lowering
//! helpers shared with the (deleted) legacy compile path.

mod canonicalize;
mod ordered_union;
mod project;
mod providers;
mod scan;
mod ssa;

pub use ssa::compile_ssa;
