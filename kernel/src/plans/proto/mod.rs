//! Protobuf wire format mirroring the kernel's plan / schema / expression IR.
//!
//! Each submodule wraps the prost-generated code for one `.proto` file in
//! `kernel/proto/`. Consumers can serialise a kernel-built plan and ship it
//! across a process / language boundary (Rust kernel -> JVM engine, etc.).
//!
//! - [`schema`] -- mirror of `kernel/src/schema/mod.rs` (`DataType`, `PrimitiveType`, `StructType`,
//!   ...).
//! - [`expressions`] -- mirror of `kernel/src/expressions/mod.rs` and `scalars.rs` (`Expression`,
//!   `Predicate`, `Scalar`, `ColumnName`, ...).
//! - [`plan`] -- mirror of `kernel/src/plans/ir/{plan,nodes}.rs` (`Plan`, `PlanNode`, `Operator`,
//!   per-variant payload messages).
//! - [`operation`] -- mirror of `kernel/src/plans/ir/operation.rs` (`Operation`, `IoOperation`, and
//!   the I/O payload messages).

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.schema.rs"));
}

pub mod expressions {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.expressions.rs"));
}

pub mod plan {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.plan.rs"));
}

// The top-level `Operation` message's oneof generates a nested `mod operation`, which trips
// `clippy::module_inception` against this same-named module. The name mirrors the `.proto` file
// (matching the other proto modules), so suppress the lint rather than rename.
#[allow(clippy::module_inception)]
pub mod operation {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.operation.rs"));
}

mod convert;
