//! Protobuf wire format mirroring the kernel's plan / schema / expression IR.
//!
//! Each submodule wraps the prost-generated code for one `.proto` file in
//! `kernel/proto/`. Consumers can serialise a kernel-built plan and ship it
//! across a process / language boundary (Rust kernel -> JVM engine, etc.); the
//! Rust kernel produces these messages but does not interpret them on receipt.
//!
//! - [`schema`] -- mirror of `kernel/src/schema/mod.rs` (`DataType`, `PrimitiveType`, `StructType`,
//!   ...).
//! - [`expressions`] -- mirror of `kernel/src/expressions/mod.rs` and `scalars.rs` (`Expression`,
//!   `Predicate`, `Scalar`, `ColumnName`, ...).
//! - [`plan`] -- mirror of `kernel/src/plans/ir/{plan,nodes}.rs` (`Plan`, `PlanNode`, `NodeKind`,
//!   `RefId`, per-variant payload messages).
//!
//! # Stability
//!
//! Wire compatibility follows the per-file conventions in each `.proto` file: new IR
//! variants get the next unused `oneof` field number, and once published a field number is
//! never reused. There is no top-level wire version field.

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.schema.rs"));
}

pub mod expressions {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.expressions.rs"));
}

pub mod plan {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.plan.rs"));
}
