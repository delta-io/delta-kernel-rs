//! Custom physical operators for the DataFusion engine.

mod literal;
mod shape;
mod sources;

pub use literal::build_literal_exec;
pub use shape::{
    KernelAssertExec, KernelConsumeByKdfExec, KernelLoadSinkExec, KernelPartitionedWriteExec,
    NullabilityValidationExec, OrderedUnionExec,
};
pub use sources::{
    build_relation_ref_exec, build_relation_ref_logical, FileListingExec, RelationBatchRegistry,
};
