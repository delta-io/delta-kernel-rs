//! This module defines the concept of a PlanExecutor and its associated input + output types.
//!
//! This module is opt-in behind the `declarative-plans` feature flag.
pub mod errors;
pub mod ir;
pub mod proto;
mod query_builder;

use bytes::Bytes;
pub use ir::{IoOperation, Operation};
pub use query_builder::QueryPlanBuilder;

use crate::{
    AsAny, DeltaResult, DeltaResultIteratorStatic, EngineData, Error, FileMeta, ParquetFooter,
};

/// Provides the ability to execute declarative plans to the Delta Kernel.
///
/// This gives the kernel the ability to execute data-intensive operations by constructing a
/// declarative, relational plan algebra, without prescribing *how* to do it.
pub trait PlanExecutor: AsAny {
    /// Executes the given declarative plan and returns the result.
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult>;
}

/// The result of executing an [`Operation`].
///
/// Each variant describes a different shape of output that a plan can possibly produce.
pub enum PlanResult {
    /// A stream of columnar data batches (as [`EngineData`]) produced by the plan.
    Data(DeltaResultIteratorStatic<Box<dyn EngineData>>),
    /// A stream of file metadata entries.
    FileMeta(DeltaResultIteratorStatic<FileMeta>),
    /// A stream of raw byte buffers.
    Bytes(DeltaResultIteratorStatic<Bytes>),
    /// Metadata extracted from a Parquet file footer.
    ParquetFooter(ParquetFooter),
    /// Represents the successful completion of a plan, but with no return value.
    Unit,
}

impl PlanResult {
    /// Consumes the PlanResult and extracts the inner iterator of EngineData (assuming that it is a
    /// PlanResult::Data variant). Returns an error if the PlanResult is not the expected variant.
    pub fn into_data(self) -> DeltaResult<DeltaResultIteratorStatic<Box<dyn EngineData>>> {
        match self {
            Self::Data(iter) => Ok(iter),
            other => Err(other.type_mismatch("Data")),
        }
    }

    /// Consumes the PlanResult and extracts the inner iterator of FileMeta (assuming that it is a
    /// PlanResult::FileMeta variant). Returns an error if the PlanResult is not the expected
    /// variant.
    pub fn into_file_meta(self) -> DeltaResult<DeltaResultIteratorStatic<FileMeta>> {
        match self {
            Self::FileMeta(iter) => Ok(iter),
            other => Err(other.type_mismatch("FileMeta")),
        }
    }

    /// Consumes the PlanResult and extracts the inner iterator of Bytes (assuming that it is a
    /// PlanResult::Bytes variant). Returns an error if the PlanResult is not a PlanResult::Bytes
    /// variant.
    pub fn into_bytes(self) -> DeltaResult<DeltaResultIteratorStatic<Bytes>> {
        match self {
            Self::Bytes(iter) => Ok(iter),
            other => Err(other.type_mismatch("Bytes")),
        }
    }

    /// Consumes the PlanResult and extracts the inner [`ParquetFooter`] (assuming that it is a
    /// PlanResult::ParquetFooter variant). Returns an error if the PlanResult is not the expected
    /// variant.
    pub fn into_parquet_footer(self) -> DeltaResult<ParquetFooter> {
        match self {
            Self::ParquetFooter(footer) => Ok(footer),
            other => Err(other.type_mismatch("ParquetFooter")),
        }
    }

    /// Consumes the PlanResult, verifying that it is a PlanResult::Unit variant. Returns an error
    /// if the PlanResult is not the expected variant.
    pub fn into_unit(self) -> DeltaResult<()> {
        match self {
            Self::Unit => Ok(()),
            other => Err(other.type_mismatch("Unit")),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Self::Data(_) => "Data",
            Self::FileMeta(_) => "FileMeta",
            Self::Bytes(_) => "Bytes",
            Self::ParquetFooter(_) => "ParquetFooter",
            Self::Unit => "Unit",
        }
    }

    /// Build an [`Error::PlanResultTypeMismatch`] reporting `self`'s variant as the actual one.
    fn type_mismatch(&self, expected: &'static str) -> Error {
        Error::plan_result_type_mismatch(expected, self.variant_name())
    }
}
