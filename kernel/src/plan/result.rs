//! Defines the [`PlanResult`] type returned by plan execution.

use std::fmt;

use bytes::Bytes;

use crate::{DeltaResult, EngineData};

/// The result of executing a [`DeclarativePlanNode`](super::DeclarativePlanNode).
///
/// Each variant describes a different shape of output:
/// - [`Data`](Self::Data) carries columnar [`EngineData`] batches (e.g. file listings, metadata).
/// - [`ByteStream`](Self::ByteStream) carries raw byte buffers (e.g. file reads).
/// - [`Unit`](Self::Unit) signals successful completion of a side-effect operation (e.g. writes).
pub enum PlanResult {
    /// A stream of columnar data batches produced by the plan.
    Data(Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>>>),
    /// A stream of raw byte buffers, one per file or file slice.
    ByteStream(Box<dyn Iterator<Item = DeltaResult<Bytes>>>),
    /// The plan completed successfully with no output data.
    Unit,
}

impl fmt::Debug for PlanResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data(_) => f.debug_tuple("Data").field(&"<iterator>").finish(),
            Self::ByteStream(_) => f.debug_tuple("ByteStream").field(&"<iterator>").finish(),
            Self::Unit => write!(f, "Unit"),
        }
    }
}
