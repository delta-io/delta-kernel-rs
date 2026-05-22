//! The unit of work an SM hands to the executor each tick.

use crate::plans::ir::nodes::ConsumeSink;
use crate::plans::ir::plan::{PlanNode, Ref};

/// A metadata-only read: ask the engine to open a parquet file, read its
/// schema from the footer, and deliver it back as
/// [`EngineResponse::Schema`](super::step_payload::EngineResponse::Schema).
///
/// Distinct from a data-carrying [`EngineRequest::Consume`]: no row stream, no sink, no
/// KDF-producing pipeline -- the executor just does a footer read.
#[derive(Debug, Clone)]
pub struct SchemaQuery {
    /// Path to the parquet file whose schema the kernel wants.
    pub file_path: String,
}

impl SchemaQuery {
    pub fn new(file_path: impl Into<String>) -> Self {
        Self {
            file_path: file_path.into(),
        }
    }
}

/// What [`StateMachine::get_step`](super::state_machine::StateMachine::get_step)
/// hands to the executor.
///
/// Separates the concerns the executor understands:
///
/// - [`SchemaQuery`](Self::SchemaQuery) — metadata-only footer read.
/// - [`Consume`](Self::Consume) -- SSA dataflow drained into a [`ConsumeSink`]. The engine compiles
///   `stmts` (a flat SSA program), runs the DAG, and feeds the rows produced at `terminal` into
///   `sink`. The consumer's typed output flows back as
///   [`EngineResponse::Consumer`](super::step_payload::EngineResponse::Consumer) carrying the
///   [`FinishedHandle`], and the SM body recovers the typed value via the paired [`Extractor`].
///
/// [`Extractor`]: crate::plans::kernel_consumers::Extractor
/// [`FinishedHandle`]: crate::plans::kernel_consumers::FinishedHandle
#[derive(Debug, Clone)]
pub enum EngineRequest {
    /// Read a file's schema without reading data.
    SchemaQuery(SchemaQuery),
    /// SSA dataflow + consumer drain. The engine evaluates `stmts` as a DAG and pipes
    /// the stream produced at `terminal` into `sink`.
    Consume {
        stmts: Vec<PlanNode>,
        terminal: Ref,
        sink: ConsumeSink,
    },
}
