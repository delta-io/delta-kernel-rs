//! The unit of work an SM hands to the executor each tick.

use crate::plans::ir::nodes::ConsumeSink;
use crate::plans::ir::ssa::{Ref, Stmt};
use crate::plans::ir::Plan;

/// A metadata-only read: ask the engine to open a parquet file, read its
/// schema from the footer, and deliver it back through
/// [`StepResult::submit_schema`](super::step_result::StepResult::submit_schema).
///
/// Distinct from a data-carrying [`Plan`]: no row stream, no sink, no
/// KDF-producing pipeline — the executor just does a footer read.
#[derive(Debug, Clone)]
pub struct SchemaQueryNode {
    /// Path to the parquet file whose schema the kernel wants.
    pub file_path: String,
}

impl SchemaQueryNode {
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
/// - [`Plans`](Self::Plans) — one or more independent data pipelines terminated by sinks. When the
///   vec holds a single plan, this is the common case. When it holds multiple, they are independent
///   and the executor may run them concurrently; consumer state is merged across all.
/// - [`SchemaQuery`](Self::SchemaQuery) — metadata-only footer read.
/// - [`Consume`](Self::Consume) — SSA dataflow drained into a [`ConsumeSink`]. The engine compiles
///   `stmts` (a flat SSA program), runs the DAG, and feeds the rows produced at `terminal` into
///   `sink`. The consumer's typed output flows back through the `StepResult` and is recovered by
///   the SM body via the paired [`Extractor`].
///
/// [`Extractor`]: crate::plans::kernel_consumers::Extractor
#[derive(Debug, Clone)]
pub enum Step {
    /// One or more independent plans to run, plus merged consumer state.
    Plans(Vec<Plan>),
    /// Read a file's schema without reading data.
    SchemaQuery(SchemaQueryNode),
    /// SSA dataflow + consumer drain. The engine evaluates `stmts` as a DAG and pipes
    /// the stream produced at `terminal` into `sink`.
    Consume {
        stmts: Vec<Stmt>,
        terminal: Ref,
        sink: ConsumeSink,
    },
}
