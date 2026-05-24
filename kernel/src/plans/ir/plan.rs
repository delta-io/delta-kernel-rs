//! Plan containers ([`Plan`], [`PlanNode`], [`ResultPlan`]) and the [`RefId`] value
//! handle.

pub use super::nodes::{JoinKind, NodeKind};

// ============================================================================
// RefIds and plan nodes
// ============================================================================

/// Plan-scoped opaque identifier for a node's output value.
///
/// RefIds are minted sequentially by the plan builder starting from `0`. Engines must
/// treat RefIds as opaque keys: their numeric value is implementation-defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RefId(pub u32);

/// One node in a plan: an operator kind, its input RefIds, and its output RefId.
///
/// `inputs` order matters and is documented per [`NodeKind`] (e.g. for
/// `NodeKind::EquiJoin` the convention is `[left, right]`; for `NodeKind::UnionAll`
/// inputs are concatenated in order).
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub kind: NodeKind,
    pub inputs: Vec<RefId>,
    pub output: RefId,
}

// ============================================================================
// Plans
// ============================================================================

/// A plan: an ordered sequence of [`PlanNode`]s forming a dataflow DAG.
///
/// A [`RefId`] is an opaque integer that names the output value of one plan node. Each
/// [`PlanNode`] is a triple `(kind, inputs, output)`:
///
/// - `kind` ([`NodeKind`]) is the operator -- a source like `ScanParquet` or a transform like
///   `Project`.
/// - `inputs` is a `Vec<RefId>` naming the upstream values the operator reads from.
/// - `output` is the RefId the operator produces.
///
/// A node depends on another when one of its `inputs` matches that node's `output`.
/// The `inputs`/`output` cross-references encode the dataflow DAG; the engine compiles
/// the plan bottom-up via topological walk over the `inputs` edges.
///
/// Each RefId is bound exactly once -- single-static-assignment (SSA) form. Callers
/// preserve this invariant; the IR does not validate it.
///
/// # Example
///
/// A four-node plan with a fan-out / fan-in shape: one scan feeds two filters whose
/// outputs are unioned. The `nodes` `Vec<PlanNode>`:
///
/// ```text
/// Plan {
///     nodes: vec![
///         PlanNode { kind: ScanParquet(..), inputs: vec![],                   output: RefId(0) },
///         PlanNode { kind: Filter(..),      inputs: vec![RefId(0)],           output: RefId(1) },
///         PlanNode { kind: Filter(..),      inputs: vec![RefId(0)],           output: RefId(2) },
///         PlanNode { kind: UnionAll(..),    inputs: vec![RefId(1), RefId(2)], output: RefId(3) },
///     ],
/// }
/// ```
///
/// The dataflow DAG this encodes:
///
/// ```text
///             ScanParquet [RefId(0)]
///                    |
///         +----------+----------+
///         v                     v
///    Filter [RefId(1)]     Filter [RefId(2)]
///         |                     |
///         +----------+----------+
///                    v
///              UnionAll [RefId(3)]
/// ```
///
/// The engine compiles `RefId(0)` first (no inputs), then `RefId(1)` and `RefId(2)`
/// (both consuming `RefId(0)`), then `RefId(3)`. A [`ResultPlan`] pairing this `Plan`
/// with `result: RefId(3)` names the RefId the engine streams to the caller.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub nodes: Vec<PlanNode>,
}

/// A [`Plan`] paired with the [`RefId`] whose rows the engine streams to the caller.
///
/// # Example
///
/// The declarative equivalent of [`Scan::scan_metadata`](crate::scan::Scan::scan_metadata)
/// builds a plan that lists the table's log files, reconciles `add`/`remove` actions
/// across versions, and projects each surviving `add` to the scan-metadata schema.
/// The terminal projection -- say `RefId(7)` -- produces the rows the caller iterates:
///
/// ```text
/// ResultPlan {
///     plan: <ScanJson -> MaxByVersion -> ... -> Project [RefId(7)]>,
///     result: RefId(7),
/// }
/// ```
///
/// Other entry points wrap their own `ResultPlan`s -- e.g. the data-load plan behind
/// `Scan::execute` terminates at the file-data rows, the table-changes plan at the
/// per-version CDF rows. The engine compiles `plan` bottom-up and streams the rows
/// produced at `result`.
#[derive(Debug, Clone)]
pub struct ResultPlan {
    pub plan: Plan,
    pub result: RefId,
}
