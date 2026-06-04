//! Plan containers ([`Plan`], [`PlanNode`]) and the [`RefId`] value handle.

pub use super::nodes::NodeKind;

// ============================================================================
// RefIds and plan nodes
// ============================================================================

/// Plan-scoped opaque identifier for a node's output value.
///
/// Engines must treat RefIds as opaque keys: equality and hashing are the only meaningful
/// operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RefId(pub u32);

/// One node in a plan: an operator kind, its input RefIds, and its output RefId.
///
/// `inputs` order is interpreted per [`NodeKind`] (e.g. for `NodeKind::SemiJoin` the
/// convention is `[probe, build]`). `NodeKind::UnionAll` emits the rows of all inputs
/// regardless of input order.
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
/// A [`RefId`] is an opaque value handle naming the output of one plan node. Each
/// [`PlanNode`] is a triple `(kind, inputs, output)`:
///
/// - `kind` ([`NodeKind`]) is the operator: a source like `ScanParquet` or a transform like
///   `Project`.
/// - `inputs` is a `Vec<RefId>` naming the upstream values the operator reads from.
/// - `output` is the RefId the operator produces.
///
/// A node depends on another when one of its `inputs` matches that node's `output`.
/// Each RefId is bound exactly once. These input/output cross references encode the
/// dataflow DAG, and `nodes` is stored in topological order: every node appears after
/// the nodes whose outputs it consumes, so an engine can evaluate `nodes` in slice
/// order; each node's inputs are guaranteed bound by the time the node is reached.
///
/// A `Plan` has at least one node. The **terminal node** is always the
/// last entry in `nodes`: it is the only node whose `output` no other node lists in
/// `inputs`, and `Plan::result()` returns that node's `output` RefId, the value the
/// engine streams to the caller.
///
/// # Execution
///
/// For the best performance, connectors are encouraged to run kernel-produced
/// plans through their query optimizer before execution (e.g. to fold adjacent
/// filters, merge scans over the same files, or choose physical join and scan
/// strategies).
///
/// # Example
///
/// A five-node plan: two independent scans, each filtered, then unioned. The `nodes`
/// `Vec<PlanNode>`:
///
/// ```text
/// Plan {
///     nodes: vec![
///         PlanNode { kind: ScanParquet(..), inputs: vec![],                   output: RefId(0) },
///         PlanNode { kind: ScanParquet(..), inputs: vec![],                   output: RefId(1) },
///         PlanNode { kind: Filter(..),      inputs: vec![RefId(0)],           output: RefId(2) },
///         PlanNode { kind: Filter(..),      inputs: vec![RefId(1)],           output: RefId(3) },
///         PlanNode { kind: UnionAll(..),    inputs: vec![RefId(2), RefId(3)], output: RefId(4) },
///     ],
/// }
/// ```
///
/// The dataflow DAG this encodes:
///
/// ```text
///    ScanParquet [RefId(0)]     ScanParquet [RefId(1)]
///              |                          |
///              v                          v
///       Filter [RefId(2)]          Filter [RefId(3)]
///              |                          |
///              +------------+-------------+
///                           v
///                 UnionAll [RefId(4)]   <-- terminal: Plan::result() == Some(RefId(4))
/// ```
///
/// The engine evaluates the nodes in the order of the `nodes` vector: `RefId(0)` and `RefId(1)`
/// first (sources), then `RefId(2)` and `RefId(3)`, then `RefId(4)`. The engine streams the
/// rows produced at the terminal node to the caller.
#[derive(Debug, Clone)]
pub struct Plan {
    pub nodes: Vec<PlanNode>,
}

impl Plan {
    /// Returns the last node's `output` [`RefId`], the plan terminal whose rows the
    /// engine streams to the caller, or `None` if `nodes` is empty.
    pub fn result(&self) -> Option<RefId> {
        self.nodes.last().map(|node| node.output)
    }
}
