//! Plan containers ([`Plan`], [`PlanNode`]) and the [`RefId`] value handle.

pub use super::nodes::NodeKind;

// ============================================================================
// RefIds and plan nodes
// ============================================================================

/// Plan-scoped opaque identifier for a node's output value.
///
/// Engines must treat RefIds as opaque keys: equality and hashing are the only meaningful
/// operations. The numeric representation and how RefIds are minted are implementation
/// details that may change without notice; engines must not depend on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RefId(pub u32);

/// One node in a plan: an operator kind, its input RefIds, and its output RefId.
///
/// `inputs` order matters and is documented per [`NodeKind`] (e.g. for
/// `NodeKind::SemiJoin` the convention is `[probe, build]`;
/// for `NodeKind::UnionAll` inputs are concatenated in order).
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
/// - `kind` ([`NodeKind`]) is the operator -- a source like `ScanParquet` or a transform like
///   `Project`.
/// - `inputs` is a `Vec<RefId>` naming the upstream values the operator reads from.
/// - `output` is the RefId the operator produces.
///
/// A node depends on another when one of its `inputs` matches that node's `output`.
/// The `inputs`/`output` cross-references encode the dataflow DAG, and `nodes` is
/// stored in a topological order over those edges: every node appears after the
/// nodes whose outputs it consumes. Engines may therefore evaluate `nodes` in slice
/// order without further sorting -- each node's inputs are guaranteed bound by the
/// time the node is reached.
///
/// Each RefId is bound exactly once -- single-static-assignment (SSA) form. Callers
/// preserve this invariant; the IR does not validate it.
///
/// An executable plan has at least one node, and the last node is the plan's
/// terminal: its output is the value the engine streams to the caller, accessible
/// via [`Plan::result`]. Plans with no nodes, or with multiple "terminal" nodes
/// (RefIds no other node consumes), are not executable; the plan builder is
/// responsible for never producing them.
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
///              UnionAll [RefId(3)]   <-- terminal: Plan::result() == Some(RefId(3))
/// ```
///
/// The engine evaluates the nodes in slice order -- `RefId(0)` first (no inputs),
/// then `RefId(1)` and `RefId(2)` (both consuming `RefId(0)`), then `RefId(3)` --
/// and streams the rows produced at the terminal node to the caller.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub nodes: Vec<PlanNode>,
}

impl Plan {
    /// Returns the terminal node's output [`RefId`] -- the value the engine streams
    /// to the caller -- or `None` if the plan has no nodes.
    pub fn result(&self) -> Option<RefId> {
        self.nodes.last().map(|node| node.output)
    }
}
