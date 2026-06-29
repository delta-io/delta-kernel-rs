//! Plan containers ([`Plan`], [`PlanNode`]).

pub use super::nodes::Operator;

// ============================================================================
// Plan nodes
// ============================================================================

/// One node in a plan: an [`Operator`] and the indices of its input nodes.
///
/// A node is identified by its position in [`Plan::nodes`]; `inputs` lists those indices for
/// the upstream nodes this operator reads from. `inputs` order is interpreted per [`Operator`]
/// (e.g. for `Operator::SemiJoin` the convention is `[probe, build]`). `Operator::UnionAll`
/// emits the rows of all inputs regardless of input order.
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub op: Operator,
    pub inputs: Vec<usize>,
}

// ============================================================================
// Plans
// ============================================================================

/// A plan: an ordered sequence of [`PlanNode`]s forming a dataflow DAG.
///
/// A node is identified by its index in `nodes`. Each [`PlanNode`] pairs an operator with the
/// indices of its inputs:
///
/// - `op` ([`Operator`]) is the operator: a source like `ScanParquet` or a transform like
///   `Project`.
/// - `inputs` is a `Vec<usize>` naming the indices of the upstream nodes the operator reads from.
///
/// A node depends on another when one of its `inputs` is that node's index. `nodes` is stored in
/// topological order: every node appears after the nodes it consumes (each input index is
/// strictly less than the node's own index), so an engine can evaluate `nodes` in slice order;
/// each node's inputs are guaranteed bound by the time the node is reached.
///
/// A well-formed `Plan` has at least one node. The **terminal node** is always the last entry in
/// `nodes`: no other node lists its index in `inputs`, and its rows are the value the engine
/// streams to the caller.
///
/// # Optimization
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
///         PlanNode { op: ScanParquet(..), inputs: vec![]     },  // node 0
///         PlanNode { op: ScanParquet(..), inputs: vec![]     },  // node 1
///         PlanNode { op: Filter(..),      inputs: vec![0]    },  // node 2
///         PlanNode { op: Filter(..),      inputs: vec![1]    },  // node 3
///         PlanNode { op: UnionAll(..),    inputs: vec![2, 3] },  // node 4
///     ],
/// }
/// ```
///
/// The dataflow DAG this encodes:
///
/// ```text
///    ScanParquet [0]     ScanParquet [1]
///           |                    |
///           v                    v
///       Filter [2]           Filter [3]
///           |                    |
///           +---------+----------+
///                     v
///               UnionAll [4]   <-- terminal (last node)
/// ```
///
/// The engine evaluates the nodes in the order of the `nodes` vector: nodes `0` and `1` first
/// (sources), then `2` and `3`, then `4`. The engine streams the rows produced at the terminal
/// node to the caller.
#[derive(Debug, Clone)]
pub struct Plan {
    pub nodes: Vec<PlanNode>,
}
