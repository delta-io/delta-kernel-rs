//! IR for plans.
//!
//! Each [`PlanNode`] produces exactly one output [`Ref`] from zero or more input [`Ref`]s.
//! A [`Plan`] is an ordered vector of [`PlanNode`]s forming a DAG via these Ref edges.
//! All [`NodeKind`]s are pure compute -- no sinks, no named relations, no engine-side
//! side effects. Cross-step data flow happens through state machine bodies (kernel
//! consumer outputs and schema queries), never through the IR.
//!
//! A [`ResultPlan`] is the state machine's terminal value: the plan plus the Ref
//! the engine should stream to the caller after the SM completes.
//!
//! # Construction
//!
//! Plans are built by the SSA construction
//! [`Context`](crate::plans::state_machines::framework::plan_context::Context), which owns the
//! in-flight [`Plan`] and mints fresh Refs as nodes are appended. [`Plan`] itself is a pure
//! data container; the engine receives a fully-built plan and treats it as read-only.
//!
//! # Dead-code elimination
//!
//! [`Plan::reachable_from`] returns a new plan containing only the nodes
//! transitively reachable from a given terminal Ref via backward traversal. Refs
//! on surviving nodes retain their original ids -- engines treat Refs as
//! opaque keys and tolerate gaps. DCE only inspects the `inputs` / `output` edges,
//! never any counter state.

use std::collections::HashSet;

use super::nodes::{
    EquiJoinNode, FilterNode, ListFilesNode, LoadNode, MaxByVersionNode, ProjectNode, ScanNode,
    UnionNode, ValuesNode,
};

// ============================================================================
// Refs and plan nodes
// ============================================================================

/// SSA reference. Plan-scoped opaque identifier for a node's output value.
///
/// Refs are minted sequentially by the SSA
/// [`Context`](crate::plans::state_machines::framework::plan_context::Context) starting from `0`.
/// Engines must treat Refs as opaque keys: their numeric value is implementation-defined
/// and gaps are allowed (e.g. after [`Plan::reachable_from`] prunes nodes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ref(pub u32);

/// One SSA node in a plan: an operator kind, its input Refs, and its output Ref.
///
/// `inputs` order matters and is documented per [`NodeKind`] variant (e.g. for
/// [`NodeKind::EquiJoin`] the convention is `[left, right]`; for [`NodeKind::Union`]
/// inputs are concatenated in order).
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub kind: NodeKind,
    pub inputs: Vec<Ref>,
    pub output: Ref,
}

// ============================================================================
// Plans
// ============================================================================

/// SSA program: ordered nodes forming a DAG via input/output Refs.
///
/// `Plan` is a pure data container. The SSA
/// [`Context`](crate::plans::state_machines::framework::plan_context::Context) is the sole
/// authority that builds plans and mints Refs; the engine receives the assembled plan and
/// compiles it bottom-up via topological walk over the `inputs` edges.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub stmts: Vec<PlanNode>,
}

impl Plan {
    /// Empty plan with no nodes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a node by its output Ref. Returns `None` when no node
    /// produces this Ref (e.g. it was pruned by [`Self::reachable_from`] or the
    /// caller fabricated an out-of-range Ref).
    pub fn node(&self, r: Ref) -> Option<&PlanNode> {
        self.stmts.iter().find(|n| n.output == r)
    }

    /// Backward-reachability dead-code elimination from `result`.
    ///
    /// Returns a new plan containing only nodes transitively reachable from `result` via the
    /// inputs/output edges. Node order is preserved; surviving Refs keep their original ids.
    /// The traversal inspects only `inputs` / `output` edges -- no counter state to preserve.
    pub fn reachable_from(&self, result: Ref) -> Plan {
        let mut reachable: HashSet<Ref> = HashSet::new();
        let mut frontier = vec![result];
        while let Some(r) = frontier.pop() {
            if !reachable.insert(r) {
                continue;
            }
            if let Some(node) = self.node(r) {
                frontier.extend(node.inputs.iter().copied());
            }
        }
        let stmts = self
            .stmts
            .iter()
            .filter(|n| reachable.contains(&n.output))
            .cloned()
            .collect();
        Plan { stmts }
    }
}

/// State machine terminal value: an SSA program plus the Ref the engine should
/// stream to the caller.
#[derive(Debug, Clone)]
pub struct ResultPlan {
    pub plan: Plan,
    pub result: Ref,
}

// ============================================================================
// Node operator kinds
// ============================================================================

/// Equi-join semantics. Only the two kinds the kernel pipelines need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Standard inner join: emit `(left, right)` rows whose keys match.
    Inner,
    /// Left anti: emit each left row whose key matches no right row.
    LeftAnti,
}

/// Plan node operator kinds.
///
/// Each variant wraps a single payload struct defined in [`super::nodes`] that carries
/// the variant's parameters verbatim. Sources have zero inputs; transforms have one or
/// more (per-payload doc). Output schemas are stored on the payload structs of variants
/// where the caller declares them (`Scan`, `Values`, `Load`, `Project`); for the rest
/// the SSA builder derives the output schema from inputs and parameters.
#[derive(Debug, Clone)]
pub enum NodeKind {
    // === Sources (0 inputs) ==================================================
    ListFiles(ListFilesNode),
    Scan(ScanNode),
    Values(ValuesNode),

    // === Transforms (1+ inputs) ==============================================
    Project(ProjectNode),
    Filter(FilterNode),
    Union(UnionNode),
    Load(LoadNode),
    MaxByVersion(MaxByVersionNode),
    EquiJoin(EquiJoinNode),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::expressions::{col, Expression, Predicate};
    use crate::schema::{DataType, SchemaRef, StructField, StructType};

    fn schema() -> SchemaRef {
        Arc::new(StructType::try_new(vec![StructField::nullable("x", DataType::INTEGER)]).unwrap())
    }

    fn values_node(output: u32) -> PlanNode {
        PlanNode {
            kind: NodeKind::Values(ValuesNode {
                schema: schema(),
                rows: vec![],
            }),
            inputs: vec![],
            output: Ref(output),
        }
    }

    fn filter_node(predicate: Arc<Predicate>, input: Ref, output: u32) -> PlanNode {
        PlanNode {
            kind: NodeKind::Filter(FilterNode { predicate }),
            inputs: vec![input],
            output: Ref(output),
        }
    }

    /// `Plan::node(r)` looks up the node whose `output` matches `r`. Out-of-range
    /// Refs return `None`.
    #[test]
    fn node_lookup_by_ref() {
        let plan = Plan {
            stmts: vec![values_node(0)],
        };
        assert!(matches!(
            plan.node(Ref(0)).unwrap().kind,
            NodeKind::Values(_)
        ));
        assert!(plan.node(Ref(99)).is_none());
    }

    /// DCE keeps only nodes transitively reachable from the terminal Ref. Dead
    /// branches drop, surviving Refs retain their original ids, node order
    /// is preserved.
    #[test]
    fn reachable_from_prunes_unreachable_branches() {
        let src = Ref(0);
        let dead = Ref(1);
        let kept = Ref(2);
        let dead2 = Ref(3);
        let plan = Plan {
            stmts: vec![
                values_node(src.0),
                filter_node(Arc::new(col("x").is_null()), src, dead.0),
                filter_node(Arc::new(col("x").is_not_null()), src, kept.0),
                PlanNode {
                    kind: NodeKind::Project(ProjectNode {
                        named_exprs: vec![("y".to_string(), Arc::new(Expression::column(["x"])))],
                        output_schema: schema(),
                    }),
                    inputs: vec![dead],
                    output: Ref(dead2.0),
                },
            ],
        };

        let pruned = plan.reachable_from(kept);
        let outputs: Vec<Ref> = pruned.stmts.iter().map(|n| n.output).collect();
        assert_eq!(outputs, vec![src, kept]);
    }

    /// Both join inputs are reached and kept.
    #[test]
    fn reachable_from_keeps_both_join_inputs() {
        let left = Ref(0);
        let right = Ref(1);
        let joined = Ref(2);
        let plan = Plan {
            stmts: vec![
                values_node(left.0),
                values_node(right.0),
                PlanNode {
                    kind: NodeKind::EquiJoin(EquiJoinNode {
                        kind: JoinKind::Inner,
                        key_pairs: vec![(
                            Arc::new(Expression::column(["x"])),
                            Arc::new(Expression::column(["x"])),
                        )],
                    }),
                    inputs: vec![left, right],
                    output: joined,
                },
            ],
        };

        let pruned = plan.reachable_from(joined);
        let outputs: HashSet<Ref> = pruned.stmts.iter().map(|n| n.output).collect();
        assert_eq!(outputs, HashSet::from([left, right, joined]));
    }

    /// `reachable_from` on an empty plan with an out-of-range Ref returns an
    /// empty plan (no panic, no nodes).
    #[test]
    fn reachable_from_unknown_ref_returns_empty() {
        let plan = Plan::new();
        let pruned = plan.reachable_from(Ref(7));
        assert!(pruned.stmts.is_empty());
    }
}
