//! SSA (single static assignment) IR for plans.
//!
//! Each [`Stmt`] produces exactly one output [`Ref`] from zero or more input [`Ref`]s.
//! A [`Plan`] is an ordered vector of [`Stmt`]s forming a DAG via these Ref edges.
//! All [`Node`]s are pure compute -- no sinks, no named relations, no engine-side
//! side effects. Cross-step data flow happens through state machine bodies (kernel
//! consumer outputs and schema queries), never through the IR.
//!
//! A [`ResultPlan`] is the state machine's terminal value: the plan plus the Ref
//! the engine should stream to the caller after the SM completes.
//!
//! # Construction
//!
//! Plans are built by repeated [`Plan::push`] calls. Each push mints a fresh Ref
//! (sequential within the plan) and returns it; the caller threads it as an input
//! to subsequent pushes.
//!
//! ```ignore
//! use delta_kernel::plans::ir::ssa::{Node, Plan};
//!
//! let mut plan = Plan::new();
//! let src = plan.push(Node::ReadJson { files, schema }, vec![]);
//! let filt = plan.push(Node::Filter { predicate }, vec![src]);
//! ```
//!
//! # Dead-code elimination
//!
//! [`Plan::reachable_from`] returns a new plan containing only the statements
//! transitively reachable from a given terminal Ref via backward traversal. Refs
//! on surviving statements retain their original ids -- engines treat Refs as
//! opaque keys and tolerate gaps.

use std::collections::HashSet;
use std::sync::Arc;

use url::Url;

use super::nodes::{DvRef, FileType, ScanFileColumns};
use crate::expressions::{ColumnName, Expression, Predicate, Scalar};
use crate::schema::SchemaRef;
use crate::FileMeta;

// ============================================================================
// Refs and statements
// ============================================================================

/// SSA reference. Plan-scoped opaque identifier for a statement's output value.
///
/// Refs are minted sequentially by [`Plan::push`] starting from `0`. Engines must
/// treat Refs as opaque keys: their numeric value is implementation-defined and
/// gaps are allowed (e.g. after [`Plan::reachable_from`] prunes statements).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ref(pub u32);

/// One SSA statement: a node operator, its input Refs, and its output Ref.
///
/// `inputs` order matters and is documented per [`Node`] variant (e.g. for
/// [`Node::EquiJoin`] the convention is `[left, right]`; for [`Node::Union`]
/// inputs are concatenated in order).
#[derive(Debug, Clone)]
pub struct Stmt {
    pub node: Node,
    pub inputs: Vec<Ref>,
    pub output: Ref,
}

// ============================================================================
// Plans
// ============================================================================

/// SSA program: ordered statements forming a DAG via input/output Refs.
///
/// Statement order is the construction order; engines compile bottom-up via
/// topological walk over `inputs` edges.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub stmts: Vec<Stmt>,
    /// Next Ref id to mint. Tracked separately from `stmts.len()` so that
    /// [`Self::reachable_from`] can drop statements without renumbering.
    next_ref: u32,
}

impl Plan {
    /// Empty plan with no statements.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a statement and mint its output Ref.
    ///
    /// Returns the freshly minted Ref. Inputs MUST be Refs already produced by
    /// earlier `push` calls on this plan (the IR does not validate this; cursor
    /// builders should enforce it).
    pub fn push(&mut self, node: Node, inputs: Vec<Ref>) -> Ref {
        let output = Ref(self.next_ref);
        self.next_ref += 1;
        self.stmts.push(Stmt {
            node,
            inputs,
            output,
        });
        output
    }

    /// Look up a statement by its output Ref. Returns `None` when no statement
    /// produces this Ref (e.g. it was pruned by [`Self::reachable_from`] or the
    /// caller fabricated an out-of-range Ref).
    pub fn stmt(&self, r: Ref) -> Option<&Stmt> {
        self.stmts.iter().find(|s| s.output == r)
    }

    /// Backward-reachability dead-code elimination from `result`.
    ///
    /// Returns a new plan containing only statements transitively reachable
    /// from `result` via the inputs/output edges. Statement order is preserved;
    /// surviving Refs keep their original ids. The returned plan's `next_ref`
    /// is preserved so subsequent pushes never collide with kept Refs.
    pub fn reachable_from(&self, result: Ref) -> Plan {
        let mut reachable: HashSet<Ref> = HashSet::new();
        let mut frontier = vec![result];
        while let Some(r) = frontier.pop() {
            if !reachable.insert(r) {
                continue;
            }
            if let Some(stmt) = self.stmt(r) {
                frontier.extend(stmt.inputs.iter().copied());
            }
        }
        let stmts = self
            .stmts
            .iter()
            .filter(|s| reachable.contains(&s.output))
            .cloned()
            .collect();
        Plan {
            stmts,
            next_ref: self.next_ref,
        }
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
// Nodes
// ============================================================================

/// Equi-join semantics. Only the two kinds the kernel pipelines need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Standard inner join: emit `(left, right)` rows whose keys match.
    Inner,
    /// Left anti: emit each left row whose key matches no right row.
    LeftAnti,
}

/// SSA node operators.
///
/// Sources have zero inputs; transforms have one or more (per-variant doc).
/// Schemas are stored on variants where the caller declares them (`ReadJson`,
/// `ReadParquet`, `Values`, `Load`); for transforms the cursor builder derives
/// the output schema from inputs and parameters.
#[derive(Debug, Clone)]
pub enum Node {
    // === Sources (0 inputs) ==================================================
    /// List files under a storage prefix. Output is a canonical file-listing
    /// schema (path / size / modificationTime / etc.).
    ListFiles { start_from: Url },

    /// Read JSON files into row batches matching `schema`.
    ReadJson {
        files: Vec<FileMeta>,
        schema: SchemaRef,
    },

    /// Read Parquet files into row batches matching `schema`.
    ReadParquet {
        files: Vec<FileMeta>,
        schema: SchemaRef,
    },

    /// Inline literal rows. `rows[i].len() == schema.fields().count()` per row.
    Values {
        schema: SchemaRef,
        rows: Vec<Vec<Scalar>>,
    },

    // === Transforms (1+ inputs) ==============================================
    /// Project the single input through `named_exprs`, producing rows of
    /// `output_schema`. The schema is supplied by the cursor builder (either
    /// inferred for narrow projections via the kernel's expression-type
    /// inference, or declared explicitly via
    /// [`Cursor::project_with_schema`](crate::plans::operations::framework::plan_context::Cursor::project_with_schema)
    /// when inference is insufficient). Engines compile against the declared
    /// schema directly and do not re-derive it from the expressions.
    Project {
        named_exprs: Vec<(String, Arc<Expression>)>,
        output_schema: SchemaRef,
    },

    /// Keep input rows where `predicate` evaluates true (SQL null semantics).
    /// Output schema is the input schema unchanged.
    Filter { predicate: Arc<Predicate> },

    /// Concatenate N inputs (`inputs.len() >= 1`). All input schemas must agree.
    /// `ordered=true` preserves child order; `ordered=false` permits reordering.
    Union { ordered: bool },

    /// File-reader transform. Each input row carries a path (under
    /// `file_meta.path`); the engine opens the resolved file as `file_type`,
    /// reads `file_schema` columns, and broadcasts each upstream row's
    /// `passthrough_columns` onto every emitted file row.
    Load {
        file_schema: SchemaRef,
        file_type: FileType,
        base_url: Option<Url>,
        passthrough_columns: Vec<ColumnName>,
        file_meta: ScanFileColumns,
        dv_ref: Option<DvRef>,
    },

    /// "Top 1 per group, ordered by version desc" — a specialized aggregate.
    /// Output schema is `group_by` exprs (with inferred types) followed by the
    /// named `value_columns` lifted from input.
    MaxByVersion {
        group_by: Vec<Arc<Expression>>,
        version_column: Arc<Expression>,
        value_columns: Vec<String>,
    },

    /// Equi-join two inputs (`inputs.len() == 2`, convention `[left, right]`).
    EquiJoin {
        kind: JoinKind,
        key_pairs: Vec<(Arc<Expression>, Arc<Expression>)>,
    },
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::schema::{DataType, StructField, StructType};

    fn schema() -> SchemaRef {
        Arc::new(StructType::try_new(vec![StructField::nullable("x", DataType::INTEGER)]).unwrap())
    }

    fn empty_values() -> Node {
        Node::Values {
            schema: schema(),
            rows: vec![],
        }
    }

    /// `push` mints sequential Refs starting at 0 and records the Stmt with
    /// matching `output` Ref.
    #[test]
    fn push_mints_sequential_refs_and_records_stmts() {
        let mut plan = Plan::new();
        let a = plan.push(empty_values(), vec![]);
        let b = plan.push(
            Node::Filter {
                predicate: Arc::new(col("x").is_not_null()),
            },
            vec![a],
        );
        assert_eq!(a, Ref(0));
        assert_eq!(b, Ref(1));
        assert_eq!(plan.stmts.len(), 2);
        assert_eq!(plan.stmts[0].output, a);
        assert_eq!(plan.stmts[1].output, b);
        assert_eq!(plan.stmts[1].inputs, vec![a]);
    }

    /// `stmt(r)` looks up the statement whose `output` matches `r`. Out-of-range
    /// Refs return `None`.
    #[test]
    fn stmt_lookup_by_ref() {
        let mut plan = Plan::new();
        let a = plan.push(empty_values(), vec![]);
        assert!(matches!(plan.stmt(a).unwrap().node, Node::Values { .. }));
        assert!(plan.stmt(Ref(99)).is_none());
    }

    /// DCE keeps only stmts transitively reachable from the terminal Ref. Dead
    /// branches drop, surviving Refs retain their original ids, statement order
    /// is preserved.
    #[test]
    fn reachable_from_prunes_unreachable_branches() {
        let mut plan = Plan::new();
        let src = plan.push(empty_values(), vec![]); // Ref(0)
        let _dead_a = plan.push(
            Node::Filter {
                predicate: Arc::new(col("x").is_null()),
            },
            vec![src],
        ); // Ref(1) - dead
        let kept = plan.push(
            Node::Filter {
                predicate: Arc::new(col("x").is_not_null()),
            },
            vec![src],
        ); // Ref(2)
        let _dead_b = plan.push(
            Node::Project {
                named_exprs: vec![("y".to_string(), Arc::new(Expression::column(["x"])))],
                output_schema: schema(),
            },
            vec![Ref(1)],
        ); // Ref(3) - dead

        let pruned = plan.reachable_from(kept);
        let outputs: Vec<Ref> = pruned.stmts.iter().map(|s| s.output).collect();
        assert_eq!(outputs, vec![src, kept]);
        // next_ref preserved so future pushes don't collide with surviving Refs.
        assert_eq!(pruned.next_ref, 4);
    }

    /// Both join inputs are reached and kept.
    #[test]
    fn reachable_from_keeps_both_join_inputs() {
        let mut plan = Plan::new();
        let left = plan.push(empty_values(), vec![]);
        let right = plan.push(empty_values(), vec![]);
        let joined = plan.push(
            Node::EquiJoin {
                kind: JoinKind::Inner,
                key_pairs: vec![(
                    Arc::new(Expression::column(["x"])),
                    Arc::new(Expression::column(["x"])),
                )],
            },
            vec![left, right],
        );

        let pruned = plan.reachable_from(joined);
        let outputs: HashSet<Ref> = pruned.stmts.iter().map(|s| s.output).collect();
        assert_eq!(outputs, HashSet::from([left, right, joined]));
    }

    /// Pushing into a pruned plan continues from the original `next_ref`,
    /// guaranteeing fresh Refs never collide with kept ones.
    #[test]
    fn push_after_reachable_from_does_not_reuse_pruned_refs() {
        let mut plan = Plan::new();
        let a = plan.push(empty_values(), vec![]);
        let _b = plan.push(empty_values(), vec![]);
        let mut pruned = plan.reachable_from(a);
        let c = pruned.push(empty_values(), vec![a]);
        assert_eq!(c, Ref(2));
    }

    /// `reachable_from` on an empty plan with an out-of-range Ref returns an
    /// empty plan (no panic, no statements).
    #[test]
    fn reachable_from_unknown_ref_returns_empty() {
        let plan = Plan::new();
        let pruned = plan.reachable_from(Ref(7));
        assert!(pruned.stmts.is_empty());
    }
}
