//! Plan containers ([`Plan`], [`PlanNode`]) and the [`RefId`] value handle.

pub use super::nodes::Operator;
use super::nodes::{Aggregate, Filter, Project, ScanFile, ScanJson, ScanParquet, SemiJoin};
use crate::expressions::{ColumnName, ExpressionRef, PredicateRef};
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error};

// ============================================================================
// RefIds and plan nodes
// ============================================================================

/// An opaque handle to a relation produced during plan construction.
///
/// [`PlanBuilder`] combinators return a `RefId` for each operation; feed it back as an input to a
/// later combinator. A `RefId` is either *present* -- it names a real relation -- or *absent*.
/// Absent is the plan algebra's uninhabited relation: it can never be instantiated and can never
/// produce rows, so it is the identity element that collapses combinators (a `UnionAll` drops an
/// absent arm, an aggregate over an absent input is absent, and so on). This lets callers assemble
/// plans unconditionally and let degenerate branches fall away. Distinguish the two cases with
/// [`is_present`](Self::is_present) / [`is_absent`](Self::is_absent).
///
/// `RefId` is meaningful only during construction. [`PlanBuilder::build`] resolves every present
/// reference to a 0-based node index ([`PlanNode`]'s `inputs`), so a built [`Plan`] contains no
/// `RefId`s. Treat it as opaque: presence and equality are the only meaningful queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefId(Option<u32>);

impl RefId {
    /// Returns `true` if this handle names a real relation (the opposite of
    /// [`is_absent`](Self::is_absent)).
    pub fn is_present(self) -> bool {
        self.0.is_some()
    }

    /// Returns `true` if this is the absent relation; see [`RefId`] for its collapse semantics.
    ///
    /// An operator that collapses to absent strands any present input it does not forward (see
    /// [`PlanBuilder`]). When such a sibling input may be absent, guard the construction of an
    /// expensive, exclusively-consumed input with [`is_present`](Self::is_present).
    pub fn is_absent(self) -> bool {
        self.0.is_none()
    }
}

/// One node in a plan: an [`Operator`] and its inputs.
///
/// A node's *output* is implicit -- it is the node's own 0-based index in the plan's node list.
/// `inputs` are the 0-based indices of the nodes whose outputs this operator reads, in an order
/// interpreted per [`Operator`] (e.g. for `Operator::SemiJoin` the convention is `[probe, build]`).
/// `Operator::UnionAll` emits the rows of all inputs regardless of input order.
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub op: Operator,
    pub inputs: Vec<u32>,
}

// ============================================================================
// Plans
// ============================================================================

/// A plan: an ordered sequence of [`PlanNode`]s forming a dataflow DAG.
///
/// Each [`PlanNode`] is a pair `(op, inputs)`:
///
/// - `op` ([`Operator`]) is the operator: a source like `ScanParquet` or a transform like
///   `Project`.
/// - `inputs` is a `Vec<u32>` of 0-based node indices the operator reads from.
///
/// A node's *output* is implicit -- it is the node's own 0-based index in the list. A node depends
/// on another when one of its `inputs` is that node's index. The nodes are stored in topological
/// order: every node appears after the nodes it consumes, so an engine can evaluate them in order
/// (e.g. into a `Vec` indexed by node position), each node's inputs already bound by the time it is
/// reached.
///
/// A `Plan` always has a **terminal node** -- the node whose output no other node consumes, and the
/// last node in topological order; the engine streams its rows to the caller.
/// [`PlanBuilder::build`] guarantees the plan is non-empty, free of orphaned (unreachable) nodes,
/// and terminal-last.
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
/// A five-node plan: two independent scans, each filtered, then unioned. The node list (a node's
/// output is its index):
///
/// ```text
/// nodes (topological order, terminal last):
///     [0] PlanNode { op: ScanParquet(..), inputs: vec![] }
///     [1] PlanNode { op: ScanParquet(..), inputs: vec![] }
///     [2] PlanNode { op: Filter(..),      inputs: vec![0] }
///     [3] PlanNode { op: Filter(..),      inputs: vec![1] }
///     [4] PlanNode { op: UnionAll,        inputs: vec![2, 3] } // terminal
/// ```
///
/// The dataflow DAG this encodes:
///
/// ```text
///    ScanParquet [0]     ScanParquet [1]
///            |                   |
///            v                   v
///       Filter [2]          Filter [3]
///            |                   |
///            +---------+---------+
///                      v
///                 UnionAll [4]   <-- terminal (last node)
/// ```
///
/// The engine evaluates the nodes in topological order: nodes 0 and 1 first (sources), then nodes 2
/// and 3, then node 4. The engine streams the rows produced at the terminal node to the caller.
///
/// # Construction
///
/// A `Plan` is built with [`PlanBuilder`], not constructed directly: it has no public
/// constructor, so the only way to obtain one is [`PlanBuilder::build`]. This keeps a `Plan`
/// valid by construction -- engines can rely on its invariants (non-empty, topologically
/// ordered, terminal last) without re-validating.
#[derive(Debug, Clone)]
pub struct Plan {
    /// Nodes in topological order, terminal last. Non-empty and free of orphaned (unreachable)
    /// nodes by construction (see [`PlanBuilder::build`]).
    nodes: Vec<PlanNode>,
}

impl Plan {
    /// Consumes the plan and returns its nodes in execution (topological) order, terminal last.
    ///
    /// Always non-empty; the terminal node (whose output the engine streams to the caller) is the
    /// last element.
    pub fn into_nodes(self) -> Vec<PlanNode> {
        self.nodes
    }
}

// ============================================================================
// Plan builder
// ============================================================================

/// Incrementally builds a [`Plan`] by appending one node per operator method.
///
/// Each operator method ([`scan_parquet`](Self::scan_parquet), [`scan_json`](Self::scan_json),
/// [`union_all`](Self::union_all), [`aggregate`](Self::aggregate)) appends one node in topological
/// order, validates it, and returns the [`RefId`] naming its output, which later operators take as
/// input. A combinator may instead return an *absent* [`RefId`] when its result collapses to the
/// absent relation (e.g. a scan over no files, or a `UnionAll` whose arms are all absent); feeding
/// that absent handle into a later combinator collapses it in turn, so degenerate branches fall
/// away without the caller branching on them.
///
/// The builder tracks the most recently returned [`RefId`] as the plan **terminal**.
/// [`build`](Self::build) finalizes the accumulated nodes into an immutable [`Plan`]: it returns
/// `Ok(None)` when the terminal is absent (an empty plan), errors if any appended node is orphaned
/// (unreachable from the terminal -- a sign of wasted or buggy construction), and otherwise returns
/// the validated `Plan`.
///
/// # Collapse and orphaned nodes
///
/// Collapses happen at construction time -- a collapsed combinator appends no node -- so an absent
/// input never has a node behind it. The hazard is the opposite case: a combinator that collapses
/// to *absent* strands any *present* input it does not forward. If such an input is consumed only
/// by that combinator, it becomes an orphan that [`build`](Self::build) rejects. Combinators that
/// can collapse this way document the condition on their own method; guard an exclusively-consumed
/// input by building it only when the deciding input is [`present`](RefId::is_present). (An input
/// shared with another consumer stays reachable, so it is safe.)
#[derive(Debug, Clone, Default)]
pub struct PlanBuilder {
    nodes: Vec<PlanNode>,
    /// The most recent combinator output (the index of the node it produced, or `None` when that
    /// output is absent), designating the plan terminal.
    terminal: Option<u32>,
}

impl PlanBuilder {
    /// Creates a builder with no nodes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Finalizes the accumulated nodes into an immutable [`Plan`].
    ///
    /// Returns `Ok(None)` for an empty plan (the terminal is [`absent`](RefId::is_absent)). Errors
    /// if any appended node is unreachable from the terminal: such an orphan means a built branch
    /// feeds nothing, indicating wasted construction at best and a logic error at worst.
    pub fn build(self) -> DeltaResult<Option<Plan>> {
        let Self { nodes, terminal } = self;
        let Some(terminal_index) = terminal else {
            if nodes.is_empty() {
                return Ok(None);
            }
            return Err(Error::generic(
                "Builder contains nodes but produced no plan (terminal is absent); all are orphaned",
            ));
        };

        // Indices are dense (`0..nodes.len()`), so reachability uses a bool-per-node "reached" set
        // seeded from the terminal. Inputs name smaller indices, so a reverse pass visits every
        // consumer before the node it consumes; a node's reachability is thus final when visited,
        // making the first unreached node a confirmed orphan.
        let mut reached = vec![false; nodes.len()];
        reached[terminal_index as usize] = true;
        for i in (0..nodes.len()).rev() {
            if !reached[i] {
                return Err(Error::generic(format!(
                    "Plan node r{i} is orphaned: not reachable from result r{terminal_index}"
                )));
            }
            for &input in &nodes[i].inputs {
                reached[input as usize] = true;
            }
        }

        // No orphans + topological order => the terminal is the last node.
        Ok(Some(Plan { nodes }))
    }

    /// Appends a [`ScanParquet`] source over `files`, returning its output [`RefId`], or an
    /// absent [`RefId`] when `files` is empty (so callers can wire in an absent source).
    ///
    /// `file_constant_columns` names the [`ScanParquet`] fields whose per-file values come from
    /// [`ScanFile::file_constants`]; every file must carry exactly one constant per column.
    /// See [`ScanParquet`] for parameter semantics.
    pub fn scan_parquet(
        &mut self,
        files: Vec<ScanFile>,
        file_constant_columns: Vec<ColumnName>,
        schema: SchemaRef,
    ) -> DeltaResult<RefId> {
        if files.is_empty() {
            return self.record(None);
        }
        Self::validate_file_constants(&files, &file_constant_columns)?;
        self.append(
            Operator::ScanParquet(ScanParquet {
                files,
                file_constant_columns,
                schema,
            }),
            [],
        )
    }

    /// Appends a [`ScanJson`] source over `files`, returning its output [`RefId`], or an
    /// absent [`RefId`] when `files` is empty (so callers can wire in an absent source).
    ///
    /// `file_constant_columns` has the same contract as [`scan_parquet`](Self::scan_parquet).
    /// See [`ScanJson`] for parameter semantics.
    pub fn scan_json(
        &mut self,
        files: Vec<ScanFile>,
        file_constant_columns: Vec<ColumnName>,
        schema: SchemaRef,
    ) -> DeltaResult<RefId> {
        if files.is_empty() {
            return self.record(None);
        }
        Self::validate_file_constants(&files, &file_constant_columns)?;
        self.append(
            Operator::ScanJson(ScanJson {
                files,
                file_constant_columns,
                schema,
            }),
            [],
        )
    }

    /// Appends a [`UnionAll`](Operator::UnionAll) over `inputs`, dropping any absent arms first,
    /// then normalizing the degenerate cases:
    /// - an absent [`RefId`] when no arm survives,
    /// - the sole surviving arm's [`RefId`] (no node added) when exactly one remains,
    /// - otherwise the output [`RefId`] of a new [`UnionAll`](Operator::UnionAll) node.
    pub fn union_all(&mut self, inputs: impl IntoIterator<Item = RefId>) -> DeltaResult<RefId> {
        let mut inputs: Vec<u32> = inputs.into_iter().filter_map(|r| r.0).collect();
        match inputs.len() {
            0 | 1 => self.record(inputs.pop()),
            _ => self.append(Operator::UnionAll, inputs),
        }
    }

    /// Appends an [`Aggregate`] over `input`, returning its output [`RefId`], or an absent
    /// [`RefId`] when `input` is absent (aggregating an absent relation is absent).
    ///
    /// Accepts anything convertible into an [`Aggregate`], including an unbuilt
    /// [`AggregateBuilder`](super::nodes::AggregateBuilder) (from [`Aggregate::group_by`]), so
    /// callers need not call [`build`](super::nodes::AggregateBuilder::build) themselves. The
    /// conversion is skipped (and any error it would raise suppressed) when `input` is absent,
    /// since the aggregate then collapses away.
    pub fn aggregate<A>(&mut self, input: RefId, aggregate: A) -> DeltaResult<RefId>
    where
        A: TryInto<Aggregate>,
        Error: From<A::Error>,
    {
        self.append_unary(input, || Ok(Operator::Aggregate(aggregate.try_into()?)))
    }

    /// Appends a [`Project`] over `input`, evaluating `expr` into rows of `schema`, returning its
    /// output [`RefId`], or an absent [`RefId`] when `input` is absent (projecting an absent
    /// relation is absent).
    pub fn project(
        &mut self,
        input: RefId,
        expr: impl Into<ExpressionRef>,
        schema: SchemaRef,
    ) -> DeltaResult<RefId> {
        self.append_unary(input, || {
            Ok(Operator::Project(Project {
                expr: expr.into(),
                schema,
            }))
        })
    }

    /// Appends a [`Filter`] over `input`, keeping rows where `predicate` is true, returning its
    /// output [`RefId`], or an absent [`RefId`] when `input` is absent (filtering an absent
    /// relation is absent).
    pub fn filter(
        &mut self,
        input: RefId,
        predicate: impl Into<PredicateRef>,
    ) -> DeltaResult<RefId> {
        self.append_unary(input, || {
            Ok(Operator::Filter(Filter {
                predicate: predicate.into(),
            }))
        })
    }

    /// Appends a semi join (intersection): the `probe` rows whose join keys match a `build` row.
    /// `key_pairs` are `(probe_key, build_key)` column pairs compared for equality.
    ///
    /// Collapses to an absent [`RefId`] when *either* input is absent (intersection with an absent
    /// relation is empty). See [`SemiJoin`]: this cannot be made orphan-safe by construction order,
    /// so gate both sides on conditions known before building either.
    pub fn semi_join(
        &mut self,
        probe: RefId,
        build: RefId,
        key_pairs: impl IntoIterator<Item = (ColumnName, ColumnName)>,
    ) -> DeltaResult<RefId> {
        self.join(probe, build, key_pairs, false)
    }

    /// Appends an anti join (difference): the `probe` rows whose join keys match *no* `build` row.
    /// `key_pairs` are `(probe_key, build_key)` column pairs compared for equality.
    ///
    /// Collapses to an absent [`RefId`] when the `probe` is absent (nothing to emit); an absent
    /// `build` subtracts nothing, so it forwards `probe` unchanged. Building `probe` first is
    /// therefore orphan-safe. See [`SemiJoin`].
    pub fn anti_join(
        &mut self,
        probe: RefId,
        build: RefId,
        key_pairs: impl IntoIterator<Item = (ColumnName, ColumnName)>,
    ) -> DeltaResult<RefId> {
        self.join(probe, build, key_pairs, true)
    }

    /// Shared implementation of [`semi_join`](Self::semi_join) and [`anti_join`](Self::anti_join);
    /// `inverted` selects anti (difference) over semi (intersection). Each absence rule appears
    /// once: the general probe rule, then the type-specific build rule.
    fn join(
        &mut self,
        probe: RefId,
        build: RefId,
        key_pairs: impl IntoIterator<Item = (ColumnName, ColumnName)>,
        inverted: bool,
    ) -> DeltaResult<RefId> {
        // General rule (both joins): an absent probe emits nothing.
        let Some(p) = probe.0 else {
            return self.record(None);
        };
        // NOTE: A semi join (intersection) matches nothing (absent), while
        // an anti join (difference) subtracts nothing and so forwards the unmodified probe side.
        let Some(b) = build.0 else {
            return self.record(inverted.then_some(p));
        };
        let (probe_keys, build_keys): (Vec<_>, Vec<_>) =
            key_pairs.into_iter().unzip();
        self.append(
            Operator::SemiJoin(SemiJoin {
                inverted,
                probe_keys,
                build_keys,
            }),
            [p, b],
        )
    }

    /// Records `output` as the builder's terminal and returns it as the public [`RefId`] handle.
    /// This is the only place a [`RefId`] is minted. Infallible, but returns [`DeltaResult`] so
    /// combinators can `return self.record(..)` symmetrically with [`append`](Self::append).
    fn record(&mut self, output: Option<u32>) -> DeltaResult<RefId> {
        self.terminal = output;
        Ok(RefId(output))
    }

    /// Appends a single-input operator over `input`, or records absent when `input` is absent (the
    /// node then collapses away). `op` builds the operator lazily, so a fallible or expensive
    /// construction is skipped entirely for an absent input.
    fn append_unary(
        &mut self,
        input: RefId,
        op: impl FnOnce() -> DeltaResult<Operator>,
    ) -> DeltaResult<RefId> {
        match input.0 {
            None => self.record(None),
            Some(i) => self.append(op()?, [i]),
        }
    }

    /// Appends one node, validating input arity and that every input names an earlier node, then
    /// [`records`](Self::record) and returns the new node's output [`RefId`].
    ///
    /// Inputs are plain node indices: combinators resolve their [`RefId`]s (and handle absence)
    /// before calling, so absence is unrepresentable here.
    fn append(
        &mut self,
        op: Operator,
        inputs: impl IntoIterator<Item = u32>,
    ) -> DeltaResult<RefId> {
        let inputs: Vec<u32> = inputs.into_iter().collect();
        if let Some(arity) = fixed_input_arity(&op) {
            if inputs.len() != arity {
                return Err(Error::generic(format!(
                    "Plan node {op} requires exactly {arity} input(s), got {}",
                    inputs.len()
                )));
            }
        }
        let index = u32::try_from(self.nodes.len())
            .map_err(|_| Error::internal_error("Plan node count exceeded u32::MAX"))?;
        // An input must name an earlier node (its index is below this node's index).
        if let Some(&bad) = inputs.iter().find(|&&i| i >= index) {
            return Err(Error::generic(format!(
                "Invalid input reference r{bad}: only {index} relation(s) have been produced"
            )));
        }
        self.nodes.push(PlanNode { op, inputs });
        self.record(Some(index))
    }

    /// Verifies each [`ScanFile`] carries exactly one constant per declared file-constant column.
    fn validate_file_constants(
        files: &[ScanFile],
        file_constant_columns: &[ColumnName],
    ) -> DeltaResult<()> {
        for file in files {
            if file.file_constants.len() != file_constant_columns.len() {
                return Err(Error::generic(format!(
                    "ScanFile has {} file-constant value(s) but {} file-constant column(s) \
                     were declared",
                    file.file_constants.len(),
                    file_constant_columns.len()
                )));
            }
        }
        Ok(())
    }
}

/// Returns the required input count for operators with fixed arity, or `None` for variadic
/// operators (e.g. [`UnionAll`](Operator::UnionAll)). The exhaustive match makes new [`Operator`]
/// variants a
/// compile error here until their arity is declared.
fn fixed_input_arity(op: &Operator) -> Option<usize> {
    match op {
        Operator::ScanParquet(_) | Operator::ScanJson(_) | Operator::Values(_) => Some(0),
        Operator::Project(_) | Operator::Filter(_) | Operator::Load(_) | Operator::Aggregate(_) => {
            Some(1)
        }
        Operator::SemiJoin(_) => Some(2),
        Operator::UnionAll => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::plans::ir::nodes::Agg;
    use crate::schema::{column_name, DataType, StructField, StructType};
    use crate::utils::test_utils::assert_result_error_with_message;
    use crate::FileMeta;

    fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::not_null(
            "id",
            DataType::LONG,
        )]))
    }

    fn test_schema_with_version() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::not_null("id", DataType::LONG),
            StructField::not_null("version", DataType::LONG),
        ]))
    }

    fn test_file(path: &str) -> FileMeta {
        FileMeta {
            location: Url::parse(path).unwrap(),
            last_modified: 0,
            size: 0,
        }
    }

    enum Format {
        Json,
        Parquet,
    }

    #[rstest]
    #[case::json(Format::Json, &["file:///a.json", "file:///b.json"])]
    #[case::parquet(Format::Parquet, &["file:///a.parquet", "file:///b.parquet"])]
    fn scan_constructs_single_node(#[case] format: Format, #[case] urls: &[&str]) {
        let schema = test_schema();
        let files: Vec<FileMeta> = urls.iter().map(|u| test_file(u)).collect();
        let scan_files: Vec<ScanFile> = files.iter().cloned().map(ScanFile::from).collect();

        let mut builder = PlanBuilder::new();
        match format {
            Format::Json => builder.scan_json(scan_files, vec![], schema.clone()),
            Format::Parquet => builder.scan_parquet(scan_files, vec![], schema.clone()),
        }
        .unwrap();

        let plan = builder.build().unwrap().expect("non-empty plan");
        let [node] = <[_; 1]>::try_from(plan.into_nodes()).expect("single-node plan");
        assert!(node.inputs.is_empty());
        let (Operator::ScanJson(ScanJson {
            files: node_files,
            schema: node_schema,
            ..
        })
        | Operator::ScanParquet(ScanParquet {
            files: node_files,
            schema: node_schema,
            ..
        })) = node.op
        else {
            panic!("expected ScanJson / ScanParquet, got {:?}", node.op);
        };
        let scan_metas: Vec<FileMeta> = node_files.into_iter().map(|f| f.meta).collect();
        assert_eq!(scan_metas, files);
        assert!(Arc::ptr_eq(&node_schema, &schema));
    }

    #[test]
    fn empty_scan_files_collapse_to_absent_and_empty_plan() {
        let schema = test_schema();
        let mut builder = PlanBuilder::new();
        assert!(builder
            .scan_json(vec![], vec![], schema.clone())
            .unwrap()
            .is_absent());
        assert!(builder
            .scan_parquet(vec![], vec![], schema)
            .unwrap()
            .is_absent());
        // No nodes were produced and the terminal is absent, so the plan is empty.
        assert!(builder.build().unwrap().is_none());
    }

    #[test]
    fn union_then_aggregate_builds_dag() {
        let schema = test_schema_with_version();
        let checkpoint = vec![ScanFile::from(test_file("file:///cp.parquet"))];
        let commits = vec![ScanFile::from(test_file("file:///0.json"))];

        let mut builder = PlanBuilder::new();
        let cp = builder
            .scan_parquet(checkpoint, vec![], schema.clone())
            .unwrap();
        let commit = builder.scan_json(commits, vec![], schema.clone()).unwrap();
        let source = builder.union_all([cp, commit]).unwrap();
        let agg = Aggregate::group_by(schema, [])
            .aggregate(Agg::max_by(column_name!("id"), column_name!("version")));
        builder.aggregate(source, agg).unwrap();

        let plan = builder.build().unwrap().expect("non-empty plan");
        // Handles are 0-based node indices (cp=r0, commit=r1, union=r2), and a node's inputs hold
        // those same indices:
        // [0] ScanParquet, [1] ScanJson, [2] UnionAll(<- 0, 1), [3] Aggregate(<- 2)
        let nodes = plan.into_nodes();
        assert_eq!(nodes.len(), 4);
        assert!(matches!(nodes[2].op, Operator::UnionAll));
        assert_eq!(nodes[2].inputs, vec![0, 1]);
        assert!(matches!(nodes[3].op, Operator::Aggregate(_)));
        assert_eq!(nodes[3].inputs, vec![2]);
    }

    #[test]
    fn union_all_normalizes_degenerate_cases() {
        let schema = test_schema();
        let mut builder = PlanBuilder::new();
        let only = builder
            .scan_json(
                vec![ScanFile::from(test_file("file:///0.json"))],
                vec![],
                schema,
            )
            .unwrap();

        assert!(builder.union_all([]).unwrap().is_absent());
        // All-absent arms collapse to absent.
        assert!(builder
            .union_all([RefId(None), RefId(None)])
            .unwrap()
            .is_absent());
        // A single surviving arm passes through unchanged, without adding a UnionAll node, even
        // when other arms are absent.
        assert_eq!(builder.union_all([only]).unwrap(), only);
        assert_eq!(builder.union_all([only, RefId(None)]).unwrap(), only);
        // Last combinator return designates the terminal, so the plan is just the scan.
        let plan = builder.build().unwrap().expect("non-empty plan");
        assert_eq!(plan.into_nodes().len(), 1);
    }

    #[test]
    fn aggregate_over_absent_input_collapses_to_absent() {
        let schema = test_schema();
        // An unbuilt builder is accepted; its conversion is skipped because the input is absent.
        let agg = Aggregate::group_by(schema, []).aggregate(Agg::max(column_name!("id")));
        let mut builder = PlanBuilder::new();
        assert!(builder.aggregate(RefId(None), agg).unwrap().is_absent());
        assert!(builder.build().unwrap().is_none());
    }

    #[test]
    fn build_rejects_orphaned_node() {
        let schema = test_schema();
        let mut builder = PlanBuilder::new();
        // Two independent scans with no operator joining them: the first is left orphaned because
        // the terminal (the second scan) cannot reach it.
        let _orphan = builder
            .scan_json(
                vec![ScanFile::from(test_file("file:///0.json"))],
                vec![],
                schema.clone(),
            )
            .unwrap();
        let _terminal = builder
            .scan_json(
                vec![ScanFile::from(test_file("file:///1.json"))],
                vec![],
                schema,
            )
            .unwrap();
        assert_result_error_with_message(builder.build(), "orphaned");
    }

    #[test]
    fn append_rejects_unknown_input_reference() {
        let schema = test_schema();
        let agg = Aggregate::group_by(schema, [])
            .aggregate(Agg::max(column_name!("id")))
            .build()
            .unwrap();
        let mut builder = PlanBuilder::new();
        // A present handle naming node 0, but no nodes have been produced yet.
        let result = builder.aggregate(RefId(Some(0)), agg);
        assert_result_error_with_message(result, "Invalid input reference r0");
    }

    #[test]
    fn validate_file_constants_mismatch_is_rejected() {
        let schema = test_schema();
        let mut file = ScanFile::from(test_file("file:///0.json"));
        file.file_constants = vec![]; // 0 constants, but one column declared below
        let mut builder = PlanBuilder::new();
        let result = builder.scan_json(vec![file], vec![column_name!("version")], schema);
        assert_result_error_with_message(result, "file-constant");
    }
}
