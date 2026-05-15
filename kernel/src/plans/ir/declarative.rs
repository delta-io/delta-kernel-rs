//! The [`DeclarativePlanNode`] tree and its chain construction API.
//!
//! # Construction API
//!
//! The prototype uses a fluent-builder style directly on the enum — static
//! constructors for leaves, chain methods for unary transforms and n-ary
//! combinators, and terminal methods that produce a [`Plan`] (untyped) or a
//! `(Plan, Extractor<O>)` pair (typed, via a consumer KDF that also impls
//! [`crate::plans::kdf::KdfOutput`]).
//!
//! ```ignore
//! use delta_kernel::plans::ir::DeclarativePlanNode;
//!
//! // Untyped pipeline: scan JSON, project to a sub-schema, stream results.
//! let plan = DeclarativePlanNode::scan_json(files, schema)
//!     .project(projection, output_schema)
//!     .into_results();
//!
//! // Typed pipeline: scan JSON, drain into a typed consumer KDF, recover
//! // the typed output from the resulting PhaseState.
//! let (plan, extractor) = DeclarativePlanNode::scan_json(files, schema)
//!     .consume(CheckpointHintReader::default());
//! // ... after `phase.execute(PhaseOperation::Plans(vec![plan]), name).await?` ...
//! // let hint = extractor.extract(&state)?;
//! ```
//!
//! ## Leaves
//!
//! - [`DeclarativePlanNode::scan`] / [`scan_json`] / [`scan_parquet`] — explicit-schema scans.
//! - [`DeclarativePlanNode::values`] / [`values_row`] — kernel-provided constant rows
//!   (`VALUES`-style). Aligned with [`crate::EvaluationHandler::create_many`].
//! - [`DeclarativePlanNode::union`] / [`union_unordered`] — concatenate N children.
//!
//! ## Transforms
//!
//! - [`DeclarativePlanNode::with_predicate`] / [`with_row_index`] / [`with_ordered`] — scan
//!   modifiers.
//! - [`DeclarativePlanNode::filter`] — predicate filter.
//! - [`DeclarativePlanNode::project`] — projection.
//! - [`DeclarativePlanNode::window`] — window functions (`row_number`).
//! - [`DeclarativePlanNode::assert`] — schema-preserving row-level invariants.
//!
//! ## Terminals
//!
//! - [`DeclarativePlanNode::into_plan`] — explicit sink.
//! - [`DeclarativePlanNode::into_results`] — sugar for `into_plan(SinkType::Results(None))`.
//! - [`DeclarativePlanNode::into_relation`] — pipe output into a named
//!   [`RelationHandle`](super::nodes::RelationHandle) for another plan in the same
//!   `PhaseOperation::Plans(...)` to consume.
//! - [`DeclarativePlanNode::into_load`] — file-reader sink: each upstream row describes a file to
//!   open and read; optional DV masking via [`LoadSink::dv_ref`]. Materializes under a named
//!   relation handle.
//! - [`DeclarativePlanNode::into_write`] — single-destination file write sink ([`WriteSink`]);
//!   IR-only until a DataFusion-backed executor handles it.
//! - [`DeclarativePlanNode::into_partitioned_write`] — Hive-partitioned file write
//!   ([`PartitionedWriteSink`]); IR-only for the same reason.
//! - [`DeclarativePlanNode::consume`] — typed KDF consumer terminal; returns a `(Plan,
//!   Extractor<O>)` pair whose plan terminates in
//!   [`SinkType::ConsumeByKdf`](super::nodes::SinkType::ConsumeByKdf) and whose extractor pulls the
//!   typed output from the phase's [`PhaseState`].
//!
//! ## Escape hatches
//!
//! - [`DeclarativePlanNode::consume_by_kdf`] — terminate `self` into a [`SinkType::ConsumeByKdf`]
//!   sink from a pre-built [`ConsumeByKdfSink`](super::nodes::ConsumeByKdfSink) without a typed
//!   extractor.
//!
//! [`scan_json`]: DeclarativePlanNode::scan_json
//! [`scan_parquet`]: DeclarativePlanNode::scan_parquet
//! [`values_row`]: DeclarativePlanNode::values_row
//! [`union_unordered`]: DeclarativePlanNode::union_unordered
//! [`with_row_index`]: DeclarativePlanNode::with_row_index
//! [`with_ordered`]: DeclarativePlanNode::with_ordered
//! [`window`]: DeclarativePlanNode::window
//! [`WriteSink`]: super::nodes::WriteSink
//! [`PartitionedWriteSink`]: super::nodes::PartitionedWriteSink

use std::sync::Arc;

use super::nodes::*;
use super::plan::Plan;
use crate::expressions::{Expression, Scalar};
use crate::plans::kdf::typed::ExtractFn;
use crate::plans::kdf::{downcast_all, ConsumerKdf, KdfOutput, KdfStateToken};
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::plans::state_machines::framework::phase_state::PhaseState;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, FileMeta};

/// A single node in the declarative plan tree.
///
/// Trees are transforms-only: every complete pipeline terminates in a
/// [`Plan`] via one of the terminal methods (`into_plan`, `into_results`,
/// `into_relation`, `into_load`, `into_write`, `into_partitioned_write`, `consume_by_kdf`) or in a
/// `(Plan, Extractor<O>)` pair via [`DeclarativePlanNode::consume`].
#[derive(Debug, Clone)]
pub enum DeclarativePlanNode {
    // Leaves
    Scan(ScanNode),
    FileListing(FileListingNode),
    Values(ValuesNode),
    /// Read batches from a relation produced by another plan in the same
    /// `PhaseOperation::Plans(...)`. Wired up by the executor to a bounded
    /// channel whose sender is held by the producing plan's
    /// [`SinkType::Relation`](super::nodes::SinkType::Relation).
    RelationRef(RelationHandle),

    // Unary
    Filter {
        child: Box<DeclarativePlanNode>,
        node: FilterNode,
    },
    Project {
        child: Box<DeclarativePlanNode>,
        node: ProjectNode,
    },
    /// Window over partitions. See [`WindowNode`].
    Window {
        child: Box<DeclarativePlanNode>,
        node: WindowNode,
    },
    /// Schema-preserving row-level invariant. See [`AssertNode`].
    Assert {
        child: Box<DeclarativePlanNode>,
        node: AssertNode,
    },

    // N-ary
    Union {
        children: Vec<DeclarativePlanNode>,
        node: UnionNode,
    },

    // Binary
    /// Equi-join. See [`JoinNode`].
    Join {
        build: Box<DeclarativePlanNode>,
        probe: Box<DeclarativePlanNode>,
        node: JoinNode,
    },
}

// ============================================================================
// Leaves — static constructors
// ============================================================================

impl DeclarativePlanNode {
    /// Scan a fixed file list with an explicit schema when the readable [`FileFormat`] is chosen at
    /// runtime (for example FSR Plan A4 checkpoint shape).
    ///
    /// Dispatches to [`Self::scan_parquet`] or [`Self::scan_json`]. Prefer those helpers when the
    /// format is known statically.
    pub fn scan(format: FileFormat, files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        match format {
            FileType::Parquet => Self::scan_parquet(files, schema),
            FileType::Json => Self::scan_json(files, schema),
        }
    }

    /// Scan JSON files with an explicit schema.
    pub fn scan_json(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self::Scan(ScanNode::new(FileType::Json, files, schema))
    }

    /// Scan Parquet files with an explicit schema.
    pub fn scan_parquet(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self::Scan(ScanNode::new(FileType::Parquet, files, schema))
    }

    /// Read batches from a relation produced by another plan in the same
    /// `PhaseOperation::Plans(...)`. The producing plan terminates in
    /// [`SinkType::Relation`](super::nodes::SinkType::Relation) referencing
    /// the same handle.
    pub fn relation_ref(handle: RelationHandle) -> Self {
        Self::RelationRef(handle)
    }

    /// Multi-row `VALUES`-style literal table. Rows × columns layout; see [`ValuesNode`]
    /// invariants.
    pub fn values(schema: SchemaRef, rows: Vec<Vec<Scalar>>) -> DeltaResult<Self> {
        Ok(Self::Values(ValuesNode::try_new(schema, rows)?))
    }

    /// Single-row values node. Sugar for the common one-row case.
    pub fn values_row(schema: SchemaRef, values: Vec<Scalar>) -> DeltaResult<Self> {
        Ok(Self::Values(ValuesNode::try_new_row(schema, values)?))
    }

    /// Ordered union of N child streams; children are consumed in declaration
    /// order.
    ///
    /// Zero children yields an empty-struct output. With one child, the union
    /// wrapper is omitted.
    pub fn union<I>(children: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        let mut children: Vec<Self> = children.into_iter().collect();
        if children.len() == 1 {
            return children
                .pop()
                .ok_or_else(|| Error::generic("internal: single-child union drained empty"));
        }
        Ok(Self::Union {
            children,
            node: UnionNode { ordered: true },
        })
    }

    /// Unordered union — the engine may interleave or reorder children freely.
    pub fn union_unordered<I>(children: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        let children: Vec<Self> = children.into_iter().collect();
        Ok(Self::Union {
            children,
            node: UnionNode { ordered: false },
        })
    }

    /// Equi-join with `build` and `probe` subtrees. See [`JoinNode`] for the
    /// node-level contract (key arity, null semantics, output schema).
    pub fn join(node: JoinNode, build: Self, probe: Self) -> Self {
        Self::Join {
            build: Box::new(build),
            probe: Box::new(probe),
            node,
        }
    }
}

// ============================================================================
// Scan modifiers
// ============================================================================

impl DeclarativePlanNode {
    /// Attach a row-index column to a [`ScanNode`].
    ///
    /// Returns an error if `self` is not a scan — these modifiers are only
    /// meaningful immediately after a scan constructor.
    pub fn with_row_index(mut self, col: impl Into<String>) -> DeltaResult<Self> {
        match &mut self {
            Self::Scan(scan) => {
                scan.row_index_column = Some(col.into());
                Ok(self)
            }
            other => Err(Error::generic(format!(
                "with_row_index requires a Scan node, got {}",
                node_kind_name(other),
            ))),
        }
    }

    /// Attach a pushdown predicate hint to a [`ScanNode`].
    ///
    /// Returns an error if `self` is not a scan.
    pub fn with_predicate(mut self, predicate: Arc<Expression>) -> DeltaResult<Self> {
        match &mut self {
            Self::Scan(scan) => {
                scan.predicate = Some(predicate);
                Ok(self)
            }
            other => Err(Error::generic(format!(
                "with_predicate requires a Scan node, got {}",
                node_kind_name(other),
            ))),
        }
    }

    /// Mark a [`ScanNode`] as ordered (cross-file emission preserves the
    /// `files` order). See [`ScanNode`]'s ordering doc for the SQL-equivalence
    /// contract.
    ///
    /// Returns an error if `self` is not a scan.
    pub fn with_ordered(mut self) -> DeltaResult<Self> {
        match &mut self {
            Self::Scan(scan) => {
                scan.ordered = true;
                Ok(self)
            }
            other => Err(Error::generic(format!(
                "with_ordered requires a Scan node, got {}",
                node_kind_name(other),
            ))),
        }
    }
}

// ============================================================================
// Transforms
// ============================================================================

impl DeclarativePlanNode {
    /// Wrap `self` in a [`Filter`](Self::Filter).
    pub fn filter(self, predicate: Arc<Expression>) -> Self {
        Self::Filter {
            child: Box::new(self),
            node: FilterNode { predicate },
        }
    }

    /// Wrap `self` in a [`Project`](Self::Project).
    pub fn project(self, columns: Vec<Arc<Expression>>, output_schema: SchemaRef) -> Self {
        Self::Project {
            child: Box::new(self),
            node: ProjectNode {
                columns,
                output_schema,
            },
        }
    }

    /// Wrap `self` in a [`Window`](Self::Window) with partition keys and ordering specs.
    ///
    /// Returns an error when `order_by` is empty ([`WindowNode::try_new`]).
    pub fn window(
        self,
        functions: Vec<WindowFunction>,
        partition_by: Vec<Arc<Expression>>,
        order_by: Vec<OrderingSpec>,
    ) -> DeltaResult<Self> {
        let node = WindowNode::try_new(functions, partition_by, order_by)?;
        Ok(Self::Window {
            child: Box::new(self),
            node,
        })
    }

    /// Wrap `self` in an [`Assert`](Self::Assert) with the given checks.
    /// Schema-preserving row-level invariant; see [`AssertNode`].
    pub fn assert(self, checks: Vec<AssertCheck>) -> Self {
        Self::Assert {
            child: Box::new(self),
            node: AssertNode { checks },
        }
    }

    // === Convenience combinators (F7) ===
    //
    // These wrap the lower-level `union`/`union_unordered`/`join`/`window` constructors with
    // common parameter sets seen across plan builders. Use them when they fit; drop down to the
    // primitives when they don't.

    /// Ordered binary union of `self` (left) and `other` (right). For more than two children, use
    /// [`Self::union`].
    pub fn union_with(self, other: Self) -> DeltaResult<Self> {
        Self::union([self, other])
    }

    /// LeftAnti hash join: emit rows from `self` (probe) whose `probe_keys` are NOT present in
    /// `build`'s `build_keys`. Output schema = `self`'s schema.
    pub fn left_anti_join_on(
        self,
        build: Self,
        probe_keys: Vec<Arc<Expression>>,
        build_keys: Vec<Arc<Expression>>,
    ) -> Self {
        Self::join(
            JoinNode {
                build_keys,
                probe_keys,
                join_type: JoinType::LeftAnti,
                hint: JoinHint::Hash,
            },
            build,
            self,
        )
    }

    /// Inner hash join: emit one row per `(self, build)` pair matched on the supplied key
    /// expressions. Output schema = build's schema ++ probe's schema.
    pub fn inner_join_on(
        self,
        build: Self,
        probe_keys: Vec<Arc<Expression>>,
        build_keys: Vec<Arc<Expression>>,
    ) -> Self {
        Self::join(
            JoinNode {
                build_keys,
                probe_keys,
                join_type: JoinType::Inner,
                hint: JoinHint::Hash,
            },
            build,
            self,
        )
    }

    /// Single-function window emitting `row_number()` over (`partition_by`, `order_by`) into
    /// `output_col`. Fails if `order_by` is empty (row_number requires a deterministic ordering).
    pub fn window_row_number(
        self,
        output_col: impl Into<String>,
        partition_by: Vec<Arc<Expression>>,
        order_by: Vec<OrderingSpec>,
    ) -> DeltaResult<Self> {
        self.window(
            vec![WindowFunction {
                output_col: output_col.into(),
            }],
            partition_by,
            order_by,
        )
    }

    /// Typed KDF consumer terminal. Wraps `self` in a [`Plan`] terminating in
    /// [`SinkType::ConsumeByKdf`] and returns the plan paired with an
    /// [`Extractor<O>`] that pulls the typed output from the resulting
    /// [`PhaseState`].
    pub fn consume<S>(self, state: S) -> (Plan, Extractor<S::Output>)
    where
        S: ConsumerKdf + KdfOutput + 'static,
    {
        let node = ConsumeByKdfSink::new_consumer(state);
        let token = node.token.clone();
        let extract = make_extract::<S>(token.clone());
        let plan = self.into_plan(SinkType::ConsumeByKdf(node));
        (plan, Extractor { token, extract })
    }

    /// Power-user escape hatch: terminate `self` in a [`Plan`] with a
    /// [`SinkType::ConsumeByKdf`] from a pre-built consumer-KDF node. No typed
    /// extractor is threaded forward — caller harvests partition states directly.
    ///
    /// Use [`Self::consume`] instead when the state type also implements
    /// [`KdfOutput`].
    pub fn consume_by_kdf(self, node: ConsumeByKdfSink) -> Plan {
        self.into_plan(SinkType::ConsumeByKdf(node))
    }
}

/// Build the typed extract closure for a state `S` at a given token. The
/// closure downcasts each per-partition erased state back to `S` and reduces
/// via [`KdfOutput::into_output`].
fn make_extract<S>(token: KdfStateToken) -> ExtractFn<S::Output>
where
    S: KdfOutput + 'static,
{
    Box::new(move |parts| {
        let states = downcast_all::<S>(parts, &token)?;
        S::into_output(states)
    })
}

// ============================================================================
// Typed wrapper: Extractor<O>
// ============================================================================

/// A typed adapter for pulling the output of a single
/// [`SinkType::ConsumeByKdf`] (or executor-side telemetry sink) out of a
/// [`PhaseState`].
///
/// Produced as the second half of [`DeclarativePlanNode::consume`]. The
/// caller hands the paired [`Plan`] to
/// [`Phase::execute`](crate::plans::state_machines::framework::coroutine::phase::Phase::execute)
/// (typically wrapped in
/// [`PhaseOperation::Plans`](crate::plans::state_machines::framework::phase_operation::PhaseOperation::Plans))
/// then calls [`Extractor::extract`] on the resulting state to recover the typed payload.
///
/// `Extractor` is intentionally lightweight: it owns only the
/// [`KdfStateToken`] identifying the entries it will pull and the boxed
/// extraction closure. Multiple `Extractor`s freely coexist in a single
/// phase — each one targets a distinct token.
pub struct Extractor<O> {
    token: KdfStateToken,
    extract: ExtractFn<O>,
}

impl<O: Send + 'static> Extractor<O> {
    /// Token identifying the entries this extractor will pull from a [`PhaseState`].
    pub fn token(&self) -> &KdfStateToken {
        &self.token
    }

    /// Pull this extractor's payload from `state` and decode it.
    ///
    /// Drains the entries under [`Self::token`] from `state` (so a second
    /// call would see them empty) and runs the typed reduction. Decoding
    /// failures are wrapped in [`EngineError::internal`] so SM bodies can
    /// uniformly handle them on the engine-error path.
    pub fn extract(self, state: &PhaseState) -> Result<O, EngineError> {
        let parts = state.take_by_token(&self.token);
        (self.extract)(parts).map_err(EngineError::internal)
    }
}

impl<O> std::fmt::Debug for Extractor<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extractor")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

// ============================================================================
// Terminals
// ============================================================================

impl DeclarativePlanNode {
    /// Wrap this tree in a [`Plan`] with the given sink.
    pub fn into_plan(self, sink_type: SinkType) -> Plan {
        Plan::new(self, SinkNode { sink_type })
    }

    /// Terminal: stream every output batch to the caller.
    ///
    /// Leaves result schema inference to the executor (`SinkType::Results(None)`).
    pub fn into_results(self) -> Plan {
        self.into_plan(SinkType::Results(None))
    }

    /// Terminal: stream every output batch to the caller with an explicit
    /// output schema contract.
    pub fn into_results_with_schema(self, schema: SchemaRef) -> Plan {
        self.into_plan(SinkType::Results(Some(schema)))
    }

    /// Terminal: stream every output batch to the named relation. Another
    /// plan in the same `PhaseOperation::Plans(...)` consumes the relation
    /// via [`Self::relation_ref`].
    pub fn into_relation(self, handle: RelationHandle) -> Plan {
        self.into_plan(SinkType::Relation(handle))
    }

    /// Terminal: file-reader sink. For each upstream row, open the resolved
    /// file, read [`LoadSink::file_schema`] columns (with optional DV masking
    /// when [`LoadSink::dv_ref`] names an upstream descriptor column), and
    /// materialize the result under [`LoadSink::output_relation`] for downstream
    /// [`Self::relation_ref`] consumers in the same phase. See [`LoadSink`].
    pub fn into_load(self, sink: LoadSink) -> Plan {
        self.into_plan(SinkType::Load(sink))
    }

    /// Terminal: single-target file write ([`WriteSink`]). Engines like DataFusion
    /// lower this to native write operators; the default in-process executor does
    /// not execute write sinks yet.
    pub fn into_write(self, sink: WriteSink) -> Plan {
        self.into_plan(SinkType::Write(sink))
    }

    /// Terminal: partitioned file write ([`PartitionedWriteSink`]). Same executor contract as
    /// [`Self::into_write`].
    pub fn into_partitioned_write(self, sink: PartitionedWriteSink) -> Plan {
        self.into_plan(SinkType::PartitionedWrite(sink))
    }
}

// ============================================================================
// Introspection
// ============================================================================

impl DeclarativePlanNode {
    /// Borrowed child subtrees, in left-to-right declaration order.
    pub fn children(&self) -> Vec<&DeclarativePlanNode> {
        match self {
            Self::Scan(..) | Self::FileListing(..) | Self::Values(..) | Self::RelationRef(..) => {
                Vec::new()
            }
            Self::Filter { child, .. }
            | Self::Project { child, .. }
            | Self::Window { child, .. }
            | Self::Assert { child, .. } => {
                vec![child.as_ref()]
            }
            Self::Union { children, .. } => children.iter().collect(),
            Self::Join { build, probe, .. } => vec![build.as_ref(), probe.as_ref()],
        }
    }

    /// True for leaf node variants (no child subtrees).
    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::Scan(..) | Self::FileListing(..) | Self::Values(..) | Self::RelationRef(..)
        )
    }
}

/// Static label for a node variant; used in error messages and diagnostics.
fn node_kind_name(node: &DeclarativePlanNode) -> &'static str {
    match node {
        DeclarativePlanNode::Scan(..) => "Scan",
        DeclarativePlanNode::FileListing(..) => "FileListing",
        DeclarativePlanNode::Values(..) => "Values",
        DeclarativePlanNode::RelationRef(..) => "RelationRef",
        DeclarativePlanNode::Filter { .. } => "Filter",
        DeclarativePlanNode::Project { .. } => "Project",
        DeclarativePlanNode::Window { .. } => "Window",
        DeclarativePlanNode::Assert { .. } => "Assert",
        DeclarativePlanNode::Union { .. } => "Union",
        DeclarativePlanNode::Join { .. } => "Join",
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::any::Any;

    use url::Url;

    use super::*;
    use crate::expressions::{ColumnName, Expression};
    use crate::plans::errors::DeltaError;
    use crate::plans::ir::nodes::{DvRef, OrderingSpec, WindowFunction};
    use crate::plans::kdf::{ConsumerKdf, Kdf, KdfControl, KdfOutput};
    use crate::schema::{DataType, StructField, StructType};

    fn simple_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::not_null(
            "version",
            DataType::LONG,
        )]))
    }

    #[test]
    fn scan_has_no_children() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema());
        assert!(plan.is_leaf());
        assert_eq!(plan.children().len(), 0);
    }

    #[test]
    fn filter_wraps_child() {
        let predicate = Arc::new(Expression::literal(true));
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).filter(predicate);

        assert!(!plan.is_leaf());
        assert_eq!(plan.children().len(), 1);
        assert!(plan.children()[0].is_leaf());
    }

    #[test]
    fn union_with_single_child_unwraps() {
        let only = DeclarativePlanNode::scan_json(vec![], simple_schema());
        let unioned = DeclarativePlanNode::union(vec![only]).unwrap();
        assert!(matches!(unioned, DeclarativePlanNode::Scan(..)));
    }

    #[test]
    fn union_zero_children_is_empty_struct() {
        let unioned = DeclarativePlanNode::union(Vec::<DeclarativePlanNode>::new()).unwrap();
        assert!(matches!(unioned, DeclarativePlanNode::Union { .. }));
    }

    #[test]
    fn write_sink_terminal_emits_write_sink() {
        let dest = Url::parse("file:///tmp/out").unwrap();
        let sink = WriteSink::parquet(dest.clone());
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).into_write(sink);
        match plan.sink.sink_type {
            SinkType::Write(w) => {
                assert_eq!(w.destination, dest);
                assert_eq!(w.format, WriteFileFormat::Parquet);
            }
            other => panic!("expected Write sink, got {other:?}"),
        }
    }

    #[test]
    fn write_sink_equality_is_structural() {
        let u = Url::parse("s3://bucket/prefix/file.parquet").unwrap();
        let a = WriteSink::parquet(u.clone());
        let b = WriteSink::new(u, WriteFileFormat::Parquet);
        assert_eq!(a, b);
        assert_eq!(SinkType::Write(a.clone()), SinkType::Write(b.clone()));
        assert_ne!(
            SinkType::Write(WriteSink::parquet(Url::parse("file:///a").unwrap())),
            SinkType::Write(WriteSink::parquet(Url::parse("file:///b").unwrap())),
        );
        assert_ne!(
            SinkType::Write(WriteSink::parquet(Url::parse("file:///x").unwrap())),
            SinkType::Write(WriteSink::json_lines(Url::parse("file:///x").unwrap())),
        );
    }

    #[test]
    fn partitioned_write_sink_terminal_emits_partitioned_sink() {
        let dest = Url::parse("file:///tmp/table").unwrap();
        let sink = PartitionedWriteSink::parquet(dest.clone(), vec!["dt".into(), "region".into()]);
        let plan =
            DeclarativePlanNode::scan_json(vec![], simple_schema()).into_partitioned_write(sink);
        match plan.sink.sink_type {
            SinkType::PartitionedWrite(p) => {
                assert_eq!(p.destination, dest);
                assert_eq!(p.format, WriteFileFormat::Parquet);
                assert_eq!(
                    p.partition_columns,
                    vec!["dt".to_string(), "region".to_string()]
                );
            }
            other => panic!("expected PartitionedWrite sink, got {other:?}"),
        }
    }

    #[test]
    fn partitioned_write_sink_equality_is_structural() {
        let dest = Url::parse("s3://bucket/root").unwrap();
        let cols = vec!["a".into(), "b".into()];
        let a = PartitionedWriteSink::parquet(dest.clone(), cols.clone());
        let b = PartitionedWriteSink::new(dest, WriteFileFormat::Parquet, cols);
        assert_eq!(a, b);
        assert_eq!(
            SinkType::PartitionedWrite(a.clone()),
            SinkType::PartitionedWrite(b.clone())
        );
        assert_ne!(
            SinkType::PartitionedWrite(PartitionedWriteSink::parquet(
                Url::parse("file:///x").unwrap(),
                vec!["p".into()],
            )),
            SinkType::PartitionedWrite(PartitionedWriteSink::parquet(
                Url::parse("file:///x").unwrap(),
                vec!["q".into()],
            )),
        );
        assert_ne!(
            SinkType::PartitionedWrite(PartitionedWriteSink::parquet(
                Url::parse("file:///z").unwrap(),
                vec![],
            )),
            SinkType::PartitionedWrite(PartitionedWriteSink::json_lines(
                Url::parse("file:///z").unwrap(),
                vec![],
            )),
        );
    }

    #[test]
    fn load_sink_terminal_emits_load_sink() {
        let out_handle = RelationHandle::fresh("manifest_files", simple_schema());
        let load = LoadSink {
            output_relation: out_handle.clone(),
            file_schema: simple_schema(),
            base_url: None,
            file_meta: ScanFileColumns {
                path: ColumnName::new(["path"]),
                size: None,
                record_count: None,
            },
            dv_ref: None,
            passthrough_columns: vec![],
            file_type: FileType::Parquet,
        };
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).into_load(load);
        match plan.sink.sink_type {
            SinkType::Load(ls) => {
                assert_eq!(ls.output_relation, out_handle);
                assert!(ls.dv_ref.is_none());
            }
            other => panic!("expected Load sink, got {other:?}"),
        }
    }

    #[test]
    fn load_sink_preserves_dv_ref_column_hint() {
        let out_handle = RelationHandle::fresh("loaded_rows", simple_schema());
        let dv_col = ColumnName::new(["add", "deletionVector"]);
        let load = LoadSink {
            output_relation: out_handle.clone(),
            file_schema: simple_schema(),
            base_url: None,
            file_meta: ScanFileColumns {
                path: ColumnName::new(["path"]),
                size: None,
                record_count: None,
            },
            dv_ref: Some(DvRef::skip(dv_col.clone())),
            passthrough_columns: vec![],
            file_type: FileType::Parquet,
        };
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).into_load(load);
        match plan.sink.sink_type {
            SinkType::Load(ls) => {
                assert_eq!(ls.dv_ref.as_ref(), Some(&DvRef::skip(dv_col.clone())));
                assert_eq!(ls.output_relation, out_handle);
            }
            other => panic!("expected Load sink, got {other:?}"),
        }
    }

    #[test]
    fn assert_wraps_child_with_checks() {
        let plan =
            DeclarativePlanNode::scan_json(vec![], simple_schema()).assert(vec![AssertCheck {
                predicate: Arc::new(Expression::literal(true)),
                error_code: "STATS_CLUSTERING_NOT_NULL".into(),
                error_message: "missing stats for cluster col".into(),
            }]);

        assert!(!plan.is_leaf());
        assert_eq!(plan.children().len(), 1);
        assert!(plan.children()[0].is_leaf());
        match plan {
            DeclarativePlanNode::Assert { node, .. } => {
                assert_eq!(node.checks.len(), 1);
                assert_eq!(node.checks[0].error_code, "STATS_CLUSTERING_NOT_NULL");
            }
            _ => panic!("expected Assert"),
        }
    }

    #[test]
    fn into_results_terminal() {
        let base = DeclarativePlanNode::scan_json(vec![], simple_schema());
        let results_plan = base.into_results();
        assert_eq!(results_plan.sink.sink_type, SinkType::Results(None));
    }

    #[test]
    fn with_ordered_sets_scan_ordered_bit() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .with_ordered()
            .unwrap();
        match plan {
            DeclarativePlanNode::Scan(s) => assert!(s.ordered),
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn with_ordered_errors_off_scan() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .filter(Arc::new(Expression::literal(true)));
        let err = plan.with_ordered().unwrap_err();
        assert!(format!("{err}").contains("requires a Scan node"));
    }

    #[test]
    fn relation_ref_leaf_is_a_leaf() {
        let h = RelationHandle::fresh("r0", simple_schema());
        let plan = DeclarativePlanNode::relation_ref(h.clone());
        assert!(plan.is_leaf());
        assert_eq!(plan.children().len(), 0);
        match plan {
            DeclarativePlanNode::RelationRef(got) => assert_eq!(got, h),
            _ => panic!("expected RelationRef leaf"),
        }
    }

    #[test]
    fn join_holds_build_and_probe_subtrees() {
        let build = DeclarativePlanNode::scan_json(vec![], simple_schema());
        let probe = DeclarativePlanNode::scan_json(vec![], simple_schema());
        let plan = DeclarativePlanNode::join(
            JoinNode {
                build_keys: vec![Arc::new(Expression::column(["version"]))],
                probe_keys: vec![Arc::new(Expression::column(["version"]))],
                join_type: JoinType::LeftAnti,
                hint: JoinHint::Hash,
            },
            build,
            probe,
        );
        assert!(!plan.is_leaf());
        let children = plan.children();
        assert_eq!(children.len(), 2);
        assert!(children[0].is_leaf());
        assert!(children[1].is_leaf());
        match plan {
            DeclarativePlanNode::Join { node, .. } => {
                assert_eq!(node.join_type, JoinType::LeftAnti);
                assert_eq!(node.hint, JoinHint::Hash);
                assert_eq!(node.build_keys.len(), 1);
                assert_eq!(node.probe_keys.len(), 1);
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn into_relation_terminal_emits_relation_sink() {
        let h = RelationHandle::fresh("output", simple_schema());
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).into_relation(h.clone());
        match plan.sink.sink_type {
            SinkType::Relation(got) => assert_eq!(got, h),
            other => panic!("expected SinkType::Relation, got {other:?}"),
        }
    }

    #[test]
    fn fresh_relation_handles_are_distinct() {
        let a = RelationHandle::fresh("dup", simple_schema());
        let b = RelationHandle::fresh("dup", simple_schema());
        // Same diagnostic name but distinct ids — equality is id-based.
        assert_ne!(a, b);
        assert_eq!(a.name, b.name);
    }

    #[test]
    fn values_row_validates_field_count() {
        let schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("b", DataType::LONG),
        ]));
        DeclarativePlanNode::values_row(schema.clone(), vec![Scalar::Long(1), Scalar::Long(2)])
            .unwrap();
        let err = DeclarativePlanNode::values_row(schema, vec![Scalar::Long(1)]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("schema expects"), "message was: {msg}");
    }

    #[test]
    fn values_multirow_roundtrips_rows() {
        let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "v",
            DataType::LONG,
        )]));
        let rows = vec![
            vec![Scalar::Long(1)],
            vec![Scalar::Long(2)],
            vec![Scalar::Long(3)],
        ];
        let plan = DeclarativePlanNode::values(schema, rows.clone()).unwrap();
        match plan {
            DeclarativePlanNode::Values(lit) => assert_eq!(lit.rows, rows),
            _ => panic!("expected Values"),
        }
    }

    #[test]
    fn values_rejects_too_many_rows() {
        let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "v",
            DataType::LONG,
        )]));
        let too_many: Vec<Vec<Scalar>> = (0..LITERAL_NODE_MAX_ROWS + 1)
            .map(|_| vec![Scalar::Long(0)])
            .collect();
        let err = DeclarativePlanNode::values(schema, too_many).unwrap_err();
        assert!(format!("{err}").contains("max rows"));
    }

    #[test]
    fn with_row_index_errors_off_scan() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .filter(Arc::new(Expression::literal(true)));
        let err = plan.with_row_index("rowidx").unwrap_err();
        assert!(format!("{err}").contains("requires a Scan node"));
    }

    #[test]
    fn window_rejects_empty_order_by() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema());
        let wf = WindowFunction {
            output_col: "_rn".into(),
        };
        let err = plan
            .window(
                vec![wf],
                vec![Arc::new(Expression::column(["version"]))],
                vec![],
            )
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("non-empty order_by"),
            "unexpected message: {msg}"
        );
    }

    #[test]
    fn window_accepts_nonempty_order_by() {
        let wf = WindowFunction {
            output_col: "_rn".into(),
        };
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .window(
                vec![wf],
                vec![Arc::new(Expression::column(["version"]))],
                vec![OrderingSpec::asc(ColumnName::new(["version"]))],
            )
            .unwrap();
        assert!(matches!(plan, DeclarativePlanNode::Window { .. }));
    }

    // === Consumer-KDF tests (validated via a test-local consumer state) ===

    #[derive(Debug, Clone)]
    struct NoopConsumer;
    impl Kdf for NoopConsumer {
        fn kdf_id(&self) -> &'static str {
            "consumer.noop"
        }
        fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
            Box::new(*self)
        }
    }
    impl ConsumerKdf for NoopConsumer {
        fn apply(&mut self, _batch: &dyn crate::EngineData) -> DeltaResult<KdfControl> {
            Ok(KdfControl::Break)
        }
    }
    impl KdfOutput for NoopConsumer {
        type Output = bool;
        fn into_output(parts: Vec<Self>) -> Result<bool, DeltaError> {
            Ok(!parts.is_empty())
        }
    }

    #[test]
    fn consume_by_kdf_escape_hatch_terminates_in_sink() {
        let node = ConsumeByKdfSink::new_consumer(NoopConsumer);
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).consume_by_kdf(node);
        assert!(matches!(plan.sink.sink_type, SinkType::ConsumeByKdf(_)));
    }

    #[test]
    fn consume_produces_plan_and_extractor_with_kdf_sink() {
        use crate::plans::kdf::{FinishedHandle, TraceContext};
        use crate::plans::state_machines::framework::phase_state::PhaseState;

        let (plan, extractor) =
            DeclarativePlanNode::scan_json(vec![], simple_schema()).consume(NoopConsumer);
        assert!(matches!(plan.sink.sink_type, SinkType::ConsumeByKdf(_)));
        assert_eq!(extractor.token().kdf_id, "consumer.noop");

        let state = PhaseState::empty();
        state.submit_kdf_handle(FinishedHandle {
            token: extractor.token().clone(),
            ctx: TraceContext::new("test", "consume"),
            partition: 0,
            erased: Box::new(NoopConsumer),
        });
        assert!(extractor.extract(&state).unwrap());
    }

    #[test]
    fn token_embeds_kdf_id() {
        let node = ConsumeByKdfSink::new_consumer(NoopConsumer);
        assert_eq!(node.token.kdf_id, "consumer.noop");
        let s = node.token.to_string();
        assert!(s.starts_with("consumer.noop#"), "got: {s}");
    }
}
