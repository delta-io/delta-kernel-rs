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
//! use delta_kernel::plans::ir::nodes::RelationHandle;
//!
//! // Untyped pipeline: scan JSON, project to a sub-schema, pipe into a relation.
//! let handle = RelationHandle::fresh("scanned", schema.clone());
//! let plan = DeclarativePlanNode::scan_json(files, schema)
//!     .project(projection, output_schema)
//!     .into_relation(handle.clone());
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
//! - [`DeclarativePlanNode::with_predicate`] / [`with_row_index`] — scan modifiers.
//! - [`DeclarativePlanNode::filter`] — predicate filter.
//! - [`DeclarativePlanNode::project`] — projection.
//! - [`DeclarativePlanNode::window`] — window functions (`row_number`).
//!
//! ## Terminals
//!
//! - [`DeclarativePlanNode::into_plan`] — explicit sink.
//! - [`DeclarativePlanNode::into_relation`] — pipe output into a named
//!   [`RelationHandle`](super::nodes::RelationHandle); another plan in the same
//!   `PhaseOperation::Plans(...)` consumes via `relation_ref`, or a
//!   [`ResultPlan`](super::plan::ResultPlan) names it as the caller-facing output.
//! - [`DeclarativePlanNode::into_load`] — file-reader sink: each upstream row describes a file to
//!   open and read; optional DV masking via [`LoadSink::dv_ref`]. Materializes under a named
//!   relation handle.
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
//! [`window`]: DeclarativePlanNode::window

use std::collections::HashSet;
use std::sync::Arc;

use super::nodes::*;
use super::plan::Plan;
use crate::expressions::{Expression, Predicate, Scalar};
use crate::plans::kdf::typed::ExtractFn;
use crate::plans::kdf::{downcast_all, ConsumerKdf, KdfOutput, KdfStateToken};
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::plans::state_machines::framework::phase_state::PhaseState;
use crate::schema::{arc_schema, DataType, SchemaRef, StructField};
use crate::{DeltaResult, Error, FileMeta};

/// A single node in the declarative plan tree.
///
/// Trees are transforms-only: every complete pipeline terminates in a
/// [`Plan`] via one of the terminal methods (`into_plan`, `into_relation`,
/// `into_load`, `consume_by_kdf`) or in a `(Plan, Extractor<O>)` pair via
/// [`DeclarativePlanNode::consume`].
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

    /// Append a computed column to the row stream.
    ///
    /// Existing columns flow through unchanged; `expr` is appended as the new
    /// rightmost field. Materializes as a [`Project`](Self::Project) listing
    /// every prior column as a column reference plus `expr`. Use
    /// [`Self::output_schema`] to inspect the resulting schema.
    pub fn add_column<E: Into<Arc<Expression>>>(self, field: StructField, expr: E) -> Self {
        let expr = expr.into();
        let schema = self.output_schema();
        let mut projection: Vec<Arc<Expression>> = schema
            .fields()
            .map(|f| Arc::new(Expression::column([f.name().as_str()])))
            .collect();
        projection.push(expr);
        let new_schema = schema.build_on().with_field(field).build_unchecked();
        self.project(projection, new_schema)
    }

    /// Drop a single column. No-op when the name is absent from the current
    /// schema (the chain is returned unchanged with no [`Project`](Self::Project)
    /// emitted).
    pub fn drop_column(self, name: &str) -> Self {
        self.drop_columns([name])
    }

    /// Drop multiple columns in one step.
    ///
    /// Names absent from the current schema are silently skipped. If every
    /// requested name is absent the chain is returned unchanged.
    pub fn drop_columns<I, S>(self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let dropped: HashSet<String> = names.into_iter().map(|s| s.as_ref().to_string()).collect();
        let schema = self.output_schema();
        let kept_fields: Vec<StructField> = schema
            .fields()
            .filter(|f| !dropped.contains(f.name().as_str()))
            .cloned()
            .collect();
        if kept_fields.len() == schema.fields().count() {
            return self;
        }
        let projection: Vec<Arc<Expression>> = kept_fields
            .iter()
            .map(|f| Arc::new(Expression::column([f.name().as_str()])))
            .collect();
        let new_schema = arc_schema(kept_fields);
        self.project(projection, new_schema)
    }

    /// Keep only the newest row per `partition_by` group, ranked by `order_by`.
    ///
    /// Internally appends `row_number()` over the window, filters `rn <= 1`,
    /// then projects back to the original schema so the synthetic row-number
    /// column is invisible to the caller.
    ///
    /// Returns `Err` when `order_by` is empty (deterministic row_number
    /// requires explicit ordering) or when the reserved internal column name
    /// already exists on the current schema.
    pub fn window_dedup_by<P, O>(
        self,
        partition_by: impl IntoIterator<Item = P>,
        order_by: impl IntoIterator<Item = O>,
    ) -> DeltaResult<Self>
    where
        P: Into<Arc<Expression>>,
        O: Into<OrderingSpec>,
    {
        const RN_COL: &str = "__kernel_dedup_rn";
        let schema = self.output_schema();
        if schema.contains(RN_COL) {
            return Err(Error::generic(format!(
                "window_dedup_by: schema already contains reserved column `{RN_COL}`",
            )));
        }
        let partitions: Vec<Arc<Expression>> = partition_by.into_iter().map(Into::into).collect();
        let orderings: Vec<OrderingSpec> = order_by.into_iter().map(Into::into).collect();

        let windowed = self.window_row_number(RN_COL, partitions, orderings)?;
        let rn_le_one: Predicate = Expression::column([RN_COL]).le(Expression::literal(1i64));
        let filtered = windowed.filter(Arc::new(Expression::from_pred(rn_le_one)));

        // Restore the original schema by selecting each prior column by name.
        let projection: Vec<Arc<Expression>> = schema
            .fields()
            .map(|f| Arc::new(Expression::column([f.name().as_str()])))
            .collect();
        Ok(filtered.project(projection, schema))
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
    ///
    /// Test-only: production code receives the typed payload through
    /// [`Extractor::extract`] and does not need direct access to the token.
    #[cfg(test)]
    pub(crate) fn token(&self) -> &KdfStateToken {
        &self.token
    }

    /// Pull this extractor's payload from `state` and decode it.
    ///
    /// Drains the entries under this extractor's [`KdfStateToken`] from
    /// `state` (so a second call would see them empty) and runs the typed
    /// reduction. Decoding failures are wrapped in [`EngineError::internal`]
    /// so SM bodies can uniformly handle them on the engine-error path.
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

    /// Terminal: stream every output batch to the named relation. Another
    /// plan in the same `PhaseOperation::Plans(...)` consumes the relation
    /// via [`Self::relation_ref`], or a
    /// [`ResultPlan`](super::plan::ResultPlan) names this relation as its
    /// caller-facing output.
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
            | Self::Window { child, .. } => {
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

    /// Schema of the row stream this subtree produces.
    ///
    /// Walks the tree, deriving each node's output from its children plus the
    /// node-local schema metadata (project's output schema, window's appended
    /// row-number columns, etc.). Schemas are returned via
    /// [`crate::schema::arc_schema`] / [`crate::schema::SchemaBuilder::build_unchecked`]
    /// because every shape encountered here was already validated by the
    /// constructor that produced the node — there are no duplicate-name paths
    /// to surface.
    ///
    /// For `Union`, the canonical schema is the first child's (matching
    /// [`UnionNode`] semantics); a zero-child union yields the empty struct.
    /// For `Join::LeftAnti`, the output mirrors the probe child; for
    /// `Join::Inner` it concatenates build then probe.
    pub fn output_schema(&self) -> SchemaRef {
        match self {
            Self::Scan(scan) => scan_output_schema(scan),
            Self::FileListing(_) => file_listing_output_schema(),
            Self::Values(v) => v.schema.clone(),
            Self::RelationRef(handle) => handle.schema.clone(),
            Self::Filter { child, .. } => child.output_schema(),
            Self::Project { node, .. } => node.output_schema.clone(),
            Self::Window { child, node } => {
                let mut fields: Vec<StructField> =
                    child.output_schema().fields().cloned().collect();
                for func in &node.functions {
                    fields.push(StructField::not_null(
                        func.output_col.clone(),
                        DataType::LONG,
                    ));
                }
                arc_schema(fields)
            }
            Self::Union { children, .. } => match children.first() {
                Some(first) => first.output_schema(),
                None => arc_schema(Vec::<StructField>::new()),
            },
            Self::Join {
                build, probe, node, ..
            } => match node.join_type {
                JoinType::LeftAnti => probe.output_schema(),
                JoinType::Inner => {
                    let mut fields: Vec<StructField> =
                        build.output_schema().fields().cloned().collect();
                    fields.extend(probe.output_schema().fields().cloned());
                    arc_schema(fields)
                }
            },
        }
    }
}

// ============================================================================
// Free-function entry points
// ============================================================================

/// Start a chain from an upstream relation handle.
///
/// Clones the handle internally so the same `&RelationHandle` can feed multiple
/// plans without manual `.clone()` calls. The returned node's output schema
/// matches the relation's published schema.
pub fn relation_ref(handle: &RelationHandle) -> DeclarativePlanNode {
    DeclarativePlanNode::relation_ref(handle.clone())
}

/// Output schema for a [`ScanNode`], including any synthetic row-index column.
///
/// `ScanNode::effective_output_schema` only fails when the configured row-index
/// column name collides with an existing field; that collision is the IR
/// node's responsibility to reject at construction. When it does slip through,
/// fall back on the bare scan schema so callers get a usable answer.
fn scan_output_schema(scan: &ScanNode) -> SchemaRef {
    scan.effective_output_schema()
        .unwrap_or_else(|_| scan.schema.clone())
}

/// Canonical schema for [`DeclarativePlanNode::FileListing`]: one row per object
/// with `path`, `size`, and `modification_time` columns. Mirrors the columns
/// engines emit when they materialize a directory listing.
fn file_listing_output_schema() -> SchemaRef {
    arc_schema([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modification_time", DataType::LONG),
    ])
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

    // === Chain-method tests (free `relation_ref` + transforms on DeclarativePlanNode) ===

    fn ints_schema() -> SchemaRef {
        arc_schema([
            StructField::not_null("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ])
    }

    fn fresh_handle(name: &str) -> RelationHandle {
        RelationHandle::fresh(name, ints_schema())
    }

    #[test]
    fn relation_ref_initial_schema_matches_handle() {
        let h = fresh_handle("src");
        let node = relation_ref(&h);
        assert_eq!(node.output_schema(), h.schema);
    }

    #[test]
    fn add_column_appends_field_and_projects() {
        use crate::expressions::col;
        let h = fresh_handle("src");
        let node =
            relation_ref(&h).add_column(StructField::nullable("k", DataType::STRING), col("name"));
        let schema = node.output_schema();
        let names: Vec<String> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["id", "name", "k"]);
        match node {
            DeclarativePlanNode::Project { node, .. } => {
                assert_eq!(node.columns.len(), 3);
                assert_eq!(node.output_schema.fields().count(), 3);
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn drop_removes_field_and_skips_missing() {
        let h = fresh_handle("src");
        let dropped = relation_ref(&h).drop_column("name");
        let schema = dropped.output_schema();
        let names: Vec<String> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["id"]);

        // Dropping an absent column is a no-op (no Project emitted).
        let unchanged = relation_ref(&h).drop_column("nope");
        assert_eq!(unchanged.output_schema().fields().count(), 2);
        assert!(matches!(unchanged, DeclarativePlanNode::RelationRef(_)));
    }

    #[test]
    fn drop_columns_removes_multiple_fields() {
        let h = fresh_handle("src");
        let node = relation_ref(&h).drop_columns(["id", "name"]);
        assert_eq!(node.output_schema().fields().count(), 0);
    }

    #[test]
    fn filter_accepts_bare_predicate_and_wraps_in_filter_node() {
        use crate::expressions::col;
        let h = fresh_handle("src");
        let pred = col("id").is_not_null();
        let expr = Arc::new(Expression::from_pred(pred));
        let node = relation_ref(&h).filter(expr);
        match node {
            DeclarativePlanNode::Filter { .. } => {}
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[test]
    fn window_dedup_by_appends_window_filter_project_and_preserves_schema() {
        use crate::expressions::col;
        let h = fresh_handle("src");
        let node = relation_ref(&h)
            .window_dedup_by([col("id")], [OrderingSpec::desc(ColumnName::new(["id"]))])
            .unwrap();
        assert_eq!(node.output_schema(), h.schema);
        let DeclarativePlanNode::Project { child, node } = node else {
            panic!("expected Project at top of window_dedup_by");
        };
        assert_eq!(node.output_schema.fields().count(), 2);
        assert!(matches!(child.as_ref(), DeclarativePlanNode::Filter { .. }));
    }

    #[test]
    fn window_dedup_by_errors_on_empty_order_by() {
        let h = fresh_handle("src");
        let err = relation_ref(&h)
            .window_dedup_by(Vec::<Arc<Expression>>::new(), Vec::<OrderingSpec>::new())
            .unwrap_err();
        assert!(format!("{err}").contains("non-empty order_by"));
    }

    #[test]
    fn chained_pipeline_records_each_step_as_separate_ir_node() {
        use crate::expressions::col;
        let h = fresh_handle("src");
        let pipeline = relation_ref(&h)
            .add_column(StructField::nullable("k", DataType::STRING), col("name"))
            .filter(Arc::new(Expression::from_pred(col("k").is_not_null())))
            .drop_column("k");
        let schema = pipeline.output_schema();
        let names: Vec<String> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["id", "name"]);
        let DeclarativePlanNode::Project { child, .. } = pipeline else {
            panic!("outermost should be Project from drop_column");
        };
        let DeclarativePlanNode::Filter { child, .. } = *child else {
            panic!("middle should be Filter");
        };
        assert!(matches!(*child, DeclarativePlanNode::Project { .. }));
    }
}
