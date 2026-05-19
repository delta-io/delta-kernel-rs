//! The [`DeclarativePlanNode`] tree plus the [`PlanBuilder`] fluent constructor.
//!
//! # Construction API
//!
//! [`PlanBuilder`] is the only path that builds [`DeclarativePlanNode`] trees. Each constructor
//! / transform carries the cumulative output schema alongside the node so downstream operators
//! never have to re-derive it.
//!
//! ```ignore
//! use delta_kernel::plans::ir::PlanBuilder;
//! use delta_kernel::plans::ir::RelationRegistry;
//! use uuid::Uuid;
//!
//! // Untyped pipeline: scan JSON, project to a sub-schema, publish to a named relation.
//! // The terminal registers the plan into the registry as a side effect; the registry's
//! // `into_result_plan(name)` later drains the accumulator into a finished `ResultPlan`.
//! let mut registry = RelationRegistry::new(Uuid::new_v4(), "");
//! let _handle = PlanBuilder::scan_json(files, schema)
//!     .project(projection, output_schema)
//!     .into_relation("scanned", &mut registry)?;
//! let result_plan = registry.into_result_plan("scanned")?;
//!
//! // Typed pipeline: scan JSON, drain into a typed consumer KDF, recover the typed output from
//! // the resulting PhaseState.
//! let (plan, extractor) =
//!     PlanBuilder::scan_json(files, schema).consume(CheckpointHintReader::default());
//! // ... after `phase.execute(PhaseOperation::Plans(vec![plan]), name).await?` ...
//! // let hint = extractor.extract(&state)?;
//! ```
//!
//! ## Core API
//!
//! - Constructors: [`PlanBuilder::scan_json`] / [`scan_parquet`](PlanBuilder::scan_parquet),
//!   [`PlanBuilder::values`], [`PlanBuilder::file_listing`], [`PlanBuilder::union`].
//! - Transforms: [`PlanBuilder::filter`], [`PlanBuilder::project`], [`PlanBuilder::project_pair`],
//!   [`PlanBuilder::add_column`], [`PlanBuilder::window`], [`PlanBuilder::join`],
//!   [`PlanBuilder::join_on`].
//! - Terminals: [`PlanBuilder::into_relation`] (register-as-side-effect), [`PlanBuilder::load`]
//!   (register-as-side-effect), [`PlanBuilder::into_result_plan`] (combined register + drain),
//!   [`PlanBuilder::into_consume`], [`PlanBuilder::consume`].

use std::sync::Arc;

use url::Url;

use super::nodes::*;
use super::plan::Plan;
use super::relation_registry::RelationRegistry;
use crate::expressions::{ColumnName, Expression, Scalar};
use crate::plans::errors::DeltaError;
use crate::plans::kdf::{ConsumerKdf, Extractor, KdfOutput};
use crate::schema::{arc_schema, DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error, FileMeta};

/// A single node in the declarative plan tree.
///
/// The enum carries node-local payloads only; the cumulative output schema is tracked alongside
/// the tree by [`PlanBuilder`].
#[derive(Debug, Clone)]
pub enum DeclarativePlanNode {
    // Leaves
    Scan(ScanNode),
    FileListing(FileListingNode),
    Values(ValuesNode),
    /// Read batches from a relation produced by another plan terminating in
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

    // Binary. See [`JoinNode`].
    Join {
        build: Box<DeclarativePlanNode>,
        probe: Box<DeclarativePlanNode>,
        node: JoinNode,
    },
}

// ============================================================================
// PlanBuilder — fluent construction with eager schema derivation
// ============================================================================

/// Cumulative builder for a [`DeclarativePlanNode`] tree.
///
/// Every constructor stamps the leaf's output schema; every transform recomputes the new
/// cumulative output schema from `self.schema` plus the transform's local contribution. The
/// terminal methods consume the builder and produce a [`Plan`].
#[derive(Debug, Clone)]
pub struct PlanBuilder {
    schema: SchemaRef,
    node: DeclarativePlanNode,
}

// --- Leaves -----------------------------------------------------------------

impl PlanBuilder {
    /// Scan JSON files with an explicit schema.
    pub fn scan_json(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        let node =
            DeclarativePlanNode::Scan(ScanNode::new(FileType::Json, files, Arc::clone(&schema)));
        Self { schema, node }
    }

    /// Scan Parquet files with an explicit schema.
    pub fn scan_parquet(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        let node =
            DeclarativePlanNode::Scan(ScanNode::new(FileType::Parquet, files, Arc::clone(&schema)));
        Self { schema, node }
    }

    /// Wrap a pre-built [`ScanNode`] (used by callers that need to attach `with_predicate`
    /// before sealing the builder).
    pub fn from_scan(scan: ScanNode) -> Self {
        let schema = Arc::clone(&scan.schema);
        Self {
            schema,
            node: DeclarativePlanNode::Scan(scan),
        }
    }

    /// Multi-row `VALUES`-style literal table. Rows × columns layout; see [`ValuesNode`]
    /// invariants.
    pub fn values(schema: SchemaRef, rows: Vec<Vec<Scalar>>) -> DeltaResult<Self> {
        let values = ValuesNode::try_new(Arc::clone(&schema), rows)?;
        Ok(Self {
            schema,
            node: DeclarativePlanNode::Values(values),
        })
    }

    /// Internal constructor from a registered relation handle.
    pub(crate) fn from_relation_handle(handle: RelationHandle) -> Self {
        let schema = Arc::clone(&handle.schema);
        Self {
            schema,
            node: DeclarativePlanNode::RelationRef(handle),
        }
    }

    /// One-row-per-object directory listing leaf. Output schema is the canonical
    /// `(path, size, modification_time)` triple.
    pub fn file_listing(path: Url) -> Self {
        Self {
            schema: file_listing_output_schema(),
            node: DeclarativePlanNode::FileListing(FileListingNode { path }),
        }
    }

    /// Union of N child streams. See [`UnionNode`] for ordered/unordered semantics.
    ///
    /// - `ordered = false`: the engine may interleave/reorder children.
    /// - `ordered = true`: children are consumed in declaration order.
    ///
    /// For `ordered = true` with a single child, the child is unwrapped (returned directly).
    /// The cumulative schema is the first child's schema (matching [`UnionNode`] semantics);
    /// a zero-child union yields the empty struct.
    pub fn union<I>(children: I, ordered: bool) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        let mut children: Vec<Self> = children.into_iter().collect();
        if ordered && children.len() == 1 {
            return children
                .pop()
                .ok_or_else(|| Error::generic("internal: single-child union drained empty"));
        }
        let schema = children
            .first()
            .map(|c| Arc::clone(&c.schema))
            .unwrap_or_else(empty_schema);
        let nodes: Vec<DeclarativePlanNode> = children.into_iter().map(|c| c.node).collect();
        Ok(Self {
            schema,
            node: DeclarativePlanNode::Union {
                children: nodes,
                node: UnionNode { ordered },
            },
        })
    }
}

// --- Transforms -------------------------------------------------------------

impl PlanBuilder {
    /// Wrap `self` in a [`Filter`](DeclarativePlanNode::Filter). Schema unchanged.
    pub fn filter(self, predicate: Arc<Expression>) -> Self {
        Self {
            schema: self.schema,
            node: DeclarativePlanNode::Filter {
                child: Box::new(self.node),
                node: FilterNode { predicate },
            },
        }
    }

    /// Wrap `self` in a [`Project`](DeclarativePlanNode::Project). New schema is `output_schema`.
    ///
    /// `columns` accepts any iterator of `Arc<Expression>` so call sites can chain
    /// `iter::once`, slice literals, and `Vec`s without an explicit `.collect()`.
    pub fn project(
        self,
        columns: impl IntoIterator<Item = Arc<Expression>>,
        output_schema: SchemaRef,
    ) -> Self {
        let columns: Vec<Arc<Expression>> = columns.into_iter().collect();
        Self {
            schema: Arc::clone(&output_schema),
            node: DeclarativePlanNode::Project {
                child: Box::new(self.node),
                node: ProjectNode {
                    columns,
                    output_schema,
                },
            },
        }
    }

    /// Wrap `self` in a [`Project`](DeclarativePlanNode::Project) seeded from a paired
    /// `(schema, projection)` tuple. Convenience for the common case of binding the chain
    /// to a base shape produced by an action-pair helper before chaining further `add_column`
    /// calls.
    pub fn project_pair(self, pair: (SchemaRef, Vec<Arc<Expression>>)) -> Self {
        let (output_schema, columns) = pair;
        self.project(columns, output_schema)
    }

    /// Append a single `(field, expr)` column to the head [`Project`] node's projection.
    ///
    /// If `self` is already a `Project`, the head node is extended in place: `expr` is appended
    /// to its `columns` and `field` is appended to its `output_schema`. Otherwise the chain is
    /// wrapped in a passthrough+append `Project` that emits every current column followed by
    /// the new one.
    ///
    /// Used to layer dedup-key / version / synthetic columns onto a base projection without
    /// destructuring the pair at the call site.
    pub fn add_column(mut self, field: StructField, expr: Arc<Expression>) -> Self {
        match &mut self.node {
            DeclarativePlanNode::Project { node, .. } => {
                node.columns.push(expr);
                let mut fields: Vec<StructField> = node.output_schema.fields().cloned().collect();
                fields.push(field);
                let new_schema = arc_schema(fields);
                node.output_schema = Arc::clone(&new_schema);
                self.schema = new_schema;
                self
            }
            _ => {
                // No head Project: synthesize one that passes every current field through by
                // name and appends the new column.
                let mut fields: Vec<StructField> = self.schema.fields().cloned().collect();
                let mut columns: Vec<Arc<Expression>> = self
                    .schema
                    .fields()
                    .map(|f| Arc::new(Expression::column([f.name().as_str()])))
                    .collect();
                fields.push(field);
                columns.push(expr);
                let output_schema = arc_schema(fields);
                self.project(columns, output_schema)
            }
        }
    }

    /// Wrap `self` in a [`Window`](DeclarativePlanNode::Window) with partition keys and ordering
    /// specs. New schema is `self.schema` plus one `NOT NULL LONG` field per window function.
    pub fn window(
        self,
        functions: Vec<WindowFunction>,
        partition_by: Vec<Arc<Expression>>,
        order_by: Vec<OrderingSpec>,
    ) -> DeltaResult<Self> {
        let node = WindowNode::try_new(functions, partition_by, order_by)?;
        let mut fields: Vec<StructField> = self.schema.fields().cloned().collect();
        for func in &node.functions {
            fields.push(StructField::not_null(
                func.output_col.clone(),
                DataType::LONG,
            ));
        }
        let new_schema = arc_schema(fields);
        Ok(Self {
            schema: new_schema,
            node: DeclarativePlanNode::Window {
                child: Box::new(self.node),
                node,
            },
        })
    }

    /// Join `self` (left side) with `right` using same-name, same-type key columns.
    ///
    /// Key columns are inferred from same-name top-level fields in left/right schemas.
    /// Any same-name column with mismatched types is rejected.
    pub fn join(self, right: Self, join_type: JoinType) -> DeltaResult<Self> {
        let mut right_keys = Vec::new();
        let mut left_keys = Vec::new();
        for left_field in self.schema.fields() {
            let name = left_field.name();
            if let Some(right_field) = right.schema.field(name) {
                if right_field.data_type() != left_field.data_type() {
                    return Err(Error::generic(format!(
                        "join key type mismatch for column `{name}`: left={:?}, right={:?}",
                        left_field.data_type(),
                        right_field.data_type()
                    )));
                }
                right_keys.push(Arc::new(Expression::column([name.as_str()])));
                left_keys.push(Arc::new(Expression::column([name.as_str()])));
            }
        }
        if right_keys.is_empty() {
            return Err(Error::generic(
                "join requires at least one same-name same-type key column between left/right",
            ));
        }
        self.join_on(right, left_keys, right_keys, join_type)
    }

    /// Join `self` (left side) with `right` using explicit key expressions.
    ///
    /// Schema follows [`JoinType`]:
    /// - [`JoinType::Inner`]: left fields concatenated with right fields.
    /// - [`JoinType::LeftAnti`]: right schema (probe rows that did NOT match).
    pub fn join_on(
        self,
        right: Self,
        left_keys: Vec<Arc<Expression>>,
        right_keys: Vec<Arc<Expression>>,
        join_type: JoinType,
    ) -> DeltaResult<Self> {
        if left_keys.is_empty() || right_keys.is_empty() {
            return Err(Error::generic(
                "join_on requires at least one key on both left/right",
            ));
        }
        if left_keys.len() != right_keys.len() {
            return Err(Error::generic(format!(
                "join_on requires the same key count on left/right, got left={}, right={}",
                left_keys.len(),
                right_keys.len()
            )));
        }
        let output_schema = match join_type {
            JoinType::LeftAnti => Arc::clone(&right.schema),
            JoinType::Inner => {
                let mut fields: Vec<StructField> = self.schema.fields().cloned().collect();
                fields.extend(right.schema.fields().cloned());
                arc_schema(fields)
            }
        };
        Ok(Self {
            schema: Arc::clone(&output_schema),
            node: DeclarativePlanNode::Join {
                build: Box::new(self.node),
                probe: Box::new(right.node),
                node: JoinNode {
                    left_keys,
                    right_keys,
                    join_type,
                    output_schema,
                },
            },
        })
    }
}

// --- Accessors --------------------------------------------------------------

impl PlanBuilder {
    /// Cumulative output schema of the row stream this subtree produces.
    pub fn schema_ref(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Borrow the underlying node tree (useful for tests / debug formatting).
    pub fn node(&self) -> &DeclarativePlanNode {
        &self.node
    }
}

// --- Terminals --------------------------------------------------------------

impl PlanBuilder {
    /// Terminal: register a `SinkType::Relation` for the chain under `name` and push the
    /// resulting [`Plan`] into the registry's accumulator. Returns the minted
    /// [`RelationHandle`] for callers that need it (typically discarded; downstream code
    /// references the relation by name via
    /// [`RelationRegistry::relation_ref`](super::relation_registry::RelationRegistry::relation_ref)).
    ///
    /// The accumulator is drained later via
    /// [`RelationRegistry::into_result_plan`](super::relation_registry::RelationRegistry::into_result_plan)
    /// or [`Self::consume_phase`].
    pub fn into_relation(
        self,
        name: &str,
        registry: &mut RelationRegistry,
    ) -> Result<RelationHandle, DeltaError> {
        let handle = registry.register_relation_sink(name, self.schema_ref())?;
        let plan = Plan::new(self.node, SinkType::Relation(handle.clone()));
        registry.push_plan(plan);
        Ok(handle)
    }

    /// Terminal: file-reader sink. For each upstream row, open the resolved file, read
    /// [`LoadSink::file_schema`] columns (with optional DV masking when [`LoadSink::dv_ref`]
    /// names an upstream descriptor column), and materialize the result under
    /// [`LoadSink::output_relation`] for downstream
    /// [`RelationRegistry::relation_ref`](super::relation_registry::RelationRegistry::relation_ref)
    /// consumers in the same phase. See [`LoadSink`].
    ///
    /// Registers the resulting [`Plan`] into the registry's accumulator as a side effect and
    /// returns the minted [`RelationHandle`] (typically discarded; downstream code references
    /// the relation by name).
    #[allow(clippy::too_many_arguments)]
    pub fn load(
        self,
        name: &str,
        file_schema: SchemaRef,
        file_type: FileType,
        base_url: Option<Url>,
        passthrough_columns: Vec<ColumnName>,
        file_meta: ScanFileColumns,
        dv_ref: Option<DvRef>,
        registry: &mut RelationRegistry,
    ) -> Result<RelationHandle, DeltaError> {
        let handle = registry.register_load_sink(
            name,
            &self.schema_ref(),
            &file_schema,
            &passthrough_columns,
            file_type,
        )?;
        let mut sink = LoadSink::new(handle.clone(), file_schema, file_type)
            .with_passthrough_columns(passthrough_columns)
            .with_file_meta(file_meta);
        if let Some(base_url) = base_url {
            sink = sink.with_base_url(base_url);
        }
        if let Some(dv_ref) = dv_ref {
            sink = sink.with_dv_ref(dv_ref);
        }
        let plan = Plan::new(self.node, SinkType::Load(Box::new(sink)));
        registry.push_plan(plan);
        Ok(handle)
    }

    /// Terminal convenience: bind this chain to a `SinkType::Relation` named `name`, then
    /// immediately drain the registry's accumulator into a finished [`ResultPlan`] whose
    /// terminal handle is the one just minted.
    ///
    /// Equivalent to `self.into_relation(name, ctx)?; ctx.into_result_plan(name)` — kept as a
    /// single method so call sites at the end of a pipeline don't have to crack the chain
    /// open to recover the terminal name twice.
    pub fn into_result_plan(
        self,
        name: &str,
        registry: &mut RelationRegistry,
    ) -> Result<crate::plans::ir::plan::ResultPlan, DeltaError> {
        self.into_relation(name, registry)?;
        registry.into_result_plan(name)
    }

    /// Terminal: explicit [`SinkType::Consume`] with a pre-built [`ConsumeSink`]. Use when the
    /// caller already wired the sink (e.g. to share a token outside the builder), otherwise
    /// prefer [`Self::consume`] which constructs the sink and returns a paired [`Extractor`].
    pub fn into_consume(self, sink: ConsumeSink) -> Plan {
        Plan::new(self.node, SinkType::Consume(sink))
    }

    /// Typed consumer terminal. Wraps `self` in a [`Plan`] terminating in
    /// [`SinkType::Consume`] and returns the plan paired with an [`Extractor<O>`] that pulls
    /// the typed output from the resulting
    /// [`PhaseState`](crate::plans::state_machines::framework::phase_state::PhaseState).
    pub fn consume<S>(self, state: S) -> (Plan, Extractor<S::Output>)
    where
        S: ConsumerKdf + KdfOutput + 'static,
    {
        let node = ConsumeSink::new_consumer(state);
        let token = node.token.clone();
        let plan = Plan::new(self.node, SinkType::Consume(node));
        (plan, Extractor::for_kdf::<S>(token))
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Canonical schema for [`DeclarativePlanNode::FileListing`]: one row per object with `path`,
/// `size`, and `modification_time` columns. Mirrors the columns engines emit when they
/// materialize a directory listing.
fn file_listing_output_schema() -> SchemaRef {
    arc_schema([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modification_time", DataType::LONG),
    ])
}

fn empty_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked(Vec::<StructField>::new()))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::any::Any;

    use rstest::rstest;

    use super::*;
    use crate::expressions::{ColumnName, Expression};
    use crate::plans::errors::DeltaError;
    use crate::plans::ir::nodes::{OrderingSpec, WindowFunction};
    use crate::plans::kdf::{ConsumerKdf, ConsumerKdfId, KdfControl, KdfOutput};
    use crate::schema::{DataType, StructField, StructType};

    fn simple_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::not_null(
            "version",
            DataType::LONG,
        )]))
    }

    #[test]
    fn join_rejects_same_name_type_mismatch() {
        let left_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "k",
            DataType::LONG,
        )]));
        let right_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "k",
            DataType::STRING,
        )]));
        let left = PlanBuilder::scan_json(vec![], left_schema);
        let right = PlanBuilder::scan_json(vec![], right_schema);
        let err = left.join(right, JoinType::Inner).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("join key type mismatch for column `k`"),
            "unexpected message: {msg}"
        );
    }

    #[test]
    fn fresh_relation_handles_are_distinct() {
        let a = RelationHandle::fresh("dup", simple_schema());
        let b = RelationHandle::fresh("dup", simple_schema());
        // Same diagnostic name but distinct ids — equality is id-based.
        assert_ne!(a, b);
        assert_eq!(a.name, b.name);
    }

    #[rstest]
    #[case(vec![Scalar::Long(1), Scalar::Long(2)], true)]
    #[case(vec![Scalar::Long(1)], false)]
    fn values_field_count_cases(#[case] row: Vec<Scalar>, #[case] expect_ok: bool) {
        let schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("b", DataType::LONG),
        ]));
        let result = PlanBuilder::values(schema, vec![row]);
        if expect_ok {
            assert!(result.is_ok());
        } else {
            let msg = format!("{}", result.unwrap_err());
            assert!(msg.contains("schema expects"), "message was: {msg}");
        }
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![OrderingSpec::asc(ColumnName::new(["version"]))])]
    fn window_order_by_cases(#[case] order_by: Vec<OrderingSpec>) {
        let wf = WindowFunction {
            output_col: "_rn".into(),
        };
        let plan = PlanBuilder::scan_json(vec![], simple_schema())
            .window(
                vec![wf],
                vec![Arc::new(Expression::column(["version"]))],
                order_by,
            )
            .unwrap();
        assert!(matches!(plan.node(), DeclarativePlanNode::Window { .. }));
    }

    #[test]
    fn empty_union_yields_empty_schema() {
        let pb = PlanBuilder::union(Vec::<PlanBuilder>::new(), false).unwrap();
        assert_eq!(pb.schema_ref().fields().count(), 0);
    }

    #[test]
    fn file_listing_schema_is_canonical_triple() {
        let pb = PlanBuilder::file_listing(url::Url::parse("file:///tmp").unwrap());
        let names: Vec<String> = pb.schema_ref().fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["path", "size", "modification_time"]);
    }

    // === Consumer-KDF tests (validated via a test-local consumer state) ===

    #[derive(Debug, Clone)]
    struct NoopConsumer;
    impl ConsumerKdf for NoopConsumer {
        fn kdf_id(&self) -> ConsumerKdfId {
            ConsumerKdfId::CheckpointHint
        }
        fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
            Box::new(*self)
        }
        fn apply(&mut self, _batch: &dyn crate::EngineData) -> DeltaResult<KdfControl> {
            Ok(KdfControl::Break)
        }
    }
    impl KdfOutput for NoopConsumer {
        type Output = bool;
        fn into_output(self) -> Result<bool, DeltaError> {
            Ok(true)
        }
    }

    #[test]
    fn consume_produces_plan_and_extractor_with_consume_sink() {
        use uuid::Uuid;

        use crate::plans::kdf::FinishedHandle;
        use crate::plans::state_machines::framework::phase_state::PhaseState;

        let (plan, extractor) =
            PlanBuilder::scan_json(vec![], simple_schema()).consume(NoopConsumer);
        let _ = plan;
        assert_eq!(extractor.token().kdf_id, ConsumerKdfId::CheckpointHint);

        let state = PhaseState::empty();
        state.submit_kdf_handle(FinishedHandle {
            token: extractor.token().clone(),
            sm_id: Uuid::new_v4(),
            sm_kind: "test",
            phase_name: "consume",
            erased: Box::new(NoopConsumer),
        });
        assert!(extractor.extract(&state).unwrap());
    }

    #[test]
    fn token_embeds_kdf_id_name() {
        let node = ConsumeSink::new_consumer(NoopConsumer);
        assert_eq!(node.token.kdf_id, ConsumerKdfId::CheckpointHint);
        let s = node.token.to_string();
        assert!(s.starts_with("checkpoint_hint#"), "got: {s}");
    }

    // === Chain-method tests (relation_ref + transforms on PlanBuilder) ===

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
        let pb = PlanBuilder::from_relation_handle(h.clone());
        assert_eq!(pb.schema_ref(), h.schema);
    }

    #[test]
    fn join_inner_dedups_same_name_columns_join_left_anti_mirrors_right() {
        let left = PlanBuilder::scan_json(
            vec![],
            arc_schema([StructField::not_null("k", DataType::LONG)]),
        );
        let right = PlanBuilder::scan_json(
            vec![],
            arc_schema([
                StructField::not_null("k", DataType::LONG),
                StructField::nullable("v", DataType::STRING),
            ]),
        );
        let inner = left.clone().join(right.clone(), JoinType::Inner).unwrap();
        let inner_names: Vec<String> = inner
            .schema_ref()
            .fields()
            .map(|f| f.name().clone())
            .collect();
        // Inner join collapses the duplicate `k` to a single column: `k` appears once even
        // though both inputs carry it.
        assert_eq!(inner_names, vec!["k", "v"]);
        let anti = left.join(right.clone(), JoinType::LeftAnti).unwrap();
        let anti_names: Vec<String> = anti
            .schema_ref()
            .fields()
            .map(|f| f.name().clone())
            .collect();
        let right_names: Vec<String> = right
            .schema_ref()
            .fields()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(anti_names, right_names);
    }
}
