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
//! ## Core API
//!
//! - Constructors: [`DeclarativePlanNode::scan_json`] / [`scan_parquet`],
//!   [`DeclarativePlanNode::values`], [`DeclarativePlanNode::relation_ref`].
//! - Combinators: [`DeclarativePlanNode::union`].
//! - Transforms: [`DeclarativePlanNode::filter`], [`DeclarativePlanNode::project`],
//!   [`DeclarativePlanNode::window`].
//! - Terminals: [`DeclarativePlanNode::into_plan`], [`DeclarativePlanNode::into_relation`],
//!   [`DeclarativePlanNode::into_load`], [`DeclarativePlanNode::consume`],
//!   explicit `into_plan(SinkType::Consume(...))`.
//!
//! [`scan_json`]: DeclarativePlanNode::scan_json
//! [`scan_parquet`]: DeclarativePlanNode::scan_parquet
//! [`window`]: DeclarativePlanNode::window

use std::sync::Arc;

use super::nodes::*;
use super::plan::Plan;
use crate::expressions::{Expression, Scalar};
use crate::plans::kdf::typed::make_extract;
use crate::plans::kdf::{ConsumerKdf, Extractor, KdfOutput};
use crate::schema::{arc_schema, DataType, SchemaRef, StructField};
use crate::{DeltaResult, Error, FileMeta};

/// A single node in the declarative plan tree.
#[derive(Debug, Clone)]
pub enum DeclarativePlanNode {
    // Leaves
    Scan(ScanNode),
    FileListing(FileListingNode),
    Values(ValuesNode),
    /// Read batches from a relation produced by another plan 
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
// Leaves — static constructors
// ============================================================================

impl DeclarativePlanNode {
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

    /// Union of N child streams.
    ///
    /// - `ordered = false`: the engine may interleave/reorder children.
    /// - `ordered = true`: children are consumed in declaration order.
    ///
    /// For `ordered = true`, a single child is unwrapped.
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
        Ok(Self::Union {
            children,
            node: UnionNode { ordered },
        })
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

    /// Join `self` (left side) with `right` using same-name, same-type key columns.
    ///
    /// Key columns are inferred from same-name top-level fields in left/right schemas.
    /// Any same-name column with mismatched types is rejected.
    pub fn join(self, right: Self, join_type: JoinType) -> DeltaResult<Self> {
        let left_schema = self.output_schema()?;
        let right_schema = right.output_schema()?;
        let mut right_keys = Vec::new();
        let mut left_keys = Vec::new();
        for left_field in left_schema.fields() {
            let name = left_field.name();
            if let Some(right_field) = right_schema.field(name) {
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
        Ok(Self::Join {
            build: Box::new(self),
            probe: Box::new(right),
            node: JoinNode {
                left_keys,
                right_keys,
                join_type,
            },
        })
    }

}

// ============================================================================
// Terminals
// ============================================================================

impl DeclarativePlanNode {
    /// Wrap this tree in a [`Plan`] with the given sink.
    pub fn into_plan(self, sink_type: SinkType) -> Plan {
        Plan::new(self, sink_type)
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

    /// Typed consumer terminal. Wraps `self` in a [`Plan`] terminating in
    /// [`SinkType::Consume`] and returns the plan paired with an
    /// [`Extractor<O>`] that pulls the typed output from the resulting
    /// [`PhaseState`].
    pub fn consume<S>(self, state: S) -> (Plan, Extractor<S::Output>)
    where
        S: ConsumerKdf + KdfOutput + 'static,
    {
        let node = ConsumeSink::new_consumer(state);
        let token = node.token.clone();
        let extract = make_extract::<S>(token.clone());
        let plan = self.into_plan(SinkType::Consume(node));
        (plan, Extractor::new(token, extract))
    }
}

// ============================================================================
// Introspection
// ============================================================================

impl DeclarativePlanNode {
    /// Schema of the row stream this subtree produces.
    ///
    /// Walks the tree, deriving each node's output from its children plus the
    /// node-local schema metadata (project's output schema, window's appended
    /// row-number columns, etc.). Schemas are returned via
    /// [`crate::schema::arc_schema`] / [`crate::schema::SchemaBuilder::build_unchecked`].
    ///
    /// For `Union`, the canonical schema is the first child's (matching
    /// [`UnionNode`] semantics); a zero-child union yields the empty struct.
    /// For `Join::LeftAnti`, the output mirrors the right child; for
    /// `Join::Inner` it concatenates left then right.
    ///
    /// Returns an error when any node-local schema contract is invalid.
    pub fn output_schema(&self) -> DeltaResult<SchemaRef> {
        match self {
            Self::Scan(scan) => Ok(scan.schema.clone()),
            Self::FileListing(_) => Ok(file_listing_output_schema()),
            Self::Values(v) => Ok(v.schema.clone()),
            Self::RelationRef(handle) => Ok(handle.schema.clone()),
            Self::Filter { child, .. } => child.output_schema(),
            Self::Project { node, .. } => Ok(node.output_schema.clone()),
            Self::Window { child, node } => {
                let mut fields: Vec<StructField> = child.output_schema()?.fields().cloned().collect();
                for func in &node.functions {
                    fields.push(StructField::not_null(
                        func.output_col.clone(),
                        DataType::LONG,
                    ));
                }
                Ok(arc_schema(fields))
            }
            Self::Union { children, .. } => match children.first() {
                Some(first) => first.output_schema(),
                None => Ok(arc_schema(Vec::<StructField>::new())),
            },
            Self::Join {
                build: left,
                probe: right,
                node,
                ..
            } => match node.join_type {
                JoinType::LeftAnti => right.output_schema(),
                JoinType::Inner => {
                    let mut fields: Vec<StructField> = left.output_schema()?.fields().cloned().collect();
                    fields.extend(right.output_schema()?.fields().cloned());
                    Ok(arc_schema(fields))
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
    fn join_rejects_same_name_type_mismatch() {
        let left_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "k",
            DataType::LONG,
        )]));
        let right_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "k",
            DataType::STRING,
        )]));
        let left = DeclarativePlanNode::scan_json(vec![], left_schema);
        let right = DeclarativePlanNode::scan_json(vec![], right_schema);
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
        let result = DeclarativePlanNode::values(schema, vec![row]);
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
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .window(
                vec![wf],
                vec![Arc::new(Expression::column(["version"]))],
                order_by,
            )
            .unwrap();
        assert!(matches!(plan, DeclarativePlanNode::Window { .. }));
    }

    #[rstest]
    #[case(
        DeclarativePlanNode::union(Vec::<DeclarativePlanNode>::new(), false).unwrap(),
        vec![]
    )]
    #[case(
        DeclarativePlanNode::FileListing(FileListingNode {
            path: url::Url::parse("file:///tmp").unwrap(),
        }),
        vec!["path", "size", "modification_time"]
    )]
    fn output_schema_cases_match_expected_fields(
        #[case] node: DeclarativePlanNode,
        #[case] expected_fields: Vec<&str>,
    ) {
        let names: Vec<String> = node
            .output_schema()
            .unwrap()
            .fields()
            .map(|f| f.name().clone())
            .collect();
        let expected: Vec<String> = expected_fields.iter().map(|s| s.to_string()).collect();
        assert_eq!(names, expected);
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
    fn consume_produces_plan_and_extractor_with_consume_sink() {
        use crate::plans::kdf::{FinishedHandle, TraceContext};
        use crate::plans::state_machines::framework::phase_state::PhaseState;

        let (plan, extractor) =
            DeclarativePlanNode::scan_json(vec![], simple_schema()).consume(NoopConsumer);
        let _ = plan;
        assert_eq!(
            extractor.token().kdf_type,
            crate::plans::kdf::token::KdfType::Consumer("consumer.noop".to_string())
        );

        let state = PhaseState::empty();
        state.submit_kdf_handle(FinishedHandle {
            token: extractor.token().clone(),
            ctx: TraceContext::new("test", "consume"),
            erased: Box::new(NoopConsumer),
        });
        assert!(extractor.extract(&state).unwrap());
    }

    #[test]
    fn token_embeds_kdf_type_name() {
        let node = ConsumeSink::new_consumer(NoopConsumer);
        assert_eq!(
            node.token.kdf_type,
            crate::plans::kdf::token::KdfType::Consumer("consumer.noop".to_string())
        );
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
        assert_eq!(node.output_schema().unwrap(), h.schema);
    }

}
