//! The [`Plan`] envelope plus free-function constructors for plan leaves.
//!
//! A [`Plan`] pairs a transforms-only plan tree with the sink describing how
//! its output is consumed. Every complete plan has exactly one sink, and the
//! sink lives on the envelope — never inside the tree.
//!
//! ## Leaf constructors
//!
//! The free functions ([`scan`], [`scan_parquet`], [`scan_json`], [`values`], [`values_row`],
//! [`relation_ref`]) parallel the inherent associated functions on [`DeclarativePlanNode`].
//! They exist for two reasons:
//!
//! 1. **Borrow, don't move** for handles — [`relation_ref`] takes `&RelationHandle` so callers can
//!    reuse the same handle in multiple plans without manual `.clone()`.
//! 2. **Top-down readability** — `plan::relation_ref(&h).filter(...).into_relation(out)` reads
//!    cleaner than `DeclarativePlanNode::relation_ref(h.clone()).filter(...).into_relation(out)`.

use super::declarative::DeclarativePlanNode;
use super::nodes::{FileFormat, LoadSink, LoadSpec, RelationHandle, SinkNode, SinkType};
use crate::expressions::Scalar;
use crate::schema::SchemaRef;
use crate::{DeltaResult, FileMeta};

/// Complete plan: a transforms-only [`DeclarativePlanNode`] tree terminated
/// by a [`SinkNode`].
#[derive(Debug, Clone)]
pub struct Plan {
    pub root: DeclarativePlanNode,
    pub sink: SinkNode,
}

impl Plan {
    /// Assemble a plan from a root subtree and a sink.
    pub fn new(root: DeclarativePlanNode, sink: SinkNode) -> Self {
        Self { root, sink }
    }
}

/// Terminal value of read-style state machines.
///
/// Names the relation the caller will read after executing [`Self::plans`]. The
/// SM body returns this value; the engine first runs every plan in `plans`
/// (each terminating in a [`SinkType::Relation`], [`SinkType::Load`], or
/// [`SinkType::ConsumeByKdf`] sink) and then materializes the row stream
/// referenced by `result_relation`.
#[derive(Debug, Clone)]
pub struct ResultPlan {
    /// Plans that must execute before the result relation is readable. Each
    /// plan publishes its output through its sink; downstream plans in the
    /// same vector may reference earlier relations through
    /// [`crate::plans::ir::DeclarativePlanNode::RelationRef`].
    pub plans: Vec<Plan>,
    /// Final relation the caller reads to obtain the SM's row stream.
    pub result_relation: RelationHandle,
}

impl ResultPlan {
    /// Build a result plan from its constituent plan vector and the handle the
    /// caller reads at the end.
    pub fn new(plans: Vec<Plan>, result_relation: RelationHandle) -> Self {
        Self {
            plans,
            result_relation,
        }
    }
}

// === Leaf constructors (F8) ===

/// Scan files with the given format. See [`DeclarativePlanNode::scan`].
pub fn scan(format: FileFormat, files: Vec<FileMeta>, schema: SchemaRef) -> DeclarativePlanNode {
    DeclarativePlanNode::scan(format, files, schema)
}

/// Scan JSON files. See [`DeclarativePlanNode::scan_json`].
pub fn scan_json(files: Vec<FileMeta>, schema: SchemaRef) -> DeclarativePlanNode {
    DeclarativePlanNode::scan_json(files, schema)
}

/// Scan Parquet files. See [`DeclarativePlanNode::scan_parquet`].
pub fn scan_parquet(files: Vec<FileMeta>, schema: SchemaRef) -> DeclarativePlanNode {
    DeclarativePlanNode::scan_parquet(files, schema)
}

/// Literal rows. See [`DeclarativePlanNode::values`].
pub fn values(schema: SchemaRef, rows: Vec<Vec<Scalar>>) -> DeltaResult<DeclarativePlanNode> {
    DeclarativePlanNode::values(schema, rows)
}

/// Single-row literal. See [`DeclarativePlanNode::values_row`].
pub fn values_row(schema: SchemaRef, values: Vec<Scalar>) -> DeltaResult<DeclarativePlanNode> {
    DeclarativePlanNode::values_row(schema, values)
}

/// Reference a previously materialized relation. Clones the handle internally so callers can
/// reuse the same `&RelationHandle` across multiple plans without writing `.clone()`.
pub fn relation_ref(handle: &RelationHandle) -> DeclarativePlanNode {
    DeclarativePlanNode::relation_ref(handle.clone())
}

// ============================================================================
// PlanCollector
// ============================================================================

/// Accumulator that turns a sequence of declarative chains and load specs into
/// a `Vec<Plan>` suitable for a single `PhaseOperation::Plans(...)`.
///
/// Each `add_*` method mints a fresh [`RelationHandle`] from the chain's
/// output schema, binds the chain's sink to that handle, pushes the resulting
/// plan onto the internal vector, and returns the handle for downstream
/// consumers to reference.
#[derive(Default)]
pub struct PlanCollector {
    plans: Vec<Plan>,
}

impl PlanCollector {
    /// Empty collector. Equivalent to `PlanCollector::default()`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint a fresh handle from `node.output_schema()`, terminate the chain in
    /// a [`SinkType::Relation`] sink keyed by that handle, and return the
    /// handle.
    pub fn add_relation(&mut self, name: &str, node: DeclarativePlanNode) -> RelationHandle {
        let handle = RelationHandle::fresh(name, node.output_schema());
        let plan = node.into_plan(SinkType::Relation(handle.clone()));
        self.plans.push(plan);
        handle
    }

    /// Mint a fresh handle whose schema is `spec.file_schema ++ resolved passthrough fields`,
    /// terminate `spec.scan_node` in the corresponding [`LoadSink`], push the resulting
    /// plan, and return the handle.
    ///
    /// Passthrough column types are resolved against the scan's published
    /// schema when possible. Names absent from the scan's schema fall back to
    /// nullable STRING in the materialized relation.
    pub fn add_load(&mut self, name: &str, spec: LoadSpec) -> RelationHandle {
        let output_schema = spec.compute_output_schema();
        let handle = RelationHandle::fresh(name, output_schema);
        let sink = LoadSink {
            output_relation: handle.clone(),
            file_schema: spec.file_schema,
            base_url: Some(spec.base_url),
            passthrough_columns: spec.passthrough_columns,
            file_meta: spec.file_meta,
            file_type: spec.file_type,
            dv_ref: spec.dv_ref,
        };
        let plan = spec.scan_node.into_load(sink);
        self.plans.push(plan);
        handle
    }

    /// Append a pre-built [`Plan`] verbatim. Useful when composing on top of
    /// a plan vector that was built outside the collector (for example, plans
    /// returned by [`build_fsr_plans`](crate::plans::state_machines::scan::build_fsr_plans))
    /// so the collector can continue minting downstream handles.
    pub fn push_plan(&mut self, plan: Plan) {
        self.plans.push(plan);
    }

    /// Consume the collector and return the accumulated plans in submission
    /// order.
    pub fn into_vec(self) -> Vec<Plan> {
        self.plans
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::ir::nodes::{FileType, SinkType};
    use crate::schema::{arc_schema, DataType, StructField};

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
    fn plan_collector_mints_handle_from_chain_output_schema() {
        let h = fresh_handle("src");
        let mut p = PlanCollector::new();
        let added = p.add_relation("derived", relation_ref(&h).drop_column("name"));
        assert_eq!(added.name, "derived");
        assert_eq!(added.schema.fields().count(), 1);
        let plans = p.into_vec();
        assert_eq!(plans.len(), 1);
        match &plans[0].sink.sink_type {
            SinkType::Relation(got) => assert_eq!(got, &added),
            other => panic!("expected Relation sink, got {other:?}"),
        }
    }

    #[test]
    fn scan_publishes_scan_schema() {
        let read_schema = arc_schema([StructField::not_null("v", DataType::LONG)]);
        let mut p = PlanCollector::new();
        let h = p.add_relation(
            "scanned",
            DeclarativePlanNode::scan(FileType::Json, vec![], read_schema.clone()),
        );
        assert_eq!(h.schema, read_schema);
        let plans = p.into_vec();
        assert!(matches!(plans[0].root, DeclarativePlanNode::Scan(_)));
    }

    #[test]
    fn add_load_emits_load_sink_against_minted_handle() {
        use url::Url;
        let scan_schema = arc_schema([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
        ]);
        let file_schema = arc_schema([StructField::nullable("payload", DataType::STRING)]);
        let base = Url::parse("memory:///log/").unwrap();
        let mut p = PlanCollector::new();
        let h = p.add_load(
            "raw",
            LoadSpec::json(vec![], scan_schema, file_schema.clone(), base, &[]),
        );
        let plans = p.into_vec();
        match &plans[0].sink.sink_type {
            SinkType::Load(ls) => {
                assert_eq!(ls.output_relation, h);
                assert_eq!(ls.file_schema, file_schema);
            }
            other => panic!("expected Load sink, got {other:?}"),
        }
    }
}
