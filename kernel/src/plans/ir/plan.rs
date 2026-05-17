//! The [`Plan`] envelope plus small free-function plan helpers.
//!
//! A [`Plan`] pairs a transforms-only plan tree with the sink describing how
//! its output is consumed. Every complete plan has exactly one sink, and the
//! sink lives on the envelope — never inside the tree.
//!
//! ## Plan helpers
//!
//! [`relation_ref`] is kept as borrow-friendly sugar (`&RelationHandle` -> cloned handle) so
//! callers can reuse the same handle in multiple chains without spelling `.clone()` at each
//! callsite.

use std::sync::Arc;
use url::Url;

use super::declarative::{DeclarativePlanNode, PlanBuilder};
use super::nodes::{DvRef, FileType, LoadSink, RelationHandle, ScanFileColumns, SinkType};
use crate::expressions::{col, ColumnName, Expression};
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::schema::{arc_schema, SchemaRef, StructType};

/// Complete plan: a transforms-only [`DeclarativePlanNode`] tree terminated
/// by a [`SinkType`].
#[derive(Debug, Clone)]
pub struct Plan {
    pub root: DeclarativePlanNode,
    pub sink: SinkType,
}

impl Plan {
    /// Assemble a plan from a root subtree and a sink.
    pub fn new(root: DeclarativePlanNode, sink: SinkType) -> Self {
        Self { root, sink }
    }
}

/// Terminal value of read-style state machines.
///
/// Names the relation the caller will read after executing [`Self::plans`]. The
/// SM body returns this value; the engine first runs every plan in `plans`
/// (each terminating in a [`SinkType::Relation`], [`SinkType::Load`], or
/// [`SinkType::Consume`] sink) and then materializes the row stream
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

/// Reference a previously materialized relation. Clones the handle internally so callers can
/// reuse the same `&RelationHandle` across multiple plans without writing `.clone()`.
pub fn relation_ref(handle: &RelationHandle) -> PlanBuilder {
    PlanBuilder::relation_ref(handle.clone())
}

/// Project every field of `parent`'s nested struct schema as a top-level column reference
/// nested under `parent`. Equivalent to applying `Transform::identity()` to the nested struct
/// and projecting each field individually.
pub fn identity_project_struct(parent: &str, schema: &StructType) -> Vec<Arc<Expression>> {
    schema
        .fields()
        .map(|f| col([parent, f.name().as_str()]).into())
        .collect()
}

// ============================================================================
// PlanCollector
// ============================================================================

/// Accumulator that turns a sequence of declarative chains and load sinks into
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

    /// Mint a fresh handle from `node.schema_ref()`, terminate the chain in a
    /// [`SinkType::Relation`] sink keyed by that handle, and return the handle.
    pub fn add_relation(
        &mut self,
        name: &str,
        node: PlanBuilder,
    ) -> Result<RelationHandle, DeltaError> {
        let handle = RelationHandle::fresh(name, node.schema_ref());
        let plan = node.into_relation(handle.clone());
        self.plans.push(plan);
        Ok(handle)
    }

    /// Mint a fresh handle whose schema is `sink.file_schema ++ resolved passthrough fields`,
    /// terminate `scan_node` in the corresponding [`LoadSink`], push the resulting plan, and
    /// return the handle.
    ///
    /// Passthrough column types are resolved against the scan's published schema and must
    /// exist there.
    pub fn add_load(
        &mut self,
        name: &str,
        scan_node: PlanBuilder,
        file_schema: SchemaRef,
        file_type: FileType,
        base_url: Option<Url>,
        passthrough_columns: Vec<ColumnName>,
        file_meta: ScanFileColumns,
        dv_ref: Option<DvRef>,
    ) -> Result<RelationHandle, DeltaError> {
        let scan_schema = scan_node.schema_ref();
        let mut output_fields: Vec<_> = file_schema.fields().cloned().collect();
        for col in &passthrough_columns {
            let Some(leaf_name) = col.path().first() else {
                return Err(crate::delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "add_load passthrough column cannot have an empty path",
                ));
            };
            let field = scan_schema.field(leaf_name.as_str()).cloned().ok_or_else(|| {
                crate::delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "add_load passthrough column `{}` not found in scan schema",
                    leaf_name
                )
            })?;
            output_fields.push(field);
        }
        let output_schema = arc_schema(output_fields);
        let handle = RelationHandle::fresh(name, output_schema);
        let mut sink = LoadSink::new(handle.clone(), file_schema, file_type)
            .with_passthrough_columns(passthrough_columns)
            .with_file_meta(file_meta);
        if let Some(base_url) = base_url {
            sink = sink.with_base_url(base_url);
        }
        if let Some(dv_ref) = dv_ref {
            sink = sink.with_dv_ref(dv_ref);
        }
        let plan = scan_node.into_load(sink);
        self.plans.push(plan);
        Ok(handle)
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
        let added = p
            .add_relation(
                "derived",
                relation_ref(&h).project(
                    vec![col("id").into()],
                    arc_schema([StructField::not_null("id", DataType::LONG)]),
                ),
            )
            .unwrap();
        assert_eq!(added.name, "derived");
        assert_eq!(added.schema.fields().count(), 1);
        let plans = p.into_vec();
        assert_eq!(plans.len(), 1);
        match &plans[0].sink {
            SinkType::Relation(got) => assert_eq!(got, &added),
            other => panic!("expected Relation sink, got {other:?}"),
        }
    }

    #[test]
    fn scan_publishes_scan_schema() {
        let read_schema = arc_schema([StructField::not_null("v", DataType::LONG)]);
        let mut p = PlanCollector::new();
        let h = p
            .add_relation(
                "scanned",
                PlanBuilder::scan_json(vec![], read_schema.clone()),
            )
            .unwrap();
        assert_eq!(h.schema, read_schema);
        let plans = p.into_vec();
        assert!(matches!(plans[0].root, DeclarativePlanNode::Scan(_)));
    }

    #[test]
    fn add_load_emits_load_sink_against_minted_handle() {
        let scan_schema = arc_schema([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
        ]);
        let file_schema = arc_schema([StructField::nullable("payload", DataType::STRING)]);
        let base = Url::parse("memory:///log/").unwrap();
        let mut p = PlanCollector::new();
        let h = p
            .add_load(
                "raw",
                PlanBuilder::scan_json(vec![], scan_schema),
                file_schema.clone(),
                FileType::Json,
                Some(base),
                vec![],
                ScanFileColumns {
                    path: ColumnName::new(["path"]),
                    size: Some(ColumnName::new(["size"])),
                    record_count: None,
                },
                None,
            )
            .unwrap();
        let plans = p.into_vec();
        match &plans[0].sink {
            SinkType::Load(ls) => {
                assert_eq!(ls.output_relation, h);
                assert_eq!(ls.file_schema, file_schema);
            }
            other => panic!("expected Load sink, got {other:?}"),
        }
    }

    #[test]
    fn add_load_rejects_unknown_passthrough_column() {
        let scan_schema = arc_schema([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
        ]);
        let file_schema = arc_schema([StructField::nullable("payload", DataType::STRING)]);
        let mut p = PlanCollector::new();
        let err = p
            .add_load(
                "raw",
                PlanBuilder::scan_json(vec![], scan_schema),
                file_schema,
                FileType::Json,
                None,
                vec![ColumnName::new(["version"])],
                ScanFileColumns {
                    path: ColumnName::new(["path"]),
                    size: Some(ColumnName::new(["size"])),
                    record_count: None,
                },
                None,
            )
            .unwrap_err();
        assert!(err
            .message
            .contains("add_load passthrough column `version` not found in scan schema"));
    }
}
