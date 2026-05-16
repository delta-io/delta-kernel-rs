//! Plan-collector plus scan / load entry points for assembling Delta plan vectors.
//!
//! The transformation methods (`filter`, `project`, `add_column`, `drop_column`,
//! `drop_columns`, `window_dedup_by`, `window_row_number`, ...) live directly on
//! [`DeclarativePlanNode`]. This module supplies only:
//!
//! - [`relation_ref`] and [`scan_files`]: free-function entry points that return a bare
//!   [`DeclarativePlanNode`] so callers can chain transformations and hand the result to
//!   [`PlanCollector::add_relation`].
//! - [`LoadSpec`] plus [`load_json`] / [`load_parquet`] / [`load_files`]: a plain data struct
//!   describing a per-file read; [`PlanCollector::add_load`] mints the destination handle and
//!   assembles the [`LoadSink`].
//! - [`PlanCollector`]: accumulator that owns the fresh-handle minting policy and produces the
//!   `Vec<Plan>` the executor consumes.
//!
//! # Example
//!
//! ```ignore
//! use delta_kernel::expressions::col;
//! use delta_kernel::plans::ir::fluent::{load_json, relation_ref, PlanCollector};
//!
//! let mut p = PlanCollector::new();
//! let raw = p.add_load(
//!     "raw",
//!     load_json(commit_files, scan_schema.clone(), read_schema.clone(), log_root, &["version"]),
//! );
//! let _dedup = p.add_relation(
//!     "dedup",
//!     relation_ref(&raw)
//!         .filter(col("path").is_not_null())
//!         .drop_column("version"),
//! );
//! let plans = p.into_vec();
//! ```

use url::Url;

use super::declarative::DeclarativePlanNode;
use super::nodes::{FileFormat, FileType, LoadSink, RelationHandle, ScanFileColumns, SinkType};
use super::plan::Plan;
use crate::expressions::ColumnName;
use crate::schema::{arc_schema, DataType, SchemaRef, StructField};
use crate::FileMeta;

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

/// Start a chain from a fixed file list using `format`.
///
/// The returned node carries `scan_schema` as its output schema verbatim; use
/// [`PlanCollector::add_relation`] to bind the chain to a freshly minted
/// [`RelationHandle`], or [`PlanCollector::add_load`] (via [`LoadSpec`]) when
/// each scanned row should be expanded into a per-file Load.
pub fn scan_files(
    format: FileFormat,
    files: Vec<FileMeta>,
    scan_schema: SchemaRef,
) -> DeclarativePlanNode {
    DeclarativePlanNode::scan(format, files, scan_schema)
}

// ============================================================================
// LoadSpec and Load entry points
// ============================================================================

/// Plain data describing a per-file [`LoadSink`] except for the output relation
/// (minted by [`PlanCollector::add_load`]).
///
/// Holds the upstream scan node plus the file-level read schema, base URL, and
/// passthrough columns that the materialized Load will broadcast onto every
/// emitted row. The file-meta column hints default to the FSR convention
/// (`path: "path"`, `size: Some("size")`, `record_count: None`); callers that
/// need other hints can build a [`LoadSink`] directly and use
/// [`DeclarativePlanNode::into_load`].
pub struct LoadSpec {
    /// Upstream scan node producing one row per file to open.
    pub scan_node: DeclarativePlanNode,
    /// Per-file output schema read from each opened file.
    pub file_schema: SchemaRef,
    /// URL prefix joined with each per-row path on the scan.
    pub base_url: Url,
    /// Columns from the upstream relation broadcast alongside each file row.
    pub passthrough_columns: Vec<ColumnName>,
    /// Column-name hints used to read per-row file metadata from upstream.
    pub file_meta: ScanFileColumns,
    /// File format opened by the resulting [`LoadSink`].
    pub file_type: FileType,
}

/// Build a [`LoadSpec`] for JSON files.
///
/// `passthrough` lists top-level column names on `scan_schema` to broadcast
/// onto each emitted row. Names that resolve against `scan_schema` keep their
/// original type and nullability on the materialized relation; names that do
/// not resolve fall back to nullable STRING.
pub fn load_json(
    files: Vec<FileMeta>,
    scan_schema: SchemaRef,
    file_schema: SchemaRef,
    base_url: Url,
    passthrough: &[&str],
) -> LoadSpec {
    load_files(
        FileType::Json,
        files,
        scan_schema,
        file_schema,
        base_url,
        passthrough,
    )
}

/// Build a [`LoadSpec`] for Parquet files. See [`load_json`] for `passthrough`
/// semantics.
pub fn load_parquet(
    files: Vec<FileMeta>,
    scan_schema: SchemaRef,
    file_schema: SchemaRef,
    base_url: Url,
    passthrough: &[&str],
) -> LoadSpec {
    load_files(
        FileType::Parquet,
        files,
        scan_schema,
        file_schema,
        base_url,
        passthrough,
    )
}

/// Format-parametric [`LoadSpec`] constructor. Used by call sites that pick
/// the file format at runtime (for example FSR checkpoint shape selection).
pub fn load_files(
    format: FileFormat,
    files: Vec<FileMeta>,
    scan_schema: SchemaRef,
    file_schema: SchemaRef,
    base_url: Url,
    passthrough: &[&str],
) -> LoadSpec {
    let passthrough_columns: Vec<ColumnName> =
        passthrough.iter().map(|s| ColumnName::new([*s])).collect();
    let scan_node = DeclarativePlanNode::scan(format, files, scan_schema);
    LoadSpec {
        scan_node,
        file_schema,
        base_url,
        passthrough_columns,
        file_meta: default_scan_file_columns(),
        file_type: format,
    }
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
        let output_schema = compute_load_output_schema(&spec);
        let handle = RelationHandle::fresh(name, output_schema);
        let sink = LoadSink {
            output_relation: handle.clone(),
            file_schema: spec.file_schema,
            base_url: Some(spec.base_url),
            passthrough_columns: spec.passthrough_columns,
            file_meta: spec.file_meta,
            file_type: spec.file_type,
            dv_ref: None,
        };
        let plan = spec.scan_node.into_load(sink);
        self.plans.push(plan);
        handle
    }

    /// Consume the collector and return the accumulated plans in submission
    /// order.
    pub fn into_vec(self) -> Vec<Plan> {
        self.plans
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Default file-meta column hints used by [`load_files`] / [`load_json`] /
/// [`load_parquet`]. Matches the convention used across FSR plan builders.
fn default_scan_file_columns() -> ScanFileColumns {
    ScanFileColumns {
        path: ColumnName::new(["path"]),
        size: Some(ColumnName::new(["size"])),
        record_count: None,
    }
}

/// Compute the materialized output schema for a [`LoadSpec`]: the file schema
/// fields followed by the passthrough fields resolved against the scan's
/// published schema. Passthrough names that don't resolve fall back to
/// nullable STRING so the relation still publishes a usable shape.
fn compute_load_output_schema(spec: &LoadSpec) -> SchemaRef {
    let scan_schema = spec.scan_node.output_schema();
    let mut fields: Vec<StructField> = spec.file_schema.fields().cloned().collect();
    for col in &spec.passthrough_columns {
        let leaf_name = col.path().first().cloned().unwrap_or_default();
        let field = scan_schema
            .field(leaf_name.as_str())
            .cloned()
            .unwrap_or_else(|| StructField::nullable(leaf_name, DataType::STRING));
        fields.push(field);
    }
    arc_schema(fields)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::expressions::{col, ColumnName, Expression};
    use crate::plans::ir::nodes::{OrderingSpec, SinkType};
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
    fn relation_ref_initial_schema_matches_handle() {
        let h = fresh_handle("src");
        let node = relation_ref(&h);
        assert_eq!(node.output_schema(), h.schema);
    }

    #[test]
    fn add_column_appends_field_and_projects() {
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
    fn scan_files_publishes_scan_schema() {
        let read_schema = arc_schema([StructField::not_null("v", DataType::LONG)]);
        let mut p = PlanCollector::new();
        let h = p.add_relation(
            "scanned",
            scan_files(FileType::Json, vec![], read_schema.clone()),
        );
        assert_eq!(h.schema, read_schema);
        let plans = p.into_vec();
        assert!(matches!(plans[0].root, DeclarativePlanNode::Scan(_)));
    }

    #[test]
    fn load_passthrough_widens_relation_schema() {
        let scan_schema = arc_schema([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("version", DataType::LONG),
        ]);
        let file_schema = arc_schema([StructField::nullable("payload", DataType::STRING)]);
        let base = Url::parse("memory:///log/").unwrap();
        let spec = load_json(vec![], scan_schema, file_schema, base, &["version"]);
        let output_schema = compute_load_output_schema(&spec);
        let names: Vec<String> = output_schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["payload", "version"]);
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
        let h = p.add_load(
            "raw",
            load_json(vec![], scan_schema, file_schema.clone(), base, &[]),
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

    #[test]
    fn chained_pipeline_records_each_step_as_separate_ir_node() {
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
