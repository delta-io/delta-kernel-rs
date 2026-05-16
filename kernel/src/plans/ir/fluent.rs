//! Fluent combinators for assembling Delta-protocol-flavored plan trees.
//!
//! The combinators in this module wrap the lower-level [`DeclarativePlanNode`]
//! constructors so plan builders read top-down as a sequence of relational
//! transforms rather than nested `Arc::new(...)` calls. The fluent layer is a
//! thin wrapper: every method materializes into existing IR nodes immediately,
//! so the engine compilers continue to see the same shapes they always have.
//!
//! # Entry points
//!
//! - [`relation_ref`] -- start a chain from an upstream [`RelationHandle`].
//! - [`scan_json`] / [`scan_parquet`] / [`scan_files`] -- start a chain from a file list,
//!   terminating in either a [`LoadSink`] (via [`ScanLoadBuilder::into_load`]) or directly bound to
//!   a relation handle (via [`ScanLoadBuilder::into_relation`] / [`scan_into_relation`]).
//! - [`PlanCollector`] -- accumulator that mints a fresh [`RelationHandle`] from each terminal
//!   builder's `output_schema()` and pushes the resulting [`Plan`] onto an output vector.
//!
//! # Example
//!
//! ```ignore
//! use delta_kernel::expressions::col;
//! use delta_kernel::plans::ir::fluent::{
//!     relation_ref, scan_json, PlanCollector, TerminalBuilder,
//! };
//!
//! let mut p = PlanCollector::new();
//! let raw = p.add(
//!     "raw",
//!     scan_json(commit_files, scan_schema.clone())
//!         .passthrough(["version"])
//!         .into_load(read_schema.clone(), log_root.clone()),
//! );
//! let dedup = p.add(
//!     "dedup",
//!     relation_ref(&raw)
//!         .filter(col("path").is_not_null())
//!         .drop("version"),
//! );
//! let plans = p.into_vec();
//! ```
//!
//! # Schema tracking
//!
//! Each builder owns the schema of the row stream it currently represents.
//! `add_column` / `drop` / window helpers update that schema as they go, and
//! [`PlanCollector::add`] reads it back via [`TerminalBuilder::output_schema`]
//! to mint a [`RelationHandle`]. This removes the per-builder duplication
//! between `RelationHandle::fresh(name, schema)` and the schema repeated in
//! the producing sink.

use std::sync::Arc;

use url::Url;

use super::declarative::DeclarativePlanNode;
use super::nodes::{
    FileFormat, FileType, LoadSink, OrderingSpec, RelationHandle, ScanFileColumns, SinkType,
};
use super::plan::Plan;
use crate::expressions::{ColumnName, Expression, Predicate};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error, FileMeta};

// ============================================================================
// TerminalBuilder
// ============================================================================

/// A fluent chain that knows how to (a) report the schema of the row stream
/// it terminates and (b) materialize itself into a [`Plan`] given a
/// [`RelationHandle`] that names the resulting relation.
///
/// Used by [`PlanCollector::add`] to mint a handle from the chain's schema
/// and then bind the chain's output to that handle.
pub trait TerminalBuilder {
    /// Schema of the row stream produced by this chain. Used to mint the
    /// destination [`RelationHandle`] before [`Self::into_plan`] is invoked.
    fn output_schema(&self) -> SchemaRef;

    /// Materialize this chain into a [`Plan`] whose terminal sink targets
    /// `handle`. Called by [`PlanCollector::add`] after `handle` has been
    /// minted from [`Self::output_schema`].
    fn into_plan(self, handle: &RelationHandle) -> Plan;
}

// ============================================================================
// PlanCollector
// ============================================================================

/// Accumulator that turns a sequence of [`TerminalBuilder`] chains into a
/// `Vec<Plan>` suitable for a single `PhaseOperation::Plans(...)`.
///
/// For each chain, [`Self::add`] queries the chain's `output_schema`, mints a
/// fresh [`RelationHandle`] with that schema, then calls the chain's
/// `into_plan` to bind it. The returned handle is what downstream chains pass
/// to [`relation_ref`].
///
/// ```ignore
/// let mut p = PlanCollector::new();
/// let h = p.add("loaded", scan_json(files, schema).passthrough(["v"]).into_load(file_schema, base));
/// let _ = p.add("filtered", relation_ref(&h).filter(col("v").is_not_null()));
/// let plans = p.into_vec();
/// ```
#[derive(Default)]
pub struct PlanCollector {
    plans: Vec<Plan>,
}

impl PlanCollector {
    /// Empty collector. Equivalent to `PlanCollector::default()`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint a fresh handle from `builder.output_schema()`, bind the builder's
    /// chain to it via `builder.into_plan(&handle)`, push the resulting plan
    /// onto the internal vector, and return the handle for use by downstream
    /// chains.
    pub fn add<B: TerminalBuilder>(&mut self, name: &str, builder: B) -> RelationHandle {
        let schema = builder.output_schema();
        let handle = RelationHandle::fresh(name, schema);
        let plan = builder.into_plan(&handle);
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
// RelationRefBuilder
// ============================================================================

/// Fluent builder that wraps a [`DeclarativePlanNode`] sourced from a
/// [`RelationHandle`] reference, accumulating transforms eagerly.
///
/// Every method materializes immediately into the underlying IR (rather than
/// deferring as a pending Transform) so the engine sees the same
/// `Project`/`Filter`/`Window` shapes it would from a hand-rolled builder.
/// The schema is tracked through each step so [`TerminalBuilder::output_schema`]
/// can answer without re-walking the tree.
#[derive(Debug)]
pub struct RelationRefBuilder {
    node: DeclarativePlanNode,
    schema: SchemaRef,
}

impl RelationRefBuilder {
    fn new(node: DeclarativePlanNode, schema: SchemaRef) -> Self {
        Self { node, schema }
    }

    /// Schema of the row stream as-of the current chain position.
    pub fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Append a computed column to the relation.
    ///
    /// The new column's [`StructField`] carries name, type, and nullability;
    /// the expression must be evaluable against the current schema and produce
    /// a value assignable to `field.data_type()`. Existing columns flow through
    /// unchanged (kernel-side: a `Project` that lists every prior column as
    /// `Expression::column([name])` followed by `expr`).
    ///
    /// ```ignore
    /// let augmented = relation_ref(&raw)
    ///     .add_column(StructField::nullable("k", DataType::STRING), col("path"));
    /// ```
    pub fn add_column<E: Into<Arc<Expression>>>(self, field: StructField, expr: E) -> Self {
        let expr = expr.into();
        let mut projection: Vec<Arc<Expression>> = self
            .schema
            .fields()
            .map(|f| Arc::new(Expression::column([f.name().as_str()])))
            .collect();
        projection.push(expr);

        let mut new_fields: Vec<StructField> = self.schema.fields().cloned().collect();
        new_fields.push(field);
        let new_schema = Arc::new(StructType::new_unchecked(new_fields));

        let node = self.node.project(projection, new_schema.clone());
        Self::new(node, new_schema)
    }

    /// Drop a single column.
    ///
    /// Equivalent to `Project(all_columns_except(name))`. If the column is not
    /// present in the current schema, the call is a no-op and the chain is
    /// returned unchanged.
    pub fn drop(self, name: &str) -> Self {
        self.drop_columns([name])
    }

    /// Drop multiple columns in one step.
    ///
    /// Names that are not present in the current schema are silently skipped
    /// (matching the no-op semantics of [`Self::drop`]). When every requested
    /// name is absent the chain is returned unchanged, with no `Project`
    /// emitted.
    pub fn drop_columns<I, S>(self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let dropped: std::collections::HashSet<String> =
            names.into_iter().map(|s| s.as_ref().to_string()).collect();
        let total = self.schema.fields().count();
        let kept_fields: Vec<StructField> = self
            .schema
            .fields()
            .filter(|f| !dropped.contains(f.name().as_str()))
            .cloned()
            .collect();
        if kept_fields.len() == total {
            return self;
        }
        let projection: Vec<Arc<Expression>> = kept_fields
            .iter()
            .map(|f| Arc::new(Expression::column([f.name().as_str()])))
            .collect();
        let new_schema = Arc::new(StructType::new_unchecked(kept_fields));
        let node = self.node.project(projection, new_schema.clone());
        Self::new(node, new_schema)
    }

    /// Keep rows for which `predicate` evaluates to `true`. Anything
    /// convertible to `Arc<Predicate>` is accepted -- a bare [`Predicate`]
    /// auto-wraps via the blanket `From<T> for Arc<T>` implementation, so
    /// `col(x).is_not_null()` flows in without explicit `Arc::new(...)`.
    pub fn filter<P: Into<Arc<Predicate>>>(self, predicate: P) -> Self {
        let pred_arc: Arc<Predicate> = predicate.into();
        let expr_arc: Arc<Expression> =
            Arc::new(Expression::from_pred(Arc::unwrap_or_clone(pred_arc)));
        let node = self.node.filter(expr_arc);
        Self::new(node, self.schema)
    }

    /// Keep only the newest row per `partition_by` group, ordered by
    /// `order_by`. Internally appends a `row_number()` window, filters
    /// `rn <= 1`, then drops the synthetic row-number column so callers never
    /// see it.
    ///
    /// Returns `Err` when `order_by` is empty (the IR layer requires a
    /// deterministic ordering for `row_number`) or when the reserved
    /// internal column name collides with an existing schema field.
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
        if self.schema.contains(RN_COL) {
            return Err(Error::generic(format!(
                "window_dedup_by: schema already contains reserved column `{RN_COL}`",
            )));
        }
        let partitions: Vec<Arc<Expression>> = partition_by.into_iter().map(Into::into).collect();
        let orderings: Vec<OrderingSpec> = order_by.into_iter().map(Into::into).collect();

        let windowed = self.node.window_row_number(RN_COL, partitions, orderings)?;
        let rn_le_one = Expression::column([RN_COL]).le(Expression::literal(1i64));
        let filtered = windowed.filter(Arc::new(rn_le_one.into()));

        // Drop the synthetic rn column via a final projection over the original schema --
        // preserves field order and nullability.
        let projection: Vec<Arc<Expression>> = self
            .schema
            .fields()
            .map(|f| Arc::new(Expression::column([f.name().as_str()])))
            .collect();
        let node = filtered.project(projection, self.schema.clone());
        Ok(Self::new(node, self.schema))
    }

    /// Terminate this chain in a [`SinkType::Results`] stream-to-caller plan.
    ///
    /// Most callers should hand the builder to [`PlanCollector::add`] instead,
    /// which terminates in a [`SinkType::Relation`] sink keyed by a freshly
    /// minted [`RelationHandle`]. This method is the streaming-output escape
    /// hatch when the chain is meant for direct consumption by the caller.
    pub fn into_results(self) -> Plan {
        self.node.into_results_with_schema(self.schema)
    }

    /// Consume the chain and return the underlying [`DeclarativePlanNode`].
    /// Use when a step needs to drop down to the lower-level IR (for example
    /// to feed a join or a union that the fluent layer does not yet wrap).
    pub fn into_node(self) -> DeclarativePlanNode {
        self.node
    }
}

impl TerminalBuilder for RelationRefBuilder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn into_plan(self, handle: &RelationHandle) -> Plan {
        // The handle's schema is what PlanCollector minted from our
        // `output_schema()`, so they always match.
        self.node.into_plan(SinkType::Relation(handle.clone()))
    }
}

/// Start a fluent chain from an upstream relation handle.
///
/// Clones the handle internally so the same `&RelationHandle` can feed
/// multiple chains without manual `.clone()` calls. Initial schema is taken
/// from the handle, matching the relation's published output schema.
pub fn relation_ref(handle: &RelationHandle) -> RelationRefBuilder {
    let schema = handle.schema.clone();
    RelationRefBuilder::new(DeclarativePlanNode::relation_ref(handle.clone()), schema)
}

// ============================================================================
// Scan / Load chains
// ============================================================================

/// Default file-meta column names used by [`ScanLoadBuilder`] when constructing
/// a [`LoadSink`]. Matches the convention used by FSR plan builders
/// (`path: "path"`, `size: Some("size")`, `record_count: None`).
fn default_scan_file_columns() -> ScanFileColumns {
    ScanFileColumns {
        path: ColumnName::new(["path"]),
        size: Some(ColumnName::new(["size"])),
        record_count: None,
    }
}

/// Fluent chain over a fixed file list. Most callers either:
///
/// - hand the result to [`ScanLoadBuilder::into_load`] to materialize a per-row file-reader sink
///   ([`LoadSink`]), or
/// - call [`ScanLoadBuilder::into_relation`] to bind the scan output directly to a relation handle
///   without an intervening Load step.
///
/// `passthrough` columns are broadcast from the scan onto every emitted row
/// when [`Self::into_load`] is used. They are ignored by [`Self::into_relation`].
pub struct ScanLoadBuilder {
    format: FileFormat,
    files: Vec<FileMeta>,
    scan_schema: SchemaRef,
    passthrough: Vec<ColumnName>,
}

impl ScanLoadBuilder {
    fn new(format: FileFormat, files: Vec<FileMeta>, scan_schema: SchemaRef) -> Self {
        Self {
            format,
            files,
            scan_schema,
            passthrough: Vec::new(),
        }
    }

    /// Add columns that the eventual [`LoadSink`] will broadcast onto every
    /// emitted row. Each entry must be a top-level column on the scan's
    /// schema (or a nested path expressible as [`ColumnName`]).
    pub fn passthrough<I, S>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.passthrough
            .extend(cols.into_iter().map(|s| ColumnName::new([s.as_ref()])));
        self
    }

    /// Terminate the scan in a [`LoadSink`] that opens each scanned row's
    /// file under `base_url` and reads `file_schema` from it. The materialized
    /// output schema is `file_schema ++ passthrough_columns`.
    ///
    /// File-meta column hints default to `path: "path"`, `size: Some("size")`,
    /// `record_count: None`. Callers that need a different shape can build a
    /// [`LoadSink`] by hand and use [`DeclarativePlanNode::into_load`] directly.
    pub fn into_load(self, file_schema: SchemaRef, base_url: Url) -> LoadTerminal {
        // Output relation schema = file_schema fields followed by the
        // passthrough fields (resolved against the scan schema when possible
        // so the broadcast columns keep their original types).
        let mut fields: Vec<StructField> = file_schema.fields().cloned().collect();
        for col in &self.passthrough {
            let field = col
                .path()
                .first()
                .and_then(|n| self.scan_schema.field(n.as_str()))
                .cloned()
                .unwrap_or_else(|| {
                    StructField::nullable(
                        col.path().last().cloned().unwrap_or_default(),
                        DataType::STRING,
                    )
                });
            fields.push(field);
        }
        let output_schema = Arc::new(StructType::new_unchecked(fields));

        LoadTerminal {
            scan_node: DeclarativePlanNode::scan(self.format, self.files, self.scan_schema),
            file_schema,
            base_url,
            passthrough_columns: self.passthrough,
            file_meta: default_scan_file_columns(),
            file_type: self.format,
            output_schema,
        }
    }

    /// Terminate the scan by binding its raw output directly to a relation
    /// (no per-row file read). Any `passthrough` columns set via
    /// [`Self::passthrough`] are ignored by this terminal -- they only apply
    /// to [`Self::into_load`].
    pub fn into_relation(self) -> ScanRelationTerminal {
        let output_schema = self.scan_schema.clone();
        ScanRelationTerminal {
            scan_node: DeclarativePlanNode::scan(self.format, self.files, self.scan_schema),
            output_schema,
        }
    }
}

/// JSON-formatted scan builder. See [`scan_files`] for the format-parametric
/// variant.
pub fn scan_json(files: Vec<FileMeta>, scan_schema: SchemaRef) -> ScanLoadBuilder {
    ScanLoadBuilder::new(FileType::Json, files, scan_schema)
}

/// Parquet-formatted scan builder.
pub fn scan_parquet(files: Vec<FileMeta>, scan_schema: SchemaRef) -> ScanLoadBuilder {
    ScanLoadBuilder::new(FileType::Parquet, files, scan_schema)
}

/// Format-parametric scan builder. Used by call sites that pick the file
/// format at runtime (for example FSR checkpoint shape selection).
pub fn scan_files(
    format: FileFormat,
    files: Vec<FileMeta>,
    scan_schema: SchemaRef,
) -> ScanLoadBuilder {
    ScanLoadBuilder::new(format, files, scan_schema)
}

/// Terminal builder for a scan that materializes its raw output into a
/// relation (no per-row file read). Use [`scan_into_relation`] or
/// [`ScanLoadBuilder::into_relation`] to construct one.
pub struct ScanRelationTerminal {
    scan_node: DeclarativePlanNode,
    output_schema: SchemaRef,
}

impl TerminalBuilder for ScanRelationTerminal {
    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn into_plan(self, handle: &RelationHandle) -> Plan {
        self.scan_node.into_plan(SinkType::Relation(handle.clone()))
    }
}

/// Convenience wrapper around [`ScanLoadBuilder::into_relation`] for call
/// sites that don't need a passthrough chain. The scan's output schema (as
/// passed) is published verbatim on the resulting relation.
pub fn scan_into_relation(
    format: FileFormat,
    files: Vec<FileMeta>,
    read_schema: SchemaRef,
) -> ScanRelationTerminal {
    ScanLoadBuilder::new(format, files, read_schema).into_relation()
}

/// Terminal builder for a scan that materializes file rows under a
/// [`LoadSink`]. Built by [`ScanLoadBuilder::into_load`].
pub struct LoadTerminal {
    scan_node: DeclarativePlanNode,
    file_schema: SchemaRef,
    base_url: Url,
    passthrough_columns: Vec<ColumnName>,
    file_meta: ScanFileColumns,
    file_type: FileType,
    output_schema: SchemaRef,
}

impl TerminalBuilder for LoadTerminal {
    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn into_plan(self, handle: &RelationHandle) -> Plan {
        let sink = LoadSink {
            output_relation: handle.clone(),
            file_schema: self.file_schema,
            base_url: Some(self.base_url),
            file_meta: self.file_meta,
            dv_ref: None,
            passthrough_columns: self.passthrough_columns,
            file_type: self.file_type,
        };
        self.scan_node.into_load(sink)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, ColumnName};
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
        let b = relation_ref(&h);
        assert_eq!(b.output_schema(), h.schema);
    }

    #[test]
    fn add_column_appends_field_and_projects() {
        let h = fresh_handle("src");
        let b =
            relation_ref(&h).add_column(StructField::nullable("k", DataType::STRING), col("name"));
        let schema = b.output_schema();
        let names: Vec<String> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["id", "name", "k"]);
        match b.into_node() {
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
        let dropped = relation_ref(&h).drop("name");
        let schema = dropped.output_schema();
        let names: Vec<String> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["id"]);

        // Dropping an absent column is a no-op (no Project emitted).
        let unchanged = relation_ref(&h).drop("nope");
        assert_eq!(unchanged.output_schema().fields().count(), 2);
        assert!(matches!(
            unchanged.into_node(),
            DeclarativePlanNode::RelationRef(_)
        ));
    }

    #[test]
    fn drop_columns_removes_multiple_fields() {
        let h = fresh_handle("src");
        let b = relation_ref(&h).drop_columns(["id", "name"]);
        assert_eq!(b.output_schema().fields().count(), 0);
    }

    #[test]
    fn filter_accepts_bare_predicate_and_wraps_in_filter_node() {
        let h = fresh_handle("src");
        let b = relation_ref(&h).filter(col("id").is_not_null());
        match b.into_node() {
            DeclarativePlanNode::Filter { .. } => {}
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[test]
    fn window_dedup_by_appends_window_filter_project_and_preserves_schema() {
        let h = fresh_handle("src");
        let b = relation_ref(&h)
            .window_dedup_by([col("id")], [OrderingSpec::desc(ColumnName::new(["id"]))])
            .unwrap();
        assert_eq!(b.output_schema(), h.schema);
        let DeclarativePlanNode::Project { child, node } = b.into_node() else {
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
    fn plan_collector_mints_handle_from_terminal_builder_schema() {
        let h = fresh_handle("src");
        let mut p = PlanCollector::new();
        let added = p.add("derived", relation_ref(&h).drop("name"));
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
    fn scan_into_relation_publishes_scan_schema() {
        let read_schema = arc_schema([StructField::not_null("v", DataType::LONG)]);
        let mut p = PlanCollector::new();
        let h = p.add(
            "scanned",
            scan_into_relation(FileType::Json, vec![], read_schema.clone()),
        );
        assert_eq!(h.schema, read_schema);
        let plans = p.into_vec();
        assert!(matches!(plans[0].root, DeclarativePlanNode::Scan(_)));
    }

    #[test]
    fn scan_load_passthrough_widens_relation_schema() {
        let scan_schema = arc_schema([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("version", DataType::LONG),
        ]);
        let file_schema = arc_schema([StructField::nullable("payload", DataType::STRING)]);
        let base = Url::parse("memory:///log/").unwrap();
        let terminal = scan_json(vec![], scan_schema)
            .passthrough(["version"])
            .into_load(file_schema, base);
        let schema = terminal.output_schema();
        let names: Vec<String> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["payload", "version"]);
    }

    #[test]
    fn scan_load_terminal_emits_load_sink_against_minted_handle() {
        let scan_schema = arc_schema([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
        ]);
        let file_schema = arc_schema([StructField::nullable("payload", DataType::STRING)]);
        let base = Url::parse("memory:///log/").unwrap();
        let mut p = PlanCollector::new();
        let h = p.add(
            "raw",
            scan_json(vec![], scan_schema)
                .passthrough(Vec::<&str>::new())
                .into_load(file_schema.clone(), base),
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
            .filter(col("k").is_not_null())
            .drop("k");
        let schema = pipeline.output_schema();
        let names: Vec<String> = schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["id", "name"]);
        let DeclarativePlanNode::Project { child, .. } = pipeline.into_node() else {
            panic!("outermost should be Project from drop");
        };
        let DeclarativePlanNode::Filter { child, .. } = *child else {
            panic!("middle should be Filter");
        };
        assert!(matches!(*child, DeclarativePlanNode::Project { .. }));
    }
}
