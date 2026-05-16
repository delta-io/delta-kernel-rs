//! Sink IR types — relation piping, [`ConsumeByKdf`], and file-reader [`LoadSink`].
//!
//! Also hosts [`LoadSpec`], a plain-data builder for [`LoadSink`] minus the
//! output-relation handle (the handle is minted by
//! [`PlanCollector::add_load`](crate::plans::ir::plan::PlanCollector::add_load)).

use url::Url;

use super::{FileFormat, FileType, OrderingSpec};
use crate::expressions::ColumnName;
use crate::plans::ir::declarative::DeclarativePlanNode;
use crate::plans::kdf::{ConsumerKdf, Handle, KdfStateToken, TraceContext};
use crate::schema::{arc_schema, DataType, SchemaRef, StructField};
use crate::FileMeta;

/// Template for draining a row stream into a [`ConsumerKdf`] via
/// [`SinkType::ConsumeByKdf`]. KDF lives exclusively under sinks in the IR.
///
/// - `initial_state`: cloned per partition via [`DynClone`](dyn_clone::DynClone) into a
///   [`Handle`](crate::plans::kdf::Handle).
/// - `requires_ordering`: stamped from [`ConsumerKdf::required_ordering`] at construction.
/// - `token`: joins finalized state back to the phase's `PhaseState`.
pub struct ConsumeByKdfSink {
    pub initial_state: Box<dyn ConsumerKdf>,
    pub requires_ordering: Option<OrderingSpec>,
    pub token: KdfStateToken,
}

impl ConsumeByKdfSink {
    /// Construct from a concrete consumer. Mints a fresh token from the KDF's
    /// `kdf_id` and reads ordering from [`ConsumerKdf::required_ordering`].
    pub fn new_consumer<C: ConsumerKdf + 'static>(state: C) -> Self {
        let requires_ordering = state.required_ordering();
        let token = KdfStateToken::new(state.kdf_id());
        Self {
            initial_state: Box::new(state),
            requires_ordering,
            token,
        }
    }

    /// Mint a per-partition runtime [`Handle`] for this sink template.
    pub fn new_handle(&self, ctx: TraceContext, partition: u32) -> Handle<dyn ConsumerKdf> {
        Handle::new(
            self.token.clone(),
            ctx,
            partition,
            self.initial_state.clone(),
        )
    }
}

impl Clone for ConsumeByKdfSink {
    fn clone(&self) -> Self {
        Self {
            initial_state: self.initial_state.clone(),
            requires_ordering: self.requires_ordering.clone(),
            token: self.token.clone(),
        }
    }
}

impl std::fmt::Debug for ConsumeByKdfSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumeByKdfSink")
            .field("kdf_id", &self.initial_state.kdf_id())
            .field("requires_ordering", &self.requires_ordering)
            .field("token", &self.token)
            .finish()
    }
}

// Token identity drives equality: tokens are process-unique by serial, and the
// `initial_state` trait object (`Box<dyn ConsumerKdf>`) is not `Eq`-able. Two
// sinks sharing a token were constructed from the same plan node and therefore
// describe the same consumer.
impl PartialEq for ConsumeByKdfSink {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl Eq for ConsumeByKdfSink {}

/// Identifier for a relation produced by one plan and consumed by another in
/// the same `PhaseOperation::Plans(...)`. Created via [`RelationHandle::fresh`];
/// each handle is unique across all kernel plans for the lifetime of the
/// process (id-based comparison).
///
/// Handles connect a [`SinkType::Relation`] in one plan to a
/// [`crate::plans::ir::declarative::DeclarativePlanNode::RelationRef`] leaf in another.
/// The executor allocates a bounded channel per handle, the producing plan's
/// sink writes to it, and the consuming plan's source reads from it —
/// streaming end-to-end, not materialized.
///
/// The handle also carries the producing plan's output [`SchemaRef`] so that
/// consuming sources (`RelationRef` / `HashJoin` / `Union`) can publish
/// a static output schema during pipeline construction; that unblocks
/// operators like `FilterByExpression` and `Select` that need an input
/// schema to build their evaluators. Schema is metadata, not identity:
/// equality and hashing key on `id` only.
#[derive(Debug, Clone)]
pub struct RelationHandle {
    /// Diagnostic name (used in tracing spans, error messages); not part of
    /// equality / hashing.
    pub name: String,
    /// Process-wide monotonic id. Drives equality and hashing so that two
    /// freshly-minted handles with the same name are still distinct.
    pub id: u64,
    /// Output schema of the producing plan. Carried alongside the handle so
    /// that consuming sources publish a static schema to downstream
    /// operators without an external lookup. Not part of equality / hashing.
    pub schema: SchemaRef,
}

impl RelationHandle {
    /// Mint a fresh handle with the given diagnostic name and producer
    /// output schema. Distinct ids regardless of `name` collisions, so names
    /// are diagnostic-only — relations are identified by id.
    pub fn fresh(name: impl Into<String>, schema: SchemaRef) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self {
            name: name.into(),
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            schema,
        }
    }
}

// `id` alone drives equality and hashing — `name` is diagnostic and `schema`
// is metadata. The `fresh` constructor guarantees process-wide unique ids.
impl PartialEq for RelationHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for RelationHandle {}
impl std::hash::Hash for RelationHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Column-name hints that a [`LoadSink`] reads from each upstream row to
/// resolve which file to open. The `path` column is mandatory; `size` and
/// `record_count` are advisory and used by engines for split-sizing /
/// pruning decisions.
///
/// Names are [`ColumnName`]s so they may reference nested fields (e.g.
/// `add.path` on a Delta-checkpoint upstream).
///
/// Spec: `declarative_plan_docs/algebra/plan_nodes.md` §7.3
/// (`ScanFileColumns`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanFileColumns {
    /// Column on the upstream relation holding the per-row file path /
    /// URL fragment. Joined to [`LoadSink::base_url`] when set.
    pub path: ColumnName,
    /// Optional column with the file's total size in bytes.
    pub size: Option<ColumnName>,
    /// Optional column with the file's row-count (parquet-encoded `numRecords`).
    pub record_count: Option<ColumnName>,
}

/// Deletion-vector reference attached to a [`LoadSink`]. Rows present in the
/// referenced DV are skipped from the file read.
///
/// `column` is a [`crate::actions::deletion_vector::DeletionVectorDescriptor`]
/// struct column on the upstream relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DvRef {
    pub column: ColumnName,
}

impl DvRef {
    /// Build a skip-style DV reference from a descriptor column.
    pub fn skip(column: ColumnName) -> Self {
        Self { column }
    }

    /// Return the DV column reference.
    pub fn primary_column(&self) -> &ColumnName {
        &self.column
    }
}

/// File-reader sink. For each upstream row, opens the resolved file in
/// [`Self::file_type`], reads [`Self::file_schema`] columns, and broadcasts the row's
/// [`Self::passthrough_columns`] alongside each emitted file row.
///
/// The materialized result is named via [`Self::output_relation`] so that
/// downstream plans in the same phase can consume it through
/// [`crate::plans::ir::declarative::DeclarativePlanNode::relation_ref`].
///
/// An optional [`Self::dv_ref`] column hint enables per-row deletion-vector
/// masking against a descriptor column on the upstream relation.
///
/// Spec: `declarative_plan_docs/algebra/plan_nodes.md` §5
/// (`Sink: Load (LoadSinkNode)`).
#[derive(Debug, Clone)]
pub struct LoadSink {
    /// Where Load's output is materialized. Downstream plans reference this
    /// handle via [`crate::plans::ir::declarative::DeclarativePlanNode::relation_ref`].
    pub output_relation: RelationHandle,
    /// Desired per-file output columns (nullability follows
    /// [`crate::ParquetHandler::read_parquet_files`] semantics).
    pub file_schema: SchemaRef,
    /// Optional URL prefix joined with each per-row path. When `None`, the
    /// path column is treated as an absolute URL.
    pub base_url: Option<url::Url>,
    /// Column-name hints used to read per-row file metadata from upstream.
    pub file_meta: ScanFileColumns,
    /// Optional upstream deletion-vector policy. When `None`, IR does not request
    /// DV-based masking in load.
    pub dv_ref: Option<DvRef>,
    /// Names of upstream columns to broadcast verbatim onto every output row
    /// (e.g. `add.path` for downstream joins back to the manifest).
    pub passthrough_columns: Vec<ColumnName>,
    /// Read format (`Parquet` or `Json`).
    pub file_type: FileType,
}

impl PartialEq for LoadSink {
    fn eq(&self, other: &Self) -> bool {
        // Identity follows the materialized relation handle (process-wide
        // unique). Two sinks that target the same handle are the same sink
        // for plan-equality purposes.
        self.output_relation == other.output_relation
    }
}

impl Eq for LoadSink {}

/// What the engine does with the terminal row stream.
///
/// Sink shapes include: `Results` (stream batches to the caller), `Relation`
/// (pipe into another plan), `ConsumeByKdf` (drain into a [`ConsumerKdf`]),
/// and `Load` (per-row file read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkType {
    /// Stream every output batch to the caller.
    ///
    /// `None` means "infer from root output schema". Newer plan builders should
    /// prefer setting `Some(schema)` explicitly.
    Results(Option<SchemaRef>),
    /// Stream every output batch to the named [`RelationHandle`]. Another
    /// plan in the same phase consumes via
    /// [`crate::plans::ir::declarative::DeclarativePlanNode::relation_ref`].
    Relation(RelationHandle),
    /// Drain every output batch through the wrapped consumer KDF. The KDF's
    /// finalized per-partition state is harvested by the engine into the
    /// phase's `PhaseState` (and recovered by the typed
    /// [`Extractor`](crate::plans::ir::declarative::Extractor) that yields
    /// `O = ConsumerKdf::Output`).
    ConsumeByKdf(ConsumeByKdfSink),
    /// File-reader sink — for each upstream row, read a file and materialize
    /// the result under [`LoadSink::output_relation`]. See [`LoadSink`].
    Load(LoadSink),
}

/// Terminal node of a [`crate::plans::ir::plan::Plan`], carrying a [`SinkType`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkNode {
    pub sink_type: SinkType,
}

// ============================================================================
// LoadSpec — builder for LoadSink (output relation minted by PlanCollector)
// ============================================================================

/// Plain data describing a per-file [`LoadSink`] except for the output relation
/// (minted by [`PlanCollector::add_load`](crate::plans::ir::plan::PlanCollector::add_load)).
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
    /// Optional upstream deletion-vector policy. When `None`, IR does not
    /// request DV-based masking in load.
    pub dv_ref: Option<DvRef>,
}

impl LoadSpec {
    /// Build a [`LoadSpec`] for JSON files.
    ///
    /// `passthrough` lists top-level column names on `scan_schema` to broadcast
    /// onto each emitted row. Names that resolve against `scan_schema` keep their
    /// original type and nullability on the materialized relation; names that do
    /// not resolve fall back to nullable STRING.
    pub fn json(
        files: Vec<FileMeta>,
        scan_schema: SchemaRef,
        file_schema: SchemaRef,
        base_url: Url,
        passthrough: &[&str],
    ) -> Self {
        Self::for_format(
            FileType::Json,
            files,
            scan_schema,
            file_schema,
            base_url,
            passthrough,
        )
    }

    /// Build a [`LoadSpec`] for Parquet files. See [`LoadSpec::json`] for
    /// `passthrough` semantics.
    pub fn parquet(
        files: Vec<FileMeta>,
        scan_schema: SchemaRef,
        file_schema: SchemaRef,
        base_url: Url,
        passthrough: &[&str],
    ) -> Self {
        Self::for_format(
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
    pub fn for_format(
        format: FileFormat,
        files: Vec<FileMeta>,
        scan_schema: SchemaRef,
        file_schema: SchemaRef,
        base_url: Url,
        passthrough: &[&str],
    ) -> Self {
        let passthrough_columns: Vec<ColumnName> =
            passthrough.iter().map(|s| ColumnName::new([*s])).collect();
        let scan_node = DeclarativePlanNode::scan(format, files, scan_schema);
        Self {
            scan_node,
            file_schema,
            base_url,
            passthrough_columns,
            file_meta: default_scan_file_columns(),
            file_type: format,
            dv_ref: None,
        }
    }

    /// Attach a deletion-vector reference to the resulting [`LoadSink`].
    pub fn with_dv_ref(mut self, dv_ref: DvRef) -> Self {
        self.dv_ref = Some(dv_ref);
        self
    }

    /// Materialized output schema for this spec: the file schema fields
    /// followed by passthrough fields resolved against the scan's published
    /// schema. Passthrough names that don't resolve fall back to nullable
    /// STRING so the relation still publishes a usable shape.
    pub(crate) fn compute_output_schema(&self) -> SchemaRef {
        let scan_schema = self.scan_node.output_schema();
        let mut fields: Vec<StructField> = self.file_schema.fields().cloned().collect();
        for col in &self.passthrough_columns {
            let leaf_name = col.path().first().cloned().unwrap_or_default();
            let field = scan_schema
                .field(leaf_name.as_str())
                .cloned()
                .unwrap_or_else(|| StructField::nullable(leaf_name, DataType::STRING));
            fields.push(field);
        }
        arc_schema(fields)
    }
}

/// Default file-meta column hints used by [`LoadSpec::for_format`] /
/// [`LoadSpec::json`] / [`LoadSpec::parquet`]. Matches the convention used
/// across FSR plan builders.
fn default_scan_file_columns() -> ScanFileColumns {
    ScanFileColumns {
        path: ColumnName::new(["path"]),
        size: Some(ColumnName::new(["size"])),
        record_count: None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{arc_schema, DataType, StructField};

    #[test]
    fn load_passthrough_widens_relation_schema() {
        let scan_schema = arc_schema([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("version", DataType::LONG),
        ]);
        let file_schema = arc_schema([StructField::nullable("payload", DataType::STRING)]);
        let base = Url::parse("memory:///log/").unwrap();
        let spec = LoadSpec::json(vec![], scan_schema, file_schema, base, &["version"]);
        let output_schema = spec.compute_output_schema();
        let names: Vec<String> = output_schema.fields().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["payload", "version"]);
    }
}
