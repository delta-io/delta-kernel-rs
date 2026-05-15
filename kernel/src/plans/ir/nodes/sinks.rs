//! Sink IR types — relation piping, [`ConsumeByKdf`], file-reader [`LoadSink`], and
//! file-writer sinks ([`WriteSink`], [`PartitionedWriteSink`]).

use super::{FileType, OrderingSpec};
use crate::expressions::ColumnName;
use crate::plans::kdf::{ConsumerKdf, Handle, KdfStateToken, TraceContext};
use crate::schema::SchemaRef;

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

/// On-disk encoding for [`WriteSink`] and [`PartitionedWriteSink`].
///
/// This is intentionally separate from read-side [`super::FileType`]: scans
/// use `Json` for newline-delimited JSON, while writers name the format
/// `JsonLines` to match the declarative-plan spec vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteFileFormat {
    Parquet,
    /// One JSON object per line (newline-delimited / NDJSON).
    JsonLines,
}

/// Non-partitioned file write sink: the engine materializes the upstream row
/// stream to a single destination (interpretation of `destination` — file vs
/// directory prefix — is executor-specific).
///
/// DataFusion-backed executors interpret `format` and `destination` when
/// lowering the plan; the default in-process executor does not support this sink.
///
/// # Equality
///
/// [`PartialEq`] compares `destination` and `format` structurally. This differs from
/// [`LoadSink`], which keys equality on the process-unique [`RelationHandle`]
/// because Load materializes into a named relation. [`PartitionedWriteSink`]
/// applies the same structural rule including its partition-key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteSink {
    /// Output location (e.g. `file://` / `s3://` URL). Exact file-naming
    /// semantics are left to the engine.
    pub destination: url::Url,
    pub format: WriteFileFormat,
}

impl WriteSink {
    /// Construct a write sink with the given destination and format.
    pub fn new(destination: url::Url, format: WriteFileFormat) -> Self {
        Self {
            destination,
            format,
        }
    }

    /// Shorthand for [`WriteFileFormat::Parquet`].
    pub fn parquet(destination: url::Url) -> Self {
        Self::new(destination, WriteFileFormat::Parquet)
    }

    /// Shorthand for [`WriteFileFormat::JsonLines`].
    pub fn json_lines(destination: url::Url) -> Self {
        Self::new(destination, WriteFileFormat::JsonLines)
    }
}

/// Partitioned file write sink (Hive-style `col=value/` layout under a base URL).
///
/// The engine groups upstream rows by [`Self::partition_columns`] and emits files
/// beneath nested directories rooted at [`Self::destination`]. Exact naming,
/// commit semantics, and overwrite behavior are executor-defined.
///
/// Intended for DataFusion (or similar) lowering; the default in-process
/// executor does not support this sink.
///
/// # Equality
///
/// [`PartialEq`] compares `destination`, `format`, and `partition_columns`
/// structurally (including column order). Same rationale as [`WriteSink`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedWriteSink {
    /// Base directory / table prefix for partition subdirectories.
    pub destination: url::Url,
    pub format: WriteFileFormat,
    /// Ordered Hive partition key names; must match columns on the upstream row.
    pub partition_columns: Vec<String>,
}

impl PartitionedWriteSink {
    pub fn new(
        destination: url::Url,
        format: WriteFileFormat,
        partition_columns: Vec<String>,
    ) -> Self {
        Self {
            destination,
            format,
            partition_columns,
        }
    }

    pub fn parquet(destination: url::Url, partition_columns: Vec<String>) -> Self {
        Self::new(destination, WriteFileFormat::Parquet, partition_columns)
    }

    pub fn json_lines(destination: url::Url, partition_columns: Vec<String>) -> Self {
        Self::new(destination, WriteFileFormat::JsonLines, partition_columns)
    }
}

/// What the engine does with the terminal row stream.
///
/// Sink shapes include: `Results` (stream batches to the caller), `Relation`
/// (pipe into another plan), `ConsumeByKdf` (drain into a [`ConsumerKdf`]),
/// `Load` (per-row file read), `Write` (single-target file write), and
/// `PartitionedWrite` (Hive-style partitions under a base URL).
#[derive(Debug, Clone)]
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
    /// Stream rows to files at a single [`WriteSink::destination`]. See [`WriteSink`].
    Write(WriteSink),
    /// Partitioned writer — nested directories under [`PartitionedWriteSink::destination`].
    PartitionedWrite(PartitionedWriteSink),
}

impl PartialEq for SinkType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SinkType::Results(a), SinkType::Results(b)) => a == b,
            (SinkType::Relation(a), SinkType::Relation(b)) => a == b,
            (SinkType::ConsumeByKdf(a), SinkType::ConsumeByKdf(b)) => a.token == b.token,
            (SinkType::Load(a), SinkType::Load(b)) => a == b,
            (SinkType::Write(a), SinkType::Write(b)) => a == b,
            (SinkType::PartitionedWrite(a), SinkType::PartitionedWrite(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for SinkType {}

/// Terminal node of a [`crate::plans::ir::plan::Plan`], carrying a [`SinkType`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkNode {
    pub sink_type: SinkType,
}
