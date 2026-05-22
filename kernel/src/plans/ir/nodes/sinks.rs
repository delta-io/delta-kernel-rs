//! Helper IR types referenced by the SSA IR's `Node::Load` and `Step::Consume`, and by the
//! engine's compile path (file-reader sink, deletion-vector hints, consumer drain template).

use url::Url;
use uuid::Uuid;

use super::FileType;
use crate::expressions::ColumnName;
use crate::plans::kernel_consumers::{Handle, KernelConsumer, KernelConsumerToken};
use crate::schema::SchemaRef;

/// Template for draining a row stream into a [`KernelConsumer`] via
/// [`Step::Consume`](crate::plans::operations::framework::step::Step::Consume).
///
/// - `initial_state`: cloned per partition via [`DynClone`](dyn_clone::DynClone) into a
///   [`Handle`](crate::plans::kernel_consumers::Handle).
/// - `token`: joins finalized state back to the phase's `StepResult`.
#[derive(Debug, Clone)]
pub struct ConsumeSink {
    pub initial_state: Box<dyn KernelConsumer>,
    pub token: KernelConsumerToken,
}

impl ConsumeSink {
    /// Construct from a concrete consumer and mint a fresh token from its `kind`.
    pub fn new_consumer<C: KernelConsumer + 'static>(state: C) -> Self {
        let token = KernelConsumerToken::new(state.kind());
        Self {
            initial_state: Box::new(state),
            token,
        }
    }

    /// Mint a runtime [`Handle`] for this sink template, stamped with the owning state machine's
    /// identity tuple.
    pub fn new_handle(
        &self,
        sm_id: Uuid,
        sm_kind: &'static str,
        step_name: &'static str,
    ) -> Handle<dyn KernelConsumer> {
        Handle::new(
            self.token.clone(),
            sm_id,
            sm_kind,
            step_name,
            self.initial_state.clone(),
        )
    }
}

// Token identity drives equality: tokens are process-unique by id, and the
// `initial_state` trait object (`Box<dyn KernelConsumer>`) is not `Eq`-able. Two
// sinks sharing a token were constructed from the same plan node and therefore
// describe the same consumer.
impl PartialEq for ConsumeSink {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl Eq for ConsumeSink {}

/// Diagnostic + arrow-output handle synthesized by the engine when lowering an SSA
/// `Node::Load`. Carries a unique id, a diagnostic name, and the kernel-typed output schema
/// the load materializes; the engine's `LoadTableProvider` reads `schema` to publish its
/// arrow output. Equality / hashing key on `id` only — `name` and `schema` are metadata.
#[derive(Debug, Clone)]
pub struct RelationHandle {
    /// Diagnostic name (used in tracing spans, error messages); not part of
    /// equality / hashing.
    pub name: String,
    /// UUID identifier. Drives equality and hashing so that two freshly-minted
    /// handles with the same name are still distinct.
    pub id: String,
    /// Output schema of the producing plan. Carried alongside the handle so
    /// that consuming sources publish a static schema to downstream
    /// operators without an external lookup. Not part of equality / hashing.
    pub schema: SchemaRef,
}

impl RelationHandle {
    /// Mint a fresh handle with the given diagnostic name and producer
    /// output schema. Distinct UUID ids regardless of `name` collisions, so names
    /// are diagnostic-only — relations are identified by id.
    pub fn fresh(name: impl Into<String>, schema: SchemaRef) -> Self {
        Self {
            name: name.into(),
            id: Uuid::new_v4().to_string(),
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
}

/// File-reader sink template consumed by the engine's `LoadTableProvider` when lowering an
/// SSA `Node::Load`. For each upstream row, the engine opens the resolved file in
/// [`Self::file_type`], reads [`Self::file_schema`] columns, and broadcasts the row's
/// [`Self::passthrough_columns`] alongside each emitted file row.
///
/// An optional [`Self::dv_ref`] column hint enables per-row deletion-vector masking against
/// a descriptor column on the upstream relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadSink {
    /// Synthetic handle the engine attaches so its `LoadTableProvider` can publish a stable
    /// arrow output schema (id-based identity, schema-only consumption).
    pub output_relation: RelationHandle,
    /// Desired per-file output columns (nullability follows
    /// [`crate::ParquetHandler::read_parquet_files`] semantics).
    pub file_schema: SchemaRef,
    /// Optional URL prefix joined with each per-row path. When `None`, the
    /// path column is treated as an absolute URL.
    pub base_url: Option<url::Url>,
    /// Column-name hints for per-row file metadata on the upstream relation.
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

impl LoadSink {
    /// Build a Load sink with default file-meta hints and no optional policies.
    pub fn new(
        output_relation: RelationHandle,
        file_schema: SchemaRef,
        file_type: FileType,
    ) -> Self {
        Self {
            output_relation,
            file_schema,
            base_url: None,
            file_meta: default_scan_file_columns(),
            dv_ref: None,
            passthrough_columns: vec![],
            file_type,
        }
    }

    /// Set the base URL joined with each per-row file path.
    pub fn with_base_url(mut self, base_url: Url) -> Self {
        self.base_url = Some(base_url);
        self
    }

    /// Set passthrough columns broadcast onto each emitted file row.
    pub fn with_passthrough_columns(mut self, passthrough_columns: Vec<ColumnName>) -> Self {
        self.passthrough_columns = passthrough_columns;
        self
    }

    /// Set file-meta column hints read from each upstream row.
    pub fn with_file_meta(mut self, file_meta: ScanFileColumns) -> Self {
        self.file_meta = file_meta;
        self
    }

    /// Attach a deletion-vector reference.
    pub fn with_dv_ref(mut self, dv_ref: DvRef) -> Self {
        self.dv_ref = Some(dv_ref);
        self
    }
}

/// Default file-meta column hints: `path` + `size`.
fn default_scan_file_columns() -> ScanFileColumns {
    ScanFileColumns {
        path: ColumnName::new(["path"]),
        size: Some(ColumnName::new(["size"])),
        record_count: None,
    }
}
