//! Per-variant payload structs referenced by [`NodeKind`](super::plan::NodeKind) and the
//! engine's compile path. Every payload struct is held inside exactly one `NodeKind`
//! variant and carries that variant's fields verbatim -- the IR has a single source of
//! truth for each operator's shape, and engine helpers take a `&FooNode` reference
//! directly.

use std::sync::Arc;

use url::Url;

use super::plan::JoinKind;
use crate::expressions::{ColumnName, Expression, Predicate, Scalar};
use crate::plans::kernel_consumers::{Handle, KernelConsumer, KernelConsumerToken};
use crate::schema::SchemaRef;
use crate::FileMeta;

// ============================================================================
// File-format selector (shared by `ScanNode` and `LoadNode`)
// ============================================================================

/// File formats supported by [`ScanNode`] and [`LoadNode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Json,
}

/// Alias for APIs that pick file format at runtime.
pub type FileFormat = FileType;

// ============================================================================
// Source-variant payloads (zero inputs)
// ============================================================================

/// Payload of [`NodeKind::ListFiles`](super::plan::NodeKind::ListFiles).
///
/// Lists files under a storage prefix. Engine emits a canonical file-listing
/// schema (path / size / modificationTime / etc.).
#[derive(Debug, Clone)]
pub struct ListFilesNode {
    pub start_from: Url,
}

/// Payload of [`NodeKind::Scan`](super::plan::NodeKind::Scan).
///
/// Reads `files` of the given [`FileType`] into row batches matching `schema`.
#[derive(Debug, Clone)]
pub struct ScanNode {
    pub file_type: FileType,
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
}

/// Payload of [`NodeKind::Values`](super::plan::NodeKind::Values).
///
/// Inline literal rows. Builder enforces `rows[i].len() == schema.fields().count()`.
#[derive(Debug, Clone)]
pub struct ValuesNode {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<Scalar>>,
}

// ============================================================================
// Transform-variant payloads (1+ inputs)
// ============================================================================

/// Payload of [`NodeKind::Project`](super::plan::NodeKind::Project).
///
/// Projects the single input through `named_exprs`, producing rows of `output_schema`.
/// The schema is supplied by the SSA builder (either inferred for narrow projections
/// via the kernel's expression-type inference, or declared explicitly via
/// [`PlanBuilder::project_with_schema`](crate::plans::state_machines::framework::plan_context::PlanBuilder::project_with_schema)
/// when inference is insufficient). Engines compile against the declared schema
/// directly and do not re-derive it from the expressions.
#[derive(Debug, Clone)]
pub struct ProjectNode {
    pub named_exprs: Vec<(String, Arc<Expression>)>,
    pub output_schema: SchemaRef,
}

/// Payload of [`NodeKind::Filter`](super::plan::NodeKind::Filter).
///
/// Keeps input rows where `predicate` evaluates true (SQL null semantics).
/// Output schema is the input schema unchanged.
#[derive(Debug, Clone)]
pub struct FilterNode {
    pub predicate: Arc<Predicate>,
}

/// Payload of [`NodeKind::Union`](super::plan::NodeKind::Union).
///
/// Concatenates N inputs (`inputs.len() >= 1`). All input schemas must agree.
/// `ordered=true` preserves child order; `ordered=false` permits reordering.
#[derive(Debug, Clone)]
pub struct UnionNode {
    pub ordered: bool,
}

/// Payload of [`NodeKind::Load`](super::plan::NodeKind::Load).
///
/// File-reader transform. Each input row carries a path (under `file_meta.path`); the
/// engine opens the resolved file as `file_type`, reads `file_schema` columns, and
/// broadcasts each upstream row's `passthrough_columns` onto every emitted file row.
/// An optional [`DvRef`] enables per-row deletion-vector masking against a descriptor
/// column on the upstream relation.
#[derive(Debug, Clone)]
pub struct LoadNode {
    pub file_schema: SchemaRef,
    pub file_type: FileType,
    pub base_url: Option<Url>,
    pub passthrough_columns: Vec<ColumnName>,
    pub file_meta: ScanFileColumns,
    pub dv_ref: Option<DvRef>,
}

/// Payload of [`NodeKind::MaxByVersion`](super::plan::NodeKind::MaxByVersion).
///
/// "Top 1 per group, ordered by version desc" -- a specialized aggregate. Output
/// schema is `group_by` exprs (with inferred types) followed by the named
/// `value_columns` lifted from input.
#[derive(Debug, Clone)]
pub struct MaxByVersionNode {
    pub group_by: Vec<Arc<Expression>>,
    pub version_column: Arc<Expression>,
    pub value_columns: Vec<String>,
}

/// Payload of [`NodeKind::EquiJoin`](super::plan::NodeKind::EquiJoin).
///
/// Equi-join two inputs (`inputs.len() == 2`, convention `[left, right]`).
#[derive(Debug, Clone)]
pub struct EquiJoinNode {
    pub kind: JoinKind,
    pub key_pairs: Vec<(Arc<Expression>, Arc<Expression>)>,
}

// ============================================================================
// Load helper types (referenced by `LoadNode`)
// ============================================================================

/// Column-name hints that a [`LoadNode`] reads from each upstream row to resolve which
/// file to open. The `path` column is mandatory; `size` and `record_count` are advisory
/// and used by engines for split-sizing / pruning decisions.
///
/// Names are [`ColumnName`]s so they may reference nested fields (e.g.
/// `add.path` on a Delta-checkpoint upstream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanFileColumns {
    /// Column on the upstream relation holding the per-row file path /
    /// URL fragment. Joined to [`LoadNode::base_url`] when set.
    pub path: ColumnName,
    /// Optional column with the file's total size in bytes.
    pub size: Option<ColumnName>,
    /// Optional column with the file's row-count (parquet-encoded `numRecords`).
    pub record_count: Option<ColumnName>,
}

/// Deletion-vector reference attached to a [`LoadNode`]. Rows present in the referenced
/// DV are skipped from the file read.
///
/// `column` is a [`crate::actions::deletion_vector::DeletionVectorDescriptor`] struct
/// column on the upstream relation.
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

/// Default file-meta column hints: `path` + `size` (no `record_count`). Shared by the
/// scan pipeline's [`LoadNode`] construction and the canonical `{path, size}` upstream
/// shape produced by the reconciliation builder.
pub(crate) fn default_scan_file_columns() -> ScanFileColumns {
    ScanFileColumns {
        path: ColumnName::new(["path"]),
        size: Some(ColumnName::new(["size"])),
        record_count: None,
    }
}

// ============================================================================
// Consumer-drain sink (referenced by `EngineRequest::Consume`)
// ============================================================================

/// Template for draining a row stream into a [`KernelConsumer`] via
/// [`EngineRequest::Consume`](crate::plans::state_machines::framework::step::EngineRequest::Consume).
///
/// - `initial_state`: cloned per partition via [`DynClone`](dyn_clone::DynClone) into a
///   [`Handle`](crate::plans::kernel_consumers::Handle).
/// - `token`: keys the finished handle returned from the executor and validated at decode time by
///   the paired [`Extractor`](crate::plans::kernel_consumers::Extractor).
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
        sm_id: uuid::Uuid,
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
