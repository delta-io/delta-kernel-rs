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
//! 1. **Borrow, don't move** for handles — [`relation_ref`] takes `&RelationHandle` so callers
//!    can reuse the same handle in multiple plans without manual `.clone()`.
//! 2. **Top-down readability** — `plan::relation_ref(&h).filter(...).into_results()` reads
//!    cleaner than `DeclarativePlanNode::relation_ref(h.clone()).filter(...).into_results()`.

use super::declarative::DeclarativePlanNode;
use super::nodes::{FileFormat, RelationHandle, SinkNode};
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
