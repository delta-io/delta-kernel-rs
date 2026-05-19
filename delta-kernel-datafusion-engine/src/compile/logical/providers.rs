//! [`TableProvider`] implementations for the logical-plan lowering.
//!
//! Two providers live here:
//! - [`NullabilityEnforcingTableProvider`] wraps a
//!   [`ListingTable`](datafusion::datasource::listing::ListingTable) built with a
//!   nullability-relaxed schema and re-asserts the strict kernel schema on the scan's output (used
//!   for JSON scans).
//! - [`FileListingTableProvider`] backs
//!   [`DeclarativePlanNode::FileListing`](delta_kernel::plans::ir::DeclarativePlanNode::FileListing)
//!   nodes, emitting one `(path, size, modification_time)` row per object under a URL prefix.

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanBuilder, TableType};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::ir::nodes::FileListingNode;

use crate::exec::{FileListingExec, NullabilityValidationExec};

/// [`TableProvider`] that wraps an inner provider (typically a
/// [`ListingTable`](datafusion::datasource::listing::ListingTable) built with a nullability-relaxed
/// schema so its file source's planner accepts it) and re-asserts the strict kernel schema on the
/// scan's output. The wrapper declares the strict schema as its [`TableProvider::schema`], so
/// logical plans built on top see the strict types; the runtime [`NullabilityValidationExec`]
/// guarantees emitted batches conform.
///
/// Used for both Parquet and JSON scans today; either decoder can hand back batches whose
/// nullability is laxer than the kernel-declared schema (parquet checkpoints commonly write
/// `add.path` as nullable; the JSON decoder silently drops declared NOT NULL on nested
/// children). The wrapper re-asserts the strict contract at the scan boundary.
pub(super) struct NullabilityEnforcingTableProvider {
    inner: Arc<dyn TableProvider>,
    strict_schema: Arc<ArrowSchema>,
}

impl NullabilityEnforcingTableProvider {
    pub(super) fn new(inner: Arc<dyn TableProvider>, strict_schema: Arc<ArrowSchema>) -> Self {
        Self {
            inner,
            strict_schema,
        }
    }
}

impl std::fmt::Debug for NullabilityEnforcingTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NullabilityEnforcingTableProvider")
            .field("strict_root_fields", &self.strict_schema.fields().len())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl TableProvider for NullabilityEnforcingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::clone(&self.strict_schema)
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.scan(state, projection, filters, limit).await?;
        let projected_strict: Arc<ArrowSchema> = match projection {
            Some(p) => Arc::new(self.strict_schema.project(p)?),
            None => Arc::clone(&self.strict_schema),
        };
        Ok(Arc::new(NullabilityValidationExec::new(
            inner,
            projected_strict,
        )))
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<datafusion_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

/// [`TableProvider`] for
/// [`DeclarativePlanNode::FileListing`](delta_kernel::plans::ir::DeclarativePlanNode::FileListing):
/// enumerates a storage prefix via the object store registered for the path's scheme/host and
/// emits a `(path, size, modification_time)` row per object. The actual listing happens inside
/// the returned [`ExecutionPlan`] at execute time; planning is fast.
#[derive(Debug)]
struct FileListingTableProvider {
    path: url::Url,
    schema: Arc<ArrowSchema>,
}

impl FileListingTableProvider {
    fn new(path: url::Url) -> Self {
        use datafusion_common::arrow::datatypes::{DataType, Field};
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("modification_time", DataType::Int64, false),
        ]));
        Self { path, schema }
    }
}

#[async_trait::async_trait]
impl TableProvider for FileListingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FileListingExec::new(self.path.clone())))
    }
}

pub(super) fn file_listing_to_logical_plan(
    node: &FileListingNode,
) -> Result<LogicalPlan, DataFusionError> {
    let provider: Arc<dyn TableProvider> =
        Arc::new(FileListingTableProvider::new(node.path.clone()));
    LogicalPlanBuilder::scan("file_listing", provider_as_source(provider), None)?.build()
}
