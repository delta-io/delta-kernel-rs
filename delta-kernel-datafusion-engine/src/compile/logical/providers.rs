//! [`TableProvider`] implementations used by the logical-plan lowering.
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
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanBuilder, TableType};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::ir::nodes::FileListingNode;

use crate::exec::{FileListingExec, NullabilityValidationExec};

/// Walk `field` recursively and emit `(parent_path, child_name)` pairs for every NOT NULL
/// child sitting inside a nullable parent struct -- the case neither DataFusion's
/// `check_not_null_constraints` (top-level only) nor delta-rs's `DataValidationStream`
/// (skips nested) handles. Spark Delta enforces this case; we match its semantics via
/// [`NullabilityValidationExec`].
pub(super) fn collect_nested_non_null_validations(
    path: String,
    field: &ArrowField,
    out: &mut Vec<(String, String)>,
) {
    if let ArrowDataType::Struct(fields) = field.data_type() {
        if field.is_nullable() {
            for child in fields {
                if !child.is_nullable() {
                    out.push((path.clone(), child.name().to_string()));
                }
            }
        }
        for child in fields {
            collect_nested_non_null_validations(
                format!("{path}.{}", child.name()),
                child.as_ref(),
                out,
            );
        }
    }
}

pub(super) fn nested_non_null_validations(schema: &ArrowSchema) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for field in schema.fields() {
        collect_nested_non_null_validations(field.name().to_string(), field.as_ref(), &mut out);
    }
    out
}

/// [`TableProvider`] that wraps an inner provider (typically a
/// [`ListingTable`](datafusion::datasource::listing::ListingTable) built with a nullability-relaxed
/// schema so its file source's planner accepts it) and re-asserts the strict kernel schema on the
/// scan's output. The wrapper declares the strict schema as its [`TableProvider::schema`], so
/// logical plans built on top see the strict types; the runtime [`NullabilityValidationExec`]
/// guarantees emitted batches conform.
///
/// Used for JSON scans where DataFusion's JSON decoder cannot accept Delta protocol NOT NULL
/// constraints on nested fields. Parquet scans skip the wrapper -- parquet's own decoder
/// enforces declared nullability at decode time.
pub(super) struct NullabilityEnforcingTableProvider {
    inner: Arc<dyn TableProvider>,
    strict_schema: Arc<ArrowSchema>,
    nested_validations: Vec<(String, String)>,
}

impl NullabilityEnforcingTableProvider {
    pub(super) fn new(inner: Arc<dyn TableProvider>, strict_schema: Arc<ArrowSchema>) -> Self {
        let nested_validations = nested_non_null_validations(strict_schema.as_ref());
        Self {
            inner,
            strict_schema,
            nested_validations,
        }
    }
}

impl std::fmt::Debug for NullabilityEnforcingTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NullabilityEnforcingTableProvider")
            .field("nested_validations", &self.nested_validations.len())
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
        // Schema we promise to deliver after wrapping: strict schema, then projection.
        // NullabilityValidationExec zips this against the inner batches positionally and casts
        // each column to the matching field, so the column count and order must match the
        // inner plan's output.
        let projected_schema: Arc<ArrowSchema> = match projection {
            Some(p) => Arc::new(self.strict_schema.project(p)?),
            None => Arc::clone(&self.strict_schema),
        };
        // Drop validations whose top-level root was projected out -- they reference columns
        // that don't exist in `projected_schema` and would fail by-name lookup at runtime.
        let kept_roots: HashSet<&str> = projected_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let validations: Vec<(String, String)> = self
            .nested_validations
            .iter()
            .filter(|(parent_path, _)| {
                let root = parent_path.split('.').next().unwrap_or(parent_path);
                kept_roots.contains(root)
            })
            .cloned()
            .collect();
        Ok(Arc::new(NullabilityValidationExec::new(
            inner,
            validations,
            projected_schema,
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
