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
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
};
use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanBuilder, TableType};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::ir::nodes::FileListingNode;

use crate::exec::{FileListingExec, NullabilityValidationExec};

/// Merge `source` and `strict` schemas: each output field takes its NAME, METADATA, and inner
/// child structure from `source` (so PARQUET:field_id and other source-side metadata that
/// downstream operators rely on survive the wrapper), but its NULLABILITY flag from `strict`
/// (so the kernel-declared NOT NULL contract is what we promise downstream).
///
/// The schemas must have matching field counts at every level; if a struct's child counts differ,
/// we fall back to `source`'s structure for that subtree.
pub(super) fn merge_target_nullability(source: &ArrowSchema, strict: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<Arc<ArrowField>> = if source.fields().len() == strict.fields().len() {
        source
            .fields()
            .iter()
            .zip(strict.fields().iter())
            .map(|(s, t)| Arc::new(merge_field(s.as_ref(), t.as_ref())))
            .collect()
    } else {
        source.fields().iter().cloned().collect()
    };
    ArrowSchema::new_with_metadata(fields, source.metadata().clone())
}

fn merge_field(source: &ArrowField, strict: &ArrowField) -> ArrowField {
    let data_type = merge_data_type(source.data_type(), strict.data_type());
    ArrowField::new(source.name(), data_type, strict.is_nullable())
        .with_metadata(source.metadata().clone())
}

fn merge_data_type(source: &ArrowDataType, strict: &ArrowDataType) -> ArrowDataType {
    use ArrowDataType::*;
    match (source, strict) {
        (Struct(src_fields), Struct(tgt_fields)) if src_fields.len() == tgt_fields.len() => {
            let merged: ArrowFields = src_fields
                .iter()
                .zip(tgt_fields.iter())
                .map(|(s, t)| Arc::new(merge_field(s.as_ref(), t.as_ref())))
                .collect();
            Struct(merged)
        }
        (List(s), List(t)) => List(Arc::new(merge_field(s, t))),
        (LargeList(s), LargeList(t)) => LargeList(Arc::new(merge_field(s, t))),
        (FixedSizeList(s, n), FixedSizeList(t, _)) => {
            FixedSizeList(Arc::new(merge_field(s, t)), *n)
        }
        (Map(s, sorted), Map(t, _)) => Map(Arc::new(merge_field(s, t)), *sorted),
        _ => source.clone(),
    }
}

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
        // We compute the effective target schema from `inner.schema()` (preserving source-side
        // names + metadata like PARQUET:field_id that downstream operators rely on) merged with
        // the projected strict schema (taking only its nullability flags). The wrapper then
        // promises this merged schema and re-asserts only the nullability contract; we never
        // overwrite source field names or metadata while validating.
        let projected_strict: Arc<ArrowSchema> = match projection {
            Some(p) => Arc::new(self.strict_schema.project(p)?),
            None => Arc::clone(&self.strict_schema),
        };
        let inner_schema = inner.schema();
        let merged_target = Arc::new(merge_target_nullability(
            inner_schema.as_ref(),
            projected_strict.as_ref(),
        ));
        // Derive validations against the merged schema so paths resolve against source-side
        // names (e.g. physical names on nested column-mapping struct fields).
        let validations = nested_non_null_validations(merged_target.as_ref());
        // Defensive: drop validations whose top-level root was projected out.
        let kept_roots: HashSet<&str> = merged_target
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let validations: Vec<(String, String)> = validations
            .into_iter()
            .filter(|(parent_path, _)| {
                let root = parent_path.split('.').next().unwrap_or(parent_path);
                kept_roots.contains(root)
            })
            .collect();
        Ok(Arc::new(NullabilityValidationExec::new(
            inner,
            validations,
            merged_target,
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
