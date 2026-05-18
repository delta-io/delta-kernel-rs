//! [`LoadTableProvider`] -- a lazy [`TableProvider`] wrapping a [`LoadSink`] over an upstream
//! [`LogicalPlan`].
//!
//! Registered into the executor's `relation_providers` map at sink-registration time. Holds the
//! upstream logical plan, the [`LoadSink`] config, the kernel [`Engine`], and a pre-computed
//! Arrow output schema. Upstream is never executed until a downstream consumer calls
//! [`TableProvider::scan`]: at that point we lower the upstream `LogicalPlan` to a physical plan
//! via [`Session::create_physical_plan`] and hand it to [`super::LoadExec`], which streams
//! per-row file batches one at a time.
//!
//! `get_logical_plan` returns `None` so DataFusion's `InlineTableScan` analyzer rule treats the
//! provider as opaque -- pushdown flows through the standard [`TableProvider::scan(state,
//! projection, filters, limit)`] hook. Filter pushdown is currently off
//! ([`TableProvider::supports_filters_pushdown`] returns `Unsupported`); both projection and
//! limit are pushed down into [`super::LoadExec`] -- projection narrows the kernel parquet/json
//! handler's read schema and the broadcast set, and limit caps total streamed rows (no further
//! files are opened once the budget is exhausted).

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::LoadSink;
use delta_kernel::Engine;

use crate::exec::LoadExec;

/// Lazy [`TableProvider`] for a [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load)
/// registration. See module docs.
pub struct LoadTableProvider {
    upstream_logical: LogicalPlan,
    sink: Arc<LoadSink>,
    engine: Arc<dyn Engine>,
    /// Full output Arrow schema = file_schema fields ++ passthrough fields. Pre-computed once
    /// at construction so `scan()` (and `schema()`) are cheap.
    output_schema: ArrowSchemaRef,
}

impl LoadTableProvider {
    /// Build the provider, materializing the Arrow form of the sink's declared output schema.
    pub fn try_new(
        upstream_logical: LogicalPlan,
        sink: Arc<LoadSink>,
        engine: Arc<dyn Engine>,
    ) -> Result<Self, DataFusionError> {
        let output_schema: ArrowSchemaRef = Arc::new(
            sink.output_relation
                .schema
                .as_ref()
                .try_into_arrow()
                .map_err(|e| {
                    crate::error::plan_compilation(format!(
                        "LoadTableProvider output schema conversion: {e}"
                    ))
                })?,
        );
        Ok(Self {
            upstream_logical,
            sink,
            engine,
            output_schema,
        })
    }
}

impl std::fmt::Debug for LoadTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadTableProvider")
            .field("output_relation", &self.sink.output_relation.name)
            .field("file_type", &self.sink.file_type)
            .field("output_field_count", &self.output_schema.fields().len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for LoadTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let upstream_physical = state.create_physical_plan(&self.upstream_logical).await?;
        let load_exec = LoadExec::new(
            upstream_physical,
            Arc::clone(&self.sink),
            Arc::clone(&self.engine),
            Arc::clone(&self.output_schema),
            projection.cloned(),
            limit,
        )?;
        Ok(Arc::new(load_exec))
    }

    // Initial: leave filter pushdown off. Revisit once a benchmark motivates pushing predicates
    // down into LoadExec (then onward into the kernel parquet handler's read).
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
