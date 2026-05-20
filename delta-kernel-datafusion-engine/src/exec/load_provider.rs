//! Lazy [`TableProvider`] for `LoadSink`: defers all work to `scan()`, which lowers the
//! upstream `LogicalPlan` and wraps it in a [`super::LoadExec`]. Used for non-Values
//! upstreams or any sink with a deletion vector. See also [`super::EagerLoadTableProvider`]
//! for the bare-Values fast path. Filter pushdown is currently off; projection and limit
//! flow through to [`super::LoadExec`].

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

pub struct LoadTableProvider {
    upstream_logical: LogicalPlan,
    sink: Arc<LoadSink>,
    engine: Arc<dyn Engine>,
    /// `file_schema_fields ++ passthrough_fields`, pre-materialized so `schema()` is cheap.
    output_schema: ArrowSchemaRef,
}

impl LoadTableProvider {
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
                        "LoadTableProvider output schema: {e}"
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
