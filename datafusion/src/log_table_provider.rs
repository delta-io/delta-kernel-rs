use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_catalog::memory::MemorySourceConfig;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::error::Result;
use datafusion_common::DataFusionError;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use delta_kernel::actions::get_log_schema;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::{snapshot::Snapshot, Table};
use itertools::Itertools;

use crate::session::KernelSessionExt;

static LOG_SCHEMA: LazyLock<ArrowSchemaRef> =
    LazyLock::new(|| Arc::new(get_log_schema().as_ref().try_into().unwrap()));

#[derive(Debug)]
pub struct DeltaLogTableProvider {
    table: Arc<Table>,
}

impl DeltaLogTableProvider {
    pub fn try_new(table: Arc<Table>) -> Result<Self> {
        Ok(Self { table })
    }

    pub fn log_schema() -> ArrowSchemaRef {
        Arc::clone(&LOG_SCHEMA)
    }
}

#[async_trait]
impl TableProvider for DeltaLogTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Self::log_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let engine = state.kernel_engine()?;
        let table_root = self.table.location().clone();

        let snapshot = tokio::task::spawn_blocking(move || {
            Snapshot::try_new(table_root, engine.as_ref(), None)
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        })
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))??;

        let engine = state.kernel_engine()?;
        let actions = tokio::task::spawn_blocking(move || {
            snapshot
                .log_segment()
                .read_actions(
                    engine.as_ref(),
                    get_log_schema().clone(),
                    get_log_schema().clone(),
                    None,
                )?
                .map(|res| {
                    res.and_then(|(data, _)| {
                        ArrowEngineData::try_from_engine_data(data)
                            .map(|d| d.record_batch().clone())
                    })
                })
                .try_collect::<_, Vec<_>, _>()
        })
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let source = MemorySourceConfig::try_new(&[actions], self.schema(), projection.cloned())?
            .with_limit(limit);

        Ok(DataSourceExec::from_data_source(source))
    }
}
