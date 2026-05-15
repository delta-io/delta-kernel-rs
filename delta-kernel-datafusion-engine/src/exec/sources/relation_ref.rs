use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use datafusion::datasource::{provider_as_source, MemTable};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_expr::LogicalPlanBuilder;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::RelationHandle;

/// In-memory relation materialization used by sequential multi-plan execution.
#[derive(Default)]
pub struct RelationBatchRegistry {
    inner: RwLock<HashMap<u64, Vec<Vec<RecordBatch>>>>,
}

impl RelationBatchRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, handle_id: u64, batches: Vec<RecordBatch>) {
        self.register_partitions(handle_id, vec![batches]);
    }

    pub fn register_partitions(&self, handle_id: u64, partitions: Vec<Vec<RecordBatch>>) {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        guard.insert(handle_id, partitions);
    }

    pub fn get_cloned(&self, handle_id: u64) -> Option<Vec<RecordBatch>> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard
            .get(&handle_id)
            .map(|parts| parts.iter().flat_map(|p| p.clone()).collect())
    }

    pub fn get_partitions_cloned(&self, handle_id: u64) -> Option<Vec<Vec<RecordBatch>>> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.get(&handle_id).cloned()
    }
}

impl Debug for RelationBatchRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationBatchRegistry")
            .finish_non_exhaustive()
    }
}

pub fn build_relation_ref_exec(
    handle: &RelationHandle,
    registry: &RelationBatchRegistry,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let schema: delta_kernel::arrow::datatypes::SchemaRef =
        Arc::new(handle.schema.as_ref().try_into_arrow().map_err(|e| {
            crate::error::internal_error(format!("relation schema conversion: {e}"))
        })?);
    let empty_partitions = vec![Vec::new()];
    let guard = registry.inner.read().unwrap_or_else(|e| e.into_inner());
    let partitions: &[Vec<RecordBatch>] = match guard.get(&handle.id) {
        Some(p) if p.is_empty() => empty_partitions.as_slice(),
        Some(p) => p.as_slice(),
        None => empty_partitions.as_slice(),
    };
    MemorySourceConfig::try_new_exec(partitions, schema, None)
        .map(|exec| exec as Arc<dyn ExecutionPlan>)
        .map_err(crate::error::datafusion_err_to_delta)
}

pub fn build_relation_ref_logical(
    handle: &RelationHandle,
    registry: &RelationBatchRegistry,
) -> Result<datafusion_expr::logical_plan::LogicalPlan, DeltaError> {
    let schema: delta_kernel::arrow::datatypes::SchemaRef =
        Arc::new(handle.schema.as_ref().try_into_arrow().map_err(|e| {
            crate::error::internal_error(format!("relation schema conversion: {e}"))
        })?);
    let empty_partitions = vec![Vec::new()];
    let guard = registry.inner.read().unwrap_or_else(|e| e.into_inner());
    let partitions = match guard.get(&handle.id) {
        Some(p) if p.is_empty() => empty_partitions.clone(),
        Some(p) => p.clone(),
        None => empty_partitions.clone(),
    };
    let table = Arc::new(
        MemTable::try_new(schema, partitions).map_err(crate::error::datafusion_err_to_delta)?,
    );
    LogicalPlanBuilder::scan(
        format!("relation_{}", handle.id),
        provider_as_source(table),
        None,
    )
    .map_err(crate::error::datafusion_err_to_delta)?
    .build()
    .map_err(crate::error::datafusion_err_to_delta)
}
