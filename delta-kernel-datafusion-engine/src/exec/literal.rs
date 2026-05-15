//! Values source helper backed by DataFusion native memory source.

use std::sync::Arc;

use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::array::{Array, ArrayRef, RecordBatch};
use delta_kernel::arrow::compute::concat;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::errors::{DeltaError, DeltaResultExt, KernelErrAsDelta};
use delta_kernel::schema::SchemaRef as KernelSchemaRef;

pub fn build_literal_exec(
    kernel_schema: KernelSchemaRef,
    rows: Vec<Vec<Scalar>>,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let arrow_schema: ArrowSchemaRef =
        Arc::new(kernel_schema.as_ref().try_into_arrow().map_err(|e| {
            crate::error::internal_error(format!("literal schema conversion: {e}"))
        })?);
    let batch = build_batch(&arrow_schema, &rows)?;
    let partitions = vec![vec![batch]];
    MemorySourceConfig::try_new_exec(&partitions, arrow_schema, None)
        .map(|exec| exec as Arc<dyn ExecutionPlan>)
        .map_err(crate::error::datafusion_err_to_delta)
}

fn build_batch(
    arrow_schema: &ArrowSchemaRef,
    rows: &[Vec<Scalar>],
) -> Result<RecordBatch, DeltaError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(arrow_schema.clone()));
    }
    let width = rows[0].len();
    let mut cols = Vec::with_capacity(width);
    for col_idx in 0..width {
        let mut pieces: Vec<ArrayRef> = Vec::with_capacity(rows.len());
        for row in rows {
            pieces.push(
                row[col_idx]
                    .to_array(1)
                    .map_err(|e| e.into_delta_default())?,
            );
        }
        let refs: Vec<&dyn Array> = pieces.iter().map(|a| a.as_ref()).collect();
        cols.push(concat(&refs).or_delta(
            delta_kernel::plans::errors::DeltaErrorCode::DeltaCommandInvariantViolation,
        )?);
    }
    RecordBatch::try_new(arrow_schema.clone(), cols)
        .map_err(|e| crate::error::plan_compilation(format!("literal batch build: {e}")))
}
