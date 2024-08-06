use crate::ArrowEngineData;
use arrow::record_batch::RecordBatch;
use delta_kernel::DeltaResult;
use delta_kernel::EngineData;

pub(crate) fn to_arrow(data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    Ok(data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into())
}
