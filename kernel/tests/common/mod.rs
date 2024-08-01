use crate::ArrowEngineData;
use arrow::record_batch::RecordBatch;
use delta_kernel::DeltaResult;
use delta_kernel::EngineData;

#[macro_export]
macro_rules! sort_lines {
    ($lines: expr) => {{
        // sort except for header + footer
        let num_lines = $lines.len();
        if num_lines > 3 {
            $lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }
    }};
}

pub(crate) fn to_arrow(data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    Ok(data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into())
}
