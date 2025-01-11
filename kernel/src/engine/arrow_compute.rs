use crate::engine::arrow_data::ArrowEngineData;
use crate::error::DeltaResult;
use crate::scan::ScanResult;

use arrow::compute::filter_record_batch;
use arrow_array::RecordBatch;

/// Utility function that transforms an iterator of `ScanResult`s into an iterator of
/// `RecordBatch`s containing the actual scan data by applying the scan result's mask to the data.
///
/// It uses arrow-compute's `filter_record_batch` function to apply the mask to the data.
pub fn materialize_scan_results(
    scan_results: impl Iterator<Item = DeltaResult<ScanResult>>,
) -> impl Iterator<Item = DeltaResult<RecordBatch>> {
    scan_results.map(|res| {
        let scan_res = res.and_then(|res| Ok((res.full_mask(), res.raw_data?)));
        let (mask, data) = scan_res?;
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| crate::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into();
        Ok(match mask {
            Some(mask) => filter_record_batch(&record_batch, &mask.into())?,
            None => record_batch,
        })
    })
}
