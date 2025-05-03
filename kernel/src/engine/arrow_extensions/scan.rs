use std::sync::Arc;

use crate::arrow::array::BooleanArray;
use crate::arrow::compute::filter_record_batch;
use crate::arrow::record_batch::RecordBatch;
use itertools::Itertools;

use crate::scan::{Scan, ScanMetadata, ScanResult};
use crate::{DeltaResult, Engine, Error, ExpressionRef};

use super::super::arrow_data::ArrowEngineData;

/// [`ScanMetadata`] contains (1) a [`RecordBatch`] specifying data files to be scanned
/// and (2) a vector of transforms (one transform per scan file) that must be applied to the data read
/// from those files.
pub struct ScanMetadataArrow {
    /// Record batch with one row per file to scan
    pub scan_files: RecordBatch,

    /// Row-level transformations to apply to data read from files.
    ///
    /// Each entry in this vector corresponds to a row in the `scan_files` data. The entry is an
    /// expression that must be applied to convert the file's data into the logical schema
    /// expected by the scan:
    ///
    /// - `Some(expr)`: Apply this expression to transform the data to match [`Scan::schema()`].
    /// - `None`: No transformation is needed; the data is already in the correct logical form.
    ///
    /// Note: This vector can be indexed by row number.
    pub scan_file_transforms: Vec<Option<ExpressionRef>>,
}

impl TryFrom<ScanMetadata> for ScanMetadataArrow {
    type Error = Error;

    fn try_from(metadata: ScanMetadata) -> Result<Self, Self::Error> {
        let scan_file_transforms = metadata
            .scan_file_transforms
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| metadata.scan_files.selection_vector[i].then_some(v))
            .collect();
        let batch = ArrowEngineData::try_from_engine_data(metadata.scan_files.data)?.into();
        let scan_files = filter_record_batch(
            &batch,
            &BooleanArray::from(metadata.scan_files.selection_vector),
        )?;
        Ok(ScanMetadataArrow {
            scan_files,
            scan_file_transforms,
        })
    }
}

impl TryFrom<ScanResult> for RecordBatch {
    type Error = Error;

    fn try_from(result: ScanResult) -> Result<Self, Self::Error> {
        let (mask, data) = (result.full_mask(), result.raw_data?);
        let record_batch = ArrowEngineData::try_from_engine_data(data)?.into();
        mask.map(|m| Ok(filter_record_batch(&record_batch, &m.into())?))
            .unwrap_or(Ok(record_batch))
    }
}

pub trait ScanExt {
    fn scan_metadata_arrow(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;

    fn execute_arrow(
        &self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>>;
}

impl ScanExt for Scan {
    fn scan_metadata_arrow(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
        Ok(self
            .scan_metadata(engine)?
            .map_ok(TryFrom::try_from)
            .flatten())
    }

    fn execute_arrow(
        &self,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
        Ok(self.execute(engine)?.map_ok(TryFrom::try_from).flatten())
    }
}
