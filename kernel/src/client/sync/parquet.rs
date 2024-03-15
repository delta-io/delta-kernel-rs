use tracing::debug;

use crate::{
    schema::SchemaRef, DeltaResult, Expression, FileDataReadResultIterator, FileMeta,
    ParquetHandler,
};

pub(crate) struct SyncParquetHandler {}

impl ParquetHandler for SyncParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        _predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        debug!("Reading parquet files: {:#?}", files);
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let locations: Vec<_> = files.iter().map(|file| file.location.clone()).collect();
        Ok(Box::new(locations.into_iter().map(move |location| {
            let d = crate::client::arrow_data::ArrowEngineData::try_create_from_parquet(
                schema.clone(),
                location,
            );
            d.map(|d| Box::new(d) as _)
        })))
    }
}
