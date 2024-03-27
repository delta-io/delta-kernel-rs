use std::fs::File;

use arrow_schema::Schema as ArrowSchema;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use tracing::debug;
use url::Url;

use crate::{
    client::{
        arrow_data::ArrowEngineData,
        arrow_utils::{generate_mask, get_requested_indices, reorder_record_batch},
    },
    schema::SchemaRef,
    DeltaResult, Error, Expression, FileDataReadResultIterator, FileMeta, ParquetHandler,
};

pub(crate) struct SyncParquetHandler;

fn try_create_from_parquet(schema: SchemaRef, location: Url) -> DeltaResult<ArrowEngineData> {
    let file = File::open(
        location
            .to_file_path()
            .map_err(|_| Error::generic("can only read local files"))?,
    )?;
    let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
    let parquet_schema = metadata.schema();
    let requested_schema: ArrowSchema = (&*schema).try_into()?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let indicies = get_requested_indices(&requested_schema, parquet_schema)?;
    if let Some(mask) = generate_mask(
        &requested_schema,
        parquet_schema,
        builder.parquet_schema(),
        &indicies,
    ) {
        builder = builder.with_projection(mask);
    }
    let mut reader = builder.build()?;
    let data = reader
        .next()
        .ok_or(Error::generic("No data found reading parquet file"))?;
    Ok(ArrowEngineData::new(reorder_record_batch(
        requested_schema.into(),
        data?,
        &indicies,
    )?))
}

impl ParquetHandler for SyncParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        _predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        debug!("Reading parquet files: {files:#?} with schema {schema:#?}");
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let locations: Vec<_> = files.iter().map(|file| file.location.clone()).collect();
        Ok(Box::new(locations.into_iter().map(move |location| {
            let d = try_create_from_parquet(schema.clone(), location);
            d.map(|d| Box::new(d) as _)
        })))
    }
}
