use std::fs::File;

use crate::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use crate::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};

use super::read_files;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{
    fixup_parquet_read, generate_mask, get_requested_indices, ordering_needs_row_indexes,
    RowIndexBuilder,
};
use crate::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use crate::schema::SchemaRef;
use crate::{DeltaResult, FileDataReadResultIterator, FileMeta, ParquetHandler, PredicateRef};

pub(crate) struct SyncParquetHandler;

fn try_create_from_parquet(
    file: File,
    schema: SchemaRef,
    _arrow_schema: ArrowSchemaRef,
    predicate: Option<PredicateRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
    let parquet_schema = metadata.schema();
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let (indices, requested_ordering) = get_requested_indices(&schema, parquet_schema)?;
    if let Some(mask) = generate_mask(&schema, parquet_schema, builder.parquet_schema(), &indices) {
        builder = builder.with_projection(mask);
    }

    // Only create RowIndexBuilder if row indexes are actually needed
    let mut row_indexes = ordering_needs_row_indexes(&requested_ordering)
        .then(|| RowIndexBuilder::new(builder.metadata().row_groups()));

    // Filter row groups and row indexes if a predicate is provided
    if let Some(predicate) = predicate {
        builder = builder.with_row_group_filter(predicate.as_ref(), row_indexes.as_mut());
    }

    let mut row_indexes = row_indexes.map(|rb| rb.build()).transpose()?;
    let stream = builder.build()?;
    Ok(stream.map(move |rbr| fixup_parquet_read(rbr?, &requested_ordering, row_indexes.as_mut())))
}

impl ParquetHandler for SyncParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        read_files(files, schema, predicate, try_create_from_parquet)
    }

    fn get_parquet_schema(&self, file_meta: &FileMeta) -> DeltaResult<SchemaRef> {
        use crate::engine::arrow_conversion::TryFromArrow as _;
        use crate::schema::StructType;
        use crate::Error;
        use std::sync::Arc;

        // Open the file for reading
        let file = File::open(file_meta.location.as_str()).map_err(|e| {
            Error::generic(format!("Failed to open file {}: {}", file_meta.location, e))
        })?;

        // Load just the metadata (footer) from the file
        let metadata = ArrowReaderMetadata::load(&file, Default::default())
            .map_err(|e| Error::generic(format!("Failed to load parquet metadata: {}", e)))?;

        // Get the Arrow schema from metadata
        let arrow_schema = metadata.schema();

        // Convert Arrow schema to kernel schema
        let kernel_schema = StructType::try_from_arrow(arrow_schema.as_ref())
            .map_err(|e| Error::generic(format!("Failed to convert schema: {}", e)))?;

        Ok(Arc::new(kernel_schema) as SchemaRef)
    }
}
