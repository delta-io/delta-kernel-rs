use crate::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use crate::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use std::fs::File;
use url::Url;

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

    fn write_parquet_file(
        &self,
        url: Url,
        data: Box<dyn Iterator<Item = crate::DeltaResult<crate::FilteredEngineData>> + Send + '_>,
    ) -> DeltaResult<()> {
        // Collect all ArrowEngineData batches first
        let mut batches = Vec::new();
        for filtered_batch in data {
            let filtered_batch = filtered_batch?;
            let (engine_data, _selection_vector) = filtered_batch.into_parts();
            let batch: Box<ArrowEngineData> = ArrowEngineData::try_from_engine_data(engine_data)?;
            batches.push(batch);
        }

        // If there are no batches, return early
        if batches.is_empty() {
            return Ok(());
        }

        // Convert URL to file path
        let path = url
            .to_file_path()
            .map_err(|_| crate::Error::generic(format!("Invalid file URL: {}", url)))?;
        let mut file = File::create(&path)?;

        let mut writer = ArrowWriter::try_new(&mut file, batches[0].record_batch().schema(), None)?;

        for batch in &batches {
            writer.write(batch.record_batch())?;
        }

        writer.close()?; // writer must be closed to write footer

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
    use crate::engine::arrow_conversion::TryIntoKernel as _;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_sync_write_parquet_file() {
        let handler = SyncParquetHandler;
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.parquet");
        let url = Url::from_file_path(&file_path).unwrap();

        // Create test data
        let engine_data: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
                ),
                (
                    "name",
                    Arc::new(StringArray::from(vec!["a", "b", "c"])) as Arc<dyn Array>,
                ),
            ])
            .unwrap(),
        ));

        // Wrap in FilteredEngineData with all rows selected
        let filtered_data = crate::FilteredEngineData::with_all_rows_selected(engine_data);
        let data_iter: Box<
            dyn Iterator<Item = crate::DeltaResult<crate::FilteredEngineData>> + Send,
        > = Box::new(std::iter::once(Ok(filtered_data)));

        // Write the file
        handler.write_parquet_file(url.clone(), data_iter).unwrap();

        // Verify the file exists
        assert!(file_path.exists());

        // Read it back to verify
        let file = File::open(&file_path).unwrap();
        let reader =
            crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap();
        let schema = reader.schema().clone();

        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: 0,
        };

        let mut result = handler
            .read_parquet_files(
                &[file_meta],
                Arc::new(schema.try_into_kernel().unwrap()),
                None,
            )
            .unwrap();

        let engine_data = result.next().unwrap().unwrap();
        let batch = ArrowEngineData::try_from_engine_data(engine_data).unwrap();
        let record_batch = batch.record_batch();

        // Verify shape
        assert_eq!(record_batch.num_rows(), 3);
        assert_eq!(record_batch.num_columns(), 2);

        // Verify content - id column
        let id_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[1, 2, 3]);

        // Verify content - name column
        let name_col = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "a");
        assert_eq!(name_col.value(1), "b");
        assert_eq!(name_col.value(2), "c");

        assert!(result.next().is_none());
    }
}
