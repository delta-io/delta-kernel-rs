use std::fs::File;
use std::sync::Arc;

use crate::engine::default::parquet::{reader_options, writer_options};
use crate::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};

use super::read_files;
use crate::engine::arrow_conversion::{TryFromArrow as _, TryIntoArrow as _};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{
    fixup_parquet_read, generate_mask, get_requested_indices, ordering_needs_row_indexes,
    RowIndexBuilder,
};
use crate::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::schema::{SchemaRef, StructType};
use crate::{
    DeltaResult, Error, FileDataReadResultIterator, FileMeta, ParquetFooter, ParquetHandler,
    PredicateRef,
};

use url::Url;

pub(crate) struct SyncParquetHandler;

fn try_create_from_parquet(
    file: File,
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    file_location: String,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);
    let reader_options = reader_options();
    let metadata = ArrowReaderMetadata::load(&file, reader_options.clone())?;
    let parquet_schema = metadata.schema();
    let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, reader_options)?;
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
    Ok(stream.map(move |rbr| {
        fixup_parquet_read(
            rbr?,
            &requested_ordering,
            row_indexes.as_mut(),
            Some(&file_location),
            Some(&arrow_schema),
        )
    }))
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

    /// Writes engine data to a Parquet file at the specified location.
    ///
    /// This implementation uses synchronous file I/O to write the Parquet file.
    /// If a file already exists at the given location, it will be overwritten.
    ///
    /// # Parameters
    ///
    /// - `location` - The full URL path where the Parquet file should be written
    ///   (e.g., `file:///path/to/file.parquet`).
    /// - `data` - An iterator of engine data to be written to the Parquet file.
    ///
    /// # Returns
    ///
    /// A [`DeltaResult`] indicating success or failure.
    fn write_parquet_file(
        &self,
        location: Url,
        mut data: Box<dyn Iterator<Item = DeltaResult<Box<dyn crate::EngineData>>> + Send>,
    ) -> DeltaResult<()> {
        // Convert URL to file path
        let path = location
            .to_file_path()
            .map_err(|_| crate::Error::generic(format!("Invalid file URL: {}", location)))?;

        let mut file = File::create(&path)?;

        // Get first batch to initialize writer with schema
        let first_batch = data.next().ok_or_else(|| {
            crate::Error::generic("Cannot write parquet file with empty data iterator")
        })??;
        let first_arrow = ArrowEngineData::try_from_engine_data(first_batch)?;
        let first_record_batch: crate::arrow::array::RecordBatch = (*first_arrow).into();

        let mut writer = ArrowWriter::try_new_with_options(
            &mut file,
            first_record_batch.schema(),
            writer_options(),
        )?;
        writer.write(&first_record_batch)?;

        // Write remaining batches
        for result in data {
            let engine_data = result?;
            let arrow_data = ArrowEngineData::try_from_engine_data(engine_data)?;
            let batch: crate::arrow::array::RecordBatch = (*arrow_data).into();
            writer.write(&batch)?;
        }

        writer.close()?; // writer must be closed to write footer

        Ok(())
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        let path = file
            .location
            .to_file_path()
            .map_err(|_| Error::generic("SyncEngine can only read local files"))?;
        let file = File::open(path)?;
        let metadata = ArrowReaderMetadata::load(&file, reader_options())?;
        let schema = StructType::try_from_arrow(metadata.schema().as_ref())
            .map(Arc::new)
            .map_err(Error::Arrow)?;
        Ok(ParquetFooter { schema })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
    use crate::engine::arrow_conversion::TryIntoKernel as _;
    use std::sync::Arc;
    use tempfile::tempdir;
    use url::Url;

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

        // Create iterator with single batch
        let data_iter: Box<
            dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>> + Send,
        > = Box::new(std::iter::once(Ok(engine_data)));

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
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
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

    #[test]
    fn test_sync_write_parquet_file_with_filter() {
        let handler = SyncParquetHandler;
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_filtered.parquet");
        let url = Url::from_file_path(&file_path).unwrap();

        // Create test data with only filtered rows: 1, 3, 5
        let engine_data: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from(vec![1, 3, 5])) as Arc<dyn Array>,
                ),
                (
                    "name",
                    Arc::new(StringArray::from(vec!["a", "c", "e"])) as Arc<dyn Array>,
                ),
            ])
            .unwrap(),
        ));

        // Create iterator with single batch
        let data_iter: Box<
            dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>> + Send,
        > = Box::new(std::iter::once(Ok(engine_data)));

        // Write the file
        handler.write_parquet_file(url.clone(), data_iter).unwrap();

        // Verify the file exists
        assert!(file_path.exists());

        // Read it back to verify only filtered rows are present
        let file = File::open(&file_path).unwrap();
        let reader =
            crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap();
        let schema = reader.schema().clone();
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
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

        // Verify shape - should only have 3 rows (filtered from 5)
        assert_eq!(record_batch.num_rows(), 3);
        assert_eq!(record_batch.num_columns(), 2);

        // Verify content - id column should have values 1, 3, 5
        let id_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[1, 3, 5]);

        // Verify content - name column should have values "a", "c", "e"
        let name_col = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "a");
        assert_eq!(name_col.value(1), "c");
        assert_eq!(name_col.value(2), "e");

        assert!(result.next().is_none());
    }

    #[test]
    fn test_sync_write_parquet_file_multiple_batches() {
        let handler = SyncParquetHandler;
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_multi_batch.parquet");
        let url = Url::from_file_path(&file_path).unwrap();

        // Create multiple batches
        let batch1: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let batch2: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![4, 5, 6])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let batch3: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![7, 8, 9])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));

        // Create iterator with multiple batches
        let batches = vec![Ok(batch1), Ok(batch2), Ok(batch3)];
        let data_iter: Box<
            dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>> + Send,
        > = Box::new(batches.into_iter());

        // Write the file
        handler.write_parquet_file(url.clone(), data_iter).unwrap();

        // Verify the file exists
        assert!(file_path.exists());

        // Read it back to verify all batches were written
        let file = File::open(&file_path).unwrap();
        let reader =
            crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap();
        let schema = reader.schema().clone();
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
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

        // Verify we have all 9 rows from 3 batches
        assert_eq!(record_batch.num_rows(), 9);
        let value_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_col.values(), &[1, 2, 3, 4, 5, 6, 7, 8, 9]);

        assert!(result.next().is_none());
    }
}
