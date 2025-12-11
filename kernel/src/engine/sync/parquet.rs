use std::fs::File;
use std::sync::Arc;

use crate::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use crate::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};

use super::read_files;
use crate::engine::arrow_conversion::TryFromArrow as _;
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
    _arrow_schema: ArrowSchemaRef,
    predicate: Option<PredicateRef>,
    file_location: String,
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
    Ok(stream.map(move |rbr| {
        fixup_parquet_read(
            rbr?,
            &requested_ordering,
            row_indexes.as_mut(),
            Some(&file_location),
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

        let mut writer = ArrowWriter::try_new(&mut file, first_record_batch.schema(), None)?;
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
        let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
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
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::engine::arrow_conversion::TryIntoKernel as _;
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use std::collections::HashMap;
    use std::path::PathBuf;
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

    #[test]
    fn test_sync_read_parquet_footer() -> DeltaResult<()> {
        let handler = SyncParquetHandler;
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet",
        ))?;
        let file_size = std::fs::metadata(&path)?.len();
        let url = Url::from_file_path(path).unwrap();

        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
        };

        let footer = handler.read_parquet_footer(&file_meta)?;
        crate::utils::test_utils::validate_checkpoint_schema(&footer.schema);

        Ok(())
    }

    #[test]
    fn test_sync_read_parquet_footer_invalid_file() {
        let handler = SyncParquetHandler;

        let mut temp_path = std::env::temp_dir();
        temp_path.push("non_existent_file_for_sync_test.parquet");
        let url = Url::from_file_path(temp_path).unwrap();
        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: 0,
        };

        let result = handler.read_parquet_footer(&file_meta);
        assert!(result.is_err(), "Should error on non-existent file");
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
    fn test_sync_write_parquet_file_overwrite_true() {
        let handler = SyncParquetHandler;
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_overwrite.parquet");
        let url = Url::from_file_path(&file_path).unwrap();

        // Create first data set
        let engine_data1: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let data_iter1: Box<
            dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>> + Send,
        > = Box::new(std::iter::once(Ok(engine_data1)));

        // Write the first file
        handler.write_parquet_file(url.clone(), data_iter1).unwrap();
        assert!(file_path.exists());

        // Create second data set with different data
        let engine_data2: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![10, 20])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let data_iter2: Box<
            dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>> + Send,
        > = Box::new(std::iter::once(Ok(engine_data2)));

        // Overwrite with second file (overwrite=true)
        handler.write_parquet_file(url.clone(), data_iter2).unwrap();

        // Read back and verify it contains the second data set
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

        // Verify we have the second data set (2 rows, not 3)
        assert_eq!(record_batch.num_rows(), 2);
        let value_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_col.values(), &[10, 20]);

        assert!(result.next().is_none());
    }

    #[test]
    fn test_sync_write_parquet_file_always_overwrites() {
        let handler = SyncParquetHandler;
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_no_overwrite.parquet");
        let url = Url::from_file_path(&file_path).unwrap();

        // Create first data set
        let engine_data1: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let data_iter1: Box<
            dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>> + Send,
        > = Box::new(std::iter::once(Ok(engine_data1)));

        // Write the first file
        handler.write_parquet_file(url.clone(), data_iter1).unwrap();
        assert!(file_path.exists());

        // Create second data set
        let engine_data2: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![10, 20])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let data_iter2: Box<
            dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>> + Send,
        > = Box::new(std::iter::once(Ok(engine_data2)));

        // Write again - should overwrite successfully (new behavior always overwrites)
        handler.write_parquet_file(url.clone(), data_iter2).unwrap();

        // Verify the file was overwritten with the new data
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

        // Verify we now have the second data set (2 rows)
        assert_eq!(record_batch.num_rows(), 2);
        let value_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_col.values(), &[10, 20]);

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

    #[test]
    fn test_sync_read_parquet_footer_preserves_field_ids() {
        // Create Arrow schema with field IDs in metadata
        let field_with_id = Field::new("id", ArrowDataType::Int64, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );
        let field_with_id_2 = Field::new("name", ArrowDataType::Utf8, true).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![field_with_id, field_with_id_2]));

        // Write a parquet file with this schema
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test_field_ids.parquet");

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read footer and verify schema
        let handler = SyncParquetHandler;
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let url = Url::from_file_path(&file_path).unwrap();

        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
        };

        let footer = handler.read_parquet_footer(&file_meta).unwrap();

        // Verify field IDs are preserved
        let id_field = footer.schema.fields().find(|f| f.name() == "id").unwrap();
        assert_eq!(
            id_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".into())
        );

        let name_field = footer.schema.fields().find(|f| f.name() == "name").unwrap();
        assert_eq!(
            name_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2".into())
        );
    }
}
