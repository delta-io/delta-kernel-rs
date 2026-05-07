use std::sync::Arc;

use bytes::Bytes;
use url::Url;

use super::{get_bytes, read_files, resolve_scope};
use crate::engine::arrow_conversion::{TryFromArrow as _, TryIntoArrow as _};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{
    fixup_parquet_read, generate_mask, get_requested_indices, ordering_needs_row_indexes,
    RowIndexBuilder,
};
use crate::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use crate::engine::{reader_options, writer_options};
use crate::object_store::DynObjectStore;
// `ObjectStoreExt` is needed for `store.get()` etc. in arrow-58 mode where these methods moved
// off the `ObjectStore` trait. In arrow-57 mode the compat shim makes the import a no-op, so
// silence the resulting unused-import warning.
#[allow(unused_imports)]
use crate::object_store::ObjectStoreExt as _;
use crate::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::schema::{SchemaRef, StructType};
use crate::{
    DeltaResult, Error, FileDataReadResultIterator, FileMeta, ParquetFooter, ParquetHandler,
    PredicateRef,
};

pub(crate) struct SyncParquetHandler {
    store: Option<Arc<DynObjectStore>>,
}

impl SyncParquetHandler {
    pub(crate) fn new(store: Option<Arc<DynObjectStore>>) -> Self {
        Self { store }
    }
}

fn try_create_from_parquet(
    data: Bytes,
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    file_location: String,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);
    let reader_options = reader_options();
    let metadata = ArrowReaderMetadata::load(&data, reader_options.clone())?;
    let parquet_schema = metadata.schema();
    let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(data, reader_options)?;
    let (indices, requested_ordering) = get_requested_indices(&schema, parquet_schema)?;
    if let Some(mask) = generate_mask(&schema, parquet_schema, builder.parquet_schema(), &indices) {
        builder = builder.with_projection(mask);
    }

    let mut row_indexes = ordering_needs_row_indexes(&requested_ordering)
        .then(|| RowIndexBuilder::new(builder.metadata().row_groups()));

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
        read_files(
            self.store.as_ref(),
            files,
            schema,
            predicate,
            try_create_from_parquet,
        )
    }

    /// Writes engine data to a Parquet file at the specified location.
    ///
    /// Buffers the entire file in memory and `put`s it to the underlying [`ObjectStore`].
    /// If a file already exists at the given location, it will be overwritten.
    ///
    /// # Parameters
    ///
    /// - `location` - The full URL path where the Parquet file should be written (e.g. `file:///path/to/file.parquet`).
    /// - `data` - An iterator of engine data to be written to the Parquet file.
    fn write_parquet_file(
        &self,
        location: Url,
        mut data: Box<dyn Iterator<Item = DeltaResult<Box<dyn crate::EngineData>>> + Send>,
    ) -> DeltaResult<()> {
        let first_batch = data.next().ok_or_else(|| {
            crate::Error::generic("Cannot write parquet file with empty data iterator")
        })??;
        let first_arrow = ArrowEngineData::try_from_engine_data(first_batch)?;
        let first_record_batch: crate::arrow::array::RecordBatch = (*first_arrow).into();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new_with_options(
            &mut buf,
            first_record_batch.schema(),
            writer_options(),
        )?;
        writer.write(&first_record_batch)?;
        for result in data {
            let engine_data = result?;
            let arrow_data = ArrowEngineData::try_from_engine_data(engine_data)?;
            let batch: crate::arrow::array::RecordBatch = (*arrow_data).into();
            writer.write(&batch)?;
        }
        writer.close()?;

        // For local writes, ensure parent directories exist; `LocalFileSystem::put` does not
        // create them. No-op for non-file:// URLs. Must happen before `resolve_scope` so that
        // canonicalization of the parent succeeds.
        if location.scheme() == "file" {
            if let Ok(file_path) = location.to_file_path() {
                if let Some(parent) = file_path.parent() {
                    if !parent.exists() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
            }
        }

        let (store, _, object_path) = resolve_scope(self.store.as_ref(), &location)?;
        futures::executor::block_on(store.put(&object_path, buf.into()))?;
        Ok(())
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        let data = get_bytes(self.store.as_ref(), &file.location)?;
        let metadata = ArrowReaderMetadata::load(&data, reader_options())?;
        let schema = StructType::try_from_arrow(metadata.schema().as_ref())
            .map(Arc::new)
            .map_err(Error::Arrow)?;
        Ok(ParquetFooter { schema })
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use tempfile::tempdir;
    use url::Url;

    use super::*;
    use crate::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
    use crate::engine::arrow_conversion::TryIntoKernel as _;
    use crate::{DeltaResult, EngineData};

    fn test_data_iter() -> Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> {
        let engine_data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
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
        Box::new(std::iter::once(Ok(engine_data)))
    }

    #[test]
    fn test_sync_write_parquet_file() {
        let handler = SyncParquetHandler::new(None);
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.parquet");
        let url = Url::from_file_path(&file_path).unwrap();

        handler
            .write_parquet_file(url.clone(), test_data_iter())
            .unwrap();
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

        assert_eq!(record_batch.num_rows(), 3);
        assert_eq!(record_batch.num_columns(), 2);

        let id_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[1, 2, 3]);

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
    fn test_sync_write_parquet_file_multiple_batches() {
        let handler = SyncParquetHandler::new(None);
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_multi_batch.parquet");
        let url = Url::from_file_path(&file_path).unwrap();

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

        let batches = vec![Ok(batch1), Ok(batch2), Ok(batch3)];
        let data_iter: Box<
            dyn Iterator<Item = crate::DeltaResult<Box<dyn crate::EngineData>>> + Send,
        > = Box::new(batches.into_iter());

        handler.write_parquet_file(url.clone(), data_iter).unwrap();
        assert!(file_path.exists());

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
    fn write_parquet_creates_parent_directories() {
        let handler = SyncParquetHandler::new(None);
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("a/b/c/test.parquet");
        let url = Url::from_file_path(&file_path).unwrap();

        handler.write_parquet_file(url, test_data_iter()).unwrap();
        assert!(file_path.exists());
    }

    // === Contract tests (delegate to shared helpers in `engine::tests`) ===

    #[test]
    fn parquet_handler_reads_footer() {
        crate::engine::tests::test_parquet_handler_reads_footer(&SyncParquetHandler::new(None));
    }

    #[test]
    fn parquet_handler_footer_errors_on_missing_file() {
        crate::engine::tests::test_parquet_handler_footer_errors_on_missing_file(
            &SyncParquetHandler::new(None),
        );
    }

    #[test]
    fn parquet_handler_footer_preserves_field_ids() {
        crate::engine::tests::test_parquet_handler_footer_preserves_field_ids(
            &SyncParquetHandler::new(None),
        );
    }

    #[test]
    fn parquet_handler_write_always_overwrites() {
        crate::engine::tests::test_parquet_handler_write_always_overwrites(
            &SyncParquetHandler::new(None),
        );
    }

    #[test]
    fn parquet_handler_write_omits_arrow_schema() {
        crate::engine::tests::test_parquet_handler_write_omits_arrow_schema(
            &SyncParquetHandler::new(None),
        );
    }

    #[test]
    fn parquet_handler_reads_file_with_arrow_schema() {
        crate::engine::tests::test_parquet_handler_reads_file_with_arrow_schema(
            &SyncParquetHandler::new(None),
        );
    }

    /// Ensures `write_parquet_file` and `read_parquet_footer` work end-to-end with an
    /// `ObjectStore` backend. The local path is exercised by the other tests in this module.
    #[test]
    fn parquet_store_write_and_footer_roundtrip() {
        let store = Arc::new(crate::object_store::memory::InMemory::new());
        let handler = SyncParquetHandler::new(Some(store));
        let url = Url::parse("memory:///t/data.parquet").unwrap();

        handler
            .write_parquet_file(url.clone(), test_data_iter())
            .unwrap();

        let footer = handler
            .read_parquet_footer(&FileMeta {
                location: url,
                last_modified: 0,
                size: 0,
            })
            .unwrap();
        let field_names: Vec<_> = footer
            .schema
            .fields()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(field_names, vec!["id".to_string(), "name".to_string()]);
    }
}
