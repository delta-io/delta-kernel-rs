//! Default Parquet handler implementation

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use crate::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
use crate::arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
#[cfg(test)]
use crate::engine_data::FilteredEngineData;
use crate::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use futures::StreamExt;
use object_store::path::Path;
use object_store::DynObjectStore;
use uuid::Uuid;

use super::file_stream::{FileOpenFuture, FileOpener, FileStream};
use super::UrlExt;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{fixup_parquet_read, generate_mask, get_requested_indices};
use crate::engine::default::executor::TaskExecutor;
use crate::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, ExpressionRef, FileDataReadResultIterator, FileMeta,
    ParquetHandler,
};

#[derive(Debug)]
pub struct DefaultParquetHandler<E: TaskExecutor> {
    store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    readahead: usize,
}

/// Metadata of a data file (typically a parquet file), currently just includes the file metadata
/// but will expand to include file statistics and other metadata in the future.
#[derive(Debug)]
pub struct DataFileMetadata {
    file_meta: FileMeta,
}

impl DataFileMetadata {
    pub fn new(file_meta: FileMeta) -> Self {
        Self { file_meta }
    }

    // convert DataFileMetadata into a record batch which matches the 'write_metadata' schema
    fn as_record_batch(
        &self,
        partition_values: &HashMap<String, String>,
        data_change: bool,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let DataFileMetadata {
            file_meta:
                FileMeta {
                    location,
                    last_modified,
                    size,
                },
        } = self;
        let write_metadata_schema = crate::transaction::get_write_metadata_schema();

        // create the record batch of the write metadata
        let path = Arc::new(StringArray::from(vec![location.to_string()]));
        let key_builder = StringBuilder::new();
        let val_builder = StringBuilder::new();
        let names = MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut builder = MapBuilder::new(Some(names), key_builder, val_builder);
        for (k, v) in partition_values {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true).unwrap();
        let partitions = Arc::new(builder.finish());
        // this means max size we can write is i64::MAX (~8EB)
        let size: i64 = (*size)
            .try_into()
            .map_err(|_| Error::generic("Failed to convert parquet metadata 'size' to i64"))?;
        let size = Arc::new(Int64Array::from(vec![size]));
        let data_change = Arc::new(BooleanArray::from(vec![data_change]));
        let modification_time = Arc::new(Int64Array::from(vec![*last_modified]));
        Ok(Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(write_metadata_schema.as_ref().try_into()?),
            vec![path, partitions, size, modification_time, data_change],
        )?)))
    }
}

impl<E: TaskExecutor> DefaultParquetHandler<E> {
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            store,
            task_executor,
            readahead: 10,
        }
    }

    /// Max number of batches to read ahead while executing [Self::read_parquet_files()].
    ///
    /// Defaults to 10.
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.readahead = readahead;
        self
    }

    // Writes an iterator of `EngineData` batches to a parquet file at the specified path and
    // returns the parquet metadata as `DataFileMetadata`.
    //
    // Notes:
    // 1. After encoding the data as parquet, this issues a PUT followed by a HEAD to storage in
    // order to obtain metadata about the object just written.
    // 2. The schema of all batches must be the same.
    async fn write_parquet_from_batches(
        &self,
        path: url::Url,
        mut data_iter: impl Iterator<Item = DeltaResult<Box<dyn EngineData>>>,
    ) -> DeltaResult<DataFileMetadata> {
        // Get the first batch to extract schema
        let first_data = data_iter
            .next()
            .ok_or_else(|| Error::generic("No data to write"))??;
        let arrow_engine_data = ArrowEngineData::try_from_engine_data(first_data)?;
        let first_batch = arrow_engine_data.record_batch();
        let schema = first_batch.schema();

        // Setup buffer and writer with compression
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)?;

        // Write the batches to buffer
        writer.write(first_batch)?;
        for batch_result in data_iter {
            let data = batch_result?;
            let batch = ArrowEngineData::try_from_engine_data(data)?;
            let record_batch = batch.record_batch();
            writer.write(record_batch)?;
        }

        writer.close()?; // writer must be closed to write footer

        let size = buffer.len();
        let file_path = Path::from(path.path());
        self.store.put(&file_path, buffer.into()).await?;

        // Get the metadata of the written file
        let metadata = self.store.head(&file_path).await?;
        let modification_time = metadata.last_modified.timestamp_millis();
        if size != metadata.size {
            return Err(Error::generic(format!(
                "Size mismatch after writing parquet file: expected {}, got {}",
                size, metadata.size
            )));
        }

        let file_meta = FileMeta::new(path, modification_time, size);
        Ok(DataFileMetadata::new(file_meta))
    }

    // Write `data` to `{path}/<uuid>.parquet` as parquet using ArrowWriter and return the parquet
    // metadata (where `<uuid>` is a generated UUIDv4).
    //
    // This function is a convenience wrapper around `write_parquet_from_batches` that creates a
    // unique filename for the parquet file. The path must end with a trailing slash.
    async fn write_parquet(
        &self,
        path: &url::Url,
        data: Box<dyn EngineData>,
    ) -> DeltaResult<DataFileMetadata> {
        // fail if path does not end with a trailing slash
        if !path.path().ends_with('/') {
            return Err(Error::generic(format!(
                "Path must end with a trailing slash: {}",
                path
            )));
        }
        let name: String = format!("{}.parquet", Uuid::new_v4());
        let path = path.join(&name)?;
        self.write_parquet_from_batches(path, std::iter::once(Ok(data)))
            .await
    }

    /// Write `data` to `{path}/<uuid>.parquet` as parquet using ArrowWriter and return the parquet
    /// metadata as an EngineData batch which matches the [write metadata] schema (where `<uuid>` is
    /// a generated UUIDv4).
    ///
    /// [write metadata]: crate::transaction::get_write_metadata_schema
    pub async fn write_parquet_file(
        &self,
        path: &url::Url,
        data: Box<dyn EngineData>,
        partition_values: HashMap<String, String>,
        data_change: bool,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let parquet_metadata = self.write_parquet(path, data).await?;
        parquet_metadata.as_record_batch(&partition_values, data_change)
    }

    /// Writes filtered data to a parquet file at the specified path.
    ///
    /// This function takes an iterator of [`FilteredEngineData`] objects, each containing a batch of
    /// [`EngineData`] and a selection vector indicating which rows to keep. It filters each batch
    /// according to its selection vector and writes all retained rows to a single parquet file at the
    /// specified path.
    ///
    /// Note: The selection vector must match the number of rows in the corresponding batch.
    #[cfg(test)]
    async fn write_parquet_from_filtered_batches(
        &self,
        path: url::Url,
        filtered_data: impl Iterator<Item = FilteredEngineData>,
    ) -> DeltaResult<DataFileMetadata> {
        use arrow_53::compute::filter;
        use itertools::Itertools;

        // Process each `FilteredEngineData` item, transforming each into filtered `EngineData`
        let data = filtered_data.map(|batch| -> DeltaResult<Box<dyn EngineData>> {
            let arrow_engine_data = ArrowEngineData::try_from_engine_data(batch.data)?;
            let record_batch = arrow_engine_data.record_batch();

            // Check that the selection vector length matches the number of rows
            if batch.selection_vector.len() != record_batch.num_rows() {
                return Err(Error::generic(format!(
                    "Mask length ({}) doesn't match number of rows ({})",
                    batch.selection_vector.len(),
                    record_batch.num_rows()
                )));
            }

            // Create arrow array from selection vector
            let selection_array = Arc::new(BooleanArray::from(batch.selection_vector));

            // Filter each column in the record batch using the selection arrow array
            let filtered_columns: Vec<_> = record_batch
                .columns()
                .iter()
                .map(|col| {
                    filter(col.as_ref(), &selection_array)
                        .map_err(|e| Error::generic(format!("Error filtering column: {e}")))
                })
                .try_collect()?;

            // Create filtered record batch and convert back to EngineData
            let filtered_batch = RecordBatch::try_new(record_batch.schema(), filtered_columns)?;
            Ok(Box::new(ArrowEngineData::new(filtered_batch)))
        });

        // Write the filtered data to parquet
        self.write_parquet_from_batches(path, data).await
    }
}

impl<E: TaskExecutor> ParquetHandler for DefaultParquetHandler<E> {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // get the first FileMeta to decide how to fetch the file.
        // NB: This means that every file in `FileMeta` _must_ have the same scheme or things will break
        // s3://    -> aws   (ParquetOpener)
        // nothing  -> local (ParquetOpener)
        // https:// -> assume presigned URL (and fetch without object_store)
        //   -> reqwest to get data
        //   -> parse to parquet
        // SAFETY: we did is_empty check above, this is ok.
        let file_opener: Box<dyn FileOpener> = if files[0].location.is_presigned() {
            Box::new(PresignedUrlOpener::new(
                1024,
                physical_schema.clone(),
                predicate,
            ))
        } else {
            Box::new(ParquetOpener::new(
                1024,
                physical_schema.clone(),
                predicate,
                self.store.clone(),
            ))
        };
        FileStream::new_async_read_iterator(
            self.task_executor.clone(),
            Arc::new(physical_schema.as_ref().try_into()?),
            file_opener,
            files,
            self.readahead,
        )
    }
}

/// Implements [`FileOpener`] for a parquet file
struct ParquetOpener {
    // projection: Arc<[usize]>,
    batch_size: usize,
    table_schema: SchemaRef,
    predicate: Option<ExpressionRef>,
    limit: Option<usize>,
    store: Arc<DynObjectStore>,
}

impl ParquetOpener {
    pub(crate) fn new(
        batch_size: usize,
        table_schema: SchemaRef,
        predicate: Option<ExpressionRef>,
        store: Arc<DynObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            table_schema,
            predicate,
            limit: None,
            store,
        }
    }
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta, _range: Option<Range<i64>>) -> DeltaResult<FileOpenFuture> {
        let path = Path::from_url_path(file_meta.location.path())?;
        let store = self.store.clone();

        let batch_size = self.batch_size;
        // let projection = self.projection.clone();
        let table_schema = self.table_schema.clone();
        let predicate = self.predicate.clone();
        let limit = self.limit;

        Ok(Box::pin(async move {
            // TODO avoid IO by converting passed file meta to ObjectMeta
            let meta = store.head(&path).await?;
            let mut reader = ParquetObjectReader::new(store, meta);
            let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
            let parquet_schema = metadata.schema();
            let (indices, requested_ordering) =
                get_requested_indices(&table_schema, parquet_schema)?;
            let options = ArrowReaderOptions::new(); //.with_page_index(enable_page_index);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;
            if let Some(mask) = generate_mask(
                &table_schema,
                parquet_schema,
                builder.parquet_schema(),
                &indices,
            ) {
                builder = builder.with_projection(mask)
            }

            if let Some(ref predicate) = predicate {
                builder = builder.with_row_group_filter(predicate);
            }
            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder.with_batch_size(batch_size).build()?;

            let stream = stream.map(move |rbr| fixup_parquet_read(rbr?, &requested_ordering));
            Ok(stream.boxed())
        }))
    }
}

/// Implements [`FileOpener`] for a opening a parquet file from a presigned URL
struct PresignedUrlOpener {
    batch_size: usize,
    predicate: Option<ExpressionRef>,
    limit: Option<usize>,
    table_schema: SchemaRef,
    client: reqwest::Client,
}

impl PresignedUrlOpener {
    pub(crate) fn new(
        batch_size: usize,
        schema: SchemaRef,
        predicate: Option<ExpressionRef>,
    ) -> Self {
        Self {
            batch_size,
            table_schema: schema,
            predicate,
            limit: None,
            client: reqwest::Client::new(),
        }
    }
}

impl FileOpener for PresignedUrlOpener {
    fn open(&self, file_meta: FileMeta, _range: Option<Range<i64>>) -> DeltaResult<FileOpenFuture> {
        let batch_size = self.batch_size;
        let table_schema = self.table_schema.clone();
        let predicate = self.predicate.clone();
        let limit = self.limit;
        let client = self.client.clone(); // uses Arc internally according to reqwest docs

        Ok(Box::pin(async move {
            // fetch the file from the interweb
            let reader = client.get(file_meta.location).send().await?.bytes().await?;
            let metadata = ArrowReaderMetadata::load(&reader, Default::default())?;
            let parquet_schema = metadata.schema();
            let (indices, requested_ordering) =
                get_requested_indices(&table_schema, parquet_schema)?;

            let options = ArrowReaderOptions::new();
            let mut builder =
                ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)?;
            if let Some(mask) = generate_mask(
                &table_schema,
                parquet_schema,
                builder.parquet_schema(),
                &indices,
            ) {
                builder = builder.with_projection(mask)
            }

            if let Some(ref predicate) = predicate {
                builder = builder.with_row_group_filter(predicate);
            }
            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let reader = builder.with_batch_size(batch_size).build()?;

            let stream = futures::stream::iter(reader);
            let stream = stream.map(move |rbr| fixup_parquet_read(rbr?, &requested_ordering));
            Ok(stream.boxed())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::arrow::array::{Array, RecordBatch};
    use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
    use url::Url;

    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::EngineData;

    use itertools::Itertools;

    use super::*;

    fn into_record_batch(
        engine_data: DeltaResult<Box<dyn EngineData>>,
    ) -> DeltaResult<RecordBatch> {
        engine_data
            .and_then(ArrowEngineData::try_from_engine_data)
            .map(Into::into)
    }

    fn make_batch(ids: Vec<i64>, names: Vec<&str>) -> Box<dyn EngineData> {
        Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![
                ("id", Arc::new(Int64Array::from(ids)) as Arc<dyn Array>),
                ("name", Arc::new(StringArray::from(names)) as Arc<dyn Array>),
            ])
            .unwrap(),
        ))
    }

    #[tokio::test]
    async fn test_read_parquet_files() {
        let store = Arc::new(LocalFileSystem::new());

        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
        )).unwrap();
        let url = url::Url::from_file_path(path).unwrap();
        let location = Path::from(url.path());
        let meta = store.head(&location).await.unwrap();

        let reader = ParquetObjectReader::new(store.clone(), meta.clone());
        let physical_schema = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .schema()
            .clone();

        let files = &[FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp(),
            size: meta.size,
        }];

        let handler = DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
        let data: Vec<RecordBatch> = handler
            .read_parquet_files(files, Arc::new(physical_schema.try_into().unwrap()), None)
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 10);
    }

    #[test]
    fn test_as_record_batch() {
        let location = Url::parse("file:///test_url").unwrap();
        let size = 1_000_000;
        let last_modified = 10000000000;
        let file_metadata = FileMeta::new(location.clone(), last_modified, size as usize);
        let data_file_metadata = DataFileMetadata::new(file_metadata);
        let partition_values = HashMap::from([("partition1".to_string(), "a".to_string())]);
        let data_change = true;
        let actual = data_file_metadata
            .as_record_batch(&partition_values, data_change)
            .unwrap();
        let actual = ArrowEngineData::try_from_engine_data(actual).unwrap();

        let schema = Arc::new(
            crate::transaction::get_write_metadata_schema()
                .as_ref()
                .try_into()
                .unwrap(),
        );
        let key_builder = StringBuilder::new();
        let val_builder = StringBuilder::new();
        let mut partition_values_builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "key_value".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            key_builder,
            val_builder,
        );
        partition_values_builder.keys().append_value("partition1");
        partition_values_builder.values().append_value("a");
        partition_values_builder.append(true).unwrap();
        let partition_values = partition_values_builder.finish();
        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![location.to_string()])),
                Arc::new(partition_values),
                Arc::new(Int64Array::from(vec![size])),
                Arc::new(Int64Array::from(vec![last_modified])),
                Arc::new(BooleanArray::from(vec![data_change])),
            ],
        )
        .unwrap();

        assert_eq!(actual.record_batch(), &expected);
    }

    #[tokio::test]
    async fn test_write_parquet() {
        let store = Arc::new(InMemory::new());
        let parquet_handler =
            DefaultParquetHandler::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        let data = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);

        let write_metadata = parquet_handler
            .write_parquet(&Url::parse("memory:///data/").unwrap(), data)
            .await
            .unwrap();

        let DataFileMetadata {
            file_meta:
                ref parquet_file @ FileMeta {
                    ref location,
                    last_modified,
                    size,
                },
        } = write_metadata;
        let expected_location = Url::parse("memory:///data/").unwrap();

        // head the object to get metadata
        let meta = store.head(&Path::from(location.path())).await.unwrap();
        let expected_size = meta.size;

        // check that last_modified is within 10s of now
        let now: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap();

        let filename = location.path().split('/').next_back().unwrap();
        assert_eq!(&expected_location.join(filename).unwrap(), location);
        assert_eq!(expected_size, size);
        assert!(now - last_modified < 10_000);

        // check we can read back
        let path = Path::from(location.path());
        let meta = store.head(&path).await.unwrap();
        let reader = ParquetObjectReader::new(store.clone(), meta.clone());
        let physical_schema = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .schema()
            .clone();

        let data: Vec<RecordBatch> = parquet_handler
            .read_parquet_files(
                &[parquet_file.clone()],
                Arc::new(physical_schema.try_into().unwrap()),
                None,
            )
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_disallow_non_trailing_slash() {
        let store = Arc::new(InMemory::new());
        let parquet_handler =
            DefaultParquetHandler::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        let data = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "a",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));

        assert!(parquet_handler
            .write_parquet(&Url::parse("memory:///data").unwrap(), data)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_write_multiple_filtered_parquet() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let parquet_handler =
            DefaultParquetHandler::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        let filtered_data = vec![
            FilteredEngineData::new(
                make_batch(vec![1, 2, 3], vec!["a", "b", "c"]),
                vec![true, false, true], // Keep rows 0 and 2 from batch1
            ),
            FilteredEngineData::new(
                make_batch(vec![4, 5, 6], vec!["d", "e", "f"]),
                vec![false, true, false], // Keep row 1 from batch2
            ),
        ];

        // Write the filtered data
        let file_meta = parquet_handler
            .write_parquet_from_filtered_batches(
                Url::parse("memory:///multiple_filtered_data/")?,
                filtered_data.into_iter(),
            )
            .await?;

        // Read back the written data
        let schema = Arc::new(
            RecordBatch::try_from_iter(vec![
                ("id", Arc::new(Int64Array::from(vec![0])) as Arc<dyn Array>),
                (
                    "name",
                    Arc::new(StringArray::from(vec![""])) as Arc<dyn Array>,
                ),
            ])?
            .schema()
            .try_into()?,
        );

        let data: Vec<RecordBatch> = parquet_handler
            .read_parquet_files(&[file_meta.file_meta.clone()], schema, None)?
            .map(into_record_batch)
            .try_collect()?;

        // Verify that we have the expected number of results
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 3);

        // Get the values from the returned arrays
        let ids = data[0].column(0).as_any().downcast_ref::<Int64Array>();
        let names = data[0].column(1).as_any().downcast_ref::<StringArray>();

        // Verify the combined values
        assert_eq!(
            ids.unwrap().values(),
            &[1, 3, 5],
            "Should have the expected ID values"
        );
        assert_eq!(
            names.unwrap().iter().collect::<Vec<_>>(),
            vec![Some("a"), Some("c"), Some("e")],
            "Should have the expected name values"
        );

        Ok(())
    }
}
