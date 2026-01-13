//! Default Parquet handler implementation

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
use crate::arrow::array::{Int64Array, RecordBatch, StringArray, StructArray};
use crate::arrow::datatypes::{DataType, Field};
use crate::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use crate::parquet::arrow::async_writer::AsyncArrowWriter;
use crate::parquet::arrow::async_writer::ParquetObjectWriter;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore};
use uuid::Uuid;

use super::file_stream::{FileOpenFuture, FileOpener, FileStream};
use super::UrlExt;
use crate::engine::arrow_conversion::{TryFromArrow as _, TryIntoArrow as _};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{
    fixup_parquet_read, generate_mask, get_requested_indices, ordering_needs_row_indexes,
    RowIndexBuilder,
};
use crate::engine::default::executor::TaskExecutor;
use crate::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use crate::schema::{SchemaRef, StructType};
use crate::{
    DeltaResult, EngineData, Error, FileDataReadResultIterator, FileMeta, ParquetFooter,
    ParquetHandler, PredicateRef,
};

#[derive(Debug)]
pub struct DefaultParquetHandler<E: TaskExecutor> {
    store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    readahead: usize,
}

/// Metadata of a data file (typically a parquet file).
///
/// Currently just includes the the number of records as statistics, but will expand to include
/// more statistics and other metadata in the future.
#[derive(Debug)]
pub struct DataFileMetadata {
    file_meta: FileMeta,
    // NB: We use usize instead of u64 since arrow uses usize for record batch sizes
    num_records: usize,
}

impl DataFileMetadata {
    pub fn new(file_meta: FileMeta, num_records: usize) -> Self {
        Self {
            file_meta,
            num_records,
        }
    }

    /// Convert DataFileMetadata into a record batch which matches the schema returned by
    /// [`add_files_schema`].
    ///
    /// [`add_files_schema`]: crate::transaction::Transaction::add_files_schema
    #[internal_api]
    pub(crate) fn as_record_batch(
        &self,
        partition_values: &HashMap<String, String>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let DataFileMetadata {
            file_meta:
                FileMeta {
                    location,
                    last_modified,
                    size,
                },
            num_records,
        } = self;
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
        builder.append(true)?;
        let partitions = Arc::new(builder.finish());
        // this means max size we can write is i64::MAX (~8EB)
        let size: i64 = (*size)
            .try_into()
            .map_err(|_| Error::generic("Failed to convert parquet metadata 'size' to i64"))?;
        let size = Arc::new(Int64Array::from(vec![size]));
        let modification_time = Arc::new(Int64Array::from(vec![*last_modified]));
        let stats = Arc::new(StructArray::try_new_with_length(
            vec![Field::new("numRecords", DataType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![*num_records as i64]))],
            None,
            1,
        )?);

        Ok(Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(
                crate::transaction::BASE_ADD_FILES_SCHEMA
                    .as_ref()
                    .try_into_arrow()?,
            ),
            vec![path, partitions, size, modification_time, stats],
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

    // Write `data` to `{path}/<uuid>.parquet` as parquet using ArrowWriter and return the parquet
    // metadata (where `<uuid>` is a generated UUIDv4).
    //
    // Note: after encoding the data as parquet, this issues a PUT followed by a HEAD to storage in
    // order to obtain metadata about the object just written.
    async fn write_parquet(
        &self,
        path: &url::Url,
        data: Box<dyn EngineData>,
    ) -> DeltaResult<DataFileMetadata> {
        let batch: Box<_> = ArrowEngineData::try_from_engine_data(data)?;
        let record_batch = batch.record_batch();
        let num_records = record_batch.num_rows();

        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
        writer.write(record_batch)?;
        writer.close()?; // writer must be closed to write footer

        let size: u64 = buffer
            .len()
            .try_into()
            .map_err(|_| Error::generic("unable to convert usize to u64"))?;
        let name: String = format!("{}.parquet", Uuid::new_v4());
        // fail if path does not end with a trailing slash
        if !path.path().ends_with('/') {
            return Err(Error::generic(format!(
                "Path must end with a trailing slash: {path}"
            )));
        }
        let path = path.join(&name)?;

        self.store
            .put(&Path::from_url_path(path.path())?, buffer.into())
            .await?;

        let metadata = self.store.head(&Path::from_url_path(path.path())?).await?;
        let modification_time = metadata.last_modified.timestamp_millis();
        if size != metadata.size {
            return Err(Error::generic(format!(
                "Size mismatch after writing parquet file: expected {}, got {}",
                size, metadata.size
            )));
        }

        let file_meta = FileMeta::new(path, modification_time, size);
        Ok(DataFileMetadata::new(file_meta, num_records))
    }

    /// Write `data` to `{path}/<uuid>.parquet` as parquet using ArrowWriter and return the parquet
    /// metadata as an EngineData batch which matches the [add file metadata] schema (where `<uuid>`
    /// is a generated UUIDv4).
    ///
    /// Note that the schema does not contain the dataChange column. In order to set `data_change` flag,
    /// use [`crate::transaction::Transaction::with_data_change`].
    ///
    /// [add file metadata]: crate::transaction::Transaction::add_files_schema
    pub async fn write_parquet_file(
        &self,
        path: &url::Url,
        data: Box<dyn EngineData>,
        partition_values: HashMap<String, String>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let parquet_metadata = self.write_parquet(path, data).await?;
        parquet_metadata.as_record_batch(&partition_values)
    }
}

/// Internal async implementation of read_parquet_files
async fn read_parquet_files_impl(
    store: Arc<DynObjectStore>,
    files: Vec<FileMeta>,
    physical_schema: SchemaRef,
    predicate: Option<PredicateRef>,
) -> DeltaResult<BoxStream<'static, DeltaResult<Box<dyn EngineData>>>> {
    if files.is_empty() {
        return Ok(Box::pin(stream::empty()));
    }

    let arrow_schema = Arc::new(physical_schema.as_ref().try_into_arrow()?);

    // get the first FileMeta to decide how to fetch the file.
    // NB: This means that every file in `FileMeta` _must_ have the same scheme or things will break
    // s3://    -> aws   (ParquetOpener)
    // nothing  -> local (ParquetOpener)
    // https:// -> assume presigned URL (and fetch without object_store)
    //   -> reqwest to get data
    //   -> parse to parquet
    // SAFETY: we did is_empty check above, this is ok.
    if files[0].location.is_presigned() {
        let file_opener = Box::new(PresignedUrlOpener::new(
            1024,
            physical_schema.clone(),
            predicate,
        ));
        let stream = FileStream::new(files, arrow_schema, file_opener)?.map_ok(
            |record_batch| -> Box<dyn EngineData> { Box::new(ArrowEngineData::new(record_batch)) },
        );
        return Ok(Box::pin(stream));
    }

    // an iterator of futures that open each file
    let file_futures = files.into_iter().map(move |file| {
        let store = store.clone();
        let schema = physical_schema.clone();
        let predicate = predicate.clone();
        async move {
            open_parquet_file(
                store,
                schema,
                predicate,
                None,
                super::DEFAULT_BATCH_SIZE,
                file,
            )
            .await
        }
    });
    // create a stream from that iterator which buffers up to `buffer_size` futures at a time
    let result_stream = stream::iter(file_futures)
        .buffered(super::DEFAULT_BUFFER_SIZE)
        .try_flatten()
        .map_ok(|record_batch| -> Box<dyn EngineData> {
            Box::new(ArrowEngineData::new(record_batch))
        });

    Ok(Box::pin(result_stream))
}

impl<E: TaskExecutor> ParquetHandler for DefaultParquetHandler<E> {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let future = read_parquet_files_impl(
            self.store.clone(),
            files.to_vec(),
            physical_schema,
            predicate,
        );
        super::stream_future_to_iter(self.task_executor.clone(), future)
    }

    /// Writes engine data to a Parquet file at the specified location.
    ///
    /// This implementation uses asynchronous file I/O with object_store to write the Parquet file.
    /// If a file already exists at the given location, it will be overwritten.
    ///
    /// # Parameters
    ///
    /// - `location` - The full URL path where the Parquet file should be written
    ///   (e.g., `s3://bucket/path/file.parquet`, `file:///path/to/file.parquet`).
    /// - `data` - An iterator of engine data to be written to the Parquet file.
    ///
    /// # Returns
    ///
    /// A [`DeltaResult`] indicating success or failure.
    fn write_parquet_file(
        &self,
        location: url::Url,
        mut data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
    ) -> DeltaResult<()> {
        let store = self.store.clone();

        self.task_executor.block_on(async move {
            let path = Path::from_url_path(location.path())?;

            // Get first batch to initialize writer with schema
            let first_batch = data.next().ok_or_else(|| {
                Error::generic("Cannot write parquet file with empty data iterator")
            })??;
            let first_arrow = ArrowEngineData::try_from_engine_data(first_batch)?;
            let first_record_batch: RecordBatch = (*first_arrow).into();

            let object_writer = ParquetObjectWriter::new(store, path);
            let schema = first_record_batch.schema();
            let mut writer = AsyncArrowWriter::try_new(object_writer, schema, None)?;

            // Write the first batch
            writer.write(&first_record_batch).await?;

            // Write remaining batches
            for result in data {
                let engine_data = result?;
                let arrow_data = ArrowEngineData::try_from_engine_data(engine_data)?;
                let batch: RecordBatch = (*arrow_data).into();
                writer.write(&batch).await?;
            }

            writer.finish().await?;

            Ok(())
        })
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        let store = self.store.clone();
        let location = file.location.clone();
        let file_size = file.size;

        self.task_executor.block_on(async move {
            let metadata = if location.is_presigned() {
                let client = reqwest::Client::new();
                let response =
                    client.get(location.as_str()).send().await.map_err(|e| {
                        Error::generic(format!("Failed to fetch presigned URL: {}", e))
                    })?;
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| Error::generic(format!("Failed to read response bytes: {}", e)))?;
                ArrowReaderMetadata::load(&bytes, Default::default())?
            } else {
                let path = Path::from_url_path(location.path())?;
                let mut reader = ParquetObjectReader::new(store, path).with_file_size(file_size);
                ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?
            };

            let schema = StructType::try_from_arrow(metadata.schema().as_ref())
                .map(Arc::new)
                .map_err(Error::Arrow)?;
            Ok(ParquetFooter { schema })
        })
    }
}

/// Opens a Parquet file and returns a stream of record batches
async fn open_parquet_file(
    store: Arc<DynObjectStore>,
    table_schema: SchemaRef,
    predicate: Option<PredicateRef>,
    limit: Option<usize>,
    batch_size: usize,
    file_meta: FileMeta,
) -> DeltaResult<BoxStream<'static, DeltaResult<RecordBatch>>> {
    let file_location = file_meta.location.to_string();
    let path = Path::from_url_path(file_meta.location.path())?;

    let mut reader = {
        use object_store::ObjectStoreScheme;
        // HACK: unfortunately, `ParquetObjectReader` under the hood does a suffix range
        // request which isn't supported by Azure. For now we just detect if the URL is
        // pointing to azure and if so, do a HEAD request so we can pass in file size to the
        // reader which will cause the reader to avoid a suffix range request.
        // see also: https://github.com/delta-io/delta-kernel-rs/issues/968
        //
        // TODO(#1010): Note that we don't need this at all and can actually just _always_
        // do the `with_file_size` but need to (1) update our unit tests which often
        // hardcode size=0 and (2) update CDF execute which also hardcodes size=0.
        if let Ok((ObjectStoreScheme::MicrosoftAzure, _)) =
            ObjectStoreScheme::parse(&file_meta.location)
        {
            // also note doing HEAD then actual GET isn't atomic, and leaves us vulnerable
            // to file changing between the two calls.
            let meta = store.head(&path).await?;
            ParquetObjectReader::new(store, path).with_file_size(meta.size)
        } else {
            ParquetObjectReader::new(store, path)
        }
    };

    let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
    let parquet_schema = metadata.schema();
    let (indices, requested_ordering) = get_requested_indices(&table_schema, parquet_schema)?;
    let options = ArrowReaderOptions::new(); //.with_page_index(enable_page_index);
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;
    if let Some(mask) = generate_mask(
        &table_schema,
        parquet_schema,
        builder.parquet_schema(),
        &indices,
    ) {
        builder = builder.with_projection(mask)
    }

    // Only create RowIndexBuilder if row indexes are actually needed
    let mut row_indexes = ordering_needs_row_indexes(&requested_ordering)
        .then(|| RowIndexBuilder::new(builder.metadata().row_groups()));

    // Filter row groups and row indexes if a predicate is provided
    if let Some(ref predicate) = predicate {
        builder = builder.with_row_group_filter(predicate, row_indexes.as_mut());
    }
    if let Some(limit) = limit {
        builder = builder.with_limit(limit)
    }

    let mut row_indexes = row_indexes.map(|rb| rb.build()).transpose()?;
    let stream = builder.with_batch_size(batch_size).build()?;

    let stream = stream.map(move |rbr| {
        fixup_parquet_read(
            rbr?,
            &requested_ordering,
            row_indexes.as_mut(),
            Some(&file_location),
        )
    });
    Ok(stream.boxed())
}

/// Implements [`FileOpener`] for a opening a parquet file from a presigned URL
struct PresignedUrlOpener {
    batch_size: usize,
    predicate: Option<PredicateRef>,
    limit: Option<usize>,
    table_schema: SchemaRef,
    client: reqwest::Client,
}

impl PresignedUrlOpener {
    pub(crate) fn new(
        batch_size: usize,
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
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
        let file_location = file_meta.location.to_string();

        Ok(Box::pin(async move {
            // fetch the file from the interweb
            let reader = client.get(&file_location).send().await?.bytes().await?;
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

            // Only create RowIndexBuilder if row indexes are actually needed
            let mut row_indexes = ordering_needs_row_indexes(&requested_ordering)
                .then(|| RowIndexBuilder::new(builder.metadata().row_groups()));

            // Filter row groups and row indexes if a predicate is provided
            if let Some(ref predicate) = predicate {
                builder = builder.with_row_group_filter(predicate, row_indexes.as_mut());
            }
            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let reader = builder.with_batch_size(batch_size).build()?;

            let mut row_indexes = row_indexes.map(|rb| rb.build()).transpose()?;
            let stream = futures::stream::iter(reader);
            let stream = stream.map(move |rbr| {
                fixup_parquet_read(
                    rbr?,
                    &requested_ordering,
                    row_indexes.as_mut(),
                    Some(&file_location),
                )
            });
            Ok(stream.boxed())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::slice;

    use crate::arrow::array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int8Array, RecordBatch, TimestampMicrosecondArray,
    };
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::engine::arrow_conversion::TryIntoKernel as _;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use crate::EngineData;

    use itertools::Itertools;
    use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
    use url::Url;

    use crate::utils::current_time_ms;
    use crate::utils::test_utils::assert_result_error_with_message;

    use super::*;

    fn into_record_batch(
        engine_data: DeltaResult<Box<dyn EngineData>>,
    ) -> DeltaResult<RecordBatch> {
        engine_data
            .and_then(ArrowEngineData::try_from_engine_data)
            .map(Into::into)
    }

    #[tokio::test]
    async fn test_read_parquet_files() {
        let store = Arc::new(LocalFileSystem::new());

        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
        )).unwrap();
        let url = url::Url::from_file_path(path).unwrap();
        let location = Path::from_url_path(url.path()).unwrap();
        let meta = store.head(&location).await.unwrap();

        let reader = ParquetObjectReader::new(store.clone(), location);
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
            .read_parquet_files(
                files,
                Arc::new(physical_schema.try_into_kernel().unwrap()),
                None,
            )
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
        let num_records = 10;
        let file_metadata = FileMeta::new(location.clone(), last_modified, size);
        let data_file_metadata = DataFileMetadata::new(file_metadata, num_records);
        let partition_values = HashMap::from([("partition1".to_string(), "a".to_string())]);
        let actual = data_file_metadata
            .as_record_batch(&partition_values)
            .unwrap();
        let actual = ArrowEngineData::try_from_engine_data(actual).unwrap();

        let schema = Arc::new(
            crate::transaction::BASE_ADD_FILES_SCHEMA
                .as_ref()
                .try_into_arrow()
                .unwrap(),
        );
        let mut partition_values_builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "key_value".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        );
        partition_values_builder.keys().append_value("partition1");
        partition_values_builder.values().append_value("a");
        partition_values_builder.append(true).unwrap();
        let partition_values = partition_values_builder.finish();
        let stats_struct = StructArray::try_new_with_length(
            vec![Field::new("numRecords", ArrowDataType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![num_records as i64]))],
            None,
            1,
        )
        .unwrap();

        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![location.to_string()])),
                Arc::new(partition_values),
                Arc::new(Int64Array::from(vec![size as i64])),
                Arc::new(Int64Array::from(vec![last_modified])),
                Arc::new(stats_struct),
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

        let data = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "a",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));

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
            num_records,
        } = write_metadata;
        let expected_location = Url::parse("memory:///data/").unwrap();

        // head the object to get metadata
        let meta = store
            .head(&Path::from_url_path(location.path()).unwrap())
            .await
            .unwrap();
        let expected_size = meta.size;

        // check that last_modified is within 10s of now
        let now: i64 = current_time_ms().unwrap();

        let filename = location.path().split('/').next_back().unwrap();
        assert_eq!(&expected_location.join(filename).unwrap(), location);
        assert_eq!(expected_size, size);
        assert!(now - last_modified < 10_000);
        assert_eq!(num_records, 3);

        // check we can read back
        let path = Path::from_url_path(location.path()).unwrap();
        let reader = ParquetObjectReader::new(store.clone(), path);
        let physical_schema = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .schema()
            .clone();

        let data: Vec<RecordBatch> = parquet_handler
            .read_parquet_files(
                slice::from_ref(parquet_file),
                Arc::new(physical_schema.try_into_kernel().unwrap()),
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

        assert_result_error_with_message(
            parquet_handler
                .write_parquet(&Url::parse("memory:///data").unwrap(), data)
                .await,
            "Generic delta kernel error: Path must end with a trailing slash: memory:///data",
        );
    }

    #[tokio::test]
    async fn test_parquet_handler_trait_write() {
        let store = Arc::new(InMemory::new());
        let parquet_handler: Arc<dyn ParquetHandler> = Arc::new(DefaultParquetHandler::new(
            store.clone(),
            Arc::new(TokioBackgroundExecutor::new()),
        ));

        let engine_data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![
                (
                    "x",
                    Arc::new(Int64Array::from(vec![10, 20, 30])) as Arc<dyn Array>,
                ),
                (
                    "y",
                    Arc::new(Int64Array::from(vec![100, 200, 300])) as Arc<dyn Array>,
                ),
            ])
            .unwrap(),
        ));

        // Create iterator with single batch
        let data_iter: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::once(Ok(engine_data)));

        // Test writing through the trait method
        let file_url = Url::parse("memory:///test/data.parquet").unwrap();
        parquet_handler
            .write_parquet_file(file_url.clone(), data_iter)
            .unwrap();

        // Verify we can read the file back
        let path = Path::from_url_path(file_url.path()).unwrap();
        let reader = ParquetObjectReader::new(store.clone(), path);
        let physical_schema = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .schema()
            .clone();

        let file_meta = FileMeta {
            location: file_url,
            last_modified: 0,
            size: 0,
        };

        let data: Vec<RecordBatch> = parquet_handler
            .read_parquet_files(
                slice::from_ref(&file_meta),
                Arc::new(physical_schema.try_into_kernel().unwrap()),
                None,
            )
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 3);
        assert_eq!(data[0].num_columns(), 2);
    }

    #[test]
    fn test_read_parquet_footer() {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet",
        ))
        .unwrap();
        let file_size = std::fs::metadata(&path).unwrap().len();
        let url = Url::from_file_path(path).unwrap();

        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
        };

        let footer = handler.read_parquet_footer(&file_meta).unwrap();
        crate::utils::test_utils::validate_checkpoint_schema(&footer.schema);
    }

    #[test]
    fn test_read_parquet_footer_invalid_file() {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

        let mut temp_path = std::env::temp_dir();
        temp_path.push("non_existent_file_for_test.parquet");
        let url = Url::from_file_path(temp_path).unwrap();
        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: 0,
        };

        let result = handler.read_parquet_footer(&file_meta);
        assert!(result.is_err(), "Should error on non-existent file");
    }

    #[tokio::test]
    async fn test_parquet_handler_trait_write_and_read_roundtrip() {
        let store = Arc::new(InMemory::new());
        let parquet_handler: Arc<dyn ParquetHandler> = Arc::new(DefaultParquetHandler::new(
            store.clone(),
            Arc::new(TokioBackgroundExecutor::new()),
        ));

        // Create test data with all Delta-supported primitive types
        let engine_data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![
                // Byte (i8)
                (
                    "byte_col",
                    Arc::new(Int8Array::from(vec![1i8, 2, 3, 4, 5])) as Arc<dyn Array>,
                ),
                // Short (i16)
                (
                    "short_col",
                    Arc::new(Int16Array::from(vec![100i16, 200, 300, 400, 500])) as Arc<dyn Array>,
                ),
                // Integer (i32)
                (
                    "int_col",
                    Arc::new(Int32Array::from(vec![1000i32, 2000, 3000, 4000, 5000]))
                        as Arc<dyn Array>,
                ),
                // Long (i64)
                (
                    "long_col",
                    Arc::new(Int64Array::from(vec![10000i64, 20000, 30000, 40000, 50000]))
                        as Arc<dyn Array>,
                ),
                // Float (f32)
                (
                    "float_col",
                    Arc::new(Float32Array::from(vec![1.1f32, 2.2, 3.3, 4.4, 5.5]))
                        as Arc<dyn Array>,
                ),
                // Double (f64)
                (
                    "double_col",
                    Arc::new(Float64Array::from(vec![1.11f64, 2.22, 3.33, 4.44, 5.55]))
                        as Arc<dyn Array>,
                ),
                // Boolean
                (
                    "bool_col",
                    Arc::new(BooleanArray::from(vec![true, false, true, false, true]))
                        as Arc<dyn Array>,
                ),
                // String
                (
                    "string_col",
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as Arc<dyn Array>,
                ),
                // Binary
                (
                    "binary_col",
                    Arc::new(BinaryArray::from_vec(vec![
                        b"bin1", b"bin2", b"bin3", b"bin4", b"bin5",
                    ])) as Arc<dyn Array>,
                ),
                // Date
                (
                    "date_col",
                    Arc::new(Date32Array::from(vec![18262, 18263, 18264, 18265, 18266]))
                        as Arc<dyn Array>, // Days since epoch (2020-01-01 onwards)
                ),
                // Timestamp (with UTC timezone)
                (
                    "timestamp_col",
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![
                            1609459200000000i64, // 2021-01-01 00:00:00 UTC
                            1609545600000000i64,
                            1609632000000000i64,
                            1609718400000000i64,
                            1609804800000000i64,
                        ])
                        .with_timezone("UTC"),
                    ) as Arc<dyn Array>,
                ),
                // TimestampNtz (without timezone)
                (
                    "timestamp_ntz_col",
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        1609459200000000i64, // 2021-01-01 00:00:00
                        1609545600000000i64,
                        1609632000000000i64,
                        1609718400000000i64,
                        1609804800000000i64,
                    ])) as Arc<dyn Array>,
                ),
                // Decimal (precision 10, scale 2)
                (
                    "decimal_col",
                    Arc::new(
                        Decimal128Array::from(vec![12345i128, 23456, 34567, 45678, 56789])
                            .with_precision_and_scale(10, 2)
                            .unwrap(),
                    ) as Arc<dyn Array>,
                ),
            ])
            .unwrap(),
        ));

        // Create iterator with single batch
        let data_iter: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::once(Ok(engine_data)));

        // Write the data
        let file_url = Url::parse("memory:///roundtrip/test.parquet").unwrap();
        parquet_handler
            .write_parquet_file(file_url.clone(), data_iter)
            .unwrap();

        // Read it back
        let path = Path::from_url_path(file_url.path()).unwrap();
        let reader = ParquetObjectReader::new(store.clone(), path);
        let physical_schema = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .schema()
            .clone();

        let file_meta = FileMeta {
            location: file_url.clone(),
            last_modified: 0,
            size: 0,
        };

        let data: Vec<RecordBatch> = parquet_handler
            .read_parquet_files(
                slice::from_ref(&file_meta),
                Arc::new(physical_schema.try_into_kernel().unwrap()),
                None,
            )
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .unwrap();

        // Verify the data
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 5);
        assert_eq!(data[0].num_columns(), 13);

        let mut col_idx = 0;

        // Verify byte column
        let byte_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(byte_col.values(), &[1i8, 2, 3, 4, 5]);
        col_idx += 1;

        // Verify short column
        let short_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(short_col.values(), &[100i16, 200, 300, 400, 500]);
        col_idx += 1;

        // Verify int column
        let int_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int_col.values(), &[1000i32, 2000, 3000, 4000, 5000]);
        col_idx += 1;

        // Verify long column
        let long_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(long_col.values(), &[10000i64, 20000, 30000, 40000, 50000]);
        col_idx += 1;

        // Verify float column
        let float_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(float_col.values(), &[1.1f32, 2.2, 3.3, 4.4, 5.5]);
        col_idx += 1;

        // Verify double column
        let double_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(double_col.values(), &[1.11f64, 2.22, 3.33, 4.44, 5.55]);
        col_idx += 1;

        // Verify bool column
        let bool_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bool_col.value(0));
        assert!(!bool_col.value(1));
        col_idx += 1;

        // Verify string column
        let string_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(string_col.value(0), "a");
        assert_eq!(string_col.value(4), "e");
        col_idx += 1;

        // Verify binary column
        let binary_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(binary_col.value(0), b"bin1");
        assert_eq!(binary_col.value(4), b"bin5");
        col_idx += 1;

        // Verify date column
        let date_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(date_col.values(), &[18262, 18263, 18264, 18265, 18266]);
        col_idx += 1;

        // Verify timestamp column (with UTC timezone)
        let timestamp_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(timestamp_col.value(0), 1609459200000000i64);
        assert_eq!(timestamp_col.value(4), 1609804800000000i64);
        assert!(timestamp_col
            .timezone()
            .is_some_and(|tz| tz.eq_ignore_ascii_case("utc")));
        col_idx += 1;

        // Verify timestamp_ntz column (without timezone)
        let timestamp_ntz_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(timestamp_ntz_col.value(0), 1609459200000000i64);
        assert_eq!(timestamp_ntz_col.value(4), 1609804800000000i64);
        assert!(timestamp_ntz_col.timezone().is_none());
        col_idx += 1;

        // Verify decimal column
        let decimal_col = data[0]
            .column(col_idx)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(decimal_col.value(0), 12345i128);
        assert_eq!(decimal_col.value(4), 56789i128);
        assert_eq!(decimal_col.precision(), 10);
        assert_eq!(decimal_col.scale(), 2);
    }

    #[tokio::test]
    async fn test_parquet_handler_trait_write_overwrite_true() {
        let store = Arc::new(InMemory::new());
        let parquet_handler: Arc<dyn ParquetHandler> = Arc::new(DefaultParquetHandler::new(
            store.clone(),
            Arc::new(TokioBackgroundExecutor::new()),
        ));

        let file_url = Url::parse("memory:///overwrite_test/data.parquet").unwrap();

        // Create first data set
        let engine_data1: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let data_iter1: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::once(Ok(engine_data1)));

        // Write the first file
        parquet_handler
            .write_parquet_file(file_url.clone(), data_iter1)
            .unwrap();

        // Create second data set with different data
        let engine_data2: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![10, 20])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let data_iter2: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::once(Ok(engine_data2)));

        // Overwrite with second file (overwrite=true)
        parquet_handler
            .write_parquet_file(file_url.clone(), data_iter2)
            .unwrap();

        // Read back and verify it contains the second data set
        let path = Path::from_url_path(file_url.path()).unwrap();
        let reader = ParquetObjectReader::new(store.clone(), path);
        let physical_schema = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .schema()
            .clone();

        let file_meta = FileMeta {
            location: file_url,
            last_modified: 0,
            size: 0,
        };

        let data: Vec<RecordBatch> = parquet_handler
            .read_parquet_files(
                slice::from_ref(&file_meta),
                Arc::new(physical_schema.try_into_kernel().unwrap()),
                None,
            )
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .unwrap();

        // Verify we have the second data set (2 rows, not 3)
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 2);
        let value_col = data[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_col.values(), &[10, 20]);
    }

    #[tokio::test]
    async fn test_parquet_handler_trait_write_always_overwrites() {
        let store = Arc::new(InMemory::new());
        let parquet_handler: Arc<dyn ParquetHandler> = Arc::new(DefaultParquetHandler::new(
            store.clone(),
            Arc::new(TokioBackgroundExecutor::new()),
        ));

        let file_url = Url::parse("memory:///no_overwrite_test/data.parquet").unwrap();

        // Create first data set
        let engine_data1: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let data_iter1: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::once(Ok(engine_data1)));

        // Write the first file
        parquet_handler
            .write_parquet_file(file_url.clone(), data_iter1)
            .unwrap();

        // Create second data set
        let engine_data2: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![10, 20])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));
        let data_iter2: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::once(Ok(engine_data2)));

        // Write again - should overwrite successfully (new behavior always overwrites)
        parquet_handler
            .write_parquet_file(file_url.clone(), data_iter2)
            .unwrap();

        // Verify the file was overwritten with the new data
        let path = Path::from_url_path(file_url.path()).unwrap();
        let reader = ParquetObjectReader::new(store.clone(), path);
        let physical_schema = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .schema()
            .clone();

        let file_meta = FileMeta {
            location: file_url,
            last_modified: 0,
            size: 0,
        };

        let data: Vec<RecordBatch> = parquet_handler
            .read_parquet_files(
                slice::from_ref(&file_meta),
                Arc::new(physical_schema.try_into_kernel().unwrap()),
                None,
            )
            .unwrap()
            .map(into_record_batch)
            .try_collect()
            .unwrap();

        // Verify we now have the second data set (2 rows)
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 2);
        let value_col = data[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_col.values(), &[10, 20]);
    }

    #[test]
    fn test_read_parquet_footer_preserves_field_ids() {
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
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

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
