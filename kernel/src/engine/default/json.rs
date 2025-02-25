//! Default Json handler implementation

use std::io::BufReader;
use std::ops::Range;
use std::sync::{mpsc, Arc};
use std::task::Poll;

use crate::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use crate::arrow::json::ReaderBuilder;
use crate::arrow::record_batch::RecordBatch;
use bytes::{Buf, Bytes};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, GetResultPayload};
use tracing::warn;
use url::Url;

use super::executor::TaskExecutor;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::parse_json as arrow_parse_json;
use crate::engine::arrow_utils::to_json_bytes;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, ExpressionRef, FileDataReadResultIterator, FileMeta,
    JsonHandler,
};

#[derive(Debug)]
pub struct DefaultJsonHandler<E: TaskExecutor> {
    /// The object store to read files from
    store: Arc<DynObjectStore>,
    /// The executor to run async tasks on
    task_executor: Arc<E>,
    /// The maximum number of batches to read ahead
    readahead: usize,
    /// The number of rows to read per batch
    batch_size: usize,
}

impl<E: TaskExecutor> DefaultJsonHandler<E> {
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            store,
            task_executor,
            readahead: 1000,
            batch_size: 1024 * 128,
        }
    }

    /// Set the maximum number of batches to read ahead during [Self::read_json_files()].
    ///
    /// Defaults to 10.
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.readahead = readahead;
        self
    }

    /// Set the number of rows to read per batch during [Self::parse_json()].
    ///
    /// Defaults to 1024.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl<E: TaskExecutor> JsonHandler for DefaultJsonHandler<E> {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<ExpressionRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into()?);
        let file_opener = JsonOpener::new(self.batch_size, schema.clone(), self.store.clone());

        let (tx, rx) = mpsc::sync_channel(self.readahead);
        let files = files.to_vec();
        let readahead = self.readahead;

        self.task_executor.spawn(async move {
            // an iterator of futures that open each file
            let file_futures = files.into_iter().map(|file| file_opener.open(file, None));

            // create a stream from that iterator which buffers up to `readahead` futures at a time
            let mut stream = stream::iter(file_futures)
                .buffered(readahead)
                .try_flatten()
                .map_ok(|record_batch| {
                    Box::new(ArrowEngineData::new(record_batch)) as Box<dyn EngineData>
                });

            // send each record batch over the channel
            while let Some(item) = stream.next().await {
                if tx.send(item).is_err() {
                    warn!("read_json receiver end of channel dropped before sending completed");
                }
            }
        });

        Ok(Box::new(rx.into_iter()))
    }

    // note: for now we just buffer all the data and write it out all at once
    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + '_>,
        _overwrite: bool,
    ) -> DeltaResult<()> {
        let buffer = to_json_bytes(data)?;
        // Put if absent
        let store = self.store.clone(); // cheap Arc
        let path = Path::from(path.path());
        let path_str = path.to_string();
        self.task_executor
            .block_on(async move {
                store
                    .put_opts(&path, buffer.into(), object_store::PutMode::Create.into())
                    .await
            })
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { .. } => Error::FileAlreadyExists(path_str),
                e => e.into(),
            })?;
        Ok(())
    }
}

/// Opens JSON files and returns a stream of record batches
#[allow(missing_debug_implementations)]
pub struct JsonOpener {
    batch_size: usize,
    projected_schema: ArrowSchemaRef,
    object_store: Arc<DynObjectStore>,
}

impl JsonOpener {
    /// Returns a [`JsonOpener`]
    pub fn new(
        batch_size: usize,
        projected_schema: ArrowSchemaRef,
        object_store: Arc<DynObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            object_store,
        }
    }
}

impl JsonOpener {
    pub async fn open(
        &self,
        file_meta: FileMeta,
        _: Option<Range<i64>>,
    ) -> DeltaResult<BoxStream<'static, DeltaResult<RecordBatch>>> {
        let store = self.object_store.clone();
        let schema = self.projected_schema.clone();
        let batch_size = self.batch_size;

        let path = Path::from_url_path(file_meta.location.path())?;
        let get_result = store.get(&path).await?;
        match get_result.payload {
            GetResultPayload::File(file, _) => {
                let reader = ReaderBuilder::new(schema)
                    .with_batch_size(batch_size)
                    .build(BufReader::new(file))?;
                Ok(futures::stream::iter(reader).map_err(Error::from).boxed())
            }
            GetResultPayload::Stream(s) => {
                let mut decoder = ReaderBuilder::new(schema)
                    .with_batch_size(batch_size)
                    .build_decoder()?;

                let mut input = s.map_err(Error::from);
                let mut buffered = Bytes::new();

                let stream = futures::stream::poll_fn(move |cx| {
                    loop {
                        if buffered.is_empty() {
                            buffered = match futures::ready!(input.poll_next_unpin(cx)) {
                                Some(Ok(b)) => b,
                                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                                None => break,
                            };
                        }
                        let read = buffered.len();

                        let decoded = match decoder.decode(buffered.as_ref()) {
                            Ok(decoded) => decoded,
                            Err(e) => return Poll::Ready(Some(Err(e.into()))),
                        };

                        buffered.advance(decoded);
                        if decoded != read {
                            break;
                        }
                    }

                    Poll::Ready(decoder.flush().map_err(Error::from).transpose())
                });
                Ok(stream.map_err(Error::from).boxed())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::arrow::array::{AsArray, RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use futures::future;
    use itertools::Itertools;
    use object_store::{local::LocalFileSystem, ObjectStore};
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOpts,
        PutOptions, PutPayload, PutResult, Result,
    };

    use super::*;
    use crate::{
        actions::get_log_schema, engine::arrow_data::ArrowEngineData,
        engine::default::executor::tokio::TokioBackgroundExecutor,
    };

    /// Store wrapper that wraps an inner store to purposefully delay GET requests of certain keys.
    #[derive(Debug)]
    struct SlowGetStore<T> {
        inner: T,
        wait_keys: Arc<Mutex<HashMap<Path, Duration>>>,
    }

    impl<T> SlowGetStore<T> {
        fn new(inner: T, wait_keys: HashMap<Path, Duration>) -> Self {
            Self {
                inner,
                wait_keys: Arc::new(Mutex::new(wait_keys)),
            }
        }
    }

    impl<T: ObjectStore> std::fmt::Display for SlowGetStore<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SlowGetStore({:?})", self.wait_keys)
        }
    }

    #[async_trait::async_trait]
    impl<T: ObjectStore> ObjectStore for SlowGetStore<T> {
        async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
            self.inner.put(location, payload).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart(location).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get(&self, location: &Path) -> Result<GetResult> {
            let wait_time = {
                let guard = self.wait_keys.lock().expect("lock key map");
                guard.get(location).copied()
            };

            if let Some(duration) = wait_time {
                tokio::time::sleep(duration).await;
            }

            self.inner.get(location).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
            self.inner.get_range(location, range).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        async fn head(&self, location: &Path) -> Result<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &Path) -> Result<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'_, Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.copy(from, to).await
        }

        async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.rename(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }

        async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.rename_if_not_exists(from, to).await
        }
    }

    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    #[test]
    fn test_parse_json() {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

        let json_strings = StringArray::from(vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]);
        let output_schema = get_log_schema().clone();

        let batch = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        assert_eq!(batch.len(), 4);
    }

    #[test]
    fn test_parse_json_drop_field() {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
        let json_strings = StringArray::from(vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":false}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"},"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2, "maxRowId": 3}}}"#,
        ]);
        let output_schema = get_log_schema().clone();

        let batch: RecordBatch = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .map(|sd| sd.into())
            .unwrap();
        assert_eq!(batch.column(0).len(), 1);
        let add_array = batch.column_by_name("add").unwrap().as_struct();
        let dv_col = add_array
            .column_by_name("deletionVector")
            .unwrap()
            .as_struct();
        assert!(dv_col.column_by_name("storageType").is_some());
        assert!(dv_col.column_by_name("maxRowId").is_none());
    }

    #[tokio::test]
    async fn test_read_json_files() {
        let store = Arc::new(LocalFileSystem::new());

        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/00000000000000000000.json",
        ))
        .unwrap();
        let url = url::Url::from_file_path(path).unwrap();
        let location = Path::from(url.path());
        let meta = store.head(&location).await.unwrap();

        let files = &[FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
        }];

        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
        let physical_schema = Arc::new(ArrowSchema::try_from(get_log_schema().as_ref()).unwrap());
        let data: Vec<RecordBatch> = handler
            .read_json_files(files, Arc::new(physical_schema.try_into().unwrap()), None)
            .unwrap()
            .map(|ed_res| {
                // TODO(nick) make this easier
                ed_res.and_then(|ed| {
                    ed.into_any()
                        .downcast::<ArrowEngineData>()
                        .map_err(|_| Error::engine_data_type("ArrowEngineData"))
                        .map(|sd| sd.into())
                })
            })
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn test_read_json_files_ordering() {
        let paths = [
            "./tests/data/table-with-dv-small/_delta_log/00000000000000000000.json",
            "./tests/data/table-with-dv-small/_delta_log/00000000000000000001.json",
        ];
        let paths = paths.map(|p| std::fs::canonicalize(PathBuf::from(p)).unwrap());

        let path_string = paths[0].to_string_lossy().to_string();
        let object_store_path = Path::from(path_string);
        // for the first 000000.json, wait for 1 second
        let key_map = (object_store_path, Duration::from_secs(1));

        let store = Arc::new(SlowGetStore::new(
            LocalFileSystem::new(),
            vec![key_map].into_iter().collect(),
        ));

        let file_futures: Vec<_> = paths
            .iter()
            .map(|path| {
                let store = store.clone();
                async move {
                    let url = url::Url::from_file_path(path).unwrap();
                    let location = Path::from(url.path());
                    let meta = store.head(&location).await.unwrap();
                    FileMeta {
                        location: url.clone(),
                        last_modified: meta.last_modified.timestamp_millis(),
                        size: meta.size,
                    }
                }
            })
            .collect();
        let files = future::join_all(file_futures).await;

        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
        let physical_schema = Arc::new(ArrowSchema::try_from(get_log_schema().as_ref()).unwrap());
        let data: Vec<RecordBatch> = handler
            .read_json_files(&files, Arc::new(physical_schema.try_into().unwrap()), None)
            .unwrap()
            .map(|ed_res| {
                // TODO(nick) make this easier
                ed_res.and_then(|ed| {
                    ed.into_any()
                        .downcast::<ArrowEngineData>()
                        .map_err(|_| Error::engine_data_type("ArrowEngineData"))
                        .map(|sd| sd.into())
                })
            })
            .try_collect()
            .unwrap();
        assert_eq!(data.len(), 2);
        // check for ordering: first commit has 4 actions, second commit has 3 actions
        assert_eq!(data[0].num_rows(), 4);
        assert_eq!(data[1].num_rows(), 3);
    }
}
