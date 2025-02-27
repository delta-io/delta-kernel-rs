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

const DEFAULT_BUFFER_SIZE: usize = 1000;
const DEFAULT_BATCH_SIZE: usize = 10_000;

#[derive(Debug)]
pub struct DefaultJsonHandler<E: TaskExecutor> {
    /// The object store to read files from
    store: Arc<DynObjectStore>,
    /// The executor to run async tasks on
    task_executor: Arc<E>,
    /// The maximum number of read requests to buffer in memory at once
    buffer_size: usize,
    /// The number of rows to read per batch
    batch_size: usize,
}

impl<E: TaskExecutor> DefaultJsonHandler<E> {
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            store,
            task_executor,
            buffer_size: DEFAULT_BUFFER_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Deprecated: use [Self::with_buffer_size()].
    ///
    /// Set the maximum number read requests to buffer in memory at once in
    /// [Self::read_json_files()].
    ///
    /// Defaults to 1000.
    #[deprecated(note = "use with_buffer_size() instead")]
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.buffer_size = readahead;
        self
    }

    /// Set the maximum number read requests to buffer in memory at once in
    /// [Self::read_json_files()].
    ///
    /// Defaults to 1000.
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the number of rows to read per batch during [Self::parse_json()].
    ///
    /// Defaults to 10_000 rows.
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

        let (tx, rx) = mpsc::sync_channel(self.buffer_size);
        let files = files.to_vec();
        let buffer_size = self.buffer_size;

        self.task_executor.spawn(async move {
            // an iterator of futures that open each file
            let file_futures = files.into_iter().map(|file| file_opener.open(file, None));

            // create a stream from that iterator which buffers up to `readahead` futures at a time
            let mut stream = stream::iter(file_futures)
                .buffered(buffer_size)
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
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::task::Waker;

    use crate::actions::get_log_schema;
    use crate::arrow::array::{AsArray, RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::{
        TokioBackgroundExecutor, TokioMultiThreadExecutor,
    };
    use futures::future;
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
    };

    // TODO: should just use the one from test_utils, but running into dependency issues
    fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
        ArrowEngineData::try_from_engine_data(engine_data)
            .unwrap()
            .into()
    }

    use super::*;

    /// Store wrapper that wraps an inner store to guarantee the ordering of GET requests.
    ///
    /// WARN: Does not handle duplicate keys, and will fail on duplicate requests of the same key.
    ///
    // TODO(zach): we can handle duplicate requests if we retain the ordering of the keys track
    // that all of the keys prior to the one requested have been resolved.
    #[derive(Debug)]
    struct OrderedGetStore<T: ObjectStore> {
        // The ObjectStore we are wrapping
        inner: T,
        // Combined state: queue and wakers, protected by a single mutex
        state: Arc<Mutex<KeysAndWakers>>,
    }

    #[derive(Debug, Default)]
    struct KeysAndWakers {
        // Queue of paths in order which they will resolve
        ordered_keys: VecDeque<Path>,
        // Map of paths to wakers for pending get requests
        wakers: HashMap<Path, Vec<Waker>>,
    }

    impl<T: ObjectStore> OrderedGetStore<T> {
        fn new(inner: T, ordered_keys: impl Into<VecDeque<Path>>) -> Self {
            let ordered_keys = ordered_keys.into();
            // Check for duplicates
            let mut seen = HashSet::new();
            for key in ordered_keys.iter() {
                if !seen.insert(key) {
                    panic!("Duplicate key in OrderedGetStore: {}", key);
                }
            }

            let state = KeysAndWakers {
                ordered_keys,
                wakers: HashMap::new(),
            };

            Self {
                inner,
                state: Arc::new(Mutex::new(state)),
            }
        }

        // Wake the wakers for the next path in order, if any
        fn wake_next(&self) {
            let mut state = self.state.lock().unwrap();
            // If we have a next key, get its wakers and wake them
            if let Some(next_key) = state.ordered_keys.front().cloned() {
                if let Some(path_wakers) = state.wakers.remove(&next_key) {
                    // We need to release the lock before waking wakers to prevent
                    // any potential reentrant lock attempts
                    drop(state);
                    for waker in path_wakers {
                        waker.wake();
                    }
                }
            }
        }
    }

    impl<T: ObjectStore> std::fmt::Display for OrderedGetStore<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let state = self.state.lock().unwrap();
            write!(f, "OrderedGetStore({:?})", state.ordered_keys)
        }
    }

    #[async_trait::async_trait]
    impl<T: ObjectStore> ObjectStore for OrderedGetStore<T> {
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

        // A GET request is fulfilled by checking if the requested path is next in order:
        // - if yes, remove the path from the queue and proceed with the GET request, then wake the
        //   next path in order
        // - if no, register the waker and wait
        async fn get(&self, location: &Path) -> Result<GetResult> {
            // we implement a future which only resolves once the requested path is next in order
            future::poll_fn(move |cx| {
                let mut state = self.state.lock().unwrap();
                match state.ordered_keys.front() {
                    Some(key) if key == location => {
                        // this key is next: remove it and return Ready so we proceed to do the GET
                        state.ordered_keys.pop_front();
                        Poll::Ready(())
                    }
                    Some(_) => {
                        // this key isn't next: register our waker and return Pending
                        state
                            .wakers
                            .entry(location.clone())
                            .or_insert_with(Vec::new)
                            .push(cx.waker().clone());
                        Poll::Pending
                    }
                    None => {
                        // empty: someone asked for a key not in the order
                        panic!("Key ordering not specified for {}", location);
                    }
                }
            })
            .await;

            // after doing our GET request, wake the next path in order
            let result = self.inner.get(location).await;
            self.wake_next();
            return result;
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
            .map_ok(into_record_batch)
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 4);
    }

    // this test creates an OrderedGetStore with 1000 paths that resolve in REVERSE order. it
    // spawns 1000 tasks to read the paths in natural order (0, 1, 2, ...) and checks that they
    // complete in reverse.
    #[tokio::test]
    async fn test_ordered_get_store() {
        let num_paths = 1000;
        let paths: Vec<Path> = (0..num_paths)
            .map(|i| Path::from(format!("/test/path{}", i)))
            .collect();

        let memory_store = InMemory::new();
        for (i, path) in paths.iter().enumerate() {
            memory_store
                .put(path, Bytes::from(format!("content_{}", i)).into())
                .await
                .unwrap();
        }

        // Create ordered store with REVERSE order (999, 998, ...)
        let rev_paths: VecDeque<Path> = paths.iter().rev().cloned().collect();
        let ordered_store = Arc::new(OrderedGetStore::new(memory_store.fork(), rev_paths.clone()));

        let (tx, rx) = std::sync::mpsc::channel();

        // Spawn tasks to GET each path in the natural order (0, 1, 2, ...)
        // They should complete in the REVERSE order (999, 998, ...) due to OrderedGetStore
        let handles: Vec<_> = paths
            .iter()
            .cloned()
            .map(|path| {
                let store = ordered_store.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let _ = store.get(&path).await.unwrap();
                    tx.send(path).unwrap();
                })
            })
            .collect();

        let _ = future::join_all(handles).await;
        let mut completed = Vec::new();
        while let Ok(path) = rx.try_recv() {
            completed.push(path);
        }

        // Expected order is reversed (999, 998, ..., 1, 0)
        assert_eq!(
            completed,
            rev_paths.into_iter().collect_vec(),
            "Expected paths to complete in reverse order"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_read_json_files_ordering() {
        let paths = [
            "./tests/data/table-with-dv-small/_delta_log/00000000000000000000.json",
            "./tests/data/table-with-dv-small/_delta_log/00000000000000000001.json",
        ];
        let paths = paths.map(|p| std::fs::canonicalize(PathBuf::from(p)).unwrap());

        // object store will resolve GETs to paths in reverse: 0001.json, 0000.json
        let reverse_paths = paths
            .iter()
            .rev()
            .map(|p| Path::from(p.to_string_lossy().to_string()))
            .collect::<VecDeque<_>>();
        let store = Arc::new(OrderedGetStore::new(LocalFileSystem::new(), reverse_paths));

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

        // note: join_all is ordered
        let files = future::join_all(file_futures).await;

        let handler = DefaultJsonHandler::new(
            store,
            Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )),
        );
        let physical_schema = Arc::new(ArrowSchema::try_from(get_log_schema().as_ref()).unwrap());
        let data: Vec<RecordBatch> = handler
            .read_json_files(&files, Arc::new(physical_schema.try_into().unwrap()), None)
            .unwrap()
            .map_ok(into_record_batch)
            .try_collect()
            .unwrap();
        assert_eq!(data.len(), 2);
        // check for ordering: first commit has 4 actions, second commit has 3 actions
        assert_eq!(data[0].num_rows(), 4);
        assert_eq!(data[1].num_rows(), 3);
    }
}
