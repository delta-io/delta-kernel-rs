//! Default Json handler implementation

use std::io::BufReader;
use std::ops::Range;
use std::sync::{mpsc, Arc};
use std::task::Poll;

use crate::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use crate::arrow::json::ReaderBuilder;
use crate::arrow::record_batch::RecordBatch;
use bytes::{Buf, Bytes};
use futures::stream::{self, BoxStream};
use futures::{ready, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{self, DynObjectStore, GetResultPayload, PutMode};
use tracing::warn;
use url::Url;

use super::executor::TaskExecutor;
use crate::engine::arrow_conversion::TryFromKernel as _;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::parse_json as arrow_parse_json;
use crate::engine::arrow_utils::to_json_bytes;
use crate::engine_data::FilteredEngineData;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, FileDataReadResultIterator, FileMeta, JsonHandler, PredicateRef,
};

const DEFAULT_BUFFER_SIZE: usize = 1000;
const DEFAULT_BATCH_SIZE: usize = 1000;

#[derive(Debug)]
pub struct DefaultJsonHandler<E: TaskExecutor> {
    /// The object store to read files from
    store: Arc<DynObjectStore>,
    /// The executor to run async tasks on
    task_executor: Arc<E>,
    /// The maximum number of read requests to buffer in memory at once. Note that this actually
    /// controls two things: the number of concurrent requests (done by `buffered`) and the size of
    /// the buffer (via our `sync_channel`).
    buffer_size: usize,
    /// Limit the number of rows per batch. That is, for batch_size = N, then each RecordBatch
    /// yielded by the stream will have at most N rows.
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

    /// Set the maximum number read requests to buffer in memory at once in
    /// [Self::read_json_files()].
    ///
    /// Defaults to 1000.
    ///
    /// Memory constraints can be imposed by constraining the buffer size and batch size. Note that
    /// overall memory usage is proportional to the product of these two values.
    /// 1. Batch size governs the size of RecordBatches yielded in each iteration of the stream
    /// 2. Buffer size governs the number of concurrent tasks (which equals the size of the buffer
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Limit the number of rows per batch. That is, for batch_size = N, then each RecordBatch
    /// yielded by the stream will have at most N rows.
    ///
    /// Defaults to 1000 rows (json objects).
    ///
    /// See [Decoder::with_buffer_size] for details on constraining memory usage with buffer size
    /// and batch size.
    ///
    /// [Decoder::with_buffer_size]: crate::arrow::json::reader::Decoder
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
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let schema = Arc::new(ArrowSchema::try_from_kernel(physical_schema.as_ref())?);
        let file_opener = JsonOpener::new(self.batch_size, schema.clone(), self.store.clone());

        let (tx, rx) = mpsc::sync_channel(self.buffer_size);
        let files = files.to_vec();
        let buffer_size = self.buffer_size;

        self.task_executor.spawn(async move {
            // an iterator of futures that open each file
            let file_futures = files.into_iter().map(|file| file_opener.open(file, None));

            // create a stream from that iterator which buffers up to `buffer_size` futures at a time
            let mut stream = stream::iter(file_futures)
                .buffered(buffer_size)
                .try_flatten()
                .map_ok(|record_batch| -> Box<dyn EngineData> {
                    Box::new(ArrowEngineData::new(record_batch))
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
        data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        let buffer = to_json_bytes(data)?;
        let put_mode = if overwrite {
            PutMode::Overwrite
        } else {
            PutMode::Create
        };

        let store = self.store.clone(); // cheap Arc
        let path = Path::from_url_path(path.path())?;
        let path_str = path.to_string();
        self.task_executor
            .block_on(async move { store.put_opts(&path, buffer.into(), put_mode.into()).await })
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
        match store.get(&path).await?.payload {
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

                let s = futures::stream::poll_fn(move |cx| {
                    loop {
                        if buffered.is_empty() {
                            buffered = match ready!(input.poll_next_unpin(cx)) {
                                Some(Ok(b)) => b,
                                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                                None => break,
                            };
                        }
                        let read = buffered.len();

                        // NB (from Decoder::decode docs):
                        // Read JSON objects from `buf` (param), returning the number of bytes read
                        //
                        // This method returns once `batch_size` objects have been parsed since the
                        // last call to [`Self::flush`], or `buf` is exhausted. Any remaining bytes
                        // should be included in the next call to [`Self::decode`]
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
                Ok(s.map_err(Error::from).boxed())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::path::PathBuf;
    use std::sync::{mpsc, Arc, Mutex};
    use std::task::Waker;

    use crate::actions::get_commit_schema;
    use crate::arrow::array::{AsArray, Int32Array, RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::{
        TokioBackgroundExecutor, TokioMultiThreadExecutor,
    };
    use crate::schema::{DataType as DeltaDataType, Schema, StructField};
    use crate::utils::test_utils::string_array_to_engine_data;
    use futures::future;
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::PutMultipartOptions;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutOptions,
        PutPayload, PutResult, Result,
    };
    use serde_json::json;

    // TODO: should just use the one from test_utils, but running into dependency issues
    fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
        ArrowEngineData::try_from_engine_data(engine_data)
            .unwrap()
            .into()
    }

    use super::*;

    /// Store wrapper that wraps an inner store to guarantee the ordering of GET requests. Note
    /// that since the keys are resolved in order, requests to subsequent keys in the order will
    /// block until the earlier keys are requested.
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
        state: Mutex<KeysAndWakers>,
    }

    #[derive(Debug)]
    struct KeysAndWakers {
        // Queue of paths in order which they will resolve
        ordered_keys: VecDeque<Path>,
        // Map of paths to wakers for pending get requests
        wakers: HashMap<Path, Waker>,
    }

    impl<T: ObjectStore> OrderedGetStore<T> {
        fn new(inner: T, ordered_keys: &[Path]) -> Self {
            let ordered_keys = ordered_keys.to_vec();
            // Check for duplicates
            let mut seen = HashSet::new();
            for key in ordered_keys.iter() {
                if !seen.insert(key) {
                    panic!("Duplicate key in OrderedGetStore: {key}");
                }
            }

            let state = KeysAndWakers {
                ordered_keys: ordered_keys.into(),
                wakers: HashMap::new(),
            };

            Self {
                inner,
                state: Mutex::new(state),
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
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        // A GET request is fulfilled by checking if the requested path is next in order:
        // - if yes, remove the path from the queue and proceed with the GET request, then wake the
        //   next path in order
        // - if no, register the waker and wait
        async fn get(&self, location: &Path) -> Result<GetResult> {
            // Do the actual GET request first, then introduce any artificial ordering delays as needed
            let result = self.inner.get(location).await;

            // we implement a future which only resolves once the requested path is next in order
            future::poll_fn(move |cx| {
                let mut state = self.state.lock().unwrap();
                let Some(next_key) = state.ordered_keys.front() else {
                    panic!("Ran out of keys before {location}");
                };
                if next_key == location {
                    // We are next in line. Nobody else can remove our key, and our successor
                    // cannot race with us to register itself because we hold the lock.
                    //
                    // first, remove our key from the queue.
                    //
                    // note: safe to unwrap because we just checked that the front key exists (and
                    // is the same as our requested location)
                    state.ordered_keys.pop_front().unwrap();

                    // there are three possible cases, either:
                    // 1. the key has already been requested, hence there is a waker waiting, and we
                    //    need to wake it up
                    // 2. the next key has no waker registered, in which case we do nothing, and
                    //    whenever the request for said key is made, it will either be next in line
                    //    or a waker will be registered - either case ensuring that the request is
                    //    completed
                    // 3. the next key is the last key in the queue, in which case there is nothing
                    //    left to do (no need to wake anyone)
                    if let Some(next_key) = state.ordered_keys.front().cloned() {
                        if let Some(waker) = state.wakers.remove(&next_key) {
                            waker.wake(); // NOTE: Not async, returns instantly.
                        }
                    }
                    Poll::Ready(())
                } else {
                    // We are not next in line, so wait on our key. Nobody can race to remove it
                    // because we own it; nobody can race to wake us because we hold the lock.
                    if state
                        .wakers
                        .insert(location.clone(), cx.waker().clone())
                        .is_some()
                    {
                        panic!("Somebody else is already waiting on {location}");
                    }
                    Poll::Pending
                }
            })
            .await;

            // When we return this result, the future succeeds instantly. Any pending wake() call
            // will not be processed before the next time we yield -- unless our executor is
            // multi-threaded and happens to have another thread available. In that case, the
            // serialization point is the moment our next-key poll_fn issues the wake call (or
            // proves no wake is needed).
            result
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
            self.inner.get_range(location, range).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        async fn head(&self, location: &Path) -> Result<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &Path) -> Result<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, Result<ObjectMeta>> {
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
        let output_schema = get_commit_schema().clone();

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
        let output_schema = get_commit_schema().clone();

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
        let url = Url::from_file_path(path).unwrap();
        let location = Path::from_url_path(url.path()).unwrap();
        let meta = store.head(&location).await.unwrap();

        let files = &[FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
        }];

        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));
        let data: Vec<RecordBatch> = handler
            .read_json_files(files, get_commit_schema().clone(), None)
            .unwrap()
            .map_ok(into_record_batch)
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 4);

        // limit batch size
        let handler = handler.with_batch_size(2);
        let data: Vec<RecordBatch> = handler
            .read_json_files(files, get_commit_schema().clone(), None)
            .unwrap()
            .map_ok(into_record_batch)
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].num_rows(), 2);
        assert_eq!(data[1].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_ordered_get_store() {
        // note we don't want to go over 1000 since we only buffer 1000 requests at a time
        let num_paths = 1000;
        let ordered_paths: Vec<Path> = (0..num_paths)
            .map(|i| Path::from(format!("/test/path{i}")))
            .collect();
        let jumbled_paths: Vec<_> = ordered_paths[100..400]
            .iter()
            .chain(ordered_paths[400..].iter().rev())
            .chain(ordered_paths[..100].iter())
            .cloned()
            .collect();

        let memory_store = InMemory::new();
        for (i, path) in ordered_paths.iter().enumerate() {
            memory_store
                .put(path, Bytes::from(format!("content_{i}")).into())
                .await
                .unwrap();
        }

        // Create ordered store with natural order (0, 1, 2, ...)
        let ordered_store = Arc::new(OrderedGetStore::new(memory_store, &ordered_paths));

        let (tx, rx) = mpsc::channel();

        // Spawn tasks to GET each path in our somewhat jumbled order
        // They should complete in order (0, 1, 2, ...) due to OrderedGetStore
        let handles = jumbled_paths.into_iter().map(|path| {
            let store = ordered_store.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let _ = store.get(&path).await.unwrap();
                tx.send(path).unwrap();
            })
        });

        // TODO(zach): we need to join all the handles otherwise none of the tasks run? despite the
        // docs?
        future::join_all(handles).await;
        drop(tx);

        // NB (from mpsc::Receiver::recv): This function will always block the current thread if
        // there is no data available and it's possible for more data to be sent (at least one
        // sender still exists).
        let mut completed = Vec::new();
        while let Ok(path) = rx.recv() {
            completed.push(path);
        }

        assert_eq!(
            completed,
            ordered_paths.into_iter().collect_vec(),
            "Expected paths to complete in order"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_read_json_files_ordering() {
        // this test checks that the read_json_files method returns the files in order in the
        // presence of an ObjectStore (OrderedGetStore) that resolves paths in a jumbled order:
        // 1. we set up a list of FileMetas (and some random JSON content) in order
        // 2. we then set up an ObjectStore to resolves those paths in a jumbled order
        // 3. then call read_json_files and check that the results are in order
        let ordered_paths: Vec<Path> = (0..1000)
            .map(|i| Path::from(format!("test/path{i}")))
            .collect();

        let test_list: &[(usize, Vec<Path>)] = &[
            // test 1: buffer_size = 1000, just 1000 jumbled paths
            (
                1000, // buffer_size
                ordered_paths[100..400]
                    .iter()
                    .chain(ordered_paths[400..].iter().rev())
                    .chain(ordered_paths[..100].iter())
                    .cloned()
                    .collect(),
            ),
            // test 2: buffer_size = 4, jumbled paths in groups of 4
            (
                4, // buffer_size
                (0..250)
                    .flat_map(|i| {
                        [
                            ordered_paths[1 + 4 * i].clone(),
                            ordered_paths[4 * i].clone(),
                            ordered_paths[3 + 4 * i].clone(),
                            ordered_paths[2 + 4 * i].clone(),
                        ]
                    })
                    .collect_vec(),
            ),
        ];

        let memory_store = InMemory::new();
        for (i, path) in ordered_paths.iter().enumerate() {
            memory_store
                .put(path, Bytes::from(format!("{{\"val\": {i}}}")).into())
                .await
                .unwrap();
        }

        for (buffer_size, jumbled_paths) in test_list {
            // set up our ObjectStore to resolve paths in a jumbled order
            let store = Arc::new(OrderedGetStore::new(memory_store.fork(), jumbled_paths));

            // convert the paths to FileMeta
            let ordered_file_meta: Vec<_> = ordered_paths
                .iter()
                .map(|path| {
                    let store = store.clone();
                    async move {
                        let url = Url::parse(&format!("memory:/{path}")).unwrap();
                        let location = Path::from(path.as_ref());
                        let meta = store.head(&location).await.unwrap();
                        FileMeta {
                            location: url,
                            last_modified: meta.last_modified.timestamp_millis(),
                            size: meta.size,
                        }
                    }
                })
                .collect();

            // note: join_all is ordered
            let files = future::join_all(ordered_file_meta).await;

            // fire off the read_json_files call (for all the files in order)
            let handler = DefaultJsonHandler::new(
                store,
                Arc::new(TokioMultiThreadExecutor::new(
                    tokio::runtime::Handle::current(),
                )),
            );
            let handler = handler.with_buffer_size(*buffer_size);
            let physical_schema = Arc::new(Schema::new_unchecked(vec![StructField::nullable(
                "val",
                DeltaDataType::INTEGER,
            )]));
            let data: Vec<RecordBatch> = handler
                .read_json_files(&files, physical_schema, None)
                .unwrap()
                .map_ok(into_record_batch)
                .try_collect()
                .unwrap();

            // check the order
            let all_values: Vec<i32> = data
                .iter()
                .flat_map(|batch| {
                    let val_col: &Int32Array = batch.column(0).as_primitive();
                    (0..val_col.len()).map(|i| val_col.value(i)).collect_vec()
                })
                .collect();
            assert_eq!(all_values, (0..1000).collect_vec());
        }
    }

    // Helper function to create test data
    fn create_test_data(values: Vec<&str>) -> DeltaResult<Box<dyn EngineData>> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "dog",
            DataType::Utf8,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])?;
        Ok(Box::new(ArrowEngineData::new(batch)))
    }

    // Helper function to read JSON file asynchronously
    async fn read_json_file(
        store: &Arc<InMemory>,
        path: &Path,
    ) -> DeltaResult<Vec<serde_json::Value>> {
        let content = store.get(path).await?;
        let file_bytes = content.bytes().await?;
        let file_string =
            String::from_utf8(file_bytes.to_vec()).map_err(|e| object_store::Error::Generic {
                store: "memory",
                source: Box::new(e),
            })?;
        let json: Vec<_> = serde_json::Deserializer::from_str(&file_string)
            .into_iter::<serde_json::Value>()
            .flatten()
            .collect();
        Ok(json)
    }

    #[tokio::test]
    async fn test_write_json_file_without_overwrite() -> DeltaResult<()> {
        do_test_write_json_file(false).await
    }

    #[tokio::test]
    async fn test_write_json_file_overwrite() -> DeltaResult<()> {
        do_test_write_json_file(true).await
    }

    async fn do_test_write_json_file(overwrite: bool) -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = DefaultJsonHandler::new(store.clone(), executor);
        let path = Url::parse("memory:///test/data/00000000000000000001.json")?;
        let object_path = Path::from("/test/data/00000000000000000001.json");

        // First write with no existing file
        let data = create_test_data(vec!["remi", "wilson"])?;
        let filtered_data = Ok(FilteredEngineData::with_all_rows_selected(data));
        let result =
            handler.write_json_file(&path, Box::new(std::iter::once(filtered_data)), overwrite);

        // Verify the first write is successful
        assert!(result.is_ok());
        let json = read_json_file(&store, &object_path).await?;
        assert_eq!(json, vec![json!({"dog": "remi"}), json!({"dog": "wilson"})]);

        // Second write with existing file
        let data = create_test_data(vec!["seb", "tia"])?;
        let filtered_data = Ok(FilteredEngineData::with_all_rows_selected(data));
        let result =
            handler.write_json_file(&path, Box::new(std::iter::once(filtered_data)), overwrite);

        if overwrite {
            // Verify the second write is successful
            assert!(result.is_ok());
            let json = read_json_file(&store, &object_path).await?;
            assert_eq!(json, vec![json!({"dog": "seb"}), json!({"dog": "tia"})]);
        } else {
            // Verify the second write fails with FileAlreadyExists error
            match result {
                Err(Error::FileAlreadyExists(err_path)) => {
                    assert_eq!(err_path, object_path.to_string());
                }
                _ => panic!("Expected FileAlreadyExists error, got: {result:?}"),
            }
        }

        Ok(())
    }
}
