//! Default Json handler implementation

use std::io::{BufRead, BufReader, Cursor};
use std::ops::Range;
use std::sync::Arc;
use std::task::{ready, Poll};

use arrow_array::{new_null_array, Array, RecordBatch, StringArray};
use arrow_json::{reader::Decoder, ReaderBuilder};
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use arrow_select::concat::concat_batches;
use bytes::{Buf, Bytes};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, GetResultPayload};

use super::executor::TaskExecutor;
use super::file_handler::{FileOpenFuture, FileOpener, FileStream};
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, Expression, FileDataReadResultIterator, FileMeta, JsonHandler};

#[derive(Debug)]
pub struct DefaultJsonHandler<E: TaskExecutor> {
    /// The object store to read files from
    store: Arc<DynObjectStore>,
    /// The executor to run async tasks on
    task_executor: Arc<E>,
    /// The maximun number of batches to read ahead
    readahead: usize,
    /// The number of rows to read per batch
    batch_size: usize,
}

impl<E: TaskExecutor> DefaultJsonHandler<E> {
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            store,
            task_executor,
            readahead: 10,
            batch_size: 1024,
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

fn read_from_json<R: BufRead>(
    mut reader: R,
    decoder: &mut Decoder,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let mut next = move || {
        loop {
            // Decoder is agnostic that buf doesn't contain whole records
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break; // Input exhausted
            }
            let read = buf.len();
            let decoded = decoder.decode(buf)?;

            // Consume the number of bytes read
            reader.consume(decoded);
            if decoded != read {
                break; // Read batch size
            }
        }
        decoder.flush()
    };
    std::iter::from_fn(move || next().transpose()).collect::<Result<Vec<_>, _>>()
}

fn insert_nulls(
    batches: &mut Vec<RecordBatch>,
    null_count: usize,
    schema: ArrowSchemaRef,
) -> Result<(), ArrowError> {
    let columns = schema
        .fields
        .iter()
        .map(|field| new_null_array(field.data_type(), null_count))
        .collect();
    batches.push(RecordBatch::try_new(schema, columns)?);
    Ok(())
}

#[inline]
fn get_reader(data: &[u8]) -> BufReader<Cursor<&[u8]>> {
    BufReader::new(Cursor::new(data))
}

impl<E: TaskExecutor> JsonHandler for DefaultJsonHandler<E> {
    fn parse_json(
        &self,
        json_strings: StringArray,
        output_schema: SchemaRef,
    ) -> DeltaResult<RecordBatch> {
        let json_strings = json_strings
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::Generic(format!(
                    "Expected json_strings to be a StringArray, found {:?}",
                    json_strings
                ))
            })?;

        let output_schema: ArrowSchemaRef = Arc::new(output_schema.as_ref().try_into()?);
        let mut decoder = ReaderBuilder::new(output_schema.clone())
            .with_batch_size(self.batch_size)
            .build_decoder()?;
        let mut batches = Vec::new();

        let mut null_count = 0;
        let mut value_count = 0;
        let mut value_start = 0;

        for it in 0..json_strings.len() {
            if json_strings.is_null(it) {
                if value_count > 0 {
                    let slice = json_strings.slice(value_start, value_count);
                    let batch = read_from_json(get_reader(slice.value_data()), &mut decoder)?;
                    batches.extend(batch);
                    value_count = 0;
                }
                null_count += 1;
                continue;
            }
            if value_count == 0 {
                value_start = it;
            }
            if null_count > 0 {
                insert_nulls(&mut batches, null_count, output_schema.clone())?;
                null_count = 0;
            }
            value_count += 1;
        }

        if null_count > 0 {
            insert_nulls(&mut batches, null_count, output_schema.clone())?;
        }

        if value_count > 0 {
            let slice = json_strings.slice(value_start, value_count);
            let batch = read_from_json(get_reader(slice.value_data()), &mut decoder)?;
            batches.extend(batch);
        }

        Ok(concat_batches(&output_schema, &batches)?)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into()?);
        let store = self.store.clone();
        let file_reader = JsonOpener::new(self.batch_size, schema.clone(), store);

        let files = files.to_vec();
        let stream = FileStream::new(files, schema, file_reader)?;

        // This channel will become the output iterator
        // The stream will execute in the background, and we allow up to `readahead`
        // batches to be buffered in the channel.
        let (sender, receiver) = std::sync::mpsc::sync_channel(self.readahead);

        self.task_executor.spawn(stream.for_each(move |res| {
            sender.send(res).ok();
            futures::future::ready(())
        }));

        Ok(Box::new(receiver.into_iter()))
    }
}

/// A [`FileOpener`] that opens a JSON file and yields a [`FileOpenFuture`]
#[allow(missing_debug_implementations)]
pub struct JsonOpener {
    batch_size: usize,
    projected_schema: ArrowSchemaRef,
    object_store: Arc<DynObjectStore>,
}

impl JsonOpener {
    /// Returns a  [`JsonOpener`]
    pub fn new(
        batch_size: usize,
        projected_schema: ArrowSchemaRef,
        // file_compression_type: FileCompressionType,
        object_store: Arc<DynObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            // file_compression_type,
            object_store,
        }
    }
}

impl FileOpener for JsonOpener {
    fn open(&self, file_meta: FileMeta, _: Option<Range<i64>>) -> DeltaResult<FileOpenFuture> {
        let store = self.object_store.clone();
        let schema = self.projected_schema.clone();
        let batch_size = self.batch_size;

        Ok(Box::pin(async move {
            let path = Path::from(file_meta.location.path());
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
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use arrow_schema::Schema as ArrowSchema;
    use itertools::Itertools;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use super::*;
    use crate::{actions::schemas::log_schema, executor::tokio::TokioBackgroundExecutor};

    #[test]
    fn test_parse_json() {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = Arc::new(log_schema().clone());

        let batch = handler.parse_json(json_strings, output_schema).unwrap();
        assert_eq!(batch.num_rows(), 4);
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
        let physical_schema = Arc::new(ArrowSchema::try_from(log_schema()).unwrap());
        let data: Vec<RecordBatch> = handler
            .read_json_files(files, Arc::new(physical_schema.try_into().unwrap()), None)
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 4);
    }
}
