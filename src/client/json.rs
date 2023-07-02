//! Default Json handler implementation

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_json::ReaderBuilder;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use object_store::path::Path;
use object_store::DynObjectStore;

use crate::{DeltaResult, Expression, FileDataReadResult, FileHandler, FileMeta, JsonHandler};

#[derive(Debug)]
pub struct JsonReadContext {
    pub(crate) store: Arc<DynObjectStore>,
    pub(crate) path: Path,
    pub(crate) meta: FileMeta,
}

#[derive(Debug)]
pub struct DefaultJsonHandler {
    store: Arc<DynObjectStore>,
}

impl DefaultJsonHandler {
    pub fn new(store: Arc<DynObjectStore>) -> Self {
        Self { store }
    }
}

impl FileHandler for DefaultJsonHandler {
    type FileReadContext = JsonReadContext;

    fn contextualize_file_reads(
        &self,
        files: Vec<FileMeta>,
        _predicate: Option<Expression>,
    ) -> DeltaResult<Vec<JsonReadContext>> {
        Ok(files
            .into_iter()
            .map(|meta| JsonReadContext {
                store: self.store.clone(),
                path: Path::from(meta.location.path()),
                meta,
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl JsonHandler for DefaultJsonHandler {
    fn parse_json(
        &self,
        json_strings: StringArray,
        output_schema: SchemaRef,
    ) -> DeltaResult<RecordBatch> {
        // TODO concatenating to a single string is probaly not needed if we use the
        // lower level RawDecoder APIs
        let data = json_strings
            .into_iter()
            .filter_map(|d| {
                d.map(|dd| {
                    let mut data = dd.as_bytes().to_vec();
                    data.extend("\n".as_bytes());
                    data
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        let batches = ReaderBuilder::new(output_schema.clone())
            .build(Cursor::new(data))?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(concat_batches(&output_schema, &batches)?)
    }

    async fn read_json_files(
        &self,
        files: Vec<JsonReadContext>,
        physical_schema: SchemaRef,
    ) -> DeltaResult<Vec<FileDataReadResult>> {
        let mut results = Vec::new();
        // TODO run on threads
        for context in files {
            let raw = context.store.get(&context.path).await?.bytes().await?;
            let data = ReaderBuilder::new(physical_schema.clone())
                .build(raw.as_ref())?
                .collect::<Result<Vec<_>, _>>()?;
            let batch = concat_batches(&physical_schema, &data)?;
            results.push((context.meta, batch));
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use object_store::{local::LocalFileSystem, ObjectStore};

    use super::*;
    use crate::actions::get_log_schema;

    #[test]
    fn test_parse_json() {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultJsonHandler::new(store);

        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = Arc::new(get_log_schema());

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

        let files = vec![FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp(),
            size: meta.size,
        }];

        let handler = DefaultJsonHandler::new(store);
        let context = handler.contextualize_file_reads(files, None).unwrap();

        let physical_schema = Arc::new(get_log_schema());
        let data = handler
            .read_json_files(context, physical_schema)
            .await
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].0.location, url);
        assert_eq!(data[0].1.num_rows(), 4);
    }
}
