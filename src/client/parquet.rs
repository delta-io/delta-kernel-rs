//! Default Parquet handler implementation

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use chrono::{TimeZone, Utc};
use futures::stream::TryStreamExt;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectMeta};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::{DeltaResult, Expression, FileDataReadResult, FileHandler, FileMeta, ParquetHandler};

#[derive(Debug)]
pub struct ParquetReadContext {
    pub(crate) reader: ParquetObjectReader,
    pub(crate) meta: FileMeta,
}

#[derive(Debug)]
pub struct DefaultParquetHandler {
    store: Arc<DynObjectStore>,
}

impl DefaultParquetHandler {
    pub fn new(store: Arc<DynObjectStore>) -> Self {
        Self { store }
    }
}

impl FileHandler for DefaultParquetHandler {
    type FileReadContext = ParquetReadContext;

    fn contextualize_file_reads(
        &self,
        files: Vec<FileMeta>,
        _predicate: Option<Expression>,
    ) -> DeltaResult<Vec<ParquetReadContext>> {
        Ok(files
            .into_iter()
            .map(|meta| ParquetReadContext {
                reader: ParquetObjectReader::new(
                    self.store.clone(),
                    ObjectMeta {
                        location: Path::from(meta.location.path()),
                        size: meta.size,
                        last_modified: Utc.timestamp_millis_opt(meta.last_modified).unwrap(),
                        e_tag: None,
                    },
                ),
                meta,
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl ParquetHandler for DefaultParquetHandler {
    async fn read_parquet_files(
        &self,
        files: Vec<ParquetReadContext>,
        _physical_schema: SchemaRef,
    ) -> DeltaResult<Vec<FileDataReadResult>> {
        let mut results = Vec::new();
        // TODO run on threads
        for context in files {
            let builder = ParquetRecordBatchStreamBuilder::new(context.reader).await?;
            let schema = Arc::new(
                builder
                    .schema()
                    .as_ref()
                    .clone()
                    // FIXME right now needed due to errors handling spark metadata
                    .with_metadata(HashMap::default()),
            );
            let batches = builder.build()?.try_collect::<Vec<_>>().await?;
            // TODO align with passed physical schema
            let batch = concat_batches(&schema, &batches)?;
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

    #[tokio::test]
    async fn test_read_json_files() {
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

        let files = vec![FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp(),
            size: meta.size,
        }];

        let handler = DefaultParquetHandler::new(store);
        let context = handler.contextualize_file_reads(files, None).unwrap();

        let data = handler
            .read_parquet_files(context, physical_schema)
            .await
            .unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].0.location, url);
        assert_eq!(data[0].1.num_rows(), 10);
    }
}
