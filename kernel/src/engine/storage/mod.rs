//! Native Arrow execution over a connector-supplied synchronous storage handler.

use std::io::{BufReader, Read};
use std::sync::Arc;

use bytes::Bytes;
use url::Url;

use crate::arrow::json::ReaderBuilder;
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_expression::ArrowEvaluationHandler;
use crate::engine::arrow_utils::{
    build_json_reorder_indices, fixup_json_read, fixup_parquet_read, json_arrow_schema,
    ordering_needs_row_indexes, parquet_read_plan, parse_json, to_json_bytes, RowIndexBuilder,
};
use crate::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use crate::engine::{reader_options, writer_options};
use crate::engine_data::FilteredEngineData;
use crate::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::parquet::errors::ParquetError;
use crate::parquet::file::reader::{ChunkReader, Length};
use crate::schema::{SchemaRef, StructType};
use crate::{
    Engine, EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, FileSize,
    JsonHandler, KernelError, ParquetFooter, ParquetHandler, PredicateRef, Result, ResultIterator,
    ResultIteratorStatic, StorageHandler,
};

/// An Arrow engine that delegates only raw I/O to the connector.
///
/// Parsing, expression evaluation, and log replay run in Rust. Storage calls, including lazy
/// iterator reads, run synchronously on the thread invoking the kernel operation. This engine
/// does not expose a declarative plan executor.
pub struct StorageEngine {
    storage: Arc<dyn StorageHandler>,
    formats: Arc<StorageFormats>,
}

impl StorageEngine {
    /// Construct an engine using `storage` for all file access.
    ///
    /// The handler must implement ordered listing, byte reads, stat, and atomic writes for the
    /// operations the connector uses. This constructor performs no I/O.
    pub fn new(storage: Arc<dyn StorageHandler>) -> Self {
        Self {
            formats: Arc::new(StorageFormats(storage.clone())),
            storage,
        }
    }
}

impl Engine for StorageEngine {
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.formats.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.formats.clone()
    }

    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        Arc::new(ArrowEvaluationHandler)
    }
}

struct StorageFormats(Arc<dyn StorageHandler>);

impl JsonHandler for StorageFormats {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> Result<Box<dyn EngineData>> {
        parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> Result<FileDataReadResultIterator> {
        let storage = self.0.clone();
        let files = files.to_vec();
        Ok(Box::new(files.into_iter().flat_map(move |file| {
            json_batches(storage.clone(), file, schema.clone())
                .unwrap_or_else(|error| Box::new(std::iter::once(Err(error))))
        })))
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: ResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> Result<FileSize> {
        let bytes = to_json_bytes(data)?;
        let size = bytes.len() as FileSize;
        self.0.put(path, bytes.into(), overwrite)?;
        Ok(size)
    }
}

impl ParquetHandler for StorageFormats {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Result<FileDataReadResultIterator> {
        let storage = self.0.clone();
        let files = files.to_vec();
        Ok(Box::new(files.into_iter().flat_map(move |file| {
            parquet_batches(storage.clone(), file, schema.clone(), predicate.clone())
                .unwrap_or_else(|error| Box::new(std::iter::once(Err(error))))
        })))
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> Result<ParquetFooter> {
        let reader = StorageChunkReader::new(self.0.clone(), file.clone());
        let metadata = ArrowReaderMetadata::load(&reader, reader_options())?;
        Ok(ParquetFooter {
            schema: Arc::new(StructType::try_from_arrow(metadata.schema().as_ref())?),
        })
    }

    fn write_parquet_file(
        &self,
        location: Url,
        mut data: ResultIteratorStatic<Box<dyn EngineData>>,
    ) -> Result<FileSize> {
        let first = data.next().ok_or_else(|| {
            KernelError::generic("Cannot write parquet file with empty data iterator")
        })??;
        let first: crate::arrow::record_batch::RecordBatch =
            ArrowEngineData::try_from_engine_data(first)?.into();
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buffer, first.schema(), writer_options())?;
        writer.write(&first)?;
        for batch in data {
            writer.write(&ArrowEngineData::try_from_engine_data(batch?)?.into())?;
        }
        writer.close()?;
        let size = buffer.len() as FileSize;
        self.0.put(&location, buffer.into(), true)?;
        Ok(size)
    }
}

fn json_batches(
    storage: Arc<dyn StorageHandler>,
    file: FileMeta,
    schema: SchemaRef,
) -> Result<FileDataReadResultIterator> {
    let ordering = build_json_reorder_indices(&schema)?;
    let json_schema = Arc::new(json_arrow_schema(&schema)?);
    let reader = StorageChunkReader::new(storage, file.clone()).get_read(0)?;
    let mut batches = ReaderBuilder::new(json_schema)
        .with_coerce_primitive(true)
        .with_batch_size(1024)
        .build(reader)?;
    // Arrow's JSON reader may repeat a parse error; yield it once and stop this file.
    let mut failed = false;
    Ok(Box::new(std::iter::from_fn(move || {
        if failed {
            return None;
        }
        let result = batches.next().map(|batch| {
            Ok(
                Box::new(fixup_json_read(batch?, &ordering, file.location.as_str())?)
                    as Box<dyn EngineData>,
            )
        });
        failed = result.as_ref().is_some_and(Result::is_err);
        result
    })))
}

fn parquet_batches(
    storage: Arc<dyn StorageHandler>,
    file: FileMeta,
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
) -> Result<FileDataReadResultIterator> {
    let reader = StorageChunkReader::new(storage, file.clone());
    let metadata = ArrowReaderMetadata::load(&reader, reader_options())?;
    let (ordering, mask) = parquet_read_plan(&schema, &metadata)?;
    let mut row_indexes = ordering_needs_row_indexes(&ordering)
        .then(|| RowIndexBuilder::new(metadata.metadata().row_groups()));
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(reader, metadata);
    if let Some(mask) = mask {
        builder = builder.with_projection(mask);
    }
    if let Some(predicate) = predicate {
        builder = builder.with_row_group_filter(predicate.as_ref(), row_indexes.as_mut());
    }
    let mut row_indexes = row_indexes.map(|indexes| indexes.build()).transpose()?;
    let batches = builder.with_batch_size(1024).build()?;
    Ok(Box::new(batches.map(move |batch| {
        Ok(Box::new(fixup_parquet_read(
            batch?,
            &ordering,
            row_indexes.as_mut(),
            Some(file.location.as_str()),
            Some(&schema),
        )?) as Box<dyn EngineData>)
    })))
}

#[derive(Clone)]
struct StorageChunkReader {
    storage: Arc<dyn StorageHandler>,
    file: FileMeta,
}

impl StorageChunkReader {
    fn new(storage: Arc<dyn StorageHandler>, file: FileMeta) -> Self {
        Self { storage, file }
    }
}

impl Length for StorageChunkReader {
    fn len(&self) -> u64 {
        self.file.size
    }
}

impl ChunkReader for StorageChunkReader {
    type T = BufReader<StorageReader>;

    fn get_read(&self, start: u64) -> crate::parquet::errors::Result<Self::T> {
        if start > self.len() {
            return Err(ParquetError::EOF("read starts beyond file size".into()));
        }
        Ok(BufReader::with_capacity(
            64 * 1024,
            StorageReader {
                reader: self.clone(),
                offset: start,
            },
        ))
    }

    fn get_bytes(&self, start: u64, length: usize) -> crate::parquet::errors::Result<Bytes> {
        let end = start
            .checked_add(length as u64)
            .filter(|end| *end <= self.len())
            .ok_or_else(|| ParquetError::EOF("byte range exceeds file size".into()))?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        read_one(&self.storage, &self.file.location, Some(start..end))
            .map_err(|error| ParquetError::External(Box::new(error)))
    }
}

struct StorageReader {
    reader: StorageChunkReader,
    offset: u64,
}

impl Read for StorageReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let length = buffer.len().min((self.reader.len() - self.offset) as usize);
        if length == 0 {
            return Ok(0);
        }
        let bytes = self
            .reader
            .get_bytes(self.offset, length)
            .map_err(std::io::Error::other)?;
        buffer[..length].copy_from_slice(&bytes);
        self.offset += length as u64;
        Ok(length)
    }
}

fn read_one(
    storage: &Arc<dyn StorageHandler>,
    location: &Url,
    range: Option<std::ops::Range<u64>>,
) -> Result<Bytes> {
    let mut result = storage.read_files(vec![(location.clone(), range.clone())])?;
    let bytes = result
        .next()
        .ok_or_else(|| KernelError::generic("storage returned no byte buffer"))??;
    if let Some(extra) = result.next() {
        extra?;
        return Err(KernelError::generic(
            "storage returned more than one byte buffer",
        ));
    }
    if range.is_some_and(|range| bytes.len() as u64 != range.end - range.start) {
        return Err(KernelError::generic(
            "storage returned an incomplete byte range",
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[derive(Clone, Copy)]
    enum ReadFault {
        None,
        Call,
        Empty,
        FirstItem,
        ExtraBuffer,
        ExtraError,
        ShortRange,
    }

    // Adapt the test-only SyncEngine, which reads whole files, to the byte-range storage contract.
    struct RangedStorage(Arc<dyn StorageHandler>, ReadFault);
    impl StorageHandler for RangedStorage {
        fn read_files(&self, files: Vec<crate::FileSlice>) -> Result<ResultIteratorStatic<Bytes>> {
            let error = || KernelError::generic("connector read failure");
            let response = match self.1 {
                ReadFault::None => None,
                ReadFault::Call => return Err(error()),
                ReadFault::Empty => Some(vec![]),
                ReadFault::FirstItem => Some(vec![Err(error())]),
                ReadFault::ExtraBuffer => Some(vec![Ok(Bytes::new()), Ok(Bytes::new())]),
                ReadFault::ExtraError => Some(vec![Ok(Bytes::new()), Err(error())]),
                ReadFault::ShortRange => Some(vec![Ok(Bytes::from_static(b"x"))]),
            };
            if let Some(response) = response {
                return Ok(Box::new(response.into_iter()));
            }
            let results = files
                .into_iter()
                .map(|(url, range)| {
                    let bytes = read_one(&self.0, &url, None)?;
                    Ok(range.map_or(bytes.clone(), |r| {
                        bytes.slice(r.start as usize..r.end as usize)
                    }))
                })
                .collect::<Vec<_>>();
            Ok(Box::new(results.into_iter()))
        }
        fn list_from(&self, path: &Url) -> Result<ResultIteratorStatic<FileMeta>> {
            self.0.list_from(path)
        }
        fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> Result<()> {
            self.0.put(path, data, overwrite)
        }
        fn head(&self, path: &Url) -> Result<FileMeta> {
            self.0.head(path)
        }
        fn copy_atomic(&self, src: &Url, dst: &Url) -> Result<()> {
            self.0.copy_atomic(src, dst)
        }
        fn delete(&self, path: &Url) -> Result<()> {
            self.0.delete(path)
        }
    }
    fn engine() -> StorageEngine {
        let storage = crate::engine::sync::SyncEngine::new().storage_handler();
        StorageEngine::new(Arc::new(RangedStorage(storage, ReadFault::None)))
    }

    #[rstest]
    #[case(ReadFault::Call, "connector read failure")]
    #[case(ReadFault::Empty, "storage returned no byte buffer")]
    #[case(ReadFault::FirstItem, "connector read failure")]
    #[case(ReadFault::ExtraBuffer, "storage returned more than one byte buffer")]
    #[case(ReadFault::ExtraError, "connector read failure")]
    #[case(ReadFault::ShortRange, "storage returned an incomplete byte range")]
    fn byte_reads_reject_invalid_connector_responses(
        #[case] fault: ReadFault,
        #[case] expected: &str,
    ) {
        let storage: Arc<dyn StorageHandler> = Arc::new(RangedStorage(
            crate::engine::sync::SyncEngine::new().storage_handler(),
            fault,
        ));
        let url = Url::parse("memory:///bytes").unwrap();
        let error = read_one(&storage, &url, Some(0..2)).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        let reader = StorageChunkReader::new(
            storage,
            FileMeta {
                location: url,
                last_modified: 0,
                size: 2,
            },
        );
        let mut sequential = reader.get_read(0).unwrap();
        let error = sequential.read(&mut [0; 2]).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }

    #[test]
    fn chunk_reader_obeys_ranges_eof_and_empty_reads() {
        let directory = tempfile::tempdir().unwrap();
        let url = Url::from_file_path(directory.path().join("bytes")).unwrap();
        let engine = engine();
        let storage = engine.storage_handler();
        storage
            .put(&url, Bytes::from_static(b"0123456789"), false)
            .unwrap();
        let reader = StorageChunkReader::new(storage.clone(), storage.head(&url).unwrap());
        assert_eq!(reader.get_bytes(3, 4).unwrap().as_ref(), b"3456");
        assert!(reader.get_bytes(u64::MAX, 2).is_err());
        assert!(reader.get_bytes(9, 2).is_err());
        assert!(reader.get_read(11).is_err());
        assert!(reader.get_bytes(10, 0).unwrap().is_empty());
        let mut sequential = reader.get_read(7).unwrap();
        assert_eq!(sequential.read(&mut []).unwrap(), 0);
        let mut remaining = Vec::new();
        sequential.read_to_end(&mut remaining).unwrap();
        assert_eq!(remaining, b"789");
        assert_eq!(sequential.read(&mut [0]).unwrap(), 0);
    }
}
