use arrow::record_batch::RecordBatch;
use bytes::Bytes;

/// A type that can parse parquet from bytes. FIXME this should take a path so the engine can
/// optimize better.
pub trait ParquetReader {
    type Iter: Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>;

    fn read(&self, bytes: Bytes) -> Self::Iter;
}

pub(crate) mod arrow_parquet_reader {
    use super::*;
    use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

    #[derive(Debug, Default)]
    pub(crate) struct ArrowParquetReader;

    impl ParquetReader for ArrowParquetReader {
        type Iter = ParquetRecordBatchReader;

        fn read(&self, bytes: Bytes) -> Self::Iter {
            ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap()
        }
    }
}
