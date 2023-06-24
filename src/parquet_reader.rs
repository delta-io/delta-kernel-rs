use std::pin::Pin;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use bytes::Bytes;
use futures::stream::Stream;

use crate::DeltaResult;

/// Trait for types that stream [arrow::record_batch::RecordBatch]
pub trait RecordBatchStream: Stream<Item = DeltaResult<RecordBatch>> {
    /// Returns the schema of this [`RecordBatchStream`].
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

/// Trait for a stream of record batches.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

/// A type that can parse parquet from bytes. FIXME this should take a path so the engine can
/// optimize better.
pub trait ParquetReader {
    type Iter: Iterator<Item = Result<RecordBatch, ArrowError>>;

    fn read(&self, bytes: Bytes) -> DeltaResult<Self::Iter>;
}

pub(crate) mod arrow_parquet_reader {
    use super::*;
    use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

    #[derive(Debug, Default)]
    pub(crate) struct ArrowParquetReader;

    impl ParquetReader for ArrowParquetReader {
        type Iter = ParquetRecordBatchReader;

        fn read(&self, bytes: Bytes) -> DeltaResult<Self::Iter> {
            Ok(ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?)
        }
    }
}
