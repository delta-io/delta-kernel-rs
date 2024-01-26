use crate::{ParquetHandler, FileMeta, schema::SchemaRef, Expression, DeltaResult, FileDataReadResultIterator};


pub(crate) struct SimpleParquetHandler {}

impl ParquetHandler for SimpleParquetHandler {
    fn read_parquet_files(
         &self,
         files: &[FileMeta],
         physical_schema: SchemaRef,
         predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        Ok(Box::new(std::iter::empty()))
    }
}
