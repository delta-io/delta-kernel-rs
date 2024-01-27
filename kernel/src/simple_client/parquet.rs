use crate::{
    schema::SchemaRef, DeltaResult, EngineData, Expression, FileDataReadResultIterator, FileMeta,
    ParquetHandler,
};

pub(crate) struct SimpleParquetHandler {}

impl ParquetHandler for SimpleParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        _predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let mut res = vec![];
        for file in files.iter() {
            let d = super::data::SimpleData::try_create_from_parquet(
                schema.clone(),
                file.location.clone(),
            )?;
            let b: Box<dyn EngineData> = Box::new(d);
            res.push(Ok(b));
        }
        Ok(Box::new(res.into_iter()))
    }
}
