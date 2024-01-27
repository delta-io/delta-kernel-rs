use arrow_array::{RecordBatch, StringArray};
use arrow_schema::SchemaRef as ArrowSchemaRef;

use crate::{
    schema::SchemaRef, DeltaResult, EngineData, Expression, FileDataReadResultIterator, FileMeta,
    JsonHandler,
};

pub(crate) struct SimpleJsonHandler {}
impl JsonHandler for SimpleJsonHandler {
    fn read_json_files(
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
            let d = super::data::SimpleData::try_create_from_json(
                schema.clone(),
                file.location.clone(),
            )?;
            let b: Box<dyn EngineData> = Box::new(d);
            res.push(Ok(b));
        }
        Ok(Box::new(res.into_iter()))
    }

    fn parse_json(
        &self,
        _json_strings: StringArray,
        _output_schema: ArrowSchemaRef,
    ) -> DeltaResult<RecordBatch> {
        unimplemented!();
    }
}
