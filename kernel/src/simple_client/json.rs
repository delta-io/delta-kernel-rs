use std::{io::Cursor, sync::Arc};

use crate::{
    schema::SchemaRef, DeltaResult, EngineData, Error, Expression, FileDataReadResultIterator,
    FileMeta, JsonHandler,
};
use arrow_array::cast::AsArray;
use arrow_json::ReaderBuilder;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_select::concat::concat_batches;
use itertools::Itertools;
use tracing::debug;

use super::data::SimpleData;

pub(crate) struct SimpleJsonHandler {}
impl JsonHandler for SimpleJsonHandler {
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        _predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        debug!("Reading json files: {:#?}", files);
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let res: Vec<_> = files
            .iter()
            .map(|file| {
                let d = super::data::SimpleData::try_create_from_json(
                    schema.clone(),
                    file.location.clone(),
                );
                d.map(|d| Box::new(d) as _)
            })
            .collect();
        Ok(Box::new(res.into_iter()))
    }

    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // TODO: This is taken from the default client as it's the same. We should share an
        // implementation at some point
        let json_strings = SimpleData::try_from_engine_data(json_strings)?.into_record_batch();
        if json_strings.num_columns() != 1 {
            return Err(Error::missing_column("Expected single column"));
        }
        let json_strings =
            json_strings
                .column(0)
                .as_string_opt::<i32>()
                .ok_or(Error::unexpected_column_type(
                    "Expected column to be String",
                ))?;

        let data: Vec<_> = json_strings
            .into_iter()
            .filter_map(|d| {
                d.map(|dd| {
                    let mut data = dd.as_bytes().to_vec();
                    data.extend("\n".as_bytes());
                    data
                })
            })
            .flatten()
            .collect();

        let schema: ArrowSchemaRef = Arc::new(output_schema.as_ref().try_into()?);
        let batches: Vec<_> = ReaderBuilder::new(schema.clone())
            .build(Cursor::new(data))?
            .try_collect()?;
        Ok(Box::new(SimpleData::new(concat_batches(&schema, &batches)?)) as _)
    }
}
