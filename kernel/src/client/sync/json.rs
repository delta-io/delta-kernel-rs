use std::{
    fs::File,
    io::{BufReader, Cursor},
    sync::Arc,
};

use crate::{
    schema::SchemaRef, DeltaResult, EngineData, Error, Expression, FileDataReadResultIterator,
    FileMeta, JsonHandler,
};
use arrow_array::{cast::AsArray, RecordBatch};
use arrow_json::ReaderBuilder;
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow_select::concat::concat_batches;
use itertools::Itertools;
use tracing::debug;
use url::Url;

use crate::client::arrow_data::ArrowEngineData;

pub(crate) struct SyncJsonHandler;

fn try_create_from_json(schema: SchemaRef, location: Url) -> DeltaResult<ArrowEngineData> {
    let arrow_schema: ArrowSchema = (&*schema).try_into()?;
    debug!("Reading {:#?} with schema: {:#?}", location, arrow_schema);
    let file = File::open(
        location
            .to_file_path()
            .map_err(|_| Error::generic("can only read local files"))?,
    )?;
    let mut json =
        arrow_json::ReaderBuilder::new(Arc::new(arrow_schema)).build(BufReader::new(file))?;
    let data = json
        .next()
        .ok_or(Error::generic("No data found reading json file"))?;
    Ok(ArrowEngineData::new(data?))
}

impl JsonHandler for SyncJsonHandler {
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
                try_create_from_json(schema.clone(), file.location.clone())
                    .map(|d| Box::new(d) as _)
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
        let json_strings: RecordBatch = ArrowEngineData::try_from_engine_data(json_strings)?.into();
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
        Ok(Box::new(ArrowEngineData::new(concat_batches(&schema, &batches)?)) as _)
    }
}
