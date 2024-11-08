use std::{fs::File, io::BufReader, io::Write};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use tempfile::NamedTempFile;
use url::Url;

use super::read_files;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::parse_json as arrow_parse_json;
use crate::engine::arrow_utils::to_json_bytes;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, ExpressionRef, FileDataReadResultIterator, FileMeta,
    JsonHandler,
};

pub(crate) struct SyncJsonHandler;

fn try_create_from_json(
    file: File,
    _schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    _predicate: Option<ExpressionRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let json = arrow_json::ReaderBuilder::new(arrow_schema)
        .build(BufReader::new(file))?
        .map(|data| Ok(ArrowEngineData::new(data?)));
    Ok(json)
}

impl JsonHandler for SyncJsonHandler {
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        read_files(files, schema, predicate, try_create_from_json)
    }

    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    // For sync writer we write data to a tmp file then atomically rename it to the final path.
    // This is highly OS-dependent and for now relies on the atomicity of tempfile's
    // `persist_noclobber`.
    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + '_>,
        _overwrite: bool,
    ) -> DeltaResult<()> {
        let path = path
            .to_file_path()
            .map_err(|_| crate::Error::generic("sync client can only read local files"))?;
        let Some(parent) = path.parent() else {
            return Err(crate::Error::generic(format!(
                "no parent found for {:?}",
                path
            )));
        };

        // write data to tmp file
        let mut tmp_file = NamedTempFile::new_in(parent)?;
        let buf = to_json_bytes(data)?;
        tmp_file.write_all(&buf)?;
        tmp_file.flush()?;

        // use 'persist_noclobber' to atomically rename tmp file to final path
        tmp_file
            .persist_noclobber(path.clone())
            .map_err(|e| match e {
                tempfile::PersistError { error, .. }
                    if error.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    Error::FileAlreadyExists(path.to_string_lossy().to_string())
                }
                e => Error::IOError(e.into()),
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field;
    use arrow_schema::Schema as ArrowSchema;
    use serde_json::json;
    use url::Url;

    #[test]
    fn test_write_json_file() -> DeltaResult<()> {
        let test_dir = tempfile::tempdir().unwrap();
        let path = test_dir.path().join("00000000000000000001.json");
        let handler = SyncJsonHandler;

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "dog",
            ArrowDataType::Utf8,
            true,
        )]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["remi", "wilson"]))],
        )?;
        let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(data));
        let empty: Box<dyn EngineData> =
            Box::new(ArrowEngineData::new(RecordBatch::new_empty(schema)));

        let url = Url::from_file_path(path.clone()).unwrap();
        handler
            .write_json_file(&url, Box::new(std::iter::once(Ok(data))), false)
            .expect("write json file");
        assert!(matches!(
            handler.write_json_file(&url, Box::new(std::iter::once(Ok(empty))), false),
            Err(Error::FileAlreadyExists(_))
        ));

        let file = std::fs::read_to_string(path)?;
        let json: Vec<_> = serde_json::Deserializer::from_str(&file)
            .into_iter::<serde_json::Value>()
            .flatten()
            .collect();
        assert_eq!(
            json,
            vec![json!({"dog": "remi"}), json!({"dog": "wilson"}),]
        );

        Ok(())
    }
}
