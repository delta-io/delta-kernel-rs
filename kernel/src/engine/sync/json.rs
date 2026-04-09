use std::io::{BufReader, Cursor, Write};
use std::sync::Arc;

use bytes::Bytes;

use crate::arrow::json::ReaderBuilder;
use tempfile::NamedTempFile;
use url::Url;

use super::{read_files, read_files_from_store};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{
    build_json_reorder_indices, fixup_json_read, json_arrow_schema, parse_json as arrow_parse_json,
    to_json_bytes,
};
use crate::engine_data::FilteredEngineData;
use crate::object_store::DynObjectStore;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, FileDataReadResultIterator, FileMeta, JsonHandler, PredicateRef,
};

pub(crate) struct SyncJsonHandler {
    store: Option<Arc<DynObjectStore>>,
}

impl SyncJsonHandler {
    pub(crate) fn new() -> Self {
        Self { store: None }
    }

    pub(crate) fn with_store(store: Arc<DynObjectStore>) -> Self {
        Self { store: Some(store) }
    }
}

fn try_create_from_json(
    file: std::fs::File,
    schema: SchemaRef,
    _predicate: Option<PredicateRef>,
    file_location: String,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let json_schema = Arc::new(json_arrow_schema(&schema)?);
    let reorder_indices = build_json_reorder_indices(&schema)?;
    let json = ReaderBuilder::new(json_schema)
        .with_coerce_primitive(true)
        .build(BufReader::new(file))?
        .map(move |data| fixup_json_read(data?, &reorder_indices, &file_location));
    Ok(json)
}

fn try_create_from_json_bytes(
    data: Bytes,
    schema: SchemaRef,
    _predicate: Option<PredicateRef>,
    file_location: String,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let json_schema = Arc::new(json_arrow_schema(&schema)?);
    let reorder_indices = build_json_reorder_indices(&schema)?;
    let json = ReaderBuilder::new(json_schema)
        .with_coerce_primitive(true)
        .build(BufReader::new(Cursor::new(data)))?
        .map(move |data| fixup_json_read(data?, &reorder_indices, &file_location));
    Ok(json)
}

impl JsonHandler for SyncJsonHandler {
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if let Some(store) = &self.store {
            return read_files_from_store(
                files,
                schema,
                predicate,
                store.clone(),
                try_create_from_json_bytes,
            );
        }
        read_files(files, schema, predicate, try_create_from_json)
    }

    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        let buf = to_json_bytes(data)?;

        if let Some(store) = &self.store {
            let object_path = crate::object_store::path::Path::from(path.path());
            let opts = if overwrite {
                crate::object_store::PutOptions::default()
            } else {
                crate::object_store::PutOptions {
                    mode: crate::object_store::PutMode::Create,
                    ..Default::default()
                }
            };
            super::block_on_async(store.put_opts(&object_path, buf.into(), opts)).map_err(|e| {
                match e {
                    crate::object_store::Error::AlreadyExists { .. } => {
                        Error::FileAlreadyExists(path.to_string())
                    }
                    other => Error::generic(other.to_string()),
                }
            })?;
            return Ok(());
        }

        // Local filesystem path
        let path = path
            .to_file_path()
            .map_err(|_| crate::Error::generic("sync client can only read local files"))?;
        let Some(parent) = path.parent() else {
            return Err(crate::Error::generic(format!(
                "no parent found for {path:?}"
            )));
        };

        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }

        let mut tmp_file = NamedTempFile::new_in(parent)?;
        tmp_file.write_all(&buf)?;
        tmp_file.flush()?;

        let persist_result = if overwrite {
            tmp_file.persist(path.clone())
        } else {
            tmp_file.persist_noclobber(path.clone())
        };

        persist_result.map_err(|e| {
            if !overwrite && e.error.kind() == std::io::ErrorKind::AlreadyExists {
                Error::FileAlreadyExists(path.to_string_lossy().to_string())
            } else {
                Error::IOError(e.into())
            }
        })?;

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use serde_json::json;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;
    use url::Url;

    // Helper function to create test data
    fn create_test_data(values: Vec<&str>) -> DeltaResult<Box<dyn EngineData>> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "dog",
            ArrowDataType::Utf8,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])?;
        Ok(Box::new(ArrowEngineData::new(batch)))
    }

    // Helper function to read and parse JSON file
    fn read_json_file(path: &Path) -> DeltaResult<Vec<serde_json::Value>> {
        let file = std::fs::read_to_string(path)?;
        let json: Vec<_> = serde_json::Deserializer::from_str(&file)
            .into_iter::<serde_json::Value>()
            .flatten()
            .collect();
        Ok(json)
    }

    #[test]
    fn test_write_json_file_without_overwrite() -> DeltaResult<()> {
        do_test_write_json_file(false)
    }

    #[test]
    fn test_write_json_file_overwrite() -> DeltaResult<()> {
        do_test_write_json_file(true)
    }

    fn do_test_write_json_file(overwrite: bool) -> DeltaResult<()> {
        let test_dir = TempDir::new().unwrap();
        let path = test_dir.path().join("00000000000000000001.json");
        let handler = SyncJsonHandler::new();
        let url = Url::from_file_path(&path).unwrap();

        // First write with no existing file
        let data = create_test_data(vec!["remi", "wilson"])?;
        let filtered_data = Ok(FilteredEngineData::with_all_rows_selected(data));
        let result =
            handler.write_json_file(&url, Box::new(std::iter::once(filtered_data)), overwrite);

        // Verify the first write is successful
        assert!(result.is_ok());
        let json = read_json_file(&path)?;
        assert_eq!(json, vec![json!({"dog": "remi"}), json!({"dog": "wilson"})]);

        // Second write with existing file
        let data = create_test_data(vec!["seb", "tia"])?;
        let filtered_data = Ok(FilteredEngineData::with_all_rows_selected(data));
        let result =
            handler.write_json_file(&url, Box::new(std::iter::once(filtered_data)), overwrite);

        if overwrite {
            // Verify the second write is successful
            assert!(result.is_ok());
            let json = read_json_file(&path)?;
            assert_eq!(json, vec![json!({"dog": "seb"}), json!({"dog": "tia"})]);
        } else {
            // Verify the second write fails with FileAlreadyExists error
            match result {
                Err(Error::FileAlreadyExists(err_path)) => {
                    assert_eq!(err_path, path.to_string_lossy().to_string());
                }
                _ => panic!("Expected FileAlreadyExists error, got: {result:?}"),
            }
        }

        Ok(())
    }
}
