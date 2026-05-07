use std::io::{BufReader, Cursor};
use std::sync::Arc;

use bytes::Bytes;
use url::Url;

use super::{read_files, resolve_scope};
use crate::arrow::json::ReaderBuilder;
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
    pub(crate) fn new(store: Option<Arc<DynObjectStore>>) -> Self {
        Self { store }
    }
}

fn try_create_from_json(
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
        read_files(
            self.store.as_ref(),
            files,
            schema,
            predicate,
            try_create_from_json,
        )
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

        // For local writes, ensure parent directories exist; `LocalFileSystem::put` does not
        // create them. No-op for non-file:// URLs. Must happen before `resolve_scope` so that
        // canonicalization of the parent succeeds.
        if path.scheme() == "file" {
            if let Ok(file_path) = path.to_file_path() {
                if let Some(parent) = file_path.parent() {
                    if !parent.exists() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
            }
        }

        let (store, _, object_path) = resolve_scope(self.store.as_ref(), path)?;

        let opts = if overwrite {
            crate::object_store::PutOptions::default()
        } else {
            crate::object_store::PutOptions {
                mode: crate::object_store::PutMode::Create,
                ..Default::default()
            }
        };
        futures::executor::block_on(store.put_opts(&object_path, buf.into(), opts)).map_err(
            |e| match e {
                crate::object_store::Error::AlreadyExists { .. } => {
                    Error::FileAlreadyExists(path.to_string())
                }
                other => Error::generic(other.to_string()),
            },
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::TempDir;
    use url::Url;

    use super::*;
    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};

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
        let handler = SyncJsonHandler::new(None);
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
            assert!(matches!(result, Err(Error::FileAlreadyExists(_))));
        }

        Ok(())
    }
}
