use std::io::{BufReader, Cursor};
use std::sync::Arc;

use bytes::Bytes;
use url::Url;

use super::{put_bytes, read_files_arrow};
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
    DeltaResult, DeltaResultIterator, EngineData, Error, FileDataReadResultIterator, FileMeta,
    FileSize, JsonHandler, PredicateRef,
};

pub(crate) struct SyncJsonHandler {
    store: Option<Arc<DynObjectStore>>,
}

impl SyncJsonHandler {
    pub(crate) fn new(store: Option<Arc<DynObjectStore>>) -> Self {
        Self { store }
    }
}

pub(super) fn try_create_from_json(
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
        let iter = read_files_arrow(
            self.store.as_ref(),
            files,
            schema,
            predicate,
            try_create_from_json,
        );
        Ok(Box::new(iter.map(|data| Ok(Box::new(data?) as _))))
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
        data: DeltaResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> DeltaResult<FileSize> {
        let buf = to_json_bytes(data)?;
        let size = buf
            .len()
            .try_into()
            .map_err(|_| Error::generic("JSON file size exceeds u64::MAX"))?;
        put_bytes(self.store.as_ref(), path, buf.into(), overwrite)?;
        Ok(size)
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

        let written_size = result?;
        assert_eq!(written_size, 32);
        assert_eq!(written_size, std::fs::metadata(&path)?.len());
        let json = read_json_file(&path)?;
        assert_eq!(json, vec![json!({"dog": "remi"}), json!({"dog": "wilson"})]);

        // Second write with existing file
        let data = create_test_data(vec!["seb", "tia"])?;
        let filtered_data = Ok(FilteredEngineData::with_all_rows_selected(data));
        let result =
            handler.write_json_file(&url, Box::new(std::iter::once(filtered_data)), overwrite);

        if overwrite {
            let written_size = result?;
            assert_eq!(written_size, 28);
            assert_eq!(written_size, std::fs::metadata(&path)?.len());
            let json = read_json_file(&path)?;
            assert_eq!(json, vec![json!({"dog": "seb"}), json!({"dog": "tia"})]);
        } else {
            // Verify the second write fails with FileAlreadyExists error
            assert!(matches!(result, Err(Error::FileAlreadyExists(_))));
        }

        Ok(())
    }

    #[test]
    fn test_write_empty_json_file_reports_zero_size() -> DeltaResult<()> {
        let test_dir = TempDir::new().unwrap();
        let path = test_dir.path().join("empty.json");
        let handler = SyncJsonHandler::new(None);
        let url = Url::from_file_path(&path).unwrap();

        let written_size = handler.write_json_file(&url, Box::new(std::iter::empty()), false)?;

        assert_eq!(written_size, 0);
        assert_eq!(std::fs::metadata(&path)?.len(), 0);
        Ok(())
    }

    // TODO(#2618): Restore once the engine contract helpers move to test_utils and SyncEngine can
    // call them without the kernel-cfg-test cycle issue.
    //
    // #[test]
    // fn json_handler_file_path_contract() {
    //     test_json_handler_file_path_contract(&SyncJsonHandler::new(None));
    // }
}
