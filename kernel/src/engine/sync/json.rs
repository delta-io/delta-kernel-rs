use std::{fs::File, io::BufReader, io::Write};

use crate::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use crate::arrow::json::ReaderBuilder;
use tempfile::NamedTempFile;
use url::Url;

use super::read_files;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::parse_json as arrow_parse_json;
use crate::engine::arrow_utils::to_json_bytes;
use crate::engine_data::FilteredEngineData;
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, FileDataReadResultIterator, FileMeta, JsonHandler, PredicateRef,
};

pub(crate) struct SyncJsonHandler;

/// Note: This function must match the signature expected by `read_files` helper function,
/// which is also used by `try_create_from_parquet`. The `_file_location` parameter is unused
/// here but required to satisfy the shared function signature.
fn try_create_from_json(
    file: File,
    _schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    _predicate: Option<PredicateRef>,
    _file_location: String,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let json = ReaderBuilder::new(arrow_schema)
        .with_coerce_primitive(true)
        .build(BufReader::new(file))?
        .map(|data| Ok(ArrowEngineData::new(data?)));
    Ok(json)
}

impl JsonHandler for SyncJsonHandler {
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
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
        data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()> {
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

        // write data to tmp file
        let mut tmp_file = NamedTempFile::new_in(parent)?;
        let buf = to_json_bytes(data)?;
        tmp_file.write_all(&buf)?;
        tmp_file.flush()?;

        let persist_result = if overwrite {
            tmp_file.persist(path.clone())
        } else {
            // use 'persist_noclobber' to atomically rename tmp file to final path
            tmp_file.persist_noclobber(path.clone())
        };

        // Map errors (handling AlreadyExists only in non-overwrite mode).
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
        let handler = SyncJsonHandler;
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

    /// Verify that `read_json_files` echoes back the exact `FileMeta` that was passed in,
    /// including caller-supplied `last_modified` and `size` fields.
    #[test]
    fn test_read_json_files_returns_file_meta() -> DeltaResult<()> {
        use crate::schema::{DataType, StructField, StructType};

        let test_dir = TempDir::new().unwrap();
        let path = test_dir.path().join("test.json");
        let handler = SyncJsonHandler;
        let url = Url::from_file_path(&path).unwrap();

        // Write a JSON file so there is something to read back
        let data = create_test_data(vec!["remi"])?;
        handler.write_json_file(
            &url,
            Box::new(std::iter::once(Ok(
                FilteredEngineData::with_all_rows_selected(data),
            ))),
            false,
        )?;

        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "dog",
            DataType::STRING,
        )]));

        // Use distinctive values to confirm the exact FileMeta is passed through unchanged
        let input_meta = FileMeta {
            location: url.clone(),
            last_modified: 123_456,
            size: 99,
        };

        let mut result = handler.read_json_files(&[input_meta], schema, None)?;
        let (returned_meta, _batch) = result.next().unwrap()?;

        assert_eq!(returned_meta.location, url);
        assert_eq!(returned_meta.last_modified, 123_456);
        assert_eq!(returned_meta.size, 99);
        assert!(
            result.next().is_none(),
            "expected only one batch from one file"
        );

        Ok(())
    }

    /// Verify that when reading multiple files, each batch is tagged with its own source
    /// file's `FileMeta` — not, say, all tagged with the first file.
    #[test]
    fn test_read_json_files_multi_file_tags_correct_meta() -> DeltaResult<()> {
        use crate::schema::{DataType, StructField, StructType};

        let test_dir = TempDir::new().unwrap();
        let handler = SyncJsonHandler;
        let url1 = Url::from_file_path(test_dir.path().join("file1.json")).unwrap();
        let url2 = Url::from_file_path(test_dir.path().join("file2.json")).unwrap();

        // Write two JSON files with different content
        for (url, values) in [(&url1, vec!["remi"]), (&url2, vec!["wilson"])] {
            let data = create_test_data(values)?;
            handler.write_json_file(
                url,
                Box::new(std::iter::once(Ok(
                    FilteredEngineData::with_all_rows_selected(data),
                ))),
                false,
            )?;
        }

        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "dog",
            DataType::STRING,
        )]));

        let meta1 = FileMeta {
            location: url1.clone(),
            last_modified: 11,
            size: 111,
        };
        let meta2 = FileMeta {
            location: url2.clone(),
            last_modified: 22,
            size: 222,
        };

        let results: Vec<(FileMeta, _)> = handler
            .read_json_files(&[meta1, meta2], schema, None)?
            .collect::<DeltaResult<Vec<_>>>()?;

        assert_eq!(results.len(), 2, "expected one batch per file");
        // Each batch must be tagged with its own source file's FileMeta, not the other file's
        assert_eq!(
            results[0].0.location, url1,
            "first batch should be tagged with file1's URL"
        );
        assert_eq!(results[0].0.last_modified, 11);
        assert_eq!(
            results[1].0.location, url2,
            "second batch should be tagged with file2's URL"
        );
        assert_eq!(results[1].0.last_modified, 22);

        Ok(())
    }
}
