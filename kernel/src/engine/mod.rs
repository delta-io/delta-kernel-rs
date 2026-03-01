//! Provides an engine implementation that implements the required traits. The engine can optionally
//! be built into the kernel by setting the `default-engine` feature flag. See the related module
//! for more information.

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(feature = "arrow-expression")]
pub(crate) mod arrow_utils;
#[cfg(feature = "internal-api")]
pub use self::arrow_utils::{parse_json, to_json_bytes};

#[cfg(feature = "default-engine-base")]
pub mod default;

#[cfg(test)]
pub(crate) mod sync;

#[cfg(feature = "default-engine-base")]
pub mod arrow_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod arrow_get_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod ensure_data_types;
#[cfg(feature = "default-engine-base")]
pub mod parquet_row_group_skipping;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::LazyLock;

    use itertools::Itertools;
    use object_store::path::Path;
    use tempfile::NamedTempFile;
    use url::Url;

    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine_data::{FilteredEngineData, GetData, RowVisitor, TypedGetData as _};
    use crate::schema::{
        column_name, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, StructField,
        StructType,
    };
    use crate::{DeltaResult, Engine, EngineData, FileMeta, JsonHandler};

    use test_utils::delta_path_for_version;

    // --- Shared file setup helper ---

    /// Writes `lines` as newline-delimited JSON to a [`NamedTempFile`] and returns the file
    /// together with a [`FileMeta`] pointing at it. The temp file must be kept alive for as
    /// long as the `FileMeta` is in use.
    pub(crate) fn make_temp_json_file(lines: &[&str]) -> (NamedTempFile, FileMeta) {
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        for line in lines {
            use std::io::Write as _;
            writeln!(temp_file, "{line}").expect("Failed to write to temp file");
        }
        let path = temp_file.path();
        let file_url = Url::from_file_path(path).expect("Failed to create file URL");
        let size = std::fs::metadata(path)
            .expect("Failed to stat temp file")
            .len();
        let file_meta = FileMeta {
            location: file_url,
            last_modified: 0,
            size,
        };
        (temp_file, file_meta)
    }

    // --- Generic JsonHandler contract test ---

    /// Verifies the engine-agnostic contract for `FilePath` metadata column injection:
    /// given a schema that requests `_file`, `read_json_files` must populate it with
    /// the file URL for every row, readable via [`GetData`] without any Arrow downcasting.
    ///
    /// Call this from each engine's test module to ensure the contract holds across engines.
    pub(crate) fn test_json_handler_file_path_contract(handler: &dyn JsonHandler) {
        let (_temp, file_meta) = make_temp_json_file(&[r#"{"x": 1}"#, r#"{"x": 2}"#]);
        let expected_url = file_meta.location.to_string();

        let schema = Arc::new(
            StructType::try_new([
                StructField::not_null("x", DataType::INTEGER),
                StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
            ])
            .unwrap(),
        );

        let engine_data = handler
            .read_json_files(&[file_meta], schema, None)
            .unwrap()
            .next()
            .expect("expected at least one batch")
            .unwrap();

        // Verify via GetData — no Arrow types, works against any engine.
        struct FilePathCollector {
            paths: Vec<String>,
        }
        impl RowVisitor for FilePathCollector {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAT: LazyLock<ColumnNamesAndTypes> =
                    LazyLock::new(|| (vec![column_name!("_file")], vec![DataType::STRING]).into());
                NAT.as_ref()
            }
            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                for i in 0..row_count {
                    self.paths.push(getters[0].get(i, "_file")?);
                }
                Ok(())
            }
        }

        let mut collector = FilePathCollector { paths: vec![] };
        collector.visit_rows_of(engine_data.as_ref()).unwrap();

        assert_eq!(collector.paths.len(), 2, "expected 2 rows");
        assert!(
            collector.paths.iter().all(|p| p == &expected_url),
            "_file values should equal the file URL"
        );
    }

    // --- Storage / engine helpers ---

    fn test_list_from_should_sort_and_filter(
        engine: &dyn Engine,
        base_url: &Url,
        engine_data: impl Fn() -> Box<dyn EngineData>,
    ) {
        let json = engine.json_handler();
        let get_data = || {
            let data = engine_data();
            let filtered_data = FilteredEngineData::with_all_rows_selected(data);
            Box::new(std::iter::once(Ok(filtered_data)))
        };

        let expected_names: Vec<Path> = (1..4)
            .map(|i| delta_path_for_version(i, "json"))
            .collect_vec();

        for i in expected_names.iter().rev() {
            let path = base_url.join(i.as_ref()).unwrap();
            json.write_json_file(&path, get_data(), false).unwrap();
        }
        let path = base_url.join("other").unwrap();
        json.write_json_file(&path, get_data(), false).unwrap();

        let storage = engine.storage_handler();

        // list files after an offset
        let test_url = base_url.join(expected_names[0].as_ref()).unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len() - 1);
        for (file, expected) in files.iter().zip(expected_names.iter().skip(1)) {
            assert_eq!(file.location, base_url.join(expected.as_ref()).unwrap());
        }

        let test_url = base_url
            .join(delta_path_for_version(0, "json").as_ref())
            .unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len());

        // list files inside a directory / key prefix
        let test_url = base_url.join("_delta_log/").unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len());
        for (file, expected) in files.iter().zip(expected_names.iter()) {
            assert_eq!(file.location, base_url.join(expected.as_ref()).unwrap());
        }
    }

    fn get_arrow_data() -> Box<dyn EngineData> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "dog",
            ArrowDataType::Utf8,
            true,
        )]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["remi", "wilson"]))],
        )
        .unwrap();
        Box::new(ArrowEngineData::new(data))
    }

    pub(crate) fn test_arrow_engine(engine: &dyn Engine, base_url: &Url) {
        test_list_from_should_sort_and_filter(engine, base_url, get_arrow_data);
    }
}
