//! Shared contract tests for engine handler traits ([`JsonHandler`], [`ParquetHandler`]).
//!
//! Each function here tests a specific piece of the handler contract using only the public
//! trait API plus [`GetData`]/[`RowVisitor`] for result inspection — no engine-specific
//! downcasting. Individual engine test modules call these to verify their implementation
//! satisfies the contract, then add any engine-specific assertions (e.g. Arrow encoding
//! details) in their own tests.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use tempfile::{tempdir, NamedTempFile};
use url::Url;

use crate::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine_data::{FilteredEngineData, GetData, RowVisitor, TypedGetData as _};
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use crate::schema::{
    column_name, ColumnMetadataKey, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec,
    StructField, StructType,
};
use crate::{DeltaResult, Engine, EngineData, FileMeta, JsonHandler, ParquetHandler};
use itertools::Itertools;

use object_store::path::Path;
use test_utils::delta_path_for_version;

// ---------------------------------------------------------------------------
// Shared file-setup helpers
// ---------------------------------------------------------------------------

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

/// Builds a [`FileMeta`] for a local file path, reading the actual size from the filesystem.
fn file_meta_for(path: &std::path::Path) -> FileMeta {
    let url = Url::from_file_path(path).unwrap();
    let size = std::fs::metadata(path).unwrap().len();
    FileMeta {
        location: url,
        last_modified: 0,
        size,
    }
}

// ---------------------------------------------------------------------------
// JsonHandler contract tests
// ---------------------------------------------------------------------------

/// Contract: any [`JsonHandler`] that receives a schema with a [`MetadataColumnSpec::FilePath`]
/// column must populate it with the file URL for every row, readable via [`GetData`] without
/// any Arrow downcasting.
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

    struct FilePathCollector {
        paths: Vec<String>,
    }
    impl RowVisitor for FilePathCollector {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
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

// ---------------------------------------------------------------------------
// ParquetHandler contract tests
// ---------------------------------------------------------------------------

/// Contract: [`ParquetHandler::read_parquet_footer`] must correctly parse the schema
/// from a real Delta checkpoint file.
pub(crate) fn test_parquet_handler_reads_footer(handler: &dyn ParquetHandler) {
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/data/with_checkpoint_no_last_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet",
    ))
    .unwrap();
    let file_meta = file_meta_for(&path);
    let footer = handler.read_parquet_footer(&file_meta).unwrap();
    crate::utils::test_utils::validate_checkpoint_schema(&footer.schema);
}

/// Contract: [`ParquetHandler::read_parquet_footer`] must return an error for a
/// non-existent file.
pub(crate) fn test_parquet_handler_footer_errors_on_missing_file(handler: &dyn ParquetHandler) {
    let mut temp_path = std::env::temp_dir();
    temp_path.push("non_existent_kernel_test_file.parquet");
    let file_meta = FileMeta {
        location: Url::from_file_path(&temp_path).unwrap(),
        last_modified: 0,
        size: 0,
    };
    assert!(
        handler.read_parquet_footer(&file_meta).is_err(),
        "expected error for non-existent file"
    );
}

/// Contract: [`ParquetHandler::read_parquet_footer`] must preserve Arrow field IDs,
/// accessible via [`ColumnMetadataKey::ParquetFieldId`].
pub(crate) fn test_parquet_handler_footer_preserves_field_ids(handler: &dyn ParquetHandler) {
    let make_field_with_id = |name: &str, ty: ArrowDataType, nullable: bool, id: &str| {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    };
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        make_field_with_id("id", ArrowDataType::Int64, false, "1"),
        make_field_with_id("name", ArrowDataType::Utf8, true, "2"),
    ]));

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("field_ids.parquet");
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(&file_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let footer = handler
        .read_parquet_footer(&file_meta_for(&file_path))
        .unwrap();

    for (name, expected_id) in [("id", "1"), ("name", "2")] {
        let field = footer.schema.fields().find(|f| f.name() == name).unwrap();
        assert_eq!(
            field
                .get_config_value(&ColumnMetadataKey::ParquetFieldId)
                .map(|v| v.to_string())
                .as_deref(),
            Some(expected_id),
            "field '{name}' should have field ID {expected_id}"
        );
    }
}

/// Contract: [`ParquetHandler::write_parquet_file`] always overwrites an existing file.
/// Writes `[1, 2, 3]`, then overwrites with `[10, 20]`, and verifies only `[10, 20]`
/// is present.
pub(crate) fn test_parquet_handler_write_always_overwrites(handler: &dyn ParquetHandler) {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("overwrite_test.parquet");
    let url = Url::from_file_path(&file_path).unwrap();

    let make_data = |values: Vec<i64>| -> Box<dyn EngineData> {
        Box::new(ArrowEngineData::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(values)) as Arc<dyn Array>,
            )])
            .unwrap(),
        ))
    };

    handler
        .write_parquet_file(
            url.clone(),
            Box::new(std::iter::once(Ok(make_data(vec![1, 2, 3])))),
        )
        .unwrap();
    handler
        .write_parquet_file(
            url.clone(),
            Box::new(std::iter::once(Ok(make_data(vec![10, 20])))),
        )
        .unwrap();

    let file_meta = file_meta_for(&file_path);
    let schema = Arc::new(
        handler
            .read_parquet_footer(&file_meta)
            .unwrap()
            .schema
            .as_ref()
            .clone(),
    );
    let batches: Vec<RecordBatch> = handler
        .read_parquet_files(&[file_meta], schema, None)
        .unwrap()
        .map(|r| {
            ArrowEngineData::try_from_engine_data(r.unwrap())
                .unwrap()
                .into()
        })
        .collect();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2, "expected 2 rows after overwrite");
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[10, 20]
    );
}

// ---------------------------------------------------------------------------
// Storage / Engine helpers (used by the engine-level tests)
// ---------------------------------------------------------------------------

pub(crate) fn test_arrow_engine(engine: &dyn Engine, base_url: &Url) {
    test_list_from_should_sort_and_filter(engine, base_url, get_arrow_data);
}

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
