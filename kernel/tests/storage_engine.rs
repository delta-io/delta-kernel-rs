#![cfg(feature = "default-engine-base")]

use std::sync::{Arc, LazyLock};

use delta_kernel::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::storage::StorageEngine;
use delta_kernel::engine_data::{FilteredEngineData, RowVisitor, TypedGetData as _};
use delta_kernel::expressions::{col, column_name, lit, ColumnName, Predicate};
use delta_kernel::parquet::arrow::ArrowWriter;
use delta_kernel::schema::{schema_ref, DataType, MetadataColumnSpec, StructField};
use delta_kernel::{Engine, EngineData, KernelError, ParquetHandler, Result, Snapshot};
use rstest::rstest;
use test_utils::engine_contract::{
    test_json_handler_file_path_contract, test_parquet_handler_footer_errors_on_missing_file,
    test_parquet_handler_footer_preserves_field_ids,
    test_parquet_handler_reads_file_with_arrow_schema,
    test_parquet_handler_write_always_overwrites, test_parquet_handler_write_omits_arrow_schema,
};
use url::Url;

fn engine() -> StorageEngine {
    let inner = test_utils::create_default_engine(&Url::parse("file:///").unwrap()).unwrap();
    StorageEngine::new(inner.storage_handler())
}

fn json_strings(values: Vec<&str>) -> Box<dyn EngineData> {
    Box::new(ArrowEngineData::new(
        RecordBatch::try_from_iter(vec![(
            "json",
            Arc::new(StringArray::from(values)) as Arc<dyn Array>,
        )])
        .unwrap(),
    ))
}

fn values(engine: &StorageEngine, values: Vec<&str>) -> Box<dyn EngineData> {
    engine
        .json_handler()
        .parse_json(json_strings(values), schema_ref! { not_null "value": LONG })
        .unwrap()
}

#[test]
fn json_parse_and_write_preserve_rows_overwrite_and_conflicts() {
    let directory = tempfile::tempdir().unwrap();
    let url = Url::from_file_path(directory.path().join("rows.json")).unwrap();
    let engine = engine();
    let write = |rows, overwrite| {
        engine.json_handler().write_json_file(
            &url,
            Box::new(std::iter::once(Ok(
                FilteredEngineData::with_all_rows_selected(values(&engine, rows)),
            ))),
            overwrite,
        )
    };
    let rows = vec![r#"{"value":1}"#, r#"{"value":2}"#];
    let size = write(rows.clone(), false).unwrap();
    assert_eq!(
        std::fs::read(directory.path().join("rows.json")).unwrap(),
        b"{\"value\":1}\n{\"value\":2}\n"
    );
    assert_eq!(engine.storage_handler().head(&url).unwrap().size, size);
    assert!(matches!(
        write(rows, false),
        Err(KernelError::FileAlreadyExists(_))
    ));
    let size = write(vec![r#"{"value":100}"#], true).unwrap();
    let replacement = b"{\"value\":100}\n";
    assert_eq!(
        std::fs::read(directory.path().join("rows.json")).unwrap(),
        replacement
    );
    assert_eq!(size as usize, replacement.len());
    let error = engine
        .json_handler()
        .write_json_file(
            &url,
            Box::new(std::iter::once(Err(KernelError::generic(
                "upstream failure",
            )))),
            true,
        )
        .unwrap_err();
    assert!(error.to_string().contains("upstream failure"));
    assert_eq!(engine.storage_handler().head(&url).unwrap().size, size);
    assert_eq!(
        std::fs::read(directory.path().join("rows.json")).unwrap(),
        replacement
    );
}

#[rstest]
#[case(false, vec![(0, 1), (1, 2), (2, 10), (3, 20)])]
#[case(true, vec![(2, 10), (3, 20)])]
fn parquet_multiple_batches_preserve_projection_and_row_indexes_after_pruning(
    #[case] separate_row_groups: bool,
    #[case] expected_rows: Vec<(i64, i64)>,
) {
    let directory = tempfile::tempdir().unwrap();
    let url = Url::from_file_path(directory.path().join("rows.parquet")).unwrap();
    let engine = engine();
    let data: Vec<_> = [vec![1, 2], vec![10, 20]]
        .into_iter()
        .map(|values| {
            RecordBatch::try_from_iter(vec![
                (
                    "value",
                    Arc::new(Int64Array::from(values)) as Arc<dyn Array>,
                ),
                (
                    "unprojected",
                    Arc::new(StringArray::from(vec!["x", "y"])) as Arc<dyn Array>,
                ),
            ])
            .unwrap()
        })
        .collect();
    if separate_row_groups {
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(directory.path().join("rows.parquet")).unwrap(),
            data[0].schema(),
            None,
        )
        .unwrap();
        for batch in data {
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
    } else {
        engine
            .parquet_handler()
            .write_parquet_file(
                url.clone(),
                Box::new(
                    data.into_iter().map(|batch| {
                        Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
                    }),
                ),
            )
            .unwrap();
    }
    let file = engine.storage_handler().head(&url).unwrap();
    let schema = schema_ref! {
        (StructField::create_metadata_column("row_index", MetadataColumnSpec::RowIndex)),
        not_null "value": LONG,
    };
    let mut batches = engine
        .parquet_handler()
        .read_parquet_files(
            std::slice::from_ref(&file),
            schema.clone(),
            Some(Arc::new(Predicate::gt(col!("value"), lit(5i64)))),
        )
        .unwrap();
    // A row-group predicate may retain non-matching rows in a surviving group.
    let batch = batches.next().unwrap().unwrap();
    assert_eq!(batch.len(), expected_rows.len());
    let mut rows = RowCollector(Vec::new());
    rows.visit_rows_of(batch.as_ref()).unwrap();
    assert_eq!(rows.0, expected_rows);
    let batch = ArrowEngineData::try_from_engine_data(batch).unwrap();
    let names: Vec<_> = batch
        .record_batch()
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["row_index", "value"]);
    assert!(batches.next().is_none());
    let mut pruned = engine
        .parquet_handler()
        .read_parquet_files(
            &[file],
            schema,
            Some(Arc::new(Predicate::gt(col!("value"), lit(100i64)))),
        )
        .unwrap();
    assert!(pruned.next().is_none());
}

struct RowCollector(Vec<(i64, i64)>);
impl RowVisitor for RowCollector {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<Vec<ColumnName>> =
            LazyLock::new(|| vec![column_name!("row_index"), column_name!("value")]);
        (&NAMES, &[DataType::LONG, DataType::LONG])
    }
    fn visit<'a>(
        &mut self,
        row_count: usize,
        getters: &[&'a dyn delta_kernel::engine_data::GetData<'a>],
    ) -> Result<()> {
        for row in 0..row_count {
            self.0.push((
                getters[0].get(row, "row_index")?,
                getters[1].get(row, "value")?,
            ));
        }
        Ok(())
    }
}

#[test]
fn parquet_write_rejects_empty_and_failed_batches_without_writing() {
    let directory = tempfile::tempdir().unwrap();
    let url = Url::from_file_path(directory.path().join("rows.parquet")).unwrap();
    let engine = engine();
    assert!(engine
        .parquet_handler()
        .write_parquet_file(url.clone(), Box::new(std::iter::empty()))
        .unwrap_err()
        .to_string()
        .contains("empty data iterator"));
    for first_valid in [false, true] {
        let mut data = Vec::new();
        if first_valid {
            data.push(Ok(values(&engine, vec![r#"{"value":1}"#])));
        }
        data.push(Err(KernelError::generic("batch failure")));
        assert!(engine
            .parquet_handler()
            .write_parquet_file(url.clone(), Box::new(data.into_iter()))
            .unwrap_err()
            .to_string()
            .contains("batch failure"));
        assert!(!directory.path().join("rows.parquet").exists());
    }
}

#[rstest]
#[case(false)]
#[case(true)]
fn missing_file_returns_one_error_then_continues_to_next_file(#[case] parquet: bool) {
    let directory = tempfile::tempdir().unwrap();
    let missing = delta_kernel::FileMeta {
        location: Url::from_file_path(directory.path().join("missing")).unwrap(),
        last_modified: 0,
        size: 100,
    };
    let engine = engine();
    let files = [missing.clone(), missing];
    let schema = schema_ref! { not_null "value": LONG };
    let mut batches = if parquet {
        engine
            .parquet_handler()
            .read_parquet_files(&files, schema, None)
            .unwrap()
    } else {
        engine
            .json_handler()
            .read_json_files(&files, schema, None)
            .unwrap()
    };
    assert!(batches.next().unwrap().is_err());
    assert!(batches.next().unwrap().is_err());
    assert!(batches.next().is_none());
}

#[rstest]
#[case(false)]
#[case(true)]
fn incompatible_read_schema_returns_one_error_and_terminates(#[case] parquet: bool) {
    let directory = tempfile::tempdir().unwrap();
    let url = Url::from_file_path(directory.path().join("rows")).unwrap();
    let engine = engine();
    let data = values(&engine, vec![r#"{"value":1}"#]);
    let mut batches = if parquet {
        engine
            .parquet_handler()
            .write_parquet_file(url.clone(), Box::new(std::iter::once(Ok(data))))
            .unwrap();
        let file = engine.storage_handler().head(&url).unwrap();
        engine
            .parquet_handler()
            .read_parquet_files(&[file], schema_ref! { not_null "value": BINARY }, None)
            .unwrap()
    } else {
        engine
            .json_handler()
            .write_json_file(
                &url,
                Box::new(std::iter::once(Ok(
                    FilteredEngineData::with_all_rows_selected(data),
                ))),
                false,
            )
            .unwrap();
        let file = engine.storage_handler().head(&url).unwrap();
        let schema = schema_ref! { (StructField::create_metadata_column("row_index", MetadataColumnSpec::RowIndex)), };
        engine
            .json_handler()
            .read_json_files(&[file], schema, None)
            .unwrap()
    };
    assert!(batches.next().unwrap().is_err());
    assert!(batches.next().is_none());
}

#[test]
fn json_preserves_file_path_metadata() {
    test_json_handler_file_path_contract(engine().json_handler().as_ref());
}

#[rstest]
#[case(test_parquet_handler_footer_errors_on_missing_file)]
#[case(test_parquet_handler_footer_preserves_field_ids)]
#[case(test_parquet_handler_reads_file_with_arrow_schema)]
#[case(test_parquet_handler_write_always_overwrites)]
#[case(test_parquet_handler_write_omits_arrow_schema)]
fn parquet_contract(#[case] contract: fn(&dyn ParquetHandler)) {
    contract(engine().parquet_handler().as_ref());
}

#[test]
fn snapshot_replays_checkpoint_and_json_without_declarative_executor() {
    let engine = engine();
    #[cfg(feature = "declarative-plans")]
    assert!(engine.plan_executor().is_none());
    let root = Url::from_directory_path(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/with_checkpoint_no_last_checkpoint"),
    )
    .unwrap();
    let snapshot = Snapshot::builder_for(root).build(&engine).unwrap();
    assert_eq!(snapshot.version(), 3);
    let scan = snapshot.scan_builder().build().unwrap();
    let batches: Vec<_> = scan
        .scan_metadata(&engine)
        .unwrap()
        .collect::<Result<_>>()
        .unwrap();
    assert!(!batches.is_empty());
}

#[test]
fn malformed_json_yields_one_error_and_terminates() {
    use std::sync::Arc;

    use delta_kernel::schema::{DataType, StructType};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("malformed.json");
    std::fs::write(&path, br#"{"value": ["#).unwrap();
    let url = Url::from_file_path(path).unwrap();
    let engine = engine();
    let file = engine.storage_handler().head(&url).unwrap();
    let schema = Arc::new(StructType::new_unchecked([
        delta_kernel::schema::StructField::nullable("value", DataType::LONG),
    ]));
    let mut batches = engine
        .json_handler()
        .read_json_files(&[file], schema, None)
        .unwrap();
    assert!(batches.next().unwrap().is_err());
    assert!(batches.next().is_none());
}
