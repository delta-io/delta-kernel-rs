use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use ::test_utils::{assert_result_error_with_message, get_column, load_test_data};
use bytes::Bytes;
use rstest::rstest;
use url::Url;

use super::*;
use crate::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS, STATS_PARSED};
use crate::arrow::array::{Array, BooleanArray, Int64Array, StringArray, StructArray};
use crate::arrow::compute::filter_record_batch;
use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema};
use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::display::array_value_to_string;
use crate::committer::FileSystemCommitter;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
use crate::expressions::{
    col, column_name, column_pred, lit, Expression as Expr, Predicate as Pred, Scalar,
};
use crate::object_store::memory::InMemory;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::scan::data_skipping::{all_referenced_columns, as_checkpoint_skipping_predicate};
use crate::scan::state::ScanFile;
use crate::schema::{
    self, schema, schema_ref, ColumnMetadataKey, DataType, MetadataColumnSpec, StructField,
    StructType,
};
use crate::transaction::create_table::create_table;
use crate::{
    DeltaResultIteratorStatic, Engine, EngineData, FileDataReadResultIterator, FileMeta,
    ParquetFooter, ParquetHandler, PredicateRef, Snapshot,
};

fn field_names(s: &StructArray) -> Vec<String> {
    s.fields().iter().map(|f| f.name().clone()).collect()
}

#[test]
fn partition_values_options_carry_optional_timestamp_timezone() {
    let default = PartitionValuesOptions::with_struct();
    assert!(default.parsed_struct);
    assert_eq!(default.timestamp_timezone, None);

    let zoned = default.with_timestamp_timezone("America/Los_Angeles");
    assert_eq!(
        zoned.timestamp_timezone.as_deref(),
        Some("America/Los_Angeles")
    );
}

#[test]
fn test_static_skipping() {
    let test_cases = [
        (false, column_pred!("a")),
        (true, Pred::FALSE),
        (false, Pred::TRUE),
        (false, Pred::NULL), // NULL is unknown, not false -- conservative (no skip)
        (true, Pred::and(column_pred!("a"), Pred::FALSE)),
        (false, Pred::or(column_pred!("a"), Pred::TRUE)),
        (false, Pred::or(column_pred!("a"), Pred::FALSE)),
        (false, Pred::lt(col!("a"), lit(10))),
        (false, Pred::lt(lit(10), lit(100))),
        (true, Pred::gt(lit(10), lit(100))),
        (false, Pred::and(Pred::NULL, column_pred!("a"))), // NULL is unknown, not false
    ];
    for (should_skip, predicate) in test_cases {
        assert_eq!(
            can_statically_skip_all_files(&predicate),
            should_skip,
            "Failed for predicate: {predicate:#?}"
        );
    }
}

#[test]
fn test_physical_predicate() {
    let logical_schema = schema! {
        nullable "a": LONG,
        (StructField::nullable("b", DataType::LONG).with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            "phys_b",
        )])),
        (StructField::nullable("phys_b", DataType::LONG).with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            "phys_c",
        )])),
        (StructField::nullable(
            "nested",
            schema! {
                nullable "x": LONG,
                (StructField::nullable("y", DataType::LONG).with_metadata([(
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    "phys_y",
                )])),
            },
        )),
        (StructField::nullable(
            "mapped",
            schema! {
                (StructField::nullable("n", DataType::LONG).with_metadata([(
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    "phys_n",
                )])),
            },
        )
        .with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            "phys_mapped",
        )])),
    };

    // NOTE: We break several column mapping rules here because they don't matter for this
    // test. For example, we do not provide field ids, and not all columns have physical names.
    let test_cases = [
        (Pred::TRUE, Some(PhysicalPredicate::None)),
        (Pred::FALSE, Some(PhysicalPredicate::StaticSkipAll)),
        (column_pred!("x"), None), // no such column
        (
            column_pred!("a"),
            Some(PhysicalPredicate::Some(
                column_pred!("a").into(),
                schema_ref! { nullable "a": LONG },
            )),
        ),
        (
            column_pred!("b"),
            Some(PhysicalPredicate::Some(
                column_pred!("phys_b").into(),
                schema_ref! {
                    (StructField::nullable("phys_b", DataType::LONG).with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_b",
                    )])),
                },
            )),
        ),
        (
            column_pred!("nested.x"),
            Some(PhysicalPredicate::Some(
                column_pred!("nested.x").into(),
                schema_ref! {
                    nullable "nested": {
                        nullable "x": LONG,
                    },
                },
            )),
        ),
        (
            column_pred!("nested.y"),
            Some(PhysicalPredicate::Some(
                column_pred!("nested.phys_y").into(),
                schema_ref! {
                    nullable "nested": {
                        (StructField::nullable("phys_y", DataType::LONG).with_metadata([(
                            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                            "phys_y",
                        )])),
                    },
                },
            )),
        ),
        (
            column_pred!("mapped.n"),
            Some(PhysicalPredicate::Some(
                column_pred!("phys_mapped.phys_n").into(),
                schema_ref! {
                    (StructField::nullable(
                        "phys_mapped",
                        schema! {
                            (StructField::nullable("phys_n", DataType::LONG).with_metadata([(
                                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                                "phys_n",
                            )])),
                        },
                    )
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_mapped",
                    )])),
                },
            )),
        ),
        (
            Pred::and(column_pred!("mapped.n"), Pred::TRUE),
            Some(PhysicalPredicate::Some(
                Pred::and(column_pred!("phys_mapped.phys_n"), Pred::TRUE).into(),
                schema_ref! {
                    (StructField::nullable(
                        "phys_mapped",
                        schema! {
                            (StructField::nullable("phys_n", DataType::LONG).with_metadata([(
                                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                                "phys_n",
                            )])),
                        },
                    )
                    .with_metadata([(
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        "phys_mapped",
                    )])),
                },
            )),
        ),
        (
            Pred::and(column_pred!("mapped.n"), Pred::FALSE),
            Some(PhysicalPredicate::StaticSkipAll),
        ),
    ];

    for (predicate, expected) in test_cases {
        let result =
            PhysicalPredicate::try_new(&predicate, &logical_schema, ColumnMappingMode::Name).ok();
        assert_eq!(
            result, expected,
            "Failed for predicate: {predicate:#?}, expected {expected:#?}, got {result:#?}"
        );
    }
}

/// Delta column names are case-insensitive, so predicates with differently-cased column names
/// must still resolve against the schema. The predicate is rewritten to use the schema's casing
/// (or physical names when column mapping is enabled).
#[rstest]
#[case::without_column_mapping(
    // predicate: createdat > 500 AND value < 100, schema: createdAt, Value
    schema! {
        nullable "createdAt": LONG,
        nullable "Value": LONG,
    },
    Pred::and(
        Pred::gt(col!("createdat"), lit(500i64)),
        Pred::lt(col!("value"), lit(100i64)),
    ),
    ColumnMappingMode::None,
    PhysicalPredicate::Some(
        Arc::new(Pred::and(
            Pred::gt(col!("createdAt"), lit(500i64)),
            Pred::lt(col!("Value"), lit(100i64)),
        )),
        schema_ref! {
            nullable "createdAt": LONG,
            nullable "Value": LONG,
        },
    ),
)]
#[case::with_column_mapping(
    // predicate: createdat > 500 AND value < 100, schema has physical name metadata
    schema! {
        (StructField::nullable("createdAt", DataType::LONG).with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            "phys_created",
        )])),
        (StructField::nullable("Value", DataType::LONG).with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            "phys_value",
        )])),
    },
    Pred::and(
        Pred::gt(col!("createdat"), lit(500i64)),
        Pred::lt(col!("value"), lit(100i64)),
    ),
    ColumnMappingMode::Name,
    PhysicalPredicate::Some(
        Arc::new(Pred::and(
            Pred::gt(col!("phys_created"), lit(500i64)),
            Pred::lt(col!("phys_value"), lit(100i64)),
        )),
        schema_ref! {
            (StructField::nullable("phys_created", DataType::LONG).with_metadata([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                "phys_created",
            )])),
            (StructField::nullable("phys_value", DataType::LONG).with_metadata([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                "phys_value",
            )])),
        },
    ),
)]
#[case::duplicate_column_different_casing(
    // predicate references same column with different casings: value > 5 AND VALUE < 10
    schema! { nullable "Value": LONG },
    Pred::and(
        Pred::gt(col!("value"), lit(5i64)),
        Pred::lt(col!("VALUE"), lit(10i64)),
    ),
    ColumnMappingMode::None,
    PhysicalPredicate::Some(
        Arc::new(Pred::and(
            Pred::gt(col!("Value"), lit(5i64)),
            Pred::lt(col!("Value"), lit(10i64)),
        )),
        schema_ref! { nullable "Value": LONG },
    ),
)]
#[case::nested_fields(
    // predicate references nested.fieldname but schema has Nested.FieldName
    schema! {
        nullable "Nested": {
            nullable "FieldName": LONG,
        },
    },
    column_pred!("nested.fieldname"),
    ColumnMappingMode::None,
    PhysicalPredicate::Some(
        column_pred!("Nested.FieldName").into(),
        schema_ref! {
            nullable "Nested": {
                nullable "FieldName": LONG,
            },
        },
    ),
)]
fn test_physical_predicate_case_insensitive(
    #[case] logical_schema: StructType,
    #[case] predicate: Predicate,
    #[case] column_mapping_mode: ColumnMappingMode,
    #[case] expected: PhysicalPredicate,
) {
    let result =
        PhysicalPredicate::try_new(&predicate, &logical_schema, column_mapping_mode).unwrap();
    assert_eq!(result, expected);
}

/// Unknown column still fails even with case-insensitive matching.
#[test]
fn test_physical_predicate_case_insensitive_unknown_column() {
    let logical_schema = schema! { nullable "createdAt": LONG };
    let result = PhysicalPredicate::try_new(
        &column_pred!("nonexistent"),
        &logical_schema,
        ColumnMappingMode::None,
    );
    assert!(result.is_err());
}

#[test]
fn test_scan_builder_accepts_predicate_on_unprojected_data_column() {
    let url = "memory:///test_table/";
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store);

    let schema = schema_ref! {
        nullable "number": LONG,
        nullable "a_float": FLOAT,
    };
    create_table(url, schema, "DefaultEngine")
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(&engine)
        .unwrap()
        .unwrap_committed();

    let snapshot = Snapshot::builder_for(url::Url::parse(url).unwrap())
        .build(&engine)
        .unwrap();

    let projection = snapshot.schema().project(&["a_float"]).unwrap();
    let predicate = Arc::new(col!("number").gt(lit(5_i64)));

    let scan = snapshot
        .scan_builder()
        .with_schema(projection)
        .with_predicate(predicate)
        .build()
        .expect("build should accept a predicate referencing a non-projection table column");

    assert_eq!(scan.logical_schema().fields().len(), 1);
}

#[test]
fn test_scan_builder_rejects_predicate_on_projection_only_metadata_column() {
    let url = "memory:///test_table/";
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store);

    let schema = schema_ref! { nullable "id": LONG };
    create_table(url, schema, "DefaultEngine")
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(&engine)
        .unwrap()
        .unwrap_committed();

    let snapshot = Snapshot::builder_for(url::Url::parse(url).unwrap())
        .build(&engine)
        .unwrap();

    // `my_row_index` is computed during the scan, not stored in the table,
    // so a predicate can't filter on it
    let projection = Arc::new(
        snapshot
            .schema()
            .add_metadata_column("my_row_index", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let predicate = Arc::new(col!("my_row_index").gt(lit(5_i64)));

    let err = snapshot
        .scan_builder()
        .with_schema(projection)
        .with_predicate(predicate)
        .build()
        .expect_err("build should reject predicate referencing a projection-only metadata column");
    let msg = err.to_string();
    assert!(
        msg.contains("Predicate references unknown column") && msg.contains("my_row_index"),
        "unexpected error: {msg}"
    );
}

/// Loads `table`'s v0 snapshot with a [`SyncEngine`], for the `without_row_transforms` tests.
fn without_transforms_snapshot(table: &str) -> (Arc<SyncEngine>, Arc<Snapshot>) {
    let path = std::fs::canonicalize(PathBuf::from(table)).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url)
        .at_version(0)
        .build(engine.as_ref())
        .unwrap();
    (engine, snapshot)
}

/// A partitioned table and a column-mapped table both build a transform spec.
/// `without_row_transforms` retains the spec but sets `skip_row_transforms`, so log replay
/// builds no per-file expressions while the structural transform stays describable.
#[rstest]
#[case::partitioned("./tests/data/basic_partitioned/")]
#[case::column_mapping("./tests/data/partition_cm/name/")]
fn test_without_row_transforms_retains_spec_and_sets_skip(#[case] table: &str) {
    let (_engine, snapshot) = without_transforms_snapshot(table);

    let scan = snapshot.clone().scan_builder().build().unwrap();
    assert!(scan.state_info.transform_spec.is_some());
    assert!(!scan.state_info.skip_row_transforms);

    let scan = snapshot
        .scan_builder()
        .without_row_transforms()
        .build()
        .unwrap();
    assert!(scan.state_info.transform_spec.is_some());
    assert!(scan.state_info.skip_row_transforms);
}

/// Under `without_row_transforms`, `scan_metadata` still lists files and surfaces the partition
/// values a connector needs to reconstruct rows itself, while every per-file transform is `None`.
/// Deletion vectors are covered by the sibling test.
#[test]
fn test_without_row_transforms_scan_metadata_lists_files_with_partition_values() {
    let (engine, snapshot) = without_transforms_snapshot("./tests/data/basic_partitioned/");
    let scan = snapshot
        .scan_builder()
        .without_row_transforms()
        .build()
        .unwrap();

    // (saw a file, saw non-empty partition values)
    fn collect(acc: &mut (bool, bool), scan_file: ScanFile) {
        acc.0 = true;
        acc.1 |= !scan_file.partition_values.is_empty();
    }
    let mut acc = (false, false);
    for metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        let metadata = metadata.unwrap();
        assert!(
            metadata.scan_file_transforms.iter().all(Option::is_none),
            "every per-file transform must be None under without_row_transforms"
        );
        acc = metadata.visit_scan_files(acc, collect).unwrap();
    }
    let (saw_file, saw_partition_values) = acc;
    assert!(
        saw_file,
        "scan_metadata should still list files under without_row_transforms"
    );
    assert!(
        saw_partition_values,
        "partition values must still be surfaced so the connector can inject them itself"
    );
}

/// Column mapping also builds a per-file transform (the physical-to-logical rename). Under
/// `without_row_transforms`, `scan_metadata` still lists a column-mapped table's files with every
/// per-file transform `None`, leaving the rename to the connector.
#[test]
fn test_without_row_transforms_scan_metadata_lists_files_column_mapping() {
    let table = "table-with-columnmapping-mode-name";
    let tempdir = load_test_data("tests/golden_data", table).unwrap();
    // Golden tables extract to `<name>/delta/` (with a sibling `expected/`).
    let table_path = tempdir.path().join(table).join("delta");
    let url = url::Url::from_directory_path(table_path).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let scan = snapshot
        .scan_builder()
        .without_row_transforms()
        .build()
        .unwrap();

    let mut saw_file = false;
    for metadata in scan.scan_metadata(&engine).unwrap() {
        let metadata = metadata.unwrap();
        assert!(
            metadata.scan_file_transforms.iter().all(Option::is_none),
            "every per-file transform must be None under without_row_transforms"
        );
        saw_file = true;
    }
    assert!(
        saw_file,
        "scan_metadata should list the column-mapped table's files under without_row_transforms"
    );
}

#[test]
fn test_without_row_transforms_rejects_execute() {
    let (engine, snapshot) = without_transforms_snapshot("./tests/data/basic_partitioned/");
    let scan = snapshot
        .scan_builder()
        .without_row_transforms()
        .build()
        .unwrap();
    let err = scan
        .execute(engine)
        .err()
        .expect("execute must error when row transforms are skipped");
    assert!(
        err.to_string().contains("without_row_transforms"),
        "unexpected error: {err}"
    );
}

/// Row commit version metadata columns are unsupported by scans, so requesting one errors at
/// build time regardless of `without_row_transforms`.
#[rstest]
fn test_scan_rejects_row_commit_version(#[values(false, true)] without_row_transforms: bool) {
    let (_engine, snapshot) = without_transforms_snapshot("./tests/data/basic_partitioned/");
    let schema = Arc::new(
        snapshot
            .schema()
            .add_metadata_column("rcv", MetadataColumnSpec::RowCommitVersion)
            .unwrap(),
    );
    let mut builder = snapshot.scan_builder().with_schema(schema);
    if without_row_transforms {
        builder = builder.without_row_transforms();
    }
    let err = builder
        .build()
        .expect_err("row commit version columns are unsupported by scans");
    assert!(
        err.to_string()
            .contains("Row commit versions not supported"),
        "unexpected error: {err}"
    );
}

/// Deletion vectors flow through scan metadata independently of the row transform, so they are
/// still surfaced under `without_row_transforms` while every per-file transform is `None`.
#[test]
fn test_without_row_transforms_scan_metadata_surfaces_deletion_vectors() {
    // The deletion vector is added at the latest version, so load the full table (not v0).
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let scan = snapshot
        .scan_builder()
        .without_row_transforms()
        .build()
        .unwrap();

    fn dv_callback(seen: &mut bool, scan_file: ScanFile) {
        *seen |= scan_file.dv_info.deletion_vector.is_some();
    }
    let mut saw_dv = false;
    for res in scan.scan_metadata(&engine).unwrap() {
        let scan_metadata = res.unwrap();
        assert!(
            scan_metadata
                .scan_file_transforms
                .iter()
                .all(Option::is_none),
            "every per-file transform must be None under without_row_transforms"
        );
        saw_dv = scan_metadata.visit_scan_files(saw_dv, dv_callback).unwrap();
    }
    assert!(
        saw_dv,
        "deletion vector info should still be surfaced under without_row_transforms"
    );
}

fn get_files_for_scan(scan: Scan, engine: &dyn Engine) -> DeltaResult<Vec<String>> {
    let scan_metadata_iter = scan.scan_metadata(engine)?;
    fn scan_metadata_callback(paths: &mut Vec<String>, scan_file: ScanFile) {
        paths.push(scan_file.path.to_string());
        assert!(scan_file.dv_info.deletion_vector.is_none());
    }
    let mut files = vec![];
    for res in scan_metadata_iter {
        let scan_metadata = res?;
        files = scan_metadata.visit_scan_files(files, scan_metadata_callback)?;
    }
    Ok(files)
}

#[test]
fn test_scan_metadata_paths() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let files = get_files_for_scan(scan, &engine).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0],
        "part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"
    );
}

#[test_log::test]
fn test_scan_metadata() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let files: Vec<Box<dyn EngineData>> = scan.execute(engine).unwrap().try_collect().unwrap();

    assert_eq!(files.len(), 1);
    let num_rows = files[0].as_ref().len();
    assert_eq!(num_rows, 10)
}

#[test_log::test]
fn test_scan_metadata_from_same_version() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let version = snapshot.version();
    let scan = snapshot.scan_builder().build().unwrap();
    let files: Vec<_> = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .map_ok(|ScanMetadata { scan_files, .. }| {
            let (underlying_data, selection_vector) = scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
                .unwrap()
                .into();
            let filtered_batch =
                filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();
            Box::new(ArrowEngineData::from(filtered_batch)) as Box<dyn EngineData>
        })
        .try_collect()
        .unwrap();
    let new_files: Vec<_> = scan
        .scan_metadata_from(engine.as_ref(), version, files, None)
        .unwrap()
        .try_collect()
        .unwrap();

    assert_eq!(new_files.len(), 1);
}

// reading v0 with 3 files.
// updating to v1 with 3 more files added.
#[test_log::test]
fn test_scan_metadata_from_with_update() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url.clone())
        .at_version(0)
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let files: Vec<_> = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .map_ok(|ScanMetadata { scan_files, .. }| {
            let (underlying_data, selection_vector) = scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
                .unwrap()
                .into();
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap()
        })
        .try_collect()
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].num_rows(), 3);

    let files: Vec<_> = files
        .into_iter()
        .map(|b| Box::new(ArrowEngineData::from(b)) as Box<dyn EngineData>)
        .collect();
    let snapshot = Snapshot::builder_for(url)
        .at_version(1)
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let new_files: Vec<_> = scan
        .scan_metadata_from(engine.as_ref(), 0, files, None)
        .unwrap()
        .map_ok(|ScanMetadata { scan_files, .. }| {
            let (underlying_data, selection_vector) = scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
                .unwrap()
                .into();
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap()
        })
        .try_collect()
        .unwrap();
    assert_eq!(new_files.len(), 2);
    assert_eq!(new_files[0].num_rows(), 3);
    assert_eq!(new_files[1].num_rows(), 3);
}

#[test]
fn test_get_partition_value() {
    let cases = [
        (
            "string",
            PrimitiveType::String,
            Scalar::String("string".to_string()),
        ),
        ("123", PrimitiveType::Integer, Scalar::Integer(123)),
        ("1234", PrimitiveType::Long, Scalar::Long(1234)),
        ("12", PrimitiveType::Short, Scalar::Short(12)),
        ("1", PrimitiveType::Byte, Scalar::Byte(1)),
        ("1.1", PrimitiveType::Float, Scalar::Float(1.1)),
        ("10.10", PrimitiveType::Double, Scalar::Double(10.1)),
        ("true", PrimitiveType::Boolean, Scalar::Boolean(true)),
        ("2024-01-01", PrimitiveType::Date, Scalar::Date(19723)),
        ("1970-01-01", PrimitiveType::Date, Scalar::Date(0)),
        (
            "1970-01-01 00:00:00",
            PrimitiveType::Timestamp,
            Scalar::Timestamp(0),
        ),
        (
            "1970-01-01 00:00:00.123456",
            PrimitiveType::Timestamp,
            Scalar::Timestamp(123456),
        ),
        (
            "1970-01-01 00:00:00.123456789",
            PrimitiveType::Timestamp,
            Scalar::Timestamp(123456),
        ),
        (
            // RFC 3339 with a non-UTC offset: normalized to UTC (1969-12-31T19:00:00Z)
            "1970-01-01T00:00:00+05:00",
            PrimitiveType::Timestamp,
            Scalar::Timestamp(-18000000000),
        ),
    ];

    for (raw, data_type, expected) in &cases {
        let value = crate::scan::transform_spec::parse_partition_value_raw(
            Some(&raw.to_string()),
            &DataType::Primitive(data_type.clone()),
            crate::timestamp_timezone::TimestampTimezone::default(),
        )
        .unwrap();
        assert_eq!(value, *expected);
    }
}

#[test]
fn test_replay_for_scan_metadata() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
    let url = url::Url::from_directory_path(path.unwrap()).unwrap();
    let engine = SyncEngine::new();

    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let result = scan.replay_for_scan_metadata(&engine).unwrap();
    let data: Vec<_> = result.actions.try_collect().unwrap();
    // Metadata and protocol parts have an all-null `add.path` column and are skipped. Transaction
    // parts omit that column and are retained conservatively.
    assert_eq!(data.len(), 3);
}

#[test]
fn test_data_row_group_skipping() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
    let url = url::Url::from_directory_path(path.unwrap()).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    // No predicate pushdown attempted, so the one data file should be returned.
    //
    // NOTE: The data file contains only five rows -- near guaranteed to produce one row group.
    let scan = snapshot.clone().scan_builder().build().unwrap();
    let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
    assert_eq!(data.len(), 1);

    // Ineffective predicate pushdown attempted, so the one data file should be returned.
    let int_col = col!("numeric.ints.int32");
    let value = lit(1000i32);
    let predicate = Arc::new(int_col.clone().gt(value.clone()));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
    assert_eq!(data.len(), 1);

    // TODO(#860): we disable predicate pushdown until we support row indexes. Update this test
    // accordingly after support is reintroduced.
    //
    // Effective predicate pushdown, so no data files should be returned. BUT since we disabled
    // predicate pushdown, the one data file is still returned.
    let predicate = Arc::new(int_col.lt(value));
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let data: Vec<_> = scan.execute(engine).unwrap().try_collect().unwrap();
    assert_eq!(data.len(), 1);
}

#[test]
fn test_missing_column_row_group_skipping() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
    let url = url::Url::from_directory_path(path.unwrap()).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    // Predicate over a logically valid but physically missing column. No data files should be
    // returned because the column is inferred to be all-null.
    //
    // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 - This
    // optimization is currently disabled, so the one data file is still returned.
    let predicate = Arc::new(col!("missing").lt(lit(1000i64)));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let data: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
    assert_eq!(data.len(), 1);

    // Predicate over a logically missing column fails the scan
    let predicate = Arc::new(col!("numeric.ints.invalid").lt(lit(1000)));
    snapshot
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .expect_err("unknown column");
}

#[test_log::test]
fn test_scan_with_checkpoint() -> DeltaResult<()> {
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/data/with_checkpoint_no_last_checkpoint/",
    ))?;

    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let scan = snapshot.scan_builder().build()?;
    let files = get_files_for_scan(scan, &engine)?;
    // test case:
    //
    // commit0:     P and M, no add/remove
    // commit1:     add file-ad1
    // commit2:     remove file-ad1, add file-a19
    // checkpoint2: remove file-ad1, add file-a19
    // commit3:     remove file-a19, add file-70b
    //
    // thus replay should produce only file-70b
    assert_eq!(
        files,
        vec!["part-00000-70b1dcdf-0236-4f63-a072-124cdbafd8a0-c000.snappy.parquet"]
    );
    Ok(())
}

/// Helper to validate that JSON stats object values match the corresponding parsed struct array.
fn assert_stats_struct_matches_json(
    struct_array: &StructArray,
    json_object: &serde_json::Map<String, serde_json::Value>,
    row_idx: usize,
    field_name: &str,
) {
    for (col_name, json_val) in json_object {
        let Some(col) = struct_array.column_by_name(col_name) else {
            continue;
        };
        if col.is_null(row_idx) {
            continue;
        }
        // Currently only validates Int64 columns (the table has integer stats)
        if let Some(int_col) = col.as_any().downcast_ref::<Int64Array>() {
            assert_eq!(
                json_val.as_i64().unwrap(),
                int_col.value(row_idx),
                "{field_name}.{col_name} mismatch at row {row_idx}"
            );
        }
    }
}

/// Test that [`StatsOptions::all`] outputs parsed stats in scan_metadata batches.
/// Uses a table with a checkpoint that contains stats_parsed for e2e verification.
#[test]
fn test_scan_metadata_with_stats_columns() {
    const STATS_PARSED_COL: &str = "stats_parsed";

    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .build()
        .unwrap();

    let scan_metadata_results: Vec<_> = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(
        !scan_metadata_results.is_empty(),
        "Should have scan metadata"
    );

    let mut total_num_records: i64 = 0;
    let mut file_count = 0;

    for scan_metadata in scan_metadata_results {
        let (underlying_data, selection_vector) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();
        let filtered_batch =
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();

        // Verify stats_parsed schema
        let schema = filtered_batch.schema();
        let field = schema
            .field_with_name(STATS_PARSED_COL)
            .expect("Schema should contain stats_parsed column");
        assert!(
            matches!(field.data_type(), ArrowDataType::Struct(_)),
            "stats_parsed should be a struct type, got: {:?}",
            field.data_type()
        );

        // Extract stats_parsed struct array
        let stats_parsed = get_column!(filtered_batch, STATS_PARSED_COL, StructArray);
        let num_records = get_column!(stats_parsed, NUM_RECORDS, Int64Array);
        let min_values = get_column!(stats_parsed, MIN_VALUES, StructArray);
        let max_values = get_column!(stats_parsed, MAX_VALUES, StructArray);
        let null_count = get_column!(stats_parsed, NULL_COUNT, StructArray);

        // Extract JSON stats column
        let stats_json = get_column!(filtered_batch, "stats", StringArray);

        // Validate each row: JSON stats should match structured stats
        for i in 0..stats_json.len() {
            if stats_parsed.is_null(i) || stats_json.is_null(i) {
                continue;
            }

            let json_stats: serde_json::Value =
                serde_json::from_str(stats_json.value(i)).expect("stats JSON should be valid");

            // Validate numRecords
            if let Some(json_num) = json_stats.get(NUM_RECORDS).and_then(|v| v.as_i64()) {
                assert_eq!(
                    json_num,
                    num_records.value(i),
                    "numRecords mismatch at row {i}"
                );
            }

            // Validate minValues, maxValues, nullCount
            if let Some(obj) = json_stats.get(MIN_VALUES).and_then(|v| v.as_object()) {
                assert_stats_struct_matches_json(min_values, obj, i, MIN_VALUES);
            }
            if let Some(obj) = json_stats.get(MAX_VALUES).and_then(|v| v.as_object()) {
                assert_stats_struct_matches_json(max_values, obj, i, MAX_VALUES);
            }
            if let Some(obj) = json_stats.get(NULL_COUNT).and_then(|v| v.as_object()) {
                assert_stats_struct_matches_json(null_count, obj, i, NULL_COUNT);
            }

            total_num_records += num_records.value(i);
            file_count += 1;
        }
    }

    assert!(file_count > 0, "Should have processed at least one file");
    assert!(total_num_records > 0, "Should have non-zero numRecords");
    println!(
        "Verified {file_count} files with total {total_num_records} records from stats_parsed"
    );
}

#[test]
fn test_build_actions_meta_predicate_with_predicate() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

    // Build a scan with a predicate eligible for data skipping
    let predicate = Arc::new(Pred::gt(col!("id"), lit(400i64)));
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();

    let meta_pred = scan.build_actions_meta_predicate();
    assert!(
        meta_pred.is_some(),
        "Should produce an actions meta predicate for a data-skipping-eligible predicate"
    );

    // Verify all column references are prefixed with add.stats_parsed
    let pred = meta_pred.unwrap();
    for col_ref in pred.references() {
        let path: Vec<_> = col_ref.iter().collect();
        assert_eq!(
            path[0], "add",
            "Column reference should start with 'add': {col_ref}"
        );
        assert_eq!(
            path[1], "stats_parsed",
            "Column reference should have 'stats_parsed' as second element: {col_ref}"
        );
    }
}

#[test]
fn test_build_actions_meta_predicate_no_predicate() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

    // Build a scan with no predicate
    let scan = snapshot.scan_builder().build().unwrap();

    assert!(
        scan.build_actions_meta_predicate().is_none(),
        "Should return None when there is no predicate"
    );
}

#[test]
fn test_build_actions_meta_predicate_static_skip_all() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

    // A predicate that statically evaluates to false should produce StaticSkipAll,
    // which means build_actions_meta_predicate returns None.
    let predicate = Arc::new(Pred::FALSE);
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();

    assert!(
        scan.build_actions_meta_predicate().is_none(),
        "StaticSkipAll predicate should return None"
    );
}

// Partition-only scans have no stats schema, so the partition schema must enable the rewrite.
#[rstest]
#[case::equality(Pred::eq(
    col!("modified"),
    lit("2021-02-01"),
), None)]
#[case::date_cast_range(Pred::and(
    Pred::ge(
        Expr::cast(col!("modified"), DataType::DATE),
        Scalar::Date(20_641),
    ),
    Pred::lt(
        Expr::cast(col!("modified"), DataType::DATE),
        Scalar::Date(20_644),
    ),
), Some("CAST(Column(add.partitionValues_parsed.modified) AS date)"))]
fn test_build_actions_meta_predicate_partition_only(
    #[case] predicate: Pred,
    #[case] expected_cast: Option<&str>,
) {
    // `app-txn-checkpoint` is partitioned by `modified` (string), no column mapping.
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()
        .unwrap();

    let meta_pred = scan
        .build_actions_meta_predicate()
        .expect("partition-only predicate should produce a checkpoint meta-predicate");
    let rendered = meta_pred.to_string();
    if let Some(expected_cast) = expected_cast {
        assert!(rendered.contains(expected_cast), "{rendered}");
    }
    let refs: Vec<String> = meta_pred
        .references()
        .into_iter()
        .map(|c| c.to_string())
        .collect();
    assert_eq!(
        refs,
        vec!["add.partitionValues_parsed.modified".to_string()]
    );
}

// Under column mapping, `partitionValues_parsed` is keyed by the physical partition name, so the
// checkpoint meta-predicate must reference that physical name (not the logical one) in both name
// and id mapping modes.
#[rstest]
#[case::name_mode("name")]
#[case::id_mode("id")]
fn test_build_actions_meta_predicate_partition_column_mapping(
    #[case] mode: &str,
    #[values(false, true)] casted: bool,
) {
    // `partition_cm/{name,id}` use column mapping, partitioned by `category`
    // (physical name col-6dc68f07-711d-4f00-8bd6-1f5bc698e8ad in both fixtures).
    let path =
        std::fs::canonicalize(PathBuf::from(format!("./tests/data/partition_cm/{mode}/"))).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

    let predicate = if casted {
        Pred::eq(
            Expr::cast(col!("category"), DataType::DATE),
            Scalar::Date(20_641),
        )
    } else {
        Pred::eq(col!("category"), lit("a"))
    };
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()
        .unwrap();

    let meta_pred = scan
        .build_actions_meta_predicate()
        .expect("partition predicate under column mapping should produce a meta-predicate");
    let rendered = meta_pred.to_string();
    assert_eq!(rendered.contains("CAST("), casted, "{rendered}");
    let refs: Vec<String> = meta_pred
        .references()
        .into_iter()
        .map(|c| c.to_string())
        .collect();
    // The hyphenated physical name is backtick-quoted in the column display.
    assert!(
        refs.iter().any(|r| r.contains("partitionValues_parsed")
            && r.contains("col-6dc68f07-711d-4f00-8bd6-1f5bc698e8ad")),
        "expected a reference to the PHYSICAL partition column under partitionValues_parsed, \
         got {refs:?}"
    );
}

struct RgSpec {
    id: i64,
    x_min: i64,
    x_max: i64,
    part: Option<&'static str>,
}

struct CheckpointRowGroup {
    id_max: Vec<Option<i64>>,
    id_min: Vec<Option<i64>>,
    id_null_counts: Vec<Option<i64>>,
    x_max: Vec<Option<i64>>,
    x_min: Vec<Option<i64>>,
    x_null_counts: Vec<Option<i64>>,
    num_records: Vec<i64>,
    parts: Vec<Option<&'static str>>,
}

/// Builds checkpoint parquet with one row group per write.
struct CheckpointParquetBuilder {
    arrow_schema: Arc<ArrowSchema>,
    stat_value_fields: Fields,
    stats_fields: Fields,
    partition_fields: Fields,
    add_fields: Fields,
    include_partition_values: bool,
    buffer: Vec<u8>,
    writer: Option<ArrowWriter<Vec<u8>>>,
}

impl CheckpointParquetBuilder {
    fn new() -> Self {
        Self::with_partition_values(true)
    }

    fn without_partition_values() -> Self {
        Self::with_partition_values(false)
    }

    fn with_partition_values(include_partition_values: bool) -> Self {
        let stat_value_fields = Fields::from(vec![
            Field::new("id", ArrowDataType::Int64, true),
            Field::new("x", ArrowDataType::Int64, true),
        ]);
        let stats_fields = Fields::from(vec![
            Field::new(
                MAX_VALUES,
                ArrowDataType::Struct(stat_value_fields.clone()),
                true,
            ),
            Field::new(
                MIN_VALUES,
                ArrowDataType::Struct(stat_value_fields.clone()),
                true,
            ),
            Field::new(
                NULL_COUNT,
                ArrowDataType::Struct(stat_value_fields.clone()),
                true,
            ),
            Field::new(NUM_RECORDS, ArrowDataType::Int64, true),
        ]);
        let partition_fields = Fields::from(vec![Field::new("part", ArrowDataType::Utf8, true)]);
        let mut add_fields = vec![Field::new(
            "stats_parsed",
            ArrowDataType::Struct(stats_fields.clone()),
            true,
        )];
        if include_partition_values {
            add_fields.push(Field::new(
                "partitionValues_parsed",
                ArrowDataType::Struct(partition_fields.clone()),
                true,
            ));
        }
        let add_fields = Fields::from(add_fields);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "add",
            ArrowDataType::Struct(add_fields.clone()),
            true,
        )]));
        let buffer = Vec::new();
        let writer = ArrowWriter::try_new(buffer, arrow_schema.clone(), None).unwrap();
        Self {
            arrow_schema,
            stat_value_fields,
            stats_fields,
            partition_fields,
            add_fields,
            include_partition_values,
            buffer: Vec::new(),
            writer: Some(writer),
        }
    }

    /// Writes one row group with the given per-file stats.
    fn write_row_group(
        &mut self,
        max_ids: &[Option<i64>],
        min_ids: &[Option<i64>],
        null_counts: &[Option<i64>],
        num_records: &[i64],
    ) {
        let len = max_ids.len();
        self.write_values(CheckpointRowGroup {
            id_max: max_ids.to_vec(),
            id_min: min_ids.to_vec(),
            id_null_counts: null_counts.to_vec(),
            x_max: vec![None; len],
            x_min: vec![None; len],
            x_null_counts: vec![None; len],
            num_records: num_records.to_vec(),
            parts: vec![None; len],
        });
    }

    fn write_rg_group(&mut self, group: &[RgSpec]) {
        let ids: Vec<Option<i64>> = group.iter().map(|spec| Some(spec.id)).collect();
        let zeros = vec![Some(0); group.len()];
        self.write_values(CheckpointRowGroup {
            id_max: ids.clone(),
            id_min: ids,
            id_null_counts: zeros.clone(),
            x_max: group.iter().map(|spec| Some(spec.x_max)).collect(),
            x_min: group.iter().map(|spec| Some(spec.x_min)).collect(),
            x_null_counts: zeros,
            num_records: vec![1; group.len()],
            parts: group.iter().map(|spec| spec.part).collect(),
        });
    }

    fn write_values(&mut self, values: CheckpointRowGroup) {
        let make_stat_struct = |ids: Vec<Option<i64>>, xs: Vec<Option<i64>>| {
            StructArray::from(vec![
                (
                    self.stat_value_fields[0].clone(),
                    Arc::new(Int64Array::from(ids)) as Arc<dyn Array>,
                ),
                (
                    self.stat_value_fields[1].clone(),
                    Arc::new(Int64Array::from(xs)) as Arc<dyn Array>,
                ),
            ])
        };
        let stats_parsed = StructArray::from(vec![
            (
                self.stats_fields[0].clone(),
                Arc::new(make_stat_struct(values.id_max, values.x_max)) as Arc<dyn Array>,
            ),
            (
                self.stats_fields[1].clone(),
                Arc::new(make_stat_struct(values.id_min, values.x_min)) as Arc<dyn Array>,
            ),
            (
                self.stats_fields[2].clone(),
                Arc::new(make_stat_struct(
                    values.id_null_counts,
                    values.x_null_counts,
                )) as Arc<dyn Array>,
            ),
            (
                self.stats_fields[3].clone(),
                Arc::new(Int64Array::from(values.num_records)) as Arc<dyn Array>,
            ),
        ]);
        let mut add_children = vec![(
            self.add_fields[0].clone(),
            Arc::new(stats_parsed) as Arc<dyn Array>,
        )];
        if self.include_partition_values {
            let partition_values = StructArray::from(vec![(
                self.partition_fields[0].clone(),
                Arc::new(StringArray::from(values.parts)) as Arc<dyn Array>,
            )]);
            add_children.push((
                self.add_fields[1].clone(),
                Arc::new(partition_values) as Arc<dyn Array>,
            ));
        }
        let add = StructArray::from(add_children);
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), vec![Arc::new(add)]).unwrap();
        let writer = self.writer.as_mut().unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }

    /// Finishes writing and returns the parquet bytes.
    fn finish(mut self) -> Bytes {
        let writer = self.writer.take().unwrap();
        self.buffer = writer.into_inner().unwrap();
        Bytes::from(self.buffer)
    }
}

fn rg(id: i64, x_min: i64, x_max: i64, part: Option<&'static str>) -> RgSpec {
    RgSpec {
        id,
        x_min,
        x_max,
        part,
    }
}

fn write_checkpoint(groups: &[&[RgSpec]], include_partition_values: bool) -> Bytes {
    let mut builder = if include_partition_values {
        CheckpointParquetBuilder::new()
    } else {
        CheckpointParquetBuilder::without_partition_values()
    };
    for group in groups {
        builder.write_rg_group(group);
    }
    builder.finish()
}

/// Builds a checkpoint meta-predicate scoped under `add`, mirroring `build_actions_meta_predicate`.
fn build_checkpoint_meta_predicate(
    pred: &Pred,
    partition_columns: &HashSet<ColumnName>,
    stats_columns: &HashSet<ColumnName>,
) -> Option<Pred> {
    let skipping_pred =
        as_checkpoint_skipping_predicate(pred, partition_columns, &HashSet::new(), stats_columns)?;
    let mut prefixer = PrefixColumns {
        prefix: column_name!("add"),
    };
    Some(prefixer.transform_pred(&skipping_pred).into_owned())
}

/// Applies a meta predicate as a row group filter and returns the total rows read.
fn apply_row_group_filter(parquet_bytes: Bytes, meta_predicate: &Pred) -> usize {
    ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)
        .unwrap()
        .with_row_group_filter(meta_predicate, None)
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

/// Tests checkpoint row group skipping end-to-end with the parquet row group filter.
///
/// Shared parquet layout (4 row groups):
///   - RG 0 (2 rows): maxValues.id = [100, NULL], nullCount.id = [5, NULL]
///   - RG 1 (1 row):  maxValues.id = 300, nullCount.id = 0
///   - RG 2 (1 row):  maxValues.id = 50, nullCount.id = 10
///   - RG 3 (2 rows): maxValues.id = [150, 40], nullCount.id = [0, NULL]
///
/// | Predicate      | RG 0 (2 rows)         | RG 1 (1 row)       | RG 2 (1 row)       | RG 3 (2 rows)        | Total |
/// |----------------|-----------------------|--------------------|--------------------|-----------------------|-------|
/// | id > 200       | keep (null max stats) | keep (max=300>200) | skip (max=50<200)  | skip (max=150<200)    | 3     |
/// | id IS NULL     | keep (nullCount>0)    | skip (nullCount=0) | keep (nullCount=10)| keep (null nullCount) | 5     |
/// | id IS NOT NULL | no predicate (col vs col, #1873)                                                       | 6     |
#[rstest]
#[case::comparison(
    Pred::gt(col!("id"), lit(200i64)),
    Some(3),
    "keep RG 0 (null stats) + RG 1 (max>200), skip RG 2 + RG 3 (max<200)"
)]
#[case::is_null(
    Pred::is_null(col!("id")),
    Some(5),
    "keep RG 0 (nullCount>0) + RG 2 (nullCount>0) + RG 3 (null nullCount), skip RG 1 (nullCount=0)"
)]
#[case::is_not_null(
    Pred::not(Pred::is_null(col!("id"))),
    None,
    "IS NOT NULL produces no skipping predicate (column vs column, #1873)"
)]
fn test_checkpoint_row_group_skipping(
    #[case] pred: Pred,
    #[case] expected_rows: Option<usize>,
    #[case] description: &str,
) {
    let mut builder = CheckpointParquetBuilder::new();
    // RG 0: mixed null/non-null stats. maxValues.id = [100, NULL], nullCount.id = [5, NULL].
    builder.write_row_group(
        &[Some(100), None],
        &[Some(1), None],
        &[Some(5), None],
        &[100, 50],
    );
    // RG 1: maxValues.id = 300, nullCount.id = 0.
    builder.write_row_group(&[Some(300)], &[Some(201)], &[Some(0)], &[100]);
    // RG 2: maxValues.id = 50, nullCount.id = 10.
    builder.write_row_group(&[Some(50)], &[Some(1)], &[Some(10)], &[100]);
    // RG 3: maxValues.id = [150, 40], nullCount.id = [0, NULL].
    // Tests that null nullCount stats are conservatively kept for IS NULL.
    builder.write_row_group(
        &[Some(150), Some(40)],
        &[Some(1), Some(1)],
        &[Some(0), None],
        &[100, 50],
    );
    let parquet_bytes = builder.finish();

    let meta_predicate =
        build_checkpoint_meta_predicate(&pred, &HashSet::new(), &all_referenced_columns(&pred));

    match expected_rows {
        Some(expected) => {
            let meta_predicate =
                meta_predicate.expect("predicate should produce a checkpoint skipping predicate");
            let total_rows = apply_row_group_filter(parquet_bytes, &meta_predicate);
            assert_eq!(total_rows, expected, "{description}");
        }
        None => {
            assert!(meta_predicate.is_none(), "{description}");
            // Without a predicate, all row groups are read.
            let total_rows: usize = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            assert_eq!(total_rows, 6, "all rows should be read without a predicate");
        }
    }
}

fn surviving_ids(parquet_bytes: Bytes, pred: &Pred) -> Vec<i64> {
    let meta_predicate = build_checkpoint_meta_predicate(
        pred,
        &HashSet::from([column_name!("part")]),
        &HashSet::from([column_name!("x")]),
    );
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes).unwrap();
    if let Some(meta_predicate) = &meta_predicate {
        builder = builder.with_row_group_filter(meta_predicate, None);
    }
    let mut ids: Vec<i64> = builder
        .build()
        .unwrap()
        .map(Result::unwrap)
        .flat_map(|batch| {
            let add = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let stats = struct_field(add, "stats_parsed");
            let min = struct_field(stats, MIN_VALUES);
            min.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn struct_field<'a>(parent: &'a StructArray, name: &str) -> &'a StructArray {
    parent
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
}

fn standard_multi_rg() -> Bytes {
    write_checkpoint(
        &[
            &[rg(1, 0, 10, Some("a"))],
            &[rg(2, 100, 110, Some("b"))],
            &[rg(3, 200, 210, Some("a"))],
            &[rg(4, 900, 910, Some("c"))],
        ],
        true,
    )
}

#[rstest]
#[case::stats_gt(Pred::gt(col!("x"), lit(150i64)), vec![3, 4])]
#[case::stats_le(Pred::le(col!("x"), lit(110i64)), vec![1, 2])]
#[case::stats_all_kept(
    Pred::ge(col!("x"), lit(0i64)),
    vec![1, 2, 3, 4]
)]
#[case::partition_eq(Pred::eq(col!("part"), lit("a")), vec![1, 3])]
#[case::partition_lt(Pred::lt(col!("part"), lit("b")), vec![1, 3])]
#[case::partition_all_pruned(Pred::eq(col!("part"), lit("z")), vec![])]
#[case::partition_cast_kept(
    Pred::eq(
        Expr::cast(col!("part"), DataType::DATE),
        Scalar::Date(18_628),
    ),
    vec![1, 2, 3, 4]
)]
#[case::partition_cast_and_stats(
    Pred::and(
        Pred::eq(
            Expr::cast(col!("part"), DataType::DATE),
            Scalar::Date(18_628),
        ),
        Pred::gt(col!("x"), lit(150i64)),
    ),
    vec![3, 4]
)]
#[case::and_stats_and_partition(
    Pred::and(
        Pred::eq(col!("part"), lit("a")),
        Pred::gt(col!("x"), lit(150i64)),
    ),
    vec![3]
)]
#[case::or_stats_or_partition(
    Pred::or(
        Pred::eq(col!("part"), lit("c")),
        Pred::gt(col!("x"), lit(150i64)),
    ),
    vec![3, 4]
)]
#[case::partition_is_null(Pred::is_null(col!("part")), vec![])]
#[case::partition_is_not_null(
    Pred::is_not_null(col!("part")),
    vec![1, 2, 3, 4]
)]
fn test_checkpoint_reader_skips_expected_row_groups(
    #[case] pred: Pred,
    #[case] expected: Vec<i64>,
) {
    assert_eq!(
        surviving_ids(standard_multi_rg(), &pred),
        expected,
        "{pred:?}"
    );
}

#[rstest]
#[case::is_null_keeps_only_null_group(
    &[&[rg(1, 0, 10, Some("a"))] as &[RgSpec], &[rg(2, 100, 110, None)], &[rg(3, 200, 210, Some("c"))]],
    Pred::is_null(col!("part")),
    vec![2],
)]
// Footer min/max ignore the null-valued Add, so the non-matching range prunes the group.
#[case::mixed_group_with_null_partition_pruned(
    &[&[rg(1, 0, 10, Some("a")), rg(2, 100, 110, None)] as &[RgSpec]],
    Pred::eq(col!("part"), lit("z")),
    vec![],
)]
#[case::partition_range_contains_target(
    &[&[rg(1, 0, 10, Some("a")), rg(2, 100, 110, Some("c"))] as &[RgSpec]],
    Pred::eq(col!("part"), lit("b")),
    vec![1, 2],
)]
#[case::all_null_group_pruned_under_eq(
    &[&[rg(1, 0, 10, Some("a"))] as &[RgSpec], &[rg(2, 100, 110, None), rg(3, 200, 210, None)]],
    Pred::eq(col!("part"), lit("z")),
    vec![],
)]
#[case::all_null_group_pruned_under_gt(
    &[&[rg(1, 0, 10, Some("a"))] as &[RgSpec], &[rg(2, 100, 110, None), rg(3, 200, 210, None)]],
    Pred::gt(col!("part"), lit("m")),
    vec![],
)]
#[case::all_null_group_pruned_under_is_not_null(
    &[&[rg(1, 0, 10, Some("a"))] as &[RgSpec], &[rg(2, 100, 110, None), rg(3, 200, 210, None)]],
    Pred::is_not_null(col!("part")),
    vec![1],
)]
fn test_checkpoint_reader_handles_partition_groups(
    #[case] groups: &[&[RgSpec]],
    #[case] pred: Pred,
    #[case] expected: Vec<i64>,
) {
    let parquet_bytes = write_checkpoint(groups, true);
    assert_eq!(surviving_ids(parquet_bytes, &pred), expected, "{pred:?}");
}

/// A checkpoint may omit `partitionValues_parsed`; the absent leaf remains non-pruning.
#[rstest]
#[case::comparison(Pred::eq(col!("part"), lit("z")))]
#[case::is_not_null(Pred::is_not_null(col!("part")))]
fn test_checkpoint_reader_keeps_missing_partition_column(#[case] pred: Pred) {
    let parquet_bytes = write_checkpoint(&[&[rg(1, 0, 10, Some("a"))] as &[RgSpec]], false);
    assert_eq!(surviving_ids(parquet_bytes, &pred), vec![1]);
}

#[derive(Debug)]
struct RecordedParquetRead {
    files: Vec<FileMeta>,
    physical_schema: schema::SchemaRef,
    predicate: Option<PredicateRef>,
}

struct RecordingParquetHandler {
    inner: Arc<dyn ParquetHandler>,
    reads: Mutex<Vec<RecordedParquetRead>>,
}

impl RecordingParquetHandler {
    fn new(inner: Arc<dyn ParquetHandler>) -> Self {
        Self {
            inner,
            reads: Mutex::new(Vec::new()),
        }
    }

    fn take_reads(&self) -> Vec<RecordedParquetRead> {
        std::mem::take(&mut *self.reads.lock().unwrap())
    }
}

impl ParquetHandler for RecordingParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: schema::SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        self.reads.lock().unwrap().push(RecordedParquetRead {
            files: files.to_vec(),
            physical_schema: physical_schema.clone(),
            predicate: predicate.clone(),
        });
        self.inner
            .read_parquet_files(files, physical_schema, predicate)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }

    fn write_parquet_file(
        &self,
        location: url::Url,
        data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        self.inner.write_parquet_file(location, data)
    }
}

#[test_log::test]
fn scan_execute_passes_scan_file_modification_time_to_parquet_handler() {
    let path = fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = Url::from_directory_path(path).unwrap();
    let sync = Arc::new(SyncEngine::new());
    let recorder = Arc::new(RecordingParquetHandler::new(sync.parquet_handler()));
    let engine = Arc::new(DelegatingEngine::new(sync).with_parquet_handler(recorder.clone()));
    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot.scan_builder().build().unwrap();

    fn collect_file_modification_times(
        file_modification_times: &mut Vec<(String, i64)>,
        scan_file: ScanFile,
    ) {
        file_modification_times.push((scan_file.path.to_string(), scan_file.modification_time));
    }

    let mut expected = Vec::new();
    for scan_metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        expected = scan_metadata
            .unwrap()
            .visit_scan_files(expected, collect_file_modification_times)
            .unwrap();
    }
    recorder.take_reads();

    let _: Vec<_> = scan.execute(engine.clone()).unwrap().try_collect().unwrap();
    let reads = recorder.take_reads();
    let mut actual: Vec<_> = reads
        .into_iter()
        .flat_map(|read| read.files)
        .filter_map(|file| {
            file.location
                .to_string()
                .strip_prefix(url.as_str())
                .map(|path| (path.to_string(), file.last_modified))
        })
        .collect();

    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
}

#[rstest]
#[case::all_struct(StatsOptions::all_struct(), false, false)]
#[case::all(StatsOptions::all(), true, false)]
#[case::none_with_predicate(StatsOptions::none(), false, true)]
fn test_checkpoint_stats_projection_matches_requested_output(
    #[values(
        "v1-single-part-struct-stats-only",
        "v2-parquet-sidecars-struct-stats-only",
        "v2-checkpoints-parquet-with-sidecars"
    )]
    table: &str,
    #[case] stats: StatsOptions,
    #[case] request_json_stats: bool,
    #[case] skip_stats: bool,
) {
    let extracted = load_test_data("tests/data", table).ok();
    let path = extracted
        .as_ref()
        .map(|dir| dir.path().join(table))
        .unwrap_or_else(|| {
            fs::canonicalize(PathBuf::from(format!("./tests/data/{table}/"))).unwrap()
        });
    let url = Url::from_directory_path(path).unwrap();
    let sync = Arc::new(SyncEngine::new());
    let recorder = Arc::new(RecordingParquetHandler::new(sync.parquet_handler()));
    let engine = DelegatingEngine::new(sync).with_parquet_handler(recorder.clone());
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    recorder.take_reads();

    let predicate: Option<PredicateRef> =
        skip_stats.then(|| Arc::new(Pred::gt(col!("id"), lit(0i64))) as PredicateRef);
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(stats)
        .build()
        .unwrap();
    for action in scan.replay_for_scan_metadata(&engine).unwrap().actions {
        action.unwrap();
    }

    let reads = recorder.take_reads();
    let compatible_structured_stats = table != "v2-checkpoints-parquet-with-sidecars";
    let expect_parsed_stats = !skip_stats && compatible_structured_stats;
    let expect_json_stats = !skip_stats && (request_json_stats || !expect_parsed_stats);
    let expected_file_fragment = if table.starts_with("v2-") {
        "_sidecars/"
    } else {
        ".checkpoint."
    };
    let action_reads: Vec<_> = reads
        .iter()
        .filter(|read| {
            read.files
                .iter()
                .any(|file| file.location.as_str().contains(expected_file_fragment))
                && read.physical_schema.field("add").is_some()
        })
        .collect();
    assert!(!action_reads.is_empty(), "expected checkpoint Add reads");
    for read in action_reads {
        let add_field = read
            .physical_schema
            .field("add")
            .expect("checkpoint read schema must contain add");
        let DataType::Struct(add) = add_field.data_type() else {
            panic!("checkpoint add field must be a struct");
        };
        assert_eq!(
            add.field("stats").is_some(),
            expect_json_stats,
            "JSON checkpoint stats projection must match the requested output"
        );
        assert_eq!(
            add.field("stats_parsed").is_some(),
            expect_parsed_stats,
            "structured checkpoint stats projection must match the requested output"
        );
    }
}

#[test]
fn test_all_struct_parses_json_commit_stats() {
    let path = fs::canonicalize(PathBuf::from(
        "./tests/data/v1-single-part-struct-stats-only/",
    ))
    .unwrap();
    let url = Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    // The table's checkpoint is at version 5, so version 4 replays only JSON commits.
    let snapshot = Snapshot::builder_for(url)
        .at_version(4)
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all_struct())
        .build()
        .unwrap();

    let mut file_count = 0;
    for scan_metadata in scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    {
        let (underlying_data, selection_vector) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();
        let filtered = filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();
        let stats_parsed = get_column!(filtered, "stats_parsed", StructArray);
        let num_records = get_column!(stats_parsed, NUM_RECORDS, Int64Array);

        for row in 0..filtered.num_rows() {
            assert!(!stats_parsed.is_null(row));
            assert_eq!(num_records.value(row), 1);
            file_count += 1;
        }
    }
    assert_eq!(file_count, 4);
}

#[rstest]
#[case::v1_partition(
    "v1-multi-part-partitioned-struct-stats-only",
    Pred::eq(col!("part"), lit(1i32)),
    column_name!("add.partitionValues_parsed.part"),
    ".checkpoint.",
)]
#[case::v2_sidecar(
    "v2-parquet-sidecars-struct-stats-only",
    Pred::gt(col!("id"), lit(2i64)),
    column_name!("add.stats_parsed.maxValues.id"),
    "_sidecars/",
)]
fn test_checkpoint_predicate_reaches_parquet_handler(
    #[case] table: &str,
    #[case] pred: Pred,
    #[case] expected_ref: ColumnName,
    #[case] expected_file_fragment: &str,
) {
    let path = std::fs::canonicalize(PathBuf::from(format!("./tests/data/{table}/"))).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let sync = Arc::new(SyncEngine::new());
    let recorder = Arc::new(RecordingParquetHandler::new(sync.parquet_handler()));
    let engine = DelegatingEngine::new(sync).with_parquet_handler(recorder.clone());
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    recorder.take_reads();

    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(pred))
        .build()
        .unwrap();
    for action in scan.replay_for_scan_metadata(&engine).unwrap().actions {
        action.unwrap();
    }

    let reads = recorder.take_reads();
    assert!(
        reads.iter().any(|read| {
            read.files
                .iter()
                .any(|file| file.location.as_str().contains(expected_file_fragment))
                && read
                    .predicate
                    .as_ref()
                    .is_some_and(|predicate| predicate.references().contains(&expected_ref))
        }),
        "expected {expected_ref} on a {expected_file_fragment} read, got {reads:#?}"
    );
}

#[test]
fn test_skip_stats_disables_data_skipping() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    let predicate = Arc::new(Pred::gt(col!("id"), lit(400i64)));
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::none())
        .build()
        .unwrap();

    let scan_metadata_results: Vec<_> = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let mut selected_file_count = 0;
    for scan_metadata in &scan_metadata_results {
        let selection_vector = scan_metadata.scan_files.selection_vector();
        selected_file_count += selection_vector
            .iter()
            .filter(|&&selected| selected)
            .count();
    }

    assert_eq!(selected_file_count, 6);
}

/// Calling `with_stats` twice replaces the prior value; the last call wins.
#[test]
fn test_with_stats_last_call_wins() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    // First call is `all()` (would emit stats_parsed); last call is `none()` (no output).
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_stats(StatsOptions::none())
        .build()
        .unwrap();

    for scan_metadata in scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    {
        let (underlying_data, _) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();
        assert!(
            batch.column_by_name("stats_parsed").is_none(),
            "last call (`none`) should win: no stats_parsed in output"
        );
    }
}

#[test]
fn test_default_stats_options_no_struct_output() {
    // StatsOptions::default() produces no stats_parsed output.
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::default())
        .build()
        .unwrap();

    let scan_metadata_results: Vec<_> = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(
        !scan_metadata_results.is_empty(),
        "Should have scan metadata"
    );

    for scan_metadata in scan_metadata_results {
        let (underlying_data, _selection_vector) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();

        // stats_parsed should not be present since empty Columns means no stats output
        assert!(
            batch.column_by_name("stats_parsed").is_none(),
            "stats_parsed should not be present with empty stats columns"
        );
    }
}

#[rstest]
#[case::id_without_predicate(
    StatsOptions::struct_columns(vec![column_name!("id")]),
    &["id"],
    None,
    "id",
    &[
        ("1", "100"),
        ("101", "200"),
        ("201", "300"),
        ("301", "400"),
        ("401", "500"),
        ("501", "600"),
    ],
)]
#[case::id_with_json_without_predicate(
    StatsOptions {
        synthesize_json: true,
        struct_stats: StructStats::Columns(vec![column_name!("id")]),
    },
    &["id"],
    None,
    "id",
    &[
        ("1", "100"),
        ("101", "200"),
        ("201", "300"),
        ("301", "400"),
        ("401", "500"),
        ("501", "600"),
    ],
)]
#[case::id_predicate_requested(
    StatsOptions::struct_columns(vec![column_name!("id")]),
    &["id"],
    Some(col!("id").gt(lit(400i64))),
    "id",
    &[("401", "500"), ("501", "600")],
)]
#[case::id_predicate_not_requested(
    StatsOptions::struct_columns(vec![column_name!("name")]),
    &["id", "name"],
    Some(col!("id").gt(lit(400i64))),
    "name",
    &[("name_401", "name_500"), ("name_501", "name_600")],
)]
#[case::salary_predicate_with_multiple_requested_columns(
    StatsOptions::struct_columns(vec![column_name!("id"), column_name!("name")]),
    &["id", "name", "salary"],
    Some(col!("salary").le(lit(70_000i64))),
    "id",
    &[("1", "100"), ("101", "200")],
)]
#[case::salary_requested_with_different_predicate_column(
    StatsOptions::struct_columns(vec![column_name!("salary")]),
    &["id", "salary"],
    Some(col!("id").gt(lit(500i64))),
    "salary",
    &[("100100", "110000")],
)]
fn scan_metadata_struct_columns_returns_expected_stats(
    #[case] stats: StatsOptions,
    #[case] expected_stat_fields: &[&str],
    #[case] predicate: Option<Pred>,
    #[case] probe_column: &str,
    #[case] expected_min_max: &[(&str, &str)],
) {
    let path = fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let predicate = predicate.map(|predicate| Arc::new(predicate) as PredicateRef);
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(stats)
        .build()
        .unwrap();

    let mut actual_min_max = Vec::new();
    let mut batch_count = 0;
    for scan_metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        batch_count += 1;
        let (underlying_data, selection_vector) = scan_metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();
        let stats_parsed = get_column!(batch, STATS_PARSED, StructArray);
        let min_values = get_column!(stats_parsed, MIN_VALUES, StructArray);
        let max_values = get_column!(stats_parsed, MAX_VALUES, StructArray);
        let null_count = get_column!(stats_parsed, NULL_COUNT, StructArray);
        assert_eq!(field_names(min_values), expected_stat_fields);
        assert_eq!(field_names(max_values), expected_stat_fields);
        assert_eq!(field_names(null_count), expected_stat_fields);

        let filtered = filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();
        let stats_parsed = get_column!(filtered, STATS_PARSED, StructArray);
        let num_records = get_column!(stats_parsed, NUM_RECORDS, Int64Array);
        let min_values = get_column!(stats_parsed, MIN_VALUES, StructArray);
        let max_values = get_column!(stats_parsed, MAX_VALUES, StructArray);
        let probe_min = min_values.column_by_name(probe_column).unwrap();
        let probe_max = max_values.column_by_name(probe_column).unwrap();
        for row in 0..filtered.num_rows() {
            assert!(!stats_parsed.is_null(row));
            assert_eq!(num_records.value(row), 100);
            actual_min_max.push((
                array_value_to_string(probe_min.as_ref(), row).unwrap(),
                array_value_to_string(probe_max.as_ref(), row).unwrap(),
            ));
        }
    }

    assert!(batch_count > 0);
    actual_min_max.sort_unstable();
    let mut expected_min_max: Vec<_> = expected_min_max
        .iter()
        .map(|(min, max)| (min.to_string(), max.to_string()))
        .collect();
    expected_min_max.sort_unstable();
    assert_eq!(actual_min_max, expected_min_max);
}

#[test]
fn scan_metadata_struct_columns_fully_pruning_predicate_yields_no_batches() {
    let path = fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(col!("id").gt(lit(600i64))))
        .with_stats(StatsOptions::struct_columns(vec![column_name!("salary")]))
        .build()
        .unwrap();

    assert_eq!(scan.scan_metadata(engine.as_ref()).unwrap().count(), 0);
}

#[rstest]
fn scan_builder_validates_predicate_and_stats_columns(
    #[values(
        (column_pred!("missing_predicate"), false),
        (column_pred!("id"), true)
    )]
    predicate: (Pred, bool),
    #[values(
        (StatsOptions::struct_columns(vec![column_name!("missing_stats")]), false),
        (StatsOptions::struct_columns(vec![column_name!("id")]), true)
    )]
    stats: (StatsOptions, bool),
) {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let (predicate, predicate_exists) = predicate;
    let (stats, stats_columns_exist) = stats;
    let result = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .with_stats(stats)
        .build();

    match (predicate_exists, stats_columns_exist) {
        (true, true) => {
            result.expect("valid predicate and stats columns");
        }
        (false, _) => assert_result_error_with_message(result, "missing_predicate"),
        (true, false) => assert_result_error_with_message(result, "missing_stats"),
    }
}

/// Test that [`StructStats::Columns`] rejects nonexistent columns.
#[test]
fn test_scan_metadata_with_nonexistent_stats_columns() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();

    let result = snapshot
        .scan_builder()
        .with_stats(StatsOptions {
            synthesize_json: true,
            struct_stats: StructStats::Columns(vec![column_name!("nonexistent_column")]),
        })
        .build();

    assert_result_error_with_message(result, "Could not resolve column 'nonexistent_column'");
}

/// A [`ParquetHandler`] that returns an empty iterator for every `read_parquet_files` call.
/// Used to simulate a buggy connector that drops all data for a file.
struct EmptyParquetHandler;

impl ParquetHandler for EmptyParquetHandler {
    fn read_parquet_files(
        &self,
        _files: &[FileMeta],
        _schema: schema::SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        Ok(Box::new(std::iter::empty()))
    }

    fn read_parquet_footer(&self, _file: &FileMeta) -> DeltaResult<ParquetFooter> {
        unimplemented!()
    }

    fn write_parquet_file(
        &self,
        _location: url::Url,
        _data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        unimplemented!()
    }
}

/// When a file's Add action stats report `numRecords > 0` and the parquet handler returns an empty
/// iterator, `execute` must surface an error rather than silently producing no rows.
#[test]
fn execute_errors_when_parquet_returns_empty_for_file_with_positive_stats() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(
        DelegatingEngine::new(Arc::new(SyncEngine::new()))
            .with_parquet_handler(Arc::new(EmptyParquetHandler)),
    );

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();

    let results: Vec<_> = scan.execute(engine).unwrap().collect();
    assert_eq!(results.len(), 1, "should emit exactly one error item");
    assert!(results[0].is_err(), "the result should be an error, got Ok");
    let err = results[0].as_ref().err().unwrap().to_string();
    assert!(
        err.contains("ParquetHandler returned no data"),
        "unexpected error message: {err}"
    );
}

/// When a file's Add action has no stats, an empty iterator from the parquet handler is allowed
/// -- we conservatively treat the file as possibly legitimately empty.
#[test]
fn execute_does_not_error_when_parquet_returns_empty_and_stats_absent() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-cdf/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(
        DelegatingEngine::new(Arc::new(SyncEngine::new()))
            .with_parquet_handler(Arc::new(EmptyParquetHandler)),
    );

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();

    // All Add files in this table have no stats -- empty iterators should be silently ignored.
    let results: Vec<_> = scan.execute(engine).unwrap().collect();
    assert!(
        results.iter().all(|r| r.is_ok()),
        "expected no errors for stats-absent files"
    );
}

/// Tests for `ScanMetadataCompleted` event emission via the tracing-based metrics system.
mod scan_metadata_completed_tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use rstest::rstest;

    use super::ScanBuilder;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{col, lit, Expression as Expr, Predicate as Pred};
    use crate::metrics::MetricEvent;
    use crate::unit_test_utils::{install_thread_local_metrics_reporter, CapturingReporter};
    use crate::utils::FoldWithOption as _;
    use crate::Snapshot;

    fn run_scan(
        table: &str,
        predicate: Option<Arc<Pred>>,
        correlation_id: Option<&str>,
    ) -> (
        Arc<CapturingReporter>,
        tracing::subscriber::DefaultGuard,
        usize,
    ) {
        let path = std::fs::canonicalize(PathBuf::from(table)).unwrap();
        let url = url::Url::from_directory_path(&path).unwrap();
        let reporter = Arc::new(CapturingReporter::default());
        let engine = Arc::new(SyncEngine::new());
        let guard = install_thread_local_metrics_reporter(reporter.clone());
        let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
        let scan = snapshot
            .scan_builder()
            .fold_with(predicate, ScanBuilder::with_predicate)
            .fold_with(correlation_id, ScanBuilder::with_correlation_id)
            .build()
            .unwrap();
        let results: Vec<_> = scan
            .scan_metadata(engine.as_ref())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        (reporter, guard, results.len())
    }

    fn get_scan_event(reporter: &CapturingReporter) -> MetricEvent {
        reporter
            .events()
            .into_iter()
            .find(|e| matches!(e, MetricEvent::ScanMetadataCompleted(_)))
            .expect("expected ScanMetadataCompleted event")
    }

    #[rstest]
    #[case::basic_scan("./tests/data/parsed-stats/", None, (6, 2, 6, 17236, 0, 0))]
    #[case::static_skip_all(
        "./tests/data/parsed-stats/",
        Some(Arc::new(Pred::FALSE)),
        (0, 0, 0, 0, 0, 0)
    )]
    #[case::with_removes("./tests/data/table-with-cdf/", None, (1, 1, 0, 0, 2, 0))]
    #[case::with_checkpoint(
        "./tests/data/with_checkpoint_no_last_checkpoint/",
        None,
        (2, 1, 1, 1010, 1, 0)
    )]
    #[case::partition_filter(
        "./tests/data/basic_partitioned/",
        Some(Arc::new(Expr::eq(col!("letter"), lit("a")))),
        (6, 6, 2, 1502, 0, 4)
    )]
    fn test_scan_metrics(
        #[case] table: &str,
        #[case] predicate: Option<Arc<Pred>>,
        #[case] expected: (u64, u64, u64, u64, u64, u64),
    ) {
        let (
            expected_add_seen,
            expected_add_seen_from_delta,
            expected_active,
            expected_active_bytes,
            expected_removes,
            expected_filtered,
        ) = expected;
        let (reporter, _guard, _) = run_scan(table, predicate, None);
        let MetricEvent::ScanMetadataCompleted(e) = get_scan_event(&reporter) else {
            panic!("expected ScanMetadataCompleted");
        };
        assert!(e.duration > Duration::ZERO);
        assert_eq!(e.num_add_files_seen, expected_add_seen);
        assert_eq!(
            e.num_add_files_seen_from_delta_files,
            expected_add_seen_from_delta
        );
        assert_eq!(e.num_selected_add_files, expected_active);
        assert_eq!(e.selected_add_files_bytes, expected_active_bytes);
        assert_eq!(e.num_remove_files_seen_from_delta_files, expected_removes);
        assert_eq!(e.num_predicate_filtered, expected_filtered);
        let rendered = e.to_string();
        assert!(rendered.contains(&format!(
            "add_files_seen_from_delta_files={expected_add_seen_from_delta}"
        )));
        assert!(rendered.contains(&format!(
            "remove_files_seen_from_delta_files={expected_removes}"
        )));
    }

    // The parallel-scan paths (both sequential and parallel phase events) are covered by
    // `parallel_scan_metadata_phases_carry_correlation_id` in `parallel::parallel_phase`.
    #[rstest]
    #[case::with_id(Some("scan-req-1"))]
    #[case::without_id(None)]
    fn scan_metadata_completed_carries_correlation_id(#[case] correlation_id: Option<&str>) {
        let (reporter, _guard, _) = run_scan("./tests/data/parsed-stats/", None, correlation_id);
        let MetricEvent::ScanMetadataCompleted(e) = get_scan_event(&reporter) else {
            panic!("expected ScanMetadataCompleted");
        };
        assert_eq!(e.correlation_id.as_deref(), correlation_id);
    }

    #[test]
    fn scan_metadata_completed_not_emitted_on_early_drop() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
        let url = url::Url::from_directory_path(&path).unwrap();
        let reporter = Arc::new(CapturingReporter::default());
        let engine = Arc::new(SyncEngine::new());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
        let scan = snapshot.scan_builder().build().unwrap();
        {
            let mut iter = scan.scan_metadata(engine.as_ref()).unwrap();
            let _ = iter.next();
            // Drop without exhausting -- callback must not fire
        }
        assert!(reporter
            .events()
            .iter()
            .all(|e| !matches!(e, MetricEvent::ScanMetadataCompleted(_))));
    }
}
