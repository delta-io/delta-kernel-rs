//! Golden table tests adapted from delta-io/delta tests.
//!
//! Data (golden tables) are stored in tests/golden_data/<table_name>.tar.zst
//! Each table directory has a table/ and expected/ subdirectory with the input/output respectively

use std::path::{Path, PathBuf};
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, AsArray, StructArray};
use delta_kernel::arrow::compute::{concat_batches, take};
use delta_kernel::arrow::compute::{lexsort_to_indices, SortColumn};
use delta_kernel::arrow::datatypes::{DataType, FieldRef, Schema};
use delta_kernel::arrow::{compute::filter_record_batch, record_batch::RecordBatch};
use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};

use delta_kernel::engine::arrow_conversion::TryFromKernel as _;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Snapshot};

use futures::{stream::TryStreamExt, StreamExt};
use itertools::Itertools;
use object_store::{local::LocalFileSystem, ObjectStore};
use paste::paste;
use url::Url;

mod common;

use test_utils::{load_test_data, to_arrow};

// NB adapted from DAT: read all parquet files in the directory and concatenate them
async fn read_expected(path: &Path) -> DeltaResult<RecordBatch> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(path)?);
    let files = store.list(None).try_collect::<Vec<_>>().await?;
    let mut batches = vec![];
    let mut schema = None;
    for meta in files.into_iter() {
        if let Some(ext) = meta.location.extension() {
            if ext == "parquet" {
                let reader = ParquetObjectReader::new(store.clone(), meta.location);
                let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
                if schema.is_none() {
                    schema = Some(builder.schema().clone());
                }
                let mut stream = builder.build()?;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
            }
        }
    }
    let all_data = concat_batches(&schema.unwrap(), &batches)?;
    Ok(all_data)
}

// copied from DAT
fn sort_record_batch(batch: RecordBatch) -> DeltaResult<RecordBatch> {
    if batch.num_rows() < 2 {
        // 0 or 1 rows doesn't need sorting
        return Ok(batch);
    }
    // Sort by as many columns as possible
    let mut sort_columns = vec![];
    for col in batch.columns() {
        match col.data_type() {
            DataType::Struct(_) | DataType::Map(_, _) => {
                // can't sort by structs or maps
            }
            DataType::List(list_field) => {
                let list_dt = list_field.data_type();
                if list_dt.is_primitive() {
                    // we can sort lists of primitives
                    sort_columns.push(SortColumn {
                        values: col.clone(),
                        options: None,
                    })
                }
            }
            _ => sort_columns.push(SortColumn {
                values: col.clone(),
                options: None,
            }),
        }
    }
    let indices = lexsort_to_indices(&sort_columns, None)?;
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

// Ensure that two sets of  fields have the same names, and dict_is_ordered
// We ignore:
//  - data type: This is checked already in `assert_columns_match`
//  - nullability: parquet marks many things as nullable that we don't in our schema
//  - metadata: because that diverges from the real data to the golden tabled data
fn assert_fields_match<'a>(
    actual: impl Iterator<Item = &'a FieldRef>,
    expected: impl Iterator<Item = &'a FieldRef>,
) {
    for (actual_field, expected_field) in actual.zip(expected) {
        assert!(
            actual_field.name() == expected_field.name(),
            "Field names don't match"
        );
        assert!(
            actual_field.dict_is_ordered() == expected_field.dict_is_ordered(),
            "Field dict_is_ordered doesn't match"
        );
    }
}

fn assert_cols_eq(actual: &dyn Array, expected: &dyn Array) {
    // Our testing only exercises these nested types so far. In the future we may need to expand
    // this to more types. Any `DataType` with a nested `Field` is a candidate for needing to be
    // compared this way.
    match actual.data_type() {
        DataType::Struct(_) => {
            let actual_sa = actual.as_struct();
            let expected_sa = expected.as_struct();
            assert_eq(actual_sa, expected_sa);
        }
        DataType::List(_) => {
            let actual_la = actual.as_list::<i32>();
            let expected_la = expected.as_list::<i32>();
            assert_cols_eq(actual_la.values(), expected_la.values());
        }
        DataType::Map(_, _) => {
            let actual_ma = actual.as_map();
            let expected_ma = expected.as_map();
            assert_cols_eq(actual_ma.keys(), expected_ma.keys());
            assert_cols_eq(actual_ma.values(), expected_ma.values());
        }
        _ => {
            assert_eq!(actual, expected, "Column data didn't match.");
        }
    }
}

fn assert_eq(actual: &StructArray, expected: &StructArray) {
    let actual_fields = actual.fields();
    let expected_fields = expected.fields();
    assert_eq!(
        actual_fields.len(),
        expected_fields.len(),
        "Number of fields differed"
    );
    assert_fields_match(actual_fields.iter(), expected_fields.iter());
    let actual_cols = actual.columns();
    let expected_cols = expected.columns();
    assert_eq!(
        actual_cols.len(),
        expected_cols.len(),
        "Number of columns differed"
    );
    for (actual_col, expected_col) in actual_cols.iter().zip(expected_cols) {
        assert_cols_eq(actual_col, expected_col);
    }
}

// do a full table scan at the latest snapshot of the table and compare with the expected data
async fn latest_snapshot_test(
    engine: DefaultEngine<TokioBackgroundExecutor>,
    url: Url,
    expected_path: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(url).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;
    let scan_res = scan.execute(Arc::new(engine))?;
    let batches: Vec<RecordBatch> = scan_res
        .map(|scan_result| -> DeltaResult<_> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch = to_arrow(data)?;
            if let Some(mask) = mask {
                Ok(filter_record_batch(&record_batch, &mask.into())?)
            } else {
                Ok(record_batch)
            }
        })
        .try_collect()?;

    let expected = read_expected(&expected_path.expect("expect an expected dir")).await?;

    let schema = Arc::new(Schema::try_from_kernel(scan.logical_schema().as_ref())?);
    let result = concat_batches(&schema, &batches)?;
    let result = sort_record_batch(result)?;
    let expected = sort_record_batch(expected)?;
    assert!(
        expected.num_rows() == result.num_rows(),
        "Didn't have same number of rows"
    );
    assert_eq(&result.into(), &expected.into());
    Ok(())
}

fn setup_golden_table(
    test_name: &str,
) -> (
    DefaultEngine<TokioBackgroundExecutor>,
    Url,
    Option<PathBuf>,
    tempfile::TempDir,
) {
    let test_dir = load_test_data("tests/golden_data", test_name).unwrap();
    let test_path = test_dir.path().join(test_name);
    let table_path = test_path.join("delta");
    let url = delta_kernel::try_parse_uri(table_path.to_str().expect("table path to string"))
        .expect("table from uri");
    let engine = DefaultEngine::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )
    .unwrap();
    let expected_path = test_path.join("expected");
    let expected_path = expected_path.exists().then_some(expected_path);
    (engine, url, expected_path, test_dir)
}

// same as golden_test but we expect the test to fail
// TODO: check actual error
macro_rules! negative_test {
    ($test_name:literal) => {
        paste! {
            #[tokio::test]
            #[should_panic]
            async fn [<golden_negative_ $test_name:snake>]() {
                let (engine, table, expected, _test_dir) = setup_golden_table($test_name);
                latest_snapshot_test(engine, table, expected).await.unwrap();
            }
        }
    };
}

macro_rules! skip_test {
    ($test_name:literal: $reason:literal) => {
        paste! {
            #[ignore = $reason]
            #[tokio::test]
            async fn [<golden_skip_ $test_name:snake>]() {}
        }
    };
}

macro_rules! golden_test {
    ($test_name:literal, $test_fn:expr) => {
        paste! {
            #[tokio::test]
            async fn [<golden_ $test_name:snake>]() -> Result<(), Box<dyn std::error::Error>> {
                // we don't use _test_dir but we don't want it to go out of scope before the test
                // is done since it will cleanup the directory when it runs drop
                let (engine, table, expected, _test_dir) = setup_golden_table($test_name);
                $test_fn(engine, table, expected).await?;
                Ok(())
            }
        }
    };
}

// TODO use in canonicalized paths tests
#[allow(dead_code)]
async fn canonicalized_paths_test(
    engine: DefaultEngine<TokioBackgroundExecutor>,
    table_root: Url,
    _expected: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    // assert latest version is 1 and there are no files in the snapshot (add is removed)
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    assert_eq!(snapshot.version(), 1);
    let scan = snapshot.scan_builder().build().expect("build the scan");
    let mut scan_metadata = scan.scan_metadata(&engine).expect("scan metadata");
    assert!(scan_metadata.next().is_none());
    Ok(())
}

async fn checkpoint_test(
    engine: DefaultEngine<TokioBackgroundExecutor>,
    table_root: Url,
    _expected: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
    let version = snapshot.version();
    let scan = snapshot.scan_builder().build().expect("build the scan");
    let scan_metadata: Vec<_> = scan
        .scan_metadata(&engine)
        .expect("scan metadata")
        .collect();
    assert_eq!(version, 14);
    assert!(scan_metadata.len() == 1);
    Ok(())
}

// All the test cases are below. Four test cases are currently supported:
// 1. golden_test! - run a test function against the golden table
// 2. negative_test! - run the test with the latest snapshot and expect it to fail
// 3. skip_test! - skip the test with a reason

golden_test!("124-decimal-decode-bug", latest_snapshot_test);
golden_test!("125-iterator-bug", latest_snapshot_test);
golden_test!("basic-decimal-table", latest_snapshot_test);
golden_test!("basic-decimal-table-legacy", latest_snapshot_test);
golden_test!(
    "basic-with-inserts-deletes-checkpoint",
    latest_snapshot_test
);
golden_test!("basic-with-inserts-merge", latest_snapshot_test);
golden_test!("basic-with-inserts-overwrite-restore", latest_snapshot_test);
golden_test!("basic-with-inserts-updates", latest_snapshot_test);
golden_test!(
    "basic-with-vacuum-protocol-check-feature",
    latest_snapshot_test
);
golden_test!("checkpoint", checkpoint_test);
golden_test!("corrupted-last-checkpoint-kernel", latest_snapshot_test);
golden_test!("data-reader-array-complex-objects", latest_snapshot_test);
golden_test!("data-reader-array-primitives", latest_snapshot_test);
golden_test!("data-reader-date-types-America", latest_snapshot_test);
golden_test!("data-reader-date-types-Asia", latest_snapshot_test);
golden_test!("data-reader-date-types-Etc", latest_snapshot_test);
golden_test!("data-reader-date-types-Iceland", latest_snapshot_test);
golden_test!("data-reader-date-types-Jst", latest_snapshot_test);
golden_test!("data-reader-date-types-Pst", latest_snapshot_test);
golden_test!("data-reader-date-types-utc", latest_snapshot_test);
golden_test!("data-reader-escaped-chars", latest_snapshot_test);
golden_test!("data-reader-map", latest_snapshot_test);
golden_test!("data-reader-nested-struct", latest_snapshot_test);
golden_test!(
    "data-reader-nullable-field-invalid-schema-key",
    latest_snapshot_test
);
skip_test!("data-reader-partition-values": "Golden data needs to have 2021-09-08T11:11:11+00:00 as expected value for as_timestamp col");
golden_test!("data-reader-primitives", latest_snapshot_test);
golden_test!("data-reader-timestamp_ntz", latest_snapshot_test);
golden_test!("data-reader-timestamp_ntz-id-mode", latest_snapshot_test);
golden_test!("data-reader-timestamp_ntz-name-mode", latest_snapshot_test);

// TODO test with predicate
golden_test!("data-skipping-basic-stats-all-types", latest_snapshot_test);
golden_test!(
    "data-skipping-basic-stats-all-types-checkpoint",
    latest_snapshot_test
);
skip_test!("data-skipping-basic-stats-all-types-columnmapping-id": "id column mapping mode not supported");
golden_test!(
    "data-skipping-basic-stats-all-types-columnmapping-name",
    latest_snapshot_test
);
golden_test!(
    "data-skipping-change-stats-collected-across-versions",
    latest_snapshot_test
);
golden_test!(
    "data-skipping-partition-and-data-column",
    latest_snapshot_test
);

golden_test!("decimal-various-scale-precision", latest_snapshot_test);

// deltalog-getChanges test currently passing but needs to test the following:
// assert(snapshotImpl.getLatestTransactionVersion(engine, "fakeAppId") === Optional.of(3L))
// assert(!snapshotImpl.getLatestTransactionVersion(engine, "nonExistentAppId").isPresent)
golden_test!("deltalog-getChanges", latest_snapshot_test);

golden_test!("dv-partitioned-with-checkpoint", latest_snapshot_test);
golden_test!("dv-with-columnmapping", latest_snapshot_test);
skip_test!("hive": "test not yet implemented - different file structure");
golden_test!("kernel-timestamp-int96", latest_snapshot_test);
golden_test!("kernel-timestamp-pst", latest_snapshot_test);
golden_test!("kernel-timestamp-timestamp_micros", latest_snapshot_test);
golden_test!("kernel-timestamp-timestamp_millis", latest_snapshot_test);
golden_test!("log-replay-dv-key-cases", latest_snapshot_test);
golden_test!("log-replay-latest-metadata-protocol", latest_snapshot_test);
golden_test!("log-replay-special-characters", latest_snapshot_test);
golden_test!("log-replay-special-characters-a", latest_snapshot_test);
golden_test!("multi-part-checkpoint", latest_snapshot_test);
golden_test!("only-checkpoint-files", latest_snapshot_test);

// TODO some of the parquet tests use projections
skip_test!("parquet-all-types": "schemas disagree about nullability, need to figure out which is correct and adjust");
skip_test!("parquet-all-types-legacy-format": "legacy parquet has name `array`, we should have adjusted this to `element`");
golden_test!("parquet-decimal-dictionaries", latest_snapshot_test);
golden_test!("parquet-decimal-dictionaries-v1", latest_snapshot_test);
golden_test!("parquet-decimal-dictionaries-v2", latest_snapshot_test);
golden_test!("parquet-decimal-type", latest_snapshot_test);

golden_test!("snapshot-data0", latest_snapshot_test);
golden_test!("snapshot-data1", latest_snapshot_test);
golden_test!("snapshot-data2", latest_snapshot_test);
golden_test!("snapshot-data2-deleted", latest_snapshot_test);
golden_test!("snapshot-data3", latest_snapshot_test);
golden_test!("snapshot-repartitioned", latest_snapshot_test);
golden_test!("snapshot-vacuumed", latest_snapshot_test);

golden_test!("table-with-columnmapping-mode-name", latest_snapshot_test);
golden_test!("table-with-columnmapping-mode-id", latest_snapshot_test);

// TODO scan at different versions
golden_test!("time-travel-partition-changes-a", latest_snapshot_test);
golden_test!("time-travel-partition-changes-b", latest_snapshot_test);
golden_test!("time-travel-schema-changes-a", latest_snapshot_test);
golden_test!("time-travel-schema-changes-b", latest_snapshot_test);
golden_test!("time-travel-start", latest_snapshot_test);
golden_test!("time-travel-start-start20", latest_snapshot_test);
golden_test!("time-travel-start-start20-start40", latest_snapshot_test);
golden_test!("v2-checkpoint-json", latest_snapshot_test);
golden_test!("v2-checkpoint-parquet", latest_snapshot_test);

// BUG:
// - AddFile: 'file:/some/unqualified/absolute/path'
// - RemoveFile: '/some/unqualified/absolute/path'
// --> should give no files for the table, but currently gives 1 file
skip_test!("canonicalized-paths-normal-a": "BUG: path canonicalization");
// BUG:
// - AddFile: 'file:///some/unqualified/absolute/path'
// - RemoveFile: '/some/unqualified/absolute/path'
// --> should give no files for the table, but currently gives 1 file
// golden_test!("canonicalized-paths-normal-b", canonicalized_paths_test);
skip_test!("canonicalized-paths-normal-b": "BUG: path canonicalization");

// BUG: same issue as above but with path = '/some/unqualified/with%20space/p@%23h'
// golden_test!("canonicalized-paths-special-a", canonicalized_paths_test);
// golden_test!("canonicalized-paths-special-b", canonicalized_paths_test);
skip_test!("canonicalized-paths-special-a": "BUG: path canonicalization");
skip_test!("canonicalized-paths-special-b": "BUG: path canonicalization");

// no table data, to implement:
// assert(foundFiles.length == 2)
// assert(foundFiles.map(_.getPath.split('/').last).toSet == Set("foo", "bar"))
// // We added two add files with the same path `foo`. The first should have been removed.
// // The second should remain, and should have a hard-coded modification time of 1700000000000L
// assert(foundFiles.find(_.getPath.endsWith("foo")).exists(_.getModificationTime == 1700000000000L))
skip_test!("delete-re-add-same-file-different-transactions": "test not yet implemented");

// data file doesn't exist, get the relative path to compare
// assert(new File(addFileStatus.getPath).getName == "special p@#h")
skip_test!("log-replay-special-characters-b": "test not yet implemented");

negative_test!("deltalog-invalid-protocol-version");
negative_test!("deltalog-state-reconstruction-from-checkpoint-missing-metadata");
negative_test!("deltalog-state-reconstruction-from-checkpoint-missing-protocol");
negative_test!("deltalog-state-reconstruction-without-metadata");
negative_test!("deltalog-state-reconstruction-without-protocol");
negative_test!("no-delta-log-folder"); // expected DELTA_TABLE_NOT_FOUND
negative_test!("versions-not-contiguous"); // expected DELTA_VERSIONS_NOT_CONTIGUOUS
