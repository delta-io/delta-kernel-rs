//! Golden table tests adapted from delta-io/delta tests.
//!
//! Data (golden tables) are stored in tests/golden_data/<table_name>.tar.zst
//! Each table directory has a table/ and expected/ subdirectory with the input/output respectively

use arrow::{compute::filter_record_batch, record_batch::RecordBatch};
use arrow_select::concat::concat_batches;
use paste::paste;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::{engine::arrow_data::ArrowEngineData, DeltaResult, Table};
use futures::{stream::TryStreamExt, StreamExt};
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use arrow_array::Array;
use arrow_schema::DataType;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;

#[macro_use]
mod common;
use common::to_arrow;

/// unpack the test data from test_table.tar.zst into a temp dir, and return the dir it was
/// unpacked into
fn load_test_data(test_name: &str) -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    let path = format!("tests/golden_data/{}.tar.zst", test_name);
    let tar = zstd::Decoder::new(std::fs::File::open(path)?)?;
    let mut archive = tar::Archive::new(tar);
    let temp_dir = tempfile::tempdir()?;
    archive.unpack(temp_dir.path())?;
    Ok(temp_dir)
}

// NB adapated from DAT: read all parquet files in the directory and concatenate them
async fn read_expected(path: &Path) -> DeltaResult<RecordBatch> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(path)?);
    let files = store.list(None).try_collect::<Vec<_>>().await?;
    let mut batches = vec![];
    let mut schema = None;
    for meta in files.into_iter() {
        if let Some(ext) = meta.location.extension() {
            if ext == "parquet" {
                let reader = ParquetObjectReader::new(store.clone(), meta);
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

// TODO: change to use arrow's column Eq like dat does, instead of string comparison. Should print
// out string when test fails to make debugging easier
macro_rules! assert_batches_eq {
    ($expected: expr, $chunks: expr) => {
        let formatted_expected = arrow::util::pretty::pretty_format_batches(&[$expected])
            .unwrap()
            .to_string();
        let formatted_result = arrow::util::pretty::pretty_format_batches(&[$chunks])
            .unwrap()
            .to_string();
        let mut sorted_expected: Vec<&str> = formatted_expected.trim().lines().collect();
        let mut sorted_result: Vec<&str> = formatted_result.trim().lines().collect();
        sort_lines!(sorted_expected);
        sort_lines!(sorted_result);
        assert_eq!(
            sorted_expected, sorted_result,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            sorted_expected, sorted_result,
        );
    };
}

// copypasta from DAT
// some things are equivalent, but don't show up as equivalent for `==`, so we normalize here
fn normalize_col(col: &Arc<dyn Array>) -> Arc<dyn Array> {
    if let DataType::Timestamp(unit, Some(zone)) = col.data_type() {
        if **zone == *"+00:00" {
            arrow_cast::cast::cast(&col, &DataType::Timestamp(*unit, Some("UTC".into())))
                .expect("Could not cast to UTC")
        } else {
            col.clone()
        }
    } else {
        col.clone()
    }
}

// do a full table scan at the latest snapshot of the table and compare with the expected data
async fn latest_snapshot_test(
    engine: DefaultEngine<TokioBackgroundExecutor>,
    table: Table,
    expected_path: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = table.snapshot(&engine, None).unwrap();

    let scan = snapshot.into_scan_builder().build().unwrap();
    let scan_res = scan.execute(&engine).unwrap();
    let batches: Vec<RecordBatch> = scan_res
        .into_iter()
        .map(|sr| {
            let data = sr.raw_data.unwrap();
            let record_batch = to_arrow(data).unwrap();
            if let Some(mut mask) = sr.mask {
                let extra_rows = record_batch.num_rows() - mask.len();
                if extra_rows > 0 {
                    // we need to extend the mask here in case it's too short
                    mask.extend(std::iter::repeat(true).take(extra_rows));
                }
                filter_record_batch(&record_batch, &mask.into()).unwrap()
            } else {
                record_batch
            }
        })
        .collect();

    let expected = read_expected(&expected_path.expect("expect an expected dir"))
        .await
        .unwrap();
    let schema: ArrowSchemaRef = Arc::new(scan.schema().as_ref().try_into().unwrap());

    // convert the batch +00:00 to UTC
    let result: Vec<RecordBatch> = batches
        .iter()
        .map(|batch| {
            let result = batch.columns().iter().map(normalize_col).collect();
            RecordBatch::try_new(schema.clone(), result).unwrap()
        })
        .collect();

    let result = concat_batches(&schema, &result).unwrap();
    assert_batches_eq!(expected, result);
    Ok(())
}

fn setup_golden_table(
    test_name: &str,
) -> (
    DefaultEngine<TokioBackgroundExecutor>,
    Table,
    Option<PathBuf>,
    tempfile::TempDir,
) {
    let test_dir = load_test_data(test_name).unwrap();
    let test_path = test_dir.path().join(test_name);
    let table_path = test_path.join("delta");
    let table = Table::try_from_uri(table_path.to_str().expect("table path to string"))
        .expect("table from uri");
    let engine = DefaultEngine::try_new(
        table.location(),
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )
    .unwrap();
    let expected_path = test_path.join("expected");
    let expected_path = expected_path.exists().then_some(expected_path);
    (engine, table, expected_path, test_dir)
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
                let (engine, table, expected, test_dir) = setup_golden_table($test_name);
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
    table: Table,
    _expected: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    // assert latest version is 1 and there are no files in the snapshot (add is removed)
    let snapshot = table.snapshot(&engine, None).unwrap();
    assert_eq!(snapshot.version(), 1);
    let scan = snapshot
        .into_scan_builder()
        .build()
        .expect("build the scan");
    let mut scan_data = scan.scan_data(&engine).expect("scan data");
    assert!(scan_data.next().is_none());
    Ok(())
}

async fn checkpoint_test(
    engine: DefaultEngine<TokioBackgroundExecutor>,
    table: Table,
    _expected: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = table.snapshot(&engine, None).unwrap();
    let version = snapshot.version();
    let scan = snapshot
        .into_scan_builder()
        .build()
        .expect("build the scan");
    let scan_data: Vec<_> = scan.scan_data(&engine).expect("scan data").collect();
    assert_eq!(version, 14);
    assert!(scan_data.len() == 1);
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
skip_test!("corrupted-last-checkpoint-kernel": "BUG: should fallback to old commits/checkpoint");
skip_test!("data-reader-array-complex-objects": "list field expected name item but got name element");
skip_test!("data-reader-array-primitives": "list field expected name item but got name element");
golden_test!("data-reader-date-types-America", latest_snapshot_test);
golden_test!("data-reader-date-types-Asia", latest_snapshot_test);
golden_test!("data-reader-date-types-Etc", latest_snapshot_test);
golden_test!("data-reader-date-types-Iceland", latest_snapshot_test);
golden_test!("data-reader-date-types-Jst", latest_snapshot_test);
golden_test!("data-reader-date-types-Pst", latest_snapshot_test);
golden_test!("data-reader-date-types-utc", latest_snapshot_test);
golden_test!("data-reader-escaped-chars", latest_snapshot_test);
skip_test!("data-reader-map": "map field named 'entries' vs 'key_value'");
golden_test!("data-reader-nested-struct", latest_snapshot_test);
skip_test!("data-reader-nullable-field-invalid-schema-key":
  "list field expected name item but got name element");
skip_test!("data-reader-partition-values": "list field expected name item but got name element");
golden_test!("data-reader-primitives", latest_snapshot_test);
golden_test!("data-reader-timestamp_ntz", latest_snapshot_test);
skip_test!("data-reader-timestamp_ntz-id-mode": "id column mapping mode not supported");
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
skip_test!("hive": "test not yet implmented - different file structure");
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
skip_test!("parquet-all-types": "list field expected name item but got name element");
skip_test!("parquet-all-types-legacy-format": "list field expected name item but got name element");
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

// TODO use projections
skip_test!("table-with-columnmapping-mode-id": "id column mapping mode not supported");
skip_test!("table-with-columnmapping-mode-name":
  "BUG: Parquet(General('partial projection of MapArray is not supported'))");

// TODO scan at different versions
golden_test!("time-travel-partition-changes-a", latest_snapshot_test);
golden_test!("time-travel-partition-changes-b", latest_snapshot_test);
golden_test!("time-travel-schema-changes-a", latest_snapshot_test);
golden_test!("time-travel-schema-changes-b", latest_snapshot_test);
golden_test!("time-travel-start", latest_snapshot_test);
golden_test!("time-travel-start-start20", latest_snapshot_test);
golden_test!("time-travel-start-start20-start40", latest_snapshot_test);

golden_test!("v2-checkpoint-json", latest_snapshot_test); // passing without v2 checkpoint support
golden_test!("v2-checkpoint-parquet", latest_snapshot_test); // passing without v2 checkpoint support

// BUG:
// - AddFile: 'file:/some/unqualified/absolute/path'
// - RemoveFile: '/some/unqualified/absolute/path'
// --> should give no files for the table, but currently gives 1 file
// golden_test!("canonicalized-paths-normal-a", canonicalized_paths_test);
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
skip_test!("delete-re-add-same-file-different-transactions": "test not yet implmented");

// data file doesn't exist, get the relative path to compare
// assert(new File(addFileStatus.getPath).getName == "special p@#h")
skip_test!("log-replay-special-characters-b": "test not yet implmented");

negative_test!("deltalog-invalid-protocol-version");
negative_test!("deltalog-state-reconstruction-from-checkpoint-missing-metadata");
negative_test!("deltalog-state-reconstruction-from-checkpoint-missing-protocol");
negative_test!("deltalog-state-reconstruction-without-metadata");
negative_test!("deltalog-state-reconstruction-without-protocol");
negative_test!("no-delta-log-folder"); // expected DELTA_TABLE_NOT_FOUND
negative_test!("versions-not-contiguous"); // expected DELTA_VERSIONS_NOT_CONTIGUOUS
