//! Golden table tests adapted from delta-io/delta tests.
//!
//! Data (golden tables) are stored in tests/golden_data/<table_name>.tar.zst
//! Each table directory has a table/ and expected/ subdirectory with the input/output respectively

use arrow::{compute::filter_record_batch, record_batch::RecordBatch};
use arrow_array::ArrayRef;
use arrow_select::concat::concat_batches;
use paste::paste;
use std::path::Path;
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

// load the test data from <test_table>.tar.zst file
fn load_test_data(test_name: &str) -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    let path = format!("tests/golden_data/{}.tar.zst", test_name);
    let tar = zstd::Decoder::new(std::fs::File::open(path)?)?;
    let mut archive = tar::Archive::new(tar);
    let temp_dir = tempfile::tempdir()?;
    archive.unpack(temp_dir.path())?;
    Ok(temp_dir)
}

// NB adapated from DAT: read all parquet files in the directory and concatenate them
async fn read_expected(path: &Path) -> DeltaResult<Option<RecordBatch>> {
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
    Ok(Some(all_data))
}

// TODO: change to do something similar to dat tests instead of string comparison
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
            arrow_cast::cast::cast(&col, &DataType::Timestamp(unit.clone(), Some("UTC".into())))
                .expect("Could not cast to UTC")
        } else {
            col.clone()
        }
    } else {
        col.clone()
    }
}

// do a full table scan at the latest snapshot of the table and compare with the expected data
async fn latest_snapshot_test(test_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let test_dir = load_test_data(test_name).unwrap();
    let test_path = test_dir.path().join(test_name);
    let table_path = std::fs::canonicalize(test_path.join("table")).unwrap();
    let table_path = url::Url::from_directory_path(table_path).unwrap();
    let expected_path = std::fs::canonicalize(test_path.join("expected")).unwrap();

    let engine = DefaultEngine::try_new(
        &table_path,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )
    .unwrap();

    let table = Table::new(table_path);
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

    let expected = read_expected(&expected_path).await.unwrap().unwrap();
    let schema: ArrowSchemaRef = Arc::new(scan.schema().as_ref().try_into().unwrap());

    // convert the batch +00:00 to UTC
    let result: Vec<RecordBatch> = batches
        .iter()
        .map(|batch| {
            let result: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|column| normalize_col(column))
                .collect();
            RecordBatch::try_new(schema.clone(), result).unwrap()
        })
        .collect();

    let result = concat_batches(&schema, &result).unwrap();
    assert_batches_eq!(expected, result);
    Ok(())
}

macro_rules! full_scan_test {
    ($test_name:literal) => {
        paste! {
            #[tokio::test]
            async fn [<golden_ $test_name:snake>]() {
                latest_snapshot_test($test_name).await.unwrap();
            }
        }
    };
}

// same as golden_test but we expect the test to fail
// TODO: check actual error
macro_rules! negative_test {
    ($test_name:literal) => {
        paste! {
            #[tokio::test]
            #[should_panic]
            async fn [<golden_negative_ $test_name:snake>]() {
                latest_snapshot_test($test_name).await.unwrap();
            }
        }
    };
}

macro_rules! skip_test {
    ($test_name:literal: $reason:literal) => {
        paste! {
            #[ignore = $reason]
            #[tokio::test]
            async fn [<golden_skip_ $test_name:snake>]() {
                latest_snapshot_test($test_name).await.unwrap();
            }
        }
    };
}

macro_rules! golden_test {
    ($test_name:literal, $test_fn:expr) => {
        paste! {
            #[tokio::test]
            async fn [<golden_ $test_name:snake>]() -> Result<(), Box<dyn std::error::Error>> {
                let test_dir = load_test_data($test_name).unwrap();
                let test_path = test_dir.path().join($test_name);
                let table_path = std::fs::canonicalize(test_path.join("table")).unwrap();
                let table_path = url::Url::from_directory_path(table_path).unwrap();
                let engine =
                    DefaultEngine::try_new(
                        &table_path,
                        std::iter::empty::<(&str, &str)>(),
                        Arc::new(TokioBackgroundExecutor::new()),
                    ).unwrap();
                let table = Table::new(table_path);
                $test_fn(engine, table);
                Ok(())
            }
        }
    };
}

fn check_version_is_one(engine: DefaultEngine<TokioBackgroundExecutor>, table: Table) {
    // assert latest version is 1
    // TODO and there are no AddFiles
    let snapshot = table.snapshot(&engine, None).unwrap();
    assert_eq!(snapshot.version(), 1);
}

// All the test cases are below. Four test cases are currently supported:
// 1. full_scan! - run the test with the latest snapshot
// 2. negative_test! - run the test with the latest snapshot and expect it to fail
// 3. golden_test! - run the test with the latest snapshot and a custom test function
// 4. skip_test! - skip the test with a reason

full_scan_test!("124-decimal-decode-bug");
full_scan_test!("125-iterator-bug");
full_scan_test!("basic-decimal-table");
full_scan_test!("basic-decimal-table-legacy");
full_scan_test!("basic-with-inserts-deletes-checkpoint");
full_scan_test!("basic-with-inserts-merge");
full_scan_test!("basic-with-inserts-overwrite-restore");
full_scan_test!("basic-with-inserts-updates");
full_scan_test!("basic-with-vacuum-protocol-check-feature");
skip_test!("checkpoint": "test not yet implmented");
skip_test!("corrupted-last-checkpoint-kernel": "BUG: should fallback to old commits/checkpoint");
skip_test!("data-reader-array-complex-objects": "list field expected name item but got name element");
skip_test!("data-reader-array-primitives": "list field expected name item but got name element");
full_scan_test!("data-reader-date-types-America");
full_scan_test!("data-reader-date-types-Asia");
full_scan_test!("data-reader-date-types-Etc");
full_scan_test!("data-reader-date-types-Iceland");
full_scan_test!("data-reader-date-types-Jst");
full_scan_test!("data-reader-date-types-Pst");
full_scan_test!("data-reader-date-types-utc");
full_scan_test!("data-reader-escaped-chars");
skip_test!("data-reader-map": "map field named 'entries' vs 'key_value'");
full_scan_test!("data-reader-nested-struct");
skip_test!("data-reader-nullable-field-invalid-schema-key":
  "list field expected name item but got name element");
skip_test!("data-reader-partition-values": "list field expected name item but got name element");
full_scan_test!("data-reader-primitives");
full_scan_test!("data-reader-timestamp_ntz");
skip_test!("data-reader-timestamp_ntz-id-mode": "id column mapping mode not supported");
full_scan_test!("data-reader-timestamp_ntz-name-mode");

// TODO test with predicate
full_scan_test!("data-skipping-basic-stats-all-types");
full_scan_test!("data-skipping-basic-stats-all-types-checkpoint");
skip_test!("data-skipping-basic-stats-all-types-columnmapping-id": "id column mapping mode not supported");
full_scan_test!("data-skipping-basic-stats-all-types-columnmapping-name");
full_scan_test!("data-skipping-change-stats-collected-across-versions");
full_scan_test!("data-skipping-partition-and-data-column");

full_scan_test!("decimal-various-scale-precision");

// deltalog-getChanges test currently passing but needs to test the following:
// assert(snapshotImpl.getLatestTransactionVersion(engine, "fakeAppId") === Optional.of(3L))
// assert(!snapshotImpl.getLatestTransactionVersion(engine, "nonExistentAppId").isPresent)
full_scan_test!("deltalog-getChanges");

full_scan_test!("dv-partitioned-with-checkpoint");
full_scan_test!("dv-with-columnmapping");
skip_test!("hive": "test not yet implmented - different file structure");
full_scan_test!("kernel-timestamp-int96");
full_scan_test!("kernel-timestamp-pst");
full_scan_test!("kernel-timestamp-timestamp_micros");
full_scan_test!("kernel-timestamp-timestamp_millis");
full_scan_test!("log-replay-dv-key-cases");
full_scan_test!("log-replay-latest-metadata-protocol");
full_scan_test!("log-replay-special-characters");
full_scan_test!("log-replay-special-characters-a");
full_scan_test!("multi-part-checkpoint");
full_scan_test!("only-checkpoint-files");

// TODO some of the parquet tests use projections
skip_test!("parquet-all-types": "list field expected name item but got name element");
skip_test!("parquet-all-types-legacy-format": "list field expected name item but got name element");
full_scan_test!("parquet-decimal-dictionaries");
full_scan_test!("parquet-decimal-dictionaries-v1");
full_scan_test!("parquet-decimal-dictionaries-v2");
full_scan_test!("parquet-decimal-type");

full_scan_test!("snapshot-data0");
full_scan_test!("snapshot-data1");
full_scan_test!("snapshot-data2");
full_scan_test!("snapshot-data2-deleted");
full_scan_test!("snapshot-data3");
full_scan_test!("snapshot-repartitioned");
full_scan_test!("snapshot-vacuumed");

// TODO use projections
skip_test!("table-with-columnmapping-mode-id": "id column mapping mode not supported");
skip_test!("table-with-columnmapping-mode-name":
  "BUG: Parquet(General('partial projection of MapArray is not supported'))");

// TODO scan at different versions
full_scan_test!("time-travel-partition-changes-a");
full_scan_test!("time-travel-partition-changes-b");
full_scan_test!("time-travel-schema-changes-a");
full_scan_test!("time-travel-schema-changes-b");
full_scan_test!("time-travel-start");
full_scan_test!("time-travel-start-start20");
full_scan_test!("time-travel-start-start20-start40");

full_scan_test!("v2-checkpoint-json"); // currently passing without v2 checkpoint support
full_scan_test!("v2-checkpoint-parquet"); // currently passing without v2 checkpoint support

golden_test!("canonicalized-paths-normal-a", check_version_is_one);
golden_test!("canonicalized-paths-normal-b", check_version_is_one);
golden_test!("canonicalized-paths-special-a", check_version_is_one);
golden_test!("canonicalized-paths-special-b", check_version_is_one);

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
