use std::path::{Path, PathBuf};
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, AsArray, StructArray};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::compute::{concat_batches, take};
use delta_kernel::arrow::compute::{lexsort_to_indices, SortColumn};
use delta_kernel::arrow::datatypes::{DataType, FieldRef, Schema};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::object_store::{local::LocalFileSystem, ObjectStore};
use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};

use delta_kernel::engine::arrow_conversion::TryFromKernel as _;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Snapshot};

use futures::{stream::TryStreamExt, StreamExt};
use itertools::Itertools;
use url::Url;

use test_utils::to_arrow;

#[macro_export]
macro_rules! sort_lines {
    ($lines: expr) => {{
        // sort except for header + footer
        let num_lines = $lines.len();
        if num_lines > 3 {
            $lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }
    }};
}

// NB: expected_lines_sorted MUST be pre-sorted (via sort_lines!())
#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($expected_lines_sorted: expr, $CHUNKS: expr) => {
        let formatted = delta_kernel::arrow::util::pretty::pretty_format_batches($CHUNKS)
            .unwrap()
            .to_string();
        // fix for windows: \r\n -->
        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();
        sort_lines!(actual_lines);
        assert_eq!(
            $expected_lines_sorted, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            $expected_lines_sorted, actual_lines
        );
    };
}

/// unpack the test data from {test_parent_dir}/{test_name}.tar.zst into a temp dir, and return the dir it was
/// unpacked into
#[allow(unused)]
pub(crate) fn load_test_data(
    test_parent_dir: &str,
    test_name: &str,
) -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    let path = format!("{test_parent_dir}/{test_name}.tar.zst");
    let tar = zstd::Decoder::new(std::fs::File::open(path)?)?;
    let mut archive = tar::Archive::new(tar);
    let temp_dir = tempfile::tempdir()?;
    archive.unpack(temp_dir.path())?;
    Ok(temp_dir)
}

// do a full table scan at the latest snapshot of the table and compare with the expected data
#[allow(unused)]
pub(crate) async fn latest_snapshot_test(
    engine: DefaultEngine<TokioBackgroundExecutor>,
    url: Url,
    expected_path: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = Snapshot::try_new(url, &engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;
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

// Ensure that two sets of fields have the same names, and dict_is_ordered
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
