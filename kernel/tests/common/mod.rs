use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use itertools::Itertools;

use crate::ArrowEngineData;
use delta_kernel::scan::Scan;
use delta_kernel::{DeltaResult, Engine, EngineData, Table};

use std::sync::Arc;

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

pub(crate) fn to_arrow(data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    Ok(data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into())
}

// TODO (zach): this is listed as unused for acceptance crate
#[allow(unused)]
pub(crate) fn test_read(
    expected: &ArrowEngineData,
    table: &Table,
    engine: Arc<dyn Engine>,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = table.snapshot(engine.as_ref(), None)?;
    let scan = snapshot.into_scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let formatted = pretty_format_batches(&batches).unwrap().to_string();

    let expected = pretty_format_batches(&[expected.record_batch().clone()])
        .unwrap()
        .to_string();

    println!("actual:\n{formatted}");
    println!("expected:\n{expected}");
    assert_eq!(formatted, expected);

    Ok(())
}

// TODO (zach): this is listed as unused for acceptance crate
#[allow(unused)]
pub(crate) fn read_scan(scan: &Scan, engine: Arc<dyn Engine>) -> DeltaResult<Vec<RecordBatch>> {
    let scan_results = scan.execute(engine)?;
    scan_results
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
        .try_collect()
}
