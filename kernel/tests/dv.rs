//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::path::PathBuf;

use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::Table;

use test_log::test;

#[test]
fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;

    let stream = scan.execute(&engine)?;
    let mut total_rows = 0;
    for res in stream {
        let data = res.raw_data?;
        let rows = data.length();
        for i in 0..rows {
            if res.mask.as_ref().map_or(true, |mask| mask[i]) {
                total_rows += 1;
            }
        }
    }
    assert_eq!(total_rows, 8);
    Ok(())
}

#[test]
fn non_dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = SyncEngine::new();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None)?;
    let scan = snapshot.into_scan_builder().build()?;

    let stream = scan.execute(&engine)?;
    let mut total_rows = 0;
    for res in stream {
        let data = res.raw_data?;
        let rows = data.length();
        for i in 0..rows {
            if res.mask.as_ref().map_or(true, |mask| mask[i]) {
                total_rows += 1;
            }
        }
    }
    assert_eq!(total_rows, 10);
    Ok(())
}
