//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::path::PathBuf;

use deltakernel::scan::ScanBuilder;
use deltakernel::simple_client::SimpleClient;
use deltakernel::{EngineClient, Table};

#[test]
fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine_client = SimpleClient::new();
    let extractor = engine_client.get_data_extactor();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine_client, None)?;
    let scan = ScanBuilder::new(snapshot).build();

    let stream = scan.execute(&engine_client)?;
    let mut total_rows = 0;
    for res in stream {
        let data = res.raw_data?;
        let rows = extractor.length(&*data);
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
    let engine_client = SimpleClient::new();
    let extractor = engine_client.get_data_extactor();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine_client, None)?;
    let scan = ScanBuilder::new(snapshot).build();

    let stream = scan.execute(&engine_client)?;
    let mut total_rows = 0;
    for res in stream {
        let data = res.raw_data?;
        let rows = extractor.length(&*data);
        for i in 0..rows {
            if res.mask.as_ref().map_or(true, |mask| mask[i]) {
                total_rows += 1;
            }
        }
    }
    assert_eq!(total_rows, 10);
    Ok(())
}
