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
    for res in stream {
        for batch in res {
            let batch = batch?;
            let rows = extractor.length(&*batch);
            //     arrow::util::pretty::print_batches(&[batch]).unwrap();
            assert_eq!(rows, 8);
        }
    }
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
    for res in stream {
        for batch in res {
            let batch = batch?;
            let rows = extractor.length(&*batch);
            //     arrow::util::pretty::print_batches(&[batch]).unwrap();
            assert_eq!(rows, 10);
        }
    }
    Ok(())
}
