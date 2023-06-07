//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate

use deltakernel::{delta_table::DeltaTable, storage::StorageClient};
use std::path::PathBuf;

#[derive(Default, Debug, Clone)]
struct LocalStorageClient {}

impl StorageClient for LocalStorageClient {
    fn list(&self, prefix: &str) -> Vec<std::path::PathBuf> {
        let path = PathBuf::from(prefix);
        std::fs::read_dir(path)
            .unwrap()
            .map(|r| r.unwrap().path())
            .collect()
    }

    fn read(&self, path: &std::path::Path) -> Vec<u8> {
        std::fs::read(path).unwrap()
    }
}

#[test]
fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let storage_client = LocalStorageClient::default();
    let table = DeltaTable::new("tests/data/table-with-dv-small");
    let snapshot = table.get_latest_snapshot(&storage_client);
    let scan = snapshot.scan().build();
    let files_batches = scan.files(&storage_client);
    let reader = scan.create_reader();

    for batch in
        files_batches.flat_map(|files_batch| reader.read_batch(files_batch, &storage_client))
    {
        let batch = batch?;
        let rows = batch.num_rows();
        arrow::util::pretty::print_batches(&[batch]).unwrap();
        assert_eq!(rows, 8);
    }
    Ok(())
}

#[test]
fn non_dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let storage_client = LocalStorageClient::default();
    let table = DeltaTable::new("tests/data/table-without-dv-small");
    let snapshot = table.get_latest_snapshot(&storage_client);
    let scan = snapshot.scan().build();
    let files_batches = scan.files(&storage_client);
    let reader = scan.create_reader();

    for batch in
        files_batches.flat_map(|files_batch| reader.read_batch(files_batch, &storage_client))
    {
        let batch = batch?;
        let rows = batch.num_rows();
        assert_eq!(rows, 10);
        arrow::util::pretty::print_batches(&[batch]).unwrap();
    }
    Ok(())
}
