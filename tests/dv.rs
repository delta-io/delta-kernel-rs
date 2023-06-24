//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate

use deltakernel::delta_table::DeltaTable;
use futures::prelude::*;
use object_store::local::LocalFileSystem;

use std::sync::Arc;

#[tokio::test]
async fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = format!(
        "{}/tests/data/table-with-dv-small",
        env!["CARGO_MANIFEST_DIR"]
    );
    let storage = Arc::new(LocalFileSystem::new_with_prefix(&path)?);
    let table = DeltaTable::new("");
    let snapshot = table.get_latest_snapshot(storage.clone()).await.unwrap();
    let scan = snapshot.scan().build();
    let reader = scan.create_reader();
    let mut stream = reader
        .with_files(storage.clone(), scan.files(storage.clone()))
        .unwrap();

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let rows = batch.num_rows();
        arrow::util::pretty::print_batches(&[batch]).unwrap();
        assert_eq!(rows, 8);
    }
    Ok(())
}

#[tokio::test]
async fn non_dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = format!(
        "{}/tests/data/table-without-dv-small",
        env!["CARGO_MANIFEST_DIR"]
    );
    let storage = Arc::new(LocalFileSystem::new_with_prefix(&path)?);
    let table = DeltaTable::new("");
    let snapshot = table.get_latest_snapshot(storage.clone()).await.unwrap();
    let scan = snapshot.scan().build();

    let reader = scan.create_reader();
    let mut stream = reader
        .with_files(storage.clone(), scan.files(storage.clone()))
        .unwrap();

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let rows = batch.num_rows();
        arrow::util::pretty::print_batches(&[batch]).unwrap();
        assert_eq!(rows, 10);
    }
    Ok(())
}
