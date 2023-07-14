//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::path::PathBuf;
use std::sync::Arc;

use deltakernel::client::DefaultTableClient;
use deltakernel::Table;

#[tokio::test]
async fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let table_client = Arc::new(DefaultTableClient::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
    )?);

    let table = Table::new(url, table_client);
    let snapshot = table.snapshot(None).await?;
    let scan = snapshot.scan().await?.build();

    let mut stream = scan.execute().await?.into_iter();
    while let Some(batch) = stream.next() {
        let rows = batch.num_rows();
        arrow::util::pretty::print_batches(&[batch])?;
        assert_eq!(rows, 8);
    }
    Ok(())
}

#[tokio::test]
async fn non_dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let table_client = Arc::new(DefaultTableClient::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
    )?);

    let table = Table::new(url, table_client);
    let snapshot = table.snapshot(None).await?;
    let scan = snapshot.scan().await?.build();

    let mut stream = scan.execute().await?.into_iter();
    while let Some(batch) = stream.next() {
        let rows = batch.num_rows();
        arrow::util::pretty::print_batches(&[batch]).unwrap();
        assert_eq!(rows, 10);
    }
    Ok(())
}
// file:///home/reap/code/delta-kernel-rs/tests/data/table-with-dv-small/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin
// file:///home/reap/code/delta-kernel-rs/tests/data/table-with-dv-small/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin
