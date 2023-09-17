//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::path::PathBuf;
use std::sync::Arc;

use deltakernel::client::DefaultTableClient;
use deltakernel::executor::tokio::TokioBackgroundExecutor;
use deltakernel::Table;

#[test]
fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let table_client = Arc::new(DefaultTableClient::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?);

    let table = Table::new(url, table_client);
    let snapshot = table.snapshot(None)?;
    let scan = snapshot.scan()?.build();

    let stream = scan.execute()?;
    for batch in stream {
        let rows = batch.num_rows();
        arrow::util::pretty::print_batches(&[batch])?;
        assert_eq!(rows, 8);
    }
    Ok(())
}

#[test]
fn non_dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let table_client = Arc::new(DefaultTableClient::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?);

    let table = Table::new(url, table_client);
    let snapshot = table.snapshot(None)?;
    let scan = snapshot.scan()?.build();

    let stream = scan.execute()?;
    for batch in stream {
        let rows = batch.num_rows();
        arrow::util::pretty::print_batches(&[batch]).unwrap();
        assert_eq!(rows, 10);
    }
    Ok(())
}
