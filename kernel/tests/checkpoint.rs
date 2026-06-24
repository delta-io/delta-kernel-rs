use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::Table;

#[test]
fn read_delta_rs_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/delta-rs-table"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(DefaultEngine::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?);

    let table = Table::new(url.clone());
    let snapshot = table.snapshot(engine.as_ref(), None)?;
    let scan = snapshot.into_scan_builder().build()?;

    for batch in scan.execute(engine.clone())? {
        let arrow_data = ArrowEngineData::try_from_engine_data(batch?.raw_data?)?;
        let record_batch = arrow_data.record_batch();
        println!("{record_batch:?}");
    }

    let checkpoint = table.checkpoint(engine.as_ref(), 0)?;
    checkpoint
        .checkpoint_path()
        .map(|path| println!("Checkpoint path: {path}"))
        .unwrap_or_else(|_| panic!("No checkpoint path available"));

    for batch in checkpoint.checkpoint_data(engine.as_ref())? {
        let arrow_data = ArrowEngineData::try_from_engine_data(batch?.data)?;
        let record_batch = arrow_data.record_batch();
        println!("checkpoint data: {record_batch:?}");
    }

    // checkpoint.finalize(engine, metadata, checkpoint_data)

    Ok(())
}
