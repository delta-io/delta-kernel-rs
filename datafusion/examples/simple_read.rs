use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::util::pretty::print_batches;
use datafusion::execution::context::SessionContext;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::table::Table;
use delta_kernel_datafusion::DeltaTableProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "file:///Users/robert.pack/code/delta-kernel-rs/acceptance/tests/dat/out/reader_tests/generated/column_mapping/delta";

    // build a table and get the latest snapshot from it
    let table = Table::try_from_uri(path)?;

    let engine = Arc::new(DefaultEngine::try_new(
        table.location(),
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?);

    let snapshot = table.snapshot(engine.as_ref(), None)?;

    let provider = DeltaTableProvider::try_new(snapshot.into(), engine.clone())?;

    let ctx = SessionContext::new();
    ctx.register_table("delta_table", Arc::new(provider))?;

    let df = ctx.sql("SELECT * FROM delta_table").await?;
    let df = df.collect().await?;

    print_batches(&df)?;

    Ok(())
}
