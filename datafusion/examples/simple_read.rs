use std::sync::Arc;

use datafusion::arrow::util::pretty::print_batches;
use datafusion::execution::context::SessionContext;
use delta_kernel::table::Table;
use delta_kernel_datafusion::{DeltaLogTableProvider, KernelContextExt as _};

static PATH: &str = "file:///Users/robert.pack/code/delta-kernel-rs/acceptance/tests/dat/out/reader_tests/generated/";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new().enable_delta_kernel(None);

    let path = format!("{}column_mapping/delta", PATH);

    // build a table and get the latest snapshot from it
    let table = Table::try_from_uri(path)?;
    ctx.register_delta("delta_table", table.location()).await?;

    let df = ctx.sql("SELECT * FROM delta_table").await?;
    let df = df.collect().await?;
    print_batches(&df)?;

    let log_provider = DeltaLogTableProvider::try_new(table.into())?;
    ctx.register_table("delta_log", Arc::new(log_provider))?;

    let df = ctx
        .sql("SELECT add['path'] as path FROM delta_log WHERE add['path'] IS NOT NULL")
        .await?;
    let df = df.collect().await?;
    print_batches(&df)?;

    Ok(())
}
