use std::sync::Arc;

use datafusion::arrow::util::pretty::print_batches;
use datafusion::execution::context::SessionContext;
use delta_kernel::table::Table;
use delta_kernel_datafusion::{DeltaTableProvider, KernelSession};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new().enable_kernel_engine()?;
    let engine = ctx.kernel_engine();

    let path = "file:///Users/robert.pack/code/delta-kernel-rs/acceptance/tests/dat/out/reader_tests/generated/column_mapping/delta";

    // build a table and get the latest snapshot from it
    let table = Table::try_from_uri(path)?;

    let snapshot = table.snapshot(engine.as_ref(), None)?;
    let provider = DeltaTableProvider::try_new(snapshot.into())?;

    ctx.register_table("delta_table", Arc::new(provider))?;

    let df = ctx.sql("SELECT * FROM delta_table").await?;
    let df = df.collect().await?;

    print_batches(&df)?;

    Ok(())
}
