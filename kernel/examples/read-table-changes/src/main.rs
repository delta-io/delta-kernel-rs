use std::{collections::HashMap, sync::Arc};

use arrow::util::pretty::print_batches;
use arrow_array::RecordBatch;
use clap::Parser;
use delta_kernel::engine::arrow_compute::materialize_scan_results;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Table};
use itertools::Itertools;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,
    /// The start version of the table changes
    #[arg(short, long, default_value_t = 0)]
    start_version: u64,
    /// The end version of the table changes
    #[arg(short, long)]
    end_version: Option<u64>,
}

fn main() -> DeltaResult<()> {
    let cli = Cli::parse();
    let table = Table::try_from_uri(cli.path)?;
    let options = HashMap::from([("skip_signature", "true".to_string())]);
    let engine = Arc::new(DefaultEngine::try_new(
        table.location(),
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    )?);
    let table_changes = table.table_changes(engine.as_ref(), cli.start_version, cli.end_version)?;

    let table_changes_scan = table_changes.into_scan_builder().build()?;
    let batches: Vec<RecordBatch> =
        materialize_scan_results(table_changes_scan.execute(engine.clone())?).try_collect()?;
    print_batches(&batches)?;
    Ok(())
}
