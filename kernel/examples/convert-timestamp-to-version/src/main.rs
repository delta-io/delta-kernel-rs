use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Table};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,
    /// The start version of the table changes
    #[arg(short, long, default_value_t = 0)]
    start_timestamp: i64,
    #[arg(short, long)]
    end_timestamp: Option<i64>,
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
    let (start, end) = table.timestamp_range_to_versions(
        engine.as_ref(),
        cli.start_timestamp,
        cli.end_timestamp,
    )?;
    println!(
        "For start, end timestmaps ({},{:?}): the version is: ({:?}, {:?})",
        cli.start_timestamp, cli.end_timestamp, start, end
    );
    Ok(())
}
