use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::{compute::filter_record_batch, util::pretty::print_batches};
use delta_kernel::engine::arrow_data::ArrowEngineData;
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
    if let Some(end) = cli.end_timestamp {
        let ver = table.timestamp_range_to_versions(engine.as_ref(), cli.start_timestamp, end)?;
        println!(
            "For start, end timestmaps ({},{}): the version is: {:?}",
            cli.start_timestamp, end, ver
        );
    } else {
        let ver = table.timestamp_to_version(engine.as_ref(), cli.start_timestamp)?;
        println!(
            "For timestmap {}, the version is: {:?}",
            cli.start_timestamp, ver
        );
    }
    Ok(())
}
