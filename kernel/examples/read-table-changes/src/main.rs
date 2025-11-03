use std::sync::Arc;

use clap::Parser;
use common::{LocationArgs, ParseWithExamples};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::util::pretty::print_batches;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::DeltaResult;
use itertools::Itertools;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(flatten)]
    location_args: LocationArgs,

    /// The start version of the table changes
    #[arg(short, long, default_value_t = 0)]
    start_version: u64,
    /// The end version of the table changes
    #[arg(short, long)]
    end_version: Option<u64>,
}

fn main() -> DeltaResult<()> {
    let cli = Cli::parse_with_examples(
        env!("CARGO_PKG_NAME"),
        "Read changes in",
        "read changes in",
        "",
    );
    let url = delta_kernel::try_parse_uri(cli.location_args.path.as_str())?;
    let engine = common::get_engine(&url, &cli.location_args)?;
    let table_changes = TableChanges::try_new(url, &engine, cli.start_version, cli.end_version)?;

    let table_changes_scan = table_changes.into_scan_builder().build()?;
    let batches: Vec<RecordBatch> = table_changes_scan
        .execute(Arc::new(engine))?
        .map(|data| -> DeltaResult<_> {
            let record_batch: RecordBatch = data?
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
                .into();
            Ok(record_batch)
        })
        .try_collect()?;
    print_batches(&batches)?;
    Ok(())
}
