use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use deltakernel::client::arrow_data::ArrowEngineData;
use deltakernel::client::default::executor::tokio::TokioBackgroundExecutor;
use deltakernel::client::default::DefaultEngineInterface;
use deltakernel::client::sync::SyncEngineInterface;
use deltakernel::scan::{ScanBuilder, transform_to_logical};
use deltakernel::schema::Schema;
use deltakernel::{DeltaResult, EngineInterface, FileMeta, Table};

use clap::{Parser, ValueEnum};

/// An example program that dumps out the data of a delta table. Struct and Map types are not
/// supported.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,

    /// how many threads to read with
    #[arg(short, long, default_value_t = 2)]
    thread_count: usize,

    /// Which EngineInterface to use
    #[arg(short, long, value_enum, default_value_t = Interface::Default)]
    interface: Interface,

    /// Comma separated list of columns to select
    #[arg(long, value_delimiter=',', num_args(0..))]
    columns: Option<Vec<String>>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Interface {
    /// Use the default, async engine interface
    Default,
    /// Use the sync engine interface (local files only)
    Sync,
}

fn main() -> ExitCode {
    env_logger::init();
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            println!("{e:#?}");
            ExitCode::FAILURE
        }
    }
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();
    let url = url::Url::parse(&cli.path)?;

    println!("Reading {url}");
    let engine_interface: Box<dyn EngineInterface> = match cli.interface {
        Interface::Default => Box::new(DefaultEngineInterface::try_new(
            &url,
            HashMap::<String, String>::new(),
            Arc::new(TokioBackgroundExecutor::new()),
        )?),
        Interface::Sync => Box::new(SyncEngineInterface::new()),
    };

    let table = Table::new(url);
    let snapshot = table.snapshot(engine_interface.as_ref(), None)?;

    let read_schema_opt = cli
        .columns
        .map(|cols| {
            use itertools::Itertools;
            let table_schema = snapshot.schema();
            let selected_fields = cols
                .iter()
                .map(|col| {
                    table_schema
                        .field(col)
                        .cloned()
                        .ok_or(deltakernel::Error::Generic(format!(
                            "Table has no such column: {col}"
                        )))
                })
                .try_collect();
            selected_fields.map(|selected_fields| Arc::new(Schema::new(selected_fields)))
        })
        .transpose()?;
    let scan = ScanBuilder::new(snapshot)
        .with_schema_opt(read_schema_opt)
        .build();

    let scan_state = scan.get_scan_state();
    let scan_files = scan.scan_files(engine_interface.as_ref())?;
    let parquet_handler = engine_interface.get_parquet_handler();

    let mut batches = vec![];
    for scan_file in scan_files {
        let scan_file = scan_file?;
        let meta = &[
            FileMeta {
                last_modified: 0,
                size: scan_file.size,
                location: scan_file.location.clone(),
            }
        ];
        let read_results = parquet_handler.read_parquet_files(
            meta, scan_state.read_schema(), None
        )?;

        for read_result in read_results {
            let len = if let Ok(ref res) = read_result {
                res.length()
            } else {
                0
            };
            let logical = transform_to_logical(
                engine_interface.as_ref(),
                read_result?,
                &scan_state,
                &scan_file
            )?;

            let record_batch: RecordBatch = logical
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| deltakernel::Error::EngineDataType("ArrowEngineData".to_string()))?
                .into();
            // let batch = if let Some(mask) = res.mask {
            //     filter_record_batch(&record_batch, &mask.into())?
            // } else {
            //     record_batch
            // };
            batches.push(record_batch);
        }
    }
    print_batches(&batches)?;
    Ok(())
}
