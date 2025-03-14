use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use delta_kernel::actions::deletion_vector::split_vector;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::scan::state::{self, transform_to_logical, DvInfo, GlobalScanState, Stats};
use delta_kernel::scan::RUNTIME;
use delta_kernel::schema::Schema;
use delta_kernel::{DeltaResult, Engine, EngineData, ExpressionRef, FileMeta, Table};

use clap::{Parser, ValueEnum};
use itertools::Itertools;
use url::Url;

/// An example program that reads a table using multiple threads. This shows the use of the
/// scan_data and global_scan_state methods on a Scan, that can be used to partition work to either
/// multiple threads, or workers (in the case of a distributed engine).
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    path: String,

    /// how many threads to read with (1 - 2048)
    #[arg(short, long, default_value_t = 2, value_parser = 1..=2048)]
    thread_count: i64,

    /// Which Engine to use
    #[arg(short, long, value_enum, default_value_t = EngineType::Default)]
    engine: EngineType,

    /// Comma separated list of columns to select
    #[arg(long, value_delimiter=',', num_args(0..))]
    columns: Option<Vec<String>>,

    /// Region to specify to the cloud access store (only applies if using the default engine)
    #[arg(long)]
    region: Option<String>,

    /// Specify that the table is "public" (i.e. no cloud credentials are needed). This is required
    /// for things like s3 public buckets, otherwise the kernel will try and authenticate by talking
    /// to the aws metadata server, which will fail unless you're on an ec2 instance.
    #[arg(long)]
    public: bool,

    /// Limit to printing only LIMIT rows.
    #[arg(short, long)]
    limit: Option<usize>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum EngineType {
    /// Use the default, async engine
    Default,
    /// Use the sync engine (local files only)
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

#[no_mangle]
fn no_mangle_to_stop_optimizer<T>(_val: T) {}
fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    // build a table and get the latest snapshot from it
    let table = Table::try_from_uri(&cli.path)?;
    println!("Reading {}", table.location());

    // create the requested engine
    let engine: Arc<dyn Engine> = match cli.engine {
        EngineType::Default => {
            let mut options = if let Some(region) = cli.region {
                HashMap::from([("region", region)])
            } else {
                HashMap::new()
            };
            if cli.public {
                options.insert("skip_signature", "true".to_string());
            }
            Arc::new(DefaultEngine::try_new(
                table.location(),
                options,
                Arc::new(TokioMultiThreadExecutor::new(RUNTIME.handle().clone())),
            )?)
        }
        EngineType::Sync => Arc::new(SyncEngine::new()),
    };

    let snapshot = table.snapshot(engine.as_ref(), None)?;

    // process the columns requested and build a schema from them
    let read_schema_opt = cli
        .columns
        .map(|cols| -> DeltaResult<_> {
            let table_schema = snapshot.schema();
            let selected_fields = cols.iter().map(|col| {
                table_schema
                    .field(col)
                    .cloned()
                    .ok_or(delta_kernel::Error::Generic(format!(
                        "Table has no such column: {col}"
                    )))
            });
            Schema::try_new(selected_fields).map(Arc::new)
        })
        .transpose()?;

    // build a scan with the specified schema
    let scan = snapshot
        .into_scan_builder()
        .with_schema_opt(read_schema_opt)
        .build()?;

    // this gives us an iterator of (our engine data, selection vector). our engine data is just
    // arrow data. The schema can be obtained by calling
    // [`delta_kernel::scan::scan_row_schema`]. Generally engines will not need to interact with
    // this data directly, and can just call [`visit_scan_files`] to get pre-parsed data back from
    // the kernel.
    let scan_data = scan.scan_data(engine.as_ref())?;

    // get any global state associated with this scan
    let global_state = Arc::new(scan.global_scan_state());

    // have handed out all copies needed, drop so record_batch_rx will exit when the last thread is
    // done sending
    #[allow(unused)]
    struct ScanFile {
        path: String,
        size: i64,
        dv_info: DvInfo,
        transform: Option<ExpressionRef>,
    }
    fn scan_data_callback(
        batches: &mut Vec<ScanFile>,
        path: &str,
        size: i64,
        _: Option<Stats>,
        dv_info: DvInfo,
        transform: Option<ExpressionRef>,
    ) {
        batches.push(ScanFile {
            path: path.to_string(),
            size,
            dv_info,
            transform,
        });
    }

    let scan_files_iter = scan_data
        .map(|res| {
            let (data, vec, transforms) = res?;
            let scan_files = vec![];
            state::visit_scan_files(
                data.as_ref(),
                &vec,
                &transforms,
                scan_files,
                scan_data_callback,
            )
        })
        // Iterator<DeltaResult<Vec<ScanFile>>> to Iterator<DeltaResult<ScanFile>>
        .flatten_ok();
    for elem in scan_files_iter {
        no_mangle_to_stop_optimizer(elem.unwrap());
    }

    Ok(())
}
