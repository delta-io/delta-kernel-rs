use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc};
use std::thread;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use delta_kernel::actions::deletion_vector::split_vector;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::scan::state::{DvInfo, GlobalScanState, Stats};
use delta_kernel::scan::transform_to_logical;
use delta_kernel::schema::Schema;
use delta_kernel::{DeltaResult, Engine, EngineData, FileMeta, Table};

use clap::{Parser, ValueEnum};
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

// the way we as a connector represent data to scan. this is computed from the raw data returned
// from the scan, and could be any format the engine chooses to use to facilitate distributing work.
struct ScanFile {
    path: String,
    size: i64,
    partition_values: HashMap<String, String>,
    dv_info: DvInfo,
}

// we know we're using arrow under the hood, so cast an EngineData into something we can work with
fn to_arrow(data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    Ok(data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into())
}

// truncate a batch to the specified number of rows
fn truncate_batch(batch: RecordBatch, rows: usize) -> RecordBatch {
    let cols = batch
        .columns()
        .iter()
        .map(|col| col.slice(0, rows))
        .collect();
    RecordBatch::try_new(batch.schema(), cols).unwrap()
}

// This is the callback that will be called fo each valid scan row
fn send_scan_file(
    scan_tx: &mut spmc::Sender<ScanFile>,
    path: &str,
    size: i64,
    _stats: Option<Stats>,
    dv_info: DvInfo,
    partition_values: HashMap<String, String>,
) {
    let scan_file = ScanFile {
        path: path.to_string(),
        size,
        partition_values,
        dv_info,
    };
    scan_tx.send(scan_file).unwrap();
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    // build a table and get the lastest snapshot from it
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
                Arc::new(TokioBackgroundExecutor::new()),
            )?)
        }
        EngineType::Sync => Arc::new(SyncEngine::new()),
    };

    let snapshot = table.snapshot(engine.as_ref(), None)?;

    // process the columns requested and build a schema from them
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
                        .ok_or(delta_kernel::Error::Generic(format!(
                            "Table has no such column: {col}"
                        )))
                })
                .try_collect();
            selected_fields.map(|selected_fields| Arc::new(Schema::new(selected_fields)))
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

    // create the channels we'll use. record_batch_[t/r]x are used for the threads to send back the
    // processed RecordBatches to themain thread
    let (record_batch_tx, record_batch_rx) = mpsc::channel();
    // scan_file_[t/r]x are used to send each scan file from the iterator out to the waiting threads
    let (mut scan_file_tx, scan_file_rx) = spmc::channel();

    // fire up each thread. we don't need the handles as we rely on the channels to indicate when
    // things are done
    let _handles: Vec<_> = (0..cli.thread_count)
        .map(|_| {
            // items that we need to send to the other thread
            let scan_state = global_state.clone();
            let rb_tx = record_batch_tx.clone();
            let scan_file_rx = scan_file_rx.clone();
            let engine = engine.clone();
            thread::spawn(move || {
                do_work(engine, scan_state, rb_tx, scan_file_rx);
            })
        })
        .collect();

    // have handed out all copies needed, drop so record_batch_rx will exit when the last thread is
    // done sending
    drop(record_batch_tx);

    for res in scan_data {
        let (data, vector) = res?;
        scan_file_tx = delta_kernel::scan::state::visit_scan_files(
            data.as_ref(),
            &vector,
            scan_file_tx,
            send_scan_file,
        )?;
    }

    // have sent all scan files, drop this so threads will exit when there's no more work
    drop(scan_file_tx);

    let batches = if let Some(limit) = cli.limit {
        // gather batches while we need
        let mut batches = vec![];
        let mut rows_so_far = 0;
        for mut batch in record_batch_rx.iter() {
            let batch_rows = batch.num_rows();
            if rows_so_far < limit {
                if rows_so_far + batch_rows > limit {
                    // truncate this batch
                    batch = truncate_batch(batch, limit - rows_so_far);
                }
                batches.push(batch);
            }
            rows_so_far += batch_rows;
        }
        println!("Printing first {limit} rows of {rows_so_far} total rows");
        batches
    } else {
        // simply gather up all batches
        record_batch_rx.iter().collect()
    };
    print_batches(&batches)?;
    Ok(())
}

// this is the work each thread does
fn do_work(
    engine: Arc<dyn Engine>,
    scan_state: Arc<GlobalScanState>,
    record_batch_tx: Sender<RecordBatch>,
    scan_file_rx: spmc::Receiver<ScanFile>,
) {
    // get the type for the function calls
    let engine: &dyn Engine = engine.as_ref();
    let read_schema = scan_state.read_schema.clone();
    // in a loop, try and get a ScanFile. Note that `recv` will return an `Err` when the other side
    // hangs up, which indicates there's no more data to process.
    while let Ok(scan_file) = scan_file_rx.recv() {
        // we got a scan file, let's process it
        let root_url = Url::parse(&scan_state.table_root).unwrap();

        // get the selection vector (i.e. deletion vector)
        let mut selection_vector = scan_file
            .dv_info
            .get_selection_vector(engine, &root_url)
            .unwrap();

        // build the required metadata for our parquet handler to read this file
        let location = root_url.join(&scan_file.path).unwrap();
        let meta = FileMeta {
            last_modified: 0,
            size: scan_file.size as usize,
            location,
        };

        // this example uses the parquet_handler from the engine, but an engine could
        // choose to use whatever method it might want to read a parquet file. The reader
        // could, for example, fill in the parition columns, or apply deletion vectors. Here
        // we assume a more naive parquet reader and fix the data up after the fact.
        // further parallelism would also be possible here as we could read the parquet file
        // in chunks where each thread reads one chunk. The engine would need to ensure
        // enough meta-data was passed to each thread to correctly apply the selection
        // vector
        let read_results = engine
            .get_parquet_handler()
            .read_parquet_files(&[meta], read_schema.clone(), None)
            .unwrap();

        for read_result in read_results {
            let read_result = read_result.unwrap();
            let len = read_result.length();

            // ask the kernel to transform the physical data into the correct logical form
            let logical = transform_to_logical(
                engine,
                read_result,
                &scan_state,
                &scan_file.partition_values,
            )
            .unwrap();

            let record_batch = to_arrow(logical).unwrap();

            // need to split the dv_mask. what's left in dv_mask covers this result, and rest
            // will cover the following results
            let rest = split_vector(selection_vector.as_mut(), len, Some(true));
            let batch = if let Some(mask) = selection_vector.clone() {
                // apply the selection vector
                filter_record_batch(&record_batch, &mask.into()).unwrap()
            } else {
                record_batch
            };
            selection_vector = rest;

            // send back the processed result
            record_batch_tx.send(batch).unwrap();
        }
    }
}
