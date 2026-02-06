use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use common::{LocationArgs, ParseWithExamples};

use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::{DeltaResult, Snapshot};

/// An example program that checkpoints a table.
/// !!!WARNING!!!: This doesn't use put-if-absent, or a catalog based commit, so it is UNSAFE.
/// As such you need to pass --unsafe_i_know_what_im_doing as an argument to get this to actually
/// write the checkpoint, otherwise it will just do all the work it _would_ have done, but not
/// actually write the final checkpoint.
#[derive(Parser)]
#[command(author, version, about, verbatim_doc_comment)]
#[command(propagate_version = true)]
struct Cli {
    #[command(flatten)]
    location_args: LocationArgs,

    /// This program doesn't use put-if-absent, or a catalog based commit, so it is UNSAFE.  As such
    /// you need to pass --unsafe-i-know-what-im-doing as an argument to get this to actually write
    /// the checkpoint
    #[arg(long)]
    unsafe_i_know_what_im_doing: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();
    match try_main().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            println!("{e:#?}");
            ExitCode::FAILURE
        }
    }
}

async fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse_with_examples(env!("CARGO_PKG_NAME"), "Write", "write", "");

    let url = delta_kernel::try_parse_uri(&cli.location_args.path)?;
    println!("Checkpointing Delta table at: {url}");

    use delta_kernel::engine::default::storage::store_from_url;
    let store = store_from_url(&url)?;

    // Use TokioMultiThreadExecutor to avoid deadlock when calling snapshot.checkpoint()
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(store)
        .with_task_executor(executor)
        .build();

    let snapshot = Snapshot::builder_for(url).build(&engine)?;

    if cli.unsafe_i_know_what_im_doing {
        // Use the simplified all-in-one checkpoint API
        snapshot.checkpoint(&engine)?;
        println!("Table checkpointed");
    } else {
        // For dry-run mode, we still need to manually iterate through the data
        // to show what would be written without actually writing it
        println!("--unsafe-i-know-what-im-doing not specified, just doing a dry run");
        dry_run_checkpoint(snapshot, &engine)?;
    }
    Ok(())
}

fn dry_run_checkpoint(
    snapshot: Arc<Snapshot>,
    engine: &impl delta_kernel::Engine,
) -> DeltaResult<()> {
    let writer = snapshot.create_checkpoint_writer()?;
    let checkpoint_path = writer.checkpoint_path()?;
    let data_iter = writer.checkpoint_data(engine)?;

    let mut total_rows = 0usize;
    for data_res in data_iter {
        let data = data_res?.apply_selection_vector()?;
        total_rows += data.len();
    }

    println!(
        "Would have written a checkpoint as:\n\tpath: {checkpoint_path}\n\ttotal rows: {total_rows}"
    );
    Ok(())
}
