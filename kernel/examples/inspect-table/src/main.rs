use delta_kernel::engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::DefaultEngine;
use delta_kernel::scan::ScanBuilder;
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Table};

use delta_kernel::actions::{parse_actions, Action, ActionType};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    #[arg(short, long)]
    path: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Print the most recent version of the table
    TableVersion,
    /// Show the table's metadata
    Metadata,
    /// Show the table's schema
    Schema,
    /// Show all `Add` actions recorded on the table
    Adds,
    /// Show each action from the log-segments
    Actions {
        /// Show the log in forward order (default is to show it going backwards in time)
        #[arg(short, long)]
        forward: bool,
    },
}

fn main() {
    let cli = Cli::parse();
    let path = std::fs::canonicalize(PathBuf::from(cli.path));
    let Ok(path) = path else {
        println!("Couldn't open table: {}", path.err().unwrap());
        return;
    };
    let Ok(url) = url::Url::from_directory_path(path) else {
        println!("Invalid url");
        return;
    };
    let engine = DefaultEngine::try_new(
        &url,
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    );
    let Ok(engine) = engine else {
        println!(
            "Failed to construct engine: {}",
            engine.err().unwrap()
        );
        return;
    };

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None);
    let Ok(snapshot) = snapshot else {
        println!(
            "Failed to construct latest snapshot: {}",
            snapshot.err().unwrap()
        );
        return;
    };

    match &cli.command {
        Commands::TableVersion => {
            println!("Latest table version: {}", snapshot.version());
        }
        Commands::Metadata => {
            println!("{:#?}", snapshot.metadata());
        }
        Commands::Schema => {
            println!("{:#?}", snapshot.schema());
        }
        Commands::Adds => {
            use delta_kernel::Add;
            let scan = snapshot.into_scan_builder().build().unwrap();
            let files: Vec<Add> = scan
                .files(&engine)
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            println!("{:#?}", files);
        }
        Commands::Actions { forward } => {
            let action_types = vec![
                //ActionType::CommitInfo,
                ActionType::Metadata,
                ActionType::Protocol,
                ActionType::Remove,
                ActionType::Add,
            ];
            let read_schema = Arc::new(StructType::new(
                action_types
                    .iter()
                    .map(|a| a.schema_field())
                    .cloned()
                    .collect(),
            ));

            let batches = snapshot
                ._log_segment()
                .replay(&engine, read_schema, None);

            let batch_vec = batches
                .unwrap()
                .collect::<Vec<DeltaResult<(RecordBatch, bool)>>>();
            let len = batch_vec.len() - 1;

            let batches: Box<dyn Iterator<Item = Result<(RecordBatch, bool), delta_kernel::Error>>> =
                if *forward {
                    Box::new(batch_vec.into_iter().rev())
                } else {
                    Box::new(batch_vec.into_iter())
                };

            for (i, batch) in batches.enumerate() {
                let index = if *forward { i } else { len - i };
                if i != 0 {
                    println!("\n\n");
                }
                println!("-- at {:0>20} --", index);
                let (batch, _) = batch.unwrap();
                let actions = parse_actions(&batch, &action_types).unwrap();
                for action in actions {
                    match action {
                        Action::Metadata(md) => println!("{:#?}", md),
                        Action::Protocol(p) => println!("{:#?}", p),
                        Action::Remove(r) => println!("{:#?}", r),
                        Action::Add(a) => println!("{:#?}", a),
                    }
                }
            }
        }
    }
}
