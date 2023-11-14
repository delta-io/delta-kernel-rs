use deltakernel::client::executor::tokio::TokioBackgroundExecutor;
use deltakernel::client::DefaultTableClient;
use deltakernel::{DeltaResult, Table};

use deltakernel::actions::{Action, ActionType, parse_actions};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    #[arg(short, long)]
    path: String,

    /// Number of times to greet
    // #[arg(short, long, default_value_t = 1)]
    // count: u8,

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
        forward: bool
    },
}


fn main() -> () {
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
    let table_client = DefaultTableClient::try_new(
        &url,
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    );
    let Ok(table_client) = table_client else {
        println!("Failed to construct table client: {}", table_client.err().unwrap());
        return;
    };
    let table_client = Arc::new(table_client);

    let table = Table::new(url, table_client.clone());
    let snapshot = table.snapshot(None);
    let Ok(snapshot) = snapshot else {
        println!("Failed to construct latest snapshot: {}", snapshot.err().unwrap());
        return;
    };
    
    match &cli.command {
        Commands::TableVersion => {
            println!("Latest table version: {}", snapshot.version());
        }
        Commands::Metadata => {
            println!("{:#?}", snapshot.metadata().unwrap());
        }
        Commands::Schema => {
            println!("{:#?}", snapshot.schema().unwrap());
        }
        Commands::Adds => {
            use deltakernel::Add;
            let scan = snapshot.scan().unwrap().build();
            let files: Vec<Add> = scan.files().unwrap().map(|r| r.unwrap()).collect();
            println!("{:#?}", files);
        }
        Commands::Actions { forward } => {
            let action_types = [
                //ActionType::CommitInfo,
                ActionType::Metadata,
                ActionType::Protocol,
                ActionType::Remove,
                ActionType::Add,
            ];
            let read_schema = Arc::new(ArrowSchema {
                fields: action_types.as_ref().iter().map(|a| Arc::new(a.field())).collect(),
                metadata: Default::default(),
            });
            
            let batches = snapshot.get_log_segment().replay(
                &*table_client,
                read_schema,
                None,
            );

            let batches = batches.unwrap();
            let batch_vec = batches.collect::<Vec<DeltaResult<RecordBatch>>>();
            let len = batch_vec.len() - 1;
            
            let batches: Box<dyn Iterator<Item = Result<RecordBatch, deltakernel::Error>>> = if *forward {
                Box::new(batch_vec.into_iter().rev())
            } else {
                Box::new(batch_vec.into_iter())
            };

            for (i, batch) in batches.enumerate() {
                let index = 
                if *forward {
                    i
                } else {
                    len - i
                };
                if i != 0 {
                    println!("\n\n");
                }
                println!("-- at {:0>20} --", index);
                let batch = batch.unwrap();
                let actions = parse_actions(&batch, action_types.as_ref()).unwrap();
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
