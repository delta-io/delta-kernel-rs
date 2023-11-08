use deltakernel::client::executor::tokio::TokioBackgroundExecutor;
use deltakernel::client::DefaultTableClient;
use deltakernel::Table;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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

    let table = Table::new(url, table_client);
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
    }
}
