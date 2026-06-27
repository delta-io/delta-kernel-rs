//! Minimal CLI exercising the UC API surface.

use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::UCClient;

#[derive(Parser)]
#[command(name = "uc-cli")]
#[command(about = "Unity Catalog CLI client (UC API surface)", long_about = None)]
struct Cli {
    /// Unity Catalog URL.
    #[arg(short, long, env = "UC_WORKSPACE_URL")]
    workspace_url: String,

    /// Authentication token.
    #[arg(short, long, env = "UC_TOKEN", hide_env_values = true)]
    token: String,

    /// Enable verbose logging.
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// `load_table`: fetch metadata + inline commits for a table.
    Load {
        /// Catalog name.
        catalog: String,
        /// Schema name.
        schema: String,
        /// Table name.
        table: String,
    },
    /// `get_table_credentials`: vend temporary credentials for a table.
    Credentials {
        /// Catalog name.
        catalog: String,
        /// Schema name.
        schema: String,
        /// Table name.
        table: String,
        /// Operation type (READ or READ_WRITE).
        #[arg(short, long, value_parser = parse_operation, default_value = "READ")]
        operation: Operation,
    },
    /// `get_config`: protocol-version negotiation handshake.
    Config {
        /// Catalog name (required by the spec).
        #[arg(short, long)]
        catalog: String,
        /// Comma-separated highest protocol version per major version, e.g. `1.1,2.3`.
        #[arg(short, long, default_value = "1.0")]
        versions: String,
    },
}

fn parse_operation(s: &str) -> Result<Operation, String> {
    match s.to_uppercase().as_str() {
        "READ" => Ok(Operation::Read),
        "READ_WRITE" | "READWRITE" => Ok(Operation::ReadWrite),
        _ => Err(format!(
            "Invalid operation '{s}'. Must be READ or READ_WRITE"
        )),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let filter_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter_level)))
        .init();

    let config =
        unity_catalog_delta_rest_client::ClientConfig::build(&cli.workspace_url, &cli.token)
            .with_timeout(Duration::from_secs(60))
            .with_max_retries(3)
            .build()?;
    let uc_client = UCClient::new(config)?;

    match cli.command {
        Commands::Load {
            catalog,
            schema,
            table,
        } => {
            let resp = uc_client.load_table(&catalog, &schema, &table).await?;
            println!("table_uuid:           {}", resp.metadata.table_uuid);
            println!("location:             {}", resp.metadata.location);
            println!("latest_table_version: {:?}", resp.latest_table_version);
            println!("commits ({}):", resp.commits.len());
            for c in &resp.commits {
                println!(
                    "  v={} ts={} size={} file={}",
                    c.version, c.timestamp, c.file_size, c.file_name
                );
            }
        }
        Commands::Credentials {
            catalog,
            schema,
            table,
            operation,
        } => {
            let creds = uc_client
                .get_table_credentials(&catalog, &schema, &table, operation)
                .await?;
            println!("storage_credentials ({}):", creds.storage_credentials.len());
            for c in &creds.storage_credentials {
                println!(
                    "  prefix={} operation={} expires_ms={:?}",
                    c.prefix, c.operation, c.expiration_time_ms
                );
            }
        }
        Commands::Config { catalog, versions } => {
            let parsed: Vec<&str> = versions.split(',').map(str::trim).collect();
            let resp = uc_client.get_config(&catalog, &parsed).await?;
            println!("protocol_version: {}", resp.protocol_version);
            println!("endpoints ({}):", resp.endpoints.len());
            for e in &resp.endpoints {
                println!("  {e}");
            }
        }
    }

    Ok(())
}
