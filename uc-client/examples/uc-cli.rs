use clap::{Parser, Subcommand};
use std::time::Duration;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use uc_client::{
    models::{commits::Commit, credentials::Operation, CommitsRequest},
    UCClient, UCCommitsClient, UCCommitsRestClient,
};

#[derive(Parser)]
#[command(name = "uc-client")]
#[command(about = "Unity Catalog CLI client", long_about = None)]
struct Cli {
    /// Unity Catalog URL
    #[arg(short, long, env = "UC_WORKSPACE_URL")]
    workspace_url: String,

    /// Authentication token
    #[arg(short, long, env = "UC_TOKEN", hide_env_values = true)]
    token: String,

    /// Enable verbose logging
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Get table metadata
    Table {
        /// Full table name (catalog.schema.table)
        #[arg(value_name = "TABLE")]
        name: String,
    },

    /// Get commits for a table
    Commits {
        /// Full table name (catalog.schema.table)
        #[arg(value_name = "TABLE")]
        name: String,

        /// Start version (optional)
        #[arg(short, long)]
        start_version: Option<i64>,

        /// End version (optional)
        #[arg(short, long)]
        end_version: Option<i64>,
    },

    /// Get temporary credentials for a table
    Credentials {
        /// Table ID
        #[arg(short, long)]
        table_id: String,

        /// Operation type (READ, WRITE, or READ_WRITE)
        #[arg(short, long, value_parser = parse_operation, default_value = "READ")]
        operation: Operation,
    },
}

fn parse_operation(s: &str) -> Result<Operation, String> {
    match s.to_uppercase().as_str() {
        "READ" => Ok(Operation::Read),
        "WRITE" => Ok(Operation::Write),
        "READ_WRITE" | "READWRITE" => Ok(Operation::ReadWrite),
        _ => Err(format!(
            "Invalid operation '{}'. Must be READ, WRITE, or READ_WRITE",
            s
        )),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Setup logging
    let filter_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter_level)))
        .init();

    // Create shared config
    let config = uc_client::ClientConfig::build(&cli.workspace_url, &cli.token)
        .with_timeout(Duration::from_secs(60))
        .with_max_retries(3)
        .build()?;

    // Create shared HTTP client and UC clients
    let http_client = uc_client::http::build_http_client(&config)?;
    let uc_client = UCClient::with_http_client(http_client.clone(), config.clone());
    let uc_commits_client = UCCommitsRestClient::with_http_client(http_client, config);

    // Execute command
    match cli.command {
        Commands::Table { name } => {
            println!("Fetching table metadata for: {}", name);

            match uc_client.get_table(&name).await {
                Ok(table) => {
                    println!("\n✓ Table metadata retrieved successfully\n");
                    println!("{}", table);
                }
                Err(e) => {
                    eprintln!("✗ Failed to get table: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Commits {
            name,
            start_version,
            end_version,
        } => {
            println!("Resolving table: {}", name);

            // First, get the table metadata to obtain table_id and storage_location
            let table = match uc_client.get_table(&name).await {
                Ok(table) => {
                    println!(
                        "✓ Table resolved: {} (ID: {})",
                        table.full_name(),
                        table.table_id
                    );
                    table
                }
                Err(e) => {
                    eprintln!("✗ Failed to resolve table: {}", e);
                    std::process::exit(1);
                }
            };

            println!("Fetching commits for table...");

            let mut request = CommitsRequest::new(&table.table_id, &table.storage_location);

            // Set start_version - default to 0 if not provided
            let start = start_version.unwrap_or(0);
            request = request.with_start_version(start);

            if let Some(end) = end_version {
                request = request.with_end_version(end);
            }

            match uc_commits_client.get_commits(request).await {
                Ok(response) => {
                    println!("\n✓ Commits retrieved successfully\n");
                    println!("Table:           {}", table.full_name());
                    println!("Latest Version:  {}", response.latest_table_version);

                    if let Some(commits) = response.commits {
                        println!();
                        print_commits(&commits);
                    } else {
                        println!("No commits found in the specified range");
                    }
                }
                Err(e) => {
                    eprintln!("✗ Failed to get commits: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Credentials {
            table_id,
            operation,
        } => {
            println!(
                "Getting {} credentials for table_id: {}",
                operation, table_id
            );

            match uc_client.get_credentials(&table_id, operation).await {
                Ok(creds) => {
                    println!("\n✓ Credentials retrieved successfully\n");
                    println!("URL:              {}", creds.url);
                    let expires_str = creds
                        .expiration_as_datetime()
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| format!("Invalid timestamp: {}", creds.expiration_time));
                    println!("Expires at:       {}", expires_str);

                    if let Some(time_left) = creds.time_until_expiry() {
                        let hours = time_left.num_hours();
                        let minutes = time_left.num_minutes() % 60;
                        println!("Time until expiry: {} hours {} minutes", hours, minutes);
                    } else {
                        println!("Time until expiry: Unable to calculate");
                    }

                    if creds.is_expired() {
                        println!("⚠ WARNING: Credentials are already expired!");
                    }

                    if let Some(aws_creds) = &creds.aws_temp_credentials {
                        println!("\nAWS Credentials:");
                        println!(
                            "  Access Key ID:     {}...",
                            &aws_creds.access_key_id[..10.min(aws_creds.access_key_id.len())]
                        );
                        println!("  Secret Access Key: ***hidden***");
                        println!(
                            "  Session Token:     {}...",
                            &aws_creds.session_token[..10.min(aws_creds.session_token.len())]
                        );
                    }
                }
                Err(e) => {
                    eprintln!("✗ Failed to get credentials: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}

fn print_commits(commits: &[Commit]) {
    println!("Commits ({} total):", commits.len());
    println!(
        "{:<8} {:<20} {:<40} {:<15}",
        "Version", "Timestamp", "File Name", "Size"
    );
    println!("{}", "-".repeat(85));

    for commit in commits {
        let timestamp_str = commit
            .timestamp_as_datetime()
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| format!("Invalid timestamp: {}", commit.timestamp));

        println!(
            "{:<8} {:<20} {:<40} {:<15}",
            commit.version,
            timestamp_str,
            if commit.file_name.len() > 40 {
                format!("{}...", &commit.file_name[..37])
            } else {
                commit.file_name.clone()
            },
            format_bytes(commit.file_size)
        );
    }
}

fn format_bytes(bytes: i64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", size as i64, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}
