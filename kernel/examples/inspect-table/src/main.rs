use delta_kernel::actions::visitors::{
    AddVisitor, MetadataVisitor, ProtocolVisitor, RemoveVisitor, SetTransactionVisitor,
};
use delta_kernel::actions::{
    get_log_schema, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine_data::{GetData, TypedGetData};
use delta_kernel::expressions::ColumnName;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::schema::StructField;
use delta_kernel::{DeltaResult, RowVisitor, Table};

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::{Arc, LazyLock};

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
    /// Show the meta-data that would be used to scan the table
    ScanData,
    /// Show each action from the log-segments
    Actions {
        /// Show the log in forward order (default is to show it going backwards in time)
        #[arg(short, long)]
        forward: bool,
    },
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

enum Action {
    Metadata(delta_kernel::actions::Metadata, usize),
    Protocol(delta_kernel::actions::Protocol, usize),
    Remove(delta_kernel::actions::Remove, usize),
    Add(delta_kernel::actions::Add, usize),
    SetTransaction(delta_kernel::actions::SetTransaction, usize),
}

impl Action {
    fn row(&self) -> usize {
        match self {
            Action::Metadata(_, row) => *row,
            Action::Protocol(_, row) => *row,
            Action::Remove(_, row) => *row,
            Action::Add(_, row) => *row,
            Action::SetTransaction(_, row) => *row,
        }
    }
}

struct LogVisitor {
    actions: Vec<Action>,
    add_offset: usize,
    remove_offset: usize,
    protocol_offset: usize,
    metadata_offset: usize,
    set_transaction_offset: usize,
    previous_rows_seen: usize,
}

static NAMES_AND_FIELDS: LazyLock<(Vec<ColumnName>, Vec<StructField>)> =
    LazyLock::new(|| get_log_schema().leaf_fields(None));

impl LogVisitor {
    fn new() -> LogVisitor {
        let mut names = NAMES_AND_FIELDS.0.iter();
        let mut next_offset =
            |prev_offset, name| prev_offset + names.position(|n| n[0] == name).unwrap();
        let add_offset = 0;
        let remove_offset = next_offset(add_offset, REMOVE_NAME);
        let protocol_offset = next_offset(remove_offset, PROTOCOL_NAME);
        let metadata_offset = next_offset(protocol_offset, METADATA_NAME);
        let set_transaction_offset = next_offset(metadata_offset, SET_TRANSACTION_NAME);
        LogVisitor {
            actions: vec![],
            add_offset,
            remove_offset,
            protocol_offset,
            metadata_offset,
            set_transaction_offset,
            previous_rows_seen: 0,
        }
    }
}

impl RowVisitor for LogVisitor {
    fn selected_leaf_fields(&self) -> &'static [StructField] {
        &NAMES_AND_FIELDS.1
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            if let Some(path) = getters[self.add_offset].get_opt(i, "add.path")? {
                self.actions.push(Action::Add(
                    AddVisitor::visit_add(i, path, &getters[self.add_offset..])?,
                    self.previous_rows_seen + i,
                ));
            }
            if let Some(path) = getters[self.remove_offset].get_opt(i, "remove.path")? {
                self.actions.push(Action::Remove(
                    RemoveVisitor::visit_remove(i, path, &getters[self.remove_offset..])?,
                    self.previous_rows_seen + i,
                ));
            }
            if let Some(id) = getters[self.metadata_offset].get_opt(i, "metadata.id")? {
                self.actions.push(Action::Metadata(
                    MetadataVisitor::visit_metadata(i, id, &getters[self.metadata_offset..])?,
                    self.previous_rows_seen + i,
                ));
            }
            if let Some(min_reader_version) =
                getters[self.protocol_offset].get_opt(i, "protocol.min_reader_version")?
            {
                self.actions.push(Action::Protocol(
                    ProtocolVisitor::visit_protocol(
                        i,
                        min_reader_version,
                        &getters[self.protocol_offset..],
                    )?,
                    self.previous_rows_seen + i,
                ));
            }
            if let Some(app_id) = getters[self.set_transaction_offset].get_opt(i, "txn.appId")? {
                self.actions.push(Action::SetTransaction(
                    SetTransactionVisitor::visit_txn(
                        i,
                        app_id,
                        &getters[self.set_transaction_offset..],
                    )?,
                    self.previous_rows_seen + i,
                ));
            }
        }
        self.previous_rows_seen += row_count;
        Ok(())
    }
}

// This is the callback that will be called for each valid scan row
fn print_scan_file(
    _: &mut (),
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    partition_values: HashMap<String, String>,
) {
    let num_record_str = if let Some(s) = stats {
        format!("{}", s.num_records)
    } else {
        "[unknown]".to_string()
    };
    println!(
        "Data to process:\n  \
              Path:\t\t{path}\n  \
              Size (bytes):\t{size}\n  \
              Num Records:\t{num_record_str}\n  \
              Has DV?:\t{}\n  \
              Part Vals:\t{partition_values:?}",
        dv_info.has_vector()
    );
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    // build a table and get the lastest snapshot from it
    let table = Table::try_from_uri(&cli.path)?;

    let engine = DefaultEngine::try_new(
        table.location(),
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let snapshot = table.snapshot(&engine, None)?;

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
        Commands::ScanData => {
            let scan = ScanBuilder::new(snapshot).build()?;
            let scan_data = scan.scan_data(&engine)?;
            for res in scan_data {
                let (data, vector) = res?;
                delta_kernel::scan::state::visit_scan_files(
                    data.as_ref(),
                    &vector,
                    (),
                    print_scan_file,
                )?;
            }
        }
        Commands::Actions { forward } => {
            let log_schema = get_log_schema();
            let actions = snapshot._log_segment().replay(
                &engine,
                log_schema.clone(),
                log_schema.clone(),
                None,
            )?;

            let mut visitor = LogVisitor::new();
            for action in actions {
                action?.0.visit_rows(&NAMES_AND_FIELDS.0, &mut visitor)?;
            }

            if *forward {
                visitor
                    .actions
                    .sort_by(|a, b| a.row().partial_cmp(&b.row()).unwrap());
            } else {
                visitor
                    .actions
                    .sort_by(|a, b| b.row().partial_cmp(&a.row()).unwrap());
            }
            for action in visitor.actions.iter() {
                match action {
                    Action::Metadata(md, row) => println!("\nAction {row}:\n{:#?}", md),
                    Action::Protocol(p, row) => println!("\nAction {row}:\n{:#?}", p),
                    Action::Remove(r, row) => println!("\nAction {row}:\n{:#?}", r),
                    Action::Add(a, row) => println!("\nAction {row}:\n{:#?}", a),
                    Action::SetTransaction(t, row) => println!("\nAction {row}:\n{:#?}", t),
                }
            }
        }
    };
    Ok(())
}
