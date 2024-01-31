use deltakernel::client::executor::tokio::TokioBackgroundExecutor;
use deltakernel::client::DefaultTableClient;
use deltakernel::scan::ScanBuilder;
use deltakernel::Table;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{cast::AsArray, ArrayRef};
use arrow_schema::{DataType, TimeUnit};
use clap::Parser;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, Color, Table as ComfyTable};

/// An example program that dumps out the data of a delta table. Struct and Map types are not
/// supported.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    #[arg(short, long)]
    path: String,

    /// Dump the table in plain ascii with no color/utf-8 styling
    #[arg(short, long)]
    ascii: bool,
}

macro_rules! format_primitive {
    ($column:expr, $typ:tt, $row_id:expr) => {
        format!("{}", $column.as_primitive::<$typ>().value($row_id))
    };
    ($column:expr, $typ:tt, $row_id:expr, $suffix:expr) => {
        format!(
            "{}{}",
            $column.as_primitive::<$typ>().value($row_id),
            $suffix
        )
    };
}

fn extract_value(column: &ArrayRef, row_id: usize) -> String {
    use arrow_array::types::*;
    use DataType::*;
    match column.data_type() {
        Binary => format!("{:?}", column.as_binary::<i32>().value(row_id)),
        Boolean => format!("{}", column.as_boolean().value(row_id)),
        Date32 => format_primitive!(column, Date32Type, row_id),
        Date64 => format_primitive!(column, Date64Type, row_id),
        Decimal128(..) => format_primitive!(column, Decimal128Type, row_id),
        Float32 => format_primitive!(column, Float32Type, row_id),
        Float64 => format_primitive!(column, Float64Type, row_id),
        Int8 => format_primitive!(column, Int8Type, row_id),
        Int16 => format_primitive!(column, Int16Type, row_id),
        Int32 => format_primitive!(column, Int32Type, row_id),
        Int64 => format_primitive!(column, Int64Type, row_id),
        Timestamp(unit, _) => match unit {
            TimeUnit::Second => format_primitive!(column, TimestampSecondType, row_id, "s"),
            TimeUnit::Millisecond => {
                format_primitive!(column, TimestampMillisecondType, row_id, "ms")
            }
            TimeUnit::Microsecond => {
                format_primitive!(column, TimestampMicrosecondType, row_id, "us")
            }
            TimeUnit::Nanosecond => {
                format_primitive!(column, TimestampNanosecondType, row_id, "ns")
            }
        },
        Utf8 => column.as_string::<i32>().value(row_id).to_string(),
        _ => {
            todo!("Don't support {}", column.data_type())
        }
    }
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
    let engine_interface = DefaultTableClient::try_new(
        &url,
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    );
    let Ok(engine_interface) = engine_interface else {
        println!(
            "Failed to construct table client: {}",
            engine_interface.err().unwrap()
        );
        return;
    };

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine_interface, None);
    let Ok(snapshot) = snapshot else {
        println!(
            "Failed to construct latest snapshot: {}",
            snapshot.err().unwrap()
        );
        return;
    };

    let scan = ScanBuilder::new(snapshot).build();

    let schema = scan.schema();
    let header_names = schema.fields.iter().map(|field| {
        let cell = Cell::new(field.name());
        if cli.ascii {
            cell
        } else {
            cell.fg(Color::Green)
        }
    });

    let mut table = ComfyTable::new();
    if !cli.ascii {
        table.load_preset(UTF8_FULL);
    }
    table.set_header(header_names);

    for batch in scan.execute(&engine_interface).unwrap() {
        for row in 0..batch.num_rows() {
            let table_row =
                (0..batch.num_columns()).map(|col| extract_value(batch.column(col), row));
            table.add_row(table_row);
        }
    }

    println!("{table}");
}
