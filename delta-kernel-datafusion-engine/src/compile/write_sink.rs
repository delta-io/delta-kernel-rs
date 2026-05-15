//! Lower [`SinkType::Write`](delta_kernel::plans::ir::nodes::SinkType::Write) to DataFusion
//! [`DataSinkExec`](datafusion_datasource::sink::DataSinkExec) for single-target file output.
//!
//! Uses the stock DataFusion [`ParquetSink`](datafusion_datasource_parquet::ParquetSink) /
//! [`JsonSink`](datafusion_datasource_json::JsonSink) with [`FileSinkConfig::file_output_mode`]
//! set to [`FileOutputMode::SingleFile`] so one object-store path receives all rows (including an
//! empty Arrow schema via an empty batch when the stream has no rows).
//!
//! # Limitations
//!
//! - **Post-write metadata**: Parquet footer metadata gathered inside [`ParquetSink::written`] is
//!   not surfaced to callers (no Delta [`DataFileMetadata`] bridge). Harvesting structured stats
//!   for commits remains future work.
//! - **URLs**: Anything [`ListingTableUrl`] can parse is accepted at compile time; execution needs a
//!   matching [`object_store`] registration on the executor [`TaskContext`] (local `file://` works
//!   with the default runtime).

use std::sync::Arc;

use datafusion_common::config::TableParquetOptions;
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_sink_config::{FileOutputMode, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_json::{JsonFormat, JsonSink};
use datafusion_datasource_parquet::ParquetSink;
use datafusion_expr::dml::InsertOp;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{WriteFileFormat, WriteSink};

use crate::error::plan_compilation;

pub(crate) fn compile_write_sink(
    inner: Arc<dyn ExecutionPlan>,
    sink: &WriteSink,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    // DataSinkExec requires a single input partition unless the DF optimizer inserts merges.
    let coalesced = Arc::new(CoalescePartitionsExec::new(inner));
    let output_schema = coalesced.schema();

    let table_path = ListingTableUrl::parse(sink.destination.as_str())
        .map_err(|e| plan_compilation(format!("Write sink destination URL is invalid: {e}")))?;

    let file_extension = match sink.format {
        WriteFileFormat::Parquet => "parquet",
        WriteFileFormat::JsonLines => "json",
    }
    .to_string();

    let config = FileSinkConfig {
        original_url: sink.destination.as_str().to_string(),
        object_store_url: table_path.object_store(),
        file_group: FileGroup::new(Vec::new()),
        table_paths: vec![table_path],
        output_schema,
        table_partition_cols: Vec::new(),
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension,
        file_output_mode: FileOutputMode::SingleFile,
    };

    let data_sink: Arc<dyn DataSink> = match sink.format {
        WriteFileFormat::Parquet => {
            Arc::new(ParquetSink::new(config, TableParquetOptions::default()))
        }
        WriteFileFormat::JsonLines => {
            let json_format = JsonFormat::default().with_newline_delimited(true);
            let writer_options = JsonWriterOptions::try_from(json_format.options())
                .map_err(|e| plan_compilation(format!("JsonLines write options: {e}")))?;
            Arc::new(JsonSink::new(config, writer_options))
        }
    };

    Ok(Arc::new(DataSinkExec::new(coalesced, data_sink, None)))
}
