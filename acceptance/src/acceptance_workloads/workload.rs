//! Workload execution logic for Delta workload specifications.

use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error, Version};
use itertools::Itertools;
use url::Url;

use super::types::{Metadata, MetadataFormat, Protocol, WorkloadSpec};

/// Result of executing a read workload
pub struct ReadResult {
    /// The record batches from the scan
    pub batches: Vec<RecordBatch>,
    /// The schema of the data
    pub schema: Option<Arc<ArrowSchema>>,
}

/// Strip field-level metadata from an Arrow data type (recursive for structs, lists, maps).
///
/// This is needed because the kernel's `transform_to_logical` inconsistently applies field
/// metadata: batches that go through `apply_schema` (when there's a transform expression) get
/// metadata like `delta.typeChanges`, while batches that don't need a transform are returned
/// without it. Arrow's `concat_batches` requires identical schemas, so we strip metadata to
/// normalize.
fn strip_field_metadata(dt: &ArrowDataType) -> ArrowDataType {
    match dt {
        ArrowDataType::Struct(fields) => {
            let new_fields: Vec<ArrowField> = fields
                .iter()
                .map(|f| {
                    let new_dt = strip_field_metadata(f.data_type());
                    ArrowField::new(f.name(), new_dt, f.is_nullable())
                })
                .collect();
            ArrowDataType::Struct(new_fields.into())
        }
        ArrowDataType::List(field) => {
            let new_dt = strip_field_metadata(field.data_type());
            ArrowDataType::List(Arc::new(ArrowField::new(
                field.name(),
                new_dt,
                field.is_nullable(),
            )))
        }
        ArrowDataType::Map(field, sorted) => {
            let new_dt = strip_field_metadata(field.data_type());
            ArrowDataType::Map(
                Arc::new(ArrowField::new(field.name(), new_dt, field.is_nullable())),
                *sorted,
            )
        }
        other => other.clone(),
    }
}

/// Strip all field-level metadata from an Arrow schema.
fn strip_schema_metadata(schema: &ArrowSchema) -> ArrowSchema {
    let new_fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|f| {
            let new_dt = strip_field_metadata(f.data_type());
            ArrowField::new(f.name(), new_dt, f.is_nullable())
        })
        .collect();
    ArrowSchema::new(new_fields)
}

impl ReadResult {
    /// Concatenate all batches into a single RecordBatch.
    ///
    /// Strips field-level metadata before concatenation to work around a kernel bug where
    /// `transform_to_logical` inconsistently applies schema metadata across batches.
    pub fn concat(self) -> DeltaResult<RecordBatch> {
        let schema = self.schema.ok_or_else(|| Error::generic("No schema"))?;
        let stripped_schema = Arc::new(strip_schema_metadata(&schema));

        let normalized_batches: Vec<RecordBatch> = self
            .batches
            .into_iter()
            .map(|batch| {
                let columns: Vec<_> = batch.columns().to_vec();
                RecordBatch::try_new(stripped_schema.clone(), columns)
                    .or_else(|_| {
                        let cast_columns: Vec<_> = batch
                            .columns()
                            .iter()
                            .zip(stripped_schema.fields())
                            .map(|(col, field)| {
                                if col.data_type() == field.data_type() {
                                    col.clone()
                                } else {
                                    delta_kernel::arrow::compute::cast(col, field.data_type())
                                        .unwrap_or_else(|_| col.clone())
                                }
                            })
                            .collect();
                        RecordBatch::try_new(stripped_schema.clone(), cast_columns)
                    })
                    .map_err(Error::from)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        concat_batches(&stripped_schema, normalized_batches.iter()).map_err(Error::from)
    }
}

/// Result of executing a snapshot workload
#[derive(Debug, serde::Serialize)]
pub struct SnapshotResult {
    pub version: Version,
    pub protocol: Protocol,
    pub metadata: Metadata,
}

/// Workload execution result
#[allow(clippy::large_enum_variant)]
pub enum WorkloadResult {
    Read(ReadResult),
    Snapshot(SnapshotResult),
}

/// Execute a workload specification and return the result
pub fn execute_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    spec: &WorkloadSpec,
) -> DeltaResult<WorkloadResult> {
    match spec {
        WorkloadSpec::Read {
            version,
            timestamp,
            columns,
            ..
        } => {
            let result = execute_read_workload(
                engine,
                table_root,
                *version,
                timestamp.as_deref(),
                columns.as_deref(),
            )?;
            Ok(WorkloadResult::Read(result))
        }
        WorkloadSpec::Snapshot {
            version, timestamp, ..
        } => {
            let result =
                execute_snapshot_workload(engine, table_root, *version, timestamp.as_deref())?;
            Ok(WorkloadResult::Snapshot(result))
        }
        WorkloadSpec::Unsupported => Err(Error::generic(
            "Unsupported workload type in this harness build",
        )),
    }
}

/// Execute a read workload
pub fn execute_read_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    version: Option<i64>,
    timestamp: Option<&str>,
    columns: Option<&[String]>,
) -> DeltaResult<ReadResult> {
    // Resolve version from timestamp if needed
    let version = if let Some(ts) = timestamp {
        Some(resolve_timestamp_to_version(
            engine.as_ref(),
            table_root,
            ts,
        )?)
    } else {
        version.map(|v| v as Version)
    };

    // Build snapshot
    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v);
    }
    let snapshot = builder.build(engine.as_ref())?;

    let table_schema = snapshot.schema();

    // Build scan with optional column projection
    let mut scan_builder = snapshot.scan_builder();
    if let Some(cols) = columns {
        use delta_kernel::schema::StructType;
        let projected_fields: Vec<_> = cols
            .iter()
            .filter_map(|col_name| table_schema.field(col_name).cloned())
            .collect();
        if !projected_fields.is_empty() {
            let projected_schema =
                Arc::new(StructType::try_new(projected_fields).map_err(|e| {
                    Error::generic(format!("Failed to create projected schema: {}", e))
                })?);
            scan_builder = scan_builder.with_schema(projected_schema);
        }
    }
    let scan = scan_builder.build()?;

    // Get schema from scan
    use delta_kernel::engine::arrow_conversion::TryFromKernel;
    let arrow_schema =
        delta_kernel::arrow::datatypes::Schema::try_from_kernel(scan.logical_schema().as_ref())
            .map_err(|e| Error::generic(format!("Failed to convert schema: {}", e)))?;
    let schema = Arc::new(arrow_schema);

    // Execute scan
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| -> DeltaResult<_> {
            let record_batch = data?.try_into_record_batch()?;
            Ok(record_batch)
        })
        .try_collect()?;

    Ok(ReadResult {
        batches,
        schema: Some(schema),
    })
}

/// Execute a snapshot workload (for metadata validation)
pub fn execute_snapshot_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    version: Option<i64>,
    timestamp: Option<&str>,
) -> DeltaResult<SnapshotResult> {
    let version = if let Some(ts) = timestamp {
        Some(resolve_timestamp_to_version(
            engine.as_ref(),
            table_root,
            ts,
        )?)
    } else {
        version.map(|v| v as Version)
    };

    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v);
    }
    let snapshot = builder.build(engine.as_ref())?;

    let config = snapshot.table_configuration();
    let protocol = config.protocol();
    let metadata = config.metadata();

    Ok(SnapshotResult {
        version: snapshot.version(),
        protocol: Protocol {
            min_reader_version: protocol.min_reader_version(),
            min_writer_version: protocol.min_writer_version(),
            reader_features: protocol
                .reader_features()
                .map(|f| f.iter().map(|feat| feat.to_string()).collect()),
            writer_features: protocol
                .writer_features()
                .map(|f| f.iter().map(|feat| feat.to_string()).collect()),
        },
        metadata: Metadata {
            id: metadata.id().to_string(),
            // TODO: kernel doesn't expose Format through public API yet
            format: MetadataFormat {
                provider: "parquet".to_string(),
                options: std::collections::HashMap::new(),
            },
            schema_string: Some(metadata.schema_string().to_string()),
            partition_columns: metadata.partition_columns().to_vec(),
            configuration: metadata
                .configuration()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            created_time: None,
        },
    })
}

/// Resolve a timestamp string to a table version.
fn resolve_timestamp_to_version(
    engine: &dyn Engine,
    table_root: &Url,
    timestamp_str: &str,
) -> DeltaResult<Version> {
    use chrono::NaiveDateTime;

    let target_ts = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.3f")
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| {
            Error::generic(format!(
                "Failed to parse timestamp '{}': {}",
                timestamp_str, e
            ))
        })?;

    let target_millis = target_ts.and_utc().timestamp_millis();
    let log_url = table_root.join("_delta_log/")?;
    let storage = engine.storage_handler();
    let files = storage.list_from(&log_url)?;

    let mut version_timestamps: Vec<(Version, i64)> = Vec::new();
    for file_meta_result in files {
        let file_meta = file_meta_result?;
        let path = file_meta.location.as_ref();
        let filename = path.rsplit('/').next().unwrap_or("");
        if filename.ends_with(".json") && !filename.contains("checkpoint") {
            if let Ok(version) = filename.trim_end_matches(".json").parse::<Version>() {
                version_timestamps.push((version, file_meta.last_modified));
            }
        }
    }

    version_timestamps.sort_by_key(|(v, _)| *v);

    let mut result_version: Option<Version> = None;
    for (version, ts) in version_timestamps {
        if ts <= target_millis {
            result_version = Some(version);
        } else {
            break;
        }
    }

    result_version.ok_or_else(|| {
        Error::generic(format!(
            "No version found at or before timestamp: {}",
            timestamp_str
        ))
    })
}
