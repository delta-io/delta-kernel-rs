//! CRC file writing functionality.
//!
//! Provides CRC computation and serialization for writing version checksum files
//! after a successful transaction commit.

use std::sync::Arc;

use crate::actions::DomainMetadata;
use crate::engine_data::FilteredEngineData;
use crate::expressions::{ArrayData, Scalar, StructData};
use crate::path::LogRoot;
use crate::schema::{ArrayType, DataType, SchemaRef, ToSchema as _};
use crate::transaction::PostCommitContext;
use crate::{DeltaResult, Engine, Error, EvaluationHandlerExtension as _, IntoEngineData, Version};

use super::Crc;

#[allow(dead_code)] // Will be called from SnapshotStatistics::write_checksum()
/// Write a CRC file to the _delta_log directory.
///
/// Per the Delta protocol: "Writers MUST NOT overwrite existing Version Checksum files."
/// We use `overwrite=false` (PutMode::Create). If the file already exists, we treat
/// `FileAlreadyExists` as success (the CRC now exists, which was the goal).
pub(crate) fn write_crc_file(
    engine: &dyn Engine,
    table_root: &url::Url,
    crc: &Crc,
    version: Version,
) -> DeltaResult<()> {
    let log_root = LogRoot::new(table_root.clone())?;
    let crc_filename = format!("{version:020}.crc");
    let crc_url = log_root
        .log_root()
        .join(&crc_filename)
        .map_err(|e| Error::generic(format!("Failed to construct CRC path: {e}")))?;
    let schema: SchemaRef = Arc::new(Crc::to_schema());
    let engine_data = crc.clone().into_engine_data(schema, engine)?;
    let filtered = FilteredEngineData::with_all_rows_selected(engine_data);

    match engine.json_handler().write_json_file(
        &crc_url,
        Box::new(std::iter::once(Ok(filtered))),
        false, // overwrite = false, per protocol
    ) {
        Ok(()) => Ok(()),
        Err(Error::FileAlreadyExists(_)) => Ok(()), // Idempotent
        Err(e) => Err(e),
    }
}

#[allow(dead_code)] // Will be called from SnapshotStatistics::write_checksum()
/// Compute a CRC for version 0 (create-table). Only needs the transaction's own adds.
pub(crate) fn compute_crc_for_create_table(ctx: &PostCommitContext, crc: &mut Crc) {
    crc.table_size_bytes = ctx.total_add_file_size_bytes;
    crc.num_files = ctx.num_add_files;
    crc.in_commit_timestamp_opt = ctx.in_commit_timestamp;
}

#[allow(dead_code)] // Will be called from SnapshotStatistics::write_checksum()
/// Compute a CRC at version N+1 given the CRC at version N and the transaction delta.
/// This is the SIMPLE (cheap) path: pure arithmetic, zero I/O.
pub(crate) fn compute_crc_post_commit(old_crc: &Crc, ctx: &PostCommitContext, crc: &mut Crc) {
    crc.table_size_bytes =
        old_crc.table_size_bytes + ctx.total_add_file_size_bytes - ctx.total_remove_file_size_bytes;
    crc.num_files = old_crc.num_files + ctx.num_add_files - ctx.num_remove_files;
    crc.in_commit_timestamp_opt = ctx.in_commit_timestamp;

    // Merge domain metadata: old CRC DMs as base, txn DMs override
    crc.domain_metadata = Some(merge_domain_metadata(
        old_crc.domain_metadata.as_deref(),
        &ctx.domain_metadata_actions,
    ));
}

/// Merge domain metadata: start from a base set, apply delta (additions override,
/// removals delete). Returns only active (non-removed) domains.
fn merge_domain_metadata(
    base_dms: Option<&[DomainMetadata]>,
    delta_dms: &[DomainMetadata],
) -> Vec<DomainMetadata> {
    use std::collections::HashMap;

    let mut dm_map: HashMap<String, DomainMetadata> = base_dms
        .unwrap_or_default()
        .iter()
        .map(|dm| (dm.domain().to_string(), dm.clone()))
        .collect();

    for dm in delta_dms {
        if dm.is_tombstone() {
            dm_map.remove(dm.domain());
        } else {
            dm_map.insert(dm.domain().to_string(), dm.clone());
        }
    }

    dm_map.into_values().collect()
}

/// Serialize domain metadata as a Scalar for CRC writing.
fn domain_metadata_to_scalar(dm_opt: Option<&[DomainMetadata]>) -> DeltaResult<Scalar> {
    let array_type = ArrayType::new(DomainMetadata::to_data_type(), false);
    match dm_opt {
        None => Ok(Scalar::Null(DataType::Array(Box::new(array_type)))),
        Some(domains) => {
            let elements: DeltaResult<Vec<Scalar>> = domains
                .iter()
                .map(|dm| {
                    let fields = vec![
                        crate::schema::StructField::not_null("domain", DataType::STRING),
                        crate::schema::StructField::not_null("configuration", DataType::STRING),
                        crate::schema::StructField::not_null("removed", DataType::BOOLEAN),
                    ];
                    let values = vec![
                        Scalar::String(dm.domain().to_owned()),
                        Scalar::String(dm.configuration().to_owned()),
                        Scalar::Boolean(false), // CRC only stores active (non-removed) DMs
                    ];
                    Ok(Scalar::Struct(StructData::try_new(fields, values)?))
                })
                .collect();
            Ok(Scalar::Array(ArrayData::try_new(array_type, elements?)?))
        }
    }
}

/// Helper to create a null array scalar with the correct type.
fn null_array_scalar<T: ToDataType>() -> Scalar {
    Scalar::Null(DataType::Array(Box::new(ArrayType::new(
        T::to_data_type(),
        false,
    ))))
}

use crate::actions::{Add, SetTransaction};
use crate::schema::derive_macro_utils::ToDataType;

impl IntoEngineData for Crc {
    fn into_engine_data(
        self,
        schema: SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn crate::EngineData>> {
        let domain_metadata_scalar = domain_metadata_to_scalar(self.domain_metadata.as_deref())?;

        // Build the flattened leaf-level scalars in schema traversal order.
        // Nested structs (Metadata, Protocol) must be flattened to their leaf values.
        let mut values: Vec<Scalar> = Vec::with_capacity(30);

        // Required scalar fields
        values.push(self.table_size_bytes.into());
        values.push(self.num_files.into());
        values.push(self.num_metadata.into());
        values.push(self.num_protocol.into());

        // Metadata (9 leaf values: id, name, description, format.provider, format.options,
        //           schemaString, partitionColumns, createdTime, configuration)
        values.push(Scalar::String(self.metadata.id().to_string()));
        values.push(
            self.metadata
                .name()
                .map(|s| Scalar::String(s.to_string()))
                .unwrap_or(Scalar::Null(DataType::STRING)),
        );
        values.push(
            self.metadata
                .description()
                .map(|s| Scalar::String(s.to_string()))
                .unwrap_or(Scalar::Null(DataType::STRING)),
        );
        values.push(Scalar::String(self.metadata.format().provider.clone()));
        values.push(self.metadata.format().options.clone().try_into()?);
        values.push(Scalar::String(self.metadata.schema_string().to_string()));
        values.push(self.metadata.partition_columns().to_vec().try_into()?);
        values.push(
            self.metadata
                .created_time()
                .map(Scalar::Long)
                .unwrap_or(Scalar::Null(DataType::LONG)),
        );
        values.push(self.metadata.configuration().clone().try_into()?);

        // Protocol (4 leaf values: minReaderVersion, minWriterVersion,
        //           readerFeatures, writerFeatures)
        values.push(Scalar::Integer(self.protocol.min_reader_version()));
        values.push(Scalar::Integer(self.protocol.min_writer_version()));
        // readerFeatures and writerFeatures are Option<Vec<TableFeature>>
        // They serialize as nullable Array<String>
        let str_array_type = ArrayType::new(DataType::STRING, false);
        match self.protocol.reader_features() {
            Some(features) => {
                let feature_scalars: Vec<Scalar> = features
                    .iter()
                    .map(|f| Scalar::String(f.to_string()))
                    .collect();
                values.push(Scalar::Array(ArrayData::try_new(
                    str_array_type.clone(),
                    feature_scalars,
                )?));
            }
            None => values.push(Scalar::Null(DataType::Array(Box::new(
                str_array_type.clone(),
            )))),
        }
        match self.protocol.writer_features() {
            Some(features) => {
                let feature_scalars: Vec<Scalar> = features
                    .iter()
                    .map(|f| Scalar::String(f.to_string()))
                    .collect();
                values.push(Scalar::Array(ArrayData::try_new(
                    str_array_type.clone(),
                    feature_scalars,
                )?));
            }
            None => values.push(Scalar::Null(DataType::Array(Box::new(str_array_type)))),
        }

        // Optional scalar fields
        values.push(
            self.txn_id
                .map(Scalar::String)
                .unwrap_or(Scalar::Null(DataType::STRING)),
        );
        values.push(
            self.in_commit_timestamp_opt
                .map(Scalar::Long)
                .unwrap_or(Scalar::Null(DataType::LONG)),
        );

        // Complex nullable arrays (always None for cheap path)
        values.push(null_array_scalar::<SetTransaction>());
        values.push(domain_metadata_scalar);

        // FileSizeHistogram: a nullable struct with 3 array leaves.
        // When null, provide null for each leaf field.
        // sortedBinBoundaries: Array<Long>
        let long_array_type = ArrayType::new(DataType::LONG, false);
        values.push(Scalar::Null(DataType::Array(Box::new(
            long_array_type.clone(),
        ))));
        // fileCounts: Array<Long>
        values.push(Scalar::Null(DataType::Array(Box::new(
            long_array_type.clone(),
        ))));
        // totalBytes: Array<Long>
        values.push(Scalar::Null(DataType::Array(Box::new(long_array_type))));

        // allFiles (always None for now)
        values.push(null_array_scalar::<Add>());

        // Optional DV stats
        values.push(
            self.num_deleted_records_opt
                .map(Scalar::Long)
                .unwrap_or(Scalar::Null(DataType::LONG)),
        );
        values.push(
            self.num_deletion_vectors_opt
                .map(Scalar::Long)
                .unwrap_or(Scalar::Null(DataType::LONG)),
        );

        // DeletedRecordCountsHistogram: a nullable struct with 1 array leaf.
        // deletedRecordCounts: Array<Long>
        values.push(Scalar::Null(DataType::Array(Box::new(ArrayType::new(
            DataType::LONG,
            false,
        )))));

        engine.evaluation_handler().create_one(schema, &values)
    }
}
