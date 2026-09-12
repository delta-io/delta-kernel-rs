use std::collections::{HashMap, HashSet};
use std::num::NonZero;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;

use super::BoundWriteContext;
use crate::expressions::{lit, ColumnName, ExpressionStructPatchBuilder, Scalar};
use crate::partition::serialization::serialize_partition_value;
use crate::partition::validation::validate_partition_values;
use crate::schema::void_utils::add_void_stripping;
use crate::schema::{SchemaRef, StructField, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{ColumnMappingMode, TableFeature};
use crate::table_properties::{
    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, MATERIALIZED_ROW_ID_COLUMN_NAME,
};
use crate::utils::require;
use crate::{DataType, DeltaResult, Error, Expression};

const WRITE_STATE_FORMAT_VERSION: u32 = 1;

/// Table-wide state required to create [`BoundWriteContext`] instances.
///
/// A transaction creates this state once on the driver through
/// [`Transaction::write_state`](super::Transaction::write_state). Distributed writers can encode
/// it, transport it to another process, decode it, and bind partition values there without
/// transporting the transaction itself.
#[derive(Debug, Deserialize, Serialize)]
pub struct WriteState {
    pub(super) table_root: Url,
    /// Complete logical table schema, including partition columns.
    ///
    /// Partition binding needs this schema to validate values, preserve metadata-defined field
    /// order, and translate logical partition names to their physical names.
    pub(super) full_logical_schema: SchemaRef,
    /// Base logical data schema: the Delta schema excluding partition columns.
    ///
    /// [`BoundWriteContextBuilder`] appends any connector-specified Row ID and Row Commit Version
    /// columns to construct the final [`BoundWriteContext::logical_data_schema`].
    pub(super) base_logical_data_schema: SchemaRef,
    /// Base physical data schema used by [`BoundWriteContextBuilder`] to construct the final
    /// [`BoundWriteContext::physical_data_schema`].
    ///
    /// The builder appends the table's physical materialized Row ID and Row Commit Version columns
    /// when the connector supplies their logical counterparts.
    pub(super) base_physical_data_schema: SchemaRef,
    /// Physical name of the materialized Row ID column, when configured on the table.
    pub(super) materialized_row_id_column_name: Option<String>,
    /// Physical name of the materialized Row Commit Version column, when configured on the table.
    pub(super) materialized_row_commit_version_column_name: Option<String>,
    /// Whether Row Tracking is enabled and not suspended on the table.
    pub(super) row_tracking_enabled: bool,
    /// Whether IcebergCompatV3 is enabled on the table.
    pub(super) iceberg_compat_v3_enabled: bool,
    pub(super) column_mapping_mode: ColumnMappingMode,
    pub(super) stats_columns: Vec<ColumnName>,
    /// Logical partition column names in metadata-defined order.
    pub(super) logical_partition_columns: Vec<String>,
    pub(super) materialize_partition_columns: bool,
    /// Resolved value of the `delta.randomizeFilePrefixes` table property. When true,
    /// [`BoundWriteContext::write_dir`] emits a random alphanumeric prefix regardless of column
    /// mapping mode.
    pub(super) randomize_file_prefixes: bool,
    /// Resolved value of the `delta.randomPrefixLength` table property. Drives the length
    /// of the random prefix in [`BoundWriteContext::write_dir`] for both the column mapping and
    /// `randomizeFilePrefixes` paths.
    pub(super) random_prefix_length: NonZero<usize>,
}

/// Names of materialized row-tracking id/commit-version columns supplied by a connector.
///
/// See [Row Tracking] in the Delta protocol for details about materialized Row ID and Row Commit
/// Version columns.
///
/// [Row Tracking]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-tracking
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowTrackingMetadataColumns<'a> {
    /// Logical name of the column containing materialized Row IDs, if present in the to-be-written
    /// data.
    pub row_id_col_name: Option<&'a str>,
    /// Logical name of the column containing materialized Row Commit Versions, if present in the
    /// to-be-written data.
    pub row_commit_version_col_name: Option<&'a str>,
}

/// Builds a [`BoundWriteContext`].
#[derive(Debug)]
pub struct BoundWriteContextBuilder {
    write_state: Arc<WriteState>,
    partition_values: Option<HashMap<String, Scalar>>,
    logical_row_id_col_name: Option<String>,
    logical_row_commit_version_col_name: Option<String>,
}

impl BoundWriteContextBuilder {
    /// Binds one typed value for each logical partition column.
    ///
    /// Values are validated and serialized according to the Delta protocol when
    /// [`build`](Self::build) is called, then keyed by physical column name in the returned
    /// context. Null-equivalent values require nullable partition columns.
    ///
    /// Names are matched case-insensitively and normalized to schema case. The map must contain
    /// every partition column and no other keys.
    pub fn with_partition_values(mut self, partition_values: HashMap<String, Scalar>) -> Self {
        self.partition_values = Some(partition_values);
        self
    }

    /// Specifies which columns contain materialized Row IDs and Row Commit Versions in the
    /// to-be-written logical data.
    pub fn with_row_tracking_columns(
        mut self,
        row_tracking_columns: RowTrackingMetadataColumns<'_>,
    ) -> Self {
        self.logical_row_id_col_name = row_tracking_columns.row_id_col_name.map(str::to_string);
        self.logical_row_commit_version_col_name = row_tracking_columns
            .row_commit_version_col_name
            .map(str::to_string);
        self
    }

    /// Builds the write context.
    ///
    /// Returns an error if:
    ///
    /// - Partition values are present for an unpartitioned table, absent for a partitioned table,
    ///   or invalid.
    /// - The connector specifies a column containing materialized Row IDs or Row Commit Versions
    ///   when Row Tracking is not enabled.
    /// - The connector specifies a column containing materialized Row IDs or Row Commit Versions
    ///   for an IcebergCompatV3 table, which Kernel currently does not support writing
    ///   (TODO(#2492)).
    /// - The connector specifies a row-tracking metadata column, but its materialized column name
    ///   is not in the table properties.
    /// - The connector specifies a row-tracking metadata column that conflicts with another logical
    ///   field.
    pub fn build(self) -> DeltaResult<BoundWriteContext> {
        let is_partitioned = !self.write_state.logical_partition_columns.is_empty();
        require!(
            is_partitioned || self.partition_values.is_none(),
            Error::invalid_partition_values(
                "table is not partitioned; partition values are not allowed"
            )
        );
        require!(
            !is_partitioned || self.partition_values.is_some(),
            Error::invalid_partition_values("table is partitioned; partition values are required")
        );
        let has_row_tracking_columns = self.logical_row_id_col_name.is_some()
            || self.logical_row_commit_version_col_name.is_some();
        require!(
            !has_row_tracking_columns || self.write_state.row_tracking_enabled,
            Error::unsupported(
                "Kernel does not allow writing materialized Row IDs or Row Commit Versions when \
                 Row Tracking is not enabled"
            )
        );
        require!(
            !has_row_tracking_columns || !self.write_state.iceberg_compat_v3_enabled,
            Error::unsupported(
                "Kernel does not support writing materialized Row IDs or Row Commit Versions to \
                 IcebergCompatV3 tables"
            )
        );
        let logical_data_schema = self.build_logical_data_schema()?;
        let physical_data_schema = self.build_physical_data_schema()?;

        let normalized = self
            .partition_values
            .map(|partition_values| {
                validate_partition_values(
                    &self.write_state.logical_partition_columns,
                    &self.write_state.full_logical_schema,
                    partition_values,
                )
            })
            .transpose()?;

        let mut serialized = HashMap::with_capacity(normalized.as_ref().map_or(0, HashMap::len));
        if let Some(normalized) = &normalized {
            for logical_name in &self.write_state.logical_partition_columns {
                let scalar = normalized.get(logical_name).ok_or_else(|| {
                    Error::internal_error(format!(
                        "partition column '{logical_name}' missing after validation"
                    ))
                })?;
                let value = serialize_partition_value(scalar)?;
                let physical_name = self
                    .write_state
                    .full_logical_schema
                    .field(logical_name)
                    .ok_or_else(|| {
                        Error::internal_error(format!(
                            "partition column '{logical_name}' not found in schema after validation"
                        ))
                    })?
                    .physical_name(self.write_state.column_mapping_mode)
                    .to_string();
                serialized.insert(physical_name, value);
            }
        }
        let logical_to_physical = Arc::new(
            self.write_state
                .generate_logical_to_physical(normalized.as_ref())?,
        );

        Ok(BoundWriteContext {
            write_state: self.write_state,
            logical_data_schema,
            physical_data_schema,
            logical_to_physical,
            physical_partition_values: serialized,
        })
    }

    fn build_logical_data_schema(&self) -> DeltaResult<SchemaRef> {
        if self.logical_row_id_col_name.is_none()
            && self.logical_row_commit_version_col_name.is_none()
        {
            return Ok(self.write_state.base_logical_data_schema.clone());
        }
        let mut fields: Vec<_> = self
            .write_state
            .base_logical_data_schema
            .fields()
            .cloned()
            .collect();
        if let Some(logical_name) = self.logical_row_id_col_name.as_deref() {
            fields.push(StructField::nullable(logical_name, DataType::LONG));
        }
        if let Some(logical_name) = self.logical_row_commit_version_col_name.as_deref() {
            fields.push(StructField::nullable(logical_name, DataType::LONG));
        }
        Ok(Arc::new(StructType::try_new(fields)?))
    }

    fn build_physical_data_schema(&self) -> DeltaResult<SchemaRef> {
        if self.logical_row_id_col_name.is_none()
            && self.logical_row_commit_version_col_name.is_none()
        {
            return Ok(self.write_state.base_physical_data_schema.clone());
        }
        let mut fields: Vec<_> = self
            .write_state
            .base_physical_data_schema
            .fields()
            .cloned()
            .collect();
        fields.extend(Self::build_physical_row_tracking_field(
            self.logical_row_id_col_name.as_deref(),
            self.write_state.materialized_row_id_column_name.as_deref(),
            MATERIALIZED_ROW_ID_COLUMN_NAME,
        )?);
        fields.extend(Self::build_physical_row_tracking_field(
            self.logical_row_commit_version_col_name.as_deref(),
            self.write_state
                .materialized_row_commit_version_column_name
                .as_deref(),
            MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
        )?);
        Ok(Arc::new(StructType::try_new(fields)?))
    }

    fn build_physical_row_tracking_field(
        logical_name: Option<&str>,
        physical_name: Option<&str>,
        configuration_key: &str,
    ) -> DeltaResult<Option<StructField>> {
        if logical_name.is_none() {
            return Ok(None);
        }
        let physical_name = physical_name.ok_or_else(|| {
            Error::invalid_protocol(format!(
                "The table has Row Tracking enabled, but {configuration_key} is missing from its \
                 metadata configuration"
            ))
        })?;
        Ok(Some(StructField::nullable(physical_name, DataType::LONG)))
    }
}

#[derive(Serialize)]
struct WriteStateWire<'a> {
    version: u32,
    write_state: &'a WriteState,
}

#[derive(Deserialize)]
struct DecodedWriteStateWire {
    version: u32,
    write_state: WriteState,
}

impl WriteState {
    /// Creates a builder for a write context.
    ///
    /// For an unpartitioned table, call [`BoundWriteContextBuilder::build`] directly. For a
    /// partitioned table, call [`BoundWriteContextBuilder::with_partition_values`] first.
    pub fn write_context_builder(self: &Arc<Self>) -> BoundWriteContextBuilder {
        BoundWriteContextBuilder {
            write_state: Arc::clone(self),
            partition_values: None,
            logical_row_id_col_name: None,
            logical_row_commit_version_col_name: None,
        }
    }

    /// Encodes this write state as opaque, versioned JSON bytes for transport.
    ///
    /// The bytes are tied to this delta-kernel version. Do not inspect them or persist them across
    /// kernel upgrades.
    ///
    /// Returns an error if any field cannot be serialized.
    pub fn encode(&self) -> DeltaResult<Vec<u8>> {
        Ok(serde_json::to_vec(&WriteStateWire {
            version: WRITE_STATE_FORMAT_VERSION,
            write_state: self,
        })?)
    }

    /// Decodes shared write state from JSON bytes produced by [`encode`](Self::encode).
    ///
    /// The bytes must use the current write-state format version. Cross-version decoding is not
    /// supported.
    ///
    /// Returns an error if the bytes contain an unsupported format version or do not contain a
    /// valid serialized write state.
    pub fn decode(bytes: &[u8]) -> DeltaResult<Arc<Self>> {
        let wire: DecodedWriteStateWire = serde_json::from_slice(bytes)?;
        require!(
            wire.version == WRITE_STATE_FORMAT_VERSION,
            Error::generic(format!(
                "unsupported write state format version {}; expected {}",
                wire.version, WRITE_STATE_FORMAT_VERSION
            ))
        );
        Ok(Arc::new(wire.write_state))
    }

    pub(super) fn new(table_config: &TableConfiguration, stats_columns: Vec<ColumnName>) -> Self {
        let props = table_config.table_properties();
        Self {
            table_root: table_config.table_root().clone(),
            full_logical_schema: table_config.logical_schema(),
            base_logical_data_schema: table_config.logical_schema_without_partition_columns(),
            base_physical_data_schema: table_config.physical_write_schema(),
            materialized_row_id_column_name: props.materialized_row_id_column_name.clone(),
            materialized_row_commit_version_column_name: props
                .materialized_row_commit_version_column_name
                .clone(),
            row_tracking_enabled: table_config.is_feature_enabled(&TableFeature::RowTracking),
            iceberg_compat_v3_enabled: table_config
                .is_feature_enabled(&TableFeature::IcebergCompatV3),
            column_mapping_mode: table_config.column_mapping_mode(),
            stats_columns,
            logical_partition_columns: table_config.logical_partition_columns().to_vec(),
            materialize_partition_columns: table_config.should_materialize_partition_columns(),
            randomize_file_prefixes: props.should_randomize_file_prefixes(),
            random_prefix_length: props.random_prefix_length(),
        }
    }

    fn generate_logical_to_physical(
        &self,
        partition_values: Option<&HashMap<String, Scalar>>,
    ) -> DeltaResult<Expression> {
        let mut patch = ExpressionStructPatchBuilder::new();
        if self.materialize_partition_columns {
            let partition_cols: HashSet<&str> = self
                .logical_partition_columns
                .iter()
                .map(String::as_str)
                .collect();
            let mut predecessor: Option<&str> = None;
            for field in self.full_logical_schema.fields() {
                let name = field.name().as_str();
                if partition_cols.contains(name) {
                    let value = partition_values
                        .and_then(|values| values.get(name))
                        .ok_or_else(|| {
                            Error::internal_error(format!(
                                "partition column '{name}' missing while building \
                                 logical-to-physical expression"
                            ))
                        })?;
                    let literal = lit(value.clone());
                    patch = match predecessor {
                        Some(predecessor) => patch.insert_after(predecessor, literal),
                        None => patch.prepend(literal),
                    };
                } else if *field.data_type() != DataType::VOID {
                    predecessor = Some(name);
                }
            }
        }
        let patch = add_void_stripping(patch, &self.full_logical_schema);
        Expression::struct_patch(patch)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZero;
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::committer::FileSystemCommitter;
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::schema::schema_ref;
    use crate::transaction::create_table::create_table;
    use crate::transaction::data_layout::DataLayout;
    use crate::Engine;

    fn partitioned_write_state(
        column_mapping_mode: ColumnMappingMode,
        materialize_partition_columns: bool,
        randomize_file_prefixes: bool,
        random_prefix_length: usize,
        row_tracking_enabled: bool,
    ) -> Arc<WriteState> {
        let mut properties = HashMap::new();
        if column_mapping_mode != ColumnMappingMode::None {
            let column_mapping_mode = match column_mapping_mode {
                ColumnMappingMode::None => "none",
                ColumnMappingMode::Name => "name",
                ColumnMappingMode::Id => "id",
            };
            properties.insert(
                "delta.columnMapping.mode".to_string(),
                column_mapping_mode.to_string(),
            );
        }
        if materialize_partition_columns {
            properties.insert(
                "delta.feature.materializePartitionColumns".to_string(),
                "supported".to_string(),
            );
        }

        let engine: Arc<dyn Engine> =
            Arc::new(SyncEngine::new_with_store(Arc::new(InMemory::new())));
        let txn = create_table(
            "memory:///table",
            schema_ref! {
                not_null "year": INTEGER,
                nullable "value": INTEGER,
            },
            "test",
        )
        .with_data_layout(DataLayout::partitioned(["year"]))
        .with_table_properties(properties)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap();

        let mut write_state = txn.write_state().unwrap();
        let state = Arc::get_mut(&mut write_state).unwrap();
        state.randomize_file_prefixes = randomize_file_prefixes;
        state.random_prefix_length = NonZero::new(random_prefix_length).unwrap();
        state.row_tracking_enabled = row_tracking_enabled;
        write_state
    }

    #[rstest]
    #[case::default(ColumnMappingMode::None, false, false, 2, false, false, false)]
    #[case::column_mapping(ColumnMappingMode::Name, false, false, 7, false, false, true)]
    #[case::materialized_partition(ColumnMappingMode::None, true, false, 2, false, false, false)]
    #[case::randomized_prefix(ColumnMappingMode::None, false, true, 7, false, false, true)]
    #[case::row_tracking(ColumnMappingMode::None, false, false, 2, true, false, false)]
    #[case::row_tracking_and_iceberg_compat_v3(
        ColumnMappingMode::Name,
        false,
        false,
        2,
        true,
        true,
        true
    )]
    fn write_state_json_round_trip_preserves_worker_behavior(
        #[case] column_mapping_mode: ColumnMappingMode,
        #[case] materialize_partition_columns: bool,
        #[case] randomize_file_prefixes: bool,
        #[case] random_prefix_length: usize,
        #[case] row_tracking_enabled: bool,
        #[case] iceberg_compat_v3_enabled: bool,
        #[case] expect_random_prefix: bool,
    ) {
        let mut original = partitioned_write_state(
            column_mapping_mode,
            materialize_partition_columns,
            randomize_file_prefixes,
            random_prefix_length,
            row_tracking_enabled,
        );
        let state = Arc::get_mut(&mut original).unwrap();
        state.iceberg_compat_v3_enabled = iceberg_compat_v3_enabled;
        let encoded = original.encode().unwrap();
        let decoded = WriteState::decode(&encoded).unwrap();
        assert_eq!(decoded.full_logical_schema, original.full_logical_schema);
        assert_eq!(
            decoded.base_logical_data_schema,
            original.base_logical_data_schema
        );
        assert_eq!(
            decoded.materialized_row_id_column_name,
            original.materialized_row_id_column_name
        );
        assert_eq!(
            decoded.materialized_row_commit_version_column_name,
            original.materialized_row_commit_version_column_name
        );
        assert_eq!(decoded.row_tracking_enabled, original.row_tracking_enabled);
        assert_eq!(
            decoded.iceberg_compat_v3_enabled,
            original.iceberg_compat_v3_enabled
        );

        let values = || HashMap::from([("year".to_string(), Scalar::Integer(2024))]);
        let original_context = original
            .write_context_builder()
            .with_partition_values(values())
            .build()
            .unwrap();
        let decoded_context = decoded
            .write_context_builder()
            .with_partition_values(values())
            .build()
            .unwrap();

        assert!(Arc::ptr_eq(&original, &original_context.write_state));
        assert!(Arc::ptr_eq(&decoded, &decoded_context.write_state));
        assert_eq!(
            decoded_context.table_root_dir(),
            original_context.table_root_dir()
        );
        assert_eq!(
            decoded_context.logical_data_schema(),
            original_context.logical_data_schema()
        );
        assert_eq!(
            decoded_context.physical_data_schema(),
            original_context.physical_data_schema()
        );
        assert_eq!(
            decoded_context.stats_columns(),
            original_context.stats_columns()
        );
        assert_eq!(
            decoded_context.physical_partition_values(),
            original_context.physical_partition_values()
        );
        assert_eq!(
            decoded_context.logical_to_physical(),
            original_context.logical_to_physical()
        );
        assert_eq!(decoded_context.column_mapping_mode(), column_mapping_mode);
        let expected_partition_key = original
            .full_logical_schema
            .field("year")
            .unwrap()
            .physical_name(column_mapping_mode);
        assert_eq!(
            decoded_context.physical_partition_values(),
            &HashMap::from([(expected_partition_key.into(), Some("2024".into()))])
        );

        let write_dir = decoded_context.write_dir().path().to_string();
        if expect_random_prefix {
            let prefix = write_dir
                .strip_prefix("/table/")
                .unwrap()
                .strip_suffix('/')
                .unwrap();
            assert_eq!(prefix.len(), random_prefix_length);
            assert!(prefix
                .chars()
                .all(|character| character.is_ascii_alphanumeric()));
        } else {
            assert_eq!(write_dir, "/table/year=2024/");
        }
    }

    #[rstest]
    #[case::both(RowTrackingMetadataColumns {
        row_id_col_name: Some("connector_row_id"),
        row_commit_version_col_name: Some("connector_row_commit_version"),
    })]
    #[case::row_id_only(RowTrackingMetadataColumns {
        row_id_col_name: Some("connector_row_id"),
        row_commit_version_col_name: None,
    })]
    #[case::row_commit_version_only(RowTrackingMetadataColumns {
        row_id_col_name: None,
        row_commit_version_col_name: Some("connector_row_commit_version"),
    })]
    fn build_write_context_with_row_tracking_columns(
        #[case] row_tracking_columns: RowTrackingMetadataColumns<'_>,
        #[values(
            ColumnMappingMode::None,
            ColumnMappingMode::Name,
            ColumnMappingMode::Id
        )]
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<()> {
        let mut write_state = partitioned_write_state(
            column_mapping_mode,
            false, /* materialize_partition_columns */
            false, /* randomize_file_prefixes */
            2,     /* random_prefix_length */
            true,  /* row_tracking_enabled */
        );
        let state = Arc::get_mut(&mut write_state).unwrap();
        state.materialized_row_id_column_name = Some("_metadata_row_id".into());
        state.materialized_row_commit_version_column_name =
            Some("_metadata_row_commit_version".into());

        let write_state = WriteState::decode(&write_state.encode()?)?;
        let base_logical_field = write_state
            .base_logical_data_schema
            .fields()
            .next()
            .unwrap()
            .clone();
        let base_physical_field = write_state
            .base_physical_data_schema
            .fields()
            .next()
            .unwrap()
            .clone();
        let write_context = write_state
            .write_context_builder()
            .with_partition_values(HashMap::from([("year".to_string(), Scalar::Integer(2024))]))
            .with_row_tracking_columns(row_tracking_columns)
            .build()?;

        let mut expected_logical_fields = vec![base_logical_field];
        let mut expected_physical_fields = vec![base_physical_field];
        if let Some(row_id_name) = row_tracking_columns.row_id_col_name {
            expected_logical_fields.push(StructField::nullable(row_id_name, DataType::LONG));
            expected_physical_fields
                .push(StructField::nullable("_metadata_row_id", DataType::LONG));
        }
        if let Some(row_commit_version_name) = row_tracking_columns.row_commit_version_col_name {
            expected_logical_fields.push(StructField::nullable(
                row_commit_version_name,
                DataType::LONG,
            ));
            expected_physical_fields.push(StructField::nullable(
                "_metadata_row_commit_version",
                DataType::LONG,
            ));
        }

        assert_eq!(
            write_context.logical_data_schema(),
            &Arc::new(StructType::try_new(expected_logical_fields)?)
        );
        assert_eq!(
            write_context.physical_data_schema(),
            &Arc::new(StructType::try_new(expected_physical_fields)?)
        );
        Ok(())
    }

    #[rstest]
    #[case::row_id(
        RowTrackingMetadataColumns {
            row_id_col_name: Some("connector_row_id"),
            row_commit_version_col_name: None,
        },
        "delta.rowTracking.materializedRowIdColumnName",
    )]
    #[case::row_commit_version(
        RowTrackingMetadataColumns {
            row_id_col_name: None,
            row_commit_version_col_name: Some("connector_row_commit_version"),
        },
        "delta.rowTracking.materializedRowCommitVersionColumnName",
    )]
    fn write_context_rejects_row_tracking_column_without_physical_name(
        #[case] row_tracking_columns: RowTrackingMetadataColumns<'_>,
        #[case] expected_error: &str,
    ) {
        let write_state = partitioned_write_state(
            ColumnMappingMode::None,
            false, /* materialize_partition_columns */
            false, /* randomize_file_prefixes */
            2,     /* random_prefix_length */
            true,  /* row_tracking_enabled */
        );

        let error = write_state
            .write_context_builder()
            .with_partition_values(HashMap::from([("year".to_string(), Scalar::Integer(2024))]))
            .with_row_tracking_columns(row_tracking_columns)
            .build()
            .unwrap_err();
        assert!(error.to_string().contains(expected_error));
    }

    #[rstest]
    #[case::existing_data_column("VaLuE", "connector_row_commit_version")]
    #[case::id_version_same_name("tracking", "TRACKING")]
    fn write_context_rejects_duplicate_logical_row_tracking_names(
        #[case] row_id_name: &str,
        #[case] row_commit_version_name: &str,
    ) {
        let mut write_state = partitioned_write_state(
            ColumnMappingMode::None,
            false, /* materialize_partition_columns */
            false, /* randomize_file_prefixes */
            2,     /* random_prefix_length */
            true,  /* row_tracking_enabled */
        );
        let state = Arc::get_mut(&mut write_state).unwrap();
        state.materialized_row_id_column_name = Some("_metadata_row_id".into());
        state.materialized_row_commit_version_column_name =
            Some("_metadata_row_commit_version".into());

        let error = write_state
            .write_context_builder()
            .with_partition_values(HashMap::from([("year".to_string(), Scalar::Integer(2024))]))
            .with_row_tracking_columns(RowTrackingMetadataColumns {
                row_id_col_name: Some(row_id_name),
                row_commit_version_col_name: Some(row_commit_version_name),
            })
            .build()
            .unwrap_err();
        assert!(error.to_string().to_ascii_lowercase().contains("duplicate"));
    }

    #[test]
    fn write_state_decode_rejects_malformed_json() {
        let error = WriteState::decode(b"not valid json").unwrap_err();
        assert!(error.to_string().contains("expected ident"));
    }

    #[test]
    fn write_state_encoding_uses_current_format_version() {
        let state = partitioned_write_state(
            ColumnMappingMode::None,
            false, /* materialize_partition_columns */
            false, /* randomize_file_prefixes */
            2,     /* random_prefix_length */
            false, /* row_tracking_enabled */
        );
        let encoded: serde_json::Value = serde_json::from_slice(&state.encode().unwrap()).unwrap();
        assert_eq!(encoded["version"], 1);
        assert!(encoded.get("write_state").is_some());
    }

    #[test]
    fn write_state_decode_rejects_unsupported_format_version() {
        let state = partitioned_write_state(
            ColumnMappingMode::None,
            false, /* materialize_partition_columns */
            false, /* randomize_file_prefixes */
            2,     /* random_prefix_length */
            false, /* row_tracking_enabled */
        );
        let mut encoded: serde_json::Value =
            serde_json::from_slice(&state.encode().unwrap()).unwrap();
        encoded["version"] = 2.into();

        let error = WriteState::decode(&serde_json::to_vec(&encoded).unwrap()).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported write state format version 2; expected 1"));
    }
}
