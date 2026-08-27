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
use crate::schema::{MetadataColumnSpec, SchemaRef, StructField};
use crate::table_configuration::TableConfiguration;
use crate::table_features::ColumnMappingMode;
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
    /// Logical schema accepted from the writer, with partition columns removed.
    ///
    /// Connectors write one partition at a time, so partition values are bound separately rather
    /// than appearing in each input data batch.
    pub(super) logical_schema: SchemaRef,
    /// Physical schema expected in the written Parquet file.
    ///
    /// This differs from both logical schemas when column mapping, void stripping, or partition
    /// materialization changes the data passed to the Parquet writer.
    pub(super) physical_schema: SchemaRef,
    pub(super) column_mapping_mode: ColumnMappingMode,
    pub(super) stats_columns: Vec<ColumnName>,
    pub(super) materialized_row_id_field: Option<StructField>,
    pub(super) materialized_row_commit_version_field: Option<StructField>,
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

/// Names row-tracking metadata columns present in the connector's logical write data.
///
/// These are connector-chosen logical names. The write context maps them to the table's configured
/// physical column names and places the row ID before the row commit version when both are present.
/// A `None` field means the logical input omits that value and the physical output omits its
/// materialized column.
///
/// The Delta protocol permits a materialized column to be omitted when all its values would be
/// null. Rewrites that preserve a value differing from the new file's default must provide the
/// corresponding metadata column.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowTrackingMetadataColumns<'a> {
    /// Logical input column whose non-null values preserve stable row IDs.
    pub row_id: Option<&'a str>,
    /// Logical input column whose non-null values preserve stable row commit versions.
    pub row_commit_version: Option<&'a str>,
}

/// Builds a [`BoundWriteContext`] from table-wide write state.
///
/// Use [`with_partition_values`](Self::with_partition_values) for one output partition and
/// [`with_row_tracking_columns`](Self::with_row_tracking_columns) when the logical input contains
/// stable row-tracking values.
#[derive(Debug)]
pub struct WriteContextBuilder<'a> {
    write_state: &'a Arc<WriteState>,
    partition_values: Option<HashMap<String, Scalar>>,
    row_tracking_columns: Option<RowTrackingMetadataColumns<'a>>,
}

impl<'a> WriteContextBuilder<'a> {
    /// Binds one typed value for each logical partition column.
    ///
    /// Names are matched case-insensitively and normalized to schema case. The map must contain
    /// every partition column and no other keys.
    pub fn with_partition_values(mut self, partition_values: HashMap<String, Scalar>) -> Self {
        self.partition_values = Some(partition_values);
        self
    }

    /// Includes row-tracking metadata columns from the logical input data.
    pub fn with_row_tracking_columns(
        mut self,
        row_tracking_columns: RowTrackingMetadataColumns<'a>,
    ) -> Self {
        self.row_tracking_columns = Some(row_tracking_columns);
        self
    }

    /// Builds a write context for the configured partition and logical input columns.
    ///
    /// Returns an error when partition values do not match the table layout, a partitioned table
    /// has no bound values, or row-tracking columns conflict with the table schema or
    /// configuration.
    pub fn build(self) -> DeltaResult<BoundWriteContext> {
        self.write_state
            .build_write_context(self.partition_values, self.row_tracking_columns)
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
    pub fn write_context_builder(self: &Arc<Self>) -> WriteContextBuilder<'_> {
        WriteContextBuilder {
            write_state: self,
            partition_values: None,
            row_tracking_columns: None,
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

    pub(super) fn new(
        table_config: &TableConfiguration,
        stats_columns: Vec<ColumnName>,
    ) -> DeltaResult<Self> {
        let props = table_config.table_properties();
        let (materialized_row_id_field, materialized_row_commit_version_field) = if table_config
            .should_write_row_tracking()
            && props.enable_row_tracking == Some(true)
        {
            let (row_id_name, row_commit_version_name) =
                table_config.materialized_row_tracking_column_names()?;
            (
                Some(StructField::nullable(row_id_name, DataType::LONG)),
                Some(StructField::nullable(
                    row_commit_version_name,
                    DataType::LONG,
                )),
            )
        } else {
            (None, None)
        };
        Ok(Self {
            table_root: table_config.table_root().clone(),
            full_logical_schema: table_config.logical_schema(),
            logical_schema: table_config.logical_schema_without_partition_columns(),
            physical_schema: table_config.physical_write_schema(),
            column_mapping_mode: table_config.column_mapping_mode(),
            stats_columns,
            materialized_row_id_field,
            materialized_row_commit_version_field,
            logical_partition_columns: table_config.logical_partition_columns().to_vec(),
            materialize_partition_columns: table_config.should_materialize_partition_columns(),
            randomize_file_prefixes: props.should_randomize_file_prefixes(),
            random_prefix_length: props.random_prefix_length(),
        })
    }

    fn build_write_context(
        self: &Arc<Self>,
        partition_values: Option<HashMap<String, Scalar>>,
        row_tracking_columns: Option<RowTrackingMetadataColumns<'_>>,
    ) -> DeltaResult<BoundWriteContext> {
        let is_partitioned = !self.logical_partition_columns.is_empty();
        require!(
            is_partitioned || partition_values.is_none(),
            Error::generic("table is not partitioned; partition values are not allowed")
        );
        require!(
            !is_partitioned || partition_values.is_some(),
            Error::generic("table is partitioned; partition values are required")
        );
        let normalized = partition_values
            .map(|partition_values| {
                validate_partition_values(
                    &self.logical_partition_columns,
                    &self.full_logical_schema,
                    partition_values,
                )
            })
            .transpose()?;

        let mut serialized = HashMap::with_capacity(normalized.as_ref().map_or(0, HashMap::len));
        if let Some(normalized) = &normalized {
            for logical_name in &self.logical_partition_columns {
                let scalar = normalized.get(logical_name).ok_or_else(|| {
                    Error::internal_error(format!(
                        "partition column '{logical_name}' missing after validation"
                    ))
                })?;
                let value = serialize_partition_value(scalar)?;
                let physical_name = self
                    .full_logical_schema
                    .field(logical_name)
                    .ok_or_else(|| {
                        Error::internal_error(format!(
                            "partition column '{logical_name}' not found in schema after validation"
                        ))
                    })?
                    .physical_name(self.column_mapping_mode)
                    .to_string();
                serialized.insert(physical_name, value);
            }
        }

        let (logical_data_schema, physical_data_schema) = match row_tracking_columns {
            None => (self.logical_schema.clone(), self.physical_schema.clone()),
            Some(row_tracking_columns) => {
                for input_name in [
                    row_tracking_columns.row_id,
                    row_tracking_columns.row_commit_version,
                ]
                .into_iter()
                .flatten()
                {
                    require!(
                        !self.full_logical_schema.contains(input_name),
                        Error::schema(format!(
                            "row-tracking input column '{input_name}' conflicts with a table column"
                        ))
                    );
                }

                if let (Some(row_id), Some(row_commit_version)) = (
                    row_tracking_columns.row_id,
                    row_tracking_columns.row_commit_version,
                ) {
                    require!(
                        row_id != row_commit_version,
                        Error::schema(
                            "row-tracking logical input columns must have distinct names"
                        )
                    );
                }

                let materialized_row_id_name = row_tracking_columns
                    .row_id
                    .map(|_| {
                        self.materialized_row_id_field.as_ref().ok_or_else(|| {
                            Error::unsupported(
                                "row ID input requires row tracking to be enabled for writes",
                            )
                        })
                    })
                    .transpose()?
                    .map(StructField::name);
                let materialized_row_commit_version_name = row_tracking_columns
                    .row_commit_version
                    .map(|_| {
                        self.materialized_row_commit_version_field
                            .as_ref()
                            .ok_or_else(|| {
                                Error::unsupported(
                                    "row commit version input requires row tracking for writes",
                                )
                            })
                    })
                    .transpose()?
                    .map(StructField::name);

                if let (Some(row_id), Some(row_commit_version)) = (
                    materialized_row_id_name,
                    materialized_row_commit_version_name,
                ) {
                    require!(
                        row_id != row_commit_version,
                        Error::schema("materialized row-tracking fields must have distinct names")
                    );
                }

                let logical_data_schema = self.logical_schema.add(
                    [
                        row_tracking_columns
                            .row_id
                            .map(|name| nullable_metadata_column(name, MetadataColumnSpec::RowId)),
                        row_tracking_columns.row_commit_version.map(|name| {
                            nullable_metadata_column(name, MetadataColumnSpec::RowCommitVersion)
                        }),
                    ]
                    .into_iter()
                    .flatten(),
                )?;
                let physical_data_schema = self.physical_schema.add(
                    [
                        materialized_row_id_name
                            .map(|name| nullable_metadata_column(name, MetadataColumnSpec::RowId)),
                        materialized_row_commit_version_name.map(|name| {
                            nullable_metadata_column(name, MetadataColumnSpec::RowCommitVersion)
                        }),
                    ]
                    .into_iter()
                    .flatten(),
                )?;
                (
                    Arc::new(logical_data_schema),
                    Arc::new(physical_data_schema),
                )
            }
        };

        Ok(BoundWriteContext {
            write_state: Arc::clone(self),
            logical_data_schema,
            logical_to_physical: Arc::new(
                self.generate_logical_to_physical(normalized.as_ref(), row_tracking_columns)?,
            ),
            physical_data_schema,
            physical_partition_values: serialized,
        })
    }

    fn generate_logical_to_physical(
        &self,
        partition_values: Option<&HashMap<String, Scalar>>,
        row_tracking_columns: Option<RowTrackingMetadataColumns<'_>>,
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
        let mut patch = add_void_stripping(patch, &self.full_logical_schema);
        if let Some(row_tracking_columns) = row_tracking_columns {
            if let Some(row_id) = row_tracking_columns.row_id {
                patch = patch.drop(row_id).append(Expression::column([row_id]));
            }
            if let Some(row_commit_version) = row_tracking_columns.row_commit_version {
                patch = patch
                    .drop(row_commit_version)
                    .append(Expression::column([row_commit_version]));
            }
        }
        Expression::struct_patch(patch)
    }
}

fn nullable_metadata_column(name: impl Into<String>, spec: MetadataColumnSpec) -> StructField {
    let mut field = StructField::create_metadata_column(name, spec);
    field.nullable = true;
    field
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
        write_state
    }

    #[rstest]
    #[case::default(ColumnMappingMode::None, false, false, 2, false)]
    #[case::column_mapping(ColumnMappingMode::Name, false, false, 7, true)]
    #[case::materialized_partition(ColumnMappingMode::None, true, false, 2, false)]
    #[case::randomized_prefix(ColumnMappingMode::None, false, true, 7, true)]
    fn write_state_json_round_trip_preserves_worker_behavior(
        #[case] column_mapping_mode: ColumnMappingMode,
        #[case] materialize_partition_columns: bool,
        #[case] randomize_file_prefixes: bool,
        #[case] random_prefix_length: usize,
        #[case] expect_random_prefix: bool,
    ) {
        let original = partitioned_write_state(
            column_mapping_mode,
            materialize_partition_columns,
            randomize_file_prefixes,
            random_prefix_length,
        );
        let encoded = original.encode().unwrap();
        let decoded = WriteState::decode(&encoded).unwrap();
        assert_eq!(decoded.full_logical_schema, original.full_logical_schema);
        assert_eq!(decoded.logical_schema, original.logical_schema);

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

    #[test]
    fn write_state_decode_rejects_malformed_json() {
        let error = WriteState::decode(b"not valid json").unwrap_err();
        assert!(error.to_string().contains("expected ident"));
    }

    #[test]
    fn write_state_encoding_uses_current_format_version() {
        let state = partitioned_write_state(ColumnMappingMode::None, false, false, 2);
        let encoded: serde_json::Value = serde_json::from_slice(&state.encode().unwrap()).unwrap();
        assert_eq!(encoded["version"], 1);
        assert!(encoded.get("write_state").is_some());
    }

    #[test]
    fn write_state_decode_rejects_unsupported_format_version() {
        let state = partitioned_write_state(ColumnMappingMode::None, false, false, 2);
        let mut encoded: serde_json::Value =
            serde_json::from_slice(&state.encode().unwrap()).unwrap();
        encoded["version"] = 2.into();

        let error = WriteState::decode(&serde_json::to_vec(&encoded).unwrap()).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported write state format version 2; expected 1"));
    }
}
