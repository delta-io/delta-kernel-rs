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
use crate::schema::SchemaRef;
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
    pub(super) fn new(table_config: &TableConfiguration, stats_columns: Vec<ColumnName>) -> Self {
        let props = table_config.table_properties();
        Self {
            table_root: table_config.table_root().clone(),
            full_logical_schema: table_config.logical_schema(),
            logical_schema: table_config.logical_schema_without_partition_columns(),
            physical_schema: table_config.physical_write_schema(),
            column_mapping_mode: table_config.column_mapping_mode(),
            stats_columns,
            logical_partition_columns: table_config.logical_partition_columns().to_vec(),
            materialize_partition_columns: table_config.should_materialize_partition_columns(),
            randomize_file_prefixes: props.should_randomize_file_prefixes(),
            random_prefix_length: props.random_prefix_length(),
        }
    }

    /// Creates a write context for writing data to a specific partition.
    ///
    /// The supplied map must contain exactly one entry for every logical partition column. Keys
    /// are matched case-insensitively and values are validated against the table schema before
    /// being serialized according to the Delta partition-value rules.
    ///
    /// Returns an error if the table is not partitioned or if the partition keys or values are
    /// invalid.
    pub fn partitioned_write_context(
        self: &Arc<Self>,
        partition_values: HashMap<String, Scalar>,
    ) -> DeltaResult<BoundWriteContext> {
        require!(
            !self.logical_partition_columns.is_empty(),
            Error::generic("table is not partitioned; use unpartitioned_write_context() instead")
        );
        let normalized = validate_partition_values(
            &self.logical_partition_columns,
            &self.full_logical_schema,
            partition_values,
        )?;

        let mut serialized = HashMap::with_capacity(normalized.len());
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
        let logical_to_physical = Arc::new(self.generate_logical_to_physical(Some(&normalized))?);

        Ok(BoundWriteContext {
            shared: self.clone(),
            logical_to_physical,
            physical_partition_values: serialized,
        })
    }

    /// Creates a write context for writing data to an unpartitioned table.
    ///
    /// Returns an error if the table has partition columns.
    pub fn unpartitioned_write_context(self: &Arc<Self>) -> DeltaResult<BoundWriteContext> {
        require!(
            self.logical_partition_columns.is_empty(),
            Error::generic("table is partitioned; use partitioned_write_context() instead")
        );
        let logical_to_physical = Arc::new(self.generate_logical_to_physical(None)?);
        Ok(BoundWriteContext {
            shared: self.clone(),
            logical_to_physical,
            physical_partition_values: HashMap::new(),
        })
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

    /// Decodes a write state from JSON bytes produced by [`encode`](Self::encode).
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

    use rstest::rstest;

    use super::*;
    use crate::schema::{schema_ref, ColumnMetadataKey, MetadataValue, StructField, StructType};

    fn partitioned_write_state(
        column_mapping_mode: ColumnMappingMode,
        materialize_partition_columns: bool,
        randomize_file_prefixes: bool,
        random_prefix_length: usize,
    ) -> Arc<WriteState> {
        let year = StructField::not_null("year", DataType::INTEGER).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(1),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("phys_year".into()),
            ),
        ]);
        let value = StructField::nullable("value", DataType::INTEGER).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(2),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("phys_value".into()),
            ),
        ]);
        Arc::new(WriteState {
            table_root: Url::parse("s3://bucket/table/").unwrap(),
            full_logical_schema: Arc::new(StructType::new_unchecked([year, value])),
            logical_schema: schema_ref! { nullable "value": INTEGER },
            physical_schema: schema_ref! { nullable "value": INTEGER },
            column_mapping_mode,
            stats_columns: vec![ColumnName::new(["value"])],
            logical_partition_columns: vec!["year".into()],
            materialize_partition_columns,
            randomize_file_prefixes,
            random_prefix_length: NonZero::new(random_prefix_length).unwrap(),
        })
    }

    #[rstest]
    #[case::default(ColumnMappingMode::None, false, false, 2, "year", false)]
    #[case::column_mapping(ColumnMappingMode::Name, false, false, 7, "phys_year", true)]
    #[case::materialized_partition(ColumnMappingMode::None, true, false, 2, "year", false)]
    #[case::randomized_prefix(ColumnMappingMode::None, false, true, 7, "year", true)]
    fn write_state_json_round_trip_preserves_worker_behavior(
        #[case] column_mapping_mode: ColumnMappingMode,
        #[case] materialize_partition_columns: bool,
        #[case] randomize_file_prefixes: bool,
        #[case] random_prefix_length: usize,
        #[case] expected_partition_key: &str,
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
        let original_context = original.partitioned_write_context(values()).unwrap();
        let decoded_context = decoded.partitioned_write_context(values()).unwrap();

        assert_eq!(
            decoded_context.table_root_dir(),
            original_context.table_root_dir()
        );
        assert_eq!(
            decoded_context.logical_schema(),
            original_context.logical_schema()
        );
        assert_eq!(
            decoded_context.physical_schema(),
            original_context.physical_schema()
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

    #[test]
    fn write_state_contexts_share_table_wide_state() {
        let state = partitioned_write_state(ColumnMappingMode::None, false, false, 2);
        let values = || HashMap::from([("year".to_string(), Scalar::Integer(2024))]);

        let first = state.partitioned_write_context(values()).unwrap();
        let second = state.partitioned_write_context(values()).unwrap();

        assert!(Arc::ptr_eq(&state, &first.shared));
        assert!(Arc::ptr_eq(&first.shared, &second.shared));
    }
}
