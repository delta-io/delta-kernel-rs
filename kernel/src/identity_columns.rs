//! Support for Concurrent Identity Columns (CIC).
//!
//! A Concurrent Identity Column draws its values from a monotonic sequence hosted by the table's
//! catalog instead of from the Delta-log `delta.identity.highWaterMark`, allowing multiple
//! concurrent writers to generate unique BIGINT identity values without conflicting. A column is
//! concurrent iff its metadata carries `delta.identity.concurrent.sequenceId`; its `start`, `step`,
//! and `allowExplicitInsert` reuse the classic `delta.identity.*` keys.
//!
//! Kernel owns only the Delta protocol; it neither talks to the sequence service, generates
//! identity values, nor inserts them into a batch. A connector discovers the columns it must fill
//! via [`Transaction::concurrent_identity_columns`], reserves ranges from its own client, generates
//! the values (a reserved range enumerates as `range_start + step * i`), fills the columns into its
//! batch, and acknowledges responsibility via [`Transaction::ack_concurrent_identity_columns`].
//! This module provides:
//! - [`IdentityColumnInfo`]: owned metadata about a CIC identity column detected from schema
//! - [`ConcurrentIdentityColumn`]: a borrowed view of a CIC column, surfaced on a transaction
//! - [`detect_identity_columns`]: scans a schema for CIC identity columns
//! - [`cic_column`]: stamps the CIC metadata onto a schema field at CREATE-table time
//!
//! [`Transaction::concurrent_identity_columns`]: crate::transaction::Transaction::concurrent_identity_columns
//! [`Transaction::ack_concurrent_identity_columns`]: crate::transaction::Transaction::ack_concurrent_identity_columns

use crate::schema::{
    ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
use crate::{DeltaResult, Error};

/// Information about a CIC identity column detected from schema metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct IdentityColumnInfo {
    /// The logical column name.
    pub column_name: String,
    /// The UC Sequence Service sequence ID for this column.
    pub sequence_id: String,
    /// The start value for identity generation.
    pub start: i64,
    /// The step (increment) value for identity generation.
    pub step: i64,
    /// Whether explicit inserts are allowed for this column.
    ///
    /// Parsed from the classic `delta.identity.allowExplicitInsert` key and surfaced for callers
    /// that inspect column metadata, but **not yet enforced**: the connector always generates
    /// identity values today, and [`cic_column`] cannot stamp this key.
    pub allow_explicit_insert: bool,
}

/// A borrowed view of a Concurrent Identity Column, surfaced by
/// [`Transaction::concurrent_identity_columns`](crate::transaction::Transaction::concurrent_identity_columns).
///
/// A connector reserves values from its sequence service for [`Self::sequence_id`], generates them
/// from the reserved range (`range_start + step * i`), fills the [`Self::column_name`] column into
/// its batch itself, and then acknowledges via
/// [`Transaction::ack_concurrent_identity_columns`](crate::transaction::Transaction::ack_concurrent_identity_columns).
/// Kernel neither reserves, generates, nor inserts values.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcurrentIdentityColumn<'a> {
    column_name: &'a str,
    sequence_id: &'a str,
    start: i64,
    step: i64,
    allow_explicit_insert: bool,
}

impl<'a> ConcurrentIdentityColumn<'a> {
    /// The logical column name the connector must fill.
    pub fn column_name(&self) -> &'a str {
        self.column_name
    }

    /// The UC sequence id that issues this column's values.
    pub fn sequence_id(&self) -> &'a str {
        self.sequence_id
    }

    /// The first value the sequence issues.
    pub fn start(&self) -> i64 {
        self.start
    }

    /// The increment between successive values (non-zero).
    pub fn step(&self) -> i64 {
        self.step
    }

    /// Whether user-supplied values are permitted. Parsed from metadata but not yet enforced.
    pub fn allow_explicit_insert(&self) -> bool {
        self.allow_explicit_insert
    }
}

/// Builds a CIC identity column field with the sequence-id marker plus the classic `start`/`step`
/// metadata keys stamped on it.
///
/// Engines call this after minting a `sequence_id`. The returned [`StructField`] is a non-nullable
/// `LONG` column ready to be passed to `create_table`.
pub fn cic_column(
    name: impl Into<String>,
    sequence_id: impl Into<String>,
    start: i64,
    step: i64,
) -> StructField {
    StructField::new(name, DataType::LONG, false).with_metadata(vec![
        (
            ColumnMetadataKey::IdentityConcurrentSequenceId
                .as_ref()
                .to_string(),
            MetadataValue::String(sequence_id.into()),
        ),
        (
            ColumnMetadataKey::IdentityStart.as_ref().to_string(),
            MetadataValue::Number(start),
        ),
        (
            ColumnMetadataKey::IdentityStep.as_ref().to_string(),
            MetadataValue::Number(step),
        ),
    ])
}

/// Scans the (top-level) schema for Concurrent Identity Columns (CIC).
///
/// A column is a CIC if it has the `delta.identity.concurrent.sequenceId` metadata key. When that
/// key is present, the classic `delta.identity.start` and `delta.identity.step` keys are also
/// required.
///
/// Returns owned [`IdentityColumnInfo`] for each detected CIC, or an error if required metadata is
/// missing or malformed. `Transaction::concurrent_identity_columns` exposes a borrowed-view
/// equivalent.
pub fn detect_identity_columns(schema: &SchemaRef) -> DeltaResult<Vec<IdentityColumnInfo>> {
    let mut result = Vec::new();
    for field in schema.fields() {
        if field
            .get_config_value(&ColumnMetadataKey::IdentityConcurrentSequenceId)
            .is_none()
        {
            continue;
        }
        result.push(IdentityColumnInfo {
            column_name: field.name().to_string(),
            sequence_id: get_required_string(
                field,
                &ColumnMetadataKey::IdentityConcurrentSequenceId,
            )?,
            start: get_required_i64(field, &ColumnMetadataKey::IdentityStart)?,
            step: get_required_i64(field, &ColumnMetadataKey::IdentityStep)?,
            allow_explicit_insert: parse_allow_explicit_insert(field)?,
        });
    }
    Ok(result)
}

/// Borrowed-view equivalent of [`detect_identity_columns`] over the top-level fields of `schema`.
///
/// Powers [`Transaction::concurrent_identity_columns`](crate::transaction::Transaction::concurrent_identity_columns);
/// each returned [`ConcurrentIdentityColumn`] borrows its names from `schema` rather than cloning.
pub(crate) fn concurrent_identity_columns(
    schema: &StructType,
) -> DeltaResult<Vec<ConcurrentIdentityColumn<'_>>> {
    let mut result = Vec::new();
    for field in schema.fields() {
        if field
            .get_config_value(&ColumnMetadataKey::IdentityConcurrentSequenceId)
            .is_none()
        {
            continue;
        }
        result.push(ConcurrentIdentityColumn {
            column_name: field.name(),
            sequence_id: get_required_str(field, &ColumnMetadataKey::IdentityConcurrentSequenceId)?,
            start: get_required_i64(field, &ColumnMetadataKey::IdentityStart)?,
            step: get_required_i64(field, &ColumnMetadataKey::IdentityStep)?,
            allow_explicit_insert: parse_allow_explicit_insert(field)?,
        });
    }
    Ok(result)
}

/// Validates every top-level Concurrent Identity Column in `schema`, returning whether any exist.
///
/// Shared by the CREATE and ALTER paths. Each top-level CIC column must be a non-nullable `LONG`
/// with a non-zero step, must not also carry `delta.identity.highWaterMark` (a sequence id and a
/// high-water mark are mutually exclusive), and must not be a partition column. CIC is only
/// supported at the top level, so CIC metadata found on any nested field is rejected.
///
/// # Errors
///
/// Returns an error describing the first violation, or malformed CIC metadata (see
/// [`detect_identity_columns`]).
pub(crate) fn validate_cic_columns(
    schema: &SchemaRef,
    partition_columns: &[String],
) -> DeltaResult<bool> {
    let identity_cols = detect_identity_columns(schema)?;
    for info in &identity_cols {
        // Present by construction: `detect_identity_columns` found it in this schema.
        let field = schema.field(&info.column_name).ok_or_else(|| {
            Error::generic(format!(
                "Identity column '{}' detected but not found in schema",
                info.column_name
            ))
        })?;
        if field.data_type() != &DataType::LONG {
            return Err(Error::generic(format!(
                "Identity column '{}' must be of type LONG, got {}",
                info.column_name,
                field.data_type()
            )));
        }
        if field.is_nullable() {
            return Err(Error::generic(format!(
                "Identity column '{}' must be non-nullable",
                info.column_name
            )));
        }
        if info.step == 0 {
            return Err(Error::generic(format!(
                "Identity column '{}' has step 0, which is not allowed",
                info.column_name,
            )));
        }
        // A sequence id and a high-water mark are mutually exclusive (RFC): the value is either
        // allocated from the sequence or derived from the mark, never both.
        if field
            .get_config_value(&ColumnMetadataKey::IdentityHighWaterMark)
            .is_some()
        {
            return Err(Error::generic(format!(
                "Identity column '{}' carries both a concurrent sequence id and a \
                 '{}'; these are mutually exclusive.",
                info.column_name,
                ColumnMetadataKey::IdentityHighWaterMark.as_ref(),
            )));
        }
        if partition_columns
            .iter()
            .any(|p| p.eq_ignore_ascii_case(&info.column_name))
        {
            return Err(Error::generic(format!(
                "Identity column '{}' cannot also be a partition column",
                info.column_name
            )));
        }
    }
    // CIC is only supported at the top level; reject the metadata anywhere below it.
    for field in schema.fields() {
        reject_nested_cic(field.data_type())?;
    }
    Ok(!identity_cols.is_empty())
}

/// Returns true if any top-level field carries a classic `delta.identity.highWaterMark`.
pub(crate) fn schema_has_high_water_mark(schema: &StructType) -> bool {
    schema.fields().any(|field| {
        field
            .get_config_value(&ColumnMetadataKey::IdentityHighWaterMark)
            .is_some()
    })
}

/// Rejects CIC `sequenceId` metadata found on any field nested inside `data_type`.
fn reject_nested_cic(data_type: &DataType) -> DeltaResult<()> {
    match data_type {
        DataType::Struct(fields) => {
            for field in fields.fields() {
                if field
                    .get_config_value(&ColumnMetadataKey::IdentityConcurrentSequenceId)
                    .is_some()
                {
                    return Err(Error::generic(format!(
                        "Identity column '{}' is nested; Concurrent Identity Columns are only \
                         supported at the top level of the schema",
                        field.name()
                    )));
                }
                reject_nested_cic(field.data_type())?;
            }
        }
        DataType::Array(array) => reject_nested_cic(&array.element_type)?,
        DataType::Map(map) => {
            reject_nested_cic(&map.key_type)?;
            reject_nested_cic(&map.value_type)?;
        }
        _ => {}
    }
    Ok(())
}

/// Parses the optional classic `delta.identity.allowExplicitInsert` flag (default false).
fn parse_allow_explicit_insert(field: &StructField) -> DeltaResult<bool> {
    match field.get_config_value(&ColumnMetadataKey::IdentityAllowExplicitInsert) {
        Some(MetadataValue::Boolean(b)) => Ok(*b),
        Some(MetadataValue::String(s)) => s.parse::<bool>().map_err(|_| {
            Error::generic(format!(
                "Identity column '{}': invalid boolean for '{}': {s}",
                field.name(),
                ColumnMetadataKey::IdentityAllowExplicitInsert.as_ref(),
            ))
        }),
        Some(other) => Err(Error::generic(format!(
            "Identity column '{}': expected boolean for '{}', got: {other}",
            field.name(),
            ColumnMetadataKey::IdentityAllowExplicitInsert.as_ref(),
        ))),
        None => Ok(false),
    }
}

/// Extracts a required string metadata value from a struct field, cloned.
fn get_required_string(field: &StructField, key: &ColumnMetadataKey) -> DeltaResult<String> {
    get_required_str(field, key).map(str::to_string)
}

/// Extracts a required string metadata value from a struct field, borrowed.
fn get_required_str<'a>(field: &'a StructField, key: &ColumnMetadataKey) -> DeltaResult<&'a str> {
    match field.get_config_value(key) {
        Some(MetadataValue::String(s)) => Ok(s),
        Some(other) => Err(Error::generic(format!(
            "Identity column '{}': expected string for metadata key '{}', got: {other}",
            field.name(),
            key.as_ref(),
        ))),
        None => Err(Error::generic(format!(
            "Identity column '{}': missing required metadata key '{}'",
            field.name(),
            key.as_ref(),
        ))),
    }
}

/// Extracts a required i64 metadata value from a struct field.
fn get_required_i64(field: &StructField, key: &ColumnMetadataKey) -> DeltaResult<i64> {
    match field.get_config_value(key) {
        Some(MetadataValue::Number(n)) => Ok(*n),
        Some(other) => Err(Error::generic(format!(
            "Identity column '{}': expected number for metadata key '{}', got: {other}",
            field.name(),
            key.as_ref(),
        ))),
        None => Err(Error::generic(format!(
            "Identity column '{}': missing required metadata key '{}'",
            field.name(),
            key.as_ref(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    #[test]
    fn cic_column_stamps_all_three_metadata_keys() {
        let field = cic_column("id", "seq-123", 5, 2);
        assert_eq!(field.name(), "id");
        assert_eq!(field.data_type(), &DataType::LONG);
        assert!(!field.is_nullable());
        assert_eq!(
            field.get_config_value(&ColumnMetadataKey::IdentityConcurrentSequenceId),
            Some(&MetadataValue::String("seq-123".to_string()))
        );
        assert_eq!(
            field.get_config_value(&ColumnMetadataKey::IdentityStart),
            Some(&MetadataValue::Number(5))
        );
        assert_eq!(
            field.get_config_value(&ColumnMetadataKey::IdentityStep),
            Some(&MetadataValue::Number(2))
        );
    }

    #[test]
    fn detect_no_identity_columns() {
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::new("id", DataType::LONG, false),
                StructField::new("name", DataType::STRING, true),
            ])
            .unwrap(),
        );
        let result = detect_identity_columns(&schema).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn detect_single_identity_column() {
        let schema = Arc::new(
            StructType::try_new(vec![
                cic_column("id", "seq-123", 1, 1),
                StructField::new("name", DataType::STRING, true),
            ])
            .unwrap(),
        );
        let result = detect_identity_columns(&schema).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_name, "id");
        assert_eq!(result[0].sequence_id, "seq-123");
        assert_eq!(result[0].start, 1);
        assert_eq!(result[0].step, 1);
        assert!(!result[0].allow_explicit_insert);
    }

    #[test]
    fn detect_multiple_identity_columns() {
        let schema = Arc::new(
            StructType::try_new(vec![
                cic_column("id", "seq-1", 1, 1),
                StructField::new("name", DataType::STRING, true),
                cic_column("row_id", "seq-2", 100, 10),
            ])
            .unwrap(),
        );
        let result = detect_identity_columns(&schema).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].column_name, "id");
        assert_eq!(result[1].column_name, "row_id");
        assert_eq!(result[1].start, 100);
        assert_eq!(result[1].step, 10);
    }

    #[test]
    fn detect_identity_column_with_explicit_insert() {
        let field = cic_column("id", "seq-1", 1, 1).add_metadata(vec![(
            ColumnMetadataKey::IdentityAllowExplicitInsert
                .as_ref()
                .to_string(),
            MetadataValue::Boolean(true),
        )]);
        let schema = Arc::new(StructType::try_new(vec![field]).unwrap());
        let result = detect_identity_columns(&schema).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].allow_explicit_insert);
    }

    #[test]
    fn concurrent_identity_columns_borrows_views() {
        let schema = StructType::try_new(vec![
            cic_column("id", "seq-1", 1, 1),
            StructField::new("payload", DataType::STRING, true),
            cic_column("row_id", "seq-2", 100, 10),
        ])
        .unwrap();
        let cols = concurrent_identity_columns(&schema).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].column_name(), "id");
        assert_eq!(cols[0].sequence_id(), "seq-1");
        assert_eq!(cols[0].start(), 1);
        assert_eq!(cols[0].step(), 1);
        assert!(!cols[0].allow_explicit_insert());
        assert_eq!(cols[1].column_name(), "row_id");
        assert_eq!(cols[1].step(), 10);
    }

    #[rstest::rstest]
    #[case::missing_start(
        &[
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::String("seq-1".to_string())),
            (ColumnMetadataKey::IdentityStep, MetadataValue::Number(1)),
        ],
        "delta.identity.start",
    )]
    #[case::missing_step(
        &[
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::String("seq-1".to_string())),
            (ColumnMetadataKey::IdentityStart, MetadataValue::Number(1)),
        ],
        "delta.identity.step",
    )]
    fn detect_identity_column_missing_required_key_returns_error(
        #[case] metadata: &[(ColumnMetadataKey, MetadataValue)],
        #[case] missing_key: &str,
    ) {
        let field = StructField::new("id", DataType::LONG, false).with_metadata(
            metadata
                .iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.clone()))
                .collect::<Vec<_>>(),
        );
        let schema = Arc::new(StructType::try_new(vec![field]).unwrap());
        let err = detect_identity_columns(&schema).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing required metadata key"), "{msg}");
        assert!(msg.contains(missing_key), "{msg}");
    }

    #[test]
    fn schema_has_high_water_mark_detects_only_the_classic_key() {
        // A purely concurrent schema has no high-water mark.
        let concurrent = StructType::try_new(vec![
            cic_column("id", "seq-1", 1, 1),
            StructField::new("payload", DataType::STRING, true),
        ])
        .unwrap();
        assert!(!schema_has_high_water_mark(&concurrent));

        // A surviving classic high-water-mark column is detected.
        let with_hwm = StructType::try_new(vec![
            cic_column("id", "seq-1", 1, 1),
            StructField::new("legacy", DataType::LONG, false).with_metadata(vec![(
                ColumnMetadataKey::IdentityHighWaterMark
                    .as_ref()
                    .to_string(),
                MetadataValue::Number(7),
            )]),
        ])
        .unwrap();
        assert!(schema_has_high_water_mark(&with_hwm));
    }
}
