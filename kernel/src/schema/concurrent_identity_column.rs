//! Support for Concurrent Identity Columns (CIC).
//!
//! A Concurrent Identity Column draws its values from a monotonic sequence hosted by the table's
//! catalog instead of from the Delta-log `delta.identity.highWaterMark`, allowing multiple
//! concurrent writers to generate unique BIGINT identity values without conflicting. A column is
//! concurrent iff its metadata carries `delta.identity.concurrent.sequenceId`.
//!
//! Kernel owns only the Delta protocol's metadata. A connector discovers the columns it must fill
//! via [`Transaction::concurrent_identity_columns`], reserves ranges from its own sequence client,
//! generates the values (a reserved range enumerates as `range_start + step * i`), fills the
//! columns into its batch, and acknowledges responsibility via
//! [`Transaction::ack_concurrent_identity_columns`]. Kernel neither reserves, generates, nor
//! inserts values.
//!
//! [`Transaction::concurrent_identity_columns`]: crate::transaction::Transaction::concurrent_identity_columns
//! [`Transaction::ack_concurrent_identity_columns`]: crate::transaction::Transaction::ack_concurrent_identity_columns

use crate::schema::{
    ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
use crate::{DeltaResult, Error};

/// A borrowed view of a Concurrent Identity Column, surfaced by
/// [`Transaction::concurrent_identity_columns`](crate::transaction::Transaction::concurrent_identity_columns).
///
/// See the [module docs](self) for the connector write-flow.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcurrentIdentityColumn<'a> {
    column_name: &'a str,
    sequence_id: &'a str,
    start: i64,
    step: i64,
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
}

/// Scans the top-level fields of `schema` for Concurrent Identity Columns (CIC), returning a
/// borrowed [`ConcurrentIdentityColumn`] view of each.
///
/// A column is a CIC iff it carries the `delta.identity.concurrent.sequenceId` metadata key. When
/// that key is present, the classic `delta.identity.start` and `delta.identity.step` keys are also
/// required.
///
/// Shared by the write-path report ([`Transaction::concurrent_identity_columns`]) and the
/// CREATE/ALTER validation ([`validate_concurrent_identity_columns`]).
///
/// # Errors
///
/// Returns an error if a detected column is missing a required metadata key or its value is
/// malformed.
///
/// [`Transaction::concurrent_identity_columns`]: crate::transaction::Transaction::concurrent_identity_columns
pub(crate) fn try_collect_concurrent_identity_columns(
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
            sequence_id: parse_required(
                field,
                ColumnMetadataKey::IdentityConcurrentSequenceId,
                "string",
                as_string,
            )?,
            start: parse_required(field, ColumnMetadataKey::IdentityStart, "number", as_number)?,
            step: parse_required(field, ColumnMetadataKey::IdentityStep, "number", as_number)?,
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
/// `partition_columns` must be the raw, unescaped leaf names (as returned by
/// [`StructField::name`]) so they compare in the same vocabulary as the schema's field names.
///
/// # Errors
///
/// Returns an error describing the first violation, or malformed CIC metadata (see
/// [`try_collect_concurrent_identity_columns`]).
pub(crate) fn validate_concurrent_identity_columns(
    schema: &SchemaRef,
    partition_columns: &[String],
) -> DeltaResult<bool> {
    let mut found = false;
    for field in schema.fields() {
        if field
            .get_config_value(&ColumnMetadataKey::IdentityConcurrentSequenceId)
            .is_some()
        {
            found = true;
            validate_top_level_cic(field, partition_columns)?;
        }
        // CIC is only supported at the top level; reject the metadata anywhere below it.
        reject_nested_cic(field.data_type())?;
    }
    Ok(found)
}

pub(crate) fn schema_has_high_water_mark(schema: &StructType) -> bool {
    schema.fields().any(|field| {
        field
            .get_config_value(&ColumnMetadataKey::IdentityHighWaterMark)
            .is_some()
    })
}

/// The three metadata keys that mark a Concurrent Identity Column: the concurrent sequence id plus
/// the classic `start`/`step`.
#[cfg(test)]
pub(crate) fn cic_metadata(
    sequence_id: impl Into<String>,
    start: i64,
    step: i64,
) -> Vec<(String, MetadataValue)> {
    vec![
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
    ]
}

/// Builds a CIC identity column field with the sequence-id marker plus the classic `start`/`step`
/// metadata keys stamped on it.
///
/// The returned [`StructField`] is a non-nullable `LONG`. Passing it to `create_table` also
/// requires the `catalogManaged` feature to be enabled; CIC is not accepted on a filesystem table.
#[cfg(test)]
pub(crate) fn concurrent_identity_column(
    name: impl Into<String>,
    sequence_id: impl Into<String>,
    start: i64,
    step: i64,
) -> StructField {
    StructField::new(name, DataType::LONG, false).with_metadata(cic_metadata(
        sequence_id,
        start,
        step,
    ))
}

/// Validates a single top-level Concurrent Identity Column field. See
/// [`validate_concurrent_identity_columns`] for the rules.
fn validate_top_level_cic(field: &StructField, partition_columns: &[String]) -> DeltaResult<()> {
    let name = field.name();
    // Reject malformed metadata up front (missing or wrong-typed required keys).
    parse_required(
        field,
        ColumnMetadataKey::IdentityConcurrentSequenceId,
        "string",
        as_string,
    )?;
    parse_required(field, ColumnMetadataKey::IdentityStart, "number", as_number)?;
    let step = parse_required(field, ColumnMetadataKey::IdentityStep, "number", as_number)?;

    if field.data_type() != &DataType::LONG {
        return Err(Error::generic(format!(
            "Identity column '{name}' must be of type LONG, got {}",
            field.data_type()
        )));
    }
    if field.is_nullable() {
        return Err(Error::generic(format!(
            "Identity column '{name}' must be non-nullable"
        )));
    }
    if step == 0 {
        return Err(Error::generic(format!(
            "Identity column '{name}' has step 0, which is not allowed"
        )));
    }
    // A sequence id and a high-water mark are mutually exclusive (RFC): the value is either
    // allocated from the sequence or derived from the mark, never both.
    if field
        .get_config_value(&ColumnMetadataKey::IdentityHighWaterMark)
        .is_some()
    {
        return Err(Error::generic(format!(
            "Identity column '{name}' carries both a concurrent sequence id and a '{}'; these \
             are mutually exclusive.",
            ColumnMetadataKey::IdentityHighWaterMark.as_ref(),
        )));
    }
    if partition_columns
        .iter()
        .any(|p| p.eq_ignore_ascii_case(name))
    {
        return Err(Error::generic(format!(
            "Identity column '{name}' cannot also be a partition column"
        )));
    }
    Ok(())
}

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

/// Reads a required CIC metadata key off `field`, applying `extract` to convert the stored
/// [`MetadataValue`]. `expected` names the wanted value kind for the error message.
///
/// # Errors
///
/// Returns an error if the key is absent, or present but `extract` rejects its value type.
fn parse_required<'a, T>(
    field: &'a StructField,
    key: ColumnMetadataKey,
    expected: &str,
    extract: impl FnOnce(&'a MetadataValue) -> Option<T>,
) -> DeltaResult<T> {
    match field.get_config_value(&key) {
        Some(value) => extract(value).ok_or_else(|| {
            Error::generic(format!(
                "Identity column '{}': expected {expected} for '{}', got: {value}",
                field.name(),
                key.as_ref(),
            ))
        }),
        None => Err(Error::generic(format!(
            "Identity column '{}': missing required metadata key '{}'",
            field.name(),
            key.as_ref(),
        ))),
    }
}

fn as_number(value: &MetadataValue) -> Option<i64> {
    match value {
        MetadataValue::Number(n) => Some(*n),
        _ => None,
    }
}

fn as_string(value: &MetadataValue) -> Option<&str> {
    match value {
        MetadataValue::String(s) => Some(s),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    #[test]
    fn concurrent_identity_column_stamps_all_three_metadata_keys() {
        let field = concurrent_identity_column("id", "seq-123", 5, 2);
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
    fn collect_returns_empty_when_no_identity_columns() {
        let schema = StructType::try_new(vec![
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap();
        assert!(try_collect_concurrent_identity_columns(&schema)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn collect_borrows_a_view_of_each_identity_column() {
        let schema = StructType::try_new(vec![
            concurrent_identity_column("id", "seq-1", 1, 1),
            StructField::new("payload", DataType::STRING, true),
            concurrent_identity_column("row_id", "seq-2", 100, 10),
        ])
        .unwrap();
        let cols = try_collect_concurrent_identity_columns(&schema).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].column_name(), "id");
        assert_eq!(cols[0].sequence_id(), "seq-1");
        assert_eq!(cols[0].start(), 1);
        assert_eq!(cols[0].step(), 1);
        assert_eq!(cols[1].column_name(), "row_id");
        assert_eq!(cols[1].sequence_id(), "seq-2");
        assert_eq!(cols[1].start(), 100);
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
    fn collect_missing_required_key_returns_error(
        #[case] metadata: &[(ColumnMetadataKey, MetadataValue)],
        #[case] missing_key: &str,
    ) {
        let field = StructField::new("id", DataType::LONG, false).with_metadata(
            metadata
                .iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.clone()))
                .collect::<Vec<_>>(),
        );
        let schema = StructType::try_new(vec![field]).unwrap();
        let err = try_collect_concurrent_identity_columns(&schema).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing required metadata key"), "{msg}");
        assert!(msg.contains(missing_key), "{msg}");
    }

    #[rstest::rstest]
    #[case::start_not_a_number(
        vec![
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::String("seq".to_string())),
            (ColumnMetadataKey::IdentityStart, MetadataValue::String("x".to_string())),
            (ColumnMetadataKey::IdentityStep, MetadataValue::Number(1)),
        ],
        "expected number",
    )]
    #[case::sequence_id_not_a_string(
        vec![
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::Number(7)),
            (ColumnMetadataKey::IdentityStart, MetadataValue::Number(1)),
            (ColumnMetadataKey::IdentityStep, MetadataValue::Number(1)),
        ],
        "expected string",
    )]
    fn collect_wrong_metadata_type_returns_error(
        #[case] metadata: Vec<(ColumnMetadataKey, MetadataValue)>,
        #[case] needle: &str,
    ) {
        let field = StructField::new("id", DataType::LONG, false).with_metadata(
            metadata
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v))
                .collect::<Vec<_>>(),
        );
        let schema = StructType::try_new(vec![field]).unwrap();
        let err = try_collect_concurrent_identity_columns(&schema)
            .unwrap_err()
            .to_string();
        assert!(err.contains(needle), "{err}");
    }

    fn cic_field(name: &str, data_type: DataType, nullable: bool, step: i64) -> StructField {
        StructField::new(name, data_type, nullable).with_metadata(cic_metadata("seq", 1, step))
    }

    #[rstest::rstest]
    #[case::valid(vec![cic_field("id", DataType::LONG, false, 1)], &[], Ok(true))]
    #[case::no_cic(vec![StructField::new("id", DataType::LONG, false)], &[], Ok(false))]
    #[case::non_long(vec![cic_field("id", DataType::INTEGER, false, 1)], &[], Err("must be of type LONG"))]
    #[case::nullable(vec![cic_field("id", DataType::LONG, true, 1)], &[], Err("must be non-nullable"))]
    #[case::step_zero(vec![cic_field("id", DataType::LONG, false, 0)], &[], Err("step 0"))]
    #[case::high_water_mark(
        vec![cic_field("id", DataType::LONG, false, 1).add_metadata(vec![(
            ColumnMetadataKey::IdentityHighWaterMark.as_ref().to_string(),
            MetadataValue::Number(1),
        )])],
        &[],
        Err("mutually exclusive"),
    )]
    #[case::partition_column(vec![cic_field("id", DataType::LONG, false, 1)], &["id"], Err("cannot also be a partition column"))]
    #[case::partition_column_special_char(vec![cic_field("a.b", DataType::LONG, false, 1)], &["a.b"], Err("cannot also be a partition column"))]
    #[case::nested_in_struct(
        vec![StructField::nullable(
            "wrapper",
            StructType::try_new(vec![cic_field("id", DataType::LONG, false, 1)]).unwrap(),
        )],
        &[],
        Err("nested"),
    )]
    #[case::nested_in_array(
        vec![StructField::nullable(
            "wrapper",
            ArrayType::new(
                StructType::try_new(vec![cic_field("id", DataType::LONG, false, 1)]).unwrap(),
                true,
            ),
        )],
        &[],
        Err("nested"),
    )]
    #[case::nested_in_map_value(
        vec![StructField::nullable(
            "wrapper",
            MapType::new(
                DataType::STRING,
                StructType::try_new(vec![cic_field("id", DataType::LONG, false, 1)]).unwrap(),
                true,
            ),
        )],
        &[],
        Err("nested"),
    )]
    fn validate_enforces_each_rule(
        #[case] fields: Vec<StructField>,
        #[case] partition_columns: &[&str],
        #[case] expected: Result<bool, &str>,
    ) {
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let partition: Vec<String> = partition_columns.iter().map(|s| s.to_string()).collect();
        let result = validate_concurrent_identity_columns(&schema, &partition);
        match expected {
            Ok(has_cic) => assert_eq!(result.unwrap(), has_cic),
            Err(needle) => {
                let err = result.unwrap_err().to_string();
                assert!(err.contains(needle), "expected {needle:?} in: {err}");
            }
        }
    }
}
