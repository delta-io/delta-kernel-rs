//! Support for Delta's Concurrent Identity Columns (CIC).
//!
//! A Concurrent Identity Column draws its values from a monotonic sequence hosted by the table's
//! catalog instead of from the Delta-log `delta.identity.highWaterMark`, allowing multiple
//! concurrent writers to generate unique `LONG` identity values without conflicting. A column is
//! concurrent iff its metadata carries `delta.identity.concurrent.sequenceId`. On a table that
//! supports the `concurrentIdentityColumns` feature, every identity column must be concurrent.
//!
//! Kernel only parses and validates this metadata; allocating sequence values is the catalog's job.

use std::collections::HashSet;

use crate::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
use crate::{KernelError, KernelResult};

/// The maximum length, in characters, of a Concurrent Identity Column's `sequenceId`.
const MAX_SEQUENCE_ID_LENGTH: usize = 64;

/// The metadata keys whose presence marks a field as an identity column (classic or concurrent).
const IDENTITY_KEYS: [ColumnMetadataKey; 5] = [
    ColumnMetadataKey::IdentityStart,
    ColumnMetadataKey::IdentityStep,
    ColumnMetadataKey::IdentityHighWaterMark,
    ColumnMetadataKey::IdentityAllowExplicitInsert,
    ColumnMetadataKey::IdentityConcurrentSequenceId,
];

/// The parsed metadata of a top-level Concurrent Identity Column.
#[derive(Debug, Clone, PartialEq)]
// `start` and `allow_explicit_insert` are parsed only to validate their types.
#[allow(dead_code)]
pub(crate) struct ConcurrentIdentityColumn<'a> {
    field: &'a StructField,
    sequence_id: &'a str,
    start: i64,
    step: i64,
    allow_explicit_insert: bool,
}

impl<'a> ConcurrentIdentityColumn<'a> {
    /// Parses the CIC metadata of `field`, or returns `None` if `field` carries no
    /// `delta.identity.concurrent.sequenceId`.
    ///
    /// # Errors
    ///
    /// Returns an error if the field carries a `sequenceId` but a required key
    /// (`sequenceId`, `start`, `step`, `allowExplicitInsert`) is missing or has the wrong type.
    pub(crate) fn try_from_field(field: &'a StructField) -> KernelResult<Option<Self>> {
        if !has_concurrent_sequence_id(field) {
            return Ok(None);
        }
        Ok(Some(Self {
            field,
            sequence_id: parse_required(
                field,
                ColumnMetadataKey::IdentityConcurrentSequenceId,
                "string",
                as_string,
            )?,
            start: parse_required(field, ColumnMetadataKey::IdentityStart, "number", as_number)?,
            step: parse_required(field, ColumnMetadataKey::IdentityStep, "number", as_number)?,
            allow_explicit_insert: parse_required(
                field,
                ColumnMetadataKey::IdentityAllowExplicitInsert,
                "boolean",
                as_bool,
            )?,
        }))
    }
}

/// Validates the identity columns of `schema` for a table that supports the
/// `concurrentIdentityColumns` feature, returning whether it has any Concurrent Identity Column.
///
/// Each CIC must be a `LONG` with well-formed metadata and a non-zero step, must not be a
/// partition column, must carry a sequence id of at most [`MAX_SEQUENCE_ID_LENGTH`] characters
/// that is unique within the schema, and must not also carry `delta.identity.highWaterMark`, a
/// column default, or a generation expression. A nested identity column, classic or concurrent, is
/// always rejected.
///
/// Every identity column must be concurrent: a classic identity column (one carrying
/// `delta.identity.*` keys but no `sequenceId`) is rejected.
///
/// `partition_columns` must be the raw, unescaped leaf names (as returned by
/// [`StructField::name`]) so they compare in the same vocabulary as the schema's field names.
///
/// # Errors
///
/// Returns an error describing the first violation.
pub(crate) fn validate_concurrent_identity_columns(
    schema: &StructType,
    partition_columns: &[String],
) -> KernelResult<bool> {
    let mut seen_sequence_ids: HashSet<&str> = HashSet::new();
    for field in schema.fields() {
        match ConcurrentIdentityColumn::try_from_field(field)? {
            Some(cic) => {
                validate_concurrent_identity_column(&cic, partition_columns)?;
                if !seen_sequence_ids.insert(cic.sequence_id) {
                    return Err(KernelError::schema(format!(
                        "Sequence id '{}' is used by more than one Concurrent Identity Column; \
                         each sequence id must be unique within a table.",
                        cic.sequence_id
                    )));
                }
            }
            None if is_identity_column(field) => {
                return Err(KernelError::schema(format!(
                    "Classic identity column '{}' has no '{}'; every identity column must be \
                     concurrent (classic '{}' identity generation is not supported).",
                    field.name(),
                    ColumnMetadataKey::IdentityConcurrentSequenceId.as_ref(),
                    ColumnMetadataKey::IdentityHighWaterMark.as_ref(),
                )));
            }
            None => {}
        }
        ensure_no_nested_identity_columns(field.data_type())?;
    }
    Ok(!seen_sequence_ids.is_empty())
}

/// Returns whether `field` carries the `delta.identity.concurrent.sequenceId` metadata key.
pub(crate) fn has_concurrent_sequence_id(field: &StructField) -> bool {
    concurrent_sequence_id(field).is_some()
}

/// Returns whether any field of `schema`, at any depth, carries the
/// `delta.identity.concurrent.sequenceId` metadata key.
pub(crate) fn schema_has_concurrent_sequence_id(schema: &StructType) -> bool {
    fn data_type_has(data_type: &DataType) -> bool {
        match data_type {
            DataType::Struct(s) => schema_has_concurrent_sequence_id(s),
            DataType::Array(a) => data_type_has(&a.element_type),
            DataType::Map(m) => data_type_has(&m.key_type) || data_type_has(&m.value_type),
            _ => false,
        }
    }
    schema
        .fields()
        .any(|field| has_concurrent_sequence_id(field) || data_type_has(field.data_type()))
}

fn concurrent_sequence_id(field: &StructField) -> Option<&MetadataValue> {
    field.get_config_value(&ColumnMetadataKey::IdentityConcurrentSequenceId)
}

/// Returns whether `field` carries any of the identity metadata keys (classic or concurrent).
fn is_identity_column(field: &StructField) -> bool {
    IDENTITY_KEYS
        .iter()
        .any(|key| field.get_config_value(key).is_some())
}

/// Checks one CIC against the per-column rules of [`validate_concurrent_identity_columns`].
fn validate_concurrent_identity_column(
    cic: &ConcurrentIdentityColumn<'_>,
    partition_columns: &[String],
) -> KernelResult<()> {
    let field = cic.field;
    let name = field.name();
    if cic.sequence_id.chars().count() > MAX_SEQUENCE_ID_LENGTH {
        return Err(KernelError::schema(format!(
            "Concurrent Identity Column '{name}' has a sequence id longer than \
             {MAX_SEQUENCE_ID_LENGTH} characters."
        )));
    }
    if field.data_type() != &DataType::LONG {
        return Err(KernelError::schema(format!(
            "Concurrent Identity Column '{name}' must be of type LONG, got {}",
            field.data_type()
        )));
    }
    if cic.step == 0 {
        return Err(KernelError::schema(format!(
            "Concurrent Identity Column '{name}' has step 0, which is not allowed"
        )));
    }
    // The value is either allocated from the sequence or derived from the mark, never both. A
    // Concurrent Identity Column must also be neither defaulted nor generated.
    for key in [
        ColumnMetadataKey::IdentityHighWaterMark,
        ColumnMetadataKey::CurrentDefault,
        ColumnMetadataKey::GenerationExpression,
    ] {
        if field.get_config_value(&key).is_some() {
            return Err(KernelError::schema(format!(
                "Concurrent Identity Column '{name}' must not carry '{}'",
                key.as_ref(),
            )));
        }
    }
    // A Concurrent Identity Column cannot also be a partition column.
    if partition_columns.iter().any(|p| p == name) {
        return Err(KernelError::schema(format!(
            "Concurrent Identity Column '{name}' cannot also be a partition column"
        )));
    }
    Ok(())
}

/// Walks the fields nested in `data_type` and rejects any nested identity column.
fn ensure_no_nested_identity_columns(data_type: &DataType) -> KernelResult<()> {
    match data_type {
        DataType::Struct(fields) => {
            for field in fields.fields() {
                if is_identity_column(field) {
                    return Err(KernelError::schema(format!(
                        "Identity column '{}' is nested; all identity columns must be top-level.",
                        field.name()
                    )));
                }
                ensure_no_nested_identity_columns(field.data_type())?;
            }
        }
        DataType::Array(array) => ensure_no_nested_identity_columns(&array.element_type)?,
        DataType::Map(map) => {
            ensure_no_nested_identity_columns(&map.key_type)?;
            ensure_no_nested_identity_columns(&map.value_type)?;
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
) -> KernelResult<T> {
    match field.get_config_value(&key) {
        Some(value) => extract(value).ok_or_else(|| {
            KernelError::schema(format!(
                "Concurrent Identity Column '{}': expected {expected} for '{}', got: {value}",
                field.name(),
                key.as_ref(),
            ))
        }),
        None => Err(KernelError::schema(format!(
            "Concurrent Identity Column '{}': missing required metadata key '{}'",
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

fn as_bool(value: &MetadataValue) -> Option<bool> {
    match value {
        MetadataValue::Boolean(b) => Some(*b),
        _ => None,
    }
}

fn as_string(value: &MetadataValue) -> Option<&str> {
    match value {
        MetadataValue::String(s) => Some(s),
        _ => None,
    }
}

// === Test helpers ===

/// Builds a Concurrent Identity Column: a non-nullable `LONG` [`StructField`] carrying the
/// concurrent `sequenceId` and the classic `start`/`step`/`allowExplicitInsert` keys.
#[cfg(test)]
pub(crate) fn concurrent_identity_column(
    name: impl Into<String>,
    sequence_id: impl Into<String>,
    start: i64,
    step: i64,
    allow_explicit_insert: bool,
) -> StructField {
    StructField::not_null(name, DataType::LONG).with_metadata(cic_metadata(
        sequence_id,
        start,
        step,
        allow_explicit_insert,
    ))
}

/// Builds a classic (non-concurrent) identity column: `start`/`step`/`allowExplicitInsert`, no
/// `sequenceId`, plus a `highWaterMark` if the column has emitted values.
#[cfg(test)]
pub(crate) fn classic_identity_column(name: &str, high_water_mark: Option<i64>) -> StructField {
    use ColumnMetadataKey::*;
    let high_water_mark = high_water_mark.map(|v| (IdentityHighWaterMark, MetadataValue::from(v)));
    let metadata = [
        (IdentityStart, MetadataValue::Number(1)),
        (IdentityStep, MetadataValue::Number(1)),
        (IdentityAllowExplicitInsert, MetadataValue::Boolean(false)),
    ];
    StructField::not_null(name, DataType::LONG).with_metadata(
        metadata
            .into_iter()
            .chain(high_water_mark)
            .map(|(k, v)| (k.as_ref().to_string(), v)),
    )
}

/// The metadata keys that mark a Concurrent Identity Column: the concurrent sequence id plus the
/// classic `start`/`step`/`allowExplicitInsert`.
#[cfg(test)]
pub(crate) fn cic_metadata(
    sequence_id: impl Into<String>,
    start: i64,
    step: i64,
    allow_explicit_insert: bool,
) -> [(String, MetadataValue); 4] {
    use ColumnMetadataKey::*;
    [
        (
            IdentityConcurrentSequenceId,
            MetadataValue::String(sequence_id.into()),
        ),
        (IdentityStart, MetadataValue::from(start)),
        (IdentityStep, MetadataValue::from(step)),
        (
            IdentityAllowExplicitInsert,
            MetadataValue::from(allow_explicit_insert),
        ),
    ]
    .map(|(k, v)| (k.as_ref().to_string(), v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};
    use crate::unit_test_utils::assert_result_error_with_message;

    #[rstest::rstest]
    #[case::missing_start(
        vec![
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::from("seq")),
            (ColumnMetadataKey::IdentityStep, MetadataValue::Number(1)),
        ],
        "missing required metadata key 'delta.identity.start'",
    )]
    #[case::missing_step(
        vec![
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::from("seq")),
            (ColumnMetadataKey::IdentityStart, MetadataValue::Number(1)),
        ],
        "missing required metadata key 'delta.identity.step'",
    )]
    #[case::missing_allow_explicit_insert(
        vec![
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::from("seq")),
            (ColumnMetadataKey::IdentityStart, MetadataValue::Number(1)),
            (ColumnMetadataKey::IdentityStep, MetadataValue::Number(1)),
        ],
        "missing required metadata key 'delta.identity.allowExplicitInsert'",
    )]
    #[case::start_not_a_number(
        vec![
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::from("seq")),
            (ColumnMetadataKey::IdentityStart, MetadataValue::from("x")),
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
    #[case::allow_explicit_insert_not_a_boolean(
        vec![
            (ColumnMetadataKey::IdentityConcurrentSequenceId, MetadataValue::from("seq")),
            (ColumnMetadataKey::IdentityStart, MetadataValue::Number(1)),
            (ColumnMetadataKey::IdentityStep, MetadataValue::Number(1)),
            (ColumnMetadataKey::IdentityAllowExplicitInsert, MetadataValue::from("true")),
        ],
        "expected boolean",
    )]
    fn parse_malformed_metadata_returns_error(
        #[case] metadata: Vec<(ColumnMetadataKey, MetadataValue)>,
        #[case] needle: &str,
    ) {
        let field = StructField::not_null("id", DataType::LONG).with_metadata(
            metadata
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v)),
        );
        assert_result_error_with_message(ConcurrentIdentityColumn::try_from_field(&field), needle);
    }

    fn cic(name: &str) -> StructField {
        concurrent_identity_column(name, "seq", 1, 1, false)
    }

    fn nested(field: StructField) -> StructField {
        StructField::nullable("wrapper", StructType::try_new(vec![field]).unwrap())
    }

    #[rstest::rstest]
    #[case::valid(vec![cic("id")], &[], Ok(true))]
    #[case::no_cic(vec![StructField::new("id", DataType::LONG, false)], &[], Ok(false))]
    #[case::non_long(
        vec![StructField::not_null("id", DataType::INTEGER).with_metadata(cic_metadata("seq", 1, 1, false))],
        &[],
        Err("must be of type LONG"),
    )]
    #[case::nullable(
        vec![StructField::nullable("id", DataType::LONG).with_metadata(cic_metadata("seq", 1, 1, false))],
        &[],
        Ok(true),
    )]
    #[case::step_zero(vec![concurrent_identity_column("id", "seq", 1, 0, false)], &[], Err("step 0"))]
    #[case::high_water_mark(
        vec![cic("id").add_metadata([(
            ColumnMetadataKey::IdentityHighWaterMark.as_ref(),
            MetadataValue::Number(1),
        )])],
        &[],
        Err("must not carry 'delta.identity.highWaterMark'"),
    )]
    #[case::column_default(
        vec![cic("id").add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref(),
            MetadataValue::from("0"),
        )])],
        &[],
        Err("must not carry 'CURRENT_DEFAULT'"),
    )]
    #[case::generation_expression(
        vec![cic("id").add_metadata([(
            ColumnMetadataKey::GenerationExpression.as_ref(),
            MetadataValue::from("1"),
        )])],
        &[],
        Err("must not carry 'delta.generationExpression'"),
    )]
    #[case::partition_column(vec![cic("id")], &["id"], Err("cannot also be a partition column"))]
    #[case::partition_column_special_char(vec![cic("a.b")], &["a.b"], Err("cannot also be a partition column"))]
    #[case::partition_column_case_sensitive(vec![cic("id")], &["ID"], Ok(true))]
    #[case::sequence_id_max_length(vec![concurrent_identity_column("id", "x".repeat(64), 1, 1, false)], &[], Ok(true))]
    #[case::sequence_id_too_long(vec![concurrent_identity_column("id", "x".repeat(65), 1, 1, false)], &[], Err("longer than 64"))]
    #[case::duplicate_sequence_id(vec![cic("a"), cic("b")], &[], Err("must be unique"))]
    #[case::distinct_sequence_ids(
        vec![
            concurrent_identity_column("a", "seq-1", 1, 1, false),
            concurrent_identity_column("b", "seq-2", 1, 1, false),
        ],
        &[],
        Ok(true),
    )]
    #[case::nested_classic_next_to_cic(
        vec![cic("id"), nested(classic_identity_column("classic", None))],
        &[],
        Err("nested"),
    )]
    #[case::classic_on_cic_supported_table(vec![classic_identity_column("classic", None)], &[], Err("must be concurrent"))]
    fn validate_enforces_each_rule(
        #[case] fields: Vec<StructField>,
        #[case] partition_columns: &[&str],
        #[case] expected: Result<bool, &str>,
    ) {
        let schema = StructType::try_new(fields).unwrap();
        let partition: Vec<String> = partition_columns.iter().map(|s| s.to_string()).collect();
        let result = validate_concurrent_identity_columns(&schema, &partition);
        match expected {
            Ok(has_cic) => assert_eq!(result.unwrap(), has_cic),
            Err(needle) => assert_result_error_with_message(result, needle),
        }
    }

    #[rstest::rstest]
    fn nested_cic_detected_and_rejected(
        #[values(
            nested(cic("id")),
            StructField::nullable("v", ArrayType::new(cic_struct(), true)),
            StructField::nullable("v", MapType::new(cic_struct(), DataType::STRING, true)),
            StructField::nullable("v", MapType::new(DataType::STRING, cic_struct(), true))
        )]
        field: StructField,
    ) {
        let schema = StructType::try_new(vec![field]).unwrap();
        assert!(schema_has_concurrent_sequence_id(&schema));
        assert_result_error_with_message(
            validate_concurrent_identity_columns(&schema, &[]),
            "nested",
        );
    }

    #[rstest::rstest]
    #[case::plain(StructField::nullable("v", DataType::STRING), false)]
    #[case::top_level(cic("id"), true)]
    fn sequence_id_detected_on_top_level_field(
        #[case] field: StructField,
        #[case] has_sequence_id: bool,
    ) {
        let schema = StructType::try_new(vec![field]).unwrap();
        assert_eq!(schema_has_concurrent_sequence_id(&schema), has_sequence_id);
    }

    fn cic_struct() -> DataType {
        StructType::try_new(vec![cic("id")]).unwrap().into()
    }
}
