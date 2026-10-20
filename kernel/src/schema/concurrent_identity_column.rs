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

/// Validates the identity columns of `schema`, returning whether it has any Concurrent Identity
/// Column.
///
/// Each CIC must be a `LONG` with well-formed metadata and a non-zero step, must not be a
/// partition column, must carry a sequence id of at most [`MAX_SEQUENCE_ID_LENGTH`] characters
/// that is unique within the schema, and must not also carry `delta.identity.highWaterMark`, a
/// column default, or a generation expression. A nested identity column, classic or concurrent, is
/// always rejected.
///
/// When the table supports `concurrentIdentityColumns` (`cic_supported`) every identity column
/// must be concurrent: a classic identity column (one carrying `delta.identity.*` keys but no
/// `sequenceId`) is rejected. If the table does not support the `concurrentIdentityColumns`
/// feature, any CIC is rejected.
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
    cic_supported: bool,
) -> KernelResult<bool> {
    let mut seen_sequence_ids: HashSet<&str> = HashSet::new();
    let mut classic_identity_column: Option<&str> = None;
    for field in schema.fields() {
        match ConcurrentIdentityColumn::try_from_field(field)? {
            Some(cic) => {
                if !cic_supported {
                    return Err(KernelError::schema(format!(
                        "Concurrent Identity Column '{}' is present but the table does not support the \
                         'concurrentIdentityColumns' feature.",
                        cic.field.name()
                    )));
                }
                validate_cic(&cic, partition_columns)?;
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
                    "Classic identity column '{field.name()}' has no '{}'; on a table with Concurrent Identity Columns \
                     every identity column must be concurrent (classic '{}' identity generation is not \
                     supported).",
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

fn concurrent_sequence_id(field: &StructField) -> Option<&MetadataValue> {
    field.get_config_value(&ColumnMetadataKey::IdentityConcurrentSequenceId)
}

/// Returns whether `field` carries any of the identity metadata keys (classic or concurrent).
fn is_identity_column(field: &StructField) -> bool {
    IDENTITY_KEYS
        .iter()
        .any(|key| field.get_config_value(key).is_some())
}

/// Validates a single Concurrent Identity Column. It only validates the CIC itself, not whether
/// it is nested or not. The rules are the same as for [`validate_concurrent_identity_columns`].
fn validate_cic(
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
    // Column-name matching is case-sensitive, consistent with Delta column-name handling.
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

    #[test]
    fn concurrent_identity_column_stamps_all_identity_metadata_keys() {
        let field = concurrent_identity_column("id", "seq-123", 5, 2, true);
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
        assert_eq!(
            field.get_config_value(&ColumnMetadataKey::IdentityAllowExplicitInsert),
            Some(&MetadataValue::Boolean(true))
        );
    }

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

    fn cic_field(name: &str, data_type: DataType, nullable: bool, step: i64) -> StructField {
        StructField::new(name, data_type, nullable)
            .with_metadata(cic_metadata("seq", 1, step, false))
    }

    fn nested(field: StructField) -> StructField {
        StructField::nullable("wrapper", StructType::try_new(vec![field]).unwrap())
    }

    #[rstest::rstest]
    #[case::valid(vec![cic_field("id", DataType::LONG, false, 1)], &[], true, Ok(true))]
    #[case::no_cic(vec![StructField::new("id", DataType::LONG, false)], &[], false, Ok(false))]
    #[case::non_long(vec![cic_field("id", DataType::INTEGER, false, 1)], &[], true, Err("must be of type LONG"))]
    #[case::nullable(vec![cic_field("id", DataType::LONG, true, 1)], &[], true, Ok(true))]
    #[case::step_zero(vec![cic_field("id", DataType::LONG, false, 0)], &[], true, Err("step 0"))]
    #[case::missing_allow_explicit_insert(
        vec![StructField::not_null("id", DataType::LONG).with_metadata(
            cic_metadata("seq", 1, 1, false)
                .into_iter()
                .filter(|(k, _)| k != ColumnMetadataKey::IdentityAllowExplicitInsert.as_ref()),
        )],
        &[],
        true,
        Err("delta.identity.allowExplicitInsert"),
    )]
    #[case::high_water_mark(
        vec![cic_field("id", DataType::LONG, false, 1).add_metadata([(
            ColumnMetadataKey::IdentityHighWaterMark.as_ref(),
            MetadataValue::Number(1),
        )])],
        &[],
        true,
        Err("must not carry 'delta.identity.highWaterMark'"),
    )]
    #[case::column_default(
        vec![cic_field("id", DataType::LONG, false, 1).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref(),
            MetadataValue::from("0"),
        )])],
        &[],
        true,
        Err("must not carry 'CURRENT_DEFAULT'"),
    )]
    #[case::generation_expression(
        vec![cic_field("id", DataType::LONG, false, 1).add_metadata([(
            ColumnMetadataKey::GenerationExpression.as_ref(),
            MetadataValue::from("1"),
        )])],
        &[],
        true,
        Err("must not carry 'delta.generationExpression'"),
    )]
    #[case::partition_column(vec![cic_field("id", DataType::LONG, false, 1)], &["id"], true, Err("cannot also be a partition column"))]
    #[case::partition_column_special_char(vec![cic_field("a.b", DataType::LONG, false, 1)], &["a.b"], true, Err("cannot also be a partition column"))]
    #[case::partition_column_case_sensitive(vec![cic_field("id", DataType::LONG, false, 1)], &["ID"], true, Ok(true))]
    #[case::sequence_id_max_length(vec![concurrent_identity_column("id", "x".repeat(64), 1, 1, false)], &[], true, Ok(true))]
    #[case::sequence_id_too_long(vec![concurrent_identity_column("id", "x".repeat(65), 1, 1, false)], &[], true, Err("longer than 64"))]
    #[case::duplicate_sequence_id(
        vec![cic_field("a", DataType::LONG, false, 1), cic_field("b", DataType::LONG, false, 1)],
        &[],
        true,
        Err("must be unique"),
    )]
    #[case::distinct_sequence_ids(
        vec![
            concurrent_identity_column("a", "seq-1", 1, 1, false),
            concurrent_identity_column("b", "seq-2", 1, 1, false),
        ],
        &[],
        true,
        Ok(true),
    )]
    #[case::nested_in_struct(vec![nested(cic_field("id", DataType::LONG, false, 1))], &[], true, Err("nested"))]
    #[case::nested_in_array(
        vec![StructField::nullable(
            "wrapper",
            ArrayType::new(
                cic_struct(),
                true,
            ),
        )],
        &[],
        true,
        Err("nested"),
    )]
    #[case::nested_in_map_value(
        vec![StructField::nullable(
            "wrapper",
            MapType::new(
                DataType::STRING,
                cic_struct(),
                true,
            ),
        )],
        &[],
        true,
        Err("nested"),
    )]
    #[case::nested_in_map_key(
        vec![StructField::nullable(
            "wrapper",
            MapType::new(
                cic_struct(),
                DataType::STRING,
                true,
            ),
        )],
        &[],
        true,
        Err("nested"),
    )]
    #[case::nested_classic_next_to_cic(
        vec![cic_field("id", DataType::LONG, false, 1), nested(classic_identity_column("classic", None))],
        &[],
        true,
        Err("nested"),
    )]
    // On a table supporting the feature, every identity column must be concurrent, including a
    // classic identity column that never emitted a value (no high-water mark).
    #[case::classic_on_cic_supported_table(vec![classic_identity_column("classic", None)], &[], true, Err("must be concurrent"))]
    #[case::classic_without_cic(vec![classic_identity_column("classic", None)], &[], false, Ok(false))]
    fn validate_enforces_each_rule(
        #[case] fields: Vec<StructField>,
        #[case] partition_columns: &[&str],
        #[case] cic_supported: bool,
        #[case] expected: Result<bool, &str>,
    ) {
        let schema = StructType::try_new(fields).unwrap();
        let partition: Vec<String> = partition_columns.iter().map(|s| s.to_string()).collect();
        let result = validate_concurrent_identity_columns(&schema, &partition, cic_supported);
        match expected {
            Ok(has_cic) => assert_eq!(result.unwrap(), has_cic),
            Err(needle) => assert_result_error_with_message(result, needle),
        }
    }

    #[rstest::rstest]
    #[case::plain(StructField::nullable("v", DataType::STRING), false)]
    #[case::top_level(cic_field("id", DataType::LONG, false, 1), true)]
    #[case::struct_field(StructField::nullable("v", cic_struct()), true)]
    #[case::array_element(StructField::nullable("v", ArrayType::new(cic_struct(), true)), true)]
    #[case::map_key(
        StructField::nullable("v", MapType::new(cic_struct(), DataType::STRING, true)),
        true
    )]
    #[case::map_value(
        StructField::nullable("v", MapType::new(DataType::STRING, cic_struct(), true)),
        true
    )]
    fn sequence_id_rejected_at_any_depth_without_feature(
        #[case] field: StructField,
        #[case] rejected: bool,
    ) {
        let schema = StructType::try_new(vec![field]).unwrap();
        let result = validate_concurrent_identity_columns(&schema, &[], false);
        assert_eq!(result.is_err(), rejected, "{result:?}");
    }

    fn cic_struct() -> DataType {
        StructType::try_new(vec![cic_field("id", DataType::LONG, false, 1)])
            .unwrap()
            .into()
    }
}
