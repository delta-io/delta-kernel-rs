//! Support for Concurrent Identity Columns (CIC).
//!
//! Concurrent Identity Columns store their high water mark in a UC Sequence Service instead of
//! Delta metadata, allowing multiple concurrent writers to generate unique BIGINT identity values
//! without conflicting.
//!
//! Kernel owns only the Delta protocol and the pure value arithmetic; it neither talks to the
//! sequence service nor inserts values into a batch. A connector discovers the columns it must
//! fill via [`Transaction::concurrent_identity_columns`], reserves ranges from its own client,
//! generates the values (reusing [`ReservedRange`] for the overflow-checked arithmetic), fills the
//! columns into its batch, and acknowledges responsibility via
//! [`Transaction::ack_concurrent_identity_columns`]. This module provides:
//! - [`IdentityColumnInfo`]: owned metadata about a CIC identity column detected from schema
//! - [`ConcurrentIdentityColumn`]: a borrowed view of a CIC column, surfaced on a transaction
//! - [`detect_identity_columns`]: scans a schema for CIC identity columns
//! - [`cic_column`]: stamps the CIC metadata onto a schema field at CREATE-table time
//! - [`ReservedRange`]: a reserved range of identity values plus the arithmetic to enumerate it
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
    /// Parsed from `delta.identity.v2.allowExplicitInsert` and surfaced for callers that inspect
    /// column metadata, but **not yet enforced**: the connector always generates identity values
    /// today, and [`cic_column`] cannot stamp this key.
    pub allow_explicit_insert: bool,
}

/// A borrowed view of a Concurrent Identity Column, surfaced by
/// [`Transaction::concurrent_identity_columns`](crate::transaction::Transaction::concurrent_identity_columns).
///
/// A connector reserves values from its sequence service for [`Self::sequence_id`], generates them
/// from the declared [`Self::start`] / [`Self::step`] (reusing [`ReservedRange`] for the
/// overflow-checked arithmetic), fills the [`Self::column_name`] column into its batch itself, and
/// then acknowledges via
/// [`Transaction::ack_concurrent_identity_columns`](crate::transaction::Transaction::ack_concurrent_identity_columns).
/// Kernel neither reserves nor inserts values.
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

/// A reserved, inclusive range of identity values.
///
/// Consumers emit `range_start + i * step` for `i` in `[0, count)` and must not assume
/// `range_start <= range_end`, since a negative step produces a descending range.
#[derive(Debug, Clone, PartialEq)]
pub struct ReservedRange {
    /// The first value in the reserved range (inclusive).
    pub range_start: i64,
    /// The last value in the reserved range (inclusive).
    pub range_end: i64,
    /// The step between consecutive identity values.
    pub step: i64,
}

// Classification of a range's available row count.
enum CountResult {
    /// The range is structurally invalid (e.g. zero step).
    Malformed,
    /// The range is well-formed but its row count cannot be represented in i64.
    UnrepresentableRowCount,
    Ok(u64),
}

impl ReservedRange {
    /// Returns the number of distinct identity values in this range.
    ///
    /// Returns 0 for any range that cannot produce values. Callers that need to distinguish these
    /// cases should call [`Self::values`], which surfaces them as errors.
    pub fn count(&self) -> u64 {
        match self.count_inner() {
            CountResult::Ok(n) => n,
            CountResult::Malformed | CountResult::UnrepresentableRowCount => 0,
        }
    }

    /// Returns `count` consecutive identity values starting at `offset` rows into the range. That
    /// is, value `i` of the result is `range_start + step * (offset + i)`.
    ///
    /// A connector pooling a reservation across several batches tracks its own `offset` and calls
    /// this per batch; kernel keeps the overflow-checked arithmetic so the connector need not
    /// reimplement it.
    ///
    /// # Errors
    ///
    /// - "malformed reservation" if the range is structurally invalid (zero step, sign-mismatched
    ///   bounds, or a range whose width overflows i64).
    /// - "row count overflow" if the range is well-formed but too large to enumerate (row count + 1
    ///   does not fit in i64).
    /// - "reservation exhausted" if `offset + count` exceeds the row count.
    /// - i64 overflow on the per-element `range_start + step * stride` arithmetic.
    pub fn values(&self, column_name: &str, offset: u64, count: u64) -> DeltaResult<Vec<i64>> {
        let available = match self.count_inner() {
            CountResult::Malformed => {
                return Err(Error::generic(format!(
                    "identity column '{}': malformed reservation \
                     (step={}, range_start={}, range_end={})",
                    column_name, self.step, self.range_start, self.range_end,
                )));
            }
            CountResult::UnrepresentableRowCount => {
                return Err(Error::generic(format!(
                    "identity column '{}': row count overflow \
                     -- range too large to enumerate (range_start={}, range_end={}, step={})",
                    column_name, self.range_start, self.range_end, self.step,
                )));
            }
            CountResult::Ok(n) => n,
        };
        let end = offset.checked_add(count).ok_or_else(|| {
            Error::generic(format!(
                "identity column '{column_name}': offset + count overflows u64"
            ))
        })?;
        if end > available {
            return Err(Error::generic(format!(
                "identity column '{column_name}' reservation exhausted: offset {offset} + count \
                 {count} > available {available}"
            )));
        }
        let mut out = Vec::with_capacity(count as usize);
        for i in 0..count {
            let stride: i64 = (offset + i).try_into().map_err(|_| {
                Error::generic(format!(
                    "identity column '{column_name}': index does not fit into i64"
                ))
            })?;
            let step_times = self.step.checked_mul(stride).ok_or_else(|| {
                Error::generic(format!(
                    "identity column '{column_name}': step * index overflows i64"
                ))
            })?;
            let v = self.range_start.checked_add(step_times).ok_or_else(|| {
                Error::generic(format!(
                    "identity column '{column_name}': value overflows i64"
                ))
            })?;
            out.push(v);
        }
        Ok(out)
    }

    /// Validates a range and computes its row count.
    fn count_inner(&self) -> CountResult {
        if self.step == 0 {
            return CountResult::Malformed;
        }
        let Some(range) = self.range_end.checked_sub(self.range_start) else {
            return CountResult::Malformed;
        };
        if (range > 0 && self.step < 0) || (range < 0 && self.step > 0) {
            return CountResult::Malformed;
        }
        // step != 0 and signs match. checked_div catches `i64::MIN / -1`.
        let Some(q) = range.checked_div(self.step) else {
            return CountResult::UnrepresentableRowCount;
        };
        match q.checked_add(1).and_then(|n| u64::try_from(n).ok()) {
            Some(n) => CountResult::Ok(n),
            None => CountResult::UnrepresentableRowCount,
        }
    }
}

/// Builds a CIC identity column field with all three required metadata keys stamped on it.
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
            ColumnMetadataKey::IdentityCicSequenceId
                .as_ref()
                .to_string(),
            MetadataValue::String(sequence_id.into()),
        ),
        (
            ColumnMetadataKey::IdentityCicStart.as_ref().to_string(),
            MetadataValue::Number(start),
        ),
        (
            ColumnMetadataKey::IdentityCicStep.as_ref().to_string(),
            MetadataValue::Number(step),
        ),
    ])
}

/// Scans the (top-level) schema for Concurrent Identity Columns (CIC).
///
/// A column is a CIC if it has the `delta.identity.v2.sequenceId` metadata key. When that
/// key is present, `delta.identity.v2.start` and `delta.identity.v2.step` are also required.
///
/// Returns owned [`IdentityColumnInfo`] for each detected CIC, or an error if required metadata is
/// missing or malformed. `Transaction::concurrent_identity_columns` exposes a borrowed-view
/// equivalent.
pub fn detect_identity_columns(schema: &SchemaRef) -> DeltaResult<Vec<IdentityColumnInfo>> {
    let mut result = Vec::new();
    for field in schema.fields() {
        if field
            .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
            .is_none()
        {
            continue;
        }
        result.push(IdentityColumnInfo {
            column_name: field.name().to_string(),
            sequence_id: get_required_string(field, &ColumnMetadataKey::IdentityCicSequenceId)?,
            start: get_required_i64(field, &ColumnMetadataKey::IdentityCicStart)?,
            step: get_required_i64(field, &ColumnMetadataKey::IdentityCicStep)?,
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
            .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
            .is_none()
        {
            continue;
        }
        result.push(ConcurrentIdentityColumn {
            column_name: field.name(),
            sequence_id: get_required_str(field, &ColumnMetadataKey::IdentityCicSequenceId)?,
            start: get_required_i64(field, &ColumnMetadataKey::IdentityCicStart)?,
            step: get_required_i64(field, &ColumnMetadataKey::IdentityCicStep)?,
            allow_explicit_insert: parse_allow_explicit_insert(field)?,
        });
    }
    Ok(result)
}

/// Validates every top-level Concurrent Identity Column in `schema`, returning whether any exist.
///
/// Shared by the CREATE and ALTER paths. Each top-level CIC column must be a non-nullable `LONG`
/// with a non-zero step, must not also carry legacy `delta.identity.*` metadata, and must not be a
/// partition column. CIC is only supported at the top level, so CIC metadata found on any nested
/// field is rejected.
///
/// # Errors
///
/// Returns an error describing the first violation, or malformed CIC metadata (see
/// [`detect_identity_columns`]).
pub(crate) fn validate_cic_columns(
    schema: &SchemaRef,
    partition_columns: &[String],
) -> DeltaResult<bool> {
    const LEGACY_KEYS: &[ColumnMetadataKey] = &[
        ColumnMetadataKey::IdentityStart,
        ColumnMetadataKey::IdentityStep,
        ColumnMetadataKey::IdentityHighWaterMark,
    ];
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
        for legacy in LEGACY_KEYS {
            if field.get_config_value(legacy).is_some() {
                return Err(Error::generic(format!(
                    "Identity column '{}' carries both CIC metadata and legacy '{}'. \
                     These two cannot be mixed.",
                    info.column_name,
                    legacy.as_ref(),
                )));
            }
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

/// Rejects CIC `sequenceId` metadata found on any field nested inside `data_type`.
fn reject_nested_cic(data_type: &DataType) -> DeltaResult<()> {
    match data_type {
        DataType::Struct(fields) => {
            for field in fields.fields() {
                if field
                    .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
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

/// Parses the optional `delta.identity.v2.allowExplicitInsert` flag (default false).
fn parse_allow_explicit_insert(field: &StructField) -> DeltaResult<bool> {
    match field.get_config_value(&ColumnMetadataKey::IdentityCicAllowExplicitInsert) {
        Some(MetadataValue::Boolean(b)) => Ok(*b),
        Some(MetadataValue::String(s)) => s.parse::<bool>().map_err(|_| {
            Error::generic(format!(
                "Identity column '{}': invalid boolean for '{}': {s}",
                field.name(),
                ColumnMetadataKey::IdentityCicAllowExplicitInsert.as_ref(),
            ))
        }),
        Some(other) => Err(Error::generic(format!(
            "Identity column '{}': expected boolean for '{}', got: {other}",
            field.name(),
            ColumnMetadataKey::IdentityCicAllowExplicitInsert.as_ref(),
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
            field.get_config_value(&ColumnMetadataKey::IdentityCicSequenceId),
            Some(&MetadataValue::String("seq-123".to_string()))
        );
        assert_eq!(
            field.get_config_value(&ColumnMetadataKey::IdentityCicStart),
            Some(&MetadataValue::Number(5))
        );
        assert_eq!(
            field.get_config_value(&ColumnMetadataKey::IdentityCicStep),
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
            ColumnMetadataKey::IdentityCicAllowExplicitInsert
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
            (ColumnMetadataKey::IdentityCicSequenceId, MetadataValue::String("seq-1".to_string())),
            (ColumnMetadataKey::IdentityCicStep, MetadataValue::Number(1)),
        ],
        "delta.identity.v2.start",
    )]
    #[case::missing_step(
        &[
            (ColumnMetadataKey::IdentityCicSequenceId, MetadataValue::String("seq-1".to_string())),
            (ColumnMetadataKey::IdentityCicStart, MetadataValue::Number(1)),
        ],
        "delta.identity.v2.step",
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

    // === ReservedRange ===

    // Note: cases that expect 0 fall into two distinct internal categories (malformed vs.
    // unrepresentable row count).
    #[rstest::rstest]
    #[case::step_1(1, 10, 1, 10)]
    #[case::step_greater_than_1(0, 20, 5, 5)]
    #[case::negative_step(10, 0, -2, 6)]
    #[case::step_zero_is_invalid(1, 10, 0, 0)]
    #[case::single_value(5, 5, 1, 1)]
    #[case::positive_range_negative_step_is_invalid(1, 10, -1, 0)]
    #[case::negative_range_positive_step_is_invalid(10, 1, 1, 0)]
    #[case::full_i64_range_overflows_to_zero(i64::MIN, i64::MAX, 1, 0)]
    #[case::near_i64_max_step_1(i64::MAX - 4, i64::MAX, 1, 5)]
    #[case::near_i64_min_step_1(i64::MIN, i64::MIN + 4, 1, 5)]
    #[case::row_count_plus_one_overflows_to_zero(0, i64::MAX, 1, 0)]
    #[case::division_overflow_i64_min_div_neg_one(0, i64::MIN, -1, 0)]
    fn range_count(
        #[case] range_start: i64,
        #[case] range_end: i64,
        #[case] step: i64,
        #[case] expected: u64,
    ) {
        let r = ReservedRange {
            range_start,
            range_end,
            step,
        };
        assert_eq!(r.count(), expected);
    }

    fn res(range_start: i64, range_end: i64, step: i64) -> ReservedRange {
        ReservedRange {
            range_start,
            range_end,
            step,
        }
    }

    #[rstest::rstest]
    // Step 1 produces sequential values across multiple offsets within the range.
    #[case::step_1_first_chunk(res(1, 10, 1), 0, 3, vec![1, 2, 3])]
    #[case::step_1_middle_chunk(res(1, 10, 1), 3, 4, vec![4, 5, 6, 7])]
    #[case::step_1_last_value(res(1, 10, 1), 9, 1, vec![10])]
    // Step >1 strides correctly.
    #[case::step_5_full(res(0, 20, 5), 0, 5, vec![0, 5, 10, 15, 20])]
    #[case::step_5_partial(res(0, 20, 5), 2, 2, vec![10, 15])]
    // Negative step descends.
    #[case::neg_step_full(res(10, 0, -2), 0, 6, vec![10, 8, 6, 4, 2, 0])]
    #[case::neg_step_offset(res(10, 0, -2), 1, 3, vec![8, 6, 4])]
    // Exact-fit is allowed.
    #[case::exact_fit(res(1, 3, 1), 0, 3, vec![1, 2, 3])]
    fn range_values_returns_expected(
        #[case] r: ReservedRange,
        #[case] offset: u64,
        #[case] count: u64,
        #[case] expected: Vec<i64>,
    ) {
        assert_eq!(r.values("id", offset, count).unwrap(), expected);
    }

    #[test]
    fn range_values_rejects_exhausted_range() {
        let r = res(1, 3, 1); // count = 3
        let err = r.values("id", 0, 4).unwrap_err();
        assert!(
            err.to_string().contains("reservation exhausted"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn range_values_rejects_request_past_capacity_at_i64_boundary() {
        // count() returns 2 for this range (i64::MAX - 2, i64::MAX), so values(0, 3) trips the
        // bounds check rather than the per-element arithmetic.
        let r = res(i64::MAX - 2, i64::MAX, 2);
        let err = r.values("id", 0, 3).unwrap_err();
        assert!(
            err.to_string().contains("reservation exhausted"),
            "unexpected: {err}"
        );
    }

    #[rstest::rstest]
    #[case::zero_step(1, 10, 0)]
    #[case::ascending_range_negative_step(1, 10, -1)]
    #[case::descending_range_positive_step(10, 1, 1)]
    #[case::full_i64_range(i64::MIN, i64::MAX, 1)]
    fn range_values_rejects_malformed_range(
        #[case] range_start: i64,
        #[case] range_end: i64,
        #[case] step: i64,
    ) {
        let r = res(range_start, range_end, step);
        let err = r.values("id", 0, 1).unwrap_err();
        assert!(
            err.to_string().contains("malformed reservation"),
            "unexpected: {err}"
        );
    }

    #[rstest::rstest]
    #[case::positive_step(0, i64::MAX, 1)]
    #[case::negative_step(0, i64::MIN, -1)]
    fn range_values_rejects_unrepresentable_row_count(
        #[case] range_start: i64,
        #[case] range_end: i64,
        #[case] step: i64,
    ) {
        // Range fits in i64, step is nonzero, signs match. But enumerating it would overflow i64
        // either at `range / step` (the negative case is i64::MIN / -1) or at
        // `(range / step) + 1` (the positive case).
        let r = res(range_start, range_end, step);
        let err = r.values("id", 0, 1).unwrap_err();
        assert!(
            err.to_string().contains("row count overflow"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn exhausted_range_error_includes_column_name() {
        let r = res(1, 3, 1);
        let err = r.values("user_id", 0, 4).unwrap_err();
        assert!(
            err.to_string().contains("user_id"),
            "error should name the column: {err}"
        );
    }
}
