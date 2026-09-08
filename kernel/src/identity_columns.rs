//! Support for Concurrent Identity Columns (CIC).
//!
//! Concurrent Identity Columns store their high water mark in a UC Sequence Service instead of
//! Delta metadata, allowing multiple concurrent writers to generate unique BIGINT identity values
//! without conflicting.
//!
//! This module provides:
//! - [`IdentityColumnInfo`]: metadata about a CIC identity column detected from schema
//! - [`IdentityReservation`]: a reserved range of identity values
//! - [`IdentityColumnFiller`]: stateful helper that hands out identity values per batch
//! - [`SequenceReserver`]: sync trait for reserving identity value ranges (implemented outside
//!   kernel, e.g. in `delta-kernel-unity-catalog`)
//! - [`detect_identity_columns`]: scans a schema for CIC identity columns

use std::collections::HashMap;
use std::sync::Arc;

use crate::expressions::{ArrayData, ColumnName};
use crate::schema::{
    ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
use crate::{DeltaResult, EngineData, Error};

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
    /// Explicit inserts are not allocated from the sequence and are not checked for uniqueness.
    pub allow_explicit_insert: bool,
}

/// A raw reserved range returned by [`SequenceReserver::reserve_ids`].
///
/// Contains only the range bounds from the sequence service. Column name and step are
/// schema-level concerns and are added when constructing an [`IdentityReservation`].
#[derive(Debug, Clone, PartialEq)]
pub struct ReservedRange {
    /// The first value in the reserved range (inclusive).
    pub range_start: i64,
    /// The last value in the reserved range (inclusive).
    pub range_end: i64,
}

/// A reserved range of identity values for a specific column, ready for the engine to use.
#[derive(Debug, Clone, PartialEq)]
pub struct IdentityReservation {
    /// The column name this reservation is for.
    pub column_name: String,
    /// The first value in the reserved range (inclusive).
    pub range_start: i64,
    /// The last value in the reserved range (inclusive).
    pub range_end: i64,
    /// The step between consecutive identity values.
    pub step: i64,
}

/// Internal classification of a reservation's available row count, used by
/// [`IdentityReservation::values`] to emit precise errors.
enum CountResult {
    /// The reservation is structurally invalid (`step == 0`, range
    /// subtraction overflows i64, or range and step have inconsistent signs).
    Malformed,
    /// The reservation is well-formed but its row count cannot be represented
    /// in i64 -- either because `range / step` overflows (the `i64::MIN / -1`
    /// case) or because `(range / step) + 1` overflows. Only reachable with a
    /// service that hands out near-`i64::MAX/MIN` ranges.
    UnrepresentableRowCount,
    /// The reservation is well-formed and produces this many rows.
    Ok(u64),
}

impl IdentityReservation {
    /// Returns the number of distinct identity values in this reservation.
    ///
    /// Returns 0 for any reservation that cannot produce values: a malformed
    /// reservation (zero step, sign-mismatched bounds, or a range so large
    /// that its width overflows i64) or a well-formed reservation whose row
    /// count cannot fit in i64. Callers that need to distinguish these cases
    /// should call [`Self::values`], which surfaces them as typed errors.
    pub fn count(&self) -> u64 {
        match self.count_inner() {
            CountResult::Ok(n) => n,
            CountResult::Malformed | CountResult::UnrepresentableRowCount => 0,
        }
    }

    /// Single source of truth for validating a reservation and computing its
    /// row count. Both [`Self::count`] and [`Self::values`] consume the result.
    /// Returns:
    ///
    /// - `Malformed` if `step == 0`, `range_end - range_start` overflows i64, or range and step
    ///   have inconsistent signs.
    /// - `UnrepresentableRowCount` if the reservation is well-formed but its row count cannot be
    ///   represented in i64. Two paths reach this: the `i64::MIN / -1` division overflow, and
    ///   `(range / step) + 1` overflow.
    /// - `Ok(n)` with the row count otherwise.
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

    /// Returns `count` consecutive identity values starting at `offset` rows into the
    /// reservation. That is, value `i` of the result is `range_start + step * (offset + i)`.
    ///
    /// # Errors
    ///
    /// - "malformed reservation" if the reservation is structurally invalid (zero step,
    ///   sign-mismatched bounds, or a range whose width overflows i64).
    /// - "row count overflow" if the reservation is well-formed but too large to enumerate (row
    ///   count + 1 does not fit in i64).
    /// - "reservation exhausted" if `offset + count` exceeds the row count.
    /// - i64 overflow on the per-element `range_start + step * stride` arithmetic.
    pub fn values(&self, offset: u64, count: u64) -> DeltaResult<Vec<i64>> {
        let available = match self.count_inner() {
            CountResult::Malformed => {
                return Err(Error::generic(format!(
                    "identity column '{}': malformed reservation \
                     (step={}, range_start={}, range_end={})",
                    self.column_name, self.step, self.range_start, self.range_end,
                )));
            }
            CountResult::UnrepresentableRowCount => {
                return Err(Error::generic(format!(
                    "identity column '{}': row count overflow \
                     -- range too large to enumerate (range_start={}, range_end={}, step={})",
                    self.column_name, self.range_start, self.range_end, self.step,
                )));
            }
            CountResult::Ok(n) => n,
        };
        let end = offset.checked_add(count).ok_or_else(|| {
            Error::generic(format!(
                "identity column '{}': offset + count overflows u64",
                self.column_name
            ))
        })?;
        if end > available {
            return Err(Error::generic(format!(
                "identity column '{}' reservation exhausted: offset {} + count {} > available {}",
                self.column_name, offset, count, available
            )));
        }
        let mut out = Vec::with_capacity(count as usize);
        for i in 0..count {
            let stride: i64 = (offset + i).try_into().map_err(|_| {
                Error::generic(format!(
                    "identity column '{}': index does not fit into i64",
                    self.column_name
                ))
            })?;
            let step_times = self.step.checked_mul(stride).ok_or_else(|| {
                Error::generic(format!(
                    "identity column '{}': step * index overflows i64",
                    self.column_name
                ))
            })?;
            let v = self.range_start.checked_add(step_times).ok_or_else(|| {
                Error::generic(format!(
                    "identity column '{}': value overflows i64",
                    self.column_name
                ))
            })?;
            out.push(v);
        }
        Ok(out)
    }
}

/// Stateful helper that hands out identity values per batch during a write.
///
/// A filler wraps a set of [`IdentityReservation`]s (one per identity column detected in
/// the table schema) and tracks how many values have already been consumed for each column.
/// The engine constructs a filler once per write transaction and calls
/// [`Self::next_values`] (or, with the `arrow-expression` feature, [`Self::fill_arrow_batch`])
/// for every batch of rows it writes.
///
/// # Example
///
/// ```no_run
/// use delta_kernel::identity_columns::{IdentityColumnFiller, IdentityReservation};
///
/// let reservations = vec![IdentityReservation {
///     column_name: "id".to_string(),
///     range_start: 1,
///     range_end: 100,
///     step: 1,
/// }];
/// let mut filler = IdentityColumnFiller::new(reservations).unwrap();
///
/// // First batch of 3 rows ->
/// let v1 = filler.next_values("id", 3).unwrap();
/// assert_eq!(v1, vec![1, 2, 3]);
///
/// // Second batch of 2 rows -> cursor advanced, continues from 4.
/// let v2 = filler.next_values("id", 2).unwrap();
/// assert_eq!(v2, vec![4, 5]);
/// ```
#[derive(Debug, Clone)]
pub struct IdentityColumnFiller {
    /// column_name -> (reservation, rows already consumed)
    state: HashMap<String, (IdentityReservation, u64)>,
}

impl IdentityColumnFiller {
    /// Creates a new filler from a list of reservations.
    ///
    /// # Errors
    ///
    /// Returns an error if two reservations target the same column. Silent
    /// overwrite would corrupt one of the columns at write time.
    pub fn new(reservations: Vec<IdentityReservation>) -> DeltaResult<Self> {
        use std::collections::hash_map::Entry;
        let mut state = HashMap::with_capacity(reservations.len());
        for r in reservations {
            match state.entry(r.column_name.clone()) {
                Entry::Occupied(e) => {
                    return Err(Error::generic(format!(
                        "duplicate reservation for identity column '{}'",
                        e.key()
                    )));
                }
                Entry::Vacant(v) => {
                    v.insert((r, 0));
                }
            }
        }
        Ok(Self { state })
    }

    /// Returns the next `count` identity values for the given column and advances
    /// that column's cursor.
    ///
    /// # Errors
    ///
    /// Returns an error if the column is not in the filler or the reservation is
    /// exhausted.
    pub fn next_values(&mut self, column_name: &str, count: u64) -> DeltaResult<Vec<i64>> {
        let (reservation, cursor) = self.state.get_mut(column_name).ok_or_else(|| {
            Error::generic(format!(
                "identity column '{column_name}' has no reservation in filler"
            ))
        })?;
        let values = reservation.values(*cursor, count)?;
        *cursor += count;
        Ok(values)
    }

    /// Number of rows already consumed for the given column, or `None` if the filler
    /// has no reservation for it. Useful for diagnostics and tests.
    pub fn rows_consumed(&self, column_name: &str) -> Option<u64> {
        self.state.get(column_name).map(|(_, cursor)| *cursor)
    }

    /// Fills the identity columns of a batch by splicing freshly-generated values in at the
    /// positions dictated by `target_schema`.
    ///
    /// Expected shape of the input:
    ///
    /// - `input` contains columns for every NON-identity field in `target_schema`, matched by name.
    ///   A missing non-identity column is an error. Extra columns in `input` that do not appear in
    ///   `target_schema` are ignored, not rejected.
    /// - `input` MUST NOT already carry the identity columns -- this helper is the one that fills
    ///   them.
    ///
    /// On success, returns a new [`RecordBatch`] whose columns match the order of
    /// `target_schema`: identity columns are populated from the filler's reservations (one
    /// call to [`Self::next_values`] per column), and non-identity columns are taken from
    /// `input`. The cursor of each identity column advances by `input.num_rows()`.
    ///
    /// [`RecordBatch`]: crate::arrow::array::RecordBatch
    #[cfg(feature = "arrow-expression")]
    pub fn fill_arrow_batch(
        &mut self,
        input: &crate::arrow::array::RecordBatch,
        target_schema: &crate::schema::StructType,
    ) -> DeltaResult<crate::arrow::array::RecordBatch> {
        use std::sync::Arc;

        use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch};
        use crate::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
        use crate::engine::arrow_conversion::TryIntoArrow;

        let row_count = input.num_rows() as u64;
        let input_schema = input.schema();

        let mut fields: Vec<ArrowField> = Vec::with_capacity(target_schema.fields().len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

        for target_field in target_schema.fields() {
            let is_identity = target_field
                .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
                .is_some();
            let arrow_field: ArrowField = target_field.try_into_arrow()?;
            fields.push(arrow_field);

            if is_identity {
                if input_schema.index_of(target_field.name()).is_ok() {
                    return Err(Error::generic(format!(
                        "identity column '{}' must not be present in the input batch \
                         -- kernel fills it",
                        target_field.name()
                    )));
                }
                let values = self.next_values(target_field.name(), row_count)?;
                let array = Int64Array::from(values);
                columns.push(Arc::new(array) as ArrayRef);
            } else {
                let idx = input_schema.index_of(target_field.name()).map_err(|_| {
                    Error::generic(format!(
                        "input batch is missing non-identity column '{}'",
                        target_field.name()
                    ))
                })?;
                columns.push(input.column(idx).clone());
            }
        }

        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
            .map_err(|e| Error::generic(format!("failed to assemble filled batch: {e}")))
    }

    /// Appends generated identity columns to engine-owned data.
    ///
    /// `target_schema` identifies the identity columns to generate. Every non-identity field in
    /// the target schema must already exist in `input`, and identity fields must not exist there.
    /// Extra input fields are retained. The returned data contains the original input columns in
    /// their original order, followed by identity columns in their target-schema order.
    ///
    /// Unlike [`Self::fill_arrow_batch`], this method cannot place generated columns at arbitrary
    /// positions because [`EngineData::append_columns`] only exposes append semantics.
    ///
    /// # Errors
    ///
    /// Returns an error if an expected non-identity field is missing, an identity field is already
    /// present, generating identity values fails, or the engine cannot append the new columns.
    pub fn fill_engine_batch(
        &mut self,
        input: &dyn EngineData,
        target_schema: &SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let row_count = input.len() as u64;
        let mut added_fields = Vec::new();
        let mut added_columns: Vec<ArrayData> = Vec::new();

        for target_field in target_schema.fields() {
            let column_name = ColumnName::new([target_field.name().as_str()]);
            let is_identity = target_field
                .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
                .is_some();
            if is_identity && input.has_field(&column_name) {
                return Err(Error::generic(format!(
                    "identity column '{}' must not be present in the input batch \
                     -- kernel fills it",
                    target_field.name()
                )));
            }
            if !is_identity && !input.has_field(&column_name) {
                return Err(Error::generic(format!(
                    "input batch is missing non-identity column '{}'",
                    target_field.name()
                )));
            }
        }

        for target_field in target_schema.fields() {
            if target_field
                .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
                .is_none()
            {
                continue;
            }
            let values = self.next_values(target_field.name(), row_count)?;
            added_columns.push(ArrayData::from(values));
            added_fields.push(target_field.clone());
        }

        let added_schema = Arc::new(StructType::try_new(added_fields)?);
        input.append_columns(added_schema, added_columns)
    }
}

/// Sync trait for reserving identity value ranges from a sequence service.
///
/// Implemented outside kernel (e.g. in `delta-kernel-unity-catalog`) with the actual UC
/// service call and async bridge. Kernel code calls this trait synchronously without any
/// async runtime dependency.
///
/// Implementations must be `Send + Sync` so a single reserver can be shared across worker
/// threads in distributed writers.
pub trait SequenceReserver: Send + Sync {
    /// Reserves `count` identity values for the given sequence.
    ///
    /// `expected_step` is the step persisted in the schema metadata. Implementations
    /// MUST verify it matches the step reported by the sequence service and fail with
    /// an error on mismatch -- the step is immutable at the service, so a difference
    /// signals corrupted or drifted state.
    ///
    /// Returns a [`ReservedRange`] with the start and end bounds of the reserved range.
    ///
    /// # Errors
    ///
    /// Returns an error if the reservation fails (e.g. sequence not found, step
    /// mismatch, service unavailable).
    fn reserve_ids(
        &self,
        sequence_id: &str,
        expected_step: i64,
        count: u64,
    ) -> DeltaResult<ReservedRange>;
}

/// Builds a CIC identity column field with all three required metadata keys stamped on it.
///
/// Engines call this after allocating a sequence from the UC Identity Sequence Service
/// (`CreateIdentitySequence`), passing in the service-minted `sequence_id` along with the
/// `start` and `step` the sequence was created with. The returned [`StructField`] is a
/// non-nullable `LONG` column ready to be passed to `create_table`.
pub fn identity_column_cic(
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

/// Extracts a required string metadata value from a struct field.
fn get_required_string(
    field: &crate::schema::StructField,
    key: &ColumnMetadataKey,
) -> DeltaResult<String> {
    match field.get_config_value(key) {
        Some(MetadataValue::String(s)) => Ok(s.clone()),
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
fn get_required_i64(
    field: &crate::schema::StructField,
    key: &ColumnMetadataKey,
) -> DeltaResult<i64> {
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

/// Scans the schema for Concurrent Identity Columns (CIC).
///
/// A column is a CIC identity column if it has the `delta.identity.v2.sequenceId` metadata key.
/// When that key is present, `delta.identity.v2.start` and `delta.identity.v2.step` are also
/// required.
///
/// Returns a list of [`IdentityColumnInfo`] for each detected CIC identity column, or an error
/// if required metadata is missing or malformed.
pub fn detect_identity_columns(schema: &SchemaRef) -> DeltaResult<Vec<IdentityColumnInfo>> {
    let mut result = Vec::new();
    for field in schema.fields() {
        // A CIC identity column is identified by having a sequenceId in its metadata.
        if field
            .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
            .is_none()
        {
            continue;
        }

        let sequence_id = get_required_string(field, &ColumnMetadataKey::IdentityCicSequenceId)?;
        let start = get_required_i64(field, &ColumnMetadataKey::IdentityCicStart)?;
        let step = get_required_i64(field, &ColumnMetadataKey::IdentityCicStep)?;

        let allow_explicit_insert =
            match field.get_config_value(&ColumnMetadataKey::IdentityCicAllowExplicitInsert) {
                Some(MetadataValue::Boolean(b)) => *b,
                Some(MetadataValue::String(s)) => s.parse::<bool>().map_err(|_| {
                    Error::generic(format!(
                        "Identity column '{}': invalid boolean for '{}': {s}",
                        field.name(),
                        ColumnMetadataKey::IdentityCicAllowExplicitInsert.as_ref(),
                    ))
                })?,
                Some(other) => {
                    return Err(Error::generic(format!(
                        "Identity column '{}': expected boolean for '{}', got: {other}",
                        field.name(),
                        ColumnMetadataKey::IdentityCicAllowExplicitInsert.as_ref(),
                    )));
                }
                // Default to false if not present
                None => false,
            };

        result.push(IdentityColumnInfo {
            column_name: field.name().to_string(),
            sequence_id,
            start,
            step,
            allow_explicit_insert,
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    #[test]
    fn identity_column_cic_stamps_all_three_metadata_keys() {
        let field = identity_column_cic("id", "seq-123", 5, 2);
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
                identity_column_cic("id", "seq-123", 1, 1),
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
                identity_column_cic("id", "seq-1", 1, 1),
                StructField::new("name", DataType::STRING, true),
                identity_column_cic("row_id", "seq-2", 100, 10),
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
        let field = identity_column_cic("id", "seq-1", 1, 1).add_metadata(vec![(
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

    // Note: cases that expect 0 fall into two distinct internal categories
    // (malformed vs. unrepresentable row count). count() collapses both to 0;
    // see the matching `reservation_values_rejects_*` rstests for the per-case
    // diagnostic that values() emits.
    #[rstest::rstest]
    #[case::step_1(1, 10, 1, 10)]
    #[case::step_greater_than_1(0, 20, 5, 5)] // values: 0, 5, 10, 15, 20
    #[case::negative_step(10, 0, -2, 6)] // values: 10, 8, 6, 4, 2, 0
    #[case::step_zero_is_invalid(1, 10, 0, 0)]
    #[case::single_value(5, 5, 1, 1)]
    #[case::positive_range_negative_step_is_invalid(1, 10, -1, 0)]
    #[case::negative_range_positive_step_is_invalid(10, 1, 1, 0)]
    #[case::full_i64_range_overflows_to_zero(i64::MIN, i64::MAX, 1, 0)]
    #[case::near_i64_max_step_1(i64::MAX - 4, i64::MAX, 1, 5)]
    #[case::near_i64_min_step_1(i64::MIN, i64::MIN + 4, 1, 5)]
    #[case::row_count_plus_one_overflows_to_zero(0, i64::MAX, 1, 0)]
    #[case::division_overflow_i64_min_div_neg_one(0, i64::MIN, -1, 0)]
    fn reservation_count(
        #[case] range_start: i64,
        #[case] range_end: i64,
        #[case] step: i64,
        #[case] expected: u64,
    ) {
        let r = IdentityReservation {
            column_name: "id".to_string(),
            range_start,
            range_end,
            step,
        };
        assert_eq!(r.count(), expected);
    }

    fn res(name: &str, range_start: i64, range_end: i64, step: i64) -> IdentityReservation {
        IdentityReservation {
            column_name: name.to_string(),
            range_start,
            range_end,
            step,
        }
    }

    #[rstest::rstest]
    // Step 1 produces sequential values across multiple offsets within the reservation.
    #[case::step_1_first_chunk(res("id", 1, 10, 1), 0, 3, vec![1, 2, 3])]
    #[case::step_1_middle_chunk(res("id", 1, 10, 1), 3, 4, vec![4, 5, 6, 7])]
    #[case::step_1_last_value(res("id", 1, 10, 1), 9, 1, vec![10])]
    // Step >1 strides correctly.
    #[case::step_5_full(res("id", 0, 20, 5), 0, 5, vec![0, 5, 10, 15, 20])]
    #[case::step_5_partial(res("id", 0, 20, 5), 2, 2, vec![10, 15])]
    // Negative step descends.
    #[case::neg_step_full(res("id", 10, 0, -2), 0, 6, vec![10, 8, 6, 4, 2, 0])]
    #[case::neg_step_offset(res("id", 10, 0, -2), 1, 3, vec![8, 6, 4])]
    // Exact-fit is allowed.
    #[case::exact_fit(res("id", 1, 3, 1), 0, 3, vec![1, 2, 3])]
    fn reservation_values_returns_expected(
        #[case] r: IdentityReservation,
        #[case] offset: u64,
        #[case] count: u64,
        #[case] expected: Vec<i64>,
    ) {
        assert_eq!(r.values(offset, count).unwrap(), expected);
    }

    #[test]
    fn reservation_values_rejects_exhausted_range() {
        let r = res("id", 1, 3, 1); // count = 3
        let err = r.values(0, 4).unwrap_err();
        assert!(
            err.to_string().contains("reservation exhausted"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn reservation_values_rejects_request_past_capacity_at_i64_boundary() {
        // count() returns 2 for this reservation (i64::MAX - 2, i64::MAX),
        // so values(0, 3) trips the bounds check rather than the per-element
        // arithmetic. Documented here so a future refactor that decouples
        // count() from values() can spot the test it should also update.
        let r = res("id", i64::MAX - 2, i64::MAX, 2);
        let err = r.values(0, 3).unwrap_err();
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
    fn reservation_values_rejects_malformed_reservation(
        #[case] range_start: i64,
        #[case] range_end: i64,
        #[case] step: i64,
    ) {
        let r = res("id", range_start, range_end, step);
        let err = r.values(0, 1).unwrap_err();
        assert!(
            err.to_string().contains("malformed reservation"),
            "unexpected: {err}"
        );
    }

    #[rstest::rstest]
    #[case::positive_step(0, i64::MAX, 1)]
    #[case::negative_step(0, i64::MIN, -1)]
    fn reservation_values_rejects_unrepresentable_row_count(
        #[case] range_start: i64,
        #[case] range_end: i64,
        #[case] step: i64,
    ) {
        // Range fits in i64, step is nonzero, signs match -- the reservation
        // is well-formed -- but enumerating it would overflow i64 either at
        // `range / step` (the negative case is i64::MIN / -1) or at
        // `(range / step) + 1` (the positive case). Distinct from "malformed"
        // (range overflow / sign mismatch) and from "exhausted" (offset +
        // count > available).
        let r = res("id", range_start, range_end, step);
        let err = r.values(0, 1).unwrap_err();
        assert!(
            err.to_string().contains("row count overflow"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn filler_next_values_advances_cursor() {
        let mut f = IdentityColumnFiller::new(vec![res("id", 1, 10, 1)]).unwrap();
        assert_eq!(f.rows_consumed("id"), Some(0));
        assert_eq!(f.next_values("id", 3).unwrap(), vec![1, 2, 3]);
        assert_eq!(f.rows_consumed("id"), Some(3));
        assert_eq!(f.next_values("id", 2).unwrap(), vec![4, 5]);
        assert_eq!(f.rows_consumed("id"), Some(5));
    }

    #[test]
    fn filler_next_values_unknown_column_errors() {
        let mut f = IdentityColumnFiller::new(vec![res("id", 1, 10, 1)]).unwrap();
        let err = f.next_values("not_here", 1).unwrap_err();
        assert!(
            err.to_string().contains("no reservation in filler"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn filler_independent_cursors_per_column() {
        let mut f = IdentityColumnFiller::new(vec![
            res("id", 1, 10, 1),
            res("row_id", 1000, 1090, 10), // 10 values: 1000, 1010, ..., 1090
        ])
        .unwrap();
        assert_eq!(f.next_values("id", 3).unwrap(), vec![1, 2, 3]);
        assert_eq!(f.next_values("row_id", 3).unwrap(), vec![1000, 1010, 1020]);
        // Cursors are independent -- advancing row_id didn't move id.
        assert_eq!(f.next_values("id", 2).unwrap(), vec![4, 5]);
    }

    #[test]
    fn filler_new_rejects_duplicate_column() {
        let err = IdentityColumnFiller::new(vec![res("id", 1, 10, 1), res("id", 100, 200, 1)])
            .unwrap_err();
        assert!(
            err.to_string().contains("duplicate reservation"),
            "unexpected: {err}"
        );
    }

    #[cfg(feature = "arrow-expression")]
    mod arrow_tests {
        use std::sync::Arc;

        use super::*;
        use crate::arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
        use crate::engine::arrow_data::{extract_record_batch, ArrowEngineData};

        fn payload_batch(rows: usize) -> RecordBatch {
            let payload: ArrayRef = Arc::new(StringArray::from(
                (0..rows).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
            ));
            let schema = Arc::new(Schema::new(vec![ArrowField::new(
                "payload",
                ArrowDataType::Utf8,
                true,
            )]));
            RecordBatch::try_new(schema, vec![payload]).unwrap()
        }

        fn target_schema() -> StructType {
            StructType::try_new(vec![
                identity_column_cic("id", "seq-id", 1, 1),
                StructField::new("payload", DataType::STRING, true),
                identity_column_cic("row_id", "seq-row", 1000, 10),
            ])
            .unwrap()
        }

        #[test]
        fn fill_arrow_batch_splices_identity_columns_at_schema_positions() {
            let schema = target_schema();
            let mut filler =
                IdentityColumnFiller::new(vec![res("id", 1, 10, 1), res("row_id", 1000, 1090, 10)])
                    .unwrap();

            let input = payload_batch(3);
            let filled = filler.fill_arrow_batch(&input, &schema).unwrap();

            // Column order matches target_schema: id, payload, row_id
            let schema = filled.schema();
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(names, vec!["id", "payload", "row_id"]);
            assert_eq!(filled.num_rows(), 3);

            // Identity columns populated with generated values
            let id_col = filled
                .column(0)
                .as_any()
                .downcast_ref::<crate::arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(id_col.values(), &[1i64, 2, 3]);
            let row_id_col = filled
                .column(2)
                .as_any()
                .downcast_ref::<crate::arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(row_id_col.values(), &[1000i64, 1010, 1020]);
        }

        #[test]
        fn fill_engine_batch_appends_identity_columns() {
            let schema = Arc::new(target_schema());
            let mut filler =
                IdentityColumnFiller::new(vec![res("id", 1, 10, 1), res("row_id", 1000, 1090, 10)])
                    .unwrap();
            let input = ArrowEngineData::new(payload_batch(3));

            let filled = filler.fill_engine_batch(&input, &schema).unwrap();
            let filled = extract_record_batch(filled.as_ref()).unwrap();

            let names: Vec<&str> = filled
                .schema_ref()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            assert_eq!(names, vec!["payload", "id", "row_id"]);
            assert_eq!(
                filled
                    .column(1)
                    .as_any()
                    .downcast_ref::<crate::arrow::array::Int64Array>()
                    .unwrap()
                    .values(),
                &[1, 2, 3]
            );
            assert_eq!(
                filled
                    .column(2)
                    .as_any()
                    .downcast_ref::<crate::arrow::array::Int64Array>()
                    .unwrap()
                    .values(),
                &[1000, 1010, 1020]
            );
        }

        #[test]
        fn fill_arrow_batch_advances_cursor_across_calls() {
            let schema = target_schema();
            let mut filler =
                IdentityColumnFiller::new(vec![res("id", 1, 100, 1), res("row_id", 0, 9990, 10)])
                    .unwrap();

            let b1 = filler.fill_arrow_batch(&payload_batch(2), &schema).unwrap();
            let b2 = filler.fill_arrow_batch(&payload_batch(3), &schema).unwrap();

            let id_b1 = b1
                .column(0)
                .as_any()
                .downcast_ref::<crate::arrow::array::Int64Array>()
                .unwrap()
                .values()
                .to_vec();
            let id_b2 = b2
                .column(0)
                .as_any()
                .downcast_ref::<crate::arrow::array::Int64Array>()
                .unwrap()
                .values()
                .to_vec();
            assert_eq!(id_b1, vec![1, 2]);
            assert_eq!(id_b2, vec![3, 4, 5]);
        }

        #[test]
        fn fill_arrow_batch_rejects_identity_column_in_input() {
            let schema = target_schema();
            let mut filler =
                IdentityColumnFiller::new(vec![res("id", 1, 10, 1), res("row_id", 1000, 1090, 10)])
                    .unwrap();

            // Construct an input batch that (wrongly) already has the `id` column.
            let ids: ArrayRef = Arc::new(Int32Array::from(vec![0, 0, 0]));
            let payload: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
            let schema_with_id = Arc::new(Schema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("payload", ArrowDataType::Utf8, true),
            ]));
            let bad = RecordBatch::try_new(schema_with_id, vec![ids, payload]).unwrap();

            let err = filler.fill_arrow_batch(&bad, &schema).unwrap_err();
            assert!(
                err.to_string().contains("must not be present in the input"),
                "unexpected: {err}"
            );
        }

        #[test]
        fn fill_arrow_batch_rejects_missing_non_identity_column() {
            let schema = target_schema();
            let mut filler =
                IdentityColumnFiller::new(vec![res("id", 1, 10, 1), res("row_id", 1000, 1090, 10)])
                    .unwrap();

            // Empty input -- missing "payload".
            let empty = RecordBatch::new_empty(Arc::new(Schema::new(Vec::<ArrowField>::new())));
            let err = filler.fill_arrow_batch(&empty, &schema).unwrap_err();
            assert!(
                err.to_string().contains("missing non-identity column"),
                "unexpected: {err}"
            );
        }
    }
}
