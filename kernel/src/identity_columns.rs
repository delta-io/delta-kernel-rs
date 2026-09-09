//! Support for Concurrent Identity Columns (CIC).
//!
//! Concurrent Identity Columns store their high water mark in a UC Sequence Service instead of
//! Delta metadata, allowing multiple concurrent writers to generate unique BIGINT identity values
//! without conflicting.
//!
//! This module provides:
//! - [`IdentityColumnInfo`]: metadata about a CIC identity column detected from schema
//! - [`ReservedRange`]: a reserved range of identity values plus the arithmetic to enumerate it
//! - [`IdentitySequenceState`]: the reservation state machine (queue, counters, claim/reserve/wait
//!   decision, and value fill) that a connector drives
//! - [`detect_identity_columns`]: scans a schema for CIC identity columns
//! - [`cic_column`]: stamps the CIC metadata onto a schema field at CREATE-table time

use std::collections::VecDeque;
use std::sync::Arc;

use crate::expressions::{ArrayData, ColumnName, Expression};
use crate::schema::{
    ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
use crate::{DeltaResult, EngineData, Error, EvaluationHandler};

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
    pub allow_explicit_insert: bool,
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

/// Scans the schema for Concurrent Identity Columns (CIC).
///
/// A column is a CIC if it has the `delta.identity.v2.sequenceId` metadata key. When that
/// key is present, `delta.identity.v2.start` and `delta.identity.v2.step` are also required.
///
/// Returns a list of [`IdentityColumnInfo`] for each detected CIC, or an error if required
/// metadata is missing or malformed.
pub fn detect_identity_columns(schema: &SchemaRef) -> DeltaResult<Vec<IdentityColumnInfo>> {
    let mut result = Vec::new();
    for field in schema.fields() {
        // A CIC identity column is identified by having a sequenceId in its metadata
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

/// A single sequence's portion of a reservation request produced by
/// [`IdentitySequenceState::begin_reserve`].
///
/// It describes what the caller must obtain from its sequence service: `count` values from the
/// sequence identified by `sequence_id`, advancing by `step`. The caller translates this into
/// whatever RPC its service exposes and feeds the resulting range back via
/// [`IdentitySequenceState::complete_reserve`].
#[derive(Debug, Clone, PartialEq)]
pub struct SequenceReservationRequest {
    /// The sequence to reserve from (the column's `delta.identity.v2.sequenceId`).
    pub sequence_id: String,
    /// How many values to reserve.
    pub count: u64,
    /// The step the reserved range must advance by (the column's declared step).
    pub step: i64,
}

/// The action [`IdentitySequenceState::try_claim`] determined is needed to make `count`
/// claimable on every identity column.
#[derive(Debug, PartialEq)]
pub enum ClaimOutcome {
    /// Every column had enough unclaimed values, so `count` values are claimed for the caller,
    /// who should consume that many with [`IdentitySequenceState::fill_engine_batch`].
    Claimed,
    /// At least one column is short. Reserve this many more values from every sequence
    /// (via [`IdentitySequenceState::begin_reserve`]), then retry.
    Reserve(u64),
    /// At least one column is short but an in-flight reservation will cover it. Wait for that
    /// reservation to complete, then retry.
    Wait,
}

/// Reservation state for a table's Concurrent Identity Columns.
///
/// Holds one cursor per CIC column: a queue of [`ReservedRange`]s plus the counters that
/// coordinate concurrent consumers (`available`, in-flight, and claimed) and an error from
/// the most recent failed reservation. A caller obtains ranges from a sequence service and feeds
/// them back.
///
/// # Reserve protocol
///
/// A reservation is a three-step handshake around the caller's RPC:
///
/// 1. [`begin_reserve`](Self::begin_reserve) marks `count` values in flight on every column and
///    returns the per-sequence [`SequenceReservationRequest`]s to fulfill.
/// 2. The caller performs its reservation RPC.
/// 3. [`complete_reserve`](Self::complete_reserve) records the returned ranges (one per column, in
///    cursor order) on success, or [`fail_reserve`](Self::fail_reserve) rolls the in-flight count
///    back on failure.
///
/// # Claiming
///
/// When one state is shared by concurrent consumers, [`try_claim`](Self::try_claim) reserves
/// `count` values for a caller before it fills, so concurrent consumers receive disjoint ranges.
/// Waiting for an in-flight reservation is the caller's responsibility (this type is synchronous);
/// `try_claim` only reports that a wait is needed via [`ClaimOutcome::Wait`].
pub struct IdentitySequenceState {
    cursors: Vec<SequenceCursor>,
}

impl IdentitySequenceState {
    /// Builds a reservation state for every CIC column in `schema`, each starting empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the CIC metadata in `schema` is malformed (see
    /// [`detect_identity_columns`]).
    pub fn from_schema(schema: &SchemaRef) -> DeltaResult<Self> {
        let cursors = detect_identity_columns(schema)?
            .into_iter()
            .map(|info| SequenceCursor {
                column_name: info.column_name,
                sequence_id: info.sequence_id,
                step: info.step,
                queue: VecDeque::new(),
                available: 0,
                inflight: 0,
                claimed: 0,
                error: None,
            })
            .collect();
        Ok(Self { cursors })
    }

    /// Marks `count` values in flight on every column and returns the per-sequence requests the
    /// caller must fulfill.
    pub fn begin_reserve(&mut self, count: u64) -> Vec<SequenceReservationRequest> {
        self.cursors
            .iter_mut()
            .map(|cursor| {
                cursor.inflight += count;
                SequenceReservationRequest {
                    sequence_id: cursor.sequence_id.clone(),
                    count,
                    step: cursor.step,
                }
            })
            .collect()
    }

    /// Records a successful reservation providing one [`ReservedRange`] per column (same order as
    /// the) cursors. Each range is enqueued and `count` moves from in-flight to available.
    ///
    /// `count` must match the value passed to [`begin_reserve`](Self::begin_reserve).
    ///
    /// # Errors
    ///
    /// Returns an error if `ranges` has a different length than the number of columns, or if any
    /// range's step does not match its column's declared step.
    pub fn complete_reserve(&mut self, count: u64, ranges: Vec<ReservedRange>) -> DeltaResult<()> {
        if let Err(message) = Self::validate_reserved(&self.cursors, &ranges) {
            self.roll_back_inflight(count, &message);
            return Err(Error::Generic(message));
        }
        for (cursor, range) in self.cursors.iter_mut().zip(ranges) {
            cursor.queue.push_back(range);
            cursor.available += count;
            cursor.inflight = cursor.inflight.saturating_sub(count);
            cursor.error = None;
        }
        Ok(())
    }

    /// Records a failed reservation. rolls the in-flight `count` back on every column and stores
    /// `message` as the sticky error surfaced by a subsequent [`try_claim`](Self::try_claim).
    ///
    /// `count` must match the value passed to the [`begin_reserve`](Self::begin_reserve) this call
    /// settles.
    pub fn fail_reserve(&mut self, count: u64, message: impl Into<String>) {
        self.roll_back_inflight(count, &message.into());
    }

    /// Attempts to claim `count` values for every column.
    ///
    /// If every column has at least `count` unclaimed values, marks them claimed and returns
    /// [`ClaimOutcome::Claimed`]. Otherwise returns the action needed to make progress
    /// ([`ClaimOutcome::Reserve`] or [`ClaimOutcome::Wait`]) without claiming anything.
    ///
    /// # Errors
    ///
    /// Returns the reservation error if a column is short and its most recent reservation failed
    /// with no in-flight cover.
    pub fn try_claim(&mut self, count: u64) -> DeltaResult<ClaimOutcome> {
        let mut reserve_deficit = 0u64;
        let mut need_wait = false;
        for cursor in self.cursors.iter() {
            let unclaimed = cursor.available.saturating_sub(cursor.claimed);
            if unclaimed >= count {
                continue;
            }
            let projected = (cursor.available + cursor.inflight).saturating_sub(cursor.claimed);
            if projected < count {
                if let Some(msg) = &cursor.error {
                    return Err(Error::Generic(msg.clone()));
                }
                reserve_deficit = reserve_deficit.max(count - projected);
            } else {
                // An in-flight reservation will cover the shortfall.
                need_wait = true;
            }
        }
        Ok(if reserve_deficit > 0 {
            ClaimOutcome::Reserve(reserve_deficit)
        } else if need_wait {
            ClaimOutcome::Wait
        } else {
            for cursor in self.cursors.iter_mut() {
                cursor.claimed += count;
            }
            ClaimOutcome::Claimed
        })
    }

    /// Fills the CIC columns of a batch with generated identity values.
    ///
    /// `target_schema` identifies the CIC columns to generate. Every non-identity field in the
    /// target schema must already exist in `input`, and every identity field must not (this state
    /// fills those). The returned data holds every column of `target_schema` in schema order, so it
    /// is ready to write. Each identity column consumes `input.len()` values from its cursor.
    ///
    /// If a column has fewer than `input.len()` values available it errors. Call
    /// [`try_claim`](Self::try_claim) (and reserve as it directs) first.
    ///
    /// `evaluation_handler` reorders the columns into schema order.
    ///
    /// # Errors
    ///
    /// Returns an error if a non-identity field is missing from `input`, a CIC field is already
    /// present in `input`, a column is short, or the engine cannot construct the result.
    pub fn fill_engine_batch(
        &mut self,
        evaluation_handler: &dyn EvaluationHandler,
        input: &dyn EngineData,
        target_schema: &SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let row_count = input.len() as u64;

        for target_field in target_schema.fields() {
            let column_name = ColumnName::new([target_field.name().as_str()]);
            let is_identity = target_field
                .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
                .is_some();
            if is_identity && input.has_field(&column_name) {
                return Err(Error::generic(format!(
                    "identity column '{}' must not be present in the input batch -- kernel fills it",
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

        let mut added_fields = Vec::new();
        let mut added_columns: Vec<ArrayData> = Vec::new();
        for target_field in target_schema.fields() {
            if target_field
                .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
                .is_none()
            {
                continue;
            }
            let cursor = self
                .cursors
                .iter_mut()
                .find(|c| c.column_name == *target_field.name())
                .ok_or_else(|| {
                    Error::generic(format!(
                        "identity column '{}' is not tracked by this reservation state",
                        target_field.name()
                    ))
                })?;
            let values = cursor.take(row_count)?;
            added_columns.push(ArrayData::from(values));
            added_fields.push(target_field.clone());
        }

        let added_schema = Arc::new(StructType::try_new(added_fields)?);
        let appended = input.append_columns(added_schema, added_columns)?;

        // `append_columns` places the generated identity columns after the input columns, so the
        // combined batch is not in `target_schema` order. Reorder it into schema order by selecting
        // each column by name.
        let reorder = Arc::new(Expression::struct_from(
            target_schema
                .fields()
                .map(|field| Arc::new(Expression::column([field.name()]))),
        ));
        evaluation_handler
            .new_expression_evaluator(
                target_schema.clone(),
                reorder,
                target_schema.as_ref().clone().into(),
            )?
            .evaluate(appended.as_ref())
    }

    /// Validates a reservation response against the cursors without mutating either.
    fn validate_reserved(
        cursors: &[SequenceCursor],
        ranges: &[ReservedRange],
    ) -> Result<(), String> {
        if ranges.len() != cursors.len() {
            return Err(format!(
                "sequence reserve returned {} range(s) but {} were expected",
                ranges.len(),
                cursors.len()
            ));
        }
        for (cursor, range) in cursors.iter().zip(ranges) {
            if range.step != cursor.step {
                return Err(format!(
                    "sequence '{}' returned step {} but schema declares step {}",
                    cursor.sequence_id, range.step, cursor.step
                ));
            }
        }
        Ok(())
    }

    fn roll_back_inflight(&mut self, count: u64, message: &str) {
        for cursor in self.cursors.iter_mut() {
            cursor.inflight = cursor.inflight.saturating_sub(count);
            cursor.error = Some(message.to_string());
        }
    }
}

/// Per-column reservation state: the queue of reserved ranges plus metadata and the
/// counters coordinating concurrent consumers.
struct SequenceCursor {
    column_name: String,
    sequence_id: String,
    step: i64,
    /// Reserved ranges awaiting consumption. The front range may be partially consumed.
    queue: VecDeque<ReservedRange>,
    /// Cached count of values ready to hand out (the sum over `queue`).
    available: u64,
    /// Values requested from the service but not yet returned.
    inflight: u64,
    /// Values promised to a caller by `try_claim` but not yet consumed by a fill.
    claimed: u64,
    /// Error from the most recent failed reservation, cleared on the next success.
    error: Option<String>,
}

impl SequenceCursor {
    /// Consumes and returns the next `count` values, spanning range boundaries as needed.
    ///
    /// # Errors
    ///
    /// Errors if fewer than `count` values are available, or on i64 overflow while advancing a
    /// partially consumed range.
    fn take(&mut self, count: u64) -> DeltaResult<Vec<i64>> {
        if self.available < count {
            return Err(Error::generic(format!(
                "identity column '{}' short by {} -- call ensure_available before fill",
                self.column_name,
                count - self.available
            )));
        }
        let mut out: Vec<i64> = Vec::with_capacity(count as usize);
        let mut remaining = count;
        while remaining > 0 {
            let front = self.queue.front_mut().ok_or_else(|| {
                Error::generic(format!(
                    "identity column '{}': reservation queue exhausted",
                    self.column_name
                ))
            })?;
            let front_count = front.count();
            let take = remaining.min(front_count);
            out.extend(front.values(&self.column_name, 0, take)?);
            if take == front_count {
                self.queue.pop_front();
            } else {
                // Advance the front range past the values just consumed.
                let stride: i64 = take.try_into().map_err(|_| {
                    Error::generic(format!(
                        "identity column '{}': consumed count does not fit into i64",
                        self.column_name
                    ))
                })?;
                let step_times = front.step.checked_mul(stride).ok_or_else(|| {
                    Error::generic(format!(
                        "identity column '{}': range advance overflows i64",
                        self.column_name
                    ))
                })?;
                front.range_start = front.range_start.checked_add(step_times).ok_or_else(|| {
                    Error::generic(format!(
                        "identity column '{}': range advance overflows i64",
                        self.column_name
                    ))
                })?;
            }
            remaining -= take;
        }
        self.available -= count;
        // Release the portion of any outstanding claim this fill satisfies.
        self.claimed = self.claimed.saturating_sub(count);
        Ok(out)
    }
}

/// Extracts a required string metadata value from a struct field.
fn get_required_string(field: &StructField, key: &ColumnMetadataKey) -> DeltaResult<String> {
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

    // === IdentitySequenceState ===

    fn cic_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new(vec![
                cic_column("id", "seq-id", 1, 1),
                StructField::new("payload", DataType::STRING, true),
                cic_column("row_id", "seq-row", 1000, 10),
            ])
            .unwrap(),
        )
    }

    #[test]
    fn begin_reserve_returns_one_request_per_column_and_marks_inflight() {
        let mut state = IdentitySequenceState::from_schema(&cic_schema()).unwrap();
        let requests = state.begin_reserve(5);
        assert_eq!(
            requests,
            vec![
                SequenceReservationRequest {
                    sequence_id: "seq-id".into(),
                    count: 5,
                    step: 1,
                },
                SequenceReservationRequest {
                    sequence_id: "seq-row".into(),
                    count: 5,
                    step: 10,
                },
            ]
        );
        // Nothing available yet, but the deficit is fully covered by in-flight -> Wait.
        assert_eq!(state.try_claim(5).unwrap(), ClaimOutcome::Wait);
    }

    #[test]
    fn try_claim_reserves_full_deficit_when_empty() {
        let mut state = IdentitySequenceState::from_schema(&cic_schema()).unwrap();
        assert_eq!(state.try_claim(5).unwrap(), ClaimOutcome::Reserve(5));
    }

    #[test]
    fn try_claim_on_schema_without_cic_columns_is_a_noop_claim() {
        let schema = Arc::new(
            StructType::try_new(vec![StructField::new("payload", DataType::STRING, true)]).unwrap(),
        );
        let mut state = IdentitySequenceState::from_schema(&schema).unwrap();
        assert_eq!(state.try_claim(100).unwrap(), ClaimOutcome::Claimed);
    }

    #[test]
    fn complete_reserve_makes_values_claimable_then_exhausts() {
        let mut state = IdentitySequenceState::from_schema(&cic_schema()).unwrap();
        let _ = state.begin_reserve(3);
        state
            .complete_reserve(3, vec![res(1, 3, 1), res(1000, 1020, 10)])
            .unwrap();
        assert_eq!(state.try_claim(3).unwrap(), ClaimOutcome::Claimed);
        // Everything claimed; a further claim must reserve the deficit.
        assert_eq!(state.try_claim(1).unwrap(), ClaimOutcome::Reserve(1));
    }

    #[test]
    fn complete_reserve_rejects_step_mismatch_and_rolls_back() {
        let mut state = IdentitySequenceState::from_schema(&cic_schema()).unwrap();
        let _ = state.begin_reserve(3);
        // Second range declares step 5, but the "row_id" column declares step 10.
        let err = state
            .complete_reserve(3, vec![res(1, 3, 1), res(1000, 1010, 5)])
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("returned step 5 but schema declares step 10"),
            "{err}"
        );
        // In-flight was rolled back and the error is sticky, so try_claim surfaces it.
        let claim_err = state.try_claim(3).unwrap_err();
        assert!(
            claim_err.to_string().contains("returned step 5"),
            "{claim_err}"
        );
    }

    #[test]
    fn complete_reserve_rejects_wrong_range_count() {
        let mut state = IdentitySequenceState::from_schema(&cic_schema()).unwrap();
        let _ = state.begin_reserve(3);
        let err = state.complete_reserve(3, vec![res(1, 3, 1)]).unwrap_err();
        assert!(
            err.to_string()
                .contains("returned 1 range(s) but 2 were expected"),
            "{err}"
        );
    }

    #[test]
    fn fail_reserve_rolls_back_inflight_and_sets_sticky_error() {
        let mut state = IdentitySequenceState::from_schema(&cic_schema()).unwrap();
        let _ = state.begin_reserve(3);
        state.fail_reserve(3, "boom");
        let err = state.try_claim(3).unwrap_err();
        assert!(err.to_string().contains("boom"), "{err}");
    }

    #[cfg(feature = "arrow")]
    mod fill {
        use std::sync::Arc;

        use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::engine::arrow_expression::ArrowEvaluationHandler;
        use crate::identity_columns::tests::cic_schema;
        use crate::identity_columns::{IdentitySequenceState, ReservedRange};

        fn payload_batch(rows: usize) -> ArrowEngineData {
            let payload: ArrayRef = Arc::new(StringArray::from(
                (0..rows).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
            ));
            let arrow_schema = Arc::new(Schema::new(vec![ArrowField::new(
                "payload",
                ArrowDataType::Utf8,
                true,
            )]));
            ArrowEngineData::new(RecordBatch::try_new(arrow_schema, vec![payload]).unwrap())
        }

        fn i64_col(batch: &RecordBatch, name: &str) -> Vec<i64> {
            let idx = batch.schema().index_of(name).unwrap();
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        }

        fn reserved(state: &mut IdentitySequenceState, count: u64) {
            let _ = state.begin_reserve(count);
            let count_i64 = count as i64;
            state
                .complete_reserve(
                    count,
                    vec![
                        ReservedRange {
                            range_start: 1,
                            range_end: count_i64,
                            step: 1,
                        },
                        ReservedRange {
                            range_start: 1000,
                            range_end: 1000 + (count_i64 - 1) * 10,
                            step: 10,
                        },
                    ],
                )
                .unwrap();
        }

        #[test]
        fn fill_generates_values_in_schema_order() {
            let schema = cic_schema();
            let mut state = IdentitySequenceState::from_schema(&schema).unwrap();
            reserved(&mut state, 3);

            let filled = state
                .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(3), &schema)
                .unwrap();
            let batch = ArrowEngineData::try_from_engine_data(filled)
                .unwrap()
                .record_batch()
                .clone();
            // Columns come back in `target_schema` order, not appended at the end.
            let names: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(names, vec!["id", "payload", "row_id"]);
            assert_eq!(i64_col(&batch, "id"), vec![1, 2, 3]);
            assert_eq!(i64_col(&batch, "row_id"), vec![1000, 1010, 1020]);
        }

        #[test]
        fn fill_without_reserve_fails_fast() {
            let schema = cic_schema();
            let mut state = IdentitySequenceState::from_schema(&schema).unwrap();
            let err = state
                .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(3), &schema)
                .err()
                .unwrap();
            assert!(err.to_string().contains("short by"), "{err}");
        }
    }
}
