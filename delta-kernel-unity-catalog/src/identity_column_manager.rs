//! Write-time reserve-and-fill for Concurrent Identity Columns (CIC).
//!
//! [`IdentityColumnManager`] is the one object an engine talks to while writing to a table with
//! CIC identity columns. It owns the UC [`SequenceClient`] (so the client and all reserve logic
//! are hidden from the engine) and one private [`SequenceCursor`] per identity column.
//!
//! The engine:
//! - calls [`IdentityColumnManager::reserve`] to reserve ranges ahead of time (asynchronous).
//! - calls [`IdentityColumnManager::ensure_available`] before filling a batch as a failsafe that
//!   awaits in-flight reservations and/or reserves the deficit, such that the fill succeeds.
//! - calls [`IdentityColumnManager::fill_engine_batch`] to emit generated identity values into a
//!   batch. It fails if a column is short (i.e. `ensure_available` was skipped).

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};

use delta_kernel::expressions::{ArrayData, ColumnName};
use delta_kernel::identity_columns::{detect_identity_columns, ReservedRange};
use delta_kernel::schema::{ColumnMetadataKey, SchemaRef, StructType};
use delta_kernel::{DeltaResult, EngineData, Error};
use tokio::sync::Notify;
use unity_catalog_delta_client_api::{IdentityReservation, ReserveIdentityRanges, SequenceClient};

/// Per-CIC state containing the queue of reserved ranges plus sequence metadata.
struct SequenceCursor {
    column_name: String,
    sequence_id: String,
    step: i64,
    /// Reserved ranges awaiting consumption. The front range may be partially consumed.
    queue: VecDeque<ReservedRange>,
    /// Cached count of values ready to hand out.
    available: u64,
    /// Values requested from the service but not yet returned.
    inflight: u64,
    /// Error from the most recent failed reservation, cleared on the next success.
    error: Option<String>,
}

impl SequenceCursor {
    /// Consumes and returns the next `count` values. Errors if fewer than `count` values are
    /// available.
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
        Ok(out)
    }
}

/// What [`IdentityColumnManager::ensure_available`] should do this iteration.
enum EnsureAction {
    /// Every column already has enough available.
    Done,
    /// Reserve this many more values to cover the deficit.
    Reserve(u64),
    /// An in-flight reservation will cover the deficit. Therefore wait for to arrive.
    Wait,
}

/// Engine-facing manager to handle both the reserve and fill operations when writing to a table
/// with Concurrent Identity Columns (CIC).
pub struct IdentityColumnManager<C: SequenceClient> {
    client: Arc<C>,
    table_id: String,
    cursors: Arc<Mutex<Vec<SequenceCursor>>>,
    /// Signalled whenever a reservation completes (success or failure) so waiters re-check.
    notify: Arc<Notify>,
}

impl<C: SequenceClient + 'static> IdentityColumnManager<C> {
    /// Builds a manager handling all CICs in `schema`.
    ///
    /// Starts with nothing reserved; the engine reserves before filling.
    ///
    /// # Errors
    ///
    /// Returns an error if CIC metadata in `schema` is malformed.
    pub fn new(
        schema: &SchemaRef,
        client: Arc<C>,
        table_id: impl Into<String>,
    ) -> DeltaResult<Self> {
        let cursors = detect_identity_columns(schema)?
            .into_iter()
            .map(|info| SequenceCursor {
                column_name: info.column_name,
                sequence_id: info.sequence_id,
                step: info.step,
                queue: VecDeque::new(),
                available: 0,
                inflight: 0,
                error: None,
            })
            .collect();
        Ok(Self {
            client,
            table_id: table_id.into(),
            cursors: Arc::new(Mutex::new(cursors)),
            notify: Arc::new(Notify::new()),
        })
    }

    /// Reserves `count` more values for every CIC, in one batched RPC, asynchronously.
    ///
    /// The returned future is `'static` (it owns clones of the manager's shared state), so it can
    /// outlive a borrow of `self`.
    pub fn reserve(&self, count: u64) -> impl Future<Output = DeltaResult<()>> + 'static {
        let client = self.client.clone();
        let table_id = self.table_id.clone();
        let cursors = self.cursors.clone();
        let notify = self.notify.clone();
        async move {
            if count == 0 {
                return Ok(());
            }
            let count_i64: i64 = count
                .try_into()
                .map_err(|_| Error::generic("reserve count does not fit into i64"))?;

            // Build the batch request and mark inflight under the lock.
            let reservations: Vec<IdentityReservation> = {
                let mut guard = cursors.lock().unwrap();
                guard
                    .iter_mut()
                    .map(|cursor| {
                        cursor.inflight += count;
                        IdentityReservation {
                            sequence_id: cursor.sequence_id.clone(),
                            count: count_i64,
                            step: Some(cursor.step),
                        }
                    })
                    .collect()
            };
            if reservations.is_empty() {
                return Ok(()); // no CICs
            }

            let result = client
                .reserve_identity_ranges(ReserveIdentityRanges {
                    table_id,
                    reservations,
                })
                .await;

            let mut guard = cursors.lock().unwrap();
            let outcome = match result {
                Ok(resp) if resp.ranges.len() == guard.len() => {
                    Self::apply_reservation(&mut guard, count, resp.ranges)
                }
                Ok(_) => Err("UC sequence reserve returned a mismatched number of ranges".into()),
                Err(e) => Err(format!("UC sequence reserve error: {e}")),
            };
            match outcome {
                Ok(()) => {
                    notify.notify_waiters();
                    Ok(())
                }
                Err(msg) => {
                    // Roll back the inflight we marked and record the failure so waiters see it.
                    for cursor in guard.iter_mut() {
                        cursor.inflight -= count;
                        cursor.error = Some(msg.clone());
                    }
                    notify.notify_waiters();
                    Err(Error::Generic(msg))
                }
            }
        }
    }

    /// Enqueues one returned range per CIC, validating the step, and moves `count` from inflight
    /// to available. On any mismatch returns the error message.
    fn apply_reservation(
        cursors: &mut [SequenceCursor],
        count: u64,
        ranges: Vec<unity_catalog_delta_client_api::IdentityIdRange>,
    ) -> Result<(), String> {
        for (cursor, range) in cursors.iter_mut().zip(ranges) {
            if range.step != cursor.step {
                return Err(format!(
                    "sequence '{}' returned step {} but schema declares step {}",
                    cursor.sequence_id, range.step, cursor.step
                ));
            }
            cursor.queue.push_back(ReservedRange {
                range_start: range.range_start,
                range_end: range.range_end,
                step: range.step,
            });
            cursor.available += count;
            cursor.inflight -= count;
            cursor.error = None;
        }
        Ok(())
    }

    /// Guarantees every identity column has at least `count` values available, so a subsequent
    /// `fill` of `count` rows is certain to succeed.
    ///
    /// For a column that is short, this awaits an in-flight reservation if one will
    /// cover it, otherwise reserves the deficit itself. Returns immediately if enough is already
    /// available.
    ///
    /// # Errors
    ///
    /// Returns the underlying reservation error if a reservation fails.
    pub async fn ensure_available(&self, count: u64) -> DeltaResult<()> {
        if count == 0 {
            return Ok(());
        }
        loop {
            // Register interest before reading state so a completion between the read and the
            // await below cannot be lost.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let action = self.decide_ensure(count)?;
            match action {
                EnsureAction::Done => return Ok(()),
                EnsureAction::Reserve(deficit) => self.reserve(deficit).await?,
                EnsureAction::Wait => notified.await,
            }
        }
    }

    /// Inspects cursor state under the lock and decides the next [`EnsureAction`]. Returns an error
    /// if a column is short and its most recent reservation failed with no in-flight cover.
    fn decide_ensure(&self, count: u64) -> DeltaResult<EnsureAction> {
        let guard = self.cursors.lock().unwrap();
        let mut reserve_deficit = 0u64;
        let mut need_wait = false;
        for cursor in guard.iter() {
            if cursor.available >= count {
                continue;
            }
            let projected = cursor.available + cursor.inflight;
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
            EnsureAction::Reserve(reserve_deficit)
        } else if need_wait {
            EnsureAction::Wait
        } else {
            EnsureAction::Done
        })
    }

    /// Fills the CICs of a batch by appending generated identity values.
    ///
    /// `target_schema` identifies the CICs to generate. Every non-identity field in the target
    /// schema must already exist in `input`, and identity fields must not. The returned data holds
    /// the original input columns followed by the identity columns. Each identity column consumes
    /// `input.len()` values.
    ///
    /// It does not reserve.If a column has fewer than `input.len()` values available it errors.
    /// Therefore call [`Self::ensure_available`] first.
    ///
    /// # Errors
    ///
    /// Returns an error if a non-identity field is missing, a CIC field is already present,
    /// a column is short, or the engine cannot append the new columns.
    pub fn fill_engine_batch(
        &self,
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
        {
            let mut cursors = self.cursors.lock().unwrap();
            for target_field in target_schema.fields() {
                if target_field
                    .get_config_value(&ColumnMetadataKey::IdentityCicSequenceId)
                    .is_none()
                {
                    continue;
                }
                let cursor = cursors
                    .iter_mut()
                    .find(|c| c.column_name == *target_field.name())
                    .ok_or_else(|| {
                        Error::generic(format!(
                            "identity column '{}' has no cursor in this manager",
                            target_field.name()
                        ))
                    })?;
                let values = cursor.take(row_count)?;
                added_columns.push(ArrayData::from(values));
                added_fields.push(target_field.clone());
            }
        }

        let added_schema = Arc::new(StructType::try_new(added_fields)?);
        input.append_columns(added_schema, added_columns)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::identity_columns::identity_column_cic;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use unity_catalog_delta_client_api::InMemorySequenceClient;

    use super::*;

    const TABLE: &str = "tbl-1";

    fn schema() -> SchemaRef {
        Arc::new(
            StructType::try_new(vec![
                identity_column_cic("id", "seq-id", 1, 1),
                StructField::new("payload", DataType::STRING, true),
                identity_column_cic("row_id", "seq-row", 1000, 10),
            ])
            .unwrap(),
        )
    }

    async fn seeded_manager() -> IdentityColumnManager<InMemorySequenceClient> {
        let client = Arc::new(InMemorySequenceClient::new());
        client.seed_sequence(TABLE, "seq-id", 1, 1).unwrap();
        client.seed_sequence(TABLE, "seq-row", 1000, 10).unwrap();
        IdentityColumnManager::new(&schema(), client, TABLE).unwrap()
    }

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

    fn to_batch(filled: Box<dyn EngineData>) -> RecordBatch {
        ArrowEngineData::try_from_engine_data(filled)
            .unwrap()
            .record_batch()
            .clone()
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

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_available_then_fill_appends_values() {
        let mgr = seeded_manager().await;
        let schema = schema();
        mgr.ensure_available(3).await.unwrap();

        let filled = mgr.fill_engine_batch(&payload_batch(3), &schema).unwrap();
        let batch = to_batch(filled);

        assert_eq!(i64_col(&batch, "id"), vec![1, 2, 3]);
        assert_eq!(i64_col(&batch, "row_id"), vec![1000, 1010, 1020]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_without_ensure_available_fails_fast() {
        let mgr = seeded_manager().await;
        let err = mgr
            .fill_engine_batch(&payload_batch(3), &schema())
            .err()
            .unwrap();
        assert!(err.to_string().contains("short by"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_consumes_across_reservation_boundaries() {
        let mgr = seeded_manager().await;
        let schema = schema();
        // Two separate reservations of 2 each -> a fill of 3 must span both.
        mgr.reserve(2).await.unwrap();
        mgr.reserve(2).await.unwrap();

        let filled = mgr.fill_engine_batch(&payload_batch(3), &schema).unwrap();
        let batch = to_batch(filled);
        assert_eq!(i64_col(&batch, "id"), vec![1, 2, 3]);

        // The 4th value (tail of the second reservation) is still available.
        let filled = mgr.fill_engine_batch(&payload_batch(1), &schema).unwrap();
        let batch = to_batch(filled);
        assert_eq!(i64_col(&batch, "id"), vec![4]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prefetch_then_multiple_fills_without_more_io() {
        let mgr = seeded_manager().await;
        let schema = schema();
        mgr.reserve(10).await.unwrap();

        for expected_start in [1, 4, 7] {
            let filled = mgr.fill_engine_batch(&payload_batch(3), &schema).unwrap();
            let batch = to_batch(filled);
            assert_eq!(
                i64_col(&batch, "id"),
                vec![expected_start, expected_start + 1, expected_start + 2]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_available_reserves_deficit_when_nothing_prefetched() {
        let mgr = seeded_manager().await;
        // Nothing reserved yet; ensure_available must reserve the deficit itself.
        mgr.ensure_available(5).await.unwrap();
        let filled = mgr.fill_engine_batch(&payload_batch(5), &schema()).unwrap();
        let batch = to_batch(filled);
        assert_eq!(i64_col(&batch, "id"), vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_available_is_noop_when_already_available() {
        let mgr = seeded_manager().await;
        mgr.reserve(10).await.unwrap();
        // Already have 10; asking for 3 must not reserve more.
        mgr.ensure_available(3).await.unwrap();
        // A fill of 10 still succeeds -> ensure_available did not consume or discard anything.
        mgr.fill_engine_batch(&payload_batch(10), &schema())
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reserve_uses_one_batched_rpc_for_all_columns() {
        // InMemorySequenceClient reserves atomically across the batch; a single reserve(count)
        // advances both sequences so a later fill sees both filled from their starts.
        let mgr = seeded_manager().await;
        mgr.reserve(2).await.unwrap();
        let filled = mgr.fill_engine_batch(&payload_batch(2), &schema()).unwrap();
        let batch = to_batch(filled);
        assert_eq!(i64_col(&batch, "id"), vec![1, 2]);
        assert_eq!(i64_col(&batch, "row_id"), vec![1000, 1010]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_rejects_identity_column_present_in_input() {
        let mgr = seeded_manager().await;
        mgr.ensure_available(2).await.unwrap();
        // Input wrongly already carries `id`.
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 0]));
        let payload: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let arrow_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("payload", ArrowDataType::Utf8, true),
        ]));
        let bad =
            ArrowEngineData::new(RecordBatch::try_new(arrow_schema, vec![ids, payload]).unwrap());
        let err = mgr.fill_engine_batch(&bad, &schema()).err().unwrap();
        assert!(err.to_string().contains("must not be present"), "{err}");
    }
}
