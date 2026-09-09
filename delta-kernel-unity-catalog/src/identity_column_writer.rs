//! Write-time reserve-and-fill for Concurrent Identity Columns (CIC).
//!
//! [`IdentityColumnWriter`] is the one object an engine talks to while writing to a table with
//! CIC identity columns. It is a async wrapper around kernel's synchronous
//! [`IdentitySequenceState`] that owns the UC [`SequenceClient`] (so the client and the reserve
//! RPC are hidden from the engine) and drives the state, which holds all the
//! reservation bookkeeping and value generation.
//!
//! The engine:
//! - calls [`IdentityColumnWriter::reserve`] to reserve ranges ahead of time (asynchronous).
//! - calls [`IdentityColumnWriter::ensure_available`] before filling a batch as a failsafe that
//!   awaits in-flight reservations and/or reserves the deficit, such that the fill succeeds.
//! - calls [`IdentityColumnWriter::fill_engine_batch`] to emit generated identity values into a
//!   batch. It fails if a column is short (i.e. `ensure_available` was skipped).
//!
//! # Concurrency
//!
//! One writer may be shared by multiple concurrent consumers. `ensure_available(count)` claims
//! `count` values for the caller before returning, so concurrent consumers each doing
//! `ensure_available` + `fill` receive disjoint value ranges without conflicting. The contract is
//! that a successful `ensure_available(count)` is always followed by a `fill` of `count`
//! rows. A claim that is never filled keeps those values reserved for the life of the writer.

use std::future::Future;
use std::sync::{Arc, Mutex};

use delta_kernel::identity_columns::{ClaimOutcome, IdentitySequenceState, ReservedRange};
use delta_kernel::schema::SchemaRef;
use delta_kernel::{DeltaResult, EngineData, Error, EvaluationHandler};
use tokio::sync::Notify;
use unity_catalog_delta_client_api::{IdentityReservation, ReserveIdentityRanges, SequenceClient};

/// Engine-facing writer to handle both the reserve and fill operations when writing to a table
/// with Concurrent Identity Columns (CIC).
///
/// The kernel's [`IdentitySequenceState`] provides a reservation queue, the claim/reserve/wait
/// decision, and the value fill for a single sequence.
pub struct IdentityColumnWriter<C: SequenceClient> {
    client: Arc<C>,
    table_id: String,
    state: Arc<Mutex<IdentitySequenceState>>,
    /// Signalled whenever a reservation completes (success or failure) so waiters re-check.
    notify: Arc<Notify>,
}

impl<C: SequenceClient + 'static> IdentityColumnWriter<C> {
    /// Builds a writer handling all CICs in `schema`.
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
        Ok(Self {
            client,
            table_id: table_id.into(),
            state: Arc::new(Mutex::new(IdentitySequenceState::from_schema(schema)?)),
            notify: Arc::new(Notify::new()),
        })
    }

    /// Reserves `count` more values for every CIC, in one batched RPC, asynchronously.
    ///
    /// The returned future is `'static` (it owns clones of the writer's shared state), so it can
    /// outlive a borrow of `self`.
    pub fn reserve(&self, count: u64) -> impl Future<Output = DeltaResult<()>> + 'static {
        let client = self.client.clone();
        let table_id = self.table_id.clone();
        let state = self.state.clone();
        let notify = self.notify.clone();
        async move {
            if count == 0 {
                return Ok(());
            }
            let count_i64: i64 = count
                .try_into()
                .map_err(|_| Error::generic("reserve count does not fit into i64"))?;

            // Mark inflight and get the per-sequence requests to fulfill (pure, under the lock).
            let requests = state.lock().unwrap().begin_reserve(count);
            if requests.is_empty() {
                return Ok(()); // no CICs
            }

            // Translate the kernel requests into the UC request and perform the one RPC.
            let reservations: Vec<IdentityReservation> = requests
                .iter()
                .map(|req| IdentityReservation {
                    sequence_id: req.sequence_id.clone(),
                    count: count_i64,
                    step: Some(req.step),
                })
                .collect();
            let result = client
                .reserve_identity_ranges(ReserveIdentityRanges {
                    table_id,
                    reservations,
                })
                .await;

            // Feed the outcome back into the kernel state.
            let mut state = state.lock().unwrap();
            let outcome = match result {
                Ok(resp) => {
                    let ranges = resp
                        .ranges
                        .into_iter()
                        .map(|range| ReservedRange {
                            range_start: range.range_start,
                            range_end: range.range_end,
                            step: range.step,
                        })
                        .collect();
                    state.complete_reserve(count, ranges)
                }
                Err(e) => {
                    let msg = format!("UC sequence reserve error: {e}");
                    state.fail_reserve(count, msg.clone());
                    Err(Error::Generic(msg))
                }
            };
            notify.notify_waiters();
            outcome
        }
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

            let outcome = self.state.lock().unwrap().try_claim(count)?;
            match outcome {
                ClaimOutcome::Claimed => return Ok(()),
                ClaimOutcome::Reserve(deficit) => self.reserve(deficit).await?,
                ClaimOutcome::Wait => notified.await,
            }
        }
    }

    /// Fills the CICs of a batch with generated identity values, returning every column of
    /// `target_schema` in schema order (ready to write).
    ///
    /// Delegates to [`IdentitySequenceState::fill_engine_batch`]. `evaluation_handler` (e.g.
    /// `engine.evaluation_handler()`) reorders the columns into schema order.
    ///
    /// It does not reserve. If a column has fewer than `input.len()` values available it errors.
    /// Therefore call [`Self::ensure_available`] first.
    ///
    /// # Errors
    ///
    /// Returns an error if a non-identity field is missing, a CIC field is already present,
    /// a column is short, or the engine cannot construct the result.
    pub fn fill_engine_batch(
        &self,
        evaluation_handler: &dyn EvaluationHandler,
        input: &dyn EngineData,
        target_schema: &SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.state
            .lock()
            .unwrap()
            .fill_engine_batch(evaluation_handler, input, target_schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
    use delta_kernel::identity_columns::cic_column;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use unity_catalog_delta_client_api::InMemorySequenceClient;

    use super::*;

    const TABLE: &str = "tbl-1";

    fn schema() -> SchemaRef {
        Arc::new(
            StructType::try_new(vec![
                cic_column("id", "seq-id", 1, 1),
                StructField::new("payload", DataType::STRING, true),
                cic_column("row_id", "seq-row", 1000, 10),
            ])
            .unwrap(),
        )
    }

    async fn seeded_writer() -> IdentityColumnWriter<InMemorySequenceClient> {
        let client = Arc::new(InMemorySequenceClient::new());
        client.seed_sequence(TABLE, "seq-id", 1, 1).unwrap();
        client.seed_sequence(TABLE, "seq-row", 1000, 10).unwrap();
        IdentityColumnWriter::new(&schema(), client, TABLE).unwrap()
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
    async fn ensure_available_then_fill_generates_values() {
        let writer = seeded_writer().await;
        let schema = schema();
        writer.ensure_available(3).await.unwrap();

        let filled = writer
            .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(3), &schema)
            .unwrap();
        let batch = to_batch(filled);

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

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_without_ensure_available_fails_fast() {
        let writer = seeded_writer().await;
        let err = writer
            .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(3), &schema())
            .err()
            .unwrap();
        assert!(err.to_string().contains("short by"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_consumes_across_reservation_boundaries() {
        let writer = seeded_writer().await;
        let schema = schema();
        // Two separate reservations of 2 each -> a fill of 3 must span both.
        writer.reserve(2).await.unwrap();
        writer.reserve(2).await.unwrap();

        let filled = writer
            .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(3), &schema)
            .unwrap();
        let batch = to_batch(filled);
        assert_eq!(i64_col(&batch, "id"), vec![1, 2, 3]);

        // The 4th value (tail of the second reservation) is still available.
        let filled = writer
            .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(1), &schema)
            .unwrap();
        let batch = to_batch(filled);
        assert_eq!(i64_col(&batch, "id"), vec![4]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prefetch_then_multiple_fills_without_more_io() {
        let writer = seeded_writer().await;
        let schema = schema();
        writer.reserve(10).await.unwrap();

        for expected_start in [1, 4, 7] {
            let filled = writer
                .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(3), &schema)
                .unwrap();
            let batch = to_batch(filled);
            assert_eq!(
                i64_col(&batch, "id"),
                vec![expected_start, expected_start + 1, expected_start + 2]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_available_reserves_deficit_when_nothing_prefetched() {
        let writer = seeded_writer().await;
        // Nothing reserved yet; ensure_available must reserve the deficit itself.
        writer.ensure_available(5).await.unwrap();
        let filled = writer
            .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(5), &schema())
            .unwrap();
        let batch = to_batch(filled);
        assert_eq!(i64_col(&batch, "id"), vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_available_is_noop_when_already_available() {
        let writer = seeded_writer().await;
        writer.reserve(10).await.unwrap();
        // Already have 10; asking for 3 must not reserve more.
        writer.ensure_available(3).await.unwrap();
        // A fill of 10 still succeeds -> ensure_available did not consume or discard anything.
        writer
            .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(10), &schema())
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reserve_uses_one_batched_rpc_for_all_columns() {
        // InMemorySequenceClient reserves atomically across the batch; a single reserve(count)
        // advances both sequences so a later fill sees both filled from their starts.
        let writer = seeded_writer().await;
        writer.reserve(2).await.unwrap();
        let filled = writer
            .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(2), &schema())
            .unwrap();
        let batch = to_batch(filled);
        assert_eq!(i64_col(&batch, "id"), vec![1, 2]);
        assert_eq!(i64_col(&batch, "row_id"), vec![1000, 1010]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_consumers_get_disjoint_values() {
        use std::collections::HashSet;

        // Four consumers share one writer, each independently ensure_available(100) + fill(100).
        // Claiming in ensure_available must hand each consumer a disjoint range.
        let writer = Arc::new(seeded_writer().await);
        let mut handles = Vec::new();
        for _ in 0..4 {
            let writer = writer.clone();
            handles.push(tokio::spawn(async move {
                writer.ensure_available(100).await.unwrap();
                let filled = writer
                    .fill_engine_batch(&ArrowEvaluationHandler, &payload_batch(100), &schema())
                    .unwrap();
                i64_col(&to_batch(filled), "id")
            }));
        }

        let mut all = Vec::new();
        for handle in handles {
            all.extend(handle.await.unwrap());
        }

        assert_eq!(all.len(), 400);
        let unique: HashSet<i64> = all.iter().copied().collect();
        assert_eq!(
            unique.len(),
            400,
            "values must be unique across consumers: {all:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_rejects_identity_column_present_in_input() {
        let writer = seeded_writer().await;
        writer.ensure_available(2).await.unwrap();
        // Input wrongly already carries `id`.
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 0]));
        let payload: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let arrow_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("payload", ArrowDataType::Utf8, true),
        ]));
        let bad =
            ArrowEngineData::new(RecordBatch::try_new(arrow_schema, vec![ids, payload]).unwrap());
        let err = writer
            .fill_engine_batch(&ArrowEvaluationHandler, &bad, &schema())
            .err()
            .unwrap();
        assert!(err.to_string().contains("must not be present"), "{err}");
    }
}
