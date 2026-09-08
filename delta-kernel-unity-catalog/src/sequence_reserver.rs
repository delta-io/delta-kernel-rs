//! Async bridge implementing kernel's [`SequenceReserver`] trait using a
//! [`SequenceClient`].
//!
//! Follows the same async-to-sync bridging pattern as [`UCCommitter`](crate::UCCommitter):
//! `tokio::task::block_in_place` + `handle.block_on()`.

use std::sync::Arc;

use delta_kernel::identity_columns::{ReservedRange, SequenceReserver};
use delta_kernel::{DeltaResult, Error as DeltaError};
use unity_catalog_delta_client_api::{IdentityReservation, ReserveIdentityRanges, SequenceClient};

/// A [`SequenceReserver`] backed by a UC [`SequenceClient`], scoped to a single table.
///
/// Bridges the async [`SequenceClient`] to the sync [`SequenceReserver`] trait required by
/// kernel. Requires a multi-threaded tokio runtime.
///
/// The `table_id` is fixed at construction: kernel's [`SequenceReserver`] identifies a sequence
/// by id alone, while the service scopes sequences to a table, so the reserver supplies the table
/// for every reservation it makes.
///
/// Kernel's [`SequenceReserver`] reserves one sequence at a time, which this maps to a
/// single-reservation batch. Each request carries the `expected_step` as the advisory step so
/// the service rejects a stride mismatch; the returned step is additionally re-checked here, so
/// drift between schema metadata and service state is a hard error either way.
#[derive(Debug, Clone)]
pub struct UCSequenceReserver<C: SequenceClient> {
    sequence_client: Arc<C>,
    table_id: String,
}

impl<C: SequenceClient> UCSequenceReserver<C> {
    /// Creates a new [`UCSequenceReserver`] wrapping the given sequence client and scoped to
    /// `table_id`.
    pub fn new(sequence_client: Arc<C>, table_id: impl Into<String>) -> Self {
        UCSequenceReserver {
            sequence_client,
            table_id: table_id.into(),
        }
    }
}

impl<C: SequenceClient + 'static> SequenceReserver for UCSequenceReserver<C> {
    fn reserve_ids(
        &self,
        sequence_id: &str,
        expected_step: i64,
        count: u64,
    ) -> DeltaResult<ReservedRange> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            DeltaError::generic("UCSequenceReserver may only be used within a tokio runtime")
        })?;
        let count: i64 = count
            .try_into()
            .map_err(|_| DeltaError::generic("reserve count does not fit into i64"))?;
        let req = ReserveIdentityRanges {
            table_id: self.table_id.clone(),
            reservations: vec![IdentityReservation {
                sequence_id: sequence_id.to_string(),
                count,
                step: Some(expected_step),
            }],
        };
        let resp = tokio::task::block_in_place(|| {
            handle.block_on(async { self.sequence_client.reserve_identity_ranges(req).await })
        })
        .map_err(|e| DeltaError::Generic(format!("UC sequence reserve error: {e}")))?;

        let range = resp.ranges.into_iter().next().ok_or_else(|| {
            DeltaError::generic(format!(
                "UC sequence reserve returned no range for sequence {sequence_id}"
            ))
        })?;

        if range.step != expected_step {
            return Err(DeltaError::generic(format!(
                "Sequence {sequence_id} returned step {} but schema declares step {expected_step}",
                range.step
            )));
        }

        Ok(ReservedRange {
            range_start: range.range_start,
            range_end: range.range_end,
        })
    }
}
