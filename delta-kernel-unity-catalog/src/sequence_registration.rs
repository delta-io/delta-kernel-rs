//! CREATE-table helper for registering identity sequences.
//!
//! At CREATE-table time the caller mints a `sequence_id` per identity column and builds an
//! [`IdentityColumnInfo`] for each. It stamps those ids into the table schema via
//! [`delta_kernel::identity_columns::cic_column`] and commits the CREATE-table
//! transaction first, then calls [`register_identity_sequences`] to register them all with the UC
//! Identity Sequence Service in one batch.
//!
//! This is a one-shot setup phase, separate from the write-time reserve/fill path
//! ([`crate::IdentityColumnWriter`]), which is built later from the already-stamped schema.

use delta_kernel::identity_columns::IdentityColumnInfo;
use unity_catalog_delta_client_api::{
    CreateIdentitySequences, IdentitySequenceSpec, Result, SequenceClient,
};

/// Registers a sequence for each column with the UC Identity Sequence Service, in one batched
/// `CreateIdentitySequences` call.
///
/// The caller owns id minting: each [`IdentityColumnInfo`] must already carry the `sequence_id`
/// (a UUID) the sequence should be named by. `table_id` scopes the sequences and drives
/// authorization at the service.
///
/// Returns without contacting the service when `columns` is empty (the service rejects an empty
/// batch).
///
/// # Errors
///
/// Returns the error from the batch create call. The batch is atomic at the service: on error no
/// sequence is created, so the caller has nothing to roll back.
pub async fn register_identity_sequences<C: SequenceClient>(
    client: &C,
    table_id: impl Into<String>,
    columns: &[IdentityColumnInfo],
) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    let sequences = columns
        .iter()
        .map(|c| IdentitySequenceSpec {
            sequence_id: c.sequence_id.clone(),
            start: c.start,
            step: c.step,
        })
        .collect();
    client
        .create_identity_sequences(CreateIdentitySequences {
            table_id: table_id.into(),
            sequences,
        })
        .await
}

#[cfg(test)]
mod tests {
    use unity_catalog_delta_client_api::{
        IdentityReservation, InMemorySequenceClient, ReserveIdentityRanges,
    };
    use uuid::Uuid;

    use super::*;

    fn column(name: &str, start: i64, step: i64) -> IdentityColumnInfo {
        IdentityColumnInfo {
            column_name: name.to_string(),
            sequence_id: Uuid::new_v4().to_string(),
            start,
            step,
            allow_explicit_insert: false,
        }
    }

    #[tokio::test]
    async fn register_identity_sequences_registers_all_columns() {
        let client = InMemorySequenceClient::new();
        let columns = [column("id", 5, 2), column("row_id", 100, 10)];

        register_identity_sequences(&client, "tbl-1", &columns)
            .await
            .unwrap();

        // The sequences really exist under the table: reserving from a minted id succeeds and
        // starts at the requested start.
        let resp = client
            .reserve_identity_ranges(ReserveIdentityRanges {
                table_id: "tbl-1".to_string(),
                reservations: vec![IdentityReservation {
                    sequence_id: columns[0].sequence_id.clone(),
                    count: 3,
                    step: Some(2),
                }],
            })
            .await
            .unwrap();
        assert_eq!(resp.ranges[0].range_start, 5);
        assert_eq!(resp.ranges[0].range_end, 9); // 5, 7, 9
    }

    #[tokio::test]
    async fn register_identity_sequences_empty_is_noop() {
        let client = InMemorySequenceClient::new();
        register_identity_sequences(&client, "tbl-1", &[])
            .await
            .unwrap();
    }
}
