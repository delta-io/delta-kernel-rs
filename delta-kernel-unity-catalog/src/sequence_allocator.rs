//! Orchestration helper for allocating identity sequences at CREATE TABLE time.
//!
//! The engine declares its intent (column name + start + step per identity column) and this
//! helper mints a fresh `sequence_id` (a UUID) per column, creates them all in one batch
//! `CreateIdentitySequences` call through a [`SequenceClient`], and returns the minted ids
//! alongside the original intent so the engine can stamp them onto the schema via
//! [`delta_kernel::identity_columns::identity_column_cic`].

use delta_kernel::identity_columns::IdentityColumnInfo;
use unity_catalog_delta_client_api::{
    CreateIdentitySequences, IdentitySequenceSpec, Result, SequenceClient,
};
use uuid::Uuid;

/// The engine's intent for one identity column, prior to sequence allocation.
///
/// [`create_identity_sequences`] mints a `sequence_id` for each spec and returns it in the
/// corresponding [`IdentityColumnInfo`].
#[derive(Debug, Clone)]
pub struct IdentityColumnSpec {
    /// The logical column name. Carried through to the returned [`IdentityColumnInfo`] for the
    /// engine to use when constructing the schema.
    pub column_name: String,
    /// The start value for the sequence.
    pub start: i64,
    /// The step (increment) for the sequence. Must be non-zero.
    pub step: i64,
    /// Whether explicit inserts are allowed for this column.
    pub allow_explicit_insert: bool,
}

impl IdentityColumnSpec {
    /// Creates a new spec with `allow_explicit_insert = false`.
    pub fn new(column_name: impl Into<String>, start: i64, step: i64) -> Self {
        Self {
            column_name: column_name.into(),
            start,
            step,
            allow_explicit_insert: false,
        }
    }

    /// Sets whether explicit inserts are allowed.
    pub fn with_allow_explicit_insert(mut self, allow: bool) -> Self {
        self.allow_explicit_insert = allow;
        self
    }
}

/// Mints a `sequence_id` for each spec and creates them all under `table_id` in one batch
/// `CreateIdentitySequences` call, returning the stamped [`IdentityColumnInfo`]s in the same
/// order.
///
/// The engine should call this before `create_table`, then feed each returned
/// `IdentityColumnInfo` into [`delta_kernel::identity_columns::identity_column_cic`] to build the
/// table schema. `table_id` scopes the sequences and drives authorization at the service.
///
/// Returns an empty `Vec` without contacting the service when `specs` is empty (the service
/// rejects an empty batch).
///
/// # Errors
///
/// Returns the error from the batch create call. The batch is atomic at the service: on error no
/// sequence is created, so the caller has nothing to roll back.
pub async fn create_identity_sequences<C: SequenceClient>(
    client: &C,
    table_id: impl Into<String>,
    specs: &[IdentityColumnSpec],
) -> Result<Vec<IdentityColumnInfo>> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    let table_id = table_id.into();

    // Client-mint a UUID per column; the service names sequences by these ids.
    let infos: Vec<IdentityColumnInfo> = specs
        .iter()
        .map(|spec| IdentityColumnInfo {
            column_name: spec.column_name.clone(),
            sequence_id: Uuid::new_v4().to_string(),
            start: spec.start,
            step: spec.step,
            allow_explicit_insert: spec.allow_explicit_insert,
        })
        .collect();

    let sequences = infos
        .iter()
        .map(|info| IdentitySequenceSpec {
            sequence_id: info.sequence_id.clone(),
            start: info.start,
            step: info.step,
        })
        .collect();

    client
        .create_identity_sequences(CreateIdentitySequences {
            table_id,
            sequences,
        })
        .await?;

    Ok(infos)
}

#[cfg(test)]
mod tests {
    use unity_catalog_delta_client_api::{
        IdentityReservation, InMemorySequenceClient, ReserveIdentityRanges,
    };

    use super::*;

    #[tokio::test]
    async fn create_identity_sequences_mints_unique_ids_and_creates_them() {
        let client = InMemorySequenceClient::new();
        let infos = create_identity_sequences(
            &client,
            "tbl-1",
            &[
                IdentityColumnSpec::new("id", 5, 2),
                IdentityColumnSpec::new("row_id", 100, 10),
            ],
        )
        .await
        .unwrap();

        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].column_name, "id");
        assert_eq!(infos[0].start, 5);
        assert_eq!(infos[0].step, 2);
        assert_eq!(infos[1].column_name, "row_id");
        assert!(
            infos[0].sequence_id != infos[1].sequence_id,
            "each column should get a distinct minted id"
        );

        // The sequences really exist under the table: reserving from the minted id succeeds and
        // starts at the requested start.
        let resp = client
            .reserve_identity_ranges(ReserveIdentityRanges {
                table_id: "tbl-1".to_string(),
                reservations: vec![IdentityReservation {
                    sequence_id: infos[0].sequence_id.clone(),
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
    async fn create_identity_sequences_empty_specs_is_noop() {
        let client = InMemorySequenceClient::new();
        let infos = create_identity_sequences(&client, "tbl-1", &[])
            .await
            .unwrap();
        assert!(infos.is_empty());
    }
}
