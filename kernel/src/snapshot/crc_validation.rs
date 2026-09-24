use std::sync::{Arc, LazyLock, Mutex};

use super::Snapshot;
use crate::action_reconciliation::log_replay::{
    ActionReconciliationBatch, ActionReconciliationProcessor,
};
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::{
    ADD_FIELD, COMMIT_INFO_NAME, DOMAIN_METADATA_FIELD, METADATA_FIELD, PROTOCOL_FIELD,
    REMOVE_FIELD, SET_TRANSACTION_FIELD,
};
use crate::crc::{try_read_crc_file, Crc};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::log_replay::LogReplayProcessor;
use crate::schema::{
    column_name, lazy_schema_ref, ColumnName, ColumnNamesAndTypes, DataType, SchemaRef,
};
use crate::utils::{current_time_duration, require};
use crate::{DeltaResult, Engine, Error};

static RECONCILIATION_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    (&ADD_FIELD),
    (&REMOVE_FIELD),
    (&PROTOCOL_FIELD),
    (&METADATA_FIELD),
    (&SET_TRANSACTION_FIELD),
    (&DOMAIN_METADATA_FIELD),
};

/// Result of comparing a snapshot with its version checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrcValidationResult {
    /// All available fields covered by [`Snapshot::validate_crc`] matched log replay.
    Validated,
    /// The snapshot has no checksum at its version.
    Skipped,
}

impl Snapshot {
    /// Validates this snapshot's checksum against independently reconciled log actions.
    ///
    /// Uses `engine` to read commits and checkpoints without reading table data files. Checks
    /// complete file statistics, the optional file-size histogram, protocol, metadata, and their
    /// counts. Checks domain metadata and transaction arrays when complete, applying transaction
    /// retention to both sides. Checks deletion-vector totals and their histogram, `txnId`, and the
    /// in-commit timestamp when present. Collects live Add actions only when `allFiles` is present,
    /// ignoring order, `dataChange`, and per-file `stats`.
    ///
    /// Returns [`CrcValidationResult::Skipped`] when no checksum describes this snapshot's version.
    /// A checksum computed in memory is eligible. Stale checksums are not compared with newer
    /// state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ChecksumMismatch`] for a discrepancy, or an error if the checksum or
    /// transaction log cannot be read. A present `txnId` or in-commit timestamp requires the
    /// version's commit.
    pub fn validate_crc(&self, engine: &dyn Engine) -> DeltaResult<CrcValidationResult> {
        let crc = match self.crc_at_version() {
            Some(crc) => Some(crc.clone()),
            None => self
                .log_segment()
                .listed
                .latest_crc_file
                .as_ref()
                .filter(|file| file.version == self.version())
                .map(|file| try_read_crc_file(engine, file).map(Arc::new))
                .transpose()?,
        };
        let Some(crc) = crc else {
            return Ok(CrcValidationResult::Skipped);
        };
        let actual = Arc::new(Mutex::new(Some(self.crc_replay_accumulator(engine, &crc)?)));
        let (mut reconciled, transaction_expiration) =
            self.reconciled_actions(engine, RECONCILIATION_SCHEMA.clone(), actual.clone())?;
        reconciled.try_for_each(|batch| batch.map(|_| ()))?;
        let actual = actual
            .lock()
            .map_err(|e| Error::internal_error(format!("CRC accumulator lock poisoned: {e}")))?;
        let actual = actual
            .as_ref()
            .ok_or_else(|| Error::internal_error("CRC accumulator missing"))?;
        crc.validate_against(actual, transaction_expiration)?;
        Ok(CrcValidationResult::Validated)
    }

    pub(crate) fn reconciled_actions(
        &self,
        engine: &dyn Engine,
        read_schema: SchemaRef,
        crc: Arc<Mutex<Option<Crc>>>,
    ) -> DeltaResult<(
        impl Iterator<Item = DeltaResult<ActionReconciliationBatch>> + Send,
        Option<i64>,
    )> {
        let transaction_expiration =
            calculate_transaction_expiration_timestamp(self.table_properties())?;
        let file_retention = deleted_file_retention_timestamp_with_time(
            self.table_properties().deleted_file_retention_duration,
            current_time_duration()?,
        )?;
        let actions = self.log_segment().read_actions(engine, read_schema)?;
        let reconciled = ActionReconciliationProcessor::new(file_retention, transaction_expiration)
            .with_crc(crc)
            .process_actions_iter(actions);
        Ok((reconciled, transaction_expiration))
    }

    /// Initializes replay state without using expected values as accumulated totals.
    pub(crate) fn crc_replay_accumulator(
        &self,
        engine: &dyn Engine,
        expected: &Crc,
    ) -> DeltaResult<Crc> {
        let mut actual =
            Crc::replay_accumulator(self.version(), expected.replay_histogram()?, None);
        if expected.all_files.is_some() {
            actual.all_files = Some(Vec::new());
        }
        if expected.in_commit_timestamp_opt.is_some() || expected.txn_id.is_some() {
            let commit = self
                .log_segment()
                .listed
                .latest_commit_file
                .as_ref()
                .filter(|commit| commit.version == self.version())
                .ok_or(Error::MissingVersion(self.version()))?;
            let mut visitor = CrcCommitInfoVisitor::default();
            let batches = engine.json_handler().read_json_files(
                std::slice::from_ref(&commit.location),
                CrcCommitInfoVisitor::schema(),
                None,
            )?;
            for batch in batches {
                visitor.visit_rows_of(batch?.as_ref())?;
                if (expected.in_commit_timestamp_opt.is_none() || visitor.ict.is_some())
                    && (expected.txn_id.is_none() || visitor.txn_id.is_some())
                {
                    break;
                }
            }
            actual.in_commit_timestamp_opt = visitor.ict;
            actual.txn_id = visitor.txn_id;
        }
        Ok(actual)
    }
}

#[derive(Default)]
struct CrcCommitInfoVisitor {
    ict: Option<i64>,
    txn_id: Option<String>,
}

impl CrcCommitInfoVisitor {
    fn schema() -> SchemaRef {
        static SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
            nullable COMMIT_INFO_NAME: {
                nullable "inCommitTimestamp": LONG,
                nullable "txnId": STRING,
            },
        };
        SCHEMA.clone()
    }
}

impl RowVisitor for CrcCommitInfoVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    column_name!("commitInfo.inCommitTimestamp"),
                    column_name!("commitInfo.txnId"),
                ],
                vec![DataType::LONG, DataType::STRING],
            )
                .into()
        });
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, rows: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 2,
            Error::internal_error("Unexpected CRC commitInfo getters")
        );
        // Without ICT, commitInfo need not be the first action or even in the first batch.
        for row in 0..rows {
            if let Some(ict) = getters[0].get_opt(row, "commitInfo.inCommitTimestamp")? {
                self.ict = Some(ict);
            }
            if let Some(txn_id) = getters[1].get_opt(row, "commitInfo.txnId")? {
                self.txn_id = Some(txn_id);
            }
        }
        Ok(())
    }
}
