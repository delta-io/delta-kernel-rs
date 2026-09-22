use std::sync::{Arc, LazyLock};

use super::Snapshot;
use crate::action_reconciliation::log_replay::{
    ActionReconciliationBatch, ActionReconciliationProcessor,
};
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::actions::{
    ADD_FIELD, DOMAIN_METADATA_FIELD, METADATA_FIELD, PROTOCOL_FIELD, REMOVE_FIELD,
    SET_TRANSACTION_FIELD,
};
use crate::crc::try_read_crc_file;
use crate::crc::validation::with_crc_validation;
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::CrcReplayAccumulator;
use crate::schema::{lazy_schema_ref, SchemaRef};
use crate::utils::current_time_duration;
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
    /// retention to both sides, and checks the in-commit timestamp when present. Does not compare
    /// `allFiles`, deletion-vector aggregates, or `txnId` against the log.
    ///
    /// Returns [`CrcValidationResult::Skipped`] when no checksum describes this snapshot's version.
    /// A checksum computed in memory is eligible. Stale checksums are not compared with newer
    /// state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ChecksumMismatch`] for a discrepancy, or an error if the checksum or
    /// transaction log cannot be read. A present in-commit timestamp requires the version's commit.
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
        let (reconciled, transaction_expiration) =
            self.reconciled_actions(engine, RECONCILIATION_SCHEMA.clone())?;
        let histogram = crc.replay_histogram()?;
        let ict = crc
            .in_commit_timestamp_opt
            .map(|_| self.read_commit_in_commit_timestamp(engine))
            .transpose()?;
        with_crc_validation(
            reconciled,
            Some((CrcReplayAccumulator::new(histogram), self.version(), ict)),
            |actual| crc.validate_against(&actual, transaction_expiration),
        )
        .try_for_each(|batch| batch.map(|_| ()))?;
        Ok(CrcValidationResult::Validated)
    }

    pub(crate) fn reconciled_actions(
        &self,
        engine: &dyn Engine,
        read_schema: SchemaRef,
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
            .process_actions_iter(actions);
        Ok((reconciled, transaction_expiration))
    }

    pub(crate) fn read_commit_in_commit_timestamp(&self, engine: &dyn Engine) -> DeltaResult<i64> {
        self.log_segment()
            .listed
            .latest_commit_file
            .as_ref()
            .ok_or(Error::MissingVersion(self.version()))?
            .read_in_commit_timestamp(engine)
    }
}
