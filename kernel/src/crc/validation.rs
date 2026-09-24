//! Comparison of completed CRC state with independently replayed state.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use super::{
    Crc, DeletedRecordCountsHistogram, DomainMetadataState, FileSizeHistogram, FileStats,
    FileStatsState, SetTransactionState,
};
use crate::actions::{Add, SetTransaction};
use crate::utils::require;
use crate::{DeltaResult, Error, Version};

impl Crc {
    /// Empty replay state. Totals and required actions are valid only after successful exhaustion.
    pub(crate) fn replay_accumulator(
        version: Version,
        histogram: Option<FileSizeHistogram>,
        in_commit_timestamp_opt: Option<i64>,
    ) -> Self {
        Self {
            version,
            file_stats_state: FileStatsState::Complete(FileStats::replay_accumulator(histogram)),
            domain_metadata_state: DomainMetadataState::Complete(HashMap::new()),
            set_transaction_state: SetTransactionState::Complete(HashMap::new()),
            in_commit_timestamp_opt,
            num_deleted_records_opt: Some(0),
            num_deletion_vectors_opt: Some(0),
            deleted_record_counts_histogram_opt: Some(DeletedRecordCountsHistogram::empty()),
            ..Default::default()
        }
    }

    pub(crate) fn replay_histogram(&self) -> DeltaResult<Option<FileSizeHistogram>> {
        self.file_stats()
            .and_then(FileStats::file_size_histogram)
            .map(|histogram| {
                FileSizeHistogram::create_empty_with_boundaries(
                    histogram.sorted_bin_boundaries().to_vec(),
                )
            })
            .transpose()
    }

    pub(crate) fn add_deletion_vector(&mut self, cardinality: Option<i64>) -> DeltaResult<()> {
        let records = cardinality.unwrap_or(0);
        require!(
            records >= 0,
            Error::generic("Negative deletion-vector cardinality")
        );
        for (total, increment) in [
            (&mut self.num_deleted_records_opt, records),
            (
                &mut self.num_deletion_vectors_opt,
                i64::from(cardinality.is_some()),
            ),
        ] {
            if let Some(total) = total {
                *total = total
                    .checked_add(increment)
                    .ok_or_else(|| Error::generic("CRC deletion-vector total overflow"))?;
            }
        }
        if let Some(histogram) = &mut self.deleted_record_counts_histogram_opt {
            histogram.insert(records)?;
        }
        Ok(())
    }

    pub(crate) fn validate_against(
        &self,
        actual: &Self,
        transaction_expiration_timestamp: Option<i64>,
    ) -> DeltaResult<()> {
        check_crc_field(self.version, "version", &self.version, &actual.version)?;
        self.validate_file_stats(actual.file_stats())?;
        check_crc_field(self.version, "metadata", &self.metadata, &actual.metadata)?;
        check_crc_field(
            self.version,
            "protocol",
            &(
                self.protocol.min_reader_version(),
                self.protocol.min_writer_version(),
            ),
            &(
                actual.protocol.min_reader_version(),
                actual.protocol.min_writer_version(),
            ),
        )?;
        for (expected_features, actual_features) in [
            (
                self.protocol.reader_features(),
                actual.protocol.reader_features(),
            ),
            (
                self.protocol.writer_features(),
                actual.protocol.writer_features(),
            ),
        ] {
            check_crc_field(
                self.version,
                "protocol",
                &expected_features.map(|features| features.iter().collect::<HashSet<_>>()),
                &actual_features.map(|features| features.iter().collect::<HashSet<_>>()),
            )?;
        }
        if matches!(self.domain_metadata_state, DomainMetadataState::Complete(_)) {
            check_crc_field(
                self.version,
                "domainMetadata",
                &self.domain_metadata_state,
                &actual.domain_metadata_state,
            )?;
        }
        if let Some(expected) = complete_transactions(&self.set_transaction_state) {
            let actual = complete_transactions(&actual.set_transaction_state).ok_or_else(|| {
                Error::internal_error("CRC replay returned incomplete transactions")
            })?;
            check_crc_field(
                self.version,
                "setTransactions",
                &non_expired_transactions(expected, transaction_expiration_timestamp),
                &non_expired_transactions(actual, transaction_expiration_timestamp),
            )?;
        }
        if self.in_commit_timestamp_opt.is_some() {
            check_crc_field(
                self.version,
                "inCommitTimestampOpt",
                &self.in_commit_timestamp_opt,
                &actual.in_commit_timestamp_opt,
            )?;
        }
        for (field, expected, actual) in [
            (
                "numDeletedRecordsOpt",
                self.num_deleted_records_opt,
                actual.num_deleted_records_opt,
            ),
            (
                "numDeletionVectorsOpt",
                self.num_deletion_vectors_opt,
                actual.num_deletion_vectors_opt,
            ),
        ] {
            if expected.is_some() {
                check_crc_field(self.version, field, &expected, &actual)?;
            }
        }
        if self.deleted_record_counts_histogram_opt.is_some() {
            check_crc_field(
                self.version,
                "deletedRecordCountsHistogramOpt",
                &self.deleted_record_counts_histogram_opt,
                &actual.deleted_record_counts_histogram_opt,
            )?;
        }
        if self.txn_id.is_some() {
            check_crc_field(self.version, "txnId", &self.txn_id, &actual.txn_id)?;
        }
        if let Some(expected) = &self.all_files {
            let actual = actual
                .all_files
                .as_ref()
                .ok_or_else(|| Error::internal_error("CRC replay did not collect allFiles"))?;
            check_crc_field(self.version, "allFiles", &expected.len(), &actual.len())?;
            let mut expected: Vec<_> = expected.iter().collect();
            let mut actual: Vec<_> = actual.iter().collect();
            let sort_files = |files: &mut Vec<&Add>| {
                files.sort_by(|a, b| {
                    (&a.path, a.modification_time, a.size).cmp(&(
                        &b.path,
                        b.modification_time,
                        b.size,
                    ))
                });
            };
            sort_files(&mut expected);
            sort_files(&mut actual);
            for (expected, actual) in expected.into_iter().zip(actual) {
                // dataChange describes the commit, not the reconciled file state. Per-file
                // statistics are outside CRC validation's comparison scope.
                let mut expected = expected.clone();
                let mut actual = actual.clone();
                expected.data_change = false;
                actual.data_change = false;
                expected.stats = None;
                actual.stats = None;
                check_crc_field(self.version, "allFiles", &expected, &actual)?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_file_stats(&self, actual: Option<&FileStats>) -> DeltaResult<()> {
        let Some(expected) = self.file_stats() else {
            return Ok(());
        };
        check_crc_field(
            self.version,
            "numFiles",
            &Some(expected.num_files()),
            &actual.map(FileStats::num_files),
        )?;
        check_crc_field(
            self.version,
            "tableSizeBytes",
            &Some(expected.table_size_bytes()),
            &actual.map(FileStats::table_size_bytes),
        )?;
        if expected.file_size_histogram().is_some() {
            check_crc_field(
                self.version,
                "fileSizeHistogram",
                &expected.file_size_histogram(),
                &actual.and_then(FileStats::file_size_histogram),
            )?;
        }
        Ok(())
    }
}

impl FileStats {
    pub(crate) fn replay_accumulator(histogram: Option<FileSizeHistogram>) -> Self {
        Self {
            file_size_histogram: histogram,
            ..Default::default()
        }
    }

    pub(crate) fn add_file(&mut self, size: i64) -> DeltaResult<()> {
        require!(
            size >= 0,
            Error::generic("Cannot validate CRC: negative Add size")
        );
        self.num_files = self
            .num_files
            .checked_add(1)
            .ok_or_else(|| Error::generic("Cannot validate CRC: live file count exceeds i64"))?;
        self.table_size_bytes = self
            .table_size_bytes
            .checked_add(size)
            .ok_or_else(|| Error::generic("Cannot validate CRC: table size exceeds i64"))?;
        if let Some(histogram) = &mut self.file_size_histogram {
            histogram.insert(size)?;
        }
        Ok(())
    }
}

/// Compares completed replay state only after successful exhaustion, never on drop or input error.
pub(crate) fn with_crc_validation<T>(
    mut input: impl Iterator<Item = DeltaResult<T>>,
    validate: impl FnOnce() -> DeltaResult<()>,
) -> impl Iterator<Item = DeltaResult<T>> {
    let mut validate = Some(validate);
    std::iter::from_fn(move || {
        validate.as_ref()?;
        match input.next() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(error)) => {
                validate.take();
                Some(Err(error))
            }
            None => validate.take()?().err().map(Err),
        }
    })
}

fn complete_transactions(state: &SetTransactionState) -> Option<&HashMap<String, SetTransaction>> {
    match state {
        SetTransactionState::Complete(transactions) => Some(transactions),
        SetTransactionState::Partial(_) => None,
    }
}

fn non_expired_transactions(
    transactions: &HashMap<String, SetTransaction>,
    expiration_timestamp: Option<i64>,
) -> HashMap<String, SetTransaction> {
    transactions
        .iter()
        .filter(|(_, transaction)| {
            transaction
                .non_expired_version(expiration_timestamp)
                .is_some()
        })
        .map(|(app_id, transaction)| (app_id.clone(), transaction.clone()))
        .collect()
}

fn check_crc_field<T: Debug + PartialEq>(
    version: Version,
    field: &'static str,
    expected: &T,
    actual: &T,
) -> DeltaResult<()> {
    require!(
        expected == actual,
        Error::ChecksumMismatch {
            version,
            field,
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use test_utils::assert_result_error_with_message;

    use super::*;

    #[rstest]
    #[case::negative_size(vec![-1], "negative Add size")]
    #[case::byte_total_overflow(vec![i64::MAX, 1], "table size exceeds i64")]
    fn crc_accumulation_rejects_invalid_sizes(#[case] sizes: Vec<i64>, #[case] message: &str) {
        let mut accumulator = FileStats::replay_accumulator(None);
        assert_result_error_with_message(
            sizes
                .into_iter()
                .try_for_each(|size| accumulator.add_file(size)),
            message,
        );
    }

    #[test]
    fn crc_deletion_vector_accumulation_covers_bin_boundaries() -> DeltaResult<()> {
        let mut crc = Crc::replay_accumulator(0, None, None);
        let cardinalities = [
            0,
            1,
            9,
            10,
            99,
            100,
            999,
            1_000,
            9_999,
            10_000,
            99_999,
            100_000,
            999_999,
            1_000_000,
            9_999_999,
            10_000_000,
            2_147_483_646,
            2_147_483_647,
            2_147_483_648,
        ];
        crc.add_deletion_vector(None)?;
        for cardinality in cardinalities {
            crc.add_deletion_vector(Some(cardinality))?;
        }
        assert_eq!(
            crc.num_deleted_records_opt,
            Some(cardinalities.iter().sum())
        );
        assert_eq!(crc.num_deletion_vectors_opt, Some(19));
        assert_eq!(
            crc.deleted_record_counts_histogram_opt
                .unwrap()
                .deleted_record_counts,
            vec![2; 10]
        );
        Ok(())
    }

    #[rstest]
    #[case::negative(vec![-1], "Negative deletion-vector cardinality")]
    #[case::overflow(vec![i64::MAX, 1], "deletion-vector total overflow")]
    fn crc_deletion_vector_accumulation_rejects_invalid_totals(
        #[case] cardinalities: Vec<i64>,
        #[case] message: &str,
    ) {
        let mut crc = Crc::replay_accumulator(0, None, None);
        assert_result_error_with_message(
            cardinalities
                .into_iter()
                .try_for_each(|value| crc.add_deletion_vector(Some(value))),
            message,
        );
    }

    #[test]
    fn crc_validation_propagates_input_error_without_comparing_partial_totals() {
        let input = [Ok(()), Err(Error::generic("read failed")), Ok(())].into_iter();
        let mut iter = with_crc_validation(input, || panic!("must not validate"));
        assert!(iter.next().unwrap().is_ok());
        assert_result_error_with_message(iter.next().unwrap(), "read failed");
        assert!(iter.next().is_none());
    }

    #[test]
    fn crc_validation_runs_once_after_all_batches() {
        let mut calls = 0;
        let mut iter = with_crc_validation([Ok(()), Ok(())].into_iter(), || {
            calls += 1;
            Err(Error::generic("checksum mismatch"))
        });
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().unwrap().is_ok());
        assert_result_error_with_message(iter.next().unwrap(), "checksum mismatch");
        assert!(iter.next().is_none());
        drop(iter);
        assert_eq!(calls, 1);
    }

    #[test]
    fn crc_validation_does_not_run_on_early_drop() {
        let mut iter =
            with_crc_validation([Ok(()), Ok(())].into_iter(), || panic!("must not validate"));
        assert!(iter.next().unwrap().is_ok());
        drop(iter);
    }
}
