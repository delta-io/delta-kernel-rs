//! Comparison of completed CRC state with independently replayed state.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::LazyLock;

use super::{Crc, DomainMetadataState, FileSizeHistogram, FileStats, SetTransactionState};
use crate::action_reconciliation::log_replay::ActionReconciliationBatch;
use crate::actions::SetTransaction;
use crate::engine_data::{FilteredRowVisitor, GetData, RowIndexIterator, TypedGetData as _};
use crate::log_segment::CrcReplayAccumulator;
use crate::scan::ScanMetadata;
use crate::schema::{schema, ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, Error, Version};

impl Crc {
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

    fn add_file(&mut self, size: i64) -> DeltaResult<()> {
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

struct FileStatsVisitor<'a>(&'a mut FileStats);

impl FilteredRowVisitor for FileStatsVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| schema! { not_null "size": LONG }.leaves(None));
        COLUMNS.as_ref()
    }

    fn visit_filtered<'a>(
        &mut self,
        getters: &[&'a dyn GetData<'a>],
        rows: RowIndexIterator<'_>,
    ) -> DeltaResult<()> {
        for row in rows {
            self.0.add_file(getters[0].get(row, "size")?)?;
        }
        Ok(())
    }
}

pub(crate) trait CrcAccumulator<T> {
    type Output;

    fn observe(&mut self, batch: &T) -> DeltaResult<()>;
    fn finish(self) -> DeltaResult<Self::Output>;
}

impl CrcAccumulator<ScanMetadata> for FileStats {
    type Output = FileStats;

    fn observe(&mut self, batch: &ScanMetadata) -> DeltaResult<()> {
        FileStatsVisitor(self).visit_rows_of(&batch.scan_files)
    }

    fn finish(self) -> DeltaResult<Self::Output> {
        Ok(self)
    }
}

impl CrcAccumulator<ActionReconciliationBatch> for (CrcReplayAccumulator, Version, Option<i64>) {
    type Output = Crc;

    fn observe(&mut self, batch: &ActionReconciliationBatch) -> DeltaResult<()> {
        self.0.visit_reconciled_batch(&batch.filtered_data)
    }

    fn finish(self) -> DeltaResult<Self::Output> {
        let mut crc = self.0.into_complete_crc(self.1).ok_or_else(|| {
            Error::generic("Cannot validate CRC: replay did not produce protocol and metadata")
        })?;
        crc.in_commit_timestamp_opt = self.2;
        Ok(crc)
    }
}

pub(crate) fn with_crc_validation<T, A>(
    mut input: impl Iterator<Item = DeltaResult<T>>,
    mut accumulator: Option<A>,
    validate: impl FnOnce(A::Output) -> DeltaResult<()>,
) -> impl Iterator<Item = DeltaResult<T>>
where
    A: CrcAccumulator<T>,
{
    let mut finished = false;
    let mut validate = Some(validate);
    std::iter::from_fn(move || {
        if finished {
            return None;
        }
        match input.next() {
            Some(Ok(batch)) => {
                if let Some(accumulator) = &mut accumulator {
                    if let Err(error) = accumulator.observe(&batch) {
                        finished = true;
                        return Some(Err(error));
                    }
                }
                Some(Ok(batch))
            }
            Some(Err(error)) => {
                finished = true;
                Some(Err(error))
            }
            None => {
                finished = true;
                accumulator.take().and_then(|accumulator| {
                    let result = match validate.take() {
                        Some(validate) => accumulator.finish().and_then(validate),
                        None => Err(Error::internal_error("CRC validation ran more than once")),
                    };
                    result.err().map(Err)
                })
            }
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
    use std::sync::Arc;

    use rstest::rstest;
    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::arrow::array::{Int64Array, RecordBatch};
    use crate::arrow::datatypes::{DataType as ArrowType, Field, Schema};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine_data::FilteredEngineData;

    #[rstest]
    #[case::unselected_malformed(false)]
    #[case::selected_malformed(true)]
    fn crc_accumulation_respects_selection_vectors(#[case] selected: bool) {
        let mut accumulator = FileStats::replay_accumulator(None);
        let batch = file_batch(vec![2, -1], vec![true, selected]);
        let result = FileStatsVisitor(&mut accumulator).visit_rows_of(&batch);
        if selected {
            assert_result_error_with_message(result, "negative Add size");
        } else {
            result.unwrap();
            assert_eq!(accumulator.num_files(), 1);
        }
    }

    #[test]
    fn crc_accumulation_rejects_byte_total_overflow() {
        let mut accumulator = FileStats::replay_accumulator(None);
        assert_result_error_with_message(
            FileStatsVisitor(&mut accumulator)
                .visit_rows_of(&file_batch(vec![i64::MAX, 1], vec![])),
            "table size exceeds i64",
        );
    }

    #[test]
    fn crc_validation_propagates_input_error_without_comparing_partial_totals() {
        let input = std::iter::once(Err::<ScanMetadata, _>(Error::generic("read failed")));
        let mut iter =
            with_crc_validation(input, Some(FileStats::replay_accumulator(None)), |_| {
                Err(Error::generic("must not validate"))
            });
        assert_result_error_with_message(iter.next().unwrap(), "read failed");
        assert!(iter.next().is_none());
    }

    fn file_batch(sizes: Vec<i64>, selection: Vec<bool>) -> FilteredEngineData {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "size",
            ArrowType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(sizes))]).unwrap();
        FilteredEngineData::try_new(Box::new(ArrowEngineData::new(batch)), selection).unwrap()
    }
}
