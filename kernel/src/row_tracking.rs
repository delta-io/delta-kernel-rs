use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::actions::DomainMetadata;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, Snapshot};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RowTrackingDomainMetadata {
    // NB: The Delta spec does not rule out negative high water marks
    row_id_high_water_mark: i64,
}

impl RowTrackingDomainMetadata {
    const ROW_TRACKING_DOMAIN_NAME: &str = "delta.rowTracking";

    pub(crate) fn new(row_id_high_water_mark: i64) -> Self {
        RowTrackingDomainMetadata {
            row_id_high_water_mark,
        }
    }

    /// Retrieves the row ID high water mark from the [`Snapshot`]'s row tracking domain metadata.
    ///
    /// This method searches through the snapshot's log segment for domain metadata actions
    /// with the row tracking domain name and extracts the high water mark value.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(high_water_mark))` if row tracking domain metadata is found,
    /// `Ok(None)` if no row tracking domain metadata exists, or an error if the
    /// metadata cannot be parsed or accessed.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The domain metadata configuration cannot be read from the log segment
    /// - The domain metadata JSON cannot be deserialized into `RowTrackingDomainMetadata`
    pub(crate) fn get_high_water_mark(
        snapshot: &Snapshot,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<i64>> {
        Ok(domain_metadata_configuration(
            snapshot.log_segment(),
            Self::ROW_TRACKING_DOMAIN_NAME,
            engine,
        )?
        .map(|domain_metadata| serde_json::from_str::<Self>(&domain_metadata))
        .transpose()?
        .map(|metadata| metadata.row_id_high_water_mark))
    }
}

impl TryFrom<RowTrackingDomainMetadata> for DomainMetadata {
    type Error = crate::Error;

    fn try_from(metadata: RowTrackingDomainMetadata) -> DeltaResult<Self> {
        Ok(DomainMetadata::new(
            RowTrackingDomainMetadata::ROW_TRACKING_DOMAIN_NAME.to_string(),
            serde_json::to_string(&metadata)?,
        ))
    }
}

/// A row visitor that iterates over preliminary [`Add`] actions as returned by the engine and
/// computes a base row ID for each action.
/// It expects to visit engine data with a nested field 'stats.numRecords' which is
/// part of a Delta add action.
///
/// This visitor is only required for the row tracking write path. The read path will be completely
/// implemented via expressions.
///
/// [`Add`]: delta_kernel::actions::Add
pub(crate) struct RowTrackingVisitor {
    /// High water mark for row IDs
    pub(crate) row_id_high_water_mark: i64,

    /// Computed base row IDs of the visited actions, organized by batch
    pub(crate) base_row_id_batches: Vec<Vec<i64>>,
}

impl RowTrackingVisitor {
    /// Default value for an absent high water mark
    const DEFAULT_HIGH_WATER_MARK: i64 = -1;

    pub(crate) fn new(row_id_high_water_mark: Option<i64>, num_batches: Option<usize>) -> Self {
        // A table might not have a row ID high water mark yet, so we model the input as an Option<i64>
        Self {
            row_id_high_water_mark: row_id_high_water_mark.unwrap_or(Self::DEFAULT_HIGH_WATER_MARK),
            base_row_id_batches: Vec::with_capacity(num_batches.unwrap_or(0)),
        }
    }
}

impl RowVisitor for RowTrackingVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![ColumnName::new(["stats", "numRecords"])],
                vec![DataType::LONG],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::generic(format!(
                "Wrong number of RowTrackingVisitor getters: {}",
                getters.len()
            ))
        );

        // Create a new batch for this visit
        let mut batch_base_row_ids = Vec::with_capacity(row_count);

        let mut current_hwm = self.row_id_high_water_mark;
        for i in 0..row_count {
            let num_records: i64 = getters[0].get_opt(i, "numRecords")?.ok_or_else(|| {
                Error::InternalError(
                    "numRecords must be present in Add actions when row tracking is enabled."
                        .to_string(),
                )
            })?;
            batch_base_row_ids.push(current_hwm + 1);
            current_hwm += num_records;
        }

        self.base_row_id_batches.push(batch_base_row_ids);
        self.row_id_high_water_mark = current_hwm;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_data::GetData;
    use crate::utils::test_utils::assert_result_error_with_message;

    /// Mock GetData implementation for testing
    struct MockGetData {
        num_records_values: Vec<Option<i64>>,
    }

    impl MockGetData {
        fn new(num_records_values: Vec<Option<i64>>) -> Self {
            Self { num_records_values }
        }
    }

    impl<'a> GetData<'a> for MockGetData {
        fn get_long(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<i64>> {
            if field_name == "numRecords" {
                Ok(self.num_records_values.get(row_index).copied().flatten())
            } else {
                Ok(None)
            }
        }
    }

    fn create_getters<'a>(num_records_mock: &'a MockGetData) -> Vec<&'a dyn GetData<'a>> {
        vec![num_records_mock]
    }

    #[test]
    fn test_visit_basic_functionality() -> DeltaResult<()> {
        let mut visitor = RowTrackingVisitor::new(None, Some(1));
        let num_records_mock = MockGetData::new(vec![Some(10), Some(5), Some(20)]);
        let getters = create_getters(&num_records_mock);

        visitor.visit(3, &getters)?;

        // Check that base row IDs are calculated correctly
        assert_eq!(visitor.base_row_id_batches.len(), 1);
        assert_eq!(visitor.base_row_id_batches[0], vec![0, 10, 15]);

        // Check that high water mark is updated correctly
        assert_eq!(visitor.row_id_high_water_mark, 34); // -1 + 10 + 5 + 20

        Ok(())
    }

    #[test]
    fn test_visit_with_negative_high_water_mark() -> DeltaResult<()> {
        let mut visitor = RowTrackingVisitor::new(Some(-5), Some(1));
        let num_records_mock = MockGetData::new(vec![Some(3), Some(2)]);
        let getters = create_getters(&num_records_mock);

        visitor.visit(2, &getters)?;

        // Base row IDs should start from high_water_mark + 1
        assert_eq!(visitor.base_row_id_batches.len(), 1);
        assert_eq!(visitor.base_row_id_batches[0], vec![-4, -1]); // -5+1=-4, then -4+3=-1

        // High water mark should be updated
        assert_eq!(visitor.row_id_high_water_mark, 0); // -5 + 3 + 2 = 0

        Ok(())
    }

    #[test]
    fn test_visit_with_zero_records() -> DeltaResult<()> {
        let mut visitor = RowTrackingVisitor::new(Some(10), Some(1));
        let num_records_mock = MockGetData::new(vec![Some(0), Some(0), Some(5)]);
        let getters = create_getters(&num_records_mock);

        visitor.visit(3, &getters)?;

        // Base row IDs should still be assigned even for zero-record files
        assert_eq!(visitor.base_row_id_batches.len(), 1);
        assert_eq!(visitor.base_row_id_batches[0], vec![11, 11, 11]);

        // High water mark should only increase by non-zero records
        assert_eq!(visitor.row_id_high_water_mark, 15); // 10 + 0 + 0 + 5

        Ok(())
    }

    #[test]
    fn test_visit_empty_batch() -> DeltaResult<()> {
        let mut visitor = RowTrackingVisitor::new(Some(42), None);
        let num_records_mock = MockGetData::new(vec![]);
        let getters = create_getters(&num_records_mock);

        visitor.visit(0, &getters)?;

        // Should handle empty batch gracefully
        assert_eq!(visitor.base_row_id_batches.len(), 1);
        assert!(visitor.base_row_id_batches[0].is_empty());
        assert_eq!(visitor.row_id_high_water_mark, 42); // Should remain unchanged

        Ok(())
    }

    #[test]
    fn test_visit_multiple_batches() -> DeltaResult<()> {
        let mut visitor = RowTrackingVisitor::new(Some(0), Some(2));

        // First batch
        let num_records_mock1 = MockGetData::new(vec![Some(10), Some(5)]);
        let getters1 = create_getters(&num_records_mock1);
        visitor.visit(2, &getters1)?;

        // Second batch
        let num_records_mock2 = MockGetData::new(vec![Some(3), Some(7), Some(2)]);
        let getters2 = create_getters(&num_records_mock2);
        visitor.visit(3, &getters2)?;

        // Check that we have two batches
        assert_eq!(visitor.base_row_id_batches.len(), 2);

        // Check first batch: starts at 1, then 11
        assert_eq!(visitor.base_row_id_batches[0], vec![1, 11]);

        // Check second batch: starts at 16, then 19, then 26
        assert_eq!(visitor.base_row_id_batches[1], vec![16, 19, 26]);

        // Check final high water mark: 0 + 10 + 5 + 3 + 7 + 2 = 27
        assert_eq!(visitor.row_id_high_water_mark, 27);

        Ok(())
    }

    #[test]
    fn test_visit_wrong_getter_count() -> DeltaResult<()> {
        let mut visitor = RowTrackingVisitor::new(Some(0), None);
        let wrong_getters: Vec<&dyn GetData<'_>> = vec![]; // No getters instead of expected count

        let result = visitor.visit(1, &wrong_getters);
        assert_result_error_with_message(result, "Wrong number of RowTrackingVisitor getters");

        Ok(())
    }

    #[test]
    fn test_visit_missing_num_records() -> DeltaResult<()> {
        let mut visitor = RowTrackingVisitor::new(Some(0), None);
        let num_records_mock = MockGetData::new(vec![None]); // Missing numRecords
        let getters = create_getters(&num_records_mock);

        let result = visitor.visit(1, &getters);
        assert_result_error_with_message(
            result,
            "numRecords must be present in Add actions when row tracking is enabled",
        );

        Ok(())
    }

    #[test]
    fn test_selected_column_names_and_types() {
        let visitor = RowTrackingVisitor::new(Some(0), None);
        let (names, types) = visitor.selected_column_names_and_types();

        assert_eq!(names, (vec![ColumnName::new(["stats", "numRecords"])]));
        assert_eq!(types, vec![DataType::LONG]);
    }

    #[test]
    fn test_serialization_roundtrip() -> DeltaResult<()> {
        let original = RowTrackingDomainMetadata::new(-42);
        let json = serde_json::to_string(&original)?;
        let deserialized: RowTrackingDomainMetadata = serde_json::from_str(&json)?;

        assert_eq!(
            original.row_id_high_water_mark,
            deserialized.row_id_high_water_mark
        );

        Ok(())
    }
}
