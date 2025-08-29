use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::actions::DomainMetadata;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::transaction::add_files_schema;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, Snapshot};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RowTrackingDomainMetadata {
    // NB: The Delta spec does not rule out negative high water marks
    #[serde(skip_serializing_if = "Option::is_none")]
    row_id_high_water_mark: Option<i64>,
}

impl RowTrackingDomainMetadata {
    const ROW_TRACKING_DOMAIN_NAME: &str = "delta.rowTracking";

    pub(crate) fn create_domain_metadata(
        row_id_high_water_mark: i64,
    ) -> DeltaResult<DomainMetadata> {
        Ok(DomainMetadata::new(
            Self::ROW_TRACKING_DOMAIN_NAME.to_string(),
            serde_json::to_string(&Self {
                row_id_high_water_mark: Some(row_id_high_water_mark),
            })?,
            false,
        ))
    }

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
        .and_then(|metadata| metadata.row_id_high_water_mark))
    }
}

/// A row visitor that iterates over preliminary [`Add`] actions as returned by the engine and
/// computes a base row ID for each action.
/// It expects to visit engine data conforming to the schema returned by [`add_files_schema()`].
///
/// This visitor is only required for the row tracking write path. The read path will be completely
/// implemented via expressions.
///
/// [`Add`]: delta_kernel::actions::Add
pub(crate) struct RowTrackingVisitor {
    /// High water mark for row IDs
    pub(crate) row_id_high_water_mark: i64,

    /// Computed base row IDs of the visited actions
    pub(crate) base_row_ids: Vec<i64>,
}

impl RowTrackingVisitor {
    /// Default value for an absent high water mark
    const DEFAULT_HIGH_WATER_MARK: i64 = -1;

    /// Field index of "numRecords" in the [`add_files_schema()`]
    ///
    /// We verify this hard-coded index in a test.
    const NUM_RECORDS_FIELD_INDEX: usize = 5;

    pub(crate) fn new(row_id_high_water_mark: Option<i64>) -> Self {
        // A table might not have a row ID high water mark yet, so we model the input as an Option<i64>
        Self {
            row_id_high_water_mark: row_id_high_water_mark.unwrap_or(Self::DEFAULT_HIGH_WATER_MARK),
            base_row_ids: vec![],
        }
    }
}

impl RowVisitor for RowTrackingVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| add_files_schema().leaves(None));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == add_files_schema().fields_len(),
            Error::InternalError(format!(
                "Wrong number of RowTrackingVisitor getters: {}",
                getters.len()
            ))
        );

        // Reset base row ID vector and allocate the necessary capacity
        self.base_row_ids.clear();
        self.base_row_ids.reserve(row_count);

        let mut current_hwm = self.row_id_high_water_mark;
        for i in 0..row_count {
            let num_records: i64 = getters[Self::NUM_RECORDS_FIELD_INDEX]
                .get_opt(i, "numRecords")?
                .ok_or_else(|| {
                    Error::InternalError(
                        "numRecords must be present in Add actions when row tracking is enabled."
                            .to_string(),
                    )
                })?;
            self.base_row_ids.push(current_hwm + 1);
            current_hwm += num_records;
        }

        self.row_id_high_water_mark = current_hwm;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_num_records_field_index() {
        // Verify that the correct numRecords field index is hard-coded in the RowTrackingVisitor
        let num_records_field_index = add_files_schema()
            .leaves(None)
            .as_ref()
            .0
            .iter()
            .position(|name| name.path().last() == Some(&"numRecords".to_string()))
            .expect("numRecords field not found");

        assert_eq!(
            num_records_field_index,
            RowTrackingVisitor::NUM_RECORDS_FIELD_INDEX
        );
    }
}
