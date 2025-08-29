use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use delta_kernel_derive::{internal_api, ToSchema};

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::transaction::add_files_schema;
use crate::utils::require;
use crate::{DeltaResult, Error};

pub(crate) const ROW_TRACKING_DOMAIN_NAME: &str = "delta.rowTracking";

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RowTrackingDomainMetadata {
    // The Delta spec does not define unsigned integers, so we use i64 here
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) row_id_high_water_mark: Option<i64>,
}

/// A row visitor that iterates over preliminary [`Add`] actions as returned by the engine and
/// computes a base row ID for each action.
/// It expects to visit engine data conforming to the schema returned by [`add_files_schema()`].
///
/// This visitor it only required for the row tracking write path. The read path will be completely
/// implemented via expressions.
///
/// [`Add`]: delta_kernel::actions::Add
#[internal_api]
pub(crate) struct RowTrackingVisitor {
    /// High water mark for row IDs
    pub(crate) row_id_high_water_mark: i64,

    /// Computed base row IDs of the visited actions
    pub(crate) base_row_ids: Vec<i64>,
}

impl RowTrackingVisitor {
    /// Field index for "numRecords" in [`add_files_schema()`]
    const NUM_RECORDS_FIELD_INDEX: usize = 5;

    /// Default value for an absent high water mark
    const DEFAULT_HWM: i64 = -1;

    #[internal_api]
    pub(crate) fn new(row_id_high_water_mark: Option<i64>) -> Self {
        // A table might not have a row ID high water mark yet, so we model the input as an Option<i64>
        Self {
            row_id_high_water_mark: row_id_high_water_mark.unwrap_or(Self::DEFAULT_HWM),
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
