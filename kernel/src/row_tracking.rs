use crate::actions::stats::Statistics;
use crate::actions::Add;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::transaction::write_result_schema;
use crate::utils::require;
use crate::{DeltaResult, Error};
use delta_kernel_derive::{internal_api, ToSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

/// A row visitor that iterates over add actions and assigns a base row id and default commit version to each add action.
#[internal_api]
pub(crate) struct RowTrackingVisitor {
    pub(crate) adds: Vec<Add>,
    pub(crate) row_id_high_water_mark: Option<i64>,
    pub(crate) default_row_commit_version: i64,
}

impl RowTrackingVisitor {
    #[internal_api]
    pub(crate) fn new(
        row_id_high_water_mark: Option<i64>,
        default_row_commit_version: i64,
    ) -> Self {
        // There might not be a row ID high water mark yet, so we model it as an Option<i64>
        // There must be a default commit version for each add action though
        Self {
            adds: vec![],
            row_id_high_water_mark,
            default_row_commit_version,
        }
    }

    #[internal_api]
    fn visit_add<'a>(
        &mut self,
        row_index: usize,
        path: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Add> {
        require!(
            getters.len() == 6, // TODO: Replace this magic number
            Error::InternalError(format!(
                "Wrong number of AddVisitor getters: {}",
                getters.len()
            ))
        );

        let partition_values: HashMap<_, _> = getters[1].get(row_index, "partitionValues")?;
        let size: i64 = getters[2].get(row_index, "size")?;
        let modification_time: i64 = getters[3].get(row_index, "modificationTime")?;
        let data_change: bool = getters[4].get(row_index, "dataChange")?;
        let stats: Option<String> = getters[5].get_opt(row_index, "stats")?;

        let statistics: Statistics = match &stats {
            Some(stats) => match serde_json::from_str(stats) {
                Ok(json) => json,
                Err(error) => {
                    return Err(Error::InternalError(format!(
                        "Failed to parse stats JSON in Add action: {error}"
                    )))
                }
            },
            None => {
                return Err(Error::InternalError(
                    "Stats must be present in Add action when row tracking is enabled.".to_string(),
                ));
            }
        };

        let num_records = statistics.num_records.ok_or_else(|| {
            Error::InternalError(
                "numRecords must be present in Add action stats when row tracking is enabled."
                    .to_string(),
            )
        })?;

        let base_row_id = self.row_id_high_water_mark.map_or(0, |hwm| hwm + 1);
        self.row_id_high_water_mark = Some(base_row_id + num_records as i64 - 1);

        Ok(Add {
            path,
            partition_values,
            size,
            modification_time,
            data_change,
            stats,
            tags: None,
            deletion_vector: None,
            base_row_id: Some(base_row_id),
            default_row_commit_version: Some(self.default_row_commit_version),
            clustering_provider: None,
        })
    }
}

impl RowVisitor for RowTrackingVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| write_result_schema().leaves(None));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(i, "path")? {
                let add = self.visit_add(i, path, getters)?;
                self.adds.push(add);
            }
        }
        Ok(())
    }
}

pub(crate) const ROW_TRACKING_DOMAIN: &str = "delta.rowTracking";

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RowTrackingDomainConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) row_id_high_water_mark: Option<i64>,
}
