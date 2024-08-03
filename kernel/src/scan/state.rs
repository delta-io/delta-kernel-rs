//! This module encapsulates the state of a scan

use std::collections::HashMap;

use crate::{
    actions::{
        deletion_vector::{treemap_to_bools, DeletionVectorDescriptor},
        visitors::visit_deletion_vector_at,
    },
    engine_data::{GetData, TypedGetData},
    features::ColumnMappingMode,
    schema::SchemaRef,
    DataVisitor, DeltaResult, Engine, EngineData, Error,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::log_replay::SCAN_ROW_SCHEMA;

/// State that doesn't change beween scans
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalScanState {
    pub table_root: String,
    pub partition_columns: Vec<String>,
    pub logical_schema: SchemaRef,
    pub read_schema: SchemaRef,
    pub column_mapping_mode: ColumnMappingMode,
}

/// this struct can be used by an engine to materialize a selection vector
#[derive(Debug)]
pub struct DvInfo {
    pub(crate) deletion_vector: Option<DeletionVectorDescriptor>,
}

/// Give engines an easy way to consume stats
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    /// For any file where the deletion vector is not present (see [`DvInfo::has_vector`]), the
    /// `num_records` statistic must be present and accurate, and must equal the number of records
    /// in the data file. In the presence of Deletion Vectors the statistics may be somewhat
    /// outdated, i.e. not reflecting deleted rows yet.
    pub num_records: u64,
}

impl DvInfo {
    /// Check if this DvInfo contains a Deletion Vector. This is mostly used to know if the
    /// associated [`Stats`] struct has fully accurate information or not.
    pub fn has_vector(&self) -> bool {
        self.deletion_vector.is_some()
    }

    pub fn get_selection_vector(
        &self,
        engine: &dyn Engine,
        table_root: &url::Url,
    ) -> DeltaResult<Option<Vec<bool>>> {
        let dv_treemap = self
            .deletion_vector
            .as_ref()
            .map(|dv_descriptor| {
                let fs_client = engine.get_file_system_client();
                dv_descriptor.read(fs_client, table_root)
            })
            .transpose()?;
        Ok(dv_treemap.map(treemap_to_bools))
    }

    /// Returns a vector of row indexes that should be *removed* from the result set
    pub fn get_row_indexes(
        &self,
        engine: &dyn Engine,
        table_root: &url::Url,
    ) -> DeltaResult<Option<Vec<u64>>> {
        self.deletion_vector
            .as_ref()
            .map(|dv| {
                let fs_client = engine.get_file_system_client();
                dv.row_indexes(fs_client, table_root)
            })
            .transpose()
    }
}

pub type ScanCallback<T> = fn(
    context: &mut T,
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    partition_values: HashMap<String, String>,
);

/// Request that the kernel call a callback on each valid file that needs to be read for the
/// scan.
///
/// The arguments to the callback are:
/// * `context`: an `&mut context` argument. this can be anything that engine needs to pass through to each call
/// * `path`: a `&str` which is the path to the file
/// * `size`: an `i64` which is the size of the file
/// * `dv_info`: a [`DvInfo`] struct, which allows getting the selection vector for this file
/// * `partition_values`: a `HashMap<String, String>` which are partition values
///
/// ## Context
/// A note on the `context`. This can be any value the engine wants. This function takes ownership
/// of the passed arg, but then returns it, so the engine can repeatedly call `visit_scan_files`
/// with the same context.
///
/// ## Example
/// ```ignore
/// let mut context = [my context];
/// for res in scan_data { // scan data from scan.get_scan_data()
///     let (data, vector) = res?;
///     context = delta_kernel::scan::state::visit_scan_files(
///        data.as_ref(),
///        vector,
///        context,
///        my_callback,
///     )?;
/// }
/// ```
pub fn visit_scan_files<T>(
    data: &dyn EngineData,
    selection_vector: &[bool],
    context: T,
    callback: ScanCallback<T>,
) -> DeltaResult<T> {
    let mut visitor = ScanFileVisitor {
        callback,
        selection_vector,
        context,
    };
    data.extract(super::log_replay::SCAN_ROW_SCHEMA.clone(), &mut visitor)?;
    Ok(visitor.context)
}

// add some visitor magic for engines
struct ScanFileVisitor<'a, T> {
    callback: ScanCallback<T>,
    selection_vector: &'a [bool],
    context: T,
}

impl<T> DataVisitor for ScanFileVisitor<'_, T> {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for row_index in 0..row_count {
            if !self.selection_vector[row_index] {
                // skip skipped rows
                continue;
            }
            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(row_index, "scanFile.path")? {
                let size = getters[1].get(row_index, "scanFile.size")?;
                let stats: Option<String> = getters[3].get_opt(row_index, "scanFile.stats")?;
                let stats: Option<Stats> =
                    stats.and_then(|json| match serde_json::from_str(json.as_str()) {
                        Ok(stats) => Some(stats),
                        Err(e) => {
                            warn!("Invalid stats string in Add file {json}: {}", e);
                            None
                        }
                    });

                let dv_index = SCAN_ROW_SCHEMA
                    .index_of("deletionVector")
                    .ok_or_else(|| Error::missing_column("deletionVector"))?;
                let deletion_vector = visit_deletion_vector_at(row_index, &getters[dv_index..])?;
                let dv_info = DvInfo { deletion_vector };
                let partition_values =
                    getters[9].get(row_index, "scanFile.fileConstantValues.partitionValues")?;
                (self.callback)(
                    &mut self.context,
                    path,
                    size,
                    stats,
                    dv_info,
                    partition_values,
                )
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::scan::test_utils::{add_batch_simple, run_with_validate_callback};

    use super::{DvInfo, Stats};

    #[derive(Clone)]
    struct TestContext {
        id: usize,
    }

    fn validate_visit(
        context: &mut TestContext,
        path: &str,
        size: i64,
        stats: Option<Stats>,
        dv_info: DvInfo,
        part_vals: HashMap<String, String>,
    ) {
        assert_eq!(
            path,
            "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
        );
        assert_eq!(size, 635);
        assert!(stats.is_some());
        assert_eq!(stats.as_ref().unwrap().num_records, 10);
        assert_eq!(part_vals.get("date"), Some(&"2017-12-10".to_string()));
        assert_eq!(part_vals.get("non-existent"), None);
        assert!(dv_info.deletion_vector.is_some());
        let dv = dv_info.deletion_vector.unwrap();
        assert_eq!(dv.unique_id(), "uvBn[lx{q8@P<9BNH/isA@1");
        assert_eq!(context.id, 2);
    }

    #[test]
    fn test_simple_visit_scan_data() {
        let context = TestContext { id: 2 };
        run_with_validate_callback(
            vec![add_batch_simple()],
            &[true, false],
            context,
            validate_visit,
        );
    }
}
