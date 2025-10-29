//! This module encapsulates the state of a scan

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::actions::deletion_vector::deletion_treemap_to_bools;
use crate::scan::get_transform_for_row;
use crate::schema::Schema;
use crate::utils::require;
use crate::ExpressionRef;
use crate::{
    actions::{deletion_vector::DeletionVectorDescriptor, visitors::visit_deletion_vector_at},
    engine_data::{GetData, RowVisitor, TypedGetData as _},
    schema::{ColumnName, ColumnNamesAndTypes, DataType, SchemaRef},
    DeltaResult, Engine, EngineData, Error,
};
use roaring::RoaringTreemap;
use serde::Deserialize;
use tracing::warn;

use super::log_replay::SCAN_ROW_SCHEMA;
use super::ScanMetadata;

/// this struct can be used by an engine to materialize a selection vector
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DvInfo {
    pub(crate) deletion_vector: Option<DeletionVectorDescriptor>,
}

impl From<DeletionVectorDescriptor> for DvInfo {
    fn from(deletion_vector: DeletionVectorDescriptor) -> Self {
        let deletion_vector = Some(deletion_vector);
        DvInfo { deletion_vector }
    }
}

/// Give engines an easy way to consume stats
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

    pub(crate) fn get_treemap(
        &self,
        engine: &dyn Engine,
        table_root: &url::Url,
    ) -> DeltaResult<Option<RoaringTreemap>> {
        self.deletion_vector
            .as_ref()
            .map(|dv_descriptor| {
                let storage = engine.storage_handler();
                dv_descriptor.read(storage, table_root)
            })
            .transpose()
    }

    pub fn get_selection_vector(
        &self,
        engine: &dyn Engine,
        table_root: &url::Url,
    ) -> DeltaResult<Option<Vec<bool>>> {
        let dv_treemap = self.get_treemap(engine, table_root)?;
        Ok(dv_treemap.map(deletion_treemap_to_bools))
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
                let storage = engine.storage_handler();
                dv.row_indexes(storage, table_root)
            })
            .transpose()
    }
}

/// utility function for applying a transform expression to convert data from physical to logical
/// format
pub fn transform_to_logical(
    engine: &dyn Engine,
    physical_data: Box<dyn EngineData>,
    physical_schema: &SchemaRef,
    logical_schema: &Schema,
    transform: Option<ExpressionRef>,
) -> DeltaResult<Box<dyn EngineData>> {
    match transform {
        Some(transform) => engine
            .evaluation_handler()
            .new_expression_evaluator(
                physical_schema.clone(),
                transform,
                logical_schema.clone().into(), // TODO: expensive deep clone!
            )
            .evaluate(physical_data.as_ref()),
        None => Ok(physical_data),
    }
}

pub type ScanCallback<T> = fn(
    context: &mut T,
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    partition_values: HashMap<String, String>,
);

/// Request that the kernel call a callback on each valid file that needs to be read for the
/// scan.
///
/// The arguments to the callback are:
/// * `context`: an `&mut context` argument. this can be anything that engine needs to pass through
///   to each call
/// * `path`: a `&str` which is the path to the file
/// * `size`: an `i64` which is the size of the file
/// * `dv_info`: a [`DvInfo`] struct, which allows getting the selection vector for this file
/// * `transform`: An optional expression that, if present, _must_ be applied to physical data to
///   convert it to the correct logical format
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
/// for res in scan_metadata_iter { // scan metadata iterator from scan.scan_metadata()
///     let scan_metadata = res?;
///     context = scan_metadata.visit_scan_files(
///        context,
///        my_callback,
///     )?;
/// }
/// ```
impl ScanMetadata {
    pub fn visit_scan_files<T>(&self, context: T, callback: ScanCallback<T>) -> DeltaResult<T> {
        let mut visitor = ScanFileVisitor {
            callback,
            selection_vector: self.scan_files.selection_vector(),
            transforms: &self.scan_file_transforms,
            context,
        };
        visitor.visit_rows_of(self.scan_files.data())?;
        Ok(visitor.context)
    }
}
// add some visitor magic for engines
struct ScanFileVisitor<'a, T> {
    callback: ScanCallback<T>,
    selection_vector: &'a [bool],
    transforms: &'a [Option<ExpressionRef>],
    context: T,
}
impl<T> RowVisitor for ScanFileVisitor<'_, T> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| SCAN_ROW_SCHEMA.leaves(None));
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 11,
            Error::InternalError(format!(
                "Wrong number of ScanFileVisitor getters: {}",
                getters.len()
            ))
        );
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
                    get_transform_for_row(row_index, self.transforms),
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

    use crate::actions::get_commit_schema;
    use crate::scan::test_utils::{add_batch_simple, run_with_validate_callback};
    use crate::ExpressionRef;

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
        transform: Option<ExpressionRef>,
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
        assert!(transform.is_none());
        assert_eq!(context.id, 2);
    }

    #[test]
    fn test_simple_visit_scan_metadata() {
        let context = TestContext { id: 2 };
        run_with_validate_callback(
            vec![add_batch_simple(get_commit_schema().clone())],
            None, // not testing schema
            None, // not testing transform
            &[true, false],
            context,
            validate_visit,
        );
    }
}
