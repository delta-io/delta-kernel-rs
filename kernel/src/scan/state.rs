//! This module encapsulates the state of a scan

use std::collections::HashMap;

use crate::{
    actions::deletion_vector::{treemap_to_bools, DeletionVectorDescriptor},
    engine_data::{GetData, TypedGetData},
    schema::Schema,
    DataVisitor, DeltaResult, EngineData, EngineInterface,
};
use serde::{Deserialize, Serialize};

/// State that doesn't change beween scans
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalScanState {
    pub table_root: String,
    pub partition_columns: Vec<String>,
    pub logical_schema: Schema,
    pub read_schema: Schema,
}

/// this struct can be used by an engine to materialize a selection vector
#[derive(Debug)]
pub struct DvInfo {
    deletion_vector: Option<DeletionVectorDescriptor>,
}

impl DvInfo {
    pub fn get_selection_vector(
        &self,
        engine_interface: &dyn EngineInterface,
        table_root: &url::Url,
    ) -> DeltaResult<Option<Vec<bool>>> {
        let dv_treemap = self
            .deletion_vector
            .as_ref()
            .map(|dv_descriptor| {
                let fs_client = engine_interface.get_file_system_client();
                dv_descriptor.read(fs_client, table_root)
            })
            .transpose()?;
        Ok(dv_treemap.map(treemap_to_bools))
    }
}

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
/// A note on the `context`. This can be any value the engine wants. This function takes ownership of the passed arg, but then returns it, so the engine can repeatedly call `visit_scan_files` with the same context.
/// ## Example
/// ```ignore
/// let context = [my context];
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
    selection_vector: Vec<bool>,
    context: T,
    callback: fn(
        context: &mut T,
        path: &str,
        size: i64,
        dv_info: DvInfo,
        partition_values: HashMap<String, String>,
    ) -> (),
) -> DeltaResult<T> {
    let mut visitor = ScanFileVisitor {
        callback,
        selection_vector,
        context,
    };
    data.extract(super::file_stream::SCAN_ROW_SCHEMA.clone(), &mut visitor)?;
    Ok(visitor.context)
}

// add some visitor magic for clients
struct ScanFileVisitor<T> {
    callback: fn(&mut T, &str, i64, DvInfo, HashMap<String, String>) -> (),
    selection_vector: Vec<bool>,
    context: T,
}

impl<T> DataVisitor for ScanFileVisitor<T> {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for row_index in 0..row_count {
            if !self.selection_vector[row_index] {
                // skip skipped rows
                continue;
            }
            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(row_index, "scanFile.path")? {
                let size = getters[1].get(row_index, "scanFile.size")?;
                let deletion_vector = if let Some(storage_type) =
                    getters[3].get_opt(row_index, "scanFile.deletionVector.storageType")?
                {
                    // there is a storageType, so the whole DV must be there
                    let path_or_inline_dv: String =
                        getters[4].get(row_index, "scanFile.deletionVector.pathOrInlineDv")?;
                    let offset: Option<i32> =
                        getters[5].get_opt(row_index, "scanFile.deletionVector.offset")?;
                    let size_in_bytes: i32 =
                        getters[6].get(row_index, "scanFile.deletionVector.sizeInBytes")?;
                    let cardinality: i64 =
                        getters[7].get(row_index, "scanFile.deletionVector.cardinality")?;
                    Some(DeletionVectorDescriptor {
                        storage_type,
                        path_or_inline_dv,
                        offset,
                        size_in_bytes,
                        cardinality,
                    })
                } else {
                    None
                };
                let dv_info = DvInfo { deletion_vector };
                let partition_values: HashMap<_, _> =
                    getters[8].get(row_index, "scanFile.fileConstantValues.partitionValues")?;
                (self.callback)(&mut self.context, path, size, dv_info, partition_values)
            }
        }
        Ok(())
    }
}
