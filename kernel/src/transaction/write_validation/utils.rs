use std::collections::HashSet;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, MapItem, TypedGetData as _};
use crate::utils::require;
use crate::{DeltaResult, Error};

pub(super) fn validate_required_field_exist<T>(
    value: Option<T>,
    path: &str,
    field: &str,
) -> DeltaResult<T> {
    value.ok_or_else(|| {
        Error::missing_data(format!(
            "AddFile for '{path}' is missing required field '{field}'"
        ))
    })
}

pub(super) fn validate_partition_keys(
    path: &str,
    actual_partition_values: MapItem<'_>,
    expected_physical_partition_columns: &HashSet<String>,
) -> DeltaResult<()> {
    let actual_keys_vec: Vec<&str> = actual_partition_values.keys().collect();
    let actual_keys_set: HashSet<&str> = actual_keys_vec.iter().copied().collect();
    let keys_match = actual_keys_set.len() == expected_physical_partition_columns.len()
        && actual_keys_set
            .iter()
            .all(|key| expected_physical_partition_columns.contains(*key));

    require!(
        actual_keys_vec.len() == actual_keys_set.len(),
        Error::invalid_partition_values(format!(
            "AddFile for '{path}' has duplicate partition column names in partitionValues: \
             {actual_keys_vec:?}"
        ))
    );
    require!(
        keys_match,
        Error::invalid_partition_values(format!(
            "AddFile for '{path}' has partitionValues keys {actual_keys_vec:?}, but the table's \
             physical partition columns are {expected_physical_partition_columns:?}"
        ))
    );
    Ok(())
}

pub(super) fn deletion_vector_unique_id<'a>(
    row: usize,
    getters: &[&'a dyn GetData<'a>],
    storage_type_column: usize,
    column_names: [&str; 3],
) -> DeltaResult<Option<String>> {
    let storage_type: Option<&str> = getters[storage_type_column].get_opt(row, column_names[0])?;
    let Some(storage_type) = storage_type else {
        return Ok(None);
    };
    let path_or_inline_dv: &str = getters[storage_type_column + 1].get(row, column_names[1])?;
    let offset: Option<i32> = getters[storage_type_column + 2].get_opt(row, column_names[2])?;
    Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
        storage_type,
        path_or_inline_dv,
        offset,
    )))
}
