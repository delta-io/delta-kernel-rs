use std::collections::HashSet;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::MapItem;
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

pub(super) fn deletion_vector_unique_id(
    storage_type: Option<&str>,
    path_or_inline_dv: Option<&str>,
    offset: Option<i32>,
) -> DeltaResult<Option<String>> {
    let Some(storage_type) = storage_type else {
        return Ok(None);
    };
    let path_or_inline_dv = path_or_inline_dv.ok_or_else(|| {
        Error::missing_data("DeletionVector is missing required field 'pathOrInlineDv'")
    })?;
    Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
        storage_type,
        path_or_inline_dv,
        offset,
    )))
}
