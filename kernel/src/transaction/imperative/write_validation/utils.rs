use std::collections::HashSet;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, MapItem, TypedGetData as _};
use crate::expressions::ColumnName;
use crate::schema::{ColumnNamesAndTypes, StructType};
use crate::utils::require;
use crate::{KernelError, KernelResult};

pub(super) const DELETION_VECTOR_NAME: &str = "deletionVector";
pub(super) const STORAGE_TYPE_NAME: &str = "storageType";
pub(super) const PATH_OR_INLINE_DV_NAME: &str = "pathOrInlineDv";
pub(super) const OFFSET_NAME: &str = "offset";

pub(super) fn columns_and_types_from_schema(
    schema: &StructType,
    names: Vec<ColumnName>,
) -> KernelResult<ColumnNamesAndTypes> {
    let types = names
        .iter()
        .map(|name| schema.field_at(name).map(|field| field.data_type().clone()))
        .collect::<KernelResult<Vec<_>>>()?;
    Ok((names, types).into())
}

/// Gets the DV ID by reading contiguous `storageType`, `pathOrInlineDv`, and `offset` getters
/// starting at `base`.
pub(super) fn dv_id_at<'a>(
    getters: &[&'a dyn GetData<'a>],
    base: usize,
    row: usize,
) -> KernelResult<Option<String>> {
    deletion_vector_unique_id(
        getters[base].get_opt(row, STORAGE_TYPE_NAME)?,
        getters[base + 1].get_opt(row, PATH_OR_INLINE_DV_NAME)?,
        getters[base + 2].get_opt(row, OFFSET_NAME)?,
    )
}

pub(super) fn validate_required_field_exist<T>(
    value: Option<T>,
    path: &str,
    field: &str,
) -> KernelResult<T> {
    value.ok_or_else(|| {
        KernelError::missing_data(format!(
            "AddFile for '{path}' is missing required field '{field}'"
        ))
    })
}

pub(super) fn validate_partition_keys(
    path: &str,
    actual_partition_values: MapItem<'_>,
    expected_physical_partition_columns: &HashSet<String>,
) -> KernelResult<()> {
    let actual_keys_vec: Vec<&str> = actual_partition_values.keys().collect();
    let actual_keys_set: HashSet<&str> = actual_keys_vec.iter().copied().collect();
    let keys_match = actual_keys_set.len() == expected_physical_partition_columns.len()
        && actual_keys_set
            .iter()
            .all(|key| expected_physical_partition_columns.contains(*key));

    require!(
        actual_keys_vec.len() == actual_keys_set.len(),
        KernelError::invalid_partition_values(format!(
            "AddFile for '{path}' has duplicate partition column names in partitionValues: \
             {actual_keys_vec:?}"
        ))
    );
    require!(
        keys_match,
        KernelError::invalid_partition_values(format!(
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
) -> KernelResult<Option<String>> {
    let Some(storage_type) = storage_type else {
        return Ok(None);
    };
    let path_or_inline_dv = path_or_inline_dv.ok_or_else(|| {
        KernelError::missing_data("DeletionVector is missing required field 'pathOrInlineDv'")
    })?;
    Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
        storage_type,
        path_or_inline_dv,
        offset,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unit_test_utils::assert_result_error_with_message;

    #[test]
    fn dv_unique_id_errors_when_path_missing() {
        assert_result_error_with_message(
            deletion_vector_unique_id(Some("u"), None, Some(0)),
            "DeletionVector is missing required field 'pathOrInlineDv'",
        );
    }
}
