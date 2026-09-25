use std::collections::HashSet;

use crate::engine_data::MapItem;
use crate::scan::log_replay::{BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME};
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

// Enabled tables have these values on every active AddFile; removals and DV updates preserve them.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-row-tracking
pub(super) fn validate_row_tracking_metadata(
    path: &str,
    base_row_id: Option<i64>,
    default_row_commit_version: Option<i64>,
) -> DeltaResult<()> {
    for (field, value) in [
        (BASE_ROW_ID_NAME, base_row_id),
        (DEFAULT_ROW_COMMIT_VERSION_NAME, default_row_commit_version),
    ] {
        require!(
            value.is_some(),
            Error::missing_data(format!(
                "File '{path}' is missing required row-tracking field '{field}' \
                 when row tracking is enabled"
            ))
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::unit_test_utils::assert_result_error_with_message;

    #[rstest]
    #[case::present(Some(10), Some(2), None)]
    #[case::zero(Some(0), Some(0), None)]
    #[case::missing_base_row_id(None, Some(2), Some("baseRowId"))]
    #[case::missing_default_row_commit_version(Some(10), None, Some("defaultRowCommitVersion"))]
    #[case::missing_both(None, None, Some("baseRowId"))]
    fn row_tracking_metadata_requires_both_fields(
        #[case] base_row_id: Option<i64>,
        #[case] default_row_commit_version: Option<i64>,
        #[case] expected_error: Option<&str>,
    ) {
        let result =
            validate_row_tracking_metadata("file.parquet", base_row_id, default_row_commit_version);
        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.unwrap();
        }
    }
}
