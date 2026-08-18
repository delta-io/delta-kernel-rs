//! Remove-file validations.

use std::sync::LazyLock;

use super::utils::{deletion_vector_unique_id, validate_required_field_exist};
use super::{FileActionTracker, StagedDataValidator, Validation};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::expressions::column_name;
use crate::scan::log_replay::{PATH_NAME, SIZE_NAME};
use crate::scan::scan_row_schema;
use crate::schema::ColumnNamesAndTypes;
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Column indices, matching the order in [`MANDATORY_REMOVE_FILE_COLUMNS`].
const PATH: usize = 0;
const SIZE: usize = 1;
const DELETION_VECTOR_STORAGE_TYPE: usize = 2;

const DELETION_VECTOR_COLUMN_NAMES: [&str; 3] = [
    "deletionVector.storageType",
    "deletionVector.pathOrInlineDv",
    "deletionVector.offset",
];

static MANDATORY_REMOVE_FILE_COLUMNS: LazyLock<DeltaResult<ColumnNamesAndTypes>> =
    LazyLock::new(|| {
        let names = vec![
            column_name!(PATH_NAME),
            column_name!(SIZE_NAME),
            column_name!("deletionVector.storageType"),
            column_name!("deletionVector.pathOrInlineDv"),
            column_name!("deletionVector.offset"),
        ];
        let types = names
            .iter()
            .map(|name| {
                scan_row_schema()
                    .field_at(name)
                    .map(|field| field.data_type().clone())
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok((names, types).into())
    });

impl<'a> StagedDataValidator<'a> {
    pub(crate) fn staged_remove_file(file_actions: &'a mut FileActionTracker) -> DeltaResult<Self> {
        let columns = MANDATORY_REMOVE_FILE_COLUMNS.as_ref().map_err(|error| {
            Error::internal_error(format!(
                "RemoveFile validation columns must exist in the scan-row schema: {error}"
            ))
        })?;
        Ok(StagedDataValidator::new(
            columns,
            vec![Box::new(RemoveFileRequiredFields { file_actions })],
        ))
    }
}

/// Validates required `RemoveFile` fields: `path` must be present and non-empty, and `size`
/// must be present and non-negative.
///
/// The protocol defines `size` as optional, but kernel requires it because its `RemoveFile`
/// actions currently come only from `AddFile` actions, which provide `size`.
struct RemoveFileRequiredFields<'a> {
    file_actions: &'a mut FileActionTracker,
}

impl Validation for RemoveFileRequiredFields<'_> {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("RemoveFile is missing required field 'path'"))?;
        require!(
            !path.is_empty(),
            Error::generic("RemoveFile path must not be empty")
        );
        let size = validate_required_field_exist::<i64>(
            getters[SIZE].get_opt(row, "size")?,
            path,
            "size",
        )?;
        require!(
            size >= 0,
            Error::generic(format!(
                "RemoveFile for '{path}' has negative size {size}; size must be non-negative"
            ))
        );
        let dv_id = deletion_vector_unique_id(
            row,
            getters,
            DELETION_VECTOR_STORAGE_TYPE,
            DELETION_VECTOR_COLUMN_NAMES,
        )?;
        self.file_actions.record_remove(path, dv_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{new_null_array, ArrayRef, Int64Array, StringArray};
    use crate::arrow::compute::concat_batches;
    use crate::arrow::datatypes::Schema as ArrowSchema;
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine_data::FilteredEngineData;
    use crate::expressions::ColumnName;
    use crate::unit_test_utils::{assert_result_error_with_message, replace_column};

    #[test]
    fn column_indices_match_schema_order() {
        let columns = MANDATORY_REMOVE_FILE_COLUMNS
            .as_ref()
            .expect("RemoveFile validation columns should exist in the scan-row schema");
        let (names, _) = columns.as_ref();
        assert_eq!(names[PATH], ColumnName::new(["path"]));
        assert_eq!(names[SIZE], ColumnName::new(["size"]));
        assert_eq!(
            names[DELETION_VECTOR_STORAGE_TYPE],
            ColumnName::new(["deletionVector", "storageType"])
        );
        assert_eq!(names.len(), 5);
    }

    #[rstest]
    #[case::selected(&[true, true], Some("multiple RemoveFile actions"))]
    #[case::unselected(&[true, false], None)]
    fn duplicate_remove_paths_validate_selected_rows(
        #[case] selection_vector: &[bool],
        #[case] expected_error: Option<&str>,
    ) {
        let batch = replace_column(
            &nullable_staged_remove_files(2 /* row_count */),
            PATH_NAME,
            Arc::new(StringArray::from(vec!["same", "same"])),
        );
        let removes = [FilteredEngineData::try_new(
            Box::new(ArrowEngineData::new(batch)),
            selection_vector.to_vec(),
        )
        .expect("valid remove-file selection vector")];
        let result = validate_remove_files(&removes);

        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.expect("unselected duplicate RemoveFile should be ignored");
        }
    }

    #[rstest]
    #[case::valid_non_negative_sizes(
        &[
            Some("dummy_path_1"),
            Some("dummy_path_2"),
            Some("dummy_path_3"),
        ],
        &[Some(1), Some(0), Some(1)],
        &[true, true, true],
        None,
    )]
    #[case::missing_path_selected(
        &[None, Some("dummy_path_2"), Some("dummy_path_3")],
        &[Some(1), Some(1), Some(1)],
        &[true, true, true],
        Some("missing required field 'path'"),
    )]
    #[case::empty_path_selected(
        &[Some("dummy_path_1"), Some(""), Some("dummy_path_3")],
        &[Some(1), Some(1), Some(1)],
        &[true, true, true],
        Some("path must not be empty"),
    )]
    #[case::invalid_paths_unselected(
        &[None, Some(""), Some("dummy_path_3")],
        &[Some(1), Some(1), Some(1)],
        &[false, false, true],
        None,
    )]
    #[case::missing_size_selected(
        &[
            Some("dummy_path_1"),
            Some("dummy_path_2"),
            Some("dummy_path_3"),
        ],
        &[Some(1), None, Some(1)],
        &[true, true, true],
        Some("missing required field 'size'"),
    )]
    #[case::negative_size_selected(
        &[
            Some("dummy_path_1"),
            Some("dummy_path_2"),
            Some("dummy_path_3"),
        ],
        &[Some(1), Some(1), Some(-1)],
        &[true, true, true],
        Some("size must be non-negative"),
    )]
    #[case::invalid_sizes_unselected(
        &[
            Some("dummy_path_1"),
            Some("dummy_path_2"),
            Some("dummy_path_3"),
        ],
        &[None, Some(-1), Some(1)],
        &[false, false, true],
        None,
    )]
    #[case::short_selection_vector_selects_trailing_invalid_row(
        &[Some("dummy_path_1"), Some("dummy_path_2"), None],
        &[Some(1), Some(1), Some(1)],
        &[false, false],
        Some("missing required field 'path'"),
    )]
    fn remove_file_values_accepted_or_rejected(
        #[case] paths: &[Option<&str>],
        #[case] sizes: &[Option<i64>],
        #[case] selection_vector: &[bool],
        #[case] expected_error: Option<&str>,
        #[values(0, 1)] case_batch_index: usize,
    ) {
        let batch = replace_column(
            &nullable_staged_remove_files(paths.len()),
            "path",
            Arc::new(StringArray::from(paths.to_vec())),
        );
        let batch = replace_column(&batch, "size", Arc::new(Int64Array::from(sizes.to_vec())));
        let remove = FilteredEngineData::try_new(
            Box::new(ArrowEngineData::new(batch)),
            selection_vector.to_vec(),
        )
        .expect("valid remove-file selection vector");
        let mut removes = vec![
            all_rows_selected(nullable_staged_remove_file("default-path-0")),
            all_rows_selected(nullable_staged_remove_file("default-path-1")),
        ];
        removes[case_batch_index] = remove;
        let result = validate_remove_files(&removes);

        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.unwrap();
        }
    }

    fn nullable_staged_remove_file(path: &str) -> RecordBatch {
        let arrow_schema: ArrowSchema = scan_row_schema()
            .as_ref()
            .try_into_arrow()
            .expect("scan-row schema should convert to Arrow");
        let columns = arrow_schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "path" => Arc::new(StringArray::from(vec![path])) as ArrayRef,
                "size" => Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                _ => new_null_array(field.data_type(), 1 /* length */),
            })
            .collect();

        RecordBatch::try_new(Arc::new(arrow_schema), columns)
            .expect("valid staged remove-file batch")
    }

    fn nullable_staged_remove_files(row_count: usize) -> RecordBatch {
        let batch = nullable_staged_remove_file("dummy");
        concat_batches(&batch.schema(), &vec![batch; row_count])
            .expect("failed to concatenate rows into a multi-row remove-file batch")
    }

    fn all_rows_selected(batch: RecordBatch) -> FilteredEngineData {
        FilteredEngineData::with_all_rows_selected(Box::new(ArrowEngineData::new(batch)))
    }

    fn validate_remove_files(removes: &[FilteredEngineData]) -> DeltaResult<()> {
        let mut file_actions = FileActionTracker::default();
        let result =
            StagedDataValidator::staged_remove_file(&mut file_actions)?.validate_filtered(removes);
        result
    }
}
