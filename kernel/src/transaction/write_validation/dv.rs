//! Deletion-vector update validations.

use std::collections::HashSet;
use std::sync::LazyLock;

use super::utils::{
    columns_from_schema, dv_id_at, validate_partition_keys, validate_required_field_exist,
    DELETION_VECTOR_NAME, OFFSET_NAME, PATH_OR_INLINE_DV_NAME, STORAGE_TYPE_NAME,
};
use super::{FileActionTracker, StagedDataValidator, Validation};
use crate::engine_data::{FilteredEngineData, GetData, TypedGetData as _};
use crate::expressions::column_name;
use crate::scan::log_replay::{
    FILE_CONSTANT_VALUES_NAME, PARTITION_VALUES_NAME, PATH_NAME, SIZE_NAME,
};
use crate::schema::ColumnNamesAndTypes;
use crate::transaction::update::{intermediate_dv_schema, NEW_DELETION_VECTOR_NAME};
use crate::utils::require;
use crate::{KernelError, Result};

const PATH: usize = 0;
const SIZE: usize = 1;
const MODIFICATION_TIME: usize = 2;
const PARTITION_VALUES: usize = 3;
const OLD_DELETION_VECTOR_STORAGE_TYPE: usize = 4;
const NEW_DELETION_VECTOR_STORAGE_TYPE: usize = 7;
const MODIFICATION_TIME_NAME: &str = "modificationTime";

static DV_MATCHED_FILE_COLUMNS_FOR_VALIDATION: LazyLock<Result<ColumnNamesAndTypes>> =
    LazyLock::new(|| {
        let names = vec![
            column_name!(PATH_NAME),
            column_name!(SIZE_NAME),
            column_name!(MODIFICATION_TIME_NAME),
            column_name!(FILE_CONSTANT_VALUES_NAME, PARTITION_VALUES_NAME),
            column_name!(DELETION_VECTOR_NAME, STORAGE_TYPE_NAME),
            column_name!(DELETION_VECTOR_NAME, PATH_OR_INLINE_DV_NAME),
            column_name!(DELETION_VECTOR_NAME, OFFSET_NAME),
            column_name!(NEW_DELETION_VECTOR_NAME, STORAGE_TYPE_NAME),
            column_name!(NEW_DELETION_VECTOR_NAME, PATH_OR_INLINE_DV_NAME),
            column_name!(NEW_DELETION_VECTOR_NAME, OFFSET_NAME),
        ];
        columns_from_schema(intermediate_dv_schema(), names)
    });

/// Runs required validations for every selected DV-update row. When `staged_file_actions` is
/// provided, also validates file-action (addFile, removeFile) uniqueness.
pub(crate) fn validate_dv_matched_files(
    dv_matched_files: &[FilteredEngineData],
    physical_partition_columns: impl IntoIterator<Item = String>,
    staged_file_actions: Option<&mut FileActionTracker>,
) -> Result<()> {
    StagedDataValidator::staged_dv_matched_file(physical_partition_columns, staged_file_actions)?
        .validate_filtered(dv_matched_files)
}

/// Required validations for every selected DV-update row.
struct RequiredDvMatchedFileVal {
    physical_partition_columns: HashSet<String>,
}

impl Validation for RequiredDvMatchedFileVal {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> Result<()> {
        let path: &str = getters[PATH]
            .get_opt(row, PATH_NAME)?
            .ok_or_else(|| KernelError::missing_data("AddFile is missing required field 'path'"))?;
        require!(
            !path.is_empty(),
            KernelError::generic("AddFile path must not be empty")
        );

        let partition_values = validate_required_field_exist(
            getters[PARTITION_VALUES].get_map(row, PARTITION_VALUES_NAME)?,
            path,
            PARTITION_VALUES_NAME,
        )?;
        validate_partition_keys(path, partition_values, &self.physical_partition_columns)?;

        let size = validate_required_field_exist::<i64>(
            getters[SIZE].get_opt(row, SIZE_NAME)?,
            path,
            SIZE_NAME,
        )?;
        require!(
            size >= 0,
            KernelError::generic(format!(
                "AddFile for '{path}' has negative size {size}; size must be non-negative"
            ))
        );
        validate_required_field_exist::<i64>(
            getters[MODIFICATION_TIME].get_opt(row, MODIFICATION_TIME_NAME)?,
            path,
            MODIFICATION_TIME_NAME,
        )?;
        Ok(())
    }
}

struct RepeatedFileActionValidation<'a> {
    staged_file_actions: &'a mut FileActionTracker,
}

impl Validation for RepeatedFileActionValidation<'_> {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> Result<()> {
        let path: &str = getters[PATH].get(row, PATH_NAME)?;
        let old_dv_id = dv_id_at(getters, OLD_DELETION_VECTOR_STORAGE_TYPE, row)?;
        let new_dv_id = dv_id_at(getters, NEW_DELETION_VECTOR_STORAGE_TYPE, row)?;
        self.staged_file_actions.record_remove(path, old_dv_id)?;
        self.staged_file_actions.record_add(path, new_dv_id)
    }
}

impl<'a> StagedDataValidator<'a> {
    fn staged_dv_matched_file(
        physical_partition_columns: impl IntoIterator<Item = String>,
        staged_file_actions: Option<&'a mut FileActionTracker>,
    ) -> Result<Self> {
        let columns = DV_MATCHED_FILE_COLUMNS_FOR_VALIDATION
            .as_ref()
            .map_err(|error| {
                KernelError::internal_error(format!(
                    "DV validation columns must exist in the intermediate DV schema: {error}"
                ))
            })?;
        let mut validations: Vec<Box<dyn Validation + 'a>> =
            vec![Box::new(RequiredDvMatchedFileVal {
                physical_partition_columns: physical_partition_columns.into_iter().collect(),
            })];
        if let Some(staged_file_actions) = staged_file_actions {
            validations.push(Box::new(RepeatedFileActionValidation {
                staged_file_actions,
            }));
        }
        Ok(StagedDataValidator::new(columns, validations))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use test_utils::{deletion_vector_array, replace_column};

    use super::*;
    use crate::arrow::array::{new_null_array, Array as _, ArrayRef, Int64Array, StructArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::expressions::column_name;
    use crate::scan::scan_row_schema;
    use crate::unit_test_utils::{
        add_files_with_partition_values, assert_result_error_with_message, nullable_add_files,
        set_field_as_null,
    };

    fn make_staged_dv_from_addfile(
        batch: RecordBatch,
        dv_paths: &[&str],
        selection_vector: Vec<bool>,
    ) -> FilteredEngineData {
        let column = |name| {
            batch
                .column(
                    batch
                        .schema()
                        .index_of(name)
                        .expect("field in add-file schema"),
                )
                .clone()
        };
        let schema: ArrowSchema = scan_row_schema()
            .as_ref()
            .try_into_arrow()
            .expect("scan-row schema should convert to Arrow");
        let columns = schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "path" | "size" | "modificationTime" => column(field.name()),
                "fileConstantValues" => {
                    let ArrowDataType::Struct(fields) = field.data_type() else {
                        panic!("fileConstantValues should be a struct");
                    };
                    let values = fields
                        .iter()
                        .map(|field| match field.name().as_str() {
                            "partitionValues" => column(field.name()),
                            _ => new_null_array(field.data_type(), batch.num_rows()),
                        })
                        .collect();
                    Arc::new(StructArray::new(fields.clone(), values, None)) as ArrayRef
                }
                _ => new_null_array(field.data_type(), batch.num_rows()),
            })
            .collect();
        let mut batch = RecordBatch::try_new(Arc::new(schema), columns)
            .expect("staged DV schema and columns should form a valid batch");
        assert_eq!(batch.num_rows(), dv_paths.len());
        let dv_paths = dv_paths.iter().copied().map(Some).collect::<Vec<_>>();
        let new_dv = deletion_vector_array("i", &dv_paths);
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(ArrowField::new(
            NEW_DELETION_VECTOR_NAME,
            new_dv.data_type().clone(),
            true,
        )));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(new_dv));
        batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
            .expect("new deletion-vector column should append to the scan-row batch");
        FilteredEngineData::try_new(Box::new(ArrowEngineData::new(batch)), selection_vector)
            .expect("selection vector length should match staged DV row count")
    }

    #[test]
    fn column_indices_match_schema_order() {
        let columns = DV_MATCHED_FILE_COLUMNS_FOR_VALIDATION
            .as_ref()
            .expect("DV validation columns should exist in the intermediate DV schema");
        let (names, _) = columns.as_ref();
        assert_eq!(names[PATH], column_name!(PATH_NAME));
        assert_eq!(names[SIZE], column_name!(SIZE_NAME));
        assert_eq!(
            names[MODIFICATION_TIME],
            column_name!(MODIFICATION_TIME_NAME)
        );
        assert_eq!(
            names[PARTITION_VALUES],
            column_name!(FILE_CONSTANT_VALUES_NAME, PARTITION_VALUES_NAME)
        );
        assert_eq!(
            names[OLD_DELETION_VECTOR_STORAGE_TYPE],
            column_name!(DELETION_VECTOR_NAME, STORAGE_TYPE_NAME)
        );
        assert_eq!(
            names[OLD_DELETION_VECTOR_STORAGE_TYPE + 1],
            column_name!(DELETION_VECTOR_NAME, PATH_OR_INLINE_DV_NAME)
        );
        assert_eq!(
            names[OLD_DELETION_VECTOR_STORAGE_TYPE + 2],
            column_name!(DELETION_VECTOR_NAME, OFFSET_NAME)
        );
        assert_eq!(
            names[NEW_DELETION_VECTOR_STORAGE_TYPE],
            column_name!(NEW_DELETION_VECTOR_NAME, STORAGE_TYPE_NAME)
        );
        assert_eq!(
            names[NEW_DELETION_VECTOR_STORAGE_TYPE + 1],
            column_name!(NEW_DELETION_VECTOR_NAME, PATH_OR_INLINE_DV_NAME)
        );
        assert_eq!(
            names[NEW_DELETION_VECTOR_STORAGE_TYPE + 2],
            column_name!(NEW_DELETION_VECTOR_NAME, OFFSET_NAME)
        );
        assert_eq!(names.len(), 10);
    }

    #[rstest]
    #[case::selected(&[true, true], true, Some("multiple RemoveFile actions"))]
    #[case::implicitly_selected(&[true], true, Some("multiple RemoveFile actions"))]
    #[case::unselected(&[true, false], true, None)]
    #[case::dedup_disabled(&[true, true], false, None)]
    #[case::dedup_disabled_implicit_tail(&[true], false, None)]
    fn duplicate_dv_update_paths_validate_selected_rows(
        #[case] selection_vector: &[bool],
        #[case] dedup_validation_enabled: bool,
        #[case] expected_error: Option<&str>,
        #[values(false, true)] multiple_batches: bool,
    ) {
        let batches = if multiple_batches {
            (0..2)
                .map(|batch_index| {
                    let row_selection = selection_vector
                        .get(batch_index)
                        .copied()
                        .into_iter()
                        .collect();
                    make_staged_dv_from_addfile(
                        nullable_add_files(&["path"]),
                        &[if batch_index == 0 {
                            "new-dv-0"
                        } else {
                            "new-dv-1"
                        }],
                        row_selection,
                    )
                })
                .collect::<Vec<_>>()
        } else {
            vec![make_staged_dv_from_addfile(
                nullable_add_files(&["path", "path"]),
                &["new-dv-0", "new-dv-1"],
                selection_vector.to_vec(),
            )]
        };
        let mut file_actions = FileActionTracker::default();
        let result = StagedDataValidator::staged_dv_matched_file(
            std::iter::empty(),
            dedup_validation_enabled.then_some(&mut file_actions),
        )
        .expect("DV validator should use the intermediate DV schema")
        .validate_filtered(&batches);

        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.expect("DV update should pass the enabled validations");
        }
    }

    #[rstest]
    #[case::zero_size("size", 0)]
    #[case::negative_modification_time("modificationTime", -1)]
    fn valid_boundary_value_is_accepted(#[case] field: &str, #[case] value: i64) {
        let batch = replace_column(
            &nullable_add_files(&["file-0"]),
            field,
            Arc::new(Int64Array::from(vec![value])),
        );
        let batches = [make_staged_dv_from_addfile(
            batch,
            &["new-dv-0"],
            vec![true],
        )];
        let mut file_actions = FileActionTracker::default();
        StagedDataValidator::staged_dv_matched_file(std::iter::empty(), Some(&mut file_actions))
            .expect("DV validator should use the intermediate DV schema")
            .validate_filtered(&batches)
            .expect("protocol-valid boundary value should be accepted");
    }

    #[rstest]
    #[case::path("path")]
    #[case::partition_values("partitionValues")]
    #[case::size("size")]
    #[case::modification_time("modificationTime")]
    fn missing_required_field_rejected(
        #[case] field: &str,
        #[values(0, 1, 2)] invalid_batch: usize,
    ) {
        const BATCH_COUNT: usize = 3;
        const PATHS: [[&str; 2]; BATCH_COUNT] = [
            ["batch-0-row-0", "batch-0-row-1"],
            ["batch-1-row-0", "batch-1-row-1"],
            ["batch-2-row-0", "batch-2-row-1"],
        ];

        let batches: Vec<_> = (0..BATCH_COUNT)
            .map(|batch_index| {
                let batch = nullable_add_files(&PATHS[batch_index]);
                let batch = if batch_index == invalid_batch {
                    set_field_as_null(&batch, field, 1 /* row */)
                } else {
                    batch
                };
                make_staged_dv_from_addfile(batch, &["new-dv-0", "new-dv-1"], vec![true, true])
            })
            .collect();
        let mut file_actions = FileActionTracker::default();
        assert_result_error_with_message(
            StagedDataValidator::staged_dv_matched_file(
                std::iter::empty(),
                Some(&mut file_actions),
            )
            .expect("DV validator should use the intermediate DV schema")
            .validate_filtered(&batches),
            field,
        );
    }

    #[rstest]
    #[case::selected(&[true, true], Some("partitionValues keys"))]
    #[case::implicitly_selected(&[false], Some("partitionValues keys"))]
    #[case::unselected(&[true, false], None)]
    fn partition_column_mismatch_validates_selected_rows(
        #[case] selection_vector: &[bool],
        #[case] expected_error: Option<&str>,
    ) {
        let batch = add_files_with_partition_values(
            &["file-0", "file-1"],
            &[
                &[("p1", Some("a")), ("p2", Some("b"))],
                &[("p1", Some("a"))],
            ],
        );
        let batches = [make_staged_dv_from_addfile(
            batch,
            &["new-dv-0", "new-dv-1"],
            selection_vector.to_vec(),
        )];
        let mut file_actions = FileActionTracker::default();
        let result = StagedDataValidator::staged_dv_matched_file(
            ["p1".to_string(), "p2".to_string()],
            Some(&mut file_actions),
        )
        .expect("DV validator should use the intermediate DV schema")
        .validate_filtered(&batches);
        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.expect("unselected invalid row should be ignored");
        }
    }
}
