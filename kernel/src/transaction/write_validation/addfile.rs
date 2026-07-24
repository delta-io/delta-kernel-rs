//! Add-file validations.

use std::sync::LazyLock;

use super::{StagedDataValidator, Validation};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::ColumnNamesAndTypes;
use crate::transaction::mandatory_add_file_schema;
use crate::{DeltaResult, Error};

/// Column indices, matching the order in [`MANDATORY_ADD_FILE_COLUMNS`].
const PATH: usize = 0;
const PARTITION_VALUES: usize = 1;
const SIZE: usize = 2;
const MODIFICATION_TIME: usize = 3;

static MANDATORY_ADD_FILE_COLUMNS: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| mandatory_add_file_schema().leaves(None));

impl StagedDataValidator {
    /// Creates a validator that validates every staged add-file row.
    pub(crate) fn staged_add_file() -> Self {
        StagedDataValidator::new(
            &MANDATORY_ADD_FILE_COLUMNS,
            vec![Box::new(AddFileRequiredFields)],
        )
    }
}

/// Validates that every staged add-file row has its protocol-required fields (`path`,
/// `partitionValues`, `size`, `modificationTime`) present.
pub(crate) struct AddFileRequiredFields;

fn require_add_file_field<T>(value: Option<T>, path: &str, field: &str) -> DeltaResult<T> {
    value.ok_or_else(|| {
        Error::missing_data(format!(
            "AddFile for '{path}' is missing required field '{field}'"
        ))
    })
}

impl Validation for AddFileRequiredFields {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("AddFile is missing required field 'path'"))?;
        if path.is_empty() {
            return Err(Error::generic("AddFile path must not be empty"));
        }

        require_add_file_field(
            getters[PARTITION_VALUES].get_map(row, "partitionValues")?,
            path,
            "partitionValues",
        )?;
        let size =
            require_add_file_field::<i64>(getters[SIZE].get_opt(row, "size")?, path, "size")?;
        if size < 0 {
            return Err(Error::generic(format!(
                "AddFile for '{path}' has negative size {size}; size must be non-negative"
            )));
        }
        require_add_file_field::<i64>(
            getters[MODIFICATION_TIME].get_opt(row, "modificationTime")?,
            path,
            "modificationTime",
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{new_null_array, Array, ArrayRef, Int64Array, StringArray};
    use crate::arrow::compute::{concat, concat_batches};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::expressions::ColumnName;
    use crate::utils::test_utils::{assert_result_error_with_message, create_valid_add_file_batch};
    use crate::EngineData;

    fn nullable_add_file() -> RecordBatch {
        create_valid_add_file_batch(true /* all_nullable */)
    }

    fn nullable_add_files(row_count: usize) -> RecordBatch {
        let batch = nullable_add_file();
        concat_batches(&batch.schema(), &vec![batch; row_count])
            .expect("failed to concatenate rows into a multi-row add-file batch")
    }

    /// Return `batch` with `field` set to null at `row`.
    fn set_field_as_null(batch: &RecordBatch, field: &str, row: usize) -> RecordBatch {
        let schema = batch.schema();
        let index = schema.index_of(field).expect("field in schema");
        let mut columns = batch.columns().to_vec();
        let column = &columns[index];
        let null = new_null_array(schema.field(index).data_type(), 1);
        let slices = [
            column.slice(0, row),
            null,
            column.slice(row + 1, batch.num_rows() - row - 1),
        ];
        let arrays: Vec<&dyn Array> = slices.iter().map(|array| array.as_ref()).collect();
        columns[index] = concat(&arrays)
            .expect("failed to replace the selected add-file field with a null value");
        RecordBatch::try_new(schema, columns)
            .expect("failed to rebuild add-file batch after replacing a field value with null")
    }

    fn replace_column(batch: &RecordBatch, field: &str, column: ArrayRef) -> RecordBatch {
        let schema = batch.schema();
        let index = schema.index_of(field).expect("field not found in schema");
        let mut columns = batch.columns().to_vec();
        columns[index] = column;
        RecordBatch::try_new(schema, columns)
            .expect("failed to rebuild add-file batch after replacing a column")
    }

    fn add_validator() -> StagedDataValidator {
        StagedDataValidator::staged_add_file()
    }

    /// Guards the invariant that the column-index consts match the leaf order of
    /// `MANDATORY_ADD_FILE_COLUMNS`.
    #[test]
    fn column_indices_match_schema_order() {
        let (names, _) = MANDATORY_ADD_FILE_COLUMNS.as_ref();
        assert_eq!(names[PATH], ColumnName::new(["path"]));
        assert_eq!(
            names[PARTITION_VALUES],
            ColumnName::new(["partitionValues"])
        );
        assert_eq!(names[SIZE], ColumnName::new(["size"]));
        assert_eq!(
            names[MODIFICATION_TIME],
            ColumnName::new(["modificationTime"])
        );
        assert_eq!(
            names.len(),
            [PATH, PARTITION_VALUES, SIZE, MODIFICATION_TIME].len()
        );
    }

    #[rstest]
    #[case::path("path")]
    #[case::partition_values("partitionValues")]
    #[case::size("size")]
    #[case::modification_time("modificationTime")]
    fn required_field_missing_at_any_batch_and_row_rejected(
        #[case] field: &str,
        #[values(0, 1, 2)] invalid_batch: usize,
        #[values(0, 1, 2)] invalid_row: usize,
    ) {
        const BATCH_COUNT: usize = 3;
        const ROW_COUNT: usize = 3;

        let adds: Vec<Box<dyn EngineData>> = (0..BATCH_COUNT)
            .map(|batch_index| {
                let batch = nullable_add_files(ROW_COUNT);
                let batch = if batch_index == invalid_batch {
                    set_field_as_null(&batch, field, invalid_row)
                } else {
                    batch
                };
                Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
            })
            .collect();
        assert_result_error_with_message(
            add_validator().validate(&adds),
            &format!("missing required field '{field}'"),
        );
    }

    #[rstest]
    #[case::valid(
        "dummy" /* path */,
        1 /* size */,
        1 /* modification_time */,
        None /* expected_error */,
    )]
    #[case::empty_path(
        "" /* path */,
        1 /* size */,
        1 /* modification_time */,
        Some("path must not be empty") /* expected_error */,
    )]
    #[case::negative_size(
        "dummy" /* path */,
        -1 /* size */,
        1 /* modification_time */,
        Some("size must be non-negative") /* expected_error */,
    )]
    #[case::zero_size(
        "dummy" /* path */,
        0 /* size */,
        1 /* modification_time */,
        None /* expected_error */,
    )]
    #[case::negative_modification_time(
        "dummy" /* path */,
        1 /* size */,
        -1 /* modification_time */,
        None /* expected_error */,
    )]
    fn add_file_values_accepted_or_rejected(
        #[case] path: &str,
        #[case] size: i64,
        #[case] modification_time: i64,
        #[case] expected_error: Option<&str>,
    ) {
        let batch = replace_column(
            &nullable_add_file(),
            "path",
            Arc::new(StringArray::from(vec![path])),
        );
        let batch = replace_column(&batch, "size", Arc::new(Int64Array::from(vec![size])));
        let batch = replace_column(
            &batch,
            "modificationTime",
            Arc::new(Int64Array::from(vec![modification_time])),
        );
        let adds = [Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>];
        let result = add_validator().validate(&adds);

        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.unwrap();
        }
    }
}
