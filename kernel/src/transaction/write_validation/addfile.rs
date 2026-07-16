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

        require_add_file_field(
            getters[PARTITION_VALUES].get_map(row, "partitionValues")?,
            path,
            "partitionValues",
        )?;
        require_add_file_field::<i64>(getters[SIZE].get_opt(row, "size")?, path, "size")?;
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
    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{new_null_array, Array};
    use crate::arrow::compute::{concat, concat_batches};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::expressions::ColumnName;
    use crate::utils::test_utils::{assert_result_error_with_message, valid_add_file_batch};
    use crate::EngineData;

    fn nullable_add_file() -> RecordBatch {
        valid_add_file_batch(true /* all_nullable */)
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
        assert_eq!(names.len(), 4);
    }

    #[test]
    fn required_fields_present_ok() {
        let adds = [Box::new(ArrowEngineData::new(nullable_add_file())) as Box<dyn EngineData>];
        add_validator().validate(&adds).unwrap();
    }

    #[rstest]
    #[case::path("path")]
    #[case::partition_values("partitionValues")]
    #[case::size("size")]
    #[case::modification_time("modificationTime")]
    fn required_field_missing_at_any_row_rejected(
        #[case] field: &str,
        #[values(0, 1, 2)] invalid_row: usize,
    ) {
        let batch = set_field_as_null(&nullable_add_files(3), field, invalid_row);
        let adds = [Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>];
        assert_result_error_with_message(
            add_validator().validate(&adds),
            &format!("missing required field '{field}'"),
        );
    }
}
