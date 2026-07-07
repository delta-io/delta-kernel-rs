//! Add-file validations.

use std::sync::LazyLock;

use super::{StagedDataValidator, Validation};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::ColumnNamesAndTypes;
use crate::transaction::mandatory_add_file_schema;
use crate::utils::require;
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
/// `partitionValues`, `size`, `modificationTime`) present and non-null.
pub(crate) struct AddFileRequiredFields;

impl Validation for AddFileRequiredFields {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("Add action is missing required field 'path'"))?;

        require!(
            getters[PARTITION_VALUES]
                .get_map(row, "partitionValues")?
                .is_some(),
            Error::missing_data(format!(
                "Add action for '{path}' is missing required field 'partitionValues'"
            ))
        );

        let size: Option<i64> = getters[SIZE].get_opt(row, "size")?;
        require!(
            size.is_some(),
            Error::missing_data(format!(
                "Add action for '{path}' is missing required field 'size'"
            ))
        );

        let modification_time: Option<i64> =
            getters[MODIFICATION_TIME].get_opt(row, "modificationTime")?;
        require!(
            modification_time.is_some(),
            Error::missing_data(format!(
                "Add action for '{path}' is missing required field 'modificationTime'"
            ))
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::arrow::array::new_null_array;
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::expressions::ColumnName;
    use crate::utils::test_utils::{assert_result_error_with_message, valid_add_file_batch};
    use crate::EngineData;

    fn nullable_add_file() -> RecordBatch {
        valid_add_file_batch(true /* all_nullable */)
    }

    /// Return `batch` with `field`'s column replaced by an all-null array of the same type.
    fn set_field_as_null(batch: &RecordBatch, field: &str) -> RecordBatch {
        let schema = batch.schema();
        let index = schema.index_of(field).expect("field in schema");
        let mut columns = batch.columns().to_vec();
        columns[index] = new_null_array(schema.field(index).data_type(), batch.num_rows());
        RecordBatch::try_new(schema, columns).expect("record batch with nulled field")
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
    fn present_required_fields_ok() {
        let adds = [Box::new(ArrowEngineData::new(nullable_add_file())) as Box<dyn EngineData>];
        add_validator().validate(&adds).unwrap();
    }

    #[rstest]
    #[case::path("path")]
    #[case::partition_values("partitionValues")]
    #[case::size("size")]
    #[case::modification_time("modificationTime")]
    fn null_required_field_rejected(#[case] field: &str) {
        let batch = set_field_as_null(&nullable_add_file(), field);
        let adds = [Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>];
        assert_result_error_with_message(
            add_validator().validate(&adds),
            &format!("missing required field '{field}'"),
        );
    }
}
