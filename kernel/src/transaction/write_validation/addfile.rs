//! Add-file validations.

use std::sync::LazyLock;

use super::{ActionValidator, Validation};
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

impl ActionValidator {
    pub(crate) fn staged_add_file() -> Self {
        ActionValidator::new(
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
    use crate::utils::test_utils::valid_add_file_batch;
    use crate::{DeltaResult, EngineData};

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

    fn add_validator() -> ActionValidator {
        ActionValidator::staged_add_file()
    }

    fn assert_err_contains(result: DeltaResult<()>, needle: &str) {
        let err = result.expect_err("expected validation error").to_string();
        assert!(
            err.contains(needle),
            "error {err:?} should contain {needle:?}"
        );
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
        assert_err_contains(
            add_validator().validate(&adds),
            &format!("missing required field '{field}'"),
        );
    }
}
