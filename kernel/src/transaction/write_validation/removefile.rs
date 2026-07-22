//! Remove-file validations.

use std::sync::LazyLock;

use super::{StagedDataValidator, Validation};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::scan::log_replay::FILE_CONSTANT_VALUES_NAME;
use crate::schema::{lazy_schema_ref, ColumnNamesAndTypes, SchemaRef};
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Column indices, matching the order in [`MANDATORY_REMOVE_FILE_COLUMNS`].
const PATH: usize = 0;
const PARTITION_VALUES: usize = 1;
const SIZE: usize = 2;

static MANDATORY_REMOVE_FILE_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "path": STRING,
    nullable FILE_CONSTANT_VALUES_NAME: {
        nullable "partitionValues": { STRING => nullable STRING },
    },
    nullable "size": LONG,
};

static MANDATORY_REMOVE_FILE_COLUMNS: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| MANDATORY_REMOVE_FILE_SCHEMA.leaves(None));

impl StagedDataValidator {
    /// Creates a validator that validates every selected staged remove-file row.
    pub(crate) fn staged_remove_file() -> Self {
        StagedDataValidator::new(
            &MANDATORY_REMOVE_FILE_COLUMNS,
            vec![Box::new(RemoveFileRequiredFields)],
        )
    }
}

/// Validates fields needed to emit a Remove action with extended file metadata. See "Add File and
/// Remove File" in the Delta protocol specification.
struct RemoveFileRequiredFields;

fn require_remove_file_field<T>(value: Option<T>, path: &str, field: &str) -> DeltaResult<T> {
    value.ok_or_else(|| {
        Error::missing_data(format!(
            "RemoveFile for '{path}' is missing required field '{field}'"
        ))
    })
}

impl Validation for RemoveFileRequiredFields {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("RemoveFile is missing required field 'path'"))?;
        require!(
            !path.is_empty(),
            Error::generic("RemoveFile path must not be empty")
        );

        require_remove_file_field(
            getters[PARTITION_VALUES].get_map(row, "partitionValues")?,
            path,
            "partitionValues",
        )?;
        let size =
            require_remove_file_field::<i64>(getters[SIZE].get_opt(row, "size")?, path, "size")?;
        require!(
            size >= 0,
            Error::generic(format!(
                "RemoveFile for '{path}' has negative size {size}; size must be non-negative"
            ))
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{
        new_empty_array, ArrayRef, Int64Array, MapArray, StringArray, StructArray,
    };
    use crate::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use crate::arrow::compute::concat_batches;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine_data::FilteredEngineData;
    use crate::expressions::ColumnName;
    use crate::utils::test_utils::{
        assert_result_error_with_message, replace_record_batch_column,
        set_record_batch_field_as_null,
    };

    #[test]
    fn column_indices_match_schema_order() {
        let (names, _) = MANDATORY_REMOVE_FILE_COLUMNS.as_ref();
        assert_eq!(names[PATH], ColumnName::new(["path"]));
        assert_eq!(
            names[PARTITION_VALUES],
            ColumnName::new([FILE_CONSTANT_VALUES_NAME, "partitionValues"])
        );
        assert_eq!(names[SIZE], ColumnName::new(["size"]));
        assert_eq!(names.len(), 3);
    }

    #[test]
    fn required_fields_present_ok() {
        let removes = [all_rows_selected(nullable_remove_file())];
        remove_validator().validate_filtered(&removes).unwrap();
    }

    #[rstest]
    #[case::path("path", "path")]
    #[case::partition_values(FILE_CONSTANT_VALUES_NAME, "partitionValues")]
    #[case::size("size", "size")]
    fn required_field_missing_at_any_selected_row_rejected(
        #[case] field: &str,
        #[case] expected_field: &str,
        #[values(0, 1, 2)] invalid_row: usize,
    ) {
        let batch = set_record_batch_field_as_null(&nullable_remove_files(3), field, invalid_row);
        let removes = [all_rows_selected(batch)];
        assert_result_error_with_message(
            remove_validator().validate_filtered(&removes),
            &format!("missing required field '{expected_field}'"),
        );
    }

    #[test]
    fn missing_field_in_unselected_row_accepted() {
        let batch = set_record_batch_field_as_null(&nullable_remove_files(3), "path", 1);
        let removes = [FilteredEngineData::try_new(
            Box::new(ArrowEngineData::new(batch)),
            vec![true, false, true],
        )
        .unwrap()];
        remove_validator().validate_filtered(&removes).unwrap();
    }

    #[test]
    fn missing_field_in_later_batch_rejected() {
        let invalid = set_record_batch_field_as_null(&nullable_remove_file(), "size", 0);
        let removes = [
            all_rows_selected(nullable_remove_file()),
            all_rows_selected(invalid),
        ];
        assert_result_error_with_message(
            remove_validator().validate_filtered(&removes),
            "missing required field 'size'",
        );
    }

    #[test]
    fn empty_path_rejected() {
        let batch = replace_record_batch_column(
            &nullable_remove_file(),
            "path",
            Arc::new(StringArray::from(vec![""])),
        );
        let removes = [all_rows_selected(batch)];
        assert_result_error_with_message(
            remove_validator().validate_filtered(&removes),
            "path must not be empty",
        );
    }

    #[test]
    fn negative_size_rejected() {
        let batch = replace_record_batch_column(
            &nullable_remove_file(),
            "size",
            Arc::new(Int64Array::from(vec![-1])),
        );
        let removes = [all_rows_selected(batch)];
        assert_result_error_with_message(
            remove_validator().validate_filtered(&removes),
            "size must be non-negative",
        );
    }

    #[test]
    fn zero_size_accepted() {
        let batch = replace_record_batch_column(
            &nullable_remove_file(),
            "size",
            Arc::new(Int64Array::from(vec![0])),
        );
        let removes = [all_rows_selected(batch)];
        remove_validator().validate_filtered(&removes).unwrap();
    }

    fn nullable_remove_file() -> RecordBatch {
        let arrow_schema: ArrowSchema = MANDATORY_REMOVE_FILE_SCHEMA
            .as_ref()
            .try_into_arrow()
            .expect("remove-file schema");
        let file_constants = arrow_schema.field(1);
        let ArrowDataType::Struct(fields) = file_constants.data_type() else {
            panic!("fileConstantValues must be a struct");
        };
        let partition_values_field = fields[0].clone();
        let ArrowDataType::Map(entries_field, sorted) = partition_values_field.data_type() else {
            panic!("partitionValues must be a map");
        };
        let entries = new_empty_array(entries_field.data_type())
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("map entries struct")
            .clone();
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0]));
        let partition_values: ArrayRef = Arc::new(
            MapArray::try_new(entries_field.clone(), offsets, entries, None, *sorted)
                .expect("empty partition-values map"),
        );
        let file_constants: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![partition_values],
            None,
        ));

        RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(StringArray::from(vec!["dummy"])),
                file_constants,
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .expect("valid remove-file batch")
    }

    fn nullable_remove_files(row_count: usize) -> RecordBatch {
        let batch = nullable_remove_file();
        concat_batches(&batch.schema(), &vec![batch; row_count])
            .expect("failed to concatenate rows into a multi-row remove-file batch")
    }

    fn all_rows_selected(batch: RecordBatch) -> FilteredEngineData {
        FilteredEngineData::with_all_rows_selected(Box::new(ArrowEngineData::new(batch)))
    }

    fn remove_validator() -> StagedDataValidator {
        StagedDataValidator::staged_remove_file()
    }
}
