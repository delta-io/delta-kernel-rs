//! Remove-file validations.

use std::sync::LazyLock;

use super::{StagedDataValidator, Validation};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::{lazy_schema_ref, ColumnNamesAndTypes, SchemaRef};
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Column indices, matching the order in [`MANDATORY_REMOVE_FILE_COLUMNS`].
const PATH: usize = 0;

static MANDATORY_REMOVE_FILE_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "path": STRING,
};

static MANDATORY_REMOVE_FILE_COLUMNS: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| MANDATORY_REMOVE_FILE_SCHEMA.leaves(None));

impl StagedDataValidator {
    pub(crate) fn staged_remove_file() -> Self {
        StagedDataValidator::new(
            &MANDATORY_REMOVE_FILE_COLUMNS,
            vec![Box::new(RemoveFileRequiredFields)],
        )
    }
}

struct RemoveFileRequiredFields;

impl Validation for RemoveFileRequiredFields {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("RemoveFile is missing required field 'path'"))?;
        require!(
            !path.is_empty(),
            Error::generic("RemoveFile path must not be empty")
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
        new_null_array, ArrayRef, Int32Array, Int64Array, StringArray, StructArray,
    };
    use crate::arrow::compute::concat_batches;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine_data::FilteredEngineData;
    use crate::expressions::ColumnName;
    use crate::scan::scan_row_schema;
    use crate::unit_test_utils::{assert_result_error_with_message, replace_column};

    #[test]
    fn column_indices_match_schema_order() {
        let (names, _) = MANDATORY_REMOVE_FILE_COLUMNS.as_ref();
        assert_eq!(names[PATH], ColumnName::new(["path"]));
        assert_eq!(names.len(), 1);
    }

    #[rstest]
    #[case::size("size")]
    #[case::modification_time("modificationTime")]
    #[case::stats("stats")]
    #[case::deletion_vector("deletionVector")]
    #[case::file_constant_values("fileConstantValues")]
    fn optional_field_missing_accepted(#[case] field: &str) {
        let batch = nullable_staged_remove_file();
        let field_index = batch.schema().index_of(field).expect("field in schema");
        let null = new_null_array(batch.schema().field(field_index).data_type(), 1);
        let batch = replace_column(&batch, field, null);
        let removes = [all_rows_selected(batch)];
        remove_validator().validate_filtered(&removes).unwrap();
    }

    #[rstest]
    #[case::valid(PathValidationCase {
        value: Some("dummy"),
        selected: true,
        expected_error: None,
    })]
    #[case::missing_selected(PathValidationCase {
        value: None,
        selected: true,
        expected_error: Some("missing required field 'path'"),
    })]
    #[case::missing_unselected(PathValidationCase {
        value: None,
        selected: false,
        expected_error: None,
    })]
    #[case::empty_selected(PathValidationCase {
        value: Some(""),
        selected: true,
        expected_error: Some("path must not be empty"),
    })]
    fn path_values_accepted_or_rejected(
        #[case] case: PathValidationCase,
        #[values(0, 1, 2)] modified_row: usize,
    ) {
        const ROW_COUNT: usize = 3;

        let mut paths = vec![Some("dummy"); ROW_COUNT];
        paths[modified_row] = case.value;
        let batch = replace_column(
            &nullable_staged_remove_files(ROW_COUNT),
            "path",
            Arc::new(StringArray::from(paths)),
        );
        let mut selection_vector = vec![true; ROW_COUNT];
        selection_vector[modified_row] = case.selected;
        let remove =
            FilteredEngineData::try_new(Box::new(ArrowEngineData::new(batch)), selection_vector)
                .unwrap();
        let result = remove_validator().validate_filtered(&[remove]);

        if let Some(expected_error) = case.expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.unwrap();
        }
    }

    #[test]
    fn missing_field_in_later_batch_rejected() {
        let batch = nullable_staged_remove_file();
        let invalid = replace_column(
            &batch,
            "path",
            new_null_array(batch.schema().field(0).data_type(), 1),
        );
        let removes = [
            all_rows_selected(nullable_staged_remove_file()),
            all_rows_selected(invalid),
        ];
        assert_result_error_with_message(
            remove_validator().validate_filtered(&removes),
            "missing required field 'path'",
        );
    }

    struct PathValidationCase {
        value: Option<&'static str>,
        selected: bool,
        expected_error: Option<&'static str>,
    }

    fn nullable_staged_remove_file() -> RecordBatch {
        let arrow_schema: ArrowSchema = scan_row_schema()
            .as_ref()
            .try_into_arrow()
            .expect("scan-row schema should convert to an Arrow schema");
        let columns = arrow_schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "path" => Arc::new(StringArray::from(vec!["dummy"])) as ArrayRef,
                "size" | "modificationTime" => Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                "stats" => Arc::new(StringArray::from(vec!["{}"])) as ArrayRef,
                "deletionVector" | "fileConstantValues" => {
                    let ArrowDataType::Struct(fields) = field.data_type() else {
                        panic!("{} must be a struct", field.name());
                    };
                    let columns = fields
                        .iter()
                        .map(|field| {
                            if field.is_nullable() {
                                return new_null_array(field.data_type(), 1);
                            }
                            match field.data_type() {
                                ArrowDataType::Utf8 => {
                                    Arc::new(StringArray::from(vec!["dummy"])) as ArrayRef
                                }
                                ArrowDataType::Int32 => {
                                    Arc::new(Int32Array::from(vec![1])) as ArrayRef
                                }
                                ArrowDataType::Int64 => {
                                    Arc::new(Int64Array::from(vec![1])) as ArrayRef
                                }
                                data_type => {
                                    panic!("unsupported required nested field type {data_type}")
                                }
                            }
                        })
                        .collect();
                    Arc::new(StructArray::new(fields.clone(), columns, None))
                }
                name => panic!("unexpected scan-row field '{name}'"),
            })
            .collect();
        RecordBatch::try_new(Arc::new(arrow_schema), columns)
            .expect("valid staged remove-file batch")
    }

    fn nullable_staged_remove_files(row_count: usize) -> RecordBatch {
        let batch = nullable_staged_remove_file();
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
