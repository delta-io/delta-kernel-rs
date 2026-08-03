//! Deletion-vector update validations.

use std::collections::HashSet;
use std::sync::LazyLock;

use super::utils::{validate_partition_keys, validate_required_field};
use super::{StagedDataValidator, Validation};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::scan::scan_row_schema;
use crate::schema::{ColumnName, ColumnNamesAndTypes};
use crate::utils::require;
use crate::{DeltaResult, Error};

const PATH: usize = 0;
const SIZE: usize = 1;
const MODIFICATION_TIME: usize = 2;
const PARTITION_VALUES: usize = 3;

static DV_MATCHED_FILE_COLUMNS: LazyLock<DeltaResult<ColumnNamesAndTypes>> = LazyLock::new(|| {
    let names = vec![
        ColumnName::new(["path"]),
        ColumnName::new(["size"]),
        ColumnName::new(["modificationTime"]),
        ColumnName::new(["fileConstantValues", "partitionValues"]),
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

struct DvMatchedFileRequiredFields {
    physical_partition_columns: HashSet<String>,
}

impl Validation for DvMatchedFileRequiredFields {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("AddFile is missing required field 'path'"))?;
        require!(
            !path.is_empty(),
            Error::generic("AddFile path must not be empty")
        );

        let partition_values = validate_required_field(
            getters[PARTITION_VALUES].get_map(row, "partitionValues")?,
            path,
            "partitionValues",
        )?;
        validate_partition_keys(path, partition_values, &self.physical_partition_columns)?;

        let size =
            validate_required_field::<i64>(getters[SIZE].get_opt(row, "size")?, path, "size")?;
        require!(
            size >= 0,
            Error::generic(format!(
                "AddFile for '{path}' has negative size {size}; size must be non-negative"
            ))
        );
        validate_required_field::<i64>(
            getters[MODIFICATION_TIME].get_opt(row, "modificationTime")?,
            path,
            "modificationTime",
        )?;
        Ok(())
    }
}

impl StagedDataValidator {
    /// Creates a validator for selected rows staged for deletion-vector updates.
    ///
    /// Errors if the required columns are absent from the scan-row schema.
    pub(crate) fn staged_dv_matched_file(
        physical_partition_columns: impl IntoIterator<Item = String>,
    ) -> DeltaResult<Self> {
        let columns = DV_MATCHED_FILE_COLUMNS.as_ref().map_err(|error| {
            Error::internal_error(format!(
                "DV validation columns must exist in the scan-row schema: {error}"
            ))
        })?;
        Ok(StagedDataValidator::new(
            columns,
            vec![Box::new(DvMatchedFileRequiredFields {
                physical_partition_columns: physical_partition_columns.into_iter().collect(),
            })],
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{new_null_array, ArrayRef, Int64Array, StructArray};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine_data::FilteredEngineData;
    use crate::expressions::ColumnName;
    use crate::unit_test_utils::{
        add_files_with_partition_values, assert_result_error_with_message, nullable_add_files,
        replace_column, set_field_as_null,
    };

    fn make_staged_dv_from_addfile(
        batch: RecordBatch,
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
        let batch = RecordBatch::try_new(Arc::new(schema), columns)
            .expect("staged DV schema and columns should form a valid batch");
        FilteredEngineData::try_new(Box::new(ArrowEngineData::new(batch)), selection_vector)
            .expect("selection vector length should match staged DV row count")
    }

    #[test]
    fn column_indices_match_schema_order() {
        let columns = DV_MATCHED_FILE_COLUMNS
            .as_ref()
            .expect("DV validation columns should exist in the scan-row schema");
        let (names, _) = columns.as_ref();
        assert_eq!(names[PATH], ColumnName::new(["path"]));
        assert_eq!(names[SIZE], ColumnName::new(["size"]));
        assert_eq!(
            names[MODIFICATION_TIME],
            ColumnName::new(["modificationTime"])
        );
        assert_eq!(
            names[PARTITION_VALUES],
            ColumnName::new(["fileConstantValues", "partitionValues"])
        );
    }

    #[rstest]
    #[case::zero_size("size", 0)]
    #[case::negative_modification_time("modificationTime", -1)]
    fn valid_boundary_value_is_accepted(#[case] field: &str, #[case] value: i64) {
        let batch = replace_column(
            &nullable_add_files(1 /* row_count */),
            field,
            Arc::new(Int64Array::from(vec![value])),
        );
        let batches = [make_staged_dv_from_addfile(batch, vec![true])];
        StagedDataValidator::staged_dv_matched_file(std::iter::empty())
            .expect("DV validator should use the scan-row schema")
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

        let batches: Vec<_> = (0..BATCH_COUNT)
            .map(|batch_index| {
                let batch = nullable_add_files(2 /* row_count */);
                let batch = if batch_index == invalid_batch {
                    set_field_as_null(&batch, field, 1 /* row */)
                } else {
                    batch
                };
                make_staged_dv_from_addfile(batch, vec![true, true])
            })
            .collect();
        assert_result_error_with_message(
            StagedDataValidator::staged_dv_matched_file(std::iter::empty())
                .expect("DV validator should use the scan-row schema")
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
        let batch = add_files_with_partition_values(&[
            &[("p1", Some("a")), ("p2", Some("b"))],
            &[("p1", Some("a"))],
        ]);
        let batches = [make_staged_dv_from_addfile(
            batch,
            selection_vector.to_vec(),
        )];
        let result =
            StagedDataValidator::staged_dv_matched_file(["p1".to_string(), "p2".to_string()])
                .expect("DV validator should use the scan-row schema")
                .validate_filtered(&batches);
        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.expect("unselected invalid row should be ignored");
        }
    }
}
