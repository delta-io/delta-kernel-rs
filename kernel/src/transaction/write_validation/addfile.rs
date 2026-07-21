//! Add-file validations.

use std::collections::HashSet;
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
    /// Creates a validator for required add-file values and `partitionValues` keys.
    ///
    /// [`TableConfiguration::physical_partition_columns`]: crate::table_configuration::TableConfiguration::physical_partition_columns
    pub(crate) fn staged_add_file(physical_partition_columns: Vec<String>) -> Self {
        StagedDataValidator::new(
            &MANDATORY_ADD_FILE_COLUMNS,
            vec![
                Box::new(AddFileRequiredFields),
                Box::new(AddFilePartitionColumnsValidation {
                    physical_partition_columns: physical_partition_columns.into_iter().collect(),
                }),
            ],
        )
    }
}

/// Validates the presence of protocol-required fields and basic value constraints for a staged
/// add-file row.
///
/// Required fields must be present, `path` must be non-empty, and `size` must be non-negative.
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

/// Validates that each staged add-file's `partitionValues` keys exactly match the table's partition
/// columns (compared as physical names). Both a missing and an unexpected key are rejected.
pub(crate) struct AddFilePartitionColumnsValidation {
    physical_partition_columns: HashSet<String>,
}

impl Validation for AddFilePartitionColumnsValidation {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("AddFile is missing required field 'path'"))?;

        let actual_keys_vec: Vec<&str> = getters[PARTITION_VALUES]
            .get_map(row, "partitionValues")?
            .map_or_else(Vec::new, |map| map.keys().collect());
        let actual_keys_set: HashSet<&str> = actual_keys_vec.iter().copied().collect();
        let expected_keys_set = &self.physical_partition_columns;
        let keys_match = actual_keys_set.len() == expected_keys_set.len()
            && actual_keys_set
                .iter()
                .all(|key| expected_keys_set.contains(*key));

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
                 physical partition columns are {expected_keys_set:?}"
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
        new_null_array, Array, ArrayRef, Int64Array, MapBuilder, StringArray, StringBuilder,
    };
    use crate::arrow::compute::{concat, concat_batches};
    use crate::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
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

    /// Returns nullable add-file rows with `partitionValues` replaced by the given values.
    /// A `None` value stores a present key with a null value.
    fn add_files_with_partition_values(
        partition_values: &[&[(&str, Option<&str>)]],
    ) -> RecordBatch {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for entries in partition_values {
            for (key, value) in *entries {
                builder.keys().append_value(key);
                match value {
                    Some(v) => builder.values().append_value(v),
                    None => builder.values().append_null(),
                }
            }
            builder
                .append(true)
                .expect("failed to append partition-values map row");
        }
        let map: ArrayRef = Arc::new(builder.finish());

        let batch = nullable_add_files(partition_values.len());
        let schema = batch.schema();
        let index = schema
            .index_of("partitionValues")
            .expect("add-file schema should contain partitionValues");
        let mut fields = schema.fields().to_vec();
        fields[index] = Arc::new(ArrowField::new(
            "partitionValues",
            map.data_type().clone(),
            true,
        ));
        let mut columns = batch.columns().to_vec();
        columns[index] = map;
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
            .expect("failed to rebuild add-file batch with partition values")
    }

    fn add_file_validator(physical_partition_columns: &[&str]) -> StagedDataValidator {
        StagedDataValidator::staged_add_file(
            physical_partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect(),
        )
    }

    fn as_engine_data(batch: RecordBatch) -> [Box<dyn EngineData>; 1] {
        [Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>]
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
        add_file_validator(&[] /* physical_partition_columns */)
            .validate(&as_engine_data(nullable_add_file()))
            .unwrap();
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
        assert_result_error_with_message(
            add_file_validator(&[] /* physical_partition_columns */)
                .validate(&as_engine_data(batch)),
            &format!("missing required field '{field}'"),
        );
    }

    #[test]
    fn empty_path_rejected() {
        let batch = replace_column(
            &nullable_add_file(),
            "path",
            Arc::new(StringArray::from(vec![""])),
        );
        assert_result_error_with_message(
            add_file_validator(&[] /* physical_partition_columns */)
                .validate(&as_engine_data(batch)),
            "path must not be empty",
        );
    }

    #[test]
    fn negative_size_rejected() {
        let batch = replace_column(
            &nullable_add_file(),
            "size",
            Arc::new(Int64Array::from(vec![-1])),
        );
        assert_result_error_with_message(
            add_file_validator(&[] /* physical_partition_columns */)
                .validate(&as_engine_data(batch)),
            "size must be non-negative",
        );
    }

    #[test]
    fn zero_size_and_negative_modification_time_accepted() {
        let batch = replace_column(
            &nullable_add_file(),
            "size",
            Arc::new(Int64Array::from(vec![0])),
        );
        let batch = replace_column(
            &batch,
            "modificationTime",
            Arc::new(Int64Array::from(vec![-1])),
        );
        add_file_validator(&[] /* physical_partition_columns */)
            .validate(&as_engine_data(batch))
            .unwrap();
    }

    #[test]
    fn partition_values_exact_match_ok() {
        let batch = add_files_with_partition_values(&[&[("p1", Some("a")), ("p2", Some("b"))]]);
        add_file_validator(&["p1", "p2"] /* physical_partition_columns */)
            .validate(&as_engine_data(batch))
            .unwrap();
    }

    #[test]
    fn partition_value_null_still_counts_as_present() {
        // A partition column present with a null value satisfies the key-set check.
        let batch = add_files_with_partition_values(&[&[("p1", Some("a")), ("p2", None)]]);
        add_file_validator(&["p1", "p2"] /* physical_partition_columns */)
            .validate(&as_engine_data(batch))
            .unwrap();
    }

    #[rstest]
    #[case::missing_second_partition_column(
        &[("p1", Some("a")), ("p2", Some("b")), ("p3", Some("c"))],
        &[("p1", Some("a")), ("p3", Some("c"))],
        &["p1", "p2", "p3"],
        "partitionValues keys"
    )]
    #[case::missing_third_partition_column(
        &[("p1", Some("a")), ("p2", Some("b")), ("p3", Some("c"))],
        &[("p1", Some("a")), ("p2", Some("b"))],
        &["p1", "p2", "p3"],
        "partitionValues keys"
    )]
    #[case::extra_partition_columns(
        &[("p1", Some("a")), ("p2", Some("b"))],
        &[("p1", Some("a")), ("p2", Some("b")), ("p3", Some("c"))],
        &["p1", "p2"],
        "partitionValues keys"
    )]
    #[case::duplicate_partition_column(
        &[("p1", Some("a")), ("p2", Some("b"))],
        &[("p1", Some("a")), ("p1", Some("b")), ("p2", Some("c"))],
        &["p1", "p2"],
        "duplicate partition column names"
    )]
    fn partition_column_mismatch_rejected(
        #[case] valid_partition_values: &[(&str, Option<&str>)],
        #[case] invalid_partition_values: &[(&str, Option<&str>)],
        #[case] physical_partition_columns: &[&str],
        #[case] expected_error: &str,
        #[values(0, 1, 2)] invalid_row_index: usize,
    ) {
        let mut partition_values = [valid_partition_values; 3];
        partition_values[invalid_row_index] = invalid_partition_values;
        let batch = add_files_with_partition_values(&partition_values);
        assert_result_error_with_message(
            add_file_validator(physical_partition_columns).validate(&as_engine_data(batch)),
            expected_error,
        );
    }

    #[test]
    fn unpartitioned_table_rejects_partition_values() {
        let batch = add_files_with_partition_values(&[&[("stray", Some("x"))]]);
        assert_result_error_with_message(
            add_file_validator(&[] /* physical_partition_columns */)
                .validate(&as_engine_data(batch)),
            "partitionValues keys",
        );
    }
}
