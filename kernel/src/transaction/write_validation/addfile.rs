//! Add-file validations.

use std::collections::HashSet;
use std::sync::LazyLock;

use super::utils::{validate_partition_keys, validate_required_field_exist};
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
    pub(crate) fn staged_add_file(
        physical_partition_columns: impl IntoIterator<Item = String>,
    ) -> Self {
        StagedDataValidator::new(
            &MANDATORY_ADD_FILE_COLUMNS,
            vec![Box::new(AddFileRequiredFields {
                physical_partition_columns: physical_partition_columns.into_iter().collect(),
            })],
        )
    }
}

/// Validates required-field existence and that each row's `partitionValues` keys match the table's
/// physical partition columns.
///
/// Required fields: `path`, `partitionValues`, `size`, `modificationTime`, and `dataChange`.
/// Optional fields: `stats`, `tags`, `deletionVector`, `baseRowId`,
/// `defaultRowCommitVersion`, and `clusteringProvider`.
///
/// NOTE: Currently, Kernel doesn't require connectors to set dataChange for staged addFile.
/// TODO(2869): Add intent-based validation for dataChange.
pub(crate) struct AddFileRequiredFields {
    physical_partition_columns: HashSet<String>,
}

impl Validation for AddFileRequiredFields {
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("AddFile is missing required field 'path'"))?;
        if path.is_empty() {
            return Err(Error::generic("AddFile path must not be empty"));
        }

        let partition_values = validate_required_field_exist(
            getters[PARTITION_VALUES].get_map(row, "partitionValues")?,
            path,
            "partitionValues",
        )?;
        validate_partition_keys(path, partition_values, &self.physical_partition_columns)?;
        let size = validate_required_field_exist::<i64>(
            getters[SIZE].get_opt(row, "size")?,
            path,
            "size",
        )?;
        if size < 0 {
            return Err(Error::generic(format!(
                "AddFile for '{path}' has negative size {size}; size must be non-negative"
            )));
        }
        validate_required_field_exist::<i64>(
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
    use crate::arrow::array::{Int64Array, StringArray};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::expressions::column_name;
    use crate::unit_test_utils::{
        add_files_with_partition_values, assert_result_error_with_message, nullable_add_file,
        nullable_add_files, replace_column, set_field_as_null,
    };
    use crate::EngineData;

    fn add_file_validator(physical_partition_columns: &[&str]) -> StagedDataValidator {
        StagedDataValidator::staged_add_file(
            physical_partition_columns.iter().map(|s| s.to_string()),
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
        assert_eq!(names[PATH], column_name!("path"));
        assert_eq!(names[PARTITION_VALUES], column_name!("partitionValues"));
        assert_eq!(names[SIZE], column_name!("size"));
        assert_eq!(names[MODIFICATION_TIME], column_name!("modificationTime"));
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
            add_file_validator(&[] /* physical_partition_columns */).validate(&adds),
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
        let result = add_file_validator(&[] /* physical_partition_columns */).validate(&adds);

        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.unwrap();
        }
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
    #[case::wrong_partition_column_name(
        &[("p1", Some("a")), ("p2", Some("b"))],
        &[("p1", Some("a")), ("wrong", Some("b"))],
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
        #[values(0, 1, 2)] invalid_batch: usize,
        #[values(0, 1, 2)] invalid_row: usize,
    ) {
        const BATCH_COUNT: usize = 3;
        const ROW_COUNT: usize = 3;

        let adds: Vec<Box<dyn EngineData>> = (0..BATCH_COUNT)
            .map(|batch_index| {
                let mut partition_values = [valid_partition_values; ROW_COUNT];
                if batch_index == invalid_batch {
                    partition_values[invalid_row] = invalid_partition_values;
                }
                let batch = add_files_with_partition_values(&partition_values);
                Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
            })
            .collect();
        let error = add_file_validator(physical_partition_columns)
            .validate(&adds)
            .expect_err("invalid partition values should be rejected");
        let Error::InvalidPartitionValues(message) = error else {
            panic!("expected InvalidPartitionValues, got {error:?}");
        };
        assert!(
            message.contains(expected_error),
            "expected error message to contain {expected_error:?}, got {message:?}"
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
