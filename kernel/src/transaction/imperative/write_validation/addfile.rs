//! Add-file validations.

use std::collections::HashSet;
use std::sync::LazyLock;

use super::utils::{validate_partition_keys, validate_required_field_exist};
use super::{FileActionUniquenessValidator, StagedDataValidator, Validation};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::scan::log_replay::CLUSTERING_PROVIDER_NAME;
use crate::schema::{column_name, schema_ref, ColumnNamesAndTypes};
use crate::transaction::{mandatory_add_file_schema, ADD_FILE_CLUSTERING_PROVIDER_FIELD};
use crate::utils::require;
use crate::{EngineData, KernelError, KernelResult};

/// Column indices, matching the order in [`ADD_FILE_COLUMNS_FOR_VALIDATION`].
const PATH: usize = 0;
const PARTITION_VALUES: usize = 1;
const SIZE: usize = 2;
const MODIFICATION_TIME: usize = 3;

static ADD_FILE_COLUMNS_FOR_VALIDATION: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| mandatory_add_file_schema().leaves(None));

static CLUSTERING_PROVIDER_COLUMNS: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| schema_ref! { (&ADD_FILE_CLUSTERING_PROVIDER_FIELD) }.leaves(None));

/// Runs mandatory validations for every staged AddFile row. When `uniqueness_validator` is
/// provided, also validates file-action (addFile, removeFile) uniqueness across `adds` and the
/// actions already in `uniqueness_validator`, and records `adds` into it.
/// For example, this can be used to detect duplicates between addFile and removeFile actions.
pub(crate) fn validate_add_files(
    adds: &[Box<dyn EngineData>],
    physical_partition_columns: impl IntoIterator<Item = String>,
    uniqueness_validator: Option<&mut FileActionUniquenessValidator>,
) -> KernelResult<()> {
    StagedDataValidator::for_add_files(physical_partition_columns, uniqueness_validator)
        .validate_batches(adds)
}

// TODO(#3757): Merge this validation into StagedDataValidator::for_add_files.
/// Rejects batches with non-null clustering providers.
pub(crate) fn validate_no_clustering_providers_set(
    batches: &[Box<dyn EngineData>],
) -> KernelResult<()> {
    let mut validator = StagedDataValidator::new(
        &CLUSTERING_PROVIDER_COLUMNS,
        vec![Box::new(UnclusteredFile)],
    );
    for batch in batches {
        if batch.has_field(&column_name!(CLUSTERING_PROVIDER_NAME)) {
            RowVisitor::visit_rows_of(&mut validator, batch.as_ref())?;
        }
    }
    Ok(())
}

impl<'a> StagedDataValidator<'a> {
    /// Creates a validator that validates every staged add-file row.
    fn for_add_files(
        physical_partition_columns: impl IntoIterator<Item = String>,
        uniqueness_validator: Option<&'a mut FileActionUniquenessValidator>,
    ) -> Self {
        let mut validations: Vec<Box<dyn Validation + 'a>> =
            vec![Box::new(MandatoryAddFileValidation {
                physical_partition_columns: physical_partition_columns.into_iter().collect(),
            })];
        if let Some(uniqueness_validator) = uniqueness_validator {
            validations.push(Box::new(AddFileUniquenessValidation {
                uniqueness_validator,
            }));
        }
        StagedDataValidator::new(&ADD_FILE_COLUMNS_FOR_VALIDATION, validations)
    }
}

/// Mandatory validations for every staged AddFile row.
///
/// Validates required-field existence and that each row's `partitionValues` keys match the table's
/// physical partition columns.
///
/// Required fields: `path`, `partitionValues`, `size`, `modificationTime`, and `dataChange`.
/// Optional fields: `stats`, `tags`, `deletionVector`, `baseRowId`,
/// `defaultRowCommitVersion`, and `clusteringProvider`.
///
/// Kernel guarantees `dataChange` exists by lazily appending it to AddFile actions, so we don't
/// validate its presence here.
/// TODO(#2869): Add intent-based validation for dataChange.
struct MandatoryAddFileValidation {
    physical_partition_columns: HashSet<String>,
}

// TODO(#3112): Share required AddFile validation with DV updates.
impl Validation for MandatoryAddFileValidation {
    fn validate_row<'a>(
        &mut self,
        row: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> KernelResult<()> {
        let path: &str = getters[PATH]
            .get_opt(row, "path")?
            .ok_or_else(|| KernelError::missing_data("AddFile is missing required field 'path'"))?;
        if path.is_empty() {
            return Err(KernelError::generic("AddFile path must not be empty"));
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
            return Err(KernelError::generic(format!(
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

/// Validates staged AddFile uniqueness against file actions (addFile, removeFile) in
/// `uniqueness_validator` and records each staged AddFile into it.
struct AddFileUniquenessValidation<'a> {
    uniqueness_validator: &'a mut FileActionUniquenessValidator,
}

impl Validation for AddFileUniquenessValidation<'_> {
    fn validate_row<'a>(
        &mut self,
        row: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> KernelResult<()> {
        let path: &str = getters[PATH].get(row, "path")?;
        // Plain staged adds carry no deletion vector (the mandatory add-file schema has no
        // deletionVector column), so the dv-id is always None here.
        self.uniqueness_validator.record_add(path, None /* dv_id */)
    }
}

struct UnclusteredFile;

impl Validation for UnclusteredFile {
    fn validate_row<'a>(
        &mut self,
        row: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> KernelResult<()> {
        let provider: Option<&str> = getters[0].get_opt(row, CLUSTERING_PROVIDER_NAME)?;
        require!(
            provider.is_none(),
            KernelError::invalid_transaction_state(
                "clusteringProvider requires the 'clustering' writer feature"
            )
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use test_utils::replace_column;

    use super::*;
    use crate::arrow::array::Int64Array;
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::expressions::column_name;
    use crate::unit_test_utils::{
        add_files_with_partition_values, assert_result_error_with_message, nullable_add_file,
        nullable_add_files, set_field_as_null,
    };

    /// Test helper that provides an empty file-action (addFile, removeFile) uniqueness validator.
    fn validate_add_files_unique(
        physical_partition_columns: &[&str],
        adds: &[Box<dyn EngineData>],
    ) -> KernelResult<()> {
        let mut uniqueness_validator = FileActionUniquenessValidator::default();
        super::validate_add_files(
            adds,
            physical_partition_columns.iter().map(|s| s.to_string()),
            Some(&mut uniqueness_validator),
        )
    }

    fn as_engine_data(batch: RecordBatch) -> [Box<dyn EngineData>; 1] {
        [Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>]
    }

    /// Guards the invariant that the column-index consts match the leaf order of
    /// `ADD_FILE_COLUMNS_FOR_VALIDATION`.
    #[test]
    fn column_indices_match_schema_order() {
        let (names, _) = ADD_FILE_COLUMNS_FOR_VALIDATION.as_ref();
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
    #[case::same_batch(&[&["path", "path"][..]], None)]
    #[case::different_batches(&[&["path"][..], &["path"][..]], None)]
    #[case::different_dv_id(&[&["path"][..]], Some(("path", "existing-dv")))]
    fn duplicate_add_file_paths_rejected(
        #[case] batch_paths: &[&[&str]],
        #[case] existing_add: Option<(&str, &str)>,
    ) {
        let batches = batch_paths
            .iter()
            .map(|paths| nullable_add_files(paths))
            .collect::<Vec<_>>();
        let adds: Vec<Box<dyn EngineData>> = batches
            .into_iter()
            .map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
            .collect();
        let mut uniqueness_validator = FileActionUniquenessValidator::default();
        if let Some((path, dv_id)) = existing_add {
            uniqueness_validator
                .record_add(path, Some(dv_id.to_owned()))
                .expect("first AddFile should be accepted");
        }

        assert_result_error_with_message(
            StagedDataValidator::for_add_files(std::iter::empty(), Some(&mut uniqueness_validator))
                .validate_batches(&adds),
            "multiple AddFile actions",
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
        const PATHS: [[&str; ROW_COUNT]; BATCH_COUNT] = [
            ["batch-0-row-0", "batch-0-row-1", "batch-0-row-2"],
            ["batch-1-row-0", "batch-1-row-1", "batch-1-row-2"],
            ["batch-2-row-0", "batch-2-row-1", "batch-2-row-2"],
        ];

        let adds: Vec<Box<dyn EngineData>> = (0..BATCH_COUNT)
            .map(|batch_index| {
                let batch = nullable_add_files(&PATHS[batch_index]);
                let batch = if batch_index == invalid_batch {
                    set_field_as_null(&batch, field, invalid_row)
                } else {
                    batch
                };
                Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
            })
            .collect();
        assert_result_error_with_message(
            validate_add_files_unique(&[] /* physical_partition_columns */, &adds),
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
        let batch = nullable_add_file(path);
        let batch = replace_column(&batch, "size", Arc::new(Int64Array::from(vec![size])));
        let batch = replace_column(
            &batch,
            "modificationTime",
            Arc::new(Int64Array::from(vec![modification_time])),
        );
        let adds = [Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>];
        let result = validate_add_files_unique(&[] /* physical_partition_columns */, &adds);

        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            result.unwrap();
        }
    }

    #[test]
    fn partition_values_exact_match_ok() {
        let batch = add_files_with_partition_values(
            &["file-0"],
            &[&[("p1", Some("a")), ("p2", Some("b"))]],
        );
        validate_add_files_unique(
            &["p1", "p2"], /* physical_partition_columns */
            &as_engine_data(batch),
        )
        .unwrap();
    }

    #[test]
    fn partition_value_null_still_counts_as_present() {
        let batch =
            add_files_with_partition_values(&["file-0"], &[&[("p1", Some("a")), ("p2", None)]]);
        validate_add_files_unique(
            &["p1", "p2"], /* physical_partition_columns */
            &as_engine_data(batch),
        )
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
        const PATHS: [[&str; ROW_COUNT]; BATCH_COUNT] = [
            ["batch-0-row-0", "batch-0-row-1", "batch-0-row-2"],
            ["batch-1-row-0", "batch-1-row-1", "batch-1-row-2"],
            ["batch-2-row-0", "batch-2-row-1", "batch-2-row-2"],
        ];

        let adds: Vec<Box<dyn EngineData>> = (0..BATCH_COUNT)
            .map(|batch_index| {
                let mut partition_values = [valid_partition_values; ROW_COUNT];
                if batch_index == invalid_batch {
                    partition_values[invalid_row] = invalid_partition_values;
                }
                let batch = add_files_with_partition_values(&PATHS[batch_index], &partition_values);
                Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
            })
            .collect();
        let error = validate_add_files_unique(physical_partition_columns, &adds)
            .expect_err("invalid partition values should be rejected");
        let KernelError::InvalidPartitionValues(message) = error else {
            panic!("expected InvalidPartitionValues, got {error:?}");
        };
        assert!(
            message.contains(expected_error),
            "expected error message to contain {expected_error:?}, got {message:?}"
        );
    }

    #[test]
    fn unpartitioned_table_rejects_partition_values() {
        let batch = add_files_with_partition_values(&["file-0"], &[&[("stray", Some("x"))]]);
        assert_result_error_with_message(
            validate_add_files_unique(
                &[], /* physical_partition_columns */
                &as_engine_data(batch),
            ),
            "partitionValues keys",
        );
    }
}
