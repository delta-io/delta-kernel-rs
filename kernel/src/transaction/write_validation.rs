//! Pre-commit validation of delta actions staged on a [`Transaction`].

use std::collections::BTreeMap;
use std::sync::LazyLock;

use itertools::Itertools as _;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{column_name, ColumnName};
use crate::schema::{ColumnNamesAndTypes, DataType, MapType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error};

pub(crate) static ADDFILE_REQUIRED_COLUMNS_TYPES: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| {
        let names = vec![
            column_name!("path"),
            column_name!("partitionValues"),
            column_name!("size"),
            column_name!("modificationTime"),
        ];
        let types = vec![
            DataType::STRING,
            MapType::new(DataType::STRING, DataType::STRING, true).into(),
            DataType::LONG,
            DataType::LONG,
        ];
        (names, types).into()
    });

pub(crate) trait Validation {
    /// The leaf columns this validation reads, in the order [`Validation::validate_row`] expects
    /// them.
    fn columns(&self) -> Vec<ColumnName>;

    /// Validate a single row. `getters[k]` yields this validation's `k`-th declared column in
    /// the order of [`Validation::columns`].
    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()>;
}

pub(crate) struct AddFileRequiredFields;

impl Validation for AddFileRequiredFields {
    fn columns(&self) -> Vec<ColumnName> {
        vec![
            column_name!("path"),
            column_name!("partitionValues"),
            column_name!("size"),
            column_name!("modificationTime"),
        ]
    }

    fn validate_row<'a>(&mut self, row: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let path: &str = getters[0]
            .get_opt(row, "path")?
            .ok_or_else(|| Error::missing_data("Add action is missing required field 'path'"))?;

        require!(
            getters[1].get_map(row, "partitionValues")?.is_some(),
            Error::missing_data(format!(
                "Add action for '{path}' is missing required field 'partitionValues'"
            ))
        );

        let size: Option<i64> = getters[2].get_opt(row, "size")?;
        require!(
            size.is_some(),
            Error::missing_data(format!(
                "Add action for '{path}' is missing required field 'size'"
            ))
        );

        let modification_time: Option<i64> = getters[3].get_opt(row, "modificationTime")?;
        require!(
            modification_time.is_some(),
            Error::missing_data(format!(
                "Add action for '{path}' is missing required field 'modificationTime'"
            ))
        );
        Ok(())
    }
}

pub(crate) struct ActionValidator {
    columns_types: &'static ColumnNamesAndTypes,
    validations: Vec<Box<dyn Validation>>,
    /// `validation_column_indices[i]` maps `validations[i]`'s declared columns to their positions
    /// within `columns_types`.
    ///
    /// # Example
    ///
    /// With `columns_types` = `[path, partitionValues, size, modificationTime]`, the
    /// [`AddFileRequiredFields`] validation declares those same columns in that order, so
    /// `validation_column_indices == [[0, 1, 2, 3]]`. A validation declaring only `[size, path]`
    /// would instead map to `[[2, 0]]`.
    validation_column_indices: Vec<Vec<usize>>,
}

impl ActionValidator {
    /// Build a validator that runs `validations` against the columns in `columns_types`.
    pub(crate) fn new(
        columns_types: &'static ColumnNamesAndTypes,
        validations: Vec<Box<dyn Validation>>,
    ) -> DeltaResult<Self> {
        let (names, _) = columns_types.as_ref();
        // Use BTreeMap instead of HashMap, because ColumnName doesn't implement hash, and the
        // mapping is light part(one-time effort) for the validation.
        let name_to_index: BTreeMap<&ColumnName, usize> = names
            .iter()
            .enumerate()
            .map(|(i, name)| (name, i))
            .collect();
        let indices: Vec<Vec<usize>> = validations
            .iter()
            .map(|validation| {
                validation
                    .columns()
                    .iter()
                    .map(|column| {
                        name_to_index.get(column).copied().ok_or_else(|| {
                            Error::internal_error(format!(
                                "to-be-validated column '{column}' is not in the validator's \
                                 column list: [{}]",
                                names.iter().join(", ")
                            ))
                        })
                    })
                    .try_collect()
            })
            .try_collect()?;
        Ok(Self {
            columns_types,
            validations,
            validation_column_indices: indices,
        })
    }

    pub(crate) fn validate(mut self, adds: &[Box<dyn EngineData>]) -> DeltaResult<()> {
        let (names, _) = self.columns_types.as_ref();
        for batch in adds {
            batch.visit_rows(names, &mut self)?;
        }
        Ok(())
    }
}

impl RowVisitor for ActionValidator {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns_types.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let Self {
            validations,
            validation_column_indices,
            ..
        } = self;

        let per_validation_getters: Vec<Vec<&'a dyn GetData<'a>>> = validation_column_indices
            .iter()
            .map(|indices| indices.iter().map(|&index| getters[index]).collect())
            .collect();
        for row in 0..row_count {
            for (validation, validation_getters) in
                validations.iter_mut().zip(per_validation_getters.iter())
            {
                validation.validate_row(row, validation_getters)?;
            }
        }
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
        ActionValidator::new(
            &ADDFILE_REQUIRED_COLUMNS_TYPES,
            vec![Box::new(AddFileRequiredFields)],
        )
        .unwrap()
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
