use std::collections::HashSet;
use std::sync::LazyLock;

use serde::Deserialize;

use crate::engine_data::{
    FilteredEngineData, FilteredRowVisitor, GetData, RowIndexIterator, RowVisitor,
    TypedGetData as _,
};
use crate::expressions::{column_name, ColumnName};
use crate::schema::{ColumnNamesAndTypes, DataType};
use crate::transaction::stats_verifier::NUM_RECORDS_TYPES;
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error};

const ADD_PATH: usize = 0;
const ADD_NUM_RECORDS: usize = 1;

const REMOVE_PATH: usize = 0;
const REMOVE_STATS: usize = 1;
const REMOVE_DV_STORAGE_TYPE: usize = 2;
const REMOVE_DV_PATH: usize = 3;
const REMOVE_DV_OFFSET: usize = 4;
const REMOVE_DV_SIZE_IN_BYTES: usize = 5;
const REMOVE_DV_CARDINALITY: usize = 6;
const REMOVE_BASE_ROW_ID: usize = 7;
const REMOVE_DEFAULT_ROW_COMMIT_VERSION: usize = 8;

static REMOVE_COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    (
        vec![
            column_name!("path"),
            column_name!("stats"),
            column_name!("deletionVector.storageType"),
            column_name!("deletionVector.pathOrInlineDv"),
            column_name!("deletionVector.offset"),
            column_name!("deletionVector.sizeInBytes"),
            column_name!("deletionVector.cardinality"),
            column_name!("fileConstantValues.baseRowId"),
            column_name!("fileConstantValues.defaultRowCommitVersion"),
        ],
        vec![
            DataType::STRING,
            DataType::STRING,
            DataType::STRING,
            DataType::STRING,
            DataType::INTEGER,
            DataType::INTEGER,
            DataType::LONG,
            DataType::LONG,
            DataType::LONG,
        ],
    )
        .into()
});

#[derive(Default)]
struct AddRewriteVisitor {
    paths: HashSet<String>,
    num_records: i64,
}

impl RowVisitor for AddRewriteVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        NUM_RECORDS_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 2,
            Error::internal_error("wrong getter count for row-tracking rewrite Adds")
        );
        for row in 0..row_count {
            let path: &str = getters[ADD_PATH]
                .get_opt(row, "path")?
                .ok_or_else(|| Error::missing_data("row-tracking rewrite Add is missing path"))?;
            require!(
                self.paths.insert(path.to_string()),
                Error::generic(format!(
                    "row-tracking rewrite contains duplicate replacement path '{path}'"
                ))
            );
            let num_records: i64 = getters[ADD_NUM_RECORDS]
                .get_opt(row, "stats.numRecords")?
                .ok_or_else(|| {
                    Error::missing_data(format!(
                        "row-tracking rewrite Add '{path}' requires exact stats.numRecords"
                    ))
                })?;
            require!(
                num_records >= 0,
                Error::generic(format!(
                    "row-tracking rewrite Add '{path}' has negative stats.numRecords"
                ))
            );
            self.num_records = self
                .num_records
                .checked_add(num_records)
                .ok_or_else(|| Error::generic("row count overflow in row-tracking rewrite Adds"))?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourceStats {
    num_records: i64,
}

#[derive(Default)]
struct RemoveRewriteVisitor {
    paths: HashSet<String>,
    num_records: i64,
}

impl FilteredRowVisitor for RemoveRewriteVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        REMOVE_COLUMNS.as_ref()
    }

    fn visit_filtered<'a>(
        &mut self,
        getters: &[&'a dyn GetData<'a>],
        rows: RowIndexIterator<'_>,
    ) -> DeltaResult<()> {
        require!(
            getters.len() == 9,
            Error::internal_error("wrong getter count for row-tracking rewrite Removes")
        );
        for row in rows {
            let path: &str = getters[REMOVE_PATH].get_opt(row, "path")?.ok_or_else(|| {
                Error::missing_data("row-tracking rewrite Remove is missing path")
            })?;
            require!(
                self.paths.insert(path.to_string()),
                Error::generic(format!(
                    "row-tracking rewrite contains duplicate source path '{path}'"
                ))
            );
            let stats: &str = getters[REMOVE_STATS]
                .get_opt(row, "stats")?
                .ok_or_else(|| {
                    Error::missing_data(format!(
                        "row-tracking rewrite source '{path}' requires exact stats.numRecords"
                    ))
                })?;
            let SourceStats { num_records } = serde_json::from_str(stats).map_err(|error| {
                Error::generic(format!(
                    "invalid statistics for row-tracking rewrite source '{path}': {error}"
                ))
            })?;
            require!(
                num_records >= 0,
                Error::generic(format!(
                    "row-tracking rewrite source '{path}' has negative stats.numRecords"
                ))
            );
            self.num_records = self.num_records.checked_add(num_records).ok_or_else(|| {
                Error::generic("row count overflow in row-tracking rewrite Removes")
            })?;

            let dv_storage_type: Option<&str> =
                getters[REMOVE_DV_STORAGE_TYPE].get_opt(row, "deletionVector.storageType")?;
            let dv_path: Option<&str> =
                getters[REMOVE_DV_PATH].get_opt(row, "deletionVector.pathOrInlineDv")?;
            let dv_offset: Option<i32> =
                getters[REMOVE_DV_OFFSET].get_opt(row, "deletionVector.offset")?;
            let dv_size_in_bytes: Option<i32> =
                getters[REMOVE_DV_SIZE_IN_BYTES].get_opt(row, "deletionVector.sizeInBytes")?;
            let dv_cardinality: Option<i64> =
                getters[REMOVE_DV_CARDINALITY].get_opt(row, "deletionVector.cardinality")?;
            let has_deletion_vector = dv_storage_type.is_some()
                || dv_path.is_some()
                || dv_offset.is_some()
                || dv_size_in_bytes.is_some()
                || dv_cardinality.is_some();
            require!(
                !has_deletion_vector,
                Error::unsupported(format!(
                    "row-tracking rewrite source '{path}' has a deletion vector"
                ))
            );
            let _: i64 = getters[REMOVE_BASE_ROW_ID]
                .get_opt(row, "fileConstantValues.baseRowId")?
                .ok_or_else(|| {
                    Error::missing_data(format!(
                        "row-tracking rewrite source '{path}' is missing baseRowId"
                    ))
                })?;
            let _: i64 = getters[REMOVE_DEFAULT_ROW_COMMIT_VERSION]
                .get_opt(row, "fileConstantValues.defaultRowCommitVersion")?
                .ok_or_else(|| {
                    Error::missing_data(format!(
                        "row-tracking rewrite source '{path}' is missing defaultRowCommitVersion"
                    ))
                })?;
        }
        Ok(())
    }
}

pub(crate) fn validate_row_tracking_rewrite(
    adds: &[Box<dyn EngineData>],
    removes: &[FilteredEngineData],
) -> DeltaResult<()> {
    let mut add_visitor = AddRewriteVisitor::default();
    for add in adds {
        add_visitor.visit_rows_of(add.as_ref())?;
    }
    let mut remove_visitor = RemoveRewriteVisitor::default();
    for remove in removes {
        remove_visitor.visit_rows_of(remove)?;
    }

    require!(
        !add_visitor.paths.is_empty(),
        Error::generic("acknowledged row-tracking rewrite requires replacement Adds")
    );
    require!(
        !remove_visitor.paths.is_empty(),
        Error::generic("acknowledged row-tracking rewrite requires source Removes")
    );
    require!(
        add_visitor.num_records == remove_visitor.num_records,
        Error::generic(format!(
            "row-tracking rewrite row counts differ: replacement Adds contain {} rows but source \
             Removes contain {} rows",
            add_visitor.num_records, remove_visitor.num_records
        ))
    );
    if let Some(path) = add_visitor.paths.intersection(&remove_visitor.paths).next() {
        return Err(Error::unsupported(format!(
            "row-tracking rewrite cannot replace source path '{path}' at the same path"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray, StructArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;

    fn add(path: &str, num_records: Option<i64>) -> Box<dyn EngineData> {
        let num_records_field = Arc::new(ArrowField::new("numRecords", ArrowDataType::Int64, true));
        let stats = Arc::new(StructArray::new(
            vec![num_records_field].into(),
            vec![Arc::new(Int64Array::from(vec![num_records]))],
            None,
        )) as ArrayRef;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("stats", stats.data_type().clone(), true),
        ]));
        Box::new(ArrowEngineData::new(
            RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec![path])) as ArrayRef, stats],
            )
            .unwrap(),
        ))
    }

    fn remove(
        path: &str,
        num_records: i64,
        has_deletion_vector: bool,
        base_row_id: Option<i64>,
        default_row_commit_version: Option<i64>,
    ) -> FilteredEngineData {
        remove_with_stats(
            path,
            &format!("{{\"numRecords\":{num_records}}}"),
            has_deletion_vector,
            base_row_id,
            default_row_commit_version,
        )
    }

    fn remove_with_stats(
        path: &str,
        stats: &str,
        has_deletion_vector: bool,
        base_row_id: Option<i64>,
        default_row_commit_version: Option<i64>,
    ) -> FilteredEngineData {
        let dv_fields = vec![
            Arc::new(ArrowField::new("storageType", ArrowDataType::Utf8, true)),
            Arc::new(ArrowField::new("pathOrInlineDv", ArrowDataType::Utf8, true)),
            Arc::new(ArrowField::new("offset", ArrowDataType::Int32, true)),
            Arc::new(ArrowField::new("sizeInBytes", ArrowDataType::Int32, true)),
            Arc::new(ArrowField::new("cardinality", ArrowDataType::Int64, true)),
        ];
        let deletion_vector = Arc::new(StructArray::new(
            dv_fields.into(),
            vec![
                Arc::new(StringArray::from(vec![has_deletion_vector.then_some("u")])) as ArrayRef,
                Arc::new(StringArray::from(
                    vec![has_deletion_vector.then_some("abc")],
                )) as ArrayRef,
                Arc::new(Int32Array::from(vec![has_deletion_vector.then_some(0)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![has_deletion_vector.then_some(3)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![has_deletion_vector.then_some(1)])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let constants_fields = vec![
            Arc::new(ArrowField::new("baseRowId", ArrowDataType::Int64, true)),
            Arc::new(ArrowField::new(
                "defaultRowCommitVersion",
                ArrowDataType::Int64,
                true,
            )),
        ];
        let file_constants = Arc::new(StructArray::new(
            constants_fields.into(),
            vec![
                Arc::new(Int64Array::from(vec![base_row_id])) as ArrayRef,
                Arc::new(Int64Array::from(vec![default_row_commit_version])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("stats", ArrowDataType::Utf8, false),
            ArrowField::new("deletionVector", deletion_vector.data_type().clone(), true),
            ArrowField::new(
                "fileConstantValues",
                file_constants.data_type().clone(),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![path])) as ArrayRef,
                Arc::new(StringArray::from(vec![stats])) as ArrayRef,
                deletion_vector,
                file_constants,
            ],
        )
        .unwrap();
        FilteredEngineData::with_all_rows_selected(Box::new(ArrowEngineData::new(batch)))
    }

    #[rstest]
    #[case::missing_output_count(
        None,
        false,
        Some(0),
        Some(1),
        3,
        "requires exact stats.numRecords"
    )]
    #[case::negative_output_count(
        Some(-1),
        false,
        Some(0),
        Some(1),
        -1,
        "negative stats.numRecords"
    )]
    #[case::source_deletion_vector(Some(3), true, Some(0), Some(1), 3, "has a deletion vector")]
    #[case::missing_source_base_row_id(Some(3), false, None, Some(1), 3, "missing baseRowId")]
    #[case::missing_source_commit_version(
        Some(3),
        false,
        Some(0),
        None,
        3,
        "missing defaultRowCommitVersion"
    )]
    #[case::different_row_counts(Some(2), false, Some(0), Some(1), 3, "row counts differ")]
    #[case::negative_source_count(
        Some(0),
        false,
        Some(0),
        Some(1),
        -1,
        "negative stats.numRecords"
    )]
    fn rejects_malformed_rewrite(
        #[case] output_count: Option<i64>,
        #[case] has_deletion_vector: bool,
        #[case] base_row_id: Option<i64>,
        #[case] default_row_commit_version: Option<i64>,
        #[case] source_count: i64,
        #[case] expected_error: &str,
    ) {
        let result = validate_row_tracking_rewrite(
            &[add("replacement.parquet", output_count)],
            &[remove(
                "source.parquet",
                source_count,
                has_deletion_vector,
                base_row_id,
                default_row_commit_version,
            )],
        );
        let error = result.expect_err("malformed rewrite must fail");
        assert!(
            error.to_string().contains(expected_error),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_same_path_rewrite() {
        let result = validate_row_tracking_rewrite(
            &[add("same.parquet", Some(3))],
            &[remove("same.parquet", 3, false, Some(0), Some(1))],
        );
        assert!(result
            .expect_err("same-path rewrite must fail")
            .to_string()
            .contains("same path"));
    }

    #[test]
    fn rejects_empty_rewrite_sides() {
        let no_adds = validate_row_tracking_rewrite(
            &[],
            &[remove("source.parquet", 3, false, Some(0), Some(1))],
        );
        assert!(no_adds
            .expect_err("rewrite without Adds must fail")
            .to_string()
            .contains("replacement Adds"));

        let no_removes = validate_row_tracking_rewrite(&[add("replacement.parquet", Some(3))], &[]);
        assert!(no_removes
            .expect_err("rewrite without Removes must fail")
            .to_string()
            .contains("source Removes"));
    }

    #[test]
    fn rejects_duplicate_rewrite_paths() {
        let duplicate_adds = validate_row_tracking_rewrite(
            &[
                add("replacement.parquet", Some(1)),
                add("replacement.parquet", Some(2)),
            ],
            &[remove("source.parquet", 3, false, Some(0), Some(1))],
        );
        assert!(duplicate_adds
            .expect_err("duplicate replacement path must fail")
            .to_string()
            .contains("duplicate replacement"));

        let duplicate_removes = validate_row_tracking_rewrite(
            &[add("replacement.parquet", Some(2))],
            &[
                remove("source.parquet", 1, false, Some(0), Some(1)),
                remove("source.parquet", 1, false, Some(0), Some(1)),
            ],
        );
        assert!(duplicate_removes
            .expect_err("duplicate source path must fail")
            .to_string()
            .contains("duplicate source"));
    }

    #[test]
    fn rejects_invalid_source_statistics() {
        let result = validate_row_tracking_rewrite(
            &[add("replacement.parquet", Some(3))],
            &[remove_with_stats(
                "source.parquet",
                "not-json",
                false,
                Some(0),
                Some(1),
            )],
        );
        assert!(result
            .expect_err("invalid source statistics must fail")
            .to_string()
            .contains("invalid statistics"));
    }

    #[test]
    fn rejects_rewrite_row_count_overflow() {
        let add_overflow = validate_row_tracking_rewrite(
            &[
                add("replacement-a.parquet", Some(i64::MAX)),
                add("replacement-b.parquet", Some(1)),
            ],
            &[remove("source.parquet", 0, false, Some(0), Some(1))],
        );
        assert!(add_overflow
            .expect_err("replacement row count overflow must fail")
            .to_string()
            .contains("overflow in row-tracking rewrite Adds"));

        let remove_overflow = validate_row_tracking_rewrite(
            &[add("replacement.parquet", Some(0))],
            &[
                remove("source-a.parquet", i64::MAX, false, Some(0), Some(1)),
                remove("source-b.parquet", 1, false, Some(1), Some(1)),
            ],
        );
        assert!(remove_overflow
            .expect_err("source row count overflow must fail")
            .to_string()
            .contains("overflow in row-tracking rewrite Removes"));
    }
}
