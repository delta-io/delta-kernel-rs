//! This module defines a visitor used to extract the in-commit timestamp from a commit file if it
//! is present.
use std::sync::{Arc, LazyLock};

use crate::actions::COMMIT_INFO_NAME;
use crate::expressions::column_name;
use crate::schema::{ColumnNamesAndTypes, DataType, Schema, StructField, StructType};
use crate::utils::require;
use crate::{DeltaResult, Error, RowVisitor};

/// This visitor extracts the in-commit timestamp (ICT) from a CommitInfo action if present. The
/// [`EngineData`] being visited must have the schema defined in
/// [`InCommitTimestampVisitor::schema`].
///
/// Only one row of the engine data is checked because CommitInfo. This is because in-commit
/// timestamps requires that the CommitInfo containing the ICT be the first action in the log.
#[derive(Default)]
pub(crate) struct InCommitTimestampVisitor {
    pub(crate) in_commit_timestamp: Option<i64>,
}

impl InCommitTimestampVisitor {
    /// Get the schema that the visitor expects the data to have.
    pub(crate) fn schema() -> Arc<Schema> {
        static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
            let ict_type = StructField::new("inCommitTimestamp", DataType::LONG, true);
            Arc::new(StructType::new(vec![StructField::new(
                COMMIT_INFO_NAME,
                StructType::new([ict_type]),
                true,
            )]))
        });
        SCHEMA.clone()
    }
}
impl RowVisitor for InCommitTimestampVisitor {
    fn selected_column_names_and_types(
        &self,
    ) -> (&'static [crate::schema::ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![column_name!("commitInfo.inCommitTimestamp")];
            let types = vec![DataType::LONG];

            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(
        &mut self,
        row_count: usize,
        getters: &[&'a dyn crate::engine_data::GetData<'a>],
    ) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "Wrong number of InCommitTimestampVisitor getters: {}",
                getters.len()
            ))
        );

        // If the batch is empty, return
        if row_count == 0 {
            return Ok(());
        }
        // CommitInfo must be the first action in a commit
        if let Some(in_commit_timestamp) = getters[0].get_long(0, "commitInfo.inCommitTimestamp")? {
            self.in_commit_timestamp = Some(in_commit_timestamp);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow_55::array::StringArray;

    use crate::actions::get_log_schema;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{column_expr, Expression};
    use crate::utils::test_utils::parse_json_batch;
    use crate::{Engine, EngineData, RowVisitor};

    use super::InCommitTimestampVisitor;

    fn add_action() -> &'static str {
        r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#
    }
    fn commit_info_action() -> &'static str {
        r#"{"commitInfo":{"inCommitTimestamp":1677811178585, "timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#
    }

    fn transform_batch(batch: Box<dyn EngineData>) -> Box<dyn EngineData> {
        let engine = SyncEngine::new();
        engine
            .evaluation_handler()
            .new_expression_evaluator(
                get_log_schema().clone(),
                Expression::Struct(vec![Expression::Struct(vec![column_expr!(
                    "commitInfo.inCommitTimestamp"
                )])]),
                InCommitTimestampVisitor::schema().into(),
            )
            .evaluate(batch.as_ref())
            .unwrap()
    }

    // Helper function to reduce duplication in tests
    fn run_timestamp_visitor_test(json_strings: Vec<&str>, expected_timestamp: Option<i64>) {
        let json_strings: StringArray = json_strings.into();
        let batch = parse_json_batch(json_strings);
        let batch = transform_batch(batch);
        let mut visitor = InCommitTimestampVisitor::default();
        visitor.visit_rows_of(batch.as_ref()).unwrap();
        assert_eq!(visitor.in_commit_timestamp, expected_timestamp);
    }

    #[test]
    fn commit_info_not_first() {
        run_timestamp_visitor_test(vec![add_action(), commit_info_action()], None);
    }

    #[test]
    fn commit_info_not_present() {
        run_timestamp_visitor_test(vec![add_action()], None);
    }

    #[test]
    fn commit_info_get() {
        run_timestamp_visitor_test(
            vec![commit_info_action(), add_action()],
            Some(1677811178585), // Retrieved ICT
        );
    }
}
