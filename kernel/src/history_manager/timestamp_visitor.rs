use std::sync::{Arc, LazyLock};

use crate::actions::COMMIT_INFO_NAME;
use crate::expressions::column_name;
use crate::schema::{ColumnNamesAndTypes, DataType, Schema, StructField, StructType};
use crate::utils::require;
use crate::{DeltaResult, Error, RowVisitor};

pub(crate) struct TimestampVisitor<'a> {
    pub commit_timestamp: &'a mut i64,
}

impl TimestampVisitor<'_> {
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
impl RowVisitor for TimestampVisitor<'_> {
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
                "Wrong number of TimestampVisitor getters: {}",
                getters.len()
            ))
        );

        // If the batch is empty, return
        if row_count == 0 {
            return Ok(());
        }
        // CommitInfo must be the first action in a commit
        if let Some(in_commit_timestamp) = getters[0].get_long(0, "commitInfo.inCommitTimestamp")? {
            *self.commit_timestamp = in_commit_timestamp;
        }
        Ok(())
    }
}
