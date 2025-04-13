use std::{
    ops::RangeBounds,
    sync::{Arc, LazyLock},
};

use itertools::process_results;
use url::Url;

use crate::{
    actions::COMMIT_INFO_NAME,
    expressions::column_name,
    log_segment::LogSegment,
    schema::{ColumnNamesAndTypes, DataType, StructField, StructType},
    utils::require,
    DeltaResult, Engine, Error, RowVisitor, Version,
};

pub struct LogTimeConverter {
    start: i64,
    end: Option<i64>,
    table_root: Url,
}

pub(crate) struct RangeResult {
    pub start_version: Version,
    pub start_timestamp: i64,
    pub end_version: Option<Version>,
    pub end_timestamp: Option<i64>,
}

impl LogTimeConverter {
    pub(crate) fn new(table_root: Url, start: i64) -> Self {
        Self {
            table_root,
            start,
            end: None,
        }
    }

    pub(crate) fn new_range(table_root: Url, start: i64, end: i64) -> Self {
        Self {
            table_root,
            start,
            end: Some(end),
        }
    }

    pub(crate) fn convert(self, engine: &dyn Engine) -> DeltaResult<Option<RangeResult>> {
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            self.table_root,
        )?;

        let ict_type = StructField::new("inCommitTimestamp", DataType::LONG, true);
        let schema = Arc::new(StructType::new(vec![StructField::new(
            COMMIT_INFO_NAME,
            StructType::new([ict_type]),
            true,
        )]));

        let fallible_iter =
            log_segment
                .ascending_commit_files
                .into_iter()
                .map(|commit_file| -> DeltaResult<_> {
                    let mut commit_timestamp = commit_file.location.last_modified;
                    let action_iter = engine.json_handler().read_json_files(
                        &[commit_file.location.clone()],
                        schema.clone(),
                        None, // not safe to apply data skipping yet
                    )?;
                    if let Some(batch) = action_iter
                        .take_while(|res| res.as_ref().is_ok_and(|batch| batch.len() > 0))
                        .next()
                    {
                        let batch = batch?;
                        let mut visitor = TimestampVisitor {
                            commit_timestamp: &mut commit_timestamp,
                        };
                        visitor.visit_rows_of(batch.as_ref())?;
                    }
                    Ok((commit_file.version, commit_timestamp))
                });

        process_results(fallible_iter, |processor| {
            let mut iter = processor.filter(|(_, timestamp)| {
                let greater_than_start = *timestamp >= self.start;
                let less_than_end = self.end.map_or(true, |end| *timestamp <= end);
                greater_than_start && less_than_end
            });

            let Some((start_version, start_timestamp)) = iter.next() else {
                return Ok(None);
            };
            println!(
                "start timestamp {:?}: res: {}",
                start_timestamp, start_version
            );
            let (end_version, end_timestamp) = match iter.last() {
                Some((version, timestamp)) => (Some(version), Some(timestamp)),
                None => (None, None),
            };
            Ok(Some(RangeResult {
                start_version,
                start_timestamp,
                end_version,
                end_timestamp,
            }))
        })?
    }
}

struct TimestampVisitor<'a> {
    commit_timestamp: &'a mut i64,
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
