use std::cmp::Ordering;
use std::sync::{Arc, LazyLock};

use url::Url;

use crate::actions::COMMIT_INFO_NAME;
use crate::expressions::column_name;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::schema::{ColumnNamesAndTypes, DataType, Schema, StructField, StructType};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, RowVisitor, Version};

pub enum Bound {
    LeastUpper,
    GreatestLower,
}
fn commit_file_to_timestamp(
    engine: &dyn Engine,
    commit_file: &ParsedLogPath,
) -> DeltaResult<(Version, i64)> {
    // Take the first non-empty engine data batch
    let mut action_iter = engine
        .json_handler()
        .read_json_files(
            &[commit_file.location.clone()],
            TimestampVisitor::schema(),
            None,
        )?
        .take_while(|res| res.as_ref().is_ok_and(|batch| batch.len() > 0));

    let mut commit_timestamp = commit_file.location.last_modified;
    if let Some(batch) = action_iter.next() {
        let batch = batch?;
        let mut visitor = TimestampVisitor {
            commit_timestamp: &mut commit_timestamp,
        };
        visitor.visit_rows_of(batch.as_ref())?;
    }
    Ok((commit_file.version, commit_timestamp))
}

pub(crate) fn convert_timestamp_to_version(
    engine: &dyn Engine,
    table_root: Url,
    timestamp: i64,
    bound: Bound,
) -> DeltaResult<Version> {
    let log_segment =
        LogSegment::for_timestamp_conversion(engine.storage_handler().as_ref(), table_root)?;
    if log_segment.ascending_commit_files.is_empty() {
        // FIXME: Create error type
        return Err(Error::generic("No commit files found, cannot time travel"));
    }
    let commits = log_segment.ascending_commit_files;
    let mut lo = 0;
    let mut hi = commits.len() - 1;
    let mut out = None;
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let (mid_version, mid_timestamp) = commit_file_to_timestamp(engine, &commits[mid])?;
        match timestamp.cmp(&mid_timestamp) {
            Ordering::Equal => return Ok(mid_version),
            Ordering::Less => {
                if let Bound::GreatestLower = bound {
                    out = Some((mid_version, mid_timestamp));
                }
                hi = mid - 1;
            }
            Ordering::Greater => {
                if let Bound::LeastUpper = bound {
                    out = Some((mid_version, mid_timestamp));
                }
                lo = mid + 1;
            }
        }
    }
    match out {
        Some((out_version, out_timestamp)) => {
            println!("For timestamp query {timestamp}, found commit version {out_version}, {out_timestamp}");
            Ok(out_version)
        }
        None => Err(Error::generic("Could not find matiching timestamp")),
    }
}

struct TimestampVisitor<'a> {
    commit_timestamp: &'a mut i64,
}

impl TimestampVisitor<'_> {
    fn schema() -> Arc<Schema> {
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
