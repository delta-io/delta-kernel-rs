use crate::internal_mod;

use error::LogHistoryError;
use std::fmt::Debug;
use std::sync::Arc;

use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::snapshot::Snapshot;
use crate::Error as DeltaError;
use crate::RowVisitor;
use crate::{DeltaResult, Engine};
use timestamp_visitor::InCommitTimestampVisitor;

pub(crate) mod error;
internal_mod!(pub(crate) mod search);
mod timestamp_visitor;

type Timestamp = i64;

/// The [`LogHistoryManager`] performs timestamp queries over the Delta Log, translating
/// between timestamps and Delta versions.
///
/// # Usage
///
/// Use this manager to:
/// - Convert timestamps or timestamp ranges into Delta versions or version ranges
/// - Perform time travel queries using [`Table::snapshot`]
/// - Execute timestamp-based change data feed queries using [`Table::table_changes`]
///
/// The [`LogHistoryManager`] works with tables regardless of whether they have In-Commit
/// Timestamps enabled.
///
/// # Limitations
///
/// Once created, the [`LogHistoryManager`] does not automatically update with newer versions
/// of the table. All timestamp queries are limited to the state captured in the [`Snapshot`]
/// provided during construction.
#[allow(unused)]
#[derive(Debug)]
pub(crate) struct LogHistoryManager {
    log_segment: LogSegment,
    snapshot: Arc<Snapshot>,
}

impl LogHistoryManager {
    /// Creates a new [`LogHistoryManager`] instance that can query for timestamps up to the
    /// provided snapshot's version.
    ///
    /// # Parameters
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `snapshot`: A [`Snapshot`] of the table. The latest version in the snapshot will be the
    ///               greatest time that this [`LogHistoryManager`] will be able to resolve
    ///               timestamps for.
    /// - `limit`: Optional maximum number of versions to track in the history manager's state.
    ///            When specified, the earliest queryable version is `snapshot.version - limit`.
    ///            This parameter allows trading memory usage for historical reach.
    /// ```
    #[allow(unused)]
    pub(crate) fn try_new(
        engine: &dyn Engine,
        snapshot: Arc<Snapshot>,
        limit: Option<usize>,
    ) -> DeltaResult<Self> {
        #[allow(unused)]
        let log_segment = LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            limit,
        )?;
        debug_assert!(
            !log_segment.ascending_commit_files.is_empty(),
            "LogSegment should ensure that a segment is non-empty"
        );
        debug_assert!(log_segment.end_version == snapshot.log_segment().end_version);
        Ok(Self {
            log_segment,
            snapshot,
        })
    }

    /// Gets the timestamp for the `commit_file`. If `read_ict` is false ,this returns the file's
    /// modification timestamp. If `read_ict` is true, this reads the file's In-commit timestamp.
    #[allow(unused)]
    fn commit_file_to_timestamp(
        &self,
        engine: &dyn Engine,
        commit_file: &ParsedLogPath,
        read_ict: bool,
    ) -> Result<Timestamp, LogHistoryError> {
        let commit_timestamp = if read_ict {
            Self::read_in_commit_timestamp(engine, commit_file)?
        } else {
            // By default, the timestamp of a commit is its modification time
            commit_file.location.last_modified
        };

        Ok(commit_timestamp)
    }

    /// Reads the in-commit timestamp for the given `commit_file`.
    ///
    /// This returns a [`LogHistoryError::FailedToReadTimestampForCommit`] if this encounters an
    /// error while reading the file or visiting the rows.
    ///
    /// This returns a [`LogHistoryError::InCommitTimestampNotFoundError`] if the in-commit timestamp
    /// is not present in the commit file, or if the CommitInfo is not the first action in the
    /// commit.
    #[allow(unused)]
    fn read_in_commit_timestamp(
        engine: &dyn Engine,
        commit_file: &ParsedLogPath,
    ) -> Result<Timestamp, LogHistoryError> {
        debug_assert!(commit_file.is_commit(), "File should be a commit");
        let wrap_err = |error: DeltaError| LogHistoryError::FailedToReadTimestampForCommit {
            version: commit_file.version,
            error: Box::new(error),
        };

        // Get an iterator over the actions in the commit file
        let mut action_iter = engine
            .json_handler()
            .read_json_files(
                &[commit_file.location.clone()],
                InCommitTimestampVisitor::schema(),
                None,
            )
            .map_err(wrap_err)?;

        // Take the first non-empty engine data batch
        let batch = action_iter
            .map(|res| res.map_err(wrap_err))
            .next()
            .transpose()?;

        // Visit the rows and get the in-commit timestamp if present
        let in_commit_timestamp_opt = batch
            .map(|batch| -> Result<Option<i64>, LogHistoryError> {
                let mut visitor = InCommitTimestampVisitor::default();
                visitor.visit_rows_of(batch.as_ref()).map_err(wrap_err)?;
                Ok(visitor.in_commit_timestamp)
            })
            .transpose()?
            .flatten();

        in_commit_timestamp_opt.ok_or_else(|| LogHistoryError::InCommitTimestampNotFoundError {
            version: commit_file.version,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::time::{Duration, SystemTime};

    use crate::actions::{CommitInfo, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::snapshot::Snapshot;
    use crate::table_features::WriterFeature;
    use crate::utils::test_utils::{Action, LocalMockTable};
    use crate::Version;
    use test_utils::delta_path_for_version;
    use url::Url;

    use super::{LogHistoryError, LogHistoryManager, Timestamp};

    // Helper to set the file modification timestamp of a file
    fn set_mod_time(mock_table: &LocalMockTable, commit_version: Version, timestamp: Timestamp) {
        let file_name = delta_path_for_version(commit_version, "json")
            .filename()
            .unwrap()
            .to_string();
        let path = mock_table.table_root().join("_delta_log/").join(file_name);
        let file = File::open(path).unwrap();
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp.try_into().unwrap());
        file.set_modified(time).unwrap();
    }

    async fn mock_table() -> LocalMockTable {
        let mut mock_table = LocalMockTable::new();

        // 0: Has file modification timestamp 50
        mock_table.commit([Action::Metadata(Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        }),
            Action::Protocol(Protocol::try_new(3, 7, Some(Vec::<String>::new()), Some(vec![WriterFeature::InCommitTimestamp])).unwrap())
        ]).await;
        set_mod_time(&mock_table, 0, 50);

        // 1: Has file modification timestamp 150
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 1, 150);

        // 2: Has file modification timestamp 250
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 2, 250);

        // 3: Has in-commit timestamp 300, file modification timestamp 350
        mock_table.commit([
            Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(300),
                ..Default::default()
            }),
            Action::Metadata(Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            configuration: HashMap::from_iter([(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            ),
                ("delta.inCommitTimestampEnablementVersion".to_string(), "3".to_string()),
                ("delta.inCommitTimestampEnablementTimestamp".to_string(), "300".to_string())]),
            ..Default::default()
        }),
            Action::Protocol(Protocol::try_new(3, 7, Some(Vec::<String>::new()), Some(vec![WriterFeature::InCommitTimestamp])).unwrap())
        ]).await;
        set_mod_time(&mock_table, 3, 350);

        // 4: Has in-commit timestamp 400, file modification timestamp 450
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(400),
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 4, 450);

        mock_table
    }

    #[tokio::test]
    async fn reading_in_commit_timestamp() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let manager = LogHistoryManager::try_new(&engine, snapshot.into(), None).unwrap();
        let commits = manager.log_segment.ascending_commit_files;

        // File that has no In-commit timestamps
        let mut res = LogHistoryManager::read_in_commit_timestamp(&engine, &commits[0]);
        assert!(
            matches!(
                res,
                Err(LogHistoryError::InCommitTimestampNotFoundError { version: 0 })
            ),
            "{res:?} failed"
        );

        // File that doesn't exist
        let mut fake_path = commits[0].clone();
        fake_path.location.location = Url::from_file_path("/phony/path").unwrap();
        res = LogHistoryManager::read_in_commit_timestamp(&engine, &fake_path);
        assert!(
            matches!(
                res,
                Err(LogHistoryError::FailedToReadTimestampForCommit {
                    version: 0,
                    error: _
                })
            ),
            "{res:?} failed"
        );

        // Files with In-commit timestamps
        res = LogHistoryManager::read_in_commit_timestamp(&engine, &commits[3]);
        assert!(matches!(res, Ok(300)), "{res:?}");
        res = LogHistoryManager::read_in_commit_timestamp(&engine, &commits[4]);
        assert!(matches!(res, Ok(400)), "{res:?}");
    }

    #[tokio::test]
    async fn file_modification_conversion() {
        let mock_table = mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let manager = LogHistoryManager::try_new(&engine, snapshot.into(), None).unwrap();
        let commits = &manager.log_segment.ascending_commit_files;

        // Read the file modification timestamps
        let ts = manager.commit_file_to_timestamp(&engine, &commits[0], false);
        assert!(matches!(ts, Ok(50)));

        let ts = manager.commit_file_to_timestamp(&engine, &commits[1], false);
        assert!(matches!(ts, Ok(150)));

        let ts = manager.commit_file_to_timestamp(&engine, &commits[2], false);
        assert!(matches!(ts, Ok(250)));

        // Read the in-commit timestamps
        let ts = manager.commit_file_to_timestamp(&engine, &commits[3], true);
        assert!(matches!(ts, Ok(300)));
        let ts = manager.commit_file_to_timestamp(&engine, &commits[4], true);
        assert!(matches!(ts, Ok(400)));
    }
}
