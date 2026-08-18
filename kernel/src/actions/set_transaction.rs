pub(crate) use crate::actions::visitors::SetTransactionMap;
use crate::actions::visitors::SetTransactionVisitor;
use crate::actions::{SetTransaction, LOG_TXN_SCHEMA};
#[cfg(test)]
use crate::coroutine::engine::EngineConnector;
use crate::coroutine::{Channel, GeneratorState};
use crate::log_replay::ActionsBatch;
use crate::log_segment::LogSegment;
#[cfg(test)]
use crate::Engine;
use crate::{DeltaResult, RowVisitor as _, Version};

/// Resolves the latest `txn` action per application id via log replay, where the newest action in
/// log order wins.
///
/// Every method returns the resolved `txn` as-is. Applying retention is the caller's
/// responsibility: use [`SetTransaction::non_expired_version`] on the resolved result. Filtering
/// during replay instead would drop an expired newest `txn` and resolve to an older one.
pub(crate) struct SetTransactionScanner {}

impl SetTransactionScanner {
    /// Scan the Delta Log for the latest `txn` action for an application id.
    ///
    /// Note that each call to this function repeats log replay. Thus, if callers are interested
    /// in multiple app ids, use `get_all` (once) instead and probe the map returned.
    #[cfg(test)]
    pub(crate) fn get_one_with_engine(
        log_segment: &LogSegment,
        application_id: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<SetTransaction>> {
        // TODO: Use Arc<LogSegment> to avoid this deep clone.
        let log_segment = log_segment.clone();
        let application_id = application_id.to_owned();
        EngineConnector::run_with(engine, async move |channel| {
            Self::get_one(&log_segment, &application_id, &channel).await
        })
    }

    pub(crate) async fn get_one(
        log_segment: &LogSegment,
        application_id: &str,
        channel: &Channel,
    ) -> DeltaResult<Option<SetTransaction>> {
        let mut transactions =
            scan_application_transactions(log_segment, Some(application_id), channel).await?;
        Ok(transactions.remove(application_id))
    }

    /// Scan the Delta Log for the latest `txn` action of every application id.
    #[allow(unused)]
    #[cfg(test)]
    pub(crate) fn get_all_with_engine(
        log_segment: &LogSegment,
        engine: &dyn Engine,
    ) -> DeltaResult<SetTransactionMap> {
        // TODO: Use Arc<LogSegment> to avoid this deep clone.
        let log_segment = log_segment.clone();
        EngineConnector::run_with(engine, async move |channel| {
            Self::get_all(&log_segment, &channel).await
        })
    }

    /// Scan the Delta Log for the latest `txn` action of every application id.
    #[allow(unused)]
    pub(crate) async fn get_all(
        log_segment: &LogSegment,
        channel: &Channel,
    ) -> DeltaResult<SetTransactionMap> {
        scan_application_transactions(log_segment, None, channel).await
    }

    /// Fetch the latest `txn` action for `application_id`, rooted in an authoritative (`Complete`)
    /// but stale CRC's `base_active` map at `base_version`, scanning ONLY the commits in
    /// `(base_version, log_segment.end_version]`.
    ///
    /// The checkpoint and every commit at/below `base_version` are skipped via
    /// [`LogSegment::segment_after_version`]. When the tail holds a `txn` for `application_id`, its
    /// newest wins; otherwise the result is that id's entry in `base_active`, the value the CRC
    /// recorded at `base_version`.
    pub(crate) async fn get_one_rooted_in_crc(
        log_segment: &LogSegment,
        application_id: &str,
        base_active: &SetTransactionMap,
        base_version: Version,
        channel: &Channel,
    ) -> DeltaResult<Option<SetTransaction>> {
        let tail = Self::get_one(
            &log_segment.segment_after_version(base_version),
            application_id,
            channel,
        )
        .await?;
        Ok(tail.or_else(|| base_active.get(application_id).cloned()))
    }
}

/// Scan the entire log for all application ids but terminate early if a specific application id
/// is provided
// TODO: we could have this track _multiple_ application ids instead of only up to one.
async fn scan_application_transactions(
    log_segment: &LogSegment,
    application_id: Option<&str>,
    channel: &Channel,
) -> DeltaResult<SetTransactionMap> {
    let mut visitor = SetTransactionVisitor::new(application_id.map(|s| s.to_owned()));
    let mut replay = GeneratorState::Start(replay_for_app_ids(log_segment, channel).await?);
    // If a specific id is requested then we can terminate log replay early as soon as it was
    // found. If all ids are requested then we are forced to replay the entire log.
    while let Some(data) = replay.next(channel).await? {
        let txns = data.actions;
        visitor.visit_rows_of(txns.as_ref())?;
        // if a specific id is requested and a transaction was found, then return
        if application_id.is_some() && !visitor.set_transactions.is_empty() {
            break;
        }
    }

    Ok(visitor.set_transactions)
}

// Factored out to facilitate testing
async fn replay_for_app_ids(
    log_segment: &LogSegment,
    channel: &Channel,
) -> DeltaResult<crate::coroutine::Generator<ActionsBatch>> {
    log_segment
        .read_actions(channel, LOG_TXN_SCHEMA.clone())
        .await
}

#[cfg(test)]
fn replay_for_app_ids_with_engine(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
    let connector = EngineConnector::new(engine);
    // TODO: Use Arc<LogSegment> to avoid this deep clone.
    let log_segment = log_segment.clone();
    let actions =
        connector.run(async move |channel| replay_for_app_ids(&log_segment, &channel).await)?;
    connector.iterate_generator(Ok(actions))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use itertools::Itertools;

    use super::*;
    use crate::arrow::array::StringArray;
    use crate::engine::sync::SyncEngine;
    use crate::unit_test_utils::parse_json_batch;
    use crate::Snapshot;

    fn get_latest_transactions(
        path: &str,
        app_id: &str,
    ) -> (SetTransactionMap, Option<SetTransaction>) {
        let path = std::fs::canonicalize(PathBuf::from(path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let log_segment = snapshot.log_segment();

        (
            SetTransactionScanner::get_all_with_engine(log_segment, &engine).unwrap(),
            SetTransactionScanner::get_one_with_engine(log_segment, app_id, &engine).unwrap(),
        )
    }

    #[test]
    fn test_txn() {
        let (txns, txn) = get_latest_transactions("./tests/data/basic_partitioned/", "test");
        assert!(txn.is_none());
        assert_eq!(txns.len(), 0);

        let (txns, txn) = get_latest_transactions("./tests/data/app-txn-no-checkpoint/", "my-app");
        assert!(txn.is_some());
        assert_eq!(txns.len(), 2);
        assert_eq!(txns.get("my-app"), txn.as_ref());
        assert_eq!(
            txns.get("my-app2"),
            Some(SetTransaction {
                app_id: "my-app2".to_owned(),
                version: 2,
                last_updated: None
            })
            .as_ref()
        );

        let (txns, txn) = get_latest_transactions("./tests/data/app-txn-checkpoint/", "my-app");
        assert!(txn.is_some());
        assert_eq!(txns.len(), 2);
        assert_eq!(txns.get("my-app"), txn.as_ref());
        assert_eq!(
            txns.get("my-app2"),
            Some(SetTransaction {
                app_id: "my-app2".to_owned(),
                version: 2,
                last_updated: None
            })
            .as_ref()
        );
    }

    #[test]
    fn test_replay_for_app_ids() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let log_segment = snapshot.log_segment();

        // The checkpoint has five parts, each containing one action. There are two app ids.
        let data: Vec<_> = replay_for_app_ids_with_engine(log_segment, &engine)
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(data.len(), 2);
    }

    #[test]
    fn test_get_all_returns_every_txn_unfiltered() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-with-last-updated/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let log_segment = snapshot.log_segment();

        // The scanner returns every app_id regardless of `lastUpdated`; callers apply retention.
        let all_txns = SetTransactionScanner::get_all_with_engine(log_segment, &engine).unwrap();
        assert_eq!(all_txns.len(), 4);
    }

    #[test]
    fn test_visitor_keeps_newest_by_log_order_regardless_of_last_updated() {
        // Batches are visited in reverse log order, so the first row wins per app_id.
        let json_strings: StringArray = vec![
            r#"{"txn":{"appId":"app","version":2,"lastUpdated":100}}"#,
            r#"{"txn":{"appId":"app","version":1,"lastUpdated":999}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut visitor = SetTransactionVisitor::new(None);
        visitor.visit_rows_of(batch.as_ref()).unwrap();

        assert_eq!(visitor.set_transactions.len(), 1);
        assert_eq!(visitor.set_transactions.get("app").unwrap().version, 2);
    }
}
