//! Reconciled JSON fixtures for testing compaction readers without enabling compaction writes.
//!
//! Reconciliation preserves JSON fields independently of kernel's writer and replay code.

use std::collections::{BTreeMap, HashSet};

use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt};
use delta_kernel::{KernelError, KernelResult};
use serde_json::{Value, json};
use url::Url;

use crate::{resolve_table_path, try_parse_uri};

/// Writes a log compaction from the original commits in `start..=end` and returns its URL.
///
/// `table_root` must be a directory URL backed by `store`, and `start < end`.
/// Keeps the latest action per key without expiring removals, deleted domains, or transaction
/// markers, so they still suppress state preceding the range. Omits `commitInfo` and `cdc`.
///
/// Use for conventional Delta logs; this helper does not normalize adaptiveMetadata paths.
///
/// # Errors
/// Returns an error for an invalid range, unavailable or malformed commits, duplicate action keys
/// within a commit, unsupported action shapes, or storage failures.
pub async fn write_compaction_from_commits(
    table_root: impl AsRef<str>,
    store: &DynObjectStore,
    start: u64,
    end: u64,
) -> KernelResult<Url> {
    if start >= end {
        return Err(KernelError::generic("invalid compaction fixture range"));
    }
    let table_root = try_parse_uri(table_root)?;
    let mut winners = BTreeMap::new();
    for version in start..=end {
        let path = resolve_table_path(
            table_root.as_str(),
            &Path::from(format!("_delta_log/{version:020}.json")),
        )?;
        let bytes = store.get(&path).await?.bytes().await?;
        let mut seen = HashSet::new();
        for action in serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>() {
            let action = action?;
            let Some(key) = action_key(&action)? else {
                continue;
            };
            if !seen.insert(key.clone()) {
                return Err(KernelError::generic(
                    "duplicate action key in fixture commit",
                ));
            }
            winners.insert(key, action);
        }
    }
    let content = winners
        .values()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let relative_path = format!("_delta_log/{start:020}.{end:020}.compacted.json");
    let path = resolve_table_path(table_root.as_str(), &Path::from(relative_path.as_str()))?;
    store.put(&path, content.into()).await?;
    Ok(table_root.join(&relative_path)?)
}

fn action_key(action: &Value) -> KernelResult<Option<(String, String)>> {
    let invalid = || KernelError::generic("unsupported compaction fixture action");
    let object = action
        .as_object()
        .filter(|o| o.len() == 1)
        .ok_or_else(invalid)?;
    let (kind, fields) = object.iter().next().ok_or_else(invalid)?;
    let key = match kind.as_str() {
        "add" | "remove" => {
            let path = fields["path"].as_str().ok_or_else(invalid)?;
            let dv = &fields["deletionVector"];
            if !dv.is_null() && !matches!(dv["storageType"].as_str(), Some("i" | "u" | "p")) {
                return Err(invalid());
            }
            (
                "file",
                json!([path, dv["storageType"], dv["pathOrInlineDv"], dv["offset"]]).to_string(),
            )
        }
        "protocol" | "metaData" => (kind.as_str(), String::new()),
        "txn" => (
            "txn",
            fields["appId"].as_str().ok_or_else(invalid)?.to_owned(),
        ),
        "domainMetadata" => (
            "domainMetadata",
            fields["domain"].as_str().ok_or_else(invalid)?.to_owned(),
        ),
        "commitInfo" | "cdc" => return Ok(None),
        _ => return Err(invalid()),
    };
    Ok(Some((key.0.to_owned(), key.1)))
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::object_store::memory::InMemory;
    use rstest::rstest;

    use super::*;
    use crate::{add_commit, assert_result_error_with_message};

    #[cfg(unix)]
    #[tokio::test]
    async fn fixture_returns_canonical_url_for_symlinked_table_root() {
        let directory = tempfile::tempdir().unwrap();
        let actual = directory.path().join("actual");
        let alias = directory.path().join("alias");
        std::fs::create_dir(&actual).unwrap();
        std::os::unix::fs::symlink(&actual, &alias).unwrap();
        let root = Url::from_directory_path(alias).unwrap();
        let store = LocalFileSystem::new();
        add_commit(root.as_str(), &store, 0, r#"{"protocol":{}}"#.into())
            .await
            .unwrap();
        add_commit(root.as_str(), &store, 1, r#"{"metaData":{}}"#.into())
            .await
            .unwrap();

        let path = write_compaction_from_commits(root.as_str(), &store, 0, 1)
            .await
            .unwrap();
        let canonical_root = Url::from_directory_path(actual.canonicalize().unwrap()).unwrap();
        assert_eq!(
            path,
            canonical_root
                .join("_delta_log/00000000000000000000.00000000000000000001.compacted.json")
                .unwrap()
        );
        assert!(
            store
                .head(&Path::from_url_path(path.path()).unwrap())
                .await
                .unwrap()
                .size
                > 0
        );
    }

    #[tokio::test]
    async fn fixture_preserves_winners_and_distinct_dv_offsets() -> KernelResult<()> {
        let store = InMemory::new();
        let dv = json!({"storageType": "u", "pathOrInlineDv": "dv", "offset": 1,
            "sizeInBytes": 10, "cardinality": 1});
        let mut newer_dv = dv.clone();
        newer_dv["cardinality"] = json!(2);
        newer_dv["sizeInBytes"] = json!(20);
        let mut other_dv = dv.clone();
        other_dv["offset"] = json!(2);
        let expected = vec![
            json!({"remove": {"path": "a"}}),
            json!({"add": {"path": "b", "deletionVector": newer_dv}}),
            json!({"remove": {"path": "b", "deletionVector": other_dv, "deletionTimestamp": 1}}),
            json!({"domainMetadata": {"domain": "d", "removed": true}}),
            json!({"txn": {"appId": "app", "version": 2, "lastUpdated": 1}}),
        ];
        add_commit(
            "memory:///",
            &store,
            0,
            [
                json!({"add": {"path": "a"}}),
                json!({"add": {"path": "b", "deletionVector": dv}}),
                json!({"domainMetadata": {"domain": "d", "removed": false}}),
                json!({"txn": {"appId": "app", "version": 1}}),
            ]
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
        )
        .await?;
        let mut actions = expected.clone();
        actions.push(json!({"commitInfo": {"timestamp": 1}}));
        actions.push(json!({"cdc": {"path": "changes"}}));
        add_commit(
            "memory:///",
            &store,
            1,
            actions
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .await?;
        let path = write_compaction_from_commits("memory:///", &store, 0, 1).await?;
        let bytes = store
            .get(&Path::from_url_path(path.path())?)
            .await?
            .bytes()
            .await?;
        let actual = serde_json::Deserializer::from_slice(&bytes)
            .into_iter::<Value>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(actual.len(), expected.len());
        for action in expected {
            assert!(actual.contains(&action), "missing {action}");
        }
        Ok(())
    }

    #[rstest]
    #[case::duplicate("{\"add\":{\"path\":\"a\"}}\n{\"remove\":{\"path\":\"a\"}}")]
    #[case::unsupported("{\"checkpoint\":{}}")]
    #[case::malformed("{")]
    #[case::non_object("[]")]
    #[case::empty_object("{}")]
    #[case::multiple_actions("{\"protocol\":{},\"metaData\":{}}")]
    #[case::missing_path("{\"add\":{}}")]
    #[case::invalid_dv("{\"add\":{\"path\":\"a\",\"deletionVector\":{\"storageType\":\"bad\"}}}")]
    #[case::missing_app_id("{\"txn\":{}}")]
    #[case::missing_domain("{\"domainMetadata\":{}}")]
    #[tokio::test]
    async fn fixture_rejects_ambiguous_or_unsupported_actions(#[case] actions: &str) {
        let store = InMemory::new();
        add_commit("memory:///", &store, 0, actions.into())
            .await
            .unwrap();
        add_commit("memory:///", &store, 1, String::new())
            .await
            .unwrap();
        assert!(
            write_compaction_from_commits("memory:///", &store, 0, 1)
                .await
                .is_err()
        );
    }
    #[rstest]
    #[case::equal(1, 1)]
    #[case::reversed(2, 1)]
    #[tokio::test]
    async fn fixture_rejects_invalid_version_ranges(#[case] start: u64, #[case] end: u64) {
        assert_result_error_with_message(
            write_compaction_from_commits("memory:///", &InMemory::new(), start, end).await,
            "invalid compaction fixture range",
        );
    }
}
