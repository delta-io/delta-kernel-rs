use std::sync::Mutex;

use bytes::Bytes;
use itertools::Itertools;
use rstest::rstest;
use test_utils::{TestAction, actions_to_string_catalog_managed, add_commit, add_staged_commit};
use url::Url;

use super::*;
use crate::committer::{Committer, FileSystemCommitter, PublishMetadata};
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
use crate::log_path::LogPath;
use crate::object_store::memory::InMemory;
use crate::snapshot::{PublicationWatermark, SnapshotHint};
use crate::transaction::UpdateTableOperation;
use crate::{FileMeta, FileSlice, Snapshot, SnapshotRef, StorageHandler};

#[derive(Clone, Copy, Debug)]
enum Source {
    Full,
    Incremental,
    Cached,
    Hint(Option<Version>),
}

fn commit_actions(version: Version, downgrade: bool) -> KernelResult<String> {
    let mut actions = serde_json::json!({"commitInfo": {
        "timestamp": 1_587_968_586_155_u64 + version,
        "inCommitTimestamp": 1_587_968_586_155_u64 + version,
        "txnId": format!("test-txn-{version}"), "operation": "WRITE"
    }})
    .to_string();
    if downgrade {
        actions.push('\n');
        actions.push_str(
            &serde_json::json!({"protocol": Protocol::try_new_modern(
            std::iter::empty::<TableFeature>(), [TableFeature::InCommitTimestamp],
        )?})
            .to_string(),
        );
    }
    Ok(actions)
}

async fn read_snapshot(
    engine: &dyn Engine,
    store: &InMemory,
    source: Source,
    checkpointed: bool,
    downgrade: bool,
    publish_prefix: bool,
) -> KernelResult<SnapshotRef, Box<dyn std::error::Error>> {
    add_commit(
        "memory:///",
        store,
        0,
        actions_to_string_catalog_managed(vec![TestAction::Metadata]),
    )
    .await?;
    let base = Snapshot::builder_for("memory:///")
        .with_max_catalog_version(0)
        .build(engine)?;
    let base = if checkpointed {
        base.checkpoint(engine, None)?.1
    } else {
        base
    };
    let mut tail = Vec::new();
    for version in 1..=2 {
        let actions = commit_actions(version, downgrade && version == 2)?;
        let staged = add_staged_commit("memory:///", store, version, actions.clone()).await?;
        if version == 1 && publish_prefix {
            add_commit("memory:///", store, version, actions.clone()).await?;
        }
        tail.push(LogPath::try_new(FileMeta::new(
            Url::parse("memory:///")?.join(staged.as_ref())?,
            0,
            actions.len() as u64,
        ))?);
    }
    let snapshot = match source {
        Source::Incremental => Snapshot::builder_from(base)
            .with_log_tail(tail)
            .with_max_catalog_version(2)
            .build(engine)?,
        _ => Snapshot::builder_for("memory:///")
            .with_log_tail(tail)
            .with_max_catalog_version(2)
            .build(engine)?,
    };
    Ok(match source {
        Source::Full | Source::Incremental => Ok(snapshot),
        Source::Cached => Snapshot::builder_from(snapshot).at_version(2).build(engine),
        Source::Hint(watermark) => {
            let exported = snapshot.to_snapshot_hint()?;
            let paths = exported
                .log_segment_files()
                .iter_all_paths()
                .unique_by(|path| &path.location.location)
                .map(|path| LogPath::try_new(path.location.clone()))
                .collect::<KernelResult<_>>()?;
            let hint = SnapshotHint::try_new(
                "memory:///",
                2,
                watermark.map_or(
                    PublicationWatermark::NoPublishedCommits,
                    PublicationWatermark::PublishedThrough,
                ),
                paths,
                exported.protocol().clone(),
                exported.metadata().clone(),
                exported.last_checkpoint_hint().cloned(),
                None,
                exported.freshness(),
            )?;
            Snapshot::builder_for("memory:///")
                .at_version(2)
                .with_snapshot_hint(hint)
                .build(engine)
        }
    }?)
}

#[derive(Debug, PartialEq)]
enum Event {
    Head(Version),
    Copy(Version),
    Commit,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    None,
    HeadIo,
    FallbackHeadIo,
    CopyIo,
    PrefixCopyIo,
    CopyMissing,
    ConcurrentPublish,
    ConcurrentPublishAndDelete,
}

struct PublicationStorage {
    inner: Arc<dyn StorageHandler>,
    events: Arc<Mutex<Vec<Event>>>,
    fault: Mutex<Fault>,
}

fn version(path: &Url) -> Version {
    path.path_segments().unwrap().next_back().unwrap()[..20]
        .parse()
        .unwrap()
}

impl StorageHandler for PublicationStorage {
    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        self.events.lock().unwrap().push(Event::Head(version(path)));
        let fault = *self.fault.lock().unwrap();
        if fault == Fault::HeadIo
            || (fault == Fault::FallbackHeadIo
                && self
                    .events
                    .lock()
                    .unwrap()
                    .contains(&Event::Copy(version(path))))
        {
            return Err(KernelError::IOError(std::io::Error::other("head failed")));
        }
        self.inner.head(path)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        let v = version(dest);
        self.events.lock().unwrap().push(Event::Copy(v));
        if v == 1 && *self.fault.lock().unwrap() == Fault::PrefixCopyIo {
            return Err(KernelError::IOError(std::io::Error::other(
                "prefix copy failed",
            )));
        }
        if v == 2 {
            self.inner.head(&Url::parse(
                "memory:///_delta_log/00000000000000000001.json",
            )?)?;
            match *self.fault.lock().unwrap() {
                Fault::CopyIo => {
                    return Err(KernelError::IOError(std::io::Error::other("copy failed")));
                }
                Fault::CopyMissing | Fault::FallbackHeadIo => {
                    return Err(KernelError::FileNotFound(src.to_string()));
                }
                Fault::ConcurrentPublish => {
                    self.inner.copy_atomic(src, dest)?;
                    return Err(KernelError::FileAlreadyExists(dest.to_string()));
                }
                Fault::ConcurrentPublishAndDelete => {
                    self.inner.copy_atomic(src, dest)?;
                    self.inner.delete(src)?;
                    return Err(KernelError::FileNotFound(src.to_string()));
                }
                Fault::None | Fault::HeadIo | Fault::PrefixCopyIo => (),
            }
        }
        self.inner.copy_atomic(src, dest)
    }

    fn list_from(&self, path: &Url) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>> {
        panic!("publication must not list {path}")
    }
    fn read_files(&self, _: Vec<FileSlice>) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>> {
        panic!("publication must use atomic copy")
    }
    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        self.inner.put(path, data, overwrite)
    }
    fn delete(&self, path: &Url) -> KernelResult<()> {
        self.inner.delete(path)
    }
}

struct ObservedCommitter {
    events: Arc<Mutex<Vec<Event>>>,
    catalog_managed: bool,
}

impl Committer for ObservedCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        self.events.lock().unwrap().push(Event::Commit);
        if self.catalog_managed {
            return Ok(CommitResponse::Conflict {
                version: metadata.version(),
            });
        }
        engine.storage_handler().head(&Url::parse(&format!(
            "memory:///_delta_log/{:020}.json",
            metadata.version() - 1,
        ))?)?;
        FileSystemCommitter::new().commit(engine, actions, metadata)
    }
    fn is_catalog_committer(&self) -> bool {
        self.catalog_managed
    }
    fn publish(&self, _: &dyn Engine, _: PublishMetadata) -> KernelResult<()> {
        panic!("path-based recovery does not require a catalog committer")
    }
}

fn observed_engine(
    engine: Arc<dyn Engine>,
    fault: Fault,
) -> (DelegatingEngine, Arc<PublicationStorage>) {
    let storage = Arc::new(PublicationStorage {
        inner: engine.storage_handler(),
        events: Arc::new(Mutex::new(Vec::new())),
        fault: Mutex::new(fault),
    });
    (
        DelegatingEngine::new(engine).with_storage_handler(storage.clone()),
        storage,
    )
}

fn transaction(
    snapshot: SnapshotRef,
    engine: &dyn Engine,
    events: Arc<Mutex<Vec<Event>>>,
) -> KernelResult<Transaction> {
    let catalog_managed = snapshot.table_configuration().is_catalog_managed();
    snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .build(
            engine,
            Box::new(ObservedCommitter {
                events,
                catalog_managed,
            }),
        )
}

#[rstest]
#[case::full(Source::Full)]
#[case::incremental(Source::Incremental)]
#[case::cached(Source::Cached)]
#[case::hint_no_watermark(Source::Hint(None))]
#[case::hint_stale_watermark(Source::Hint(Some(0)))]
#[case::hint_claims_published(Source::Hint(Some(2)))]
#[tokio::test(flavor = "multi_thread")]
async fn filesystem_commit_publishes_downgrade(
    #[case] source: Source,
    #[values(false, true)] checkpointed: bool,
    #[values(false, true)] already_published: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    // GIVEN a ratified downgrade at version 2, with its prefix published and staged paths retained.
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let snapshot = read_snapshot(
        engine.as_ref(),
        store.as_ref(),
        source,
        checkpointed,
        true,
        true,
    )
    .await?;
    let catalog_commits = snapshot
        .log_segment()
        .listed
        .staged_commits()
        .map(|p| CatalogCommit::try_new(&snapshot.log_segment().log_root, p))
        .collect::<KernelResult<Vec<_>>>()?;
    let downgrade = catalog_commits.last().unwrap();
    if already_published {
        engine
            .storage_handler()
            .copy_atomic(downgrade.location(), downgrade.published_location())?;
        for commit in &catalog_commits[..catalog_commits.len() - 1] {
            engine.storage_handler().delete(commit.location())?;
        }
    }
    let original_watermark = snapshot.log_segment().listed.max_published_version;
    let (write_engine, storage) = observed_engine(engine.clone(), Fault::None);

    // WHEN a filesystem writer attempts version 3 from the selected snapshot source.
    let result = transaction(snapshot.clone(), &write_engine, storage.events.clone())?
        .commit(&write_engine)?
        .unwrap_post_commit_snapshot();

    // THEN only an unpublished downgrade is copied, and version 3 is readable from the filesystem.
    assert_eq!(result.version(), 3);
    assert!(!result.table_configuration().is_catalog_managed());
    let events = storage.events.lock().unwrap();
    let copies = events
        .iter()
        .filter_map(|event| match event {
            Event::Copy(v) => Some(*v),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(copies, if already_published { vec![] } else { vec![2] });
    assert_eq!(
        snapshot.log_segment().listed.max_published_version,
        original_watermark
    );
    assert_eq!(result.log_segment().listed.max_published_version, Some(3));
    assert_eq!(
        Snapshot::builder_for("memory:///")
            .build(engine.as_ref())?
            .version(),
        3
    );
    Ok(())
}

#[rstest]
#[case::head_io(Fault::HeadIo, false)]
#[case::fallback_head_io(Fault::FallbackHeadIo, false)]
#[case::copy_io(Fault::CopyIo, false)]
#[case::missing_source(Fault::CopyMissing, false)]
#[case::concurrent_publication(Fault::ConcurrentPublish, true)]
#[case::concurrent_publication_source_removed(Fault::ConcurrentPublishAndDelete, true)]
#[tokio::test(flavor = "multi_thread")]
async fn publication_failure_or_race(
    #[case] fault: Fault,
    #[case] succeeds: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    // GIVEN an unpublished downgrade and an injected publication failure or competing publisher.
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let snapshot = read_snapshot(
        engine.as_ref(),
        store.as_ref(),
        Source::Full,
        false,
        true,
        true,
    )
    .await?;
    let staged_source = snapshot
        .log_segment()
        .listed
        .staged_commits()
        .last()
        .unwrap()
        .location
        .location
        .to_string();
    let (write_engine, storage) = observed_engine(engine.clone(), fault);
    let txn = transaction(snapshot.clone(), &write_engine, storage.events.clone())?;

    // WHEN the successor attempts to publish the downgrade before committing.
    let result = txn.commit(&write_engine);

    // THEN only successful concurrent publication permits the successor commit.
    assert_eq!(
        storage.events.lock().unwrap().contains(&Event::Commit),
        succeeds
    );
    if succeeds {
        result?.unwrap_committed();
    } else {
        let successor = Url::parse("memory:///_delta_log/00000000000000000003.json")?;
        assert!(matches!(
            engine.storage_handler().head(&successor),
            Err(KernelError::FileNotFound(_))
        ));
        let retry = if fault == Fault::CopyMissing {
            assert!(
                matches!(result, Err(KernelError::FileNotFound(path)) if path == staged_source)
            );
            transaction(snapshot, &write_engine, storage.events.clone())?
        } else {
            let CommitResult::Retryable(retryable) = result? else {
                panic!("expected retryable publication error")
            };
            retryable.transaction
        };

        // WHEN the injected failure is removed and the transaction is retried.
        *storage.fault.lock().unwrap() = Fault::None;

        // THEN publication completes and the successor can commit.
        retry.commit(&write_engine)?.unwrap_committed();
    }
    Ok(())
}

#[rstest]
#[case::full(Source::Full)]
#[case::incremental(Source::Incremental)]
#[case::cached(Source::Cached)]
#[case::hint(Source::Hint(None))]
#[tokio::test(flavor = "multi_thread")]
async fn catalog_write_does_not_publish(
    #[case] source: Source,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    // GIVEN a CM snapshot with staged commits and a storage handler that rejects publication HEADs.
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let snapshot =
        read_snapshot(engine.as_ref(), store.as_ref(), source, false, false, true).await?;
    let (write_engine, storage) = observed_engine(engine, Fault::HeadIo);

    // WHEN the next write uses the catalog committer.
    let result =
        transaction(snapshot, &write_engine, storage.events.clone())?.commit(&write_engine)?;

    // THEN it reaches the committer without synchronous publication, returning its test conflict.
    assert!(matches!(result, CommitResult::Conflicted(_)));
    assert_eq!(*storage.events.lock().unwrap(), vec![Event::Commit]);
    Ok(())
}

#[rstest]
#[case::incremental(false)]
#[case::hint(true)]
#[tokio::test(flavor = "multi_thread")]
async fn published_successor_covers_historical_staged_paths(
    #[case] use_hint: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    // GIVEN an incremental snapshot containing a published FS successor and historical staged
    // paths.
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let snapshot = read_snapshot(
        engine.as_ref(),
        store.as_ref(),
        Source::Full,
        false,
        true,
        true,
    )
    .await?;
    let staged = snapshot
        .log_segment()
        .listed
        .staged_commits()
        .map(|p| CatalogCommit::try_new(&snapshot.log_segment().log_root, p))
        .collect::<KernelResult<Vec<_>>>()?;
    let downgrade = staged.last().unwrap();
    engine
        .storage_handler()
        .copy_atomic(downgrade.location(), downgrade.published_location())?;
    add_commit("memory:///", store.as_ref(), 3, commit_actions(3, false)?).await?;
    let snapshot = Snapshot::builder_from(snapshot).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 3);
    assert_eq!(snapshot.log_segment().listed.staged_commits().count(), 2);
    let snapshot = if use_hint {
        Snapshot::builder_for("memory:///")
            .at_version(3)
            .with_snapshot_hint(snapshot.to_snapshot_hint()?)
            .build(engine.as_ref())?
    } else {
        snapshot
    };
    for commit in staged {
        engine.storage_handler().delete(commit.location())?;
    }
    let (write_engine, storage) = observed_engine(engine, Fault::HeadIo);

    // WHEN another filesystem write starts after the staged sources have been cleaned up.
    snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .build(&write_engine, Box::new(FileSystemCommitter::new()))?
        .commit(&write_engine)?
        .unwrap_committed();

    // THEN the published successor proves the prefix was published, without publication I/O.
    assert!(storage.events.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_only_history_does_not_publish() -> KernelResult<(), Box<dyn std::error::Error>>
{
    // GIVEN an FS snapshot reconstructed from a checkpoint after its JSON commit was removed.
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let snapshot = super::super::create_table::create_table(
        "memory:///",
        schema_ref! { nullable "id": INTEGER },
        "test-engine",
        true,
    )
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_post_commit_snapshot();
    snapshot.checkpoint(engine.as_ref(), None)?;
    engine.storage_handler().delete(&Url::parse(
        "memory:///_delta_log/00000000000000000000.json",
    )?)?;
    let snapshot = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let (write_engine, storage) = observed_engine(engine, Fault::HeadIo);

    // WHEN a filesystem writer commits the next version.
    snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .build(&write_engine, Box::new(FileSystemCommitter::new()))?
        .commit(&write_engine)?
        .unwrap_committed();

    // THEN checkpoint-only history requires no staged-commit publication I/O.
    assert!(storage.events.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn published_downgrade_with_crc_survives_staged_source_cleanup()
-> KernelResult<(), Box<dyn std::error::Error>> {
    // GIVEN a cached downgrade with a CRC, now published with all retained staged sources deleted.
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let snapshot = read_snapshot(
        engine.as_ref(),
        store.as_ref(),
        Source::Full,
        false,
        true,
        true,
    )
    .await?;
    let snapshot = snapshot.write_checksum(engine.as_ref())?.1;
    let staged = snapshot
        .log_segment()
        .listed
        .staged_commits()
        .map(|p| CatalogCommit::try_new(&snapshot.log_segment().log_root, p))
        .collect::<KernelResult<Vec<_>>>()?;
    let downgrade = staged.last().unwrap();
    engine
        .storage_handler()
        .copy_atomic(downgrade.location(), downgrade.published_location())?;
    for commit in staged {
        engine.storage_handler().delete(commit.location())?;
    }
    let (write_engine, storage) = observed_engine(engine, Fault::CopyIo);

    // WHEN a filesystem write uses the cached snapshot.
    transaction(snapshot, &write_engine, storage.events.clone())?
        .commit(&write_engine)?
        .unwrap_committed();

    // THEN observing the published downgrade avoids copying deleted sources.
    assert!(
        !storage
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, Event::Copy(_)))
    );
    Ok(())
}

#[rstest]
#[case::cached(false)]
#[case::hint(true)]
#[tokio::test(flavor = "multi_thread")]
async fn unpublished_downgrade_survives_published_prefix_cleanup(
    #[case] use_hint: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    // GIVEN an unpublished downgrade at 3 and a published, checkpointed prefix with cleaned-up
    // files.
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let base = read_snapshot(
        engine.as_ref(),
        store.as_ref(),
        Source::Full,
        false,
        false,
        true,
    )
    .await?;
    let segment = base.log_segment();
    let retained = segment
        .listed
        .staged_commits()
        .map(|p| CatalogCommit::try_new(&segment.log_root, p))
        .collect::<KernelResult<Vec<_>>>()?;
    let last = retained.last().unwrap();
    engine
        .storage_handler()
        .copy_atomic(last.location(), last.published_location())?;
    let published = Snapshot::builder_for("memory:///")
        .with_max_catalog_version(2)
        .build(engine.as_ref())?;
    let actions = commit_actions(3, true)?;
    let staged = add_staged_commit("memory:///", store.as_ref(), 3, actions.clone()).await?;
    let tail = vec![LogPath::try_new(FileMeta::new(
        Url::parse("memory:///")?.join(staged.as_ref())?,
        0,
        actions.len() as u64,
    ))?];
    let snapshot = Snapshot::builder_from(base)
        .skip_new_checkpoints()
        .with_log_tail(tail)
        .with_max_catalog_version(3)
        .build(engine.as_ref())?;
    let snapshot = snapshot.write_checksum(engine.as_ref())?.1;
    let snapshot = if use_hint {
        Snapshot::builder_for("memory:///")
            .at_version(3)
            .with_snapshot_hint(snapshot.to_snapshot_hint()?)
            .build(engine.as_ref())?
    } else {
        snapshot
    };
    published.checkpoint(engine.as_ref(), None)?;
    for commit in &retained {
        engine.storage_handler().delete(commit.location())?;
    }
    engine
        .storage_handler()
        .delete(retained[0].published_location())?;
    assert_eq!(snapshot.log_segment().listed.staged_commits().count(), 3);
    let (write_engine, storage) = observed_engine(engine, Fault::None);

    // WHEN a filesystem writer attempts version 4 from the cached snapshot or its hint.
    let result = transaction(snapshot, &write_engine, storage.events.clone())?
        .commit(&write_engine)?
        .unwrap_post_commit_snapshot();

    // THEN the search stops at published version 2 and only copies downgrade 3 before committing.
    assert_eq!(result.version(), 4);
    assert_eq!(
        *storage.events.lock().unwrap(),
        vec![
            Event::Head(3),
            Event::Head(2),
            Event::Copy(3),
            Event::Commit,
            Event::Head(3)
        ]
    );
    Ok(())
}

#[rstest]
#[case::success(Fault::None)]
#[case::prefix_failure(Fault::PrefixCopyIo)]
#[case::terminal_failure(Fault::CopyIo)]
#[tokio::test(flavor = "multi_thread")]
async fn multiple_pending_commits_publish_in_order_and_retry_partial_progress(
    #[case] fault: Fault,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    // GIVEN a ratified tail whose downgrade was staged without publishing its predecessor.
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let snapshot = read_snapshot(
        engine.as_ref(),
        store.as_ref(),
        Source::Full,
        false,
        true,
        false,
    )
    .await?;
    let pending = snapshot.log_segment().get_unpublished_catalog_commits()?;
    assert_eq!(pending.len(), 2);
    let (write_engine, storage) = observed_engine(engine.clone(), fault);

    // WHEN a filesystem writer attempts recovery, possibly failing during publication.
    let result =
        transaction(snapshot, &write_engine, storage.events.clone())?.commit(&write_engine)?;

    // THEN publication proceeds in order, and any failure prevents the successor commit.
    if fault == Fault::None {
        result.unwrap_committed();
    } else {
        let CommitResult::Retryable(retryable) = result else {
            panic!("expected publication retry")
        };
        assert!(!storage.events.lock().unwrap().contains(&Event::Commit));
        assert_eq!(
            engine
                .storage_handler()
                .head(pending[0].published_location())
                .is_ok(),
            fault == Fault::CopyIo
        );
        assert!(matches!(
            engine
                .storage_handler()
                .head(pending[1].published_location()),
            Err(KernelError::FileNotFound(_))
        ));
        assert!(matches!(
            engine.storage_handler().head(&Url::parse(
                "memory:///_delta_log/00000000000000000003.json"
            )?),
            Err(KernelError::FileNotFound(_))
        ));
        if fault == Fault::PrefixCopyIo {
            assert!(!storage.events.lock().unwrap().contains(&Event::Copy(2)));
        }

        // WHEN the same transaction retries after the publication failure clears.
        *storage.fault.lock().unwrap() = Fault::None;

        // THEN recovery preserves completed publication and commits the successor.
        retryable
            .transaction
            .commit(&write_engine)?
            .unwrap_committed();
    }

    // THEN both published files match their ratified sources and the successor is readable.
    for commit in pending {
        let bytes = engine
            .storage_handler()
            .read_files(vec![
                (commit.location().clone(), None),
                (commit.published_location().clone(), None),
            ])?
            .collect::<KernelResult<Vec<_>>>()?;
        assert_eq!(bytes[0], bytes[1]);
    }
    assert_eq!(
        Snapshot::builder_for("memory:///")
            .build(engine.as_ref())?
            .version(),
        3
    );
    Ok(())
}
