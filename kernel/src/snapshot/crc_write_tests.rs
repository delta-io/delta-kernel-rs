use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rstest::rstest;
use url::Url;

use super::snapshot_crc::SnapshotCrc;
use super::{ChecksumWriteResult, IncrementalReplay, SnapshotRef};
use crate::committer::FileSystemCommitter;
use crate::crc::FileStatsState;
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
use crate::metrics::{CrcWrite, CrcWriteOutcome, CrcWriteResolutionMode, MetricEvent};
use crate::object_store::memory::InMemory;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, StructField, StructType};
use crate::transaction::UpdateTableOperation;
use crate::transaction::create_table::create_table;
use crate::unit_test_utils::{CapturingReporter, install_thread_local_metrics_reporter};
use crate::{
    Engine, FileMeta, FileSlice, KernelError, KernelResult, KernelResultIterator, Result, Snapshot,
    StorageHandler,
};

#[derive(Clone, Copy)]
enum CrcRoot {
    Cached,
    StaleCrc,
    VersionZero,
    Checkpoint,
}

fn create_snapshot(engine: &dyn Engine) -> KernelResult<SnapshotRef> {
    let schema = Arc::new(StructType::try_new([StructField::nullable(
        "café",
        DataType::INTEGER,
    )])?);
    Ok(create_table("memory:///", schema, "test", true)
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?
        .unwrap_post_commit_snapshot())
}

fn snapshot_for_root(engine: &dyn Engine, root: CrcRoot) -> Result<SnapshotRef> {
    let snapshot = create_snapshot(engine)?;
    match root {
        CrcRoot::Cached => return Ok(snapshot),
        CrcRoot::StaleCrc => {
            let (_, base) = snapshot.write_checksum(engine)?;
            base.update_table(UpdateTableOperation::Write, "test", true)
                .build(engine, Box::new(FileSystemCommitter::new()))?
                .commit(engine)?
                .unwrap_committed();
        }
        CrcRoot::Checkpoint => {
            snapshot.checkpoint(engine, None)?;
        }
        CrcRoot::VersionZero => {}
    }
    Snapshot::builder_for("memory:///")
        .with_incremental_crc_replay(IncrementalReplay::Disabled)
        .build(engine)
}

fn write_events(reporter: &CapturingReporter) -> Vec<CrcWrite> {
    reporter
        .events()
        .into_iter()
        .filter_map(|event| match event {
            MetricEvent::CrcWrite(event) => Some(event),
            _ => None,
        })
        .collect()
}

#[rstest]
#[case::cached(CrcRoot::Cached, CrcWriteResolutionMode::Cached)]
#[case::stale_crc(CrcRoot::StaleCrc, CrcWriteResolutionMode::Incremental)]
#[case::version_zero(CrcRoot::VersionZero, CrcWriteResolutionMode::Full)]
#[case::checkpoint(CrcRoot::Checkpoint, CrcWriteResolutionMode::Full)]
fn crc_write_reports_utf8_bytes_and_completes_while_snapshot_is_alive(
    #[case] root: CrcRoot,
    #[case] expected_mode: CrcWriteResolutionMode,
) -> Result<()> {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
    let snapshot = snapshot_for_root(&engine, root)?;
    let initial_write_count = write_events(&reporter).len();

    let start = Instant::now();
    let (result, updated) = snapshot.write_checksum(&engine)?;
    let elapsed = start.elapsed();
    assert_eq!(result, ChecksumWriteResult::Written);

    let events = write_events(&reporter);
    assert_eq!(events.len(), initial_write_count + 1);
    let event = &events[initial_write_count];
    assert_eq!(event.resolution_mode, expected_mode);
    assert!(event.duration > Duration::ZERO);
    assert!(event.duration <= elapsed);

    let path = ParsedLogPath::new_crc(updated.table_root(), updated.version())?;
    let data = engine
        .storage_handler()
        .read_files(vec![(path.location, None)])?
        .next()
        .unwrap()?;
    let text = std::str::from_utf8(&data).unwrap();
    assert!(text.contains("café"));
    assert!(text.len() > text.chars().count());
    assert_eq!(
        event.outcome,
        CrcWriteOutcome::Written {
            bytes_written: data.len() as u64,
        }
    );
    assert!(updated.crc_at_version().is_some());
    Ok(())
}

#[rstest]
#[case::early_exists(false, CrcWriteResolutionMode::NotAttempted)]
#[case::storage_race(true, CrcWriteResolutionMode::Cached)]
fn crc_write_already_exists_reports_no_bytes(
    #[case] use_original_snapshot: bool,
    #[case] expected_mode: CrcWriteResolutionMode,
) -> KernelResult<()> {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
    let original = create_snapshot(&engine)?;
    let (_, updated) = original.write_checksum(&engine)?;
    let snapshot = if use_original_snapshot {
        original
    } else {
        updated
    };

    let start = Instant::now();
    let (result, returned) = snapshot.write_checksum(&engine)?;
    let elapsed = start.elapsed();
    assert_eq!(result, ChecksumWriteResult::AlreadyExists);
    assert!(Arc::ptr_eq(&snapshot, &returned));

    let events = write_events(&reporter);
    assert_eq!(events.len(), 2);
    let event = &events[1];
    assert_eq!(event.outcome, CrcWriteOutcome::AlreadyExists);
    assert_eq!(event.resolution_mode, expected_mode);
    assert!(event.duration > Duration::ZERO);
    assert!(event.duration <= elapsed);
    Ok(())
}

#[rstest]
#[case::indeterminate_stats(CrcRoot::Cached, CrcWriteResolutionMode::Cached)]
#[case::stale_crc_read_error(CrcRoot::StaleCrc, CrcWriteResolutionMode::Incremental)]
#[case::version_zero_read_error(CrcRoot::VersionZero, CrcWriteResolutionMode::Full)]
fn crc_write_resolution_or_validation_error_retains_mode_and_duration(
    #[case] root: CrcRoot,
    #[case] expected_mode: CrcWriteResolutionMode,
) -> Result<()> {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
    let mut snapshot = snapshot_for_root(&engine, root)?;
    if let CrcRoot::Cached = root {
        let mut crc = snapshot.crc_at_version().unwrap().as_ref().clone();
        crc.file_stats_state = FileStatsState::Indeterminate;
        let version = snapshot.version();
        Arc::get_mut(&mut snapshot).unwrap().crc =
            SnapshotCrc::try_new(Some(Arc::new(crc)), version, None)?;
    } else {
        // Corrupt the required commit after loading the snapshot so resolution itself fails.
        let path = ParsedLogPath::new_commit(snapshot.table_root(), snapshot.version())?;
        engine.storage_handler().put(
            &path.location,
            Bytes::from_static(b"invalid commit JSON"),
            true,
        )?;
    }
    let initial_write_count = write_events(&reporter).len();

    let start = Instant::now();
    assert!(snapshot.write_checksum(&engine).is_err());
    let elapsed = start.elapsed();
    let events = write_events(&reporter);
    assert_eq!(events.len(), initial_write_count + 1);
    let event = &events[initial_write_count];
    assert_eq!(event.outcome, CrcWriteOutcome::Error);
    assert_eq!(event.resolution_mode, expected_mode);
    assert!(event.duration > Duration::ZERO);
    assert!(event.duration <= elapsed);
    Ok(())
}

#[test]
fn crc_write_snapshot_update_error_reports_no_bytes_after_successful_put() -> KernelResult<()> {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
    let mut snapshot = create_snapshot(&engine)?;
    // Force log-segment construction to fail after the CRC reaches storage.
    Arc::get_mut(&mut snapshot).unwrap().log_segment.end_version += 1;

    assert!(snapshot.write_checksum(&engine).is_err());
    let path = ParsedLogPath::new_crc(snapshot.table_root(), snapshot.version())?;
    assert!(engine.storage_handler().head(&path.location).is_ok());
    let events = write_events(&reporter);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome, CrcWriteOutcome::Error);
    assert_eq!(events[0].resolution_mode, CrcWriteResolutionMode::Cached);
    assert!(events[0].duration > Duration::ZERO);
    Ok(())
}

struct PutStorage {
    storage: Arc<dyn StorageHandler>,
    delay: Duration,
    fail: bool,
}

impl StorageHandler for PutStorage {
    fn list_from(&self, path: &Url) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>> {
        self.storage.list_from(path)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>> {
        self.storage.read_files(files)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        self.storage.copy_atomic(src, dest)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        std::thread::sleep(self.delay);
        if self.fail {
            return Err(KernelError::generic("CRC storage write failed"));
        }
        self.storage.put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        self.storage.head(path)
    }

    fn delete(&self, path: &Url) -> KernelResult<()> {
        self.storage.delete(path)
    }
}

#[rstest]
#[case::cached(CrcRoot::Cached, CrcWriteResolutionMode::Cached)]
#[case::stale_crc(CrcRoot::StaleCrc, CrcWriteResolutionMode::Incremental)]
#[case::version_zero(CrcRoot::VersionZero, CrcWriteResolutionMode::Full)]
#[case::checkpoint(CrcRoot::Checkpoint, CrcWriteResolutionMode::Full)]
fn crc_write_duration_includes_storage_even_on_error(
    #[case] root: CrcRoot,
    #[case] expected_mode: CrcWriteResolutionMode,
    #[values(false, true)] fail: bool,
) -> Result<()> {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = Arc::new(SyncEngine::new_with_store(Arc::new(InMemory::new())));
    let snapshot = snapshot_for_root(engine.as_ref(), root)?;
    let delay = Duration::from_millis(10);
    let storage = Arc::new(PutStorage {
        storage: engine.storage_handler(),
        delay,
        fail,
    });
    let engine = DelegatingEngine::new(engine).with_storage_handler(storage);
    let initial_write_count = write_events(&reporter).len();

    let result = snapshot.write_checksum(&engine);
    assert_eq!(result.is_err(), fail);
    let events = write_events(&reporter);
    assert_eq!(events.len(), initial_write_count + 1);
    let event = &events[initial_write_count];
    assert_eq!(
        event.outcome.as_ref(),
        if fail { "error" } else { "written" }
    );
    assert_eq!(event.resolution_mode, expected_mode);
    assert!(event.duration >= delay);
    Ok(())
}
