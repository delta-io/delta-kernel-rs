#[path = "../examples/support/mod.rs"]
mod support;

use std::process::Command;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::memory::{LimitedBytes, MemoryBudget, MemoryError};
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{Error, KernelError, Snapshot};

#[test]
fn global_budget() {
    let Ok(mode) = std::env::var("KERNEL_GLOBAL_TEST_MODE") else {
        let fixture = support::Fixture::create(20, 10, 10, Some(16384)).unwrap();
        assert!(fixture.directory.path().is_dir());
        assert_eq!(
            (
                fixture.width,
                fixture.initial_files,
                fixture.tail_commits,
                fixture.schema_bytes
            ),
            (20, 10, 10, 16384)
        );
        for mode in ["lifecycle", "exhaustion", "dual", "late"] {
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "global_budget", "--nocapture"])
                .env("KERNEL_GLOBAL_TEST_MODE", mode)
                .env("KERNEL_GLOBAL_TEST_TABLE", &fixture.table_root)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "mode={mode}\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };
    if mode == "late" {
        let _allocator = MemoryBudget::new(100).allocator();
        assert_eq!(
            MemoryBudget::install_global(100).unwrap_err(),
            MemoryError::AlreadyConfigured
        );
        return;
    }
    let cap = match mode.as_str() {
        "dual" => 64,
        "exhaustion" => 0,
        _ => 8 * 1024 * 1024,
    };
    let global = MemoryBudget::install_global(cap).unwrap();
    assert_eq!(
        MemoryBudget::install_global(cap).unwrap_err(),
        MemoryError::AlreadyConfigured
    );
    if mode == "dual" {
        let local = MemoryBudget::new(1024);
        let bytes = LimitedBytes::try_copy(&local, &[0; 64]).unwrap();
        assert_eq!(global.usage().reserved, 64);
        assert_eq!(local.usage().reserved, 64);
        assert!(LimitedBytes::try_copy(&local, &[0; 1]).is_err());
        assert_eq!(local.usage().reserved, 64);
        drop(bytes);
        let small = MemoryBudget::new(16);
        assert!(LimitedBytes::try_copy(&small, &[0; 32]).is_err());
        assert_eq!(global.usage().reserved, 0);
        assert_eq!(small.usage().reserved, 0);
        return;
    }
    let root = std::env::var("KERNEL_GLOBAL_TEST_TABLE").unwrap();
    let loaded = std::thread::spawn(move || Snapshot::builder_for(root).build(&SyncEngine::new()))
        .join()
        .unwrap();
    if mode == "exhaustion" {
        let error = loaded.unwrap_err();
        assert!(
            matches!(error, Error::Kernel(ref error)
            if matches!(error.without_backtrace(), KernelError::Memory(_))),
            "{error}"
        );
        assert_eq!(global.usage().reserved, 0);
        assert!(global.usage().rejected > 0);
        return;
    }
    let snapshot = loaded.unwrap();
    assert!(global.usage().reserved > 0);
    let moved = snapshot.clone();
    std::thread::spawn(move || {
        let engine = SyncEngine::new();
        let scan = moved.clone().scan_builder().build().unwrap();
        let mut files = 0;
        for batch in scan.scan_metadata(&engine).unwrap() {
            files = batch
                .unwrap()
                .visit_scan_files(files, |count, _| *count += 1)
                .unwrap();
        }
        assert_eq!(files, 20);
        let transaction = moved
            .update_table(UpdateTableOperation::AlterTable, "global-test", false)
            .with_table_properties_added([("poc.global", "yes")])
            .build(&engine, Box::new(FileSystemCommitter::new()))
            .unwrap();
        let state = transaction.write_state().unwrap();
        let before = MemoryBudget::global_usage().unwrap().reserved;
        let encoded = state.encode().unwrap();
        assert!(MemoryBudget::global_usage().unwrap().reserved >= before + encoded.len());
        drop(state);
        std::thread::spawn(move || drop(encoded)).join().unwrap();
        transaction.commit(&engine).unwrap().unwrap_committed();
    })
    .join()
    .unwrap();
    drop(snapshot);
    assert!(global.usage().peak <= cap);
    assert_eq!(global.usage().reserved, 0);
}
